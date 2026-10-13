//! Connecteur MySQL / MariaDB : un processus lancé par Quarry.

use quarry_connector_sdk::protocol::*;
use quarry_connector_sdk::{Connector, Describe, Factory, Result, connector_trait, row_decoder, run_query, serve};
use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlRow};

const VERSION: &str = env!("CARGO_PKG_VERSION");

struct MySql {
    pool: MySqlPool,
    max_rows: usize,
}

fn err(e: sqlx::Error) -> String {
    e.to_string()
}

// MySQL transmet DECIMAL & co. sous forme de texte : décodage « unchecked ».
row_decoder!(decode, MySqlRow, unchecked_text: true);

impl Describe for MySql {
    fn hello() -> Hello {
        Hello {
            protocol: PROTOCOL_VERSION,
            id: "mysql".into(),
            engine: "MySQL".into(),
            version: VERSION.into(),
            dialect: Dialect {
                identifier_quote: '`',
                backslash_escapes: true,
                has_schemas: false,
                rename_table_statement: true,
                schema_is_database: true,
                types: [
                    "INT", "BIGINT", "SMALLINT", "TINYINT", "DECIMAL(10,2)", "FLOAT", "DOUBLE", "BOOLEAN",
                    "VARCHAR(255)", "TEXT", "CHAR(36)", "DATE", "TIME", "DATETIME", "TIMESTAMP", "JSON", "BLOB",
                ]
                .map(String::from)
                .to_vec(),
                id_type: "INT AUTO_INCREMENT".into(),
                import_types: ["BIGINT".into(), "DOUBLE".into(), "BOOLEAN".into(), "TEXT".into()],
            },
        }
    }
}

#[connector_trait]
impl Factory for MySql {
    type Connector = MySql;

    async fn connect(p: ConnectParams) -> Result<MySql> {
        let mut options = MySqlConnectOptions::new()
            .host(&p.host)
            .port(p.port.unwrap_or(3306))
            .username(&p.user);
        if let Some(password) = &p.password {
            options = options.password(password);
        }
        if !p.database.is_empty() {
            options = options.database(&p.database);
        }
        let pool = MySqlPoolOptions::new().max_connections(4).connect_with(options).await.map_err(err)?;
        Ok(MySql { pool, max_rows: p.max_rows })
    }
}

#[connector_trait]
impl Connector for MySql {
    async fn list_tables(&self) -> Result<Vec<TableRef>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT CAST(table_name AS CHAR)
             FROM information_schema.tables
             WHERE table_schema = DATABASE()
             ORDER BY 1",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(err)?;
        Ok(rows.into_iter().map(|(name,)| TableRef { schema: None, name }).collect())
    }

    async fn describe_table(&self, table: &TableRef) -> Result<Vec<ColumnInfo>> {
        let rows: Vec<(String, String, i64, Option<String>, i64)> = sqlx::query_as(
            "SELECT CAST(column_name AS CHAR),
                    CAST(column_type AS CHAR),
                    is_nullable = 'YES',
                    CAST(column_default AS CHAR),
                    column_key = 'PRI'
             FROM information_schema.columns
             WHERE table_schema = DATABASE() AND table_name = ?
             ORDER BY ordinal_position",
        )
        .bind(&table.name)
        .fetch_all(&self.pool)
        .await
        .map_err(err)?;
        Ok(rows
            .into_iter()
            .map(|(name, data_type, nullable, default, pk)| ColumnInfo {
                name,
                data_type,
                nullable: nullable != 0,
                default,
                primary_key: pk != 0,
            })
            .collect())
    }

    async fn list_relations(&self) -> Result<Vec<Relation>> {
        let rows: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT CAST(table_name AS CHAR),
                    CAST(GROUP_CONCAT(column_name ORDER BY ordinal_position SEPARATOR ',') AS CHAR),
                    CAST(referenced_table_name AS CHAR),
                    CAST(GROUP_CONCAT(referenced_column_name ORDER BY ordinal_position SEPARATOR ',') AS CHAR)
             FROM information_schema.key_column_usage
             WHERE table_schema = DATABASE() AND referenced_table_name IS NOT NULL
             GROUP BY constraint_name, table_name, referenced_table_name
             ORDER BY 1",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(err)?;
        let split = |s: String| s.split(',').map(str::to_owned).collect();
        Ok(rows
            .into_iter()
            .map(|(name, cols, to_name, to_cols)| Relation {
                from: TableRef { schema: None, name },
                from_columns: split(cols),
                to: TableRef { schema: None, name: to_name },
                to_columns: split(to_cols),
            })
            .collect())
    }

    /// `EXPLAIN FORMAT=TREE` (MySQL 8.0.18+), par exemple :
    /// `-> Filter: (t.id > 3)  (cost=1.2 rows=3)`
    async fn explain(&self, sql: &str) -> Result<QueryPlan> {
        let (tree,): (String,) = sqlx::query_as(sqlx::AssertSqlSafe(format!("EXPLAIN FORMAT=TREE {sql}")))
            .persistent(false)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| format!("{e}\n(EXPLAIN FORMAT=TREE nécessite MySQL 8.0.18 ou plus)"))?;
        let nodes: Vec<PlanNode> = tree
            .lines()
            .filter_map(|line| {
                let trimmed = line.trim_start();
                let content = trimmed.strip_prefix("-> ")?;
                let (label, stats) = match content.rfind("  (") {
                    Some(i) => (&content[..i], &content[i..]),
                    None => (content, ""),
                };
                Some(PlanNode {
                    depth: (line.len() - trimmed.len()) / 4,
                    label: label.to_string(),
                    cost: number_after(stats, "cost="),
                    rows: number_after(stats, "rows="),
                })
            })
            .collect();
        Ok(QueryPlan {
            total_cost: nodes.first().and_then(|n| n.cost),
            nodes,
            cost_unit: "unités MySQL".into(),
            note: None,
        })
    }

    async fn query(&self, sql: &str) -> Result<QueryResult> {
        run_query!(&self.pool, sql, decode, self.max_rows)
    }

    async fn execute(&self, sql: &str) -> Result<u64> {
        Ok(sqlx::query(sqlx::AssertSqlSafe(sql.to_owned()))
            .persistent(false)
            .execute(&self.pool)
            .await
            .map_err(err)?
            .rows_affected())
    }
}

/// Lit le nombre qui suit `key` ; pour une plage `a..b`, garde `b`.
fn number_after(text: &str, key: &str) -> Option<f64> {
    let rest = &text[text.find(key)? + key.len()..];
    let end = rest
        .find(|c: char| !(c.is_ascii_digit() || matches!(c, '.' | 'e' | '+' | '-')))
        .unwrap_or(rest.len());
    rest[..end].rsplit("..").next()?.parse().ok()
}

#[tokio::main]
async fn main() {
    serve::<MySql>().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cost_range_keeps_upper_bound() {
        assert_eq!(number_after("(cost=0.25..0.35 rows=1)", "cost="), Some(0.35));
    }
}
