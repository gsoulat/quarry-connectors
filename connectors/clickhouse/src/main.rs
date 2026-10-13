//! Connecteur ClickHouse, par son interface HTTP (port 8123 ; `https://`
//! si l'hôte commence par `https://`). Pas de clés étrangères dans
//! ClickHouse : le diagramme n'a pas de liens.

use quarry_connector_sdk::protocol::*;
use quarry_connector_sdk::{Connector, Describe, Factory, Result, connector_trait, serve};
use serde::Deserialize;
use serde_json::Value;

const VERSION: &str = env!("CARGO_PKG_VERSION");

struct ClickHouse {
    client: reqwest::Client,
    url: String,
    user: String,
    password: String,
    database: String,
    max_rows: usize,
}

#[derive(Deserialize)]
struct Meta {
    name: String,
    #[serde(rename = "type")]
    data_type: String,
}

#[derive(Deserialize)]
struct Compact {
    meta: Vec<Meta>,
    data: Vec<Vec<Value>>,
}

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

fn cell(v: Value, data_type: &str) -> Cell {
    match v {
        Value::Null => Cell::Null,
        Value::Bool(b) => Cell::Bool(b),
        Value::Number(n) => n.as_i64().map(Cell::Int).or_else(|| n.as_f64().map(Cell::Float)).unwrap_or(Cell::Null),
        // Les entiers 64 bits et décimaux arrivent en texte : on retype si possible.
        Value::String(s) => {
            let t = data_type.trim_start_matches("Nullable(").trim_start_matches("LowCardinality(");
            if (t.starts_with("Int") || t.starts_with("UInt")) && let Ok(i) = s.parse::<i64>() {
                Cell::Int(i)
            } else if (t.starts_with("Float") || t.starts_with("Decimal")) && let Ok(f) = s.parse::<f64>() {
                Cell::Float(f)
            } else {
                Cell::Text(s)
            }
        }
        other => Cell::Text(other.to_string()),
    }
}

impl ClickHouse {
    async fn send(&self, sql: &str, with_format: bool) -> Result<String> {
        let query = if with_format { format!("{sql} FORMAT JSONCompact") } else { sql.to_string() };
        let response = self
            .client
            .post(&self.url)
            .header("X-ClickHouse-User", &self.user)
            .header("X-ClickHouse-Key", &self.password)
            .query(&[("database", self.database.as_str()), ("max_result_rows", &self.max_rows.to_string()), ("result_overflow_mode", "break")])
            .body(query)
            .send()
            .await
            .map_err(|e| format!("ClickHouse injoignable : {e}"))?;
        let status = response.status();
        let text = response.text().await.map_err(err)?;
        if !status.is_success() {
            return Err(text.lines().next().unwrap_or("erreur ClickHouse").to_string());
        }
        Ok(text)
    }

    async fn rows(&self, sql: &str) -> Result<QueryResult> {
        let text = self.send(sql, true).await?;
        if text.trim().is_empty() {
            return Ok(QueryResult::default());
        }
        let compact: Compact = serde_json::from_str(&text).map_err(|e| format!("Réponse ClickHouse illisible : {e}"))?;
        let types: Vec<String> = compact.meta.iter().map(|m| m.data_type.clone()).collect();
        Ok(QueryResult {
            columns: compact.meta.into_iter().map(|m| m.name).collect(),
            rows: compact
                .data
                .into_iter()
                .take(self.max_rows)
                .map(|row| row.into_iter().zip(&types).map(|(v, t)| cell(v, t)).collect())
                .collect(),
        })
    }
}

impl Describe for ClickHouse {
    fn hello() -> Hello {
        Hello {
            protocol: PROTOCOL_VERSION,
            id: "clickhouse".into(),
            engine: "ClickHouse".into(),
            version: VERSION.into(),
            dialect: Dialect {
                identifier_quote: '`',
                backslash_escapes: true,
                has_schemas: false,
                rename_table_statement: true,
                schema_is_database: true,
                types: ["UInt64", "Int64", "Int32", "Float64", "Decimal(18,3)", "Bool", "String", "LowCardinality(String)", "UUID", "Date", "DateTime", "DateTime64(3)", "Array(String)", "JSON"]
                    .map(String::from)
                    .to_vec(),
                id_type: "UInt64".into(),
                import_types: ["Int64".into(), "Float64".into(), "Bool".into(), "String".into()],
                create_table_suffix: Some("ENGINE = MergeTree ORDER BY tuple()".into()),
                preview_sql: None,
                rename_table_sql: None,
                rename_column_sql: None,
            },
        }
    }
}

#[connector_trait]
impl Factory for ClickHouse {
    type Connector = ClickHouse;

    async fn connect(p: ConnectParams) -> Result<ClickHouse> {
        let host = p.host.trim();
        if host.is_empty() {
            return Err("Hôte ClickHouse manquant".into());
        }
        let url = if host.starts_with("http://") || host.starts_with("https://") {
            host.to_string()
        } else {
            format!("http://{host}:{}/", p.port.unwrap_or(8123))
        };
        let ch = ClickHouse {
            client: reqwest::Client::builder().build().map_err(err)?,
            url,
            user: if p.user.trim().is_empty() { "default".into() } else { p.user.trim().into() },
            password: p.password.unwrap_or_default(),
            database: if p.database.trim().is_empty() { "default".into() } else { p.database.trim().into() },
            max_rows: p.max_rows,
        };
        ch.send("SELECT 1", false).await?;
        Ok(ch)
    }
}

#[connector_trait]
impl Connector for ClickHouse {
    async fn list_schemas(&self) -> Result<Vec<String>> {
        let r = self.rows("SELECT name FROM system.databases WHERE name NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') ORDER BY name").await?;
        Ok(r.rows.into_iter().filter_map(|row| row.into_iter().next()).map(|c| c.display()).collect())
    }

    async fn list_tables(&self) -> Result<Vec<TableRef>> {
        let r = self.rows("SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name").await?;
        Ok(r.rows.into_iter().map(|row| TableRef { schema: None, name: row[0].display() }).collect())
    }

    async fn describe_table(&self, table: &TableRef) -> Result<Vec<ColumnInfo>> {
        let name = table.name.replace('\'', "''");
        let r = self
            .rows(&format!(
                "SELECT name, type, type LIKE 'Nullable(%', default_expression, is_in_primary_key
                 FROM system.columns WHERE database = currentDatabase() AND table = '{name}' ORDER BY position"
            ))
            .await?;
        Ok(r.rows
            .into_iter()
            .map(|row| ColumnInfo {
                name: row[0].display(),
                data_type: row[1].display(),
                nullable: matches!(row[2], Cell::Int(1) | Cell::Bool(true)),
                default: Some(row[3].display()).filter(|d| !d.is_empty()),
                primary_key: matches!(row[4], Cell::Int(1) | Cell::Bool(true)),
            })
            .collect())
    }

    async fn list_relations(&self) -> Result<Vec<Relation>> {
        Ok(Vec::new())
    }

    async fn explain(&self, sql: &str) -> Result<QueryPlan> {
        let r = self.rows(&format!("EXPLAIN {sql}")).await?;
        let nodes = r
            .rows
            .iter()
            .filter_map(|row| row.first())
            .map(|c| c.display())
            .map(|line| {
                let depth = (line.len() - line.trim_start().len()) / 2;
                PlanNode { depth, label: line.trim().to_string(), cost: None, rows: None }
            })
            .collect();
        Ok(QueryPlan { nodes, total_cost: None, cost_unit: String::new(), note: Some("ClickHouse affiche son plan sans estimation de coût (EXPLAIN ESTIMATE donne les volumes).".into()) })
    }

    async fn query(&self, sql: &str) -> Result<QueryResult> {
        let head = sql.trim_start().to_uppercase();
        if head.starts_with("SELECT") || head.starts_with("WITH") || head.starts_with("SHOW") || head.starts_with("DESCRIBE") || head.starts_with("EXPLAIN") {
            self.rows(sql).await
        } else {
            self.send(sql, false).await?;
            Ok(QueryResult::default())
        }
    }

    async fn execute(&self, sql: &str) -> Result<u64> {
        self.send(sql, false).await?;
        Ok(0)
    }
}

#[tokio::main]
async fn main() {
    serve::<ClickHouse>().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Contre un ClickHouse local : `QUARRY_TEST_CLICKHOUSE=host:port:user:password`.
    #[tokio::test]
    async fn clickhouse_round_trip() {
        let Ok(spec) = std::env::var("QUARRY_TEST_CLICKHOUSE") else {
            eprintln!("QUARRY_TEST_CLICKHOUSE absent : test ignoré");
            return;
        };
        let parts: Vec<&str> = spec.split(':').collect();
        let ch = ClickHouse::connect(ConnectParams {
            host: parts[0].into(),
            port: parts.get(1).and_then(|p| p.parse().ok()),
            user: parts.get(2).unwrap_or(&"default").to_string(),
            password: parts.get(3).map(|p| p.to_string()),
            database: "default".into(),
            max_rows: 100,
            ..Default::default()
        })
        .await
        .expect("connexion");
        ch.execute("DROP TABLE IF EXISTS quarry_test").await.unwrap();
        ch.execute("CREATE TABLE quarry_test (id UInt64, name Nullable(String), score Float64) ENGINE = MergeTree ORDER BY id").await.unwrap();
        ch.execute("INSERT INTO quarry_test VALUES (1, 'Ada', 9.5), (2, NULL, 7)").await.unwrap();
        let r = ch.query("SELECT id, name, score FROM quarry_test ORDER BY id").await.unwrap();
        assert_eq!(r.columns, ["id", "name", "score"]);
        assert!(matches!(r.rows[0][0], Cell::Int(1)));
        assert!(matches!(r.rows[1][1], Cell::Null));
        let cols = ch.describe_table(&TableRef { schema: None, name: "quarry_test".into() }).await.unwrap();
        assert!(cols[0].primary_key && cols[1].nullable);
        assert!(ch.list_tables().await.unwrap().iter().any(|t| t.name == "quarry_test"));
        assert!(!ch.explain("SELECT * FROM quarry_test").await.unwrap().nodes.is_empty());
        ch.execute("DROP TABLE quarry_test").await.unwrap();
    }
}
