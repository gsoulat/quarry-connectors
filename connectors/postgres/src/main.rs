//! Connecteur PostgreSQL : un processus lancé par Quarry.

use quarry_connector_sdk::protocol::*;
use quarry_connector_sdk::{Connector, Describe, Factory, Result, connector_trait, row_decoder, run_query, serve};
use serde_json::Value;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgRow};

const VERSION: &str = env!("CARGO_PKG_VERSION");

struct Postgres {
    pool: PgPool,
    max_rows: usize,
}

fn err(e: sqlx::Error) -> String {
    e.to_string()
}

row_decoder!(decode, PgRow, unchecked_text: false);

impl Describe for Postgres {
    fn hello() -> Hello {
        Hello {
            protocol: PROTOCOL_VERSION,
            id: "postgres".into(),
            engine: "PostgreSQL".into(),
            version: VERSION.into(),
            dialect: Dialect {
                identifier_quote: '"',
                backslash_escapes: false,
                has_schemas: true,
                rename_table_statement: false,
                schema_is_database: false,
                types: [
                    "INTEGER", "BIGINT", "SMALLINT", "SERIAL", "BIGSERIAL", "NUMERIC", "REAL",
                    "DOUBLE PRECISION", "BOOLEAN", "TEXT", "VARCHAR(255)", "UUID", "DATE", "TIME",
                    "TIMESTAMP", "TIMESTAMPTZ", "JSON", "JSONB", "BYTEA",
                ]
                .map(String::from)
                .to_vec(),
                id_type: "SERIAL".into(),
                import_types: ["BIGINT".into(), "DOUBLE PRECISION".into(), "BOOLEAN".into(), "TEXT".into()],
            },
        }
    }
}

#[connector_trait]
impl Factory for Postgres {
    type Connector = Postgres;

    async fn connect(p: ConnectParams) -> Result<Postgres> {
        let mut options = PgConnectOptions::new()
            .host(&p.host)
            .port(p.port.unwrap_or(5432))
            .username(&p.user);
        if let Some(password) = &p.password {
            options = options.password(password);
        }
        if !p.database.is_empty() {
            options = options.database(&p.database);
        }
        let pool = PgPoolOptions::new().max_connections(4).connect_with(options).await.map_err(err)?;
        Ok(Postgres { pool, max_rows: p.max_rows })
    }
}

#[connector_trait]
impl Connector for Postgres {
    async fn list_schemas(&self) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT nspname::text FROM pg_namespace
             WHERE nspname NOT LIKE 'pg\\_%' AND nspname <> 'information_schema'
             ORDER BY (nspname <> 'public'), nspname",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(err)?;
        Ok(rows.into_iter().map(|(n,)| n).collect())
    }

    async fn list_tables(&self) -> Result<Vec<TableRef>> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT table_schema::text, table_name::text
             FROM information_schema.tables
             WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
             ORDER BY 1, 2",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(err)?;
        Ok(rows.into_iter().map(|(schema, name)| TableRef { schema: Some(schema), name }).collect())
    }

    async fn describe_table(&self, table: &TableRef) -> Result<Vec<ColumnInfo>> {
        let rows: Vec<(String, String, bool, Option<String>, bool)> = sqlx::query_as(
            "SELECT c.column_name::text,
                    c.data_type::text,
                    c.is_nullable = 'YES',
                    c.column_default::text,
                    EXISTS (
                        SELECT 1
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage k
                          ON k.constraint_name = tc.constraint_name
                         AND k.table_schema = tc.table_schema
                         AND k.table_name = tc.table_name
                        WHERE tc.constraint_type = 'PRIMARY KEY'
                          AND tc.table_schema = c.table_schema
                          AND tc.table_name = c.table_name
                          AND k.column_name = c.column_name
                    )
             FROM information_schema.columns c
             WHERE c.table_schema = $1 AND c.table_name = $2
             ORDER BY c.ordinal_position",
        )
        .bind(table.schema.as_deref().unwrap_or("public"))
        .bind(&table.name)
        .fetch_all(&self.pool)
        .await
        .map_err(err)?;
        Ok(rows
            .into_iter()
            .map(|(name, data_type, nullable, default, primary_key)| ColumnInfo {
                name,
                data_type,
                nullable,
                default,
                primary_key,
            })
            .collect())
    }

    async fn list_relations(&self) -> Result<Vec<Relation>> {
        let rows: Vec<(String, String, Vec<String>, String, String, Vec<String>)> = sqlx::query_as(
            "SELECT n.nspname::text, c.relname::text,
                    array_agg(a.attname::text ORDER BY k.ord),
                    fn.nspname::text, fc.relname::text,
                    array_agg(fa.attname::text ORDER BY k.ord)
             FROM pg_constraint con
             JOIN pg_class c ON c.oid = con.conrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             JOIN pg_class fc ON fc.oid = con.confrelid
             JOIN pg_namespace fn ON fn.oid = fc.relnamespace
             CROSS JOIN LATERAL unnest(con.conkey, con.confkey)
                 WITH ORDINALITY AS k(attnum, fattnum, ord)
             JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
             JOIN pg_attribute fa ON fa.attrelid = fc.oid AND fa.attnum = k.fattnum
             WHERE con.contype = 'f'
             GROUP BY con.oid, n.nspname, c.relname, fn.nspname, fc.relname
             ORDER BY 1, 2",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(err)?;
        Ok(rows
            .into_iter()
            .map(|(schema, name, cols, to_schema, to_name, to_cols)| Relation {
                from: TableRef { schema: Some(schema), name },
                from_columns: cols,
                to: TableRef { schema: Some(to_schema), name: to_name },
                to_columns: to_cols,
            })
            .collect())
    }

    /// `EXPLAIN (FORMAT JSON)`, sans ANALYZE : la requête n'est pas exécutée.
    async fn explain(&self, sql: &str) -> Result<QueryPlan> {
        let (json,): (Value,) = sqlx::query_as(sqlx::AssertSqlSafe(format!("EXPLAIN (FORMAT JSON) {sql}")))
            .persistent(false)
            .fetch_one(&self.pool)
            .await
            .map_err(err)?;
        let root = json.get(0).and_then(|v| v.get("Plan")).ok_or("Plan PostgreSQL illisible")?;
        let mut nodes = Vec::new();
        walk(root, 0, &mut nodes);
        Ok(QueryPlan {
            total_cost: root.get("Total Cost").and_then(Value::as_f64),
            nodes,
            cost_unit: "unités PostgreSQL".into(),
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

fn walk(node: &Value, depth: usize, out: &mut Vec<PlanNode>) {
    let field = |key: &str| node.get(key).and_then(Value::as_str);
    let mut label = field("Node Type").unwrap_or("?").to_string();
    if let Some(join) = field("Join Type") {
        label = format!("{label} ({join})");
    }
    if let Some(relation) = field("Relation Name") {
        label.push_str(&format!(" sur {relation}"));
    }
    if let Some(index) = field("Index Name") {
        label.push_str(&format!(" via {index}"));
    }
    out.push(PlanNode {
        depth,
        label,
        cost: node.get("Total Cost").and_then(Value::as_f64),
        rows: node.get("Plan Rows").and_then(Value::as_f64),
    });
    for child in node.get("Plans").and_then(Value::as_array).into_iter().flatten() {
        walk(child, depth + 1, out);
    }
}

#[tokio::main]
async fn main() {
    serve::<Postgres>().await;
}
