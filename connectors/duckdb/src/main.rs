//! Connecteur DuckDB : base analytique embarquée (fichier `.duckdb`, ou
//! `:memory:`). La bibliothèque est compilée dans le binaire.

use std::sync::{Arc, Mutex};

use duckdb::types::ValueRef;
use duckdb::{Connection, params};
use quarry_connector_sdk::protocol::*;
use quarry_connector_sdk::{Connector, Describe, Factory, Result, connector_trait, serve};

const VERSION: &str = env!("CARGO_PKG_VERSION");

struct DuckDb {
    // DuckDB est synchrone : chaque appel passe par `spawn_blocking`.
    conn: Arc<Mutex<Connection>>,
    max_rows: usize,
}

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

fn cell(v: ValueRef<'_>) -> Cell {
    match v {
        ValueRef::Null => Cell::Null,
        ValueRef::Boolean(b) => Cell::Bool(b),
        ValueRef::TinyInt(i) => Cell::Int(i.into()),
        ValueRef::SmallInt(i) => Cell::Int(i.into()),
        ValueRef::Int(i) => Cell::Int(i.into()),
        ValueRef::BigInt(i) => Cell::Int(i),
        ValueRef::HugeInt(i) => i64::try_from(i).map_or_else(|_| Cell::Text(i.to_string()), Cell::Int),
        ValueRef::UTinyInt(i) => Cell::Int(i.into()),
        ValueRef::USmallInt(i) => Cell::Int(i.into()),
        ValueRef::UInt(i) => Cell::Int(i.into()),
        ValueRef::UBigInt(i) => i64::try_from(i).map_or_else(|_| Cell::Text(i.to_string()), Cell::Int),
        ValueRef::Float(f) => Cell::Float(f.into()),
        ValueRef::Double(f) => Cell::Float(f),
        ValueRef::Text(t) => Cell::Text(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(b) => Cell::Unsupported(format!("BLOB {} octets", b.len())),
        other => {
            // Dates, timestamps, décimaux, listes… : représentation texte de DuckDB.
            match duckdb::types::Value::from(other) {
                duckdb::types::Value::Null => Cell::Null,
                v => Cell::Text(format!("{v:?}")),
            }
        }
    }
}

impl DuckDb {
    /// Exécute une fermeture sur la connexion, hors du runtime async.
    async fn with<T: Send + 'static>(&self, f: impl FnOnce(&Connection) -> Result<T> + Send + 'static) -> Result<T> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|_| "connexion DuckDB verrouillée".to_string())?;
            f(&guard)
        })
        .await
        .map_err(err)?
    }

    fn run(conn: &Connection, sql: &str, max_rows: usize) -> Result<QueryResult> {
        let mut stmt = conn.prepare(sql).map_err(err)?;
        let mut rows = stmt.query(params![]).map_err(err)?;
        let mut result = QueryResult::default();
        while let Some(row) = rows.next().map_err(err)? {
            if result.columns.is_empty() {
                let stmt = row.as_ref();
                result.columns = (0..stmt.column_count()).map(|i| stmt.column_name(i).map(String::from).unwrap_or_default()).collect();
            }
            let n = result.columns.len();
            result.rows.push((0..n).map(|i| row.get_ref(i).map_or(Cell::Null, cell)).collect());
            if result.rows.len() >= max_rows {
                break;
            }
        }
        if result.columns.is_empty() {
            // Requête sans ligne : on récupère tout de même les noms.
            result.columns = stmt.column_names();
        }
        Ok(result)
    }
}

impl Describe for DuckDb {
    fn hello() -> Hello {
        Hello {
            protocol: PROTOCOL_VERSION,
            id: "duckdb".into(),
            engine: "DuckDB".into(),
            version: VERSION.into(),
            dialect: Dialect {
                identifier_quote: '"',
                backslash_escapes: false,
                has_schemas: true,
                rename_table_statement: false,
                schema_is_database: false,
                types: ["INTEGER", "BIGINT", "HUGEINT", "DOUBLE", "DECIMAL(18,3)", "BOOLEAN", "VARCHAR", "DATE", "TIMESTAMP", "BLOB", "JSON"]
                    .map(String::from)
                    .to_vec(),
                id_type: "INTEGER".into(),
                import_types: ["BIGINT".into(), "DOUBLE".into(), "BOOLEAN".into(), "VARCHAR".into()],
                create_table_suffix: None,
                preview_sql: None,
                rename_table_sql: None,
                rename_column_sql: None,
            },
        }
    }
}

#[connector_trait]
impl Factory for DuckDb {
    type Connector = DuckDb;

    async fn connect(p: ConnectParams) -> Result<DuckDb> {
        let path = p.path.trim().to_owned();
        if path.is_empty() {
            return Err("Chemin du fichier DuckDB manquant (ou :memory:)".into());
        }
        let max_rows = p.max_rows;
        let conn = tokio::task::spawn_blocking(move || {
            if path == ":memory:" { Connection::open_in_memory() } else { Connection::open(&path) }
        })
        .await
        .map_err(err)?
        .map_err(err)?;
        Ok(DuckDb { conn: Arc::new(Mutex::new(conn)), max_rows })
    }
}

#[connector_trait]
impl Connector for DuckDb {
    async fn list_schemas(&self) -> Result<Vec<String>> {
        let r = self
            .with(|c| Self::run(c, "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = current_database() ORDER BY (schema_name <> 'main'), schema_name", 1000))
            .await?;
        Ok(r.rows.into_iter().filter_map(|row| row.into_iter().next()).map(|c| c.display()).collect())
    }

    async fn list_tables(&self) -> Result<Vec<TableRef>> {
        let r = self
            .with(|c| Self::run(c, "SELECT table_schema, table_name FROM information_schema.tables WHERE table_catalog = current_database() ORDER BY 1, 2", 10_000))
            .await?;
        Ok(r.rows
            .into_iter()
            .map(|row| TableRef { schema: Some(row[0].display()), name: row[1].display() })
            .collect())
    }

    async fn describe_table(&self, table: &TableRef) -> Result<Vec<ColumnInfo>> {
        let schema = table.schema.clone().unwrap_or_else(|| "main".into());
        let name = table.name.clone();
        self.with(move |c| {
            let pk: Vec<String> = {
                let mut stmt = c
                    .prepare("SELECT unnest(constraint_column_names) FROM duckdb_constraints() WHERE schema_name = ? AND table_name = ? AND constraint_type = 'PRIMARY KEY'")
                    .map_err(err)?;
                let rows = stmt.query_map(params![schema, name], |r| r.get::<_, String>(0)).map_err(err)?;
                rows.filter_map(|r| r.ok()).collect()
            };
            let mut stmt = c
                .prepare("SELECT column_name, data_type, is_nullable = 'YES', column_default FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position")
                .map_err(err)?;
            let rows = stmt
                .query_map(params![schema, name], |r| {
                    Ok(ColumnInfo {
                        name: r.get(0)?,
                        data_type: r.get(1)?,
                        nullable: r.get(2)?,
                        default: r.get(3)?,
                        primary_key: false,
                    })
                })
                .map_err(err)?;
            let mut cols: Vec<ColumnInfo> = rows.filter_map(|r| r.ok()).collect();
            for col in &mut cols {
                col.primary_key = pk.contains(&col.name);
            }
            Ok(cols)
        })
        .await
    }

    async fn list_relations(&self) -> Result<Vec<Relation>> {
        self.with(|c| {
            let mut stmt = c
                .prepare(
                    "SELECT schema_name, table_name, constraint_column_names, referenced_table, referenced_column_names
                     FROM duckdb_constraints() WHERE constraint_type = 'FOREIGN KEY'",
                )
                .map_err(err)?;
            let rows = stmt
                .query_map(params![], |r| {
                    Ok(Relation {
                        from: TableRef { schema: Some(r.get(0)?), name: r.get(1)? },
                        from_columns: list_of_strings(r.get_ref(2)?),
                        to: TableRef { schema: r.get::<_, String>(0).ok(), name: r.get(3)? },
                        to_columns: list_of_strings(r.get_ref(4)?),
                    })
                })
                .map_err(err)?;
            Ok(rows.filter_map(|r| r.ok()).collect())
        })
        .await
    }

    /// `EXPLAIN` : DuckDB renvoie l'arbre physique en texte, sans coût.
    async fn explain(&self, sql: &str) -> Result<QueryPlan> {
        let sql = format!("EXPLAIN {sql}");
        let r = self.with(move |c| Self::run(c, &sql, 100)).await?;
        let text = r.rows.iter().filter_map(|row| row.get(1)).map(|c| c.display()).collect::<Vec<_>>().join("\n");
        let nodes = text
            .lines()
            .filter(|l| l.contains('│') && l.chars().any(|ch| ch.is_alphanumeric()))
            .map(|l| PlanNode { depth: 0, label: l.trim_matches(|ch: char| ch == '│' || ch == ' ').to_string(), cost: None, rows: None })
            .filter(|n| !n.label.is_empty())
            .collect();
        Ok(QueryPlan { nodes, total_cost: None, cost_unit: String::new(), note: Some("DuckDB affiche son plan physique sans estimation de coût.".into()) })
    }

    async fn query(&self, sql: &str) -> Result<QueryResult> {
        let sql = sql.to_owned();
        let max = self.max_rows;
        self.with(move |c| Self::run(c, &sql, max)).await
    }

    async fn execute(&self, sql: &str) -> Result<u64> {
        let sql = sql.to_owned();
        self.with(move |c| c.execute(&sql, params![]).map(|n| n as u64).map_err(err)).await
    }
}

/// Un `VARCHAR[]` DuckDB lu comme texte `[a, b]` → noms de colonnes.
fn list_of_strings(v: ValueRef<'_>) -> Vec<String> {
    let text = match duckdb::types::Value::from(v) {
        duckdb::types::Value::List(items) => {
            return items
                .into_iter()
                .map(|i| match i {
                    duckdb::types::Value::Text(s) => s,
                    other => format!("{other:?}"),
                })
                .collect();
        }
        duckdb::types::Value::Text(s) => s,
        other => format!("{other:?}"),
    };
    text.trim_matches(|c| c == '[' || c == ']')
        .split(',')
        .map(|s| s.trim().trim_matches('\'').to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

#[tokio::main]
async fn main() {
    serve::<DuckDb>().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_round_trip() {
        let db = DuckDb::connect(ConnectParams { path: ":memory:".into(), max_rows: 100, ..Default::default() }).await.unwrap();
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR)").await.unwrap();
        db.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER REFERENCES users(id))").await.unwrap();
        db.execute("INSERT INTO users VALUES (1, 'Ada'), (2, NULL)").await.unwrap();
        let tables = db.list_tables().await.unwrap();
        assert_eq!(tables.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(), ["orders", "users"]);
        let cols = db.describe_table(&tables[1]).await.unwrap();
        assert!(cols[0].primary_key);
        let r = db.query("SELECT id, name FROM users ORDER BY id").await.unwrap();
        assert_eq!(r.columns, ["id", "name"]);
        assert!(matches!(r.rows[0][0], Cell::Int(1)));
        assert!(matches!(r.rows[1][1], Cell::Null));
        let rel = db.list_relations().await.unwrap();
        assert_eq!(rel.len(), 1);
        assert_eq!(rel[0].from_columns, ["user_id"]);
        assert_eq!(rel[0].to.name, "users");
        assert!(!db.explain("SELECT * FROM users").await.unwrap().nodes.is_empty());
        assert!(db.list_schemas().await.unwrap().contains(&"main".to_string()));
    }
}
