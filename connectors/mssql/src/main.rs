//! Connecteur SQL Server / Azure SQL (protocole TDS, crate tiberius).

use futures_util::TryStreamExt;
use quarry_connector_sdk::protocol::*;
use quarry_connector_sdk::{Connector, Describe, Factory, Result, connector_trait, serve};
use tiberius::time::chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use tiberius::{AuthMethod, Client, Config, EncryptionLevel, QueryItem, Row};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

const VERSION: &str = env!("CARGO_PKG_VERSION");

type Conn = Client<Compat<TcpStream>>;

struct MsSql {
    // Une connexion TDS ne sert qu'une requête à la fois.
    client: Mutex<Conn>,
    max_rows: usize,
}

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

fn cell(row: &Row, i: usize) -> Cell {
    // tiberius vérifie le type à chaque `try_get` : un essai raté est sans effet.
    if let Ok(Some(v)) = row.try_get::<i64, _>(i) {
        Cell::Int(v)
    } else if let Ok(Some(v)) = row.try_get::<i32, _>(i) {
        Cell::Int(v.into())
    } else if let Ok(Some(v)) = row.try_get::<i16, _>(i) {
        Cell::Int(v.into())
    } else if let Ok(Some(v)) = row.try_get::<u8, _>(i) {
        Cell::Int(v.into())
    } else if let Ok(Some(v)) = row.try_get::<f64, _>(i) {
        Cell::Float(v)
    } else if let Ok(Some(v)) = row.try_get::<f32, _>(i) {
        Cell::Float(v.into())
    } else if let Ok(Some(v)) = row.try_get::<bool, _>(i) {
        Cell::Bool(v)
    } else if let Ok(Some(v)) = row.try_get::<&str, _>(i) {
        Cell::Text(v.to_string())
    } else if let Ok(Some(v)) = row.try_get::<tiberius::numeric::Numeric, _>(i) {
        Cell::Text(v.to_string())
    } else if let Ok(Some(v)) = row.try_get::<tiberius::Uuid, _>(i) {
        Cell::Text(v.to_string())
    } else if let Ok(Some(v)) = row.try_get::<DateTime<Utc>, _>(i) {
        Cell::Text(v.to_string())
    } else if let Ok(Some(v)) = row.try_get::<NaiveDateTime, _>(i) {
        Cell::Text(v.to_string())
    } else if let Ok(Some(v)) = row.try_get::<NaiveDate, _>(i) {
        Cell::Text(v.to_string())
    } else if let Ok(Some(v)) = row.try_get::<NaiveTime, _>(i) {
        Cell::Text(v.to_string())
    } else if let Ok(Some(v)) = row.try_get::<&[u8], _>(i) {
        Cell::Unsupported(format!("binaire {} octets", v.len()))
    } else {
        // Toutes les tentatives renvoient None : la valeur est NULL.
        Cell::Null
    }
}

impl MsSql {
    async fn run(&self, sql: &str, max_rows: usize) -> Result<QueryResult> {
        let mut client = self.client.lock().await;
        let mut stream = client.simple_query(sql).await.map_err(err)?;
        let mut result = QueryResult::default();
        while let Some(item) = stream.try_next().await.map_err(err)? {
            match item {
                QueryItem::Metadata(meta) if result.columns.is_empty() => {
                    result.columns = meta.columns().iter().map(|c| c.name().to_string()).collect();
                }
                QueryItem::Row(row) => {
                    if result.columns.is_empty() {
                        result.columns = row.columns().iter().map(|c| c.name().to_string()).collect();
                    }
                    let n = result.columns.len();
                    result.rows.push((0..n).map(|i| cell(&row, i)).collect());
                    if result.rows.len() >= max_rows {
                        break;
                    }
                }
                QueryItem::Metadata(_) => {}
            }
        }
        Ok(result)
    }
}

impl Describe for MsSql {
    fn hello() -> Hello {
        Hello {
            protocol: PROTOCOL_VERSION,
            id: "mssql".into(),
            engine: "SQL Server".into(),
            version: VERSION.into(),
            dialect: Dialect {
                identifier_quote: '"',
                backslash_escapes: false,
                has_schemas: true,
                rename_table_statement: false,
                schema_is_database: false,
                types: ["INT", "BIGINT", "SMALLINT", "TINYINT", "DECIMAL(18,2)", "FLOAT", "BIT", "NVARCHAR(255)", "NVARCHAR(MAX)", "VARCHAR(255)", "UNIQUEIDENTIFIER", "DATE", "TIME", "DATETIME2", "DATETIMEOFFSET", "VARBINARY(MAX)"]
                    .map(String::from)
                    .to_vec(),
                id_type: "INT IDENTITY(1,1)".into(),
                import_types: ["BIGINT".into(), "FLOAT".into(), "BIT".into(), "NVARCHAR(MAX)".into()],
                create_table_suffix: None,
                preview_sql: Some("SELECT TOP {n} * FROM {table}".into()),
                rename_table_sql: Some("EXEC sp_rename '{table}', '{to}'".into()),
                rename_column_sql: Some("EXEC sp_rename '{table}.{from}', '{to}', 'COLUMN'".into()),
            },
        }
    }
}

#[connector_trait]
impl Factory for MsSql {
    type Connector = MsSql;

    async fn connect(p: ConnectParams) -> Result<MsSql> {
        let mut config = Config::new();
        config.host(p.host.trim());
        config.port(p.port.unwrap_or(1433));
        if !p.database.trim().is_empty() {
            config.database(p.database.trim());
        }
        config.authentication(AuthMethod::sql_server(p.user.trim(), p.password.clone().unwrap_or_default()));
        // Chiffrement demandé si possible ; certificat non vérifié (serveurs
        // locaux ou Docker avec certificat auto-signé). Azure SQL impose TLS.
        config.encryption(EncryptionLevel::Required);
        config.trust_cert();

        let tcp = TcpStream::connect(config.get_addr()).await.map_err(|e| format!("SQL Server injoignable : {e}"))?;
        tcp.set_nodelay(true).map_err(err)?;
        let client = Client::connect(config, tcp.compat_write()).await.map_err(err)?;
        Ok(MsSql { client: Mutex::new(client), max_rows: p.max_rows })
    }
}

#[connector_trait]
impl Connector for MsSql {
    async fn list_schemas(&self) -> Result<Vec<String>> {
        let r = self
            .run("SELECT name FROM sys.schemas WHERE name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest') AND name NOT LIKE 'db[_]%' ORDER BY (CASE WHEN name = 'dbo' THEN 0 ELSE 1 END), name", 1000)
            .await?;
        Ok(r.rows.into_iter().map(|row| row[0].display()).collect())
    }

    async fn list_tables(&self) -> Result<Vec<TableRef>> {
        let r = self
            .run("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW') ORDER BY 1, 2", 10_000)
            .await?;
        Ok(r.rows.into_iter().map(|row| TableRef { schema: Some(row[0].display()), name: row[1].display() }).collect())
    }

    async fn describe_table(&self, table: &TableRef) -> Result<Vec<ColumnInfo>> {
        let schema = table.schema.clone().unwrap_or_else(|| "dbo".into()).replace('\'', "''");
        let name = table.name.replace('\'', "''");
        let r = self
            .run(
                &format!(
                    "SELECT c.COLUMN_NAME,
                            c.DATA_TYPE + CASE WHEN c.CHARACTER_MAXIMUM_LENGTH IS NOT NULL
                                               THEN '(' + CASE WHEN c.CHARACTER_MAXIMUM_LENGTH = -1 THEN 'MAX' ELSE CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) END + ')'
                                               ELSE '' END,
                            CASE WHEN c.IS_NULLABLE = 'YES' THEN 1 ELSE 0 END,
                            c.COLUMN_DEFAULT,
                            CASE WHEN k.COLUMN_NAME IS NULL THEN 0 ELSE 1 END
                     FROM INFORMATION_SCHEMA.COLUMNS c
                     LEFT JOIN (
                         SELECT k.TABLE_SCHEMA, k.TABLE_NAME, k.COLUMN_NAME
                         FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
                         JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k ON k.CONSTRAINT_NAME = t.CONSTRAINT_NAME AND k.TABLE_SCHEMA = t.TABLE_SCHEMA
                         WHERE t.CONSTRAINT_TYPE = 'PRIMARY KEY'
                     ) k ON k.TABLE_SCHEMA = c.TABLE_SCHEMA AND k.TABLE_NAME = c.TABLE_NAME AND k.COLUMN_NAME = c.COLUMN_NAME
                     WHERE c.TABLE_SCHEMA = '{schema}' AND c.TABLE_NAME = '{name}'
                     ORDER BY c.ORDINAL_POSITION"
                ),
                10_000,
            )
            .await?;
        Ok(r.rows
            .into_iter()
            .map(|row| ColumnInfo {
                name: row[0].display(),
                data_type: row[1].display(),
                nullable: matches!(row[2], Cell::Int(1)),
                default: match &row[3] {
                    Cell::Null => None,
                    other => Some(other.display()),
                },
                primary_key: matches!(row[4], Cell::Int(1)),
            })
            .collect())
    }

    async fn list_relations(&self) -> Result<Vec<Relation>> {
        let r = self
            .run(
                "SELECT fk.name, s1.name, t1.name, c1.name, s2.name, t2.name, c2.name
                 FROM sys.foreign_key_columns fkc
                 JOIN sys.foreign_keys fk ON fk.object_id = fkc.constraint_object_id
                 JOIN sys.tables t1 ON t1.object_id = fkc.parent_object_id
                 JOIN sys.schemas s1 ON s1.schema_id = t1.schema_id
                 JOIN sys.columns c1 ON c1.object_id = fkc.parent_object_id AND c1.column_id = fkc.parent_column_id
                 JOIN sys.tables t2 ON t2.object_id = fkc.referenced_object_id
                 JOIN sys.schemas s2 ON s2.schema_id = t2.schema_id
                 JOIN sys.columns c2 ON c2.object_id = fkc.referenced_object_id AND c2.column_id = fkc.referenced_column_id
                 ORDER BY fk.name, fkc.constraint_column_id",
                10_000,
            )
            .await?;
        let mut relations: Vec<(String, Relation)> = Vec::new();
        for row in r.rows {
            let name = row[0].display();
            match relations.last_mut() {
                Some((n, rel)) if *n == name => {
                    rel.from_columns.push(row[3].display());
                    rel.to_columns.push(row[6].display());
                }
                _ => relations.push((
                    name,
                    Relation {
                        from: TableRef { schema: Some(row[1].display()), name: row[2].display() },
                        from_columns: vec![row[3].display()],
                        to: TableRef { schema: Some(row[4].display()), name: row[5].display() },
                        to_columns: vec![row[6].display()],
                    },
                )),
            }
        }
        Ok(relations.into_iter().map(|(_, r)| r).collect())
    }

    /// `SET SHOWPLAN_ALL ON` : plan estimé sans exécution, avec le coût de
    /// chaque sous-arbre.
    async fn explain(&self, sql: &str) -> Result<QueryPlan> {
        self.run("SET SHOWPLAN_ALL ON", 1).await?;
        let plan = self.run(sql, 10_000).await;
        let _ = self.run("SET SHOWPLAN_ALL OFF", 1).await;
        let r = plan?;
        let col = |name: &str| r.columns.iter().position(|c| c.eq_ignore_ascii_case(name));
        let (Some(text), Some(node_id), Some(parent), Some(cost), Some(rows)) =
            (col("StmtText"), col("NodeId"), col("Parent"), col("TotalSubtreeCost"), col("EstimateRows"))
        else {
            return Err("Plan SQL Server inattendu".into());
        };
        let number = |c: &Cell| match c {
            Cell::Int(i) => Some(*i as f64),
            Cell::Float(f) => Some(*f),
            Cell::Text(t) => t.parse().ok(),
            _ => None,
        };
        let mut depth_of: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
        let mut nodes = Vec::new();
        let mut total = None;
        for row in &r.rows {
            let id = number(&row[node_id]).unwrap_or(0.0) as i64;
            let parent_id = number(&row[parent]).unwrap_or(0.0) as i64;
            let depth = depth_of.get(&parent_id).map_or(0, |d| d + 1);
            depth_of.insert(id, depth);
            let node_cost = number(&row[cost]);
            if total.is_none() {
                total = node_cost;
            }
            nodes.push(PlanNode { depth, label: row[text].display().trim().to_string(), cost: node_cost, rows: number(&row[rows]) });
        }
        // La première ligne est l'instruction elle-même : on la retire si un
        // vrai plan la suit.
        if nodes.len() > 1 {
            nodes.remove(0);
            for n in &mut nodes {
                n.depth = n.depth.saturating_sub(1);
            }
        }
        Ok(QueryPlan { nodes, total_cost: total, cost_unit: "unités SQL Server".into(), note: None })
    }

    async fn query(&self, sql: &str) -> Result<QueryResult> {
        self.run(sql, self.max_rows).await
    }

    async fn execute(&self, sql: &str) -> Result<u64> {
        let mut client = self.client.lock().await;
        let result = client.execute(sql, &[]).await.map_err(err)?;
        Ok(result.total())
    }
}

#[tokio::main]
async fn main() {
    serve::<MsSql>().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Contre un SQL Server local : `QUARRY_TEST_MSSQL=host:port:user:password`.
    #[tokio::test]
    async fn mssql_round_trip() {
        let Ok(spec) = std::env::var("QUARRY_TEST_MSSQL") else {
            eprintln!("QUARRY_TEST_MSSQL absent : test ignoré");
            return;
        };
        let parts: Vec<&str> = spec.splitn(4, ':').collect();
        let db = MsSql::connect(ConnectParams {
            host: parts[0].into(),
            port: parts.get(1).and_then(|p| p.parse().ok()),
            user: parts.get(2).unwrap_or(&"sa").to_string(),
            password: parts.get(3).map(|p| p.to_string()),
            max_rows: 100,
            ..Default::default()
        })
        .await
        .expect("connexion");
        db.execute("IF OBJECT_ID('dbo.quarry_orders') IS NOT NULL DROP TABLE dbo.quarry_orders").await.unwrap();
        db.execute("IF OBJECT_ID('dbo.quarry_users') IS NOT NULL DROP TABLE dbo.quarry_users").await.unwrap();
        db.execute("CREATE TABLE dbo.quarry_users (id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(50) NULL, score FLOAT)").await.unwrap();
        db.execute("CREATE TABLE dbo.quarry_orders (id INT PRIMARY KEY, user_id INT REFERENCES dbo.quarry_users(id))").await.unwrap();
        assert_eq!(db.execute("INSERT INTO dbo.quarry_users (name, score) VALUES (N'Ada', 9.5), (NULL, 7)").await.unwrap(), 2);
        let r = db.query("SELECT id, name, score FROM dbo.quarry_users ORDER BY id").await.unwrap();
        assert_eq!(r.columns, ["id", "name", "score"]);
        assert!(matches!(r.rows[0][0], Cell::Int(1)));
        assert!(matches!(r.rows[1][1], Cell::Null));
        let users = TableRef { schema: Some("dbo".into()), name: "quarry_users".into() };
        let cols = db.describe_table(&users).await.unwrap();
        assert!(cols[0].primary_key && cols[1].nullable);
        let rel = db.list_relations().await.unwrap();
        assert!(rel.iter().any(|r| r.from.name == "quarry_orders" && r.to.name == "quarry_users"));
        let plan = db.explain("SELECT * FROM dbo.quarry_users WHERE id = 1").await.unwrap();
        assert!(!plan.nodes.is_empty() && plan.total_cost.is_some());
        assert!(db.list_schemas().await.unwrap().contains(&"dbo".to_string()));
        db.execute("DROP TABLE dbo.quarry_orders; DROP TABLE dbo.quarry_users").await.unwrap();
    }
}
