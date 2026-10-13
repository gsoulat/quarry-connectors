//! SDK d'un connecteur Quarry : implémente [`Connector`], puis appelle
//! [`serve`] dans `main`. Le SDK lit les requêtes JSON sur l'entrée
//! standard, les transmet au connecteur et écrit les réponses.

use async_trait::async_trait;
use quarry_protocol::*;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

pub use async_trait::async_trait as connector_trait;
pub use quarry_protocol as protocol;

pub type Result<T> = std::result::Result<T, String>;

/// Décrit un connecteur avant connexion.
pub trait Describe {
    fn hello() -> Hello;
}

#[async_trait]
pub trait Connector: Send + Sync {
    async fn list_schemas(&self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
    async fn list_tables(&self) -> Result<Vec<TableRef>>;
    async fn describe_table(&self, table: &TableRef) -> Result<Vec<ColumnInfo>>;
    async fn list_relations(&self) -> Result<Vec<Relation>>;
    async fn explain(&self, sql: &str) -> Result<QueryPlan>;
    async fn query(&self, sql: &str) -> Result<QueryResult>;
    async fn execute(&self, sql: &str) -> Result<u64>;
}

/// Fabrique un connecteur à partir des paramètres de connexion.
#[async_trait]
pub trait Factory: Describe {
    type Connector: Connector;
    async fn connect(params: ConnectParams) -> Result<Self::Connector>;
}

/// Boucle de service : à appeler depuis `main`. Ne revient qu'à la fin de
/// l'entrée standard ou sur `close`.
pub async fn serve<F: Factory>() {
    let stdin = BufReader::new(tokio::io::stdin());
    let mut stdout = tokio::io::stdout();
    let mut lines = stdin.lines();
    let mut connector: Option<F::Connector> = None;

    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        let envelope: Envelope = match serde_json::from_str(&line) {
            Ok(e) => e,
            Err(e) => {
                let response = Response { id: 0, result: None, error: Some(format!("Requête illisible : {e}")) };
                write(&mut stdout, &response).await;
                continue;
            }
        };
        let id = envelope.id;
        let mut close = false;

        let outcome: Result<Payload> = match envelope.request {
            Request::Hello => Ok(Payload::Hello(F::hello())),
            Request::Connect(params) => match F::connect(params).await {
                Ok(c) => {
                    connector = Some(c);
                    Ok(Payload::Ok)
                }
                Err(e) => Err(e),
            },
            Request::Close => {
                close = true;
                Ok(Payload::Ok)
            }
            other => match &connector {
                None => Err("Pas de connexion ouverte (envoie `connect` d'abord)".into()),
                Some(c) => match other {
                    Request::ListSchemas => c.list_schemas().await.map(Payload::Schemas),
                    Request::ListTables => c.list_tables().await.map(Payload::Tables),
                    Request::DescribeTable(t) => c.describe_table(&t).await.map(Payload::Columns),
                    Request::ListRelations => c.list_relations().await.map(Payload::Relations),
                    Request::Explain { sql } => c.explain(&sql).await.map(Payload::Plan),
                    Request::Query { sql } => c.query(&sql).await.map(Payload::Result),
                    Request::Execute { sql } => c.execute(&sql).await.map(Payload::Affected),
                    Request::Hello | Request::Connect(_) | Request::Close => unreachable!(),
                },
            },
        };

        let response = match outcome {
            Ok(result) => Response { id, result: Some(result), error: None },
            Err(error) => Response { id, result: None, error: Some(error) },
        };
        write(&mut stdout, &response).await;
        if close {
            break;
        }
    }
}

async fn write(stdout: &mut tokio::io::Stdout, response: &Response) {
    let mut line = serde_json::to_string(response).unwrap_or_else(|e| {
        format!(r#"{{"id":{},"error":"Réponse non sérialisable : {e}"}}"#, response.id)
    });
    line.push('\n');
    let _ = stdout.write_all(line.as_bytes()).await;
    let _ = stdout.flush().await;
}

/// Décodeurs partagés par les connecteurs sqlx.
pub mod sqlx_support {
    /// Macro de décodage d'une ligne sqlx vers des `Cell`, en essayant les
    /// types courants un par un.
    #[macro_export]
    macro_rules! row_decoder {
        ($name:ident, $row:ty, unchecked_text: $unchecked:literal) => {
            fn $name(row: &$row) -> Vec<$crate::protocol::Cell> {
                use $crate::protocol::Cell;
                use sqlx::types::chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
                use sqlx::types::{JsonValue, Uuid};
                use sqlx::{Row, TypeInfo, ValueRef};
                (0..row.len())
                    .map(|i| {
                        let type_name = match row.try_get_raw(i) {
                            Ok(raw) if raw.is_null() => return Cell::Null,
                            Ok(raw) => raw.type_info().name().to_string(),
                            Err(e) => return Cell::Unsupported(e.to_string()),
                        };
                        if let Ok(v) = row.try_get::<i64, _>(i) {
                            Cell::Int(v)
                        } else if let Ok(v) = row.try_get::<i32, _>(i) {
                            Cell::Int(v.into())
                        } else if let Ok(v) = row.try_get::<i16, _>(i) {
                            Cell::Int(v.into())
                        } else if let Ok(v) = row.try_get::<f64, _>(i) {
                            Cell::Float(v)
                        } else if let Ok(v) = row.try_get::<f32, _>(i) {
                            Cell::Float(v.into())
                        } else if let Ok(v) = row.try_get::<bool, _>(i) {
                            Cell::Bool(v)
                        } else if let Ok(v) = row.try_get::<String, _>(i) {
                            Cell::Text(v)
                        } else if let Ok(v) = row.try_get::<DateTime<Utc>, _>(i) {
                            Cell::Text(v.to_rfc3339())
                        } else if let Ok(v) = row.try_get::<NaiveDateTime, _>(i) {
                            Cell::Text(v.to_string())
                        } else if let Ok(v) = row.try_get::<NaiveDate, _>(i) {
                            Cell::Text(v.to_string())
                        } else if let Ok(v) = row.try_get::<NaiveTime, _>(i) {
                            Cell::Text(v.to_string())
                        } else if let Ok(v) = row.try_get::<Uuid, _>(i) {
                            Cell::Text(v.to_string())
                        } else if let Ok(v) = row.try_get::<JsonValue, _>(i) {
                            Cell::Text(v.to_string())
                        } else if $unchecked
                            && let Ok(v) = row.try_get_unchecked::<String, _>(i)
                        {
                            Cell::Text(v)
                        } else {
                            Cell::Unsupported(type_name)
                        }
                    })
                    .collect()
            }
        };
    }

    /// Lit au plus `max_rows` lignes d'une requête sqlx, sans cache de
    /// requête préparée (le SQL vient de l'utilisateur).
    #[macro_export]
    macro_rules! run_query {
        ($pool:expr, $sql:expr, $decode:ident, $max_rows:expr) => {{
            use futures_util::TryStreamExt;
            use sqlx::{Column, Row};
            let mut stream = sqlx::query(sqlx::AssertSqlSafe($sql.to_owned())).persistent(false).fetch($pool);
            let mut result = $crate::protocol::QueryResult::default();
            while let Some(row) = stream.try_next().await.map_err(|e| e.to_string())? {
                if result.columns.is_empty() {
                    result.columns = row.columns().iter().map(|c| c.name().to_string()).collect();
                }
                result.rows.push($decode(&row));
                if result.rows.len() >= $max_rows {
                    break;
                }
            }
            Ok(result)
        }};
    }
}
