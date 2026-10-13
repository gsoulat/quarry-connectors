//! Connecteur Google BigQuery (API REST, SQL standard) : un processus
//! lancé par Quarry.

use gcp_bigquery_client::Client;
use gcp_bigquery_client::model::get_query_results_parameters::GetQueryResultsParameters;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::table_row::TableRow;
use gcp_bigquery_client::model::table_schema::TableSchema;
use serde_json::Value;

use quarry_connector_sdk::protocol::*;
use quarry_connector_sdk::{Connector, Describe, Factory, Result, connector_trait, serve};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Tarif « à la demande » de BigQuery, en dollars par tébioctet analysé.
const USD_PER_TIB: f64 = 6.25;
const QUERY_TIMEOUT_MS: i32 = 30_000;

pub struct BigQueryConnector {
    client: Client,
    project_id: String,
    max_rows: usize,
}

impl Describe for BigQueryConnector {
    fn hello() -> Hello {
        Hello {
            protocol: PROTOCOL_VERSION,
            id: "bigquery".into(),
            engine: "BigQuery".into(),
            version: VERSION.into(),
            dialect: Dialect {
                identifier_quote: '`',
                backslash_escapes: true,
                has_schemas: true,
                rename_table_statement: false,
                schema_is_database: false,
                types: ["INT64", "FLOAT64", "NUMERIC", "BOOL", "STRING", "BYTES", "DATE", "TIME", "DATETIME", "TIMESTAMP", "JSON"]
                    .map(String::from)
                    .to_vec(),
                id_type: "INT64".into(),
                import_types: ["INT64".into(), "FLOAT64".into(), "BOOL".into(), "STRING".into()],
            },
        }
    }
}

#[connector_trait]
impl Factory for BigQueryConnector {
    type Connector = BigQueryConnector;

    async fn connect(p: ConnectParams) -> Result<BigQueryConnector> {
        let project_id = p.project_id.trim().to_owned();
        if project_id.is_empty() {
            return Err("Identifiant du projet GCP manquant".into());
        }
        let client = match p.credentials_path.trim() {
            "" => Client::from_application_default_credentials().await,
            path => Client::from_service_account_key_file(path).await,
        }
        .map_err(err)?;
        let connector = BigQueryConnector { client, project_id, max_rows: p.max_rows };
        // Vérifie tout de suite les droits, plutôt qu'à la première requête.
        connector.client.dataset().list(&connector.project_id, Default::default()).await.map_err(err)?;
        Ok(connector)
    }
}

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

impl BigQueryConnector {
    fn query_request(sql: &str) -> QueryRequest {
        let mut request = QueryRequest::new(sql);
        request.use_legacy_sql = false;
        request
    }
}

fn column_names(schema: Option<&TableSchema>) -> Vec<String> {
    schema
        .and_then(|s| s.fields.as_ref())
        .map(|fields| fields.iter().map(|f| f.name.clone()).collect())
        .unwrap_or_default()
}

/// BigQuery renvoie toutes les valeurs scalaires en texte JSON.
fn decode_row(row: TableRow) -> Vec<Cell> {
    row.columns
        .unwrap_or_default()
        .into_iter()
        .map(|cell| match cell.value {
            None | Some(Value::Null) => Cell::Null,
            Some(Value::String(s)) => Cell::Text(s),
            Some(Value::Bool(b)) => Cell::Bool(b),
            Some(other) => Cell::Text(other.to_string()),
        })
        .collect()
}

fn human_bytes(bytes: f64) -> String {
    const UNITS: [&str; 5] = ["o", "Ko", "Mo", "Go", "To"];
    let mut value = bytes;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

#[connector_trait]
impl Connector for BigQueryConnector {
    async fn list_tables(&self) -> Result<Vec<TableRef>> {
        let datasets = self
            .client
            .dataset()
            .list(&self.project_id, Default::default())
            .await
            .map_err(err)?;

        let mut tables = Vec::new();
        for dataset in datasets.datasets {
            let dataset_id = dataset.dataset_reference.dataset_id;
            let list = self
                .client
                .table()
                .list(&self.project_id, &dataset_id, Default::default())
                .await
                .map_err(err)?;
            tables.extend(list.tables.unwrap_or_default().into_iter().map(|t| TableRef {
                schema: Some(dataset_id.clone()),
                name: t.table_reference.table_id,
            }));
        }
        Ok(tables)
    }

    async fn list_schemas(&self) -> Result<Vec<String>> {
        let datasets = self
            .client
            .dataset()
            .list(&self.project_id, Default::default())
            .await
            .map_err(err)?;
        Ok(datasets.datasets.into_iter().map(|d| d.dataset_reference.dataset_id).collect())
    }

    async fn describe_table(&self, table: &TableRef) -> Result<Vec<ColumnInfo>> {
        let dataset = table.schema.as_deref().ok_or("Dataset manquant")?;
        let info = self
            .client
            .table()
            .get(&self.project_id, dataset, &table.name, None)
            .await
            .map_err(err)?;

        Ok(info
            .schema
            .fields
            .unwrap_or_default()
            .into_iter()
            .map(|field| {
                let mode = field.mode.unwrap_or_default();
                let mut data_type = format!("{:?}", field.r#type).to_uppercase();
                if mode == "REPEATED" {
                    data_type = format!("ARRAY<{data_type}>");
                }
                ColumnInfo {
                    name: field.name,
                    data_type,
                    nullable: mode != "REQUIRED",
                    default: None,
                    primary_key: false,
                }
            })
            .collect())
    }

    /// Contraintes déclarées (BigQuery ne les applique pas, mais on peut les
    /// déclarer). Souvent vide : on ne devine rien d'après les noms.
    async fn list_relations(&self) -> Result<Vec<Relation>> {
        let datasets = self
            .client
            .dataset()
            .list(&self.project_id, Default::default())
            .await
            .map_err(err)?;

        let mut relations = Vec::new();
        for dataset in datasets.datasets {
            let ds = dataset.dataset_reference.dataset_id;
            let prefix = format!("`{}.{ds}.INFORMATION_SCHEMA", self.project_id);
            let sql = format!(
                "SELECT k.constraint_name, k.table_name, k.column_name,
                        c.table_schema, c.table_name, c.column_name
                 FROM {prefix}.KEY_COLUMN_USAGE` k
                 JOIN {prefix}.TABLE_CONSTRAINTS` t
                   ON t.constraint_name = k.constraint_name AND t.table_name = k.table_name
                 JOIN {prefix}.CONSTRAINT_COLUMN_USAGE` c
                   ON c.constraint_name = k.constraint_name
                 WHERE t.constraint_type = 'FOREIGN KEY'
                 ORDER BY k.constraint_name, k.ordinal_position"
            );
            let result = self.query(&sql).await?;

            let mut current: Option<(String, Relation)> = None;
            for row in result.rows {
                let cell = |i: usize| row.get(i).map(Cell::display).unwrap_or_default();
                let constraint = cell(0);
                match &mut current {
                    Some((name, rel)) if *name == constraint => {
                        rel.from_columns.push(cell(2));
                        rel.to_columns.push(cell(5));
                    }
                    _ => {
                        if let Some((_, rel)) = current.take() {
                            relations.push(rel);
                        }
                        current = Some((
                            constraint,
                            Relation {
                                from: TableRef { schema: Some(ds.clone()), name: cell(1) },
                                from_columns: vec![cell(2)],
                                to: TableRef { schema: Some(cell(3)), name: cell(4) },
                                to_columns: vec![cell(5)],
                            },
                        ));
                    }
                }
            }
            if let Some((_, rel)) = current {
                relations.push(rel);
            }
        }
        Ok(relations)
    }

    /// BigQuery n'expose son plan détaillé qu'après exécution ; avant, un
    /// « dry run » (gratuit) donne le volume analysé, donc le prix.
    async fn explain(&self, sql: &str) -> Result<QueryPlan> {
        let mut request = Self::query_request(sql);
        request.dry_run = Some(true);
        let response = self.client.job().query(&self.project_id, request).await.map_err(err)?;

        let bytes: f64 = response
            .total_bytes_processed
            .as_deref()
            .and_then(|b| b.parse().ok())
            .unwrap_or(0.0);
        let dollars = bytes / 1024f64.powi(4) * USD_PER_TIB;

        Ok(QueryPlan {
            nodes: vec![PlanNode {
                depth: 0,
                label: format!("Analyse de {} de données", human_bytes(bytes)),
                cost: Some(dollars),
                rows: None,
            }],
            total_cost: Some(dollars),
            cost_unit: "$".into(),
            note: Some(format!(
                "Estimation au tarif à la demande ({USD_PER_TIB} $/Tio), hors cache et \
                 niveau gratuit. Le plan détaillé n'est disponible qu'après exécution."
            )),
        })
    }

    async fn query(&self, sql: &str) -> Result<QueryResult> {
        let mut request = Self::query_request(sql);
        request.max_results = Some(self.max_rows as i32);
        request.timeout_ms = Some(QUERY_TIMEOUT_MS);
        let response = self.client.job().query(&self.project_id, request).await.map_err(err)?;

        let (schema, rows) = if response.job_complete == Some(true) {
            (response.schema, response.rows)
        } else {
            // Requête longue : on attend la fin du job.
            let job = response.job_reference.ok_or("Job BigQuery sans référence")?;
            let job_id = job.job_id.ok_or("Job BigQuery sans identifiant")?;
            loop {
                let results = self
                    .client
                    .job()
                    .get_query_results(
                        &self.project_id,
                        &job_id,
                        GetQueryResultsParameters {
                            location: job.location.clone(),
                            max_results: Some(self.max_rows as i32),
                            timeout_ms: Some(QUERY_TIMEOUT_MS),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(err)?;
                if results.job_complete == Some(true) {
                    break (results.schema, results.rows);
                }
            }
        };

        Ok(QueryResult {
            columns: column_names(schema.as_ref()),
            rows: rows.unwrap_or_default().into_iter().map(decode_row).collect(),
        })
    }

    async fn execute(&self, sql: &str) -> Result<u64> {
        let mut request = Self::query_request(sql);
        request.timeout_ms = Some(QUERY_TIMEOUT_MS);
        let response = self.client.job().query(&self.project_id, request).await.map_err(err)?;
        Ok(response.num_dml_affected_rows.as_deref().and_then(|n| n.parse().ok()).unwrap_or(0))
    }
}

#[tokio::main]
async fn main() {
    serve::<BigQueryConnector>().await;
}
