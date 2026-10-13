//! Connecteur Snowflake par la SQL REST API (v2), authentifié par paire de
//! clés RSA (JWT). Le mot de passe n'est pas accepté par cette API.
//!
//! Champs attendus (déclarés dans `engines.json`) : `account`, `user`,
//! `private_key_path` (PEM PKCS#8, non chiffré), et en option `warehouse`,
//! `role`, `database`, `schema`.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use quarry_connector_sdk::protocol::*;
use quarry_connector_sdk::{Connector, Describe, Factory, Result, connector_trait, serve};
use rsa::pkcs8::{DecodePrivateKey, EncodePublicKey};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const VERSION: &str = env!("CARGO_PKG_VERSION");
/// Durée de vie d'un JWT Snowflake (maximum autorisé : 1 h).
const JWT_LIFETIME: Duration = Duration::from_secs(55 * 60);

struct Snowflake {
    client: reqwest::Client,
    base_url: String,
    /// `ACCOUNT.USER` en majuscules, pour les revendications du JWT.
    account_user: String,
    /// `SHA256:<empreinte base64 de la clé publique>`.
    fingerprint: String,
    key: jsonwebtoken::EncodingKey,
    warehouse: Option<String>,
    role: Option<String>,
    database: Option<String>,
    schema: Option<String>,
    max_rows: usize,
}

#[derive(Serialize)]
struct Claims {
    iss: String,
    sub: String,
    iat: u64,
    exp: u64,
}

#[derive(Deserialize)]
struct RowType {
    name: String,
    #[serde(rename = "type")]
    data_type: String,
}

#[derive(Deserialize)]
struct ResultSetMetaData {
    #[serde(rename = "rowType")]
    row_type: Vec<RowType>,
}

#[derive(Deserialize)]
struct StatementResponse {
    #[serde(rename = "resultSetMetaData")]
    meta: Option<ResultSetMetaData>,
    #[serde(default)]
    data: Vec<Vec<Value>>,
    #[serde(rename = "statementHandle")]
    handle: Option<String>,
    message: Option<String>,
}

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

/// Identifiant de compte pour le JWT : sans région ni cloud, en majuscules
/// (`xy12345.eu-west-1.aws` → `XY12345` ; `myorg-myaccount` reste entier).
pub fn jwt_account(account: &str) -> String {
    account.trim().split('.').next().unwrap_or_default().to_uppercase()
}

/// Empreinte Snowflake de la clé publique : `SHA256:` + base64 du SHA-256
/// du DER (SubjectPublicKeyInfo).
pub fn fingerprint(private_key_pem: &str) -> Result<String> {
    let key = rsa::RsaPrivateKey::from_pkcs8_pem(private_key_pem)
        .map_err(|e| format!("Clé privée illisible (PKCS#8 PEM non chiffré attendu) : {e}"))?;
    let der = key.to_public_key().to_public_key_der().map_err(err)?;
    let digest = Sha256::digest(der.as_bytes());
    Ok(format!("SHA256:{}", base64::engine::general_purpose::STANDARD.encode(digest)))
}

fn cell(v: Value, data_type: &str) -> Cell {
    match v {
        Value::Null => Cell::Null,
        Value::Bool(b) => Cell::Bool(b),
        Value::Number(n) => n.as_i64().map(Cell::Int).or_else(|| n.as_f64().map(Cell::Float)).unwrap_or(Cell::Null),
        // L'API renvoie tout en texte : on retype d'après le type de colonne.
        Value::String(s) => match data_type.to_lowercase().as_str() {
            "fixed" if !s.contains('.') => s.parse().map(Cell::Int).unwrap_or(Cell::Text(s)),
            "fixed" | "real" => s.parse().map(Cell::Float).unwrap_or(Cell::Text(s)),
            "boolean" => match s.as_str() {
                "true" | "1" => Cell::Bool(true),
                "false" | "0" => Cell::Bool(false),
                _ => Cell::Text(s),
            },
            _ => Cell::Text(s),
        },
        other => Cell::Text(other.to_string()),
    }
}

impl Snowflake {
    fn jwt(&self) -> Result<String> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(err)?.as_secs();
        let claims = Claims {
            iss: format!("{}.{}", self.account_user, self.fingerprint),
            sub: self.account_user.clone(),
            iat: now,
            exp: now + JWT_LIFETIME.as_secs(),
        };
        jsonwebtoken::encode(&jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256), &claims, &self.key).map_err(err)
    }

    async fn statement(&self, sql: &str) -> Result<StatementResponse> {
        let mut body = json!({ "statement": sql, "timeout": 120, "parameters": { "MULTI_STATEMENT_COUNT": "0" } });
        for (k, v) in [("warehouse", &self.warehouse), ("role", &self.role), ("database", &self.database), ("schema", &self.schema)] {
            if let Some(v) = v {
                body[k] = json!(v);
            }
        }
        let request = self
            .client
            .post(format!("{}/api/v2/statements", self.base_url))
            .bearer_auth(self.jwt()?)
            .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
            .header("Accept", "application/json")
            .json(&body);
        let response = request.send().await.map_err(|e| format!("Snowflake injoignable : {e}"))?;
        let status = response.status();
        let text = response.text().await.map_err(err)?;
        let mut parsed: StatementResponse = serde_json::from_str(&text).map_err(|_| format!("Snowflake : HTTP {status} — {}", text.chars().take(300).collect::<String>()))?;

        // 202 : exécution encore en cours, on interroge le handle.
        if status.as_u16() == 202 {
            let handle = parsed.handle.clone().ok_or("Réponse 202 sans handle")?;
            for _ in 0..120 {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let r = self
                    .client
                    .get(format!("{}/api/v2/statements/{handle}", self.base_url))
                    .bearer_auth(self.jwt()?)
                    .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
                    .send()
                    .await
                    .map_err(err)?;
                let s = r.status();
                let t = r.text().await.map_err(err)?;
                if s.as_u16() == 202 {
                    continue;
                }
                parsed = serde_json::from_str(&t).map_err(|_| format!("Snowflake : HTTP {s} — {}", t.chars().take(300).collect::<String>()))?;
                if !s.is_success() {
                    return Err(parsed.message.unwrap_or_else(|| format!("HTTP {s}")));
                }
                return Ok(parsed);
            }
            return Err("Snowflake : délai dépassé".into());
        }
        if !status.is_success() {
            return Err(parsed.message.unwrap_or_else(|| format!("HTTP {status}")));
        }
        Ok(parsed)
    }

    async fn rows(&self, sql: &str) -> Result<QueryResult> {
        let r = self.statement(sql).await?;
        let types: Vec<(String, String)> = r.meta.map(|m| m.row_type.into_iter().map(|c| (c.name, c.data_type)).collect()).unwrap_or_default();
        Ok(QueryResult {
            columns: types.iter().map(|(n, _)| n.clone()).collect(),
            rows: r
                .data
                .into_iter()
                .take(self.max_rows)
                .map(|row| row.into_iter().zip(&types).map(|(v, (_, t))| cell(v, t)).collect())
                .collect(),
        })
    }
}

impl Describe for Snowflake {
    fn hello() -> Hello {
        Hello {
            protocol: PROTOCOL_VERSION,
            id: "snowflake".into(),
            engine: "Snowflake".into(),
            version: VERSION.into(),
            dialect: Dialect {
                identifier_quote: '"',
                backslash_escapes: true,
                has_schemas: true,
                rename_table_statement: false,
                schema_is_database: false,
                types: ["NUMBER", "NUMBER(38,0)", "FLOAT", "BOOLEAN", "VARCHAR", "TEXT", "DATE", "TIME", "TIMESTAMP_NTZ", "TIMESTAMP_TZ", "VARIANT", "OBJECT", "ARRAY", "BINARY"]
                    .map(String::from)
                    .to_vec(),
                id_type: "NUMBER AUTOINCREMENT".into(),
                import_types: ["NUMBER".into(), "FLOAT".into(), "BOOLEAN".into(), "VARCHAR".into()],
                create_table_suffix: None,
                preview_sql: None,
                rename_table_sql: None,
                rename_column_sql: None,
            },
        }
    }
}

#[connector_trait]
impl Factory for Snowflake {
    type Connector = Snowflake;

    async fn connect(p: ConnectParams) -> Result<Snowflake> {
        let account = p.option("account").ok_or("Identifiant de compte Snowflake manquant")?.to_string();
        let user = p.option("user").ok_or("Utilisateur Snowflake manquant")?.to_string();
        let key_path = p.option("private_key_path").ok_or("Chemin de la clé privée manquant")?.to_string();
        let pem = tokio::fs::read_to_string(&key_path).await.map_err(|e| format!("{key_path} : {e}"))?;
        let fingerprint = fingerprint(&pem)?;
        let key = jsonwebtoken::EncodingKey::from_rsa_pem(pem.as_bytes()).map_err(|e| format!("Clé privée refusée : {e}"))?;

        let sf = Snowflake {
            client: reqwest::Client::builder().user_agent(concat!("quarry-connector-snowflake/", env!("CARGO_PKG_VERSION"))).build().map_err(err)?,
            base_url: format!("https://{}.snowflakecomputing.com", account.trim().to_lowercase()),
            account_user: format!("{}.{}", jwt_account(&account), user.trim().to_uppercase()),
            fingerprint,
            key,
            warehouse: p.option("warehouse").map(String::from),
            role: p.option("role").map(String::from),
            database: p.option("database").map(String::from),
            schema: p.option("schema").map(String::from),
            max_rows: p.max_rows,
        };
        sf.statement("SELECT 1").await?;
        Ok(sf)
    }
}

#[connector_trait]
impl Connector for Snowflake {
    async fn list_schemas(&self) -> Result<Vec<String>> {
        let r = self.rows("SELECT schema_name FROM information_schema.schemata WHERE schema_name <> 'INFORMATION_SCHEMA' ORDER BY 1").await?;
        Ok(r.rows.into_iter().map(|row| row[0].display()).collect())
    }

    async fn list_tables(&self) -> Result<Vec<TableRef>> {
        let r = self.rows("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema <> 'INFORMATION_SCHEMA' ORDER BY 1, 2").await?;
        Ok(r.rows.into_iter().map(|row| TableRef { schema: Some(row[0].display()), name: row[1].display() }).collect())
    }

    async fn describe_table(&self, table: &TableRef) -> Result<Vec<ColumnInfo>> {
        let schema = table.schema.clone().unwrap_or_else(|| "PUBLIC".into()).replace('\'', "''");
        let name = table.name.replace('\'', "''");
        let r = self
            .rows(&format!(
                "SELECT column_name, data_type, is_nullable = 'YES', column_default
                 FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{name}' ORDER BY ordinal_position"
            ))
            .await?;
        // Clés primaires (déclaratives chez Snowflake) via SHOW PRIMARY KEYS.
        let pk = self.rows(&format!("SHOW PRIMARY KEYS IN TABLE \"{}\".\"{}\"", schema.replace("''", "'"), name.replace("''", "'"))).await.ok();
        let pk_cols: Vec<String> = pk
            .map(|r| {
                let idx = r.columns.iter().position(|c| c.eq_ignore_ascii_case("column_name")).unwrap_or(4);
                r.rows.iter().filter_map(|row| row.get(idx)).map(|c| c.display()).collect()
            })
            .unwrap_or_default();
        Ok(r.rows
            .into_iter()
            .map(|row| ColumnInfo {
                primary_key: pk_cols.iter().any(|c| c == &row[0].display()),
                name: row[0].display(),
                data_type: row[1].display(),
                nullable: matches!(row[2], Cell::Bool(true)),
                default: match &row[3] {
                    Cell::Null => None,
                    other => Some(other.display()),
                },
            })
            .collect())
    }

    /// Clés étrangères déclaratives (non appliquées par Snowflake).
    async fn list_relations(&self) -> Result<Vec<Relation>> {
        let r = self.rows("SHOW IMPORTED KEYS").await.unwrap_or_default();
        let col = |name: &str| r.columns.iter().position(|c| c.eq_ignore_ascii_case(name));
        let (Some(pk_schema), Some(pk_table), Some(pk_col), Some(fk_schema), Some(fk_table), Some(fk_col), Some(fk_name)) = (
            col("pk_schema_name"), col("pk_table_name"), col("pk_column_name"),
            col("fk_schema_name"), col("fk_table_name"), col("fk_column_name"), col("fk_name"),
        ) else {
            return Ok(Vec::new());
        };
        let mut out: Vec<(String, Relation)> = Vec::new();
        for row in &r.rows {
            let name = row[fk_name].display();
            match out.last_mut() {
                Some((n, rel)) if *n == name => {
                    rel.from_columns.push(row[fk_col].display());
                    rel.to_columns.push(row[pk_col].display());
                }
                _ => out.push((
                    name,
                    Relation {
                        from: TableRef { schema: Some(row[fk_schema].display()), name: row[fk_table].display() },
                        from_columns: vec![row[fk_col].display()],
                        to: TableRef { schema: Some(row[pk_schema].display()), name: row[pk_table].display() },
                        to_columns: vec![row[pk_col].display()],
                    },
                )),
            }
        }
        Ok(out.into_iter().map(|(_, r)| r).collect())
    }

    async fn explain(&self, sql: &str) -> Result<QueryPlan> {
        let r = self.rows(&format!("EXPLAIN USING TEXT {sql}")).await?;
        let text = r.rows.iter().filter_map(|row| row.first()).map(|c| c.display()).collect::<Vec<_>>().join("\n");
        let nodes = text
            .lines()
            .filter(|l| l.contains("->") || l.contains("TableScan") || l.contains("Result"))
            .map(|l| PlanNode { depth: (l.len() - l.trim_start().len()) / 2, label: l.trim().trim_start_matches("->").trim().to_string(), cost: None, rows: None })
            .collect();
        Ok(QueryPlan { nodes, total_cost: None, cost_unit: String::new(), note: Some("Snowflake affiche le plan logique sans coût ; la facturation dépend de l'entrepôt et du temps d'exécution.".into()) })
    }

    async fn query(&self, sql: &str) -> Result<QueryResult> {
        self.rows(sql).await
    }

    async fn execute(&self, sql: &str) -> Result<u64> {
        let r = self.rows(sql).await?;
        // Les DML renvoient une ligne « number of rows inserted/updated ».
        Ok(r.rows.first().and_then(|row| row.first()).and_then(|c| match c {
            Cell::Int(i) => Some(*i as u64),
            _ => None,
        })
        .unwrap_or(0))
    }
}

#[tokio::main]
async fn main() {
    serve::<Snowflake>().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_locator_for_jwt() {
        assert_eq!(jwt_account("xy12345.eu-west-1"), "XY12345");
        assert_eq!(jwt_account("xy12345.eu-west-1.aws"), "XY12345");
        assert_eq!(jwt_account("myorg-myaccount"), "MYORG-MYACCOUNT");
        assert_eq!(jwt_account("MYORG-MYACCOUNT"), "MYORG-MYACCOUNT");
    }

    #[test]
    fn fingerprint_of_generated_key() {
        let mut rng = rsa::rand_core::OsRng;
        let key = rsa::RsaPrivateKey::new(&mut rng, 2048).unwrap();
        let pem = rsa::pkcs8::EncodePrivateKey::to_pkcs8_pem(&key, rsa::pkcs8::LineEnding::LF).unwrap();
        let fp = fingerprint(&pem).unwrap();
        assert!(fp.starts_with("SHA256:"));
        assert_eq!(fp.len(), "SHA256:".len() + 44);
        // Le JWT se signe avec la même clé.
        assert!(jsonwebtoken::EncodingKey::from_rsa_pem(pem.as_bytes()).is_ok());
    }

    /// Réel : `QUARRY_TEST_SNOWFLAKE=account:user:/chemin/cle.p8[:warehouse]`.
    #[tokio::test]
    async fn snowflake_round_trip() {
        let Ok(spec) = std::env::var("QUARRY_TEST_SNOWFLAKE") else {
            eprintln!("QUARRY_TEST_SNOWFLAKE absent : test ignoré");
            return;
        };
        let parts: Vec<&str> = spec.split(':').collect();
        let mut options = std::collections::HashMap::new();
        options.insert("account".into(), parts[0].to_string());
        options.insert("user".into(), parts[1].to_string());
        options.insert("private_key_path".into(), parts[2].to_string());
        if let Some(w) = parts.get(3) {
            options.insert("warehouse".into(), w.to_string());
        }
        let sf = Snowflake::connect(ConnectParams { options, max_rows: 100, ..Default::default() }).await.expect("connexion");
        let r = sf.query("SELECT 1 AS n, 'a' AS s, TRUE AS b, NULL AS z").await.unwrap();
        assert_eq!(r.columns, ["N", "S", "B", "Z"]);
        assert!(matches!(r.rows[0][0], Cell::Int(1)));
        assert!(matches!(r.rows[0][3], Cell::Null));
        assert!(!sf.list_schemas().await.unwrap().is_empty());
    }
}
