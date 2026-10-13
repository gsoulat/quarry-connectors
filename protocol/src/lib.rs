//! Protocole entre Quarry et un connecteur.
//!
//! Un connecteur est un exécutable lancé par Quarry. Les deux dialoguent en
//! **JSON, une ligne par message** (NDJSON) : Quarry écrit des [`Request`]
//! sur l'entrée standard du connecteur, qui répond par des [`Response`] sur
//! sa sortie standard, dans l'ordre, avec le même `id`.
//!
//! La version du protocole est négociée par `hello` : un connecteur dont le
//! `protocol` ne correspond pas est refusé.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Version du protocole. À incrémenter à chaque changement incompatible.
pub const PROTOCOL_VERSION: u32 = 1;

/// Une table (ou vue), éventuellement qualifiée par son schéma.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableRef {
    pub schema: Option<String>,
    pub name: String,
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.schema {
            Some(schema) => write!(f, "{schema}.{}", self.name),
            None => f.write_str(&self.name),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default: Option<String>,
    pub primary_key: bool,
}

/// Valeur d'une cellule, indépendante du moteur.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "t", content = "v")]
pub enum Cell {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    /// Type que l'on ne sait pas décoder : on garde son nom.
    Unsupported(String),
}

impl Cell {
    /// Texte affiché dans une grille (tronqué pour rester lisible).
    pub fn display(&self) -> String {
        const MAX_CHARS: usize = 200;
        match self {
            Cell::Null => "NULL".into(),
            Cell::Bool(b) => b.to_string(),
            Cell::Int(i) => i.to_string(),
            Cell::Float(f) => f.to_string(),
            Cell::Text(s) if s.chars().count() > MAX_CHARS => {
                let truncated: String = s.chars().take(MAX_CHARS).collect();
                format!("{truncated}…")
            }
            Cell::Text(s) => s.clone(),
            Cell::Unsupported(type_name) => format!("<{type_name}>"),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Cell>>,
}

/// Une clé étrangère : `from.from_columns` référence `to.to_columns`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relation {
    pub from: TableRef,
    pub from_columns: Vec<String>,
    pub to: TableRef,
    pub to_columns: Vec<String>,
}

/// Une étape du plan, en pré-ordre (parent avant enfants), `depth` donnant
/// l'imbrication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanNode {
    pub depth: usize,
    pub label: String,
    pub cost: Option<f64>,
    pub rows: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryPlan {
    pub nodes: Vec<PlanNode>,
    pub total_cost: Option<f64>,
    /// Unité du coût, propre au moteur.
    pub cost_unit: String,
    pub note: Option<String>,
}

/// Ce que Quarry doit savoir du dialecte pour générer du SQL lui-même
/// (insertion, modification, création de tables…).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dialect {
    /// Caractère qui protège un identifiant : `"` ou `` ` ``.
    pub identifier_quote: char,
    /// Les littéraux interprètent l'antislash (MySQL).
    pub backslash_escapes: bool,
    /// Le moteur a des schémas (ou datasets) nommés.
    pub has_schemas: bool,
    /// `RENAME TABLE a TO b` plutôt que `ALTER TABLE a RENAME TO b`.
    pub rename_table_statement: bool,
    /// `CREATE DATABASE` tient lieu de `CREATE SCHEMA` (MySQL).
    pub schema_is_database: bool,
    /// Types proposés dans les menus de l'interface.
    pub types: Vec<String>,
    /// Type d'identifiant auto-incrémenté pour une nouvelle table.
    pub id_type: String,
    /// Types déduits à l'import : entier, flottant, booléen, texte.
    pub import_types: [String; 4],
}

/// Réponse à `hello`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub protocol: u32,
    /// Identifiant du connecteur (« postgres », « mysql »…).
    pub id: String,
    /// Nom lisible (« PostgreSQL »).
    pub engine: String,
    pub version: String,
    pub dialect: Dialect,
}

/// Paramètres de connexion, tels que saisis dans Quarry. Les champs
/// inutiles pour un moteur sont vides.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConnectParams {
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub database: String,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub project_id: String,
    #[serde(default)]
    pub credentials_path: String,
    /// Nombre maximal de lignes renvoyées par `query`.
    #[serde(default = "default_max_rows")]
    pub max_rows: usize,
}

fn default_max_rows() -> usize {
    5_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method", content = "params", rename_all = "snake_case")]
pub enum Request {
    Hello,
    Connect(ConnectParams),
    ListSchemas,
    ListTables,
    DescribeTable(TableRef),
    ListRelations,
    Explain { sql: String },
    Query { sql: String },
    Execute { sql: String },
    /// Fin de session : le connecteur se termine.
    Close,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum Payload {
    Hello(Hello),
    Ok,
    Schemas(Vec<String>),
    Tables(Vec<TableRef>),
    Columns(Vec<ColumnInfo>),
    Relations(Vec<Relation>),
    Plan(QueryPlan),
    Result(QueryResult),
    /// Lignes touchées par `execute`.
    Affected(u64),
}

/// Enveloppe d'une requête : un identifiant à renvoyer dans la réponse.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub id: u64,
    #[serde(flatten)]
    pub request: Request,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Payload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_round_trips_as_one_json_line() {
        let env = Envelope { id: 3, request: Request::Query { sql: "SELECT 1".into() } };
        let line = serde_json::to_string(&env).unwrap();
        assert!(!line.contains('\n'));
        assert_eq!(line, r#"{"id":3,"method":"query","params":{"sql":"SELECT 1"}}"#);
        let back: Envelope = serde_json::from_str(&line).unwrap();
        assert!(matches!(back.request, Request::Query { .. }));
    }

    #[test]
    fn cells_keep_their_type_through_json() {
        let cells = vec![Cell::Null, Cell::Int(1), Cell::Float(1.5), Cell::Text("a".into())];
        let json = serde_json::to_string(&cells).unwrap();
        let back: Vec<Cell> = serde_json::from_str(&json).unwrap();
        assert!(matches!(back[1], Cell::Int(1)));
        assert!(matches!(back[2], Cell::Float(f) if (f - 1.5).abs() < 1e-9));
    }
}
