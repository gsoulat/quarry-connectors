# Connecteurs Quarry

Les connecteurs de [Quarry](https://github.com/gsoulat/quarry), un par dossier. Quarry ne les embarque pas : il télécharge depuis les [Releases](https://github.com/gsoulat/quarry-connectors/releases) ceux dont tu as besoin.

```text
protocol/              types et messages JSON partagés (crate quarry-protocol)
sdk/                   boucle de service pour écrire un connecteur (crate quarry-connector-sdk)
connectors/
├── postgres/          PostgreSQL
├── mysql/             MySQL / MariaDB
├── bigquery/          Google BigQuery
├── duckdb/            DuckDB (embarqué)
├── clickhouse/        ClickHouse (HTTP)
├── mssql/             SQL Server / Azure SQL (TDS)
└── snowflake/         Snowflake (SQL REST API, clé RSA)
engines.json           catalogue des moteurs affichés par Quarry (alias compris : MariaDB → mysql, CockroachDB → postgres…)
scripts/make_registry.py   produit registry.json à la publication
```

## Comment ça marche

Un connecteur est un **exécutable** que Quarry lance comme processus enfant. Les deux dialoguent en **JSON, une ligne par message**, sur l'entrée et la sortie standard :

```text
→ {"id":1,"method":"hello"}
← {"id":1,"result":{"kind":"hello","data":{"protocol":1,"id":"postgres","engine":"PostgreSQL",...}}}
→ {"id":2,"method":"connect","params":{"host":"localhost","port":5432,"user":"app","password":"…","database":"app"}}
← {"id":2,"result":{"kind":"ok"}}
→ {"id":3,"method":"query","params":{"sql":"SELECT 1"}}
← {"id":3,"result":{"kind":"result","data":{"columns":["?column?"],"rows":[[{"t":"Int","v":1}]]}}}
```

Méthodes : `hello`, `connect`, `list_schemas`, `list_tables`, `describe_table`, `list_relations`, `explain`, `query`, `execute`, `close`. La réponse à `hello` décrit le **dialecte** (guillemets d'identifiants, types proposés…) : Quarry s'en sert pour générer lui-même le SQL d'édition.

Un plantage du connecteur ne touche que sa connexion, et un connecteur peut être écrit dans n'importe quel langage tant qu'il parle ce protocole.

## Écrire un connecteur

```rust
use quarry_connector_sdk::{serve, Connector, Describe, Factory, connector_trait};
// implémente Describe (hello), Factory (connect) et Connector (les requêtes)
#[tokio::main]
async fn main() { serve::<MonConnecteur>().await; }
```

Regarde `connectors/postgres/src/main.rs` : c'est le plus complet.

## Publier

```bash
git tag v0.1.0 && git push --tags
```

Le workflow compile chaque connecteur pour macOS (Apple Silicon et Intel), Linux et Windows, et publie les binaires avec un `registry.json` (URL, taille, SHA-256 par cible). Quarry lit `https://github.com/gsoulat/quarry-connectors/releases/latest/download/registry.json`.

## Publier une version

Les releases sont automatiques (semantic-release). Chaque changement passe par une pull request ; son titre devient le message du squash merge et doit suivre [Conventional Commits](https://www.conventionalcommits.org/fr/) : `feat:` → version mineure, `fix:`/`perf:`/`refactor:` → correctif, `ci:`/`docs:`/`chore:` → rien n'est publié. La CI met à jour la version du workspace dans `Cargo.toml`, `Cargo.lock` et `CHANGELOG.md`, pose le tag `v<version>`, compile les connecteurs pour les quatre cibles et attache binaires et `registry.json` à la Release. Ne pas poser de tag ni modifier la version à la main.
