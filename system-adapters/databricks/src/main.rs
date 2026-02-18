/*
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use std::{collections::HashMap, time::Duration};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use clap::{Parser, Subcommand};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};
use system_adapter_protocol::{
    AdbcDriver, DatasetConfig, EtlType, Handler, QueryMethodResponse, Server, SetupResponse,
    TeardownResponse,
};
use uuid::Uuid;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the system adapter over stdio JSON-RPC.
    Stdio(StdioArgs),
}

#[derive(Parser, Debug, Clone)]
struct StdioArgs {
    /// Databricks endpoint host, e.g. dbc-xxxx.cloud.databricks.com
    #[arg(long, env = "DATABRICKS_ENDPOINT")]
    databricks_endpoint: String,

    /// Databricks personal access token
    #[arg(long, env = "DATABRICKS_TOKEN")]
    databricks_token: String,

    /// SQL Warehouse HTTP path (used for ADBC URI), e.g. sql/protocolv1/o/123/0123-456789-abcdef
    #[arg(long, env = "DATABRICKS_HTTP_PATH")]
    databricks_http_path: String,

    /// SQL Warehouse ID for statement execution API
    #[arg(long, env = "DATABRICKS_SQL_WAREHOUSE_ID")]
    databricks_sql_warehouse_id: Option<String>,

    /// Databricks catalog for created external tables
    #[arg(long, env = "DATABRICKS_CATALOG", default_value = "spiceai_sandbox")]
    databricks_catalog: String,

    /// Databricks schema for created external tables
    #[arg(long, env = "DATABRICKS_SCHEMA", default_value = "tpch")]
    databricks_schema: String,

    /// Storage credential name for accessing external S3 locations.
    /// If set, CREATE TABLE statements will include WITH (CREDENTIAL <name>).
    #[arg(long, env = "DATABRICKS_STORAGE_CREDENTIAL")]
    databricks_storage_credential: Option<String>,

    /// Drop created tables during teardown
    #[arg(long, env = "DATABRICKS_DROP_TABLES_ON_TEARDOWN", default_value_t = false)]
    drop_tables_on_teardown: bool,
}

#[derive(Debug, Clone)]
struct RunState {
    created_tables: Vec<String>,
}

struct DatabricksAdapter {
    config: AdapterConfig,
    runs: HashMap<Uuid, RunState>,
    client: reqwest::Client,
}

#[derive(Debug, Clone)]
struct AdapterConfig {
    endpoint: String,
    token: String,
    http_path: String,
    warehouse_id: String,
    catalog: String,
    schema: String,
    storage_credential: Option<String>,
    drop_tables_on_teardown: bool,
}

impl AdapterConfig {
    fn from_args(args: StdioArgs) -> Result<Self> {
        if args.databricks_endpoint.starts_with("http://")
            || args.databricks_endpoint.starts_with("https://")
        {
            return Err(anyhow!(
                "Invalid Databricks endpoint '{}': use hostname only (no scheme)",
                args.databricks_endpoint
            ));
        }

        if args.databricks_endpoint.contains('/') {
            return Err(anyhow!(
                "Invalid Databricks endpoint '{}': do not include path segments",
                args.databricks_endpoint
            ));
        }

        if args.databricks_http_path.starts_with('/') {
            return Err(anyhow!(
                "Invalid Databricks HTTP path '{}': do not start with '/'",
                args.databricks_http_path
            ));
        }

        if args.databricks_http_path.trim().is_empty() {
            return Err(anyhow!("Databricks HTTP path must not be empty"));
        }

        let warehouse_id = args.databricks_sql_warehouse_id.unwrap_or_else(|| {
            args.databricks_http_path
                .rsplit('/')
                .find(|s| !s.is_empty())
                .unwrap_or_default()
                .to_string()
        });

        if warehouse_id.is_empty() {
            return Err(anyhow!(
                "Missing Databricks warehouse ID. Set --databricks-sql-warehouse-id or provide it in --databricks-http-path"
            ));
        }

        Ok(Self {
            endpoint: args.databricks_endpoint,
            token: args.databricks_token,
            http_path: args.databricks_http_path,
            warehouse_id,
            catalog: args.databricks_catalog,
            schema: args.databricks_schema,
            storage_credential: args.databricks_storage_credential,
            drop_tables_on_teardown: args.drop_tables_on_teardown,
        })
    }
}

impl DatabricksAdapter {
    fn try_new(config: AdapterConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()?;

        Ok(Self {
            config,
            runs: HashMap::new(),
            client,
        })
    }

    fn databricks_uri(&self) -> String {
        format!(
            "databricks://token:{}@{}:443/{}",
            self.config.token, self.config.endpoint, self.config.http_path
        )
    }

    async fn execute_sql(&self, sql: &str) -> Result<()> {
        let execute_url = format!("https://{}/api/2.0/sql/statements/", self.config.endpoint);
        let payload = json!({
            "warehouse_id": self.config.warehouse_id,
            "catalog": self.config.catalog,
            "schema": self.config.schema,
            "statement": sql,
            "wait_timeout": "20s",
        });

        let response = self
            .client
            .post(execute_url)
            .bearer_auth(&self.config.token)
            .json(&payload)
            .send()
            .await?;

        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Databricks SQL execute failed ({status}): {body}"));
        }

        let body: StatementResponse = response.json().await?;

        match body.status.state {
            StatementState::Succeeded => Ok(()),
            StatementState::Failed => Err(anyhow!(
                "Databricks statement failed: {}",
                body.status.error_message()
            )),
            StatementState::Canceled => Err(anyhow!("Databricks statement canceled")),
            StatementState::Pending | StatementState::Running => {
                self.wait_for_statement_completion(&body.statement_id).await
            }
        }
    }

    async fn wait_for_statement_completion(&self, statement_id: &str) -> Result<()> {
        let status_url = format!(
            "https://{}/api/2.0/sql/statements/{statement_id}",
            self.config.endpoint
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(180);
        loop {
            if std::time::Instant::now() > deadline {
                return Err(anyhow!(
                    "Timed out waiting for Databricks statement {statement_id}"
                ));
            }

            let response = self
                .client
                .get(&status_url)
                .bearer_auth(&self.config.token)
                .send()
                .await?;

            if response.status() != StatusCode::OK {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow!(
                    "Databricks statement status check failed ({status}): {body}"
                ));
            }

            let body: StatementResponse = response.json().await?;
            match body.status.state {
                StatementState::Succeeded => return Ok(()),
                StatementState::Failed => {
                    return Err(anyhow!(
                        "Databricks statement failed: {}",
                        body.status.error_message()
                    ));
                }
                StatementState::Canceled => {
                    return Err(anyhow!("Databricks statement canceled"));
                }
                StatementState::Pending | StatementState::Running => {
                    tokio::time::sleep(Duration::from_millis(750)).await;
                }
            }
        }
    }

    fn dataset_location(config: &DatasetConfig) -> Result<String> {
        if let Some(from) = config.params.get("from").and_then(Value::as_str)
            && !from.is_empty()
        {
            return Ok(from.to_string());
        }

        if config.etl_type == EtlType::S3 {
            let bucket = config
                .params
                .get("bucket")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("Missing params.bucket for S3 dataset"))?;

            let prefix = config
                .params
                .get("prefix")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim_start_matches('/');

            if prefix.is_empty() {
                return Ok(format!("s3://{bucket}/"));
            }

            return Ok(format!("s3://{bucket}/{prefix}"));
        }

        Err(anyhow!("Unsupported dataset configuration: missing location"))
    }

    fn quoted_identifier(identifier: &str) -> String {
        format!("`{}`", identifier.replace('`', "``"))
    }

    fn sql_string_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }
}

#[derive(Debug, Deserialize)]
struct StatementResponse {
    statement_id: String,
    status: StatementStatus,
}

#[derive(Debug, Deserialize)]
struct StatementStatus {
    state: StatementState,
    #[serde(default)]
    error: Option<StatementError>,
}

impl StatementStatus {
    fn error_message(&self) -> String {
        self.error
            .as_ref()
            .and_then(|e| e.message.clone())
            .unwrap_or_else(|| "unknown error".to_string())
    }
}

#[derive(Debug, Deserialize)]
struct StatementError {
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum StatementState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Canceled,
}

#[async_trait]
impl Handler for DatabricksAdapter {
    async fn setup(
        &mut self,
        run_id: Uuid,
        datasets: HashMap<String, DatasetConfig>,
    ) -> std::result::Result<SetupResponse, String> {
        eprintln!(
            "[databricks-adapter] setup: run_id={run_id}, datasets={}",
            datasets.len()
        );

        let schema_sql = format!(
            "CREATE SCHEMA IF NOT EXISTS {}.{}",
            Self::quoted_identifier(&self.config.catalog),
            Self::quoted_identifier(&self.config.schema),
        );
        self.execute_sql(&schema_sql)
            .await
            .map_err(|e| format!("Failed to ensure schema exists: {e}"))?;

        let mut created_tables = Vec::with_capacity(datasets.len());

        for (dataset_name, dataset_cfg) in datasets {
            let location = Self::dataset_location(&dataset_cfg)
                .map_err(|e| format!("Invalid dataset '{dataset_name}' config: {e}"))?;

            let table_name = dataset_name;
            let fqn = format!(
                "{}.{}.{}",
                Self::quoted_identifier(&self.config.catalog),
                Self::quoted_identifier(&self.config.schema),
                Self::quoted_identifier(&table_name),
            );

            let drop_sql = format!("DROP TABLE IF EXISTS {fqn}");
            self.execute_sql(&drop_sql)
                .await
                .map_err(|e| format!("Failed to drop existing table '{table_name}': {e}"))?;

            let mut create_sql = format!(
                "CREATE TABLE {fqn} USING PARQUET LOCATION {}",
                Self::sql_string_literal(&location)
            );

            if let Some(credential) = &self.config.storage_credential {
                create_sql.push_str(&format!(
                    " WITH (CREDENTIAL {})",
                    Self::quoted_identifier(credential)
                ));
            }

            self.execute_sql(&create_sql)
                .await
                .map_err(|e| format!("Failed to create table '{table_name}': {e}"))?;

            created_tables.push(table_name);
        }

        self.runs.insert(run_id, RunState { created_tables });
        Ok(SetupResponse { ok: true })
    }

    async fn query_method(
        &mut self,
        run_id: Uuid,
    ) -> std::result::Result<QueryMethodResponse, String> {
        if !self.runs.contains_key(&run_id) {
            return Err(format!("Unknown run_id: {run_id}"));
        }

        Ok(QueryMethodResponse {
            driver: AdbcDriver::Databricks,
            db_kwargs: HashMap::from([(
                "uri".to_string(),
                Value::String(self.databricks_uri()),
            )]),
        })
    }

    async fn teardown(
        &mut self,
        run_id: Uuid,
    ) -> std::result::Result<TeardownResponse, String> {
        eprintln!("[databricks-adapter] teardown: run_id={run_id}");

        let Some(state) = self.runs.remove(&run_id) else {
            return Ok(TeardownResponse { ok: true });
        };

        if self.config.drop_tables_on_teardown {
            for table_name in state.created_tables {
                let sql = format!(
                    "DROP TABLE IF EXISTS {}.{}.{}",
                    Self::quoted_identifier(&self.config.catalog),
                    Self::quoted_identifier(&self.config.schema),
                    Self::quoted_identifier(&table_name),
                );

                self.execute_sql(&sql).await.map_err(|e| {
                    format!("Failed to drop table '{table_name}' during teardown: {e}")
                })?;
            }
        }

        Ok(TeardownResponse { ok: true })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Stdio(args) => {
            let config = AdapterConfig::from_args(args)?;
            let handler = DatabricksAdapter::try_new(config)?;
            let mut server = Server::new(handler);
            server
                .run_stdio()
                .await
                .map_err(|e| anyhow!("Databricks adapter stdio server failed: {e}"))?;
        }
    }

    Ok(())
}