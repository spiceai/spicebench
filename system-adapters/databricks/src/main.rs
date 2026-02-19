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
use arrow_schema::DataType;
use async_trait::async_trait;
use clap::{Parser, Subcommand, ValueEnum};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use system_adapter_protocol::{
    AdbcDriver, DatasetConfig, Handler, Server, SetupResponse, TeardownResponse,
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

    /// Databricks adapter variant.
    #[arg(
        long,
        env = "DATABRICKS_VARIANT",
        value_enum,
        default_value = "databricks"
    )]
    databricks_variant: DatabricksVariant,

    /// Databricks compute mode for setup/teardown SQL operations.
    #[arg(
        long,
        env = "DATABRICKS_COMPUTE_MODE",
        value_enum,
        default_value = "sql-warehouse"
    )]
    databricks_compute_mode: ComputeMode,

    /// SQL Warehouse ID for statement execution API
    #[arg(long, env = "DATABRICKS_SQL_WAREHOUSE_ID")]
    databricks_sql_warehouse_id: Option<String>,

    /// Existing Databricks cluster ID to use in spark-cluster mode.
    #[arg(long, env = "DATABRICKS_CLUSTER_ID")]
    databricks_cluster_id: Option<String>,

    /// Databricks cluster name to discover or create in spark-cluster mode.
    #[arg(long, env = "DATABRICKS_CLUSTER_NAME", default_value = "spicebench")]
    databricks_cluster_name: String,

    /// Databricks runtime version for newly created clusters in spark-cluster mode.
    #[arg(
        long,
        env = "DATABRICKS_CLUSTER_SPARK_VERSION",
        default_value = "15.4.x-scala2.12"
    )]
    databricks_cluster_spark_version: String,

    /// Databricks node type for newly created clusters in spark-cluster mode.
    #[arg(
        long,
        env = "DATABRICKS_CLUSTER_NODE_TYPE_ID",
        default_value = "i3.xlarge"
    )]
    databricks_cluster_node_type_id: String,

    /// Number of workers for newly created clusters in spark-cluster mode.
    #[arg(long, env = "DATABRICKS_CLUSTER_NUM_WORKERS", default_value_t = 1)]
    databricks_cluster_num_workers: i32,

    /// Auto termination minutes for newly created clusters in spark-cluster mode.
    #[arg(
        long,
        env = "DATABRICKS_CLUSTER_AUTOTERMINATION_MINUTES",
        default_value_t = 30
    )]
    databricks_cluster_autotermination_minutes: i32,

    /// Databricks catalog for created external tables
    #[arg(long, env = "DATABRICKS_CATALOG", default_value = "spiceai_sandbox")]
    databricks_catalog: String,

    /// Databricks schema for created external tables
    #[arg(long, env = "DATABRICKS_SCHEMA", default_value = "tpch")]
    databricks_schema: String,

    /// Drop created tables during teardown
    #[arg(
        long,
        env = "DATABRICKS_DROP_TABLES_ON_TEARDOWN",
        default_value_t = false
    )]
    drop_tables_on_teardown: bool,

    /// Table format to use when creating Lakebase tables.
    #[arg(
        long,
        env = "DATABRICKS_TABLE_FORMAT",
        value_enum,
        default_value = "parquet"
    )]
    databricks_table_format: TableFormat,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
#[value(rename_all = "kebab-case")]
enum DatabricksVariant {
    Databricks,
    Lakebase,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum ComputeMode {
    SqlWarehouse,
    SparkCluster,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum TableFormat {
    Parquet,
    Delta,
    Iceberg,
}

impl TableFormat {
    fn as_sql_using(self) -> &'static str {
        match self {
            Self::Parquet => "PARQUET",
            Self::Delta => "DELTA",
            Self::Iceberg => "ICEBERG",
        }
    }

    fn from_metadata_value(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "parquet" => Some(Self::Parquet),
            "delta" => Some(Self::Delta),
            "iceberg" => Some(Self::Iceberg),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct RunState {
    created_tables: Vec<String>,
    cluster_id: Option<String>,
    cluster_created_by_adapter: bool,
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
    variant: DatabricksVariant,
    table_format: TableFormat,
    warehouse_id: String,
    compute_target: ComputeTarget,
    catalog: String,
    schema: String,
    drop_tables_on_teardown: bool,
}

#[derive(Debug, Clone)]
enum ComputeTarget {
    SqlWarehouse,
    SparkCluster(ClusterConfig),
}

#[derive(Debug, Clone)]
struct ClusterConfig {
    cluster_id: Option<String>,
    cluster_name: String,
    spark_version: String,
    node_type_id: String,
    num_workers: i32,
    autotermination_minutes: i32,
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

        let compute_target = match args.databricks_compute_mode {
            ComputeMode::SqlWarehouse => {
                if warehouse_id.is_empty() {
                    return Err(anyhow!(
                        "Missing Databricks warehouse ID. Set --databricks-sql-warehouse-id or provide it in --databricks-http-path"
                    ));
                }

                ComputeTarget::SqlWarehouse
            }
            ComputeMode::SparkCluster => {
                if args.databricks_cluster_name.trim().is_empty()
                    && args
                        .databricks_cluster_id
                        .as_deref()
                        .unwrap_or_default()
                        .is_empty()
                {
                    return Err(anyhow!(
                        "Missing Databricks cluster configuration. Set --databricks-cluster-id or --databricks-cluster-name"
                    ));
                }

                if args.databricks_cluster_num_workers < 0 {
                    return Err(anyhow!(
                        "Invalid Databricks cluster num workers '{}': must be >= 0",
                        args.databricks_cluster_num_workers
                    ));
                }

                if args.databricks_cluster_autotermination_minutes < 0 {
                    return Err(anyhow!(
                        "Invalid Databricks cluster autotermination minutes '{}': must be >= 0",
                        args.databricks_cluster_autotermination_minutes
                    ));
                }

                ComputeTarget::SparkCluster(ClusterConfig {
                    cluster_id: args
                        .databricks_cluster_id
                        .as_ref()
                        .map(|value| value.trim().to_string())
                        .filter(|value| !value.is_empty()),
                    cluster_name: args.databricks_cluster_name,
                    spark_version: args.databricks_cluster_spark_version,
                    node_type_id: args.databricks_cluster_node_type_id,
                    num_workers: args.databricks_cluster_num_workers,
                    autotermination_minutes: args.databricks_cluster_autotermination_minutes,
                })
            }
        };

        if args.databricks_variant == DatabricksVariant::Lakebase
            && !matches!(args.databricks_compute_mode, ComputeMode::SqlWarehouse)
        {
            return Err(anyhow!(
                "Lakebase variant requires --databricks-compute-mode=sql-warehouse"
            ));
        }

        Ok(Self {
            endpoint: args.databricks_endpoint,
            token: args.databricks_token,
            http_path: args.databricks_http_path,
            variant: args.databricks_variant,
            table_format: args.databricks_table_format,
            warehouse_id,
            compute_target,
            catalog: args.databricks_catalog,
            schema: args.databricks_schema,
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

    fn uc_schema_full_name(&self) -> String {
        format!("{}.{}", self.config.catalog, self.config.schema)
    }

    fn uc_table_full_name(&self, table_name: &str) -> String {
        format!(
            "{}.{}.{}",
            self.config.catalog, self.config.schema, table_name
        )
    }

    fn quoted_identifier(identifier: &str) -> String {
        format!("`{}`", identifier.replace('`', "``"))
    }

    fn lakebase_table_full_name(&self, table_name: &str) -> String {
        format!(
            "{}.{}.{}",
            Self::quoted_identifier(&self.config.catalog),
            Self::quoted_identifier(&self.config.schema),
            Self::quoted_identifier(table_name)
        )
    }

    fn sql_type_for_arrow(data_type: &DataType) -> Result<String> {
        match data_type {
            DataType::Boolean => Ok("BOOLEAN".to_string()),
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::UInt8
            | DataType::UInt16 => Ok("INT".to_string()),
            DataType::Int64 | DataType::UInt32 | DataType::UInt64 => Ok("BIGINT".to_string()),
            DataType::Float16 | DataType::Float32 => Ok("FLOAT".to_string()),
            DataType::Float64 => Ok("DOUBLE".to_string()),
            DataType::Utf8 | DataType::LargeUtf8 => Ok("STRING".to_string()),
            DataType::Date32 => Ok("DATE".to_string()),
            DataType::Timestamp(_, _) => Ok("TIMESTAMP".to_string()),
            DataType::Decimal128(precision, scale) => {
                let precision = (*precision).min(38);
                Ok(format!("DECIMAL({precision}, {scale})"))
            }
            other => Err(anyhow!(
                "Unsupported Arrow data type for Lakebase table creation: {other:?}"
            )),
        }
    }

    fn lakebase_table_ddl(
        &self,
        table_name: &str,
        dataset_cfg: &DatasetConfig,
        table_format: TableFormat,
    ) -> Result<String> {
        let columns = dataset_cfg
            .schema
            .fields()
            .iter()
            .map(|field| {
                let col_type = Self::sql_type_for_arrow(field.data_type())?;
                Ok::<_, anyhow::Error>(format!(
                    "{} {}",
                    Self::quoted_identifier(field.name()),
                    col_type
                ))
            })
            .collect::<Result<Vec<_>>>()?
            .join(", ");

        Ok(format!(
            "CREATE TABLE {} ({columns}) USING {}",
            self.lakebase_table_full_name(table_name),
            table_format.as_sql_using()
        ))
    }

    fn table_format_from_setup_metadata(
        &self,
        metadata: &HashMap<String, Value>,
    ) -> Result<TableFormat> {
        if let Some(value) = metadata.get("table_format")
            && let Some(s) = value.as_str()
        {
            return TableFormat::from_metadata_value(s).ok_or_else(|| {
                anyhow!(
                    "Unsupported table_format '{s}'. Allowed values: parquet, delta, iceberg"
                )
            });
        }

        Ok(self.config.table_format)
    }

    async fn ensure_uc_schema_exists(&self) -> Result<()> {
        let schema_full_name = self.uc_schema_full_name();
        let get_url = format!(
            "https://{}/api/2.1/unity-catalog/schemas/{schema_full_name}",
            self.config.endpoint
        );

        let get_response = self
            .client
            .get(get_url)
            .bearer_auth(&self.config.token)
            .send()
            .await?;

        if get_response.status() == StatusCode::OK {
            return Ok(());
        }

        if get_response.status() != StatusCode::NOT_FOUND {
            let status = get_response.status();
            let body = get_response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Databricks Unity Catalog schemas/get failed ({status}): {body}"
            ));
        }

        let create_url = format!(
            "https://{}/api/2.1/unity-catalog/schemas",
            self.config.endpoint
        );
        let create_response = self
            .client
            .post(create_url)
            .bearer_auth(&self.config.token)
            .json(&UcSchemaCreateRequest {
                catalog_name: self.config.catalog.clone(),
                name: self.config.schema.clone(),
            })
            .send()
            .await?;

        if !create_response.status().is_success() {
            let status = create_response.status();
            let body = create_response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Databricks Unity Catalog schemas/create failed ({status}): {body}"
            ));
        }

        Ok(())
    }

    async fn delete_uc_table_if_exists(&self, table_name: &str) -> Result<()> {
        let full_name = self.uc_table_full_name(table_name);
        let delete_url = format!(
            "https://{}/api/2.1/unity-catalog/tables/{full_name}",
            self.config.endpoint
        );
        let response = self
            .client
            .delete(delete_url)
            .bearer_auth(&self.config.token)
            .send()
            .await?;

        if response.status() == StatusCode::OK || response.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(anyhow!(
            "Databricks Unity Catalog tables/delete failed ({status}) for '{full_name}': {body}"
        ))
    }

    async fn execute_sql_statement(&self, statement: &str) -> Result<()> {
        let execute_url = format!("https://{}/api/2.0/sql/statements/", self.config.endpoint);
        let payload = json!({
            "warehouse_id": self.config.warehouse_id,
            "catalog": self.config.catalog,
            "schema": self.config.schema,
            "statement": statement,
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
            return Err(anyhow!(
                "Databricks SQL statement execute failed ({status}): {body}"
            ));
        }

        let body: StatementResponse = response.json().await?;
        match body.status.state {
            StatementState::Succeeded => Ok(()),
            StatementState::Failed => {
                Err(anyhow!("Databricks SQL statement failed: {}", body.status.error_message()))
            }
            StatementState::Canceled => Err(anyhow!("Databricks SQL statement canceled")),
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
                    "Timed out waiting for Databricks SQL statement {statement_id}"
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
                    "Databricks SQL statement status check failed ({status}): {body}"
                ));
            }

            let body: StatementResponse = response.json().await?;
            match body.status.state {
                StatementState::Succeeded => return Ok(()),
                StatementState::Failed => {
                    return Err(anyhow!(
                        "Databricks SQL statement failed: {}",
                        body.status.error_message()
                    ));
                }
                StatementState::Canceled => {
                    return Err(anyhow!("Databricks SQL statement canceled"));
                }
                StatementState::Pending | StatementState::Running => {
                    tokio::time::sleep(Duration::from_millis(750)).await;
                }
            }
        }
    }

    async fn ensure_cluster_ready(&self) -> Result<(String, bool)> {
        let cluster_cfg = match &self.config.compute_target {
            ComputeTarget::SparkCluster(cfg) => cfg,
            ComputeTarget::SqlWarehouse => {
                return Err(anyhow!(
                    "ensure_cluster_ready called in sql-warehouse compute mode"
                ));
            }
        };

        let mut created_by_adapter = false;
        let cluster_summary = if let Some(cluster_id) = &cluster_cfg.cluster_id {
            self.get_cluster(cluster_id).await?.ok_or_else(|| {
                anyhow!(
                    "Configured Databricks cluster id '{}' was not found",
                    cluster_id
                )
            })?
        } else if let Some(existing) = self.find_cluster_by_name(&cluster_cfg.cluster_name).await? {
            existing
        } else {
            created_by_adapter = true;
            self.create_cluster(cluster_cfg).await?
        };

        self.ensure_cluster_running(&cluster_summary.cluster_id)
            .await?;
        Ok((cluster_summary.cluster_id, created_by_adapter))
    }

    async fn find_cluster_by_name(&self, cluster_name: &str) -> Result<Option<ClusterSummary>> {
        let list_url = format!("https://{}/api/2.0/clusters/list", self.config.endpoint);
        let response = self
            .client
            .get(list_url)
            .bearer_auth(&self.config.token)
            .send()
            .await?;

        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Databricks clusters/list failed ({status}): {body}"
            ));
        }

        let body: ClusterListResponse = response.json().await?;
        Ok(body
            .clusters
            .into_iter()
            .find(|cluster| cluster.cluster_name == cluster_name))
    }

    async fn get_cluster(&self, cluster_id: &str) -> Result<Option<ClusterSummary>> {
        let get_url = format!("https://{}/api/2.0/clusters/get", self.config.endpoint);
        let response = self
            .client
            .get(get_url)
            .bearer_auth(&self.config.token)
            .query(&[("cluster_id", cluster_id)])
            .send()
            .await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Databricks clusters/get failed ({status}): {body}"));
        }

        let body: ClusterSummary = response.json().await?;
        Ok(Some(body))
    }

    async fn create_cluster(&self, cluster_cfg: &ClusterConfig) -> Result<ClusterSummary> {
        let create_url = format!("https://{}/api/2.0/clusters/create", self.config.endpoint);
        let response = self
            .client
            .post(create_url)
            .bearer_auth(&self.config.token)
            .json(&json!({
                "cluster_name": cluster_cfg.cluster_name,
                "spark_version": cluster_cfg.spark_version,
                "node_type_id": cluster_cfg.node_type_id,
                "num_workers": cluster_cfg.num_workers,
                "autotermination_minutes": cluster_cfg.autotermination_minutes
            }))
            .send()
            .await?;

        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Databricks clusters/create failed ({status}): {body}"
            ));
        }

        let body: ClusterIdResponse = response.json().await?;
        Ok(ClusterSummary {
            cluster_id: body.cluster_id,
            cluster_name: cluster_cfg.cluster_name.clone(),
            state: Some("PENDING".to_string()),
        })
    }

    async fn ensure_cluster_running(&self, cluster_id: &str) -> Result<()> {
        let deadline = std::time::Instant::now() + Duration::from_secs(900);

        loop {
            if std::time::Instant::now() > deadline {
                return Err(anyhow!(
                    "Timed out waiting for Databricks cluster {cluster_id} to become RUNNING"
                ));
            }

            let cluster = self.get_cluster(cluster_id).await?.ok_or_else(|| {
                anyhow!(
                    "Databricks cluster {cluster_id} disappeared while waiting for RUNNING state"
                )
            })?;

            match cluster.state.as_deref().unwrap_or_default() {
                "RUNNING" => return Ok(()),
                "TERMINATED" => {
                    self.start_cluster(cluster_id).await?;
                }
                "ERROR" | "TERMINATING" => {
                    return Err(anyhow!(
                        "Databricks cluster {cluster_id} is in invalid state '{}'",
                        cluster.state.unwrap_or_else(|| "unknown".to_string())
                    ));
                }
                _ => {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn start_cluster(&self, cluster_id: &str) -> Result<()> {
        let start_url = format!("https://{}/api/2.0/clusters/start", self.config.endpoint);
        let response = self
            .client
            .post(start_url)
            .bearer_auth(&self.config.token)
            .json(&json!({ "cluster_id": cluster_id }))
            .send()
            .await?;

        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Databricks clusters/start failed ({status}): {body}"
            ));
        }

        Ok(())
    }

    async fn terminate_cluster(&self, cluster_id: &str) -> Result<()> {
        let delete_url = format!("https://{}/api/2.0/clusters/delete", self.config.endpoint);
        let response = self
            .client
            .post(delete_url)
            .bearer_auth(&self.config.token)
            .json(&json!({ "cluster_id": cluster_id }))
            .send()
            .await?;

        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Databricks clusters/delete (terminate) failed ({status}): {body}"
            ));
        }

        Ok(())
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
            .and_then(|error| error.message.clone())
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

#[derive(Debug, Serialize)]
struct UcSchemaCreateRequest {
    catalog_name: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct ClusterListResponse {
    #[serde(default)]
    clusters: Vec<ClusterSummary>,
}

#[derive(Debug, Deserialize)]
struct ClusterSummary {
    cluster_id: String,
    #[serde(default)]
    cluster_name: String,
    #[serde(default)]
    state: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ClusterIdResponse {
    cluster_id: String,
}

#[async_trait]
impl Handler for DatabricksAdapter {
    async fn setup(
        &mut self,
        run_id: Uuid,
        datasets: HashMap<String, DatasetConfig>,
        metadata: HashMap<String, Value>,
    ) -> std::result::Result<SetupResponse, String> {
        eprintln!(
            "[databricks-adapter] setup: run_id={run_id}, datasets={}",
            datasets.len()
        );

        let (cluster_id, cluster_created_by_adapter) = match &self.config.compute_target {
            ComputeTarget::SparkCluster(_) => {
                let (cluster_id, created) = self
                    .ensure_cluster_ready()
                    .await
                    .map_err(|e| format!("Failed to ensure Databricks cluster is ready: {e}"))?;
                (Some(cluster_id), created)
            }
            ComputeTarget::SqlWarehouse => (None, false),
        };

        match self.config.variant {
            DatabricksVariant::Databricks => {
                self.ensure_uc_schema_exists()
                    .await
                    .map_err(|e| format!("Failed to ensure Unity Catalog schema exists: {e}"))?;
            }
            DatabricksVariant::Lakebase => {
                let schema_sql = format!(
                    "CREATE SCHEMA IF NOT EXISTS {}.{}",
                    Self::quoted_identifier(&self.config.catalog),
                    Self::quoted_identifier(&self.config.schema),
                );
                self.execute_sql_statement(&schema_sql)
                    .await
                    .map_err(|e| format!("Failed to ensure Lakebase schema exists: {e}"))?;
            }
        }

        let mut created_tables = Vec::with_capacity(datasets.len());
        let table_format = self
            .table_format_from_setup_metadata(&metadata)
            .map_err(|e| format!("Invalid setup metadata: {e}"))?;

        for (dataset_name, dataset_cfg) in datasets {
            let table_name = dataset_name;

            match self.config.variant {
                DatabricksVariant::Databricks => {
                    eprintln!(
                        "[databricks-adapter] setup: table '{table_name}' will be loaded via ADBC DDL sink"
                    );
                }
                DatabricksVariant::Lakebase => {
                    let drop_sql = format!(
                        "DROP TABLE IF EXISTS {}",
                        self.lakebase_table_full_name(&table_name)
                    );
                    self.execute_sql_statement(&drop_sql).await.map_err(|e| {
                        format!(
                            "Failed to drop existing Lakebase table '{table_name}' during setup: {e}"
                        )
                    })?;

                    let create_sql = self
                        .lakebase_table_ddl(&table_name, &dataset_cfg, table_format)
                        .map_err(|e| {
                            format!(
                                "Failed to build Lakebase table DDL for '{table_name}': {e}"
                            )
                        })?;
                    self.execute_sql_statement(&create_sql).await.map_err(|e| {
                        format!("Failed to create Lakebase table '{table_name}': {e}")
                    })?;
                }
            }

            created_tables.push(table_name);
        }

        self.runs.insert(
            run_id,
            RunState {
                created_tables,
                cluster_id,
                cluster_created_by_adapter,
            },
        );
        Ok(SetupResponse {
            driver: AdbcDriver::Databricks,
            db_kwargs: HashMap::from([
                ("uri".to_string(), Value::String(self.databricks_uri())),
                (
                    "catalog".to_string(),
                    Value::String(self.config.catalog.clone()),
                ),
                (
                    "schema".to_string(),
                    Value::String(self.config.schema.clone()),
                ),
            ]),
        })
    }

    async fn teardown(&mut self, run_id: Uuid) -> std::result::Result<TeardownResponse, String> {
        eprintln!("[databricks-adapter] teardown: run_id={run_id}");

        let Some(state) = self.runs.remove(&run_id) else {
            return Ok(TeardownResponse { ok: true });
        };

        if self.config.drop_tables_on_teardown {
            for table_name in &state.created_tables {
                match self.config.variant {
                    DatabricksVariant::Databricks => {
                        self.delete_uc_table_if_exists(table_name)
                            .await
                            .map_err(|e| {
                                format!(
                                    "Failed to drop Unity Catalog table '{table_name}' during teardown: {e}"
                                )
                            })?;
                    }
                    DatabricksVariant::Lakebase => {
                        let sql = format!(
                            "DROP TABLE IF EXISTS {}",
                            self.lakebase_table_full_name(table_name)
                        );
                        self.execute_sql_statement(&sql).await.map_err(|e| {
                            format!(
                                "Failed to drop Lakebase table '{table_name}' during teardown: {e}"
                            )
                        })?;
                    }
                }
            }
        }

        if state.cluster_created_by_adapter
            && let Some(cluster_id) = state.cluster_id.as_deref()
        {
            self.terminate_cluster(cluster_id).await.map_err(|e| {
                format!("Failed to terminate Databricks cluster '{cluster_id}': {e}")
            })?;
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
