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

use anyhow::{Result, anyhow};
use arrow_schema::DataType;
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use clap::{Parser, Subcommand, ValueEnum};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use std::{collections::HashMap, time::Duration};
use system_adapter_protocol::{
    AdbcDriver, DatasetConfig, EtlSinkType, Handler, IngestionMetrics, MetricsResponse,
    ResourceMetrics, Server, SetupResponse, TeardownResponse,
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
        default_value_t = true
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

    /// Staging Volume Path.
    #[arg(long, env = "DATABRICKS_STAGING_VOLUME_PATH")]
    databricks_staging_volume_path: String,

    /// Lakebase PostgreSQL endpoint host (required for lakebase compute mode).
    #[arg(long, env = "LAKEBASE_PG_HOST")]
    lakebase_pg_host: Option<String>,

    /// Lakebase PostgreSQL username (required for lakebase compute mode).
    #[arg(long, env = "LAKEBASE_PG_USER")]
    lakebase_pg_user: Option<String>,

    /// Lakebase PostgreSQL database name.
    #[arg(long, env = "LAKEBASE_PG_DB_NAME", default_value = "spicebench")]
    lakebase_pg_db_name: String,

    /// Lakebase PostgreSQL schema for search_path (defaults to --databricks-schema).
    #[arg(long, env = "LAKEBASE_PG_SCHEMA")]
    lakebase_pg_schema: Option<String>,

    /// Lakebase database instance name (for Provisioned synced table creation).
    /// Mutually exclusive with --lakebase-project.
    #[arg(
        long,
        env = "LAKEBASE_DATABASE_INSTANCE",
        conflicts_with = "lakebase_project"
    )]
    lakebase_database_instance: Option<String>,

    /// Lakebase project name (for Autoscaling synced table creation).
    /// Mutually exclusive with --lakebase-database-instance.
    #[arg(
        long,
        env = "LAKEBASE_PROJECT",
        conflicts_with = "lakebase_database_instance",
        conflicts_with = "lakebase_pg_db_name"
    )]
    lakebase_project: Option<String>,

    /// Lakebase branch name (used with --lakebase-project, defaults to "production").
    #[arg(long, env = "LAKEBASE_BRANCH", default_value = "production")]
    lakebase_branch: String,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
#[value(rename_all = "kebab-case")]
enum DatabricksVariant {
    Databricks,
    Lakebase,
}

impl DatabricksVariant {
    fn from_metadata_value(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "databricks" | "sql" | "databricks-sql" => Some(Self::Databricks),
            "lakebase" | "databricks-lakebase" => Some(Self::Lakebase),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum ComputeMode {
    SqlWarehouse,
    SparkCluster,
    Lakebase,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum TableFormat {
    Parquet,
    Delta,
    Iceberg,
}

impl TableFormat {
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    table_format: TableFormat,
    variant: DatabricksVariant,
    scenario_slug: String,
    created_tables: Vec<String>,
    cluster_id: Option<String>,
    cluster_created_by_adapter: bool,
    /// Epoch millis when the run was set up (for Query History time-range filtering).
    started_at_ms: u64,
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
    table_format: TableFormat,
    warehouse_id: String,
    compute_target: ComputeTarget,
    catalog: String,
    schema: String,
    drop_tables_on_teardown: bool,
    staging_volume_path: String,
}

#[derive(Debug, Clone)]
enum ComputeTarget {
    SqlWarehouse,
    SparkCluster(ClusterConfig),
    Lakebase(LakebaseConfig),
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

#[derive(Debug, Clone)]
struct LakebaseConfig {
    user: String,
    host: String,
    db_name: String,
    schema: String,
    target: LakebaseSyncTarget,
}

#[derive(Debug, Clone)]
enum LakebaseSyncTarget {
    /// Provisioned Lakebase instance
    Instance { name: String },
    /// Autoscaling Lakebase project + branch
    Project { name: String, branch: String },
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
            ComputeMode::Lakebase => {
                if warehouse_id.is_empty() {
                    return Err(anyhow!(
                        "Missing Databricks warehouse ID. Set --databricks-sql-warehouse-id or provide it in --databricks-http-path"
                    ));
                }

                let pg_host = args.lakebase_pg_host.ok_or_else(|| {
                    anyhow!("--lakebase-pg-host is required for lakebase compute mode")
                })?;
                let pg_user = args.lakebase_pg_user.ok_or_else(|| {
                    anyhow!("--lakebase-pg-user is required for lakebase compute mode")
                })?;
                let pg_schema = args
                    .lakebase_pg_schema
                    .unwrap_or_else(|| args.databricks_schema.clone());

                let sync_target = if let Some(instance) = args.lakebase_database_instance {
                    LakebaseSyncTarget::Instance { name: instance }
                } else if let Some(project) = args.lakebase_project {
                    LakebaseSyncTarget::Project {
                        name: project,
                        branch: args.lakebase_branch.clone(),
                    }
                } else {
                    return Err(anyhow!(
                        "Either --lakebase-database-instance or --lakebase-project is required for lakebase compute mode"
                    ));
                };

                ComputeTarget::Lakebase(LakebaseConfig {
                    user: pg_user,
                    host: pg_host,
                    db_name: args.lakebase_pg_db_name.clone(),
                    schema: pg_schema,
                    target: sync_target,
                })
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

        Ok(Self {
            endpoint: args.databricks_endpoint,
            token: args.databricks_token,
            http_path: args.databricks_http_path,
            table_format: args.databricks_table_format,
            warehouse_id,
            compute_target,
            catalog: args.databricks_catalog,
            schema: args.databricks_schema,
            drop_tables_on_teardown: args.drop_tables_on_teardown,
            staging_volume_path: args.databricks_staging_volume_path.clone(),
        })
    }
}

#[derive(Debug, Deserialize)]
struct DatabaseCredentialResponse {
    token: String,
    #[serde(alias = "expiration_time", alias = "expire_time")]
    expiration_time: Option<String>,
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
            "databricks://token:{}@{}:443/{}?catalog={}&schema={}",
            self.config.token,
            self.config.endpoint,
            self.config.http_path,
            urlencoding::encode(&self.config.catalog),
            urlencoding::encode(&self.config.schema),
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

    fn lakebase_synced_table_full_name(
        &self,
        table_name: &str,
        lakebase_config: &LakebaseConfig,
    ) -> String {
        format!(
            "{}.{}.{}",
            self.config.catalog, lakebase_config.schema, table_name
        )
    }

    fn quoted_identifier(identifier: &str) -> String {
        format!("`{}`", identifier.replace('`', "``"))
    }

    fn table_full_name(&self, table_name: &str) -> String {
        format!(
            "{}.{}.{}",
            Self::quoted_identifier(&self.config.catalog),
            Self::quoted_identifier(&self.config.schema),
            Self::quoted_identifier(table_name)
        )
    }

    fn scenario_slug(metadata: &HashMap<String, Value>) -> String {
        let raw = metadata
            .get("scenario")
            .and_then(Value::as_str)
            .or_else(|| metadata.get("scenario_name").and_then(Value::as_str))
            .unwrap_or("default");

        let mut slug = String::with_capacity(raw.len());
        let mut last_was_separator = false;

        for ch in raw.chars() {
            let c = ch.to_ascii_lowercase();
            if c.is_ascii_alphanumeric() {
                slug.push(c);
                last_was_separator = false;
            } else if !last_was_separator {
                slug.push('-');
                last_was_separator = true;
            }
        }

        let slug = slug.trim_matches('-').to_string();
        if slug.is_empty() {
            "default".to_string()
        } else {
            slug
        }
    }

    fn notebook_path_for_scenario(scenario_slug: &str) -> String {
        format!("/Shared/spicebench/sync_autoloader_{scenario_slug}")
    }

    fn job_name_for_scenario(scenario_slug: &str) -> String {
        format!("spicebench_sync_tables_{scenario_slug}")
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
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok("STRING".to_string()),
            DataType::Date32 => Ok("DATE".to_string()),
            DataType::Timestamp(_, tz) => Ok(match tz {
                Some(_) => "TIMESTAMP".to_string(),
                None => "TIMESTAMP_NTZ".to_string(),
            }),
            DataType::Decimal128(precision, scale) => {
                let precision = (*precision).min(38);
                Ok(format!("DECIMAL({precision}, {scale})"))
            }
            other => Err(anyhow!(
                "Unsupported Arrow data type for Lakebase table creation: {other:?}"
            )),
        }
    }

    fn create_table_ddl(
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
            self.table_full_name(table_name),
            table_format.as_sql_using()
        ))
    }

    /// Build a CTAS statement that creates the table by reading parquet files
    /// from S3.
    ///
    /// `location` is the full S3 URI for the table data, e.g.
    /// `s3://bucket/etl-hive-output/tpch/<run-id>/lineitem/`.
    ///
    /// ```sql
    /// CREATE OR REPLACE TABLE catalog.schema.table
    ///   AS SELECT * FROM parquet.`s3://bucket/path/to/table/`
    /// ```
    fn create_table_ctas(&self, table_name: &str, location: &str) -> String {
        format!(
            "CREATE OR REPLACE TABLE {} AS SELECT * FROM parquet.`{location}`",
            self.table_full_name(table_name),
        )
    }

    fn table_format_from_setup_metadata(
        &self,
        variant: DatabricksVariant,
        metadata: &HashMap<String, Value>,
    ) -> Result<TableFormat> {
        // Databricks managed tables only support Delta format.
        if variant == DatabricksVariant::Databricks {
            return Ok(TableFormat::Delta);
        }

        if let Some(value) = metadata.get("table_format")
            && let Some(s) = value.as_str()
        {
            return TableFormat::from_metadata_value(s).ok_or_else(|| {
                anyhow!("Unsupported table_format '{s}'. Allowed values: parquet, delta, iceberg")
            });
        }

        Ok(self.config.table_format)
    }

    fn variant_from_setup_metadata(metadata: &HashMap<String, Value>) -> Result<DatabricksVariant> {
        if let Some(value) = metadata.get("system_adapter_variant")
            && let Some(s) = value.as_str()
        {
            return DatabricksVariant::from_metadata_value(s).ok_or_else(|| {
                anyhow!(
                    "Unsupported system_adapter_variant '{s}'. Allowed values: databricks, sql, lakebase"
                )
            });
        }

        if let Some(value) = metadata.get("system_under_test")
            && let Some(s) = value.as_str()
        {
            return DatabricksVariant::from_metadata_value(s).ok_or_else(|| {
                anyhow!(
                    "Unsupported system_under_test '{s}' for Databricks adapter. Expected databricks-sql or databricks-lakebase"
                )
            });
        }

        Ok(DatabricksVariant::Databricks)
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

    #[allow(dead_code)]
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
            StatementState::Failed => Err(anyhow!(
                "Databricks SQL statement failed: {}",
                body.status.error_message()
            )),
            StatementState::Canceled => Err(anyhow!("Databricks SQL statement canceled")),
            StatementState::Pending | StatementState::Running => {
                self.wait_for_statement_completion(&body.statement_id).await
            }
        }
    }

    async fn delete_job(&self, job_id: i64) -> Result<()> {
        let url = format!("https://{}/api/2.1/jobs/delete", self.config.endpoint);
        let response = self
            .client
            .post(&url)
            .bearer_auth(&self.config.token)
            .json(&json!({ "job_id": job_id }))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Databricks jobs/delete failed ({status}): {body}"));
        }

        eprintln!("[databricks-adapter] deleted job: job_id={job_id}");
        Ok(())
    }

    async fn find_notebook(&self, notebook_path: &str) -> Result<bool> {
        let url = format!(
            "https://{}/api/2.0/workspace/get-status",
            self.config.endpoint
        );
        let response = self
            .client
            .get(&url)
            .bearer_auth(&self.config.token)
            .query(&[("path", notebook_path)])
            .send()
            .await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(false);
        }

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Databricks workspace/get-status failed ({status}): {body}"
            ));
        }

        Ok(true)
    }

    async fn delete_notebook(&self, notebook_path: &str) -> Result<()> {
        let url = format!("https://{}/api/2.0/workspace/delete", self.config.endpoint);
        let response = self
            .client
            .post(&url)
            .bearer_auth(&self.config.token)
            .json(&json!({
                "path": notebook_path,
                "recursive": false
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Databricks workspace/delete failed ({status}): {body}"
            ));
        }

        eprintln!("[databricks-adapter] deleted notebook: path={notebook_path}");
        Ok(())
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

    /// Execute a SQL query via the Statements API and return inline result rows.
    async fn execute_sql_query(&self, statement: &str) -> Result<Vec<Vec<Option<String>>>> {
        let execute_url = format!("https://{}/api/2.0/sql/statements/", self.config.endpoint);
        let payload = json!({
            "warehouse_id": self.config.warehouse_id,
            "catalog": self.config.catalog,
            "schema": self.config.schema,
            "statement": statement,
            "wait_timeout": "30s",
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
            return Err(anyhow!("SQL query failed ({status}): {body}"));
        }

        let body: StatementWithResultResponse = response.json().await?;
        match body.status.state {
            StatementState::Succeeded => Ok(body.result.map(|r| r.data_array).unwrap_or_default()),
            StatementState::Failed => {
                Err(anyhow!("SQL query failed: {}", body.status.error_message()))
            }
            StatementState::Canceled => Err(anyhow!("SQL query canceled")),
            StatementState::Pending | StatementState::Running => Err(anyhow!(
                "SQL query timed out (statement_id={})",
                body.statement_id
            )),
        }
    }

    /// Fire a tagged marker query and wait for it to appear in the Query History
    /// API.  Once the marker is visible, all earlier queries from this warehouse
    /// are also guaranteed to be visible.
    ///
    /// Returns the `query_start_time_ms` of the marker query so the caller can
    /// use it as the upper bound for the time window.
    async fn fire_marker_and_wait(&self, marker_tag: &str) -> Result<()> {
        // Fire a lightweight SELECT that embeds the marker tag in a comment.
        let marker_sql = format!("SELECT 1 /* spicebench_marker:{marker_tag} */");
        self.execute_sql_statement(&marker_sql).await?;

        // Now poll the Query History API until we see a FINISHED query whose
        // query_text contains the marker tag.
        let deadline = std::time::Instant::now() + Duration::from_secs(600);
        let history_url = format!(
            "https://{}/api/2.0/sql/history/queries",
            self.config.endpoint
        );

        loop {
            if std::time::Instant::now() > deadline {
                return Err(anyhow!(
                    "Timed out (10 min) waiting for marker query to appear in Query History"
                ));
            }

            let filter = json!({
                "filter_by": {
                    "warehouse_ids": [self.config.warehouse_id],
                    "query_text": {
                        "pattern": format!("spicebench_marker:{marker_tag}")
                    },
                    "statuses": ["FINISHED"]
                },
                "max_results": 1
            });

            let response = self
                .client
                .get(&history_url)
                .bearer_auth(&self.config.token)
                .query(&[("include_metrics", "true")])
                .json(&filter)
                .send()
                .await?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow!("Query History API failed ({status}): {body}"));
            }

            let body: QueryHistoryResponse = response.json().await?;
            if !body.res.is_empty() {
                eprintln!(
                    "[databricks-adapter] marker query appeared in Query History: tag={marker_tag}"
                );
                return Ok(());
            }

            eprintln!(
                "[databricks-adapter] waiting for marker query in Query History: tag={marker_tag}"
            );
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    /// Sum `read_bytes` + `write_remote_bytes` from query history for all
    /// FINISHED queries on this warehouse since `start_time_ms`.
    ///
    /// Uses the REST Query History API (`/api/2.0/sql/history/queries`).
    ///
    /// An alternative is querying `system.query.history` via SQL (see
    /// `sum_query_history_io_sql`) which avoids pagination, but requires the
    /// service principal to have `USE SCHEMA` on `system.query` — a privilege
    /// most workspace-scoped tokens lack.
    async fn sum_query_history_io(&self, start_time_ms: u64) -> Result<(u64, u64)> {
        self.sum_query_history_io_rest(start_time_ms).await
    }

    /// SQL path: query `system.query.history` for aggregated I/O bytes.
    /// Currently unused — requires elevated permission `USE SCHEMA` on `system.query`.
    #[allow(dead_code)]
    async fn sum_query_history_io_sql(&self, start_time_ms: u64) -> Result<(u64, u64)> {
        let query = format!(
            "SELECT COALESCE(SUM(read_bytes), 0), COALESCE(SUM(write_remote_bytes), 0) \
             FROM system.query.history \
             WHERE warehouse_id = '{}' \
               AND start_time >= TIMESTAMP_MILLIS({}) \
               AND status = 'FINISHED'",
            self.config.warehouse_id, start_time_ms
        );

        let rows = self.execute_sql_query(&query).await?;
        let row = rows
            .first()
            .ok_or_else(|| anyhow!("No rows returned from system.query.history sum"))?;

        let total_read = row
            .first()
            .and_then(|v| v.as_deref())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let total_write = row
            .get(1)
            .and_then(|v| v.as_deref())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        eprintln!(
            "[databricks-adapter] query history totals (SQL): read_bytes={total_read} write_bytes={total_write}"
        );
        Ok((total_read, total_write))
    }

    /// REST path: paginate through `/api/2.0/sql/history/queries` and sum
    /// per-query `metrics.read_bytes` and `metrics.write_remote_bytes`.
    async fn sum_query_history_io_rest(&self, start_time_ms: u64) -> Result<(u64, u64)> {
        let history_url = format!(
            "https://{}/api/2.0/sql/history/queries",
            self.config.endpoint
        );

        let mut total_read: u64 = 0;
        let mut total_write: u64 = 0;
        let mut page_token: Option<String> = None;

        loop {
            let mut filter = json!({
                "filter_by": {
                    "warehouse_ids": [self.config.warehouse_id],
                    "query_start_time_range": {
                        "start_time_ms": start_time_ms
                    },
                    "statuses": ["FINISHED"]
                },
                "include_metrics": true,
                "max_results": 100
            });

            if let Some(ref token) = page_token {
                filter["page_token"] = json!(token);
            }

            let response = self
                .client
                .get(&history_url)
                .bearer_auth(&self.config.token)
                .json(&filter)
                .send()
                .await?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow!("Query History REST API failed ({status}): {body}"));
            }

            let body: QueryHistoryResponse = response.json().await?;

            for entry in &body.res {
                if let Some(ref m) = entry.metrics {
                    total_read += m.read_bytes.unwrap_or(0);
                    total_write += m.write_remote_bytes.unwrap_or(0);
                }
            }

            if body.has_next_page {
                page_token = body.next_page_token;
            } else {
                break;
            }
        }

        Ok((total_read, total_write))
    }

    async fn ensure_cluster_ready(&self) -> Result<(String, bool)> {
        let cluster_cfg = match &self.config.compute_target {
            ComputeTarget::SparkCluster(cfg) => cfg,
            ComputeTarget::SqlWarehouse => {
                return Err(anyhow!(
                    "ensure_cluster_ready called in sql-warehouse compute mode"
                ));
            }
            ComputeTarget::Lakebase(_) => {
                return Err(anyhow!(
                    "ensure_cluster_ready called in lakebase compute mode"
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

    async fn ensure_notebook(
        &self,
        scenario_slug: &str,
        table_locations: &HashMap<String, String>,
    ) -> Result<()> {
        let table_locations_json = serde_json::to_string(table_locations)?;
        let notebook_source = format!(
            r#"
from pyspark.sql.functions import *
import json

catalog = "{catalog}"
schema = "{schema}"

table_locations = json.loads('{table_locations_json}')

for table, source_path in table_locations.items():
    target_table = f"{{catalog}}.{{schema}}.{{table}}"
    checkpoint = f"/tmp/spicebench_{{schema}}_{{table}}_checkpoint"
    schema_location = f"/tmp/spicebench_{{schema}}_{{table}}_schema"

    (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.includeExistingFiles", "true")
            .option("cloudFiles.schemaLocation", schema_location)
            .load(source_path)
            .writeStream
            .option("checkpointLocation", checkpoint)
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(target_table)
            .awaitTermination()
    )

print("OK")
"#,
            catalog = self.config.catalog,
            schema = self.config.schema,
            table_locations_json = table_locations_json,
        );

        let encoded = general_purpose::STANDARD.encode(notebook_source);

        let notebook_path = Self::notebook_path_for_scenario(scenario_slug);
        let mkdirs_url = format!("https://{}/api/2.0/workspace/mkdirs", self.config.endpoint);
        let mkdirs_response = self
            .client
            .post(mkdirs_url)
            .bearer_auth(&self.config.token)
            .json(&json!({
                "path": "/Shared/spicebench"
            }))
            .send()
            .await?;

        if !mkdirs_response.status().is_success() {
            let status = mkdirs_response.status();
            let body = mkdirs_response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Databricks workspace/mkdirs failed ({status}): {body}"
            ));
        }

        eprintln!(
            "[databricks-adapter] uploading sync notebook: scenario={scenario_slug} tables_count={} path={notebook_path}",
            table_locations.len()
        );
        let url = format!("https://{}/api/2.0/workspace/import", self.config.endpoint);
        let response = self
            .client
            .post(url)
            .bearer_auth(&self.config.token)
            .json(&json!({
            "path": notebook_path,
                "language": "PYTHON",
                "format": "SOURCE",
                "content": encoded,
                "overwrite": true
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            eprintln!(
                "[databricks-adapter] failed to upload sync notebook: scenario={scenario_slug} path={notebook_path} status={status} body={body}"
            );
            return Err(anyhow!(
                "Databricks workspace/import failed ({status}): {body}"
            ));
        }

        eprintln!(
            "[databricks-adapter] sync notebook uploaded: scenario={scenario_slug} tables_count={} path={notebook_path}",
            table_locations.len()
        );

        Ok(())
    }

    async fn find_job_id_by_name(&self, job_name: &str) -> Result<Option<i64>> {
        let url = format!("https://{}/api/2.1/jobs/list", self.config.endpoint);

        let resp: serde_json::Value = self
            .client
            .get(url)
            .bearer_auth(&self.config.token)
            .send()
            .await?
            .json()
            .await?;

        if let Some(jobs) = resp["jobs"].as_array() {
            for job in jobs {
                if job["settings"]["name"].as_str() == Some(job_name) {
                    return Ok(job["job_id"].as_i64());
                }
            }
        }

        Ok(None)
    }

    async fn ensure_notebook_sync_job(&self, scenario_slug: &str) -> Result<()> {
        let notebook_path = Self::notebook_path_for_scenario(scenario_slug);
        let job_name = Self::job_name_for_scenario(scenario_slug);

        if let Some(existing_id) = self.find_job_id_by_name(&job_name).await? {
            eprintln!(
                "[databricks-adapter] sync job already exists: scenario={scenario_slug} job_name={job_name} job_id={existing_id}"
            );
            return Ok(());
        }

        eprintln!(
            "[databricks-adapter] creating scheduled sync job: scenario={scenario_slug} job_name={job_name} path={notebook_path}"
        );
        let create_url = format!("https://{}/api/2.1/jobs/create", self.config.endpoint);
        let response = self
            .client
            .post(create_url)
            .bearer_auth(&self.config.token)
            .json(&json!({
                "name": job_name,
                "max_concurrent_runs": 1,
                "tasks": [
                {
                    "task_key": "sync_autoloader",
                    "notebook_task": {
                        "notebook_path": notebook_path
                    },
                    "environment_key": "serverless_env"
                }
                ],
                "environments": [
                {
                    "environment_key": "serverless_env",
                    "spec": {
                        "client": "1"
                    }
                }
                ],
                "schedule": {
                    "quartz_cron_expression": "0 0/1 * * * ?",
                    "timezone_id": "UTC",
                    "pause_status": "UNPAUSED"
                }
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            eprintln!(
                "[databricks-adapter] failed to create scheduled sync job: scenario={scenario_slug} job_name={job_name} status={status} body={body}"
            );
            return Err(anyhow!("Databricks jobs/create failed ({status}): {body}"));
        }

        eprintln!(
            "[databricks-adapter] scheduled sync job created: scenario={scenario_slug} job_name={job_name} path={notebook_path}"
        );

        Ok(())
    }

    #[allow(dead_code)]
    fn uc_column_type_for_arrow(data_type: &DataType) -> Result<UcColumnType> {
        match data_type {
            DataType::Boolean => Ok(UcColumnType::new("BOOLEAN", "BOOLEAN", "\"boolean\"")),
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::UInt8
            | DataType::UInt16 => Ok(UcColumnType::new("INT", "INT", "\"integer\"")),
            DataType::Int64 | DataType::UInt32 | DataType::UInt64 => {
                Ok(UcColumnType::new("LONG", "BIGINT", "\"long\""))
            }
            DataType::Float32 => Ok(UcColumnType::new("FLOAT", "FLOAT", "\"float\"")),
            DataType::Float64 => Ok(UcColumnType::new("DOUBLE", "DOUBLE", "\"double\"")),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                Ok(UcColumnType::new("STRING", "STRING", "\"string\""))
            }
            DataType::Date32 => Ok(UcColumnType::new("DATE", "DATE", "\"date\"")),
            DataType::Timestamp(_, _) => {
                Ok(UcColumnType::new("TIMESTAMP", "TIMESTAMP", "\"timestamp\""))
            }
            DataType::Decimal128(precision, scale) => Ok(UcColumnType::new(
                "DECIMAL",
                format!("DECIMAL({precision}, {scale})"),
                format!("\"decimal({precision},{scale})\""),
            )),
            other => Err(anyhow!(
                "Unsupported Arrow data type for Unity Catalog table creation: {other:?}"
            )),
        }
    }

    #[allow(dead_code)]
    async fn uc_table_exists(&self, table_name: &str) -> Result<bool> {
        let full_name = self.uc_table_full_name(table_name);
        let get_url = format!(
            "https://{}/api/2.1/unity-catalog/tables/{full_name}",
            self.config.endpoint
        );

        let response = self
            .client
            .get(get_url)
            .bearer_auth(&self.config.token)
            .send()
            .await?;

        if response.status() == StatusCode::OK {
            return Ok(true);
        }

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(false);
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(anyhow!(
            "Databricks Unity Catalog tables/get failed ({status}) for '{full_name}': {body}"
        ))
    }

    #[allow(dead_code)]
    async fn create_uc_table_if_not_exists(
        &self,
        table_name: &str,
        dataset_cfg: &DatasetConfig,
    ) -> Result<bool> {
        if self.uc_table_exists(table_name).await? {
            return Ok(false);
        }

        let columns = dataset_cfg
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(position, field)| {
                let col_type = Self::uc_column_type_for_arrow(field.data_type())?;
                Ok::<_, anyhow::Error>(UcTableColumnCreateRequest {
                    name: field.name().clone(),
                    type_name: col_type.type_name,
                    type_text: col_type.type_text,
                    type_json: col_type.type_json,
                    position,
                    nullable: field.is_nullable(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let create_url = format!(
            "https://{}/api/2.1/unity-catalog/tables",
            self.config.endpoint
        );
        let response = self
            .client
            .post(create_url)
            .bearer_auth(&self.config.token)
            .json(&UcTableCreateRequest {
                name: table_name.to_string(),
                catalog_name: self.config.catalog.clone(),
                schema_name: self.config.schema.clone(),
                table_type: "MANAGED".to_string(),
                data_source_format: "DELTA".to_string(),
                columns,
            })
            .send()
            .await?;

        if response.status().is_success() {
            return Ok(true);
        }

        if response.status() == StatusCode::CONFLICT {
            return Ok(false);
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(anyhow!(
            "Databricks Unity Catalog tables/create failed ({status}) for '{}': {body}",
            self.uc_table_full_name(table_name)
        ))
    }

    async fn lakebase_pg_uri(&self) -> Result<String> {
        let lakebase_config = match &self.config.compute_target {
            ComputeTarget::Lakebase(cfg) => cfg,
            _ => {
                return Err(anyhow!(
                    "lakebase_pg_uri called without Lakebase compute target"
                ));
            }
        };

        let token = self.generate_lakebase_pg_token().await?;

        Ok(format!(
            "postgresql://{}:{}@{}/{}?sslmode=require&options=--search_path%3D{}",
            urlencoding::encode(&lakebase_config.user),
            urlencoding::encode(&token),
            lakebase_config.host,
            lakebase_config.db_name,
            urlencoding::encode(&lakebase_config.schema),
        ))
    }

    async fn create_synced_table(
        &self,
        table_name: &str,
        primary_key_columns: &[String],
    ) -> Result<()> {
        let lakebase_config = match &self.config.compute_target {
            ComputeTarget::Lakebase(cfg) => cfg,
            _ => {
                return Err(anyhow!(
                    "create_synced_table called without Lakebase compute target"
                ));
            }
        };
        let synced_table_name = self.lakebase_synced_table_full_name(table_name, lakebase_config);
        let source_table_name = self.uc_table_full_name(table_name);
        let url = format!(
            "https://{}/api/2.0/database/synced_tables",
            self.config.endpoint
        );

        let mut payload = json!({
            "name": synced_table_name,
            "logical_database_name": lakebase_config.db_name,
            "spec": {
                "source_table_full_name": source_table_name,
                "primary_key_columns": primary_key_columns,
                "scheduling_policy": "CONTINUOUS",
            },
            "new_pipeline_spec": {
                "storage_catalog": self.config.catalog,
                "storage_schema": self.config.schema,
            }
        });

        match &lakebase_config.target {
            LakebaseSyncTarget::Instance { name } => {
                payload["database_instance_name"] = json!(name);
            }
            LakebaseSyncTarget::Project { name, branch } => {
                payload["project_name"] = json!(name);
                payload["branch_name"] = json!(branch);
            }
        }
        eprintln!(
            "[databricks-adapter] create_synced_table: source_table_name={}, synced_table_name={}",
            source_table_name, synced_table_name
        );

        let mut last_err = None;
        for attempt in 1..=3 {
            let response = self
                .client
                .post(&url)
                .bearer_auth(&self.config.token)
                .json(&payload)
                .send()
                .await?;

            if response.status().is_success() {
                last_err = None;
                break;
            }

            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            let err_msg =
                format!("Failed to create synced table '{synced_table_name}' ({status}): {body}");

            if status.is_server_error() && attempt < 3 {
                eprintln!(
                    "[databricks-adapter] attempt {attempt}/3 failed (transient {status}), retrying in 5s..."
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
                last_err = Some(err_msg);
                continue;
            }

            return Err(anyhow!(err_msg));
        }

        if let Some(err) = last_err {
            return Err(anyhow!(err));
        }

        eprintln!(
            "[databricks-adapter] synced table '{}' created, waiting for ONLINE status",
            table_name
        );
        self.wait_for_synced_table_online(table_name, lakebase_config)
            .await
    }

    async fn wait_for_synced_table_online(
        &self,
        table_name: &str,
        lakebase_config: &LakebaseConfig,
    ) -> Result<()> {
        let synced_table_name = self.lakebase_synced_table_full_name(table_name, lakebase_config);
        let status_url = format!(
            "https://{}/api/2.0/database/synced_tables/{}",
            self.config.endpoint,
            urlencoding::encode(&synced_table_name),
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(600);

        loop {
            if std::time::Instant::now() > deadline {
                return Err(anyhow!(
                    "Timed out waiting for synced table '{synced_table_name}' to come ONLINE"
                ));
            }

            let response = self
                .client
                .get(&status_url)
                .bearer_auth(&self.config.token)
                .send()
                .await?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow!(
                    "Failed to get synced table status for '{synced_table_name}' ({status}): {body}"
                ));
            }

            let body: Value = response.json().await?;

            let detailed_state = body
                .pointer("/data_synchronization_status/detailed_state")
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            match detailed_state {
                "SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE"
                | "SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE" => {
                    eprintln!(
                        "[databricks-adapter] synced table '{}' is ONLINE",
                        table_name
                    );
                    return Ok(());
                }
                "SYNCED_TABLE_OFFLINE_FAILED" | "OFFLINE_FAILED" => {
                    let message = body
                        .pointer("/data_synchronization_status/message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown error");
                    return Err(anyhow!(
                        "Synced table '{synced_table_name}' failed: {message}"
                    ));
                }
                _ => {
                    eprintln!(
                        "[databricks-adapter] `synced table` '{}' state: {}, waiting...",
                        table_name, detailed_state
                    );
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// Connect to Lakebase Postgres and scrape available metrics from pg_stat views.
    /// Probes each view individually so we can discover what's accessible in the managed environment.
    /// Resets PostgreSQL cumulative statistics counters so that subsequent
    /// `scrape_lakebase_pg_metrics` calls return values that reflect only the
    /// current benchmark run.
    ///
    /// **Important:** This calls `pg_stat_reset()` which clears *all* statistics
    /// for the current database.  It is safe on a **dedicated** Lakebase
    /// instance used exclusively for benchmarking, but must **never** be run
    /// against a shared or production database.
    async fn reset_lakebase_pg_stats(&self) -> Result<()> {
        eprintln!(
            "[databricks-adapter] WARNING: Resetting PostgreSQL statistics counters via pg_stat_reset(). \
             This operation clears all cumulative statistics for the current database. \
             Do NOT use this adapter against shared or production Lakebase instances — \
             it is intended for dedicated benchmarking environments only."
        );

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls_config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| anyhow!("Failed to configure TLS: {e}"))?
        .with_root_certificates(root_store)
        .with_no_client_auth();
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);

        let pg_uri = self.lakebase_pg_uri().await?;
        let (client, connection) = tokio_postgres::connect(&pg_uri, tls)
            .await
            .map_err(|e| anyhow!("Failed to connect to Lakebase PG for stats reset: {e}"))?;
        tokio::spawn(connection);

        client
            .execute("SELECT pg_stat_reset()", &[])
            .await
            .map_err(|e| anyhow!("pg_stat_reset() failed: {e}"))?;

        eprintln!("[databricks-adapter] pg_stat_reset() completed — statistics counters zeroed");
        Ok(())
    }

    async fn scrape_lakebase_pg_metrics(&self) -> Result<(ResourceMetrics, IngestionMetrics)> {
        let lakebase_config = match &self.config.compute_target {
            ComputeTarget::Lakebase(cfg) => cfg,
            _ => return Err(anyhow!("scrape_lakebase_pg_metrics called without Lakebase compute target")),
        };

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls_config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| anyhow!("Failed to configure TLS: {e}"))?
        .with_root_certificates(root_store)
        .with_no_client_auth();
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);

        let pg_uri = self.lakebase_pg_uri().await?;
        let (client, connection) = tokio_postgres::connect(&pg_uri, tls)
            .await
            .map_err(|e| anyhow!("Failed to connect to Lakebase PG for metrics: {e}"))?;
        tokio::spawn(connection);

        let mut resource = ResourceMetrics::default();
        let mut ingestion = IngestionMetrics::default();

        // 1. active_connections from pg_stat_activity
        match client
            .query_one("SELECT count(*)::bigint FROM pg_stat_activity WHERE state = 'active'", &[])
            .await
        {
            Ok(row) => {
                let count: i64 = row.get(0);
                eprintln!("[databricks-adapter] lakebase pg_stat_activity: active_connections={count}");
                ingestion.active_connections = Some(count as u64);
            }
            Err(e) => {
                eprintln!("[databricks-adapter] lakebase pg_stat_activity: FAILED — {e}");
            }
        }

        // 2. rows_ingested from pg_stat_user_tables (cumulative n_tup_ins since stats reset)
        match client
            .query_one(
                "SELECT COALESCE(SUM(n_tup_ins), 0)::bigint FROM pg_stat_user_tables WHERE schemaname = $1",
                &[&lakebase_config.schema],
            )
            .await
        {
            Ok(row) => {
                let rows: i64 = row.get(0);
                eprintln!("[databricks-adapter] lakebase pg_stat_user_tables: rows_ingested(n_tup_ins)={rows}");
                ingestion.rows_ingested = Some(rows as u64);
            }
            Err(e) => {
                eprintln!("[databricks-adapter] lakebase pg_stat_user_tables (n_tup_ins): FAILED — {e}");
            }
        }

        // 3. disk_read_bytes from pg_stat_user_tables (heap_blks_read + idx_blks_read) * 8192
        match client
            .query_one(
                "SELECT COALESCE(SUM(heap_blks_read + idx_blks_read), 0)::bigint FROM pg_stat_user_tables WHERE schemaname = $1",
                &[&lakebase_config.schema],
            )
            .await
        {
            Ok(row) => {
                let blocks: i64 = row.get(0);
                let bytes = blocks as u64 * 8192;
                eprintln!("[databricks-adapter] lakebase pg_stat_user_tables: disk_read_blocks={blocks} disk_read_bytes={bytes}");
                resource.disk_read_bytes = Some(bytes);
            }
            Err(e) => {
                eprintln!("[databricks-adapter] lakebase pg_stat_user_tables (blks_read): FAILED — {e}");
            }
        }

        // 4. disk_read_iops from pg_stat_user_tables (heap_blks_read + idx_blks_read as total read ops)
        // Already computed above — reuse blocks count
        // (disk_read_bytes / 8192 = block count = logical read ops)

        // 5. bytes_ingested from pg_total_relation_size (approximate total table size)
        match client
            .query_one(
                &format!(
                    "SELECT COALESCE(SUM(pg_total_relation_size(schemaname || '.' || tablename)), 0)::bigint \
                     FROM pg_tables WHERE schemaname = '{}'",
                    lakebase_config.schema.replace('\'', "''")
                ),
                &[],
            )
            .await
        {
            Ok(row) => {
                let size: i64 = row.get(0);
                eprintln!("[databricks-adapter] lakebase pg_total_relation_size: bytes_ingested={size}");
                ingestion.bytes_ingested = Some(size as u64);
            }
            Err(e) => {
                eprintln!("[databricks-adapter] lakebase pg_total_relation_size: FAILED — {e}");
            }
        }

        // 6. Try pg_stat_io (PG16+) for more accurate I/O metrics
        match client
            .query_one(
                "SELECT COALESCE(SUM(reads), 0)::bigint, COALESCE(SUM(writes), 0)::bigint, \
                 COALESCE(MIN(op_bytes), 8192)::bigint FROM pg_stat_io",
                &[],
            )
            .await
        {
            Ok(row) => {
                let reads: i64 = row.get(0);
                let writes: i64 = row.get(1);
                let op_bytes: i64 = row.get(2);
                let read_bytes = reads as u64 * op_bytes as u64;
                let write_bytes = writes as u64 * op_bytes as u64;
                eprintln!(
                    "[databricks-adapter] lakebase pg_stat_io: reads={reads} writes={writes} op_bytes={op_bytes} \
                     read_bytes={read_bytes} write_bytes={write_bytes}"
                );
                // pg_stat_io is more accurate — override pg_stat_user_tables if available
                resource.disk_read_bytes = Some(read_bytes);
                resource.disk_write_bytes = Some(write_bytes);
            }
            Err(e) => {
                eprintln!("[databricks-adapter] lakebase pg_stat_io: FAILED (expected if PG<16) — {e}");
            }
        }

        // 7. Try pg_stat_bgwriter for write-related stats
        match client
            .query_one(
                "SELECT buffers_backend::bigint, buffers_clean::bigint, buffers_checkpoint::bigint FROM pg_stat_bgwriter",
                &[],
            )
            .await
        {
            Ok(row) => {
                let backend: i64 = row.get(0);
                let clean: i64 = row.get(1);
                let checkpoint: i64 = row.get(2);
                let total_write_buffers = backend + clean + checkpoint;
                let write_bytes = total_write_buffers as u64 * 8192;
                eprintln!(
                    "[databricks-adapter] lakebase pg_stat_bgwriter: backend={backend} clean={clean} \
                     checkpoint={checkpoint} total_write_bytes={write_bytes}"
                );
                // Only use if pg_stat_io wasn't available
                if resource.disk_write_bytes.is_none() {
                    resource.disk_write_bytes = Some(write_bytes);
                }
            }
            Err(e) => {
                eprintln!("[databricks-adapter] lakebase pg_stat_bgwriter: FAILED — {e}");
            }
        }

        eprintln!(
            "[databricks-adapter] lakebase metrics summary: resource={resource:?} ingestion={ingestion:?}"
        );

        Ok((resource, ingestion))
    }

    async fn delete_lakebase_pg_tables(
        &self,
        table_names: &[String],
        lakebase_config: &LakebaseConfig,
    ) -> Result<()> {
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls_config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| anyhow!("Failed to configure TLS: {e}"))?
        .with_root_certificates(root_store)
        .with_no_client_auth();
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);

        let pg_uri = self.lakebase_pg_uri().await?;
        let (client, connection) = tokio_postgres::connect(&pg_uri, tls)
            .await
            .map_err(|e| anyhow!("Failed to connect to Lakebase PG: {e}"))?;
        tokio::spawn(connection);

        for table_name in table_names {
            let sql = format!(
                "DROP TABLE IF EXISTS \"{}\".\"{}\"",
                lakebase_config.schema, table_name,
            );
            client
                .execute(&sql, &[])
                .await
                .map_err(|e| anyhow!("Failed to drop Lakebase PG table '{table_name}': {e}"))?;
            eprintln!("[databricks-adapter] dropped Lakebase PG table '{table_name}'");
        }

        Ok(())
    }

    async fn delete_synced_table(&self, table_name: &str) -> Result<()> {
        let lakebase_config = match &self.config.compute_target {
            ComputeTarget::Lakebase(cfg) => cfg,
            _ => {
                return Err(anyhow!(
                    "delete_synced_table called without Lakebase compute target"
                ));
            }
        };
        let synced_table_name = self.lakebase_synced_table_full_name(table_name, lakebase_config);
        let url = format!(
            "https://{}/api/2.0/database/synced_tables/{}",
            self.config.endpoint,
            urlencoding::encode(&synced_table_name),
        );

        let payload = json!({"purge_data": true});

        let response = self
            .client
            .delete(&url)
            .bearer_auth(&self.config.token)
            .json(&payload)
            .send()
            .await?;

        if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(anyhow!(
            "Failed to delete synced table '{synced_table_name}' ({status}): {body}"
        ))
    }

    /// Query the SQL Warehouse GET API to retrieve warehouse info (num_active_sessions, num_clusters).
    async fn get_warehouse_info(&self) -> Result<WarehouseInfoResponse> {
        let url = format!(
            "https://{}/api/2.0/sql/warehouses/{}",
            self.config.endpoint, self.config.warehouse_id
        );

        let response = self
            .client
            .get(&url)
            .bearer_auth(&self.config.token)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Failed to get warehouse info ({status}): {body}"));
        }

        let info: WarehouseInfoResponse = response.json().await?;
        Ok(info)
    }

    async fn generate_lakebase_pg_token(&self) -> Result<String> {
        let lakebase_config = match &self.config.compute_target {
            ComputeTarget::Lakebase(cfg) => cfg,
            _ => {
                return Err(anyhow!(
                    "generate_lakebase_pg_token called without Lakebase compute target"
                ));
            }
        };

        let (url, payload) = match &lakebase_config.target {
            LakebaseSyncTarget::Instance { name } => {
                let url = format!(
                    "https://{}/api/2.0/database/credentials",
                    self.config.endpoint
                );
                let payload = json!({
                    "request_id": Uuid::new_v4().to_string(),
                    "instance_names": [name],
                });
                (url, payload)
            }
            LakebaseSyncTarget::Project { name, branch } => {
                // Autoscaling uses the postgres API path and endpoint-based credential generation
                let endpoint_path =
                    format!("projects/{}/branches/{}/endpoints/default", name, branch);
                let url = format!(
                    "https://{}/api/2.0/postgres/generate-database-credential",
                    self.config.endpoint
                );
                let payload = json!({
                    "request_id": Uuid::new_v4().to_string(),
                    "endpoint": endpoint_path,
                });
                (url, payload)
            }
        };

        eprintln!("[databricks-adapter] generating fresh Lakebase PG OAuth token");

        let response = self
            .client
            .post(&url)
            .bearer_auth(&self.config.token)
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Failed to generate Lakebase database credential ({status}): {body}"
            ));
        }

        let cred: DatabaseCredentialResponse = response.json().await?;
        eprintln!(
            "[databricks-adapter] Lakebase PG token generated, expires: {}",
            cred.expiration_time.as_deref().unwrap_or("unknown")
        );

        Ok(cred.token)
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

/// Statement response that includes inline result data.
#[derive(Debug, Deserialize)]
struct StatementWithResultResponse {
    statement_id: String,
    status: StatementStatus,
    #[serde(default)]
    result: Option<StatementResultData>,
}

#[derive(Debug, Deserialize)]
struct StatementResultData {
    #[serde(default)]
    data_array: Vec<Vec<Option<String>>>,
}

#[derive(Debug, Serialize)]
struct UcSchemaCreateRequest {
    catalog_name: String,
    name: String,
}

#[allow(dead_code)]
#[derive(Debug)]
struct UcColumnType {
    type_name: String,
    type_text: String,
    type_json: String,
}

#[allow(dead_code)]
impl UcColumnType {
    fn new(
        type_name: impl Into<String>,
        type_text: impl Into<String>,
        type_json: impl Into<String>,
    ) -> Self {
        Self {
            type_name: type_name.into(),
            type_text: type_text.into(),
            type_json: type_json.into(),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Serialize)]
struct UcTableColumnCreateRequest {
    name: String,
    type_name: String,
    type_text: String,
    type_json: String,
    position: usize,
    nullable: bool,
}

#[allow(dead_code)]
#[derive(Debug, Serialize)]
struct UcTableCreateRequest {
    name: String,
    catalog_name: String,
    schema_name: String,
    table_type: String,
    data_source_format: String,
    columns: Vec<UcTableColumnCreateRequest>,
}

#[derive(Debug, Deserialize)]
struct WarehouseInfoResponse {
    #[serde(default)]
    num_active_sessions: Option<u64>,
    #[serde(default)]
    num_clusters: Option<u64>,
    #[serde(default)]
    #[allow(dead_code)]
    warehouse_type: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    cluster_size: Option<String>,
}

/// A single query entry from the Query History API.
#[derive(Debug, Deserialize)]
struct QueryHistoryEntry {
    #[serde(default)]
    #[allow(dead_code)]
    query_id: String,
    #[serde(default)]
    #[allow(dead_code)]
    status: Option<String>,
    #[serde(default)]
    metrics: Option<QueryHistoryMetrics>,
}

#[derive(Debug, Deserialize)]
struct QueryHistoryMetrics {
    /// Total bytes read by the query, including both remote cloud storage (`read_remote_bytes`)
    /// and local SSD/disk cache (`read_cache_bytes`). Maps to `disk_read_bytes`.
    #[serde(default)]
    read_bytes: Option<u64>,
    /// Bytes written to remote cloud storage (S3/ADLS/GCS). This is the only write metric
    /// available in the REST API — non-zero for DDL/DML that materializes data (e.g. CTAS).
    /// Maps to `disk_write_bytes`.
    #[serde(default)]
    write_remote_bytes: Option<u64>,
}

/// Response from GET /api/2.0/sql/history/queries
#[derive(Debug, Deserialize)]
struct QueryHistoryResponse {
    #[serde(default)]
    res: Vec<QueryHistoryEntry>,
    #[serde(default)]
    has_next_page: bool,
    #[serde(default)]
    next_page_token: Option<String>,
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
        metadata: HashMap<String, Value>,
        datasets: HashMap<String, DatasetConfig>,
        etl_sink_type: Option<EtlSinkType>,
    ) -> std::result::Result<SetupResponse, String> {
        let _ = etl_sink_type;
        eprintln!("[databricks-adapter] setup: run_id={run_id}");
        eprintln!("[databricks-adapter] endpoint={}", self.config.endpoint);

        let scenario_slug = Self::scenario_slug(&metadata);
        let variant = Self::variant_from_setup_metadata(&metadata)
            .map_err(|e| format!("Invalid setup metadata: {e}"))?;

        let (cluster_id, cluster_created_by_adapter) = match &self.config.compute_target {
            ComputeTarget::SparkCluster(_) => {
                let (cluster_id, created) = self
                    .ensure_cluster_ready()
                    .await
                    .map_err(|e| format!("Failed to ensure Databricks cluster is ready: {e}"))?;
                (Some(cluster_id), created)
            }
            ComputeTarget::SqlWarehouse => (None, false),
            ComputeTarget::Lakebase(_) => (None, false),
        };

        eprintln!("[databricks-adapter] setup: variant={variant:#?}");

        match variant {
            DatabricksVariant::Databricks => {
                self.ensure_uc_schema_exists()
                    .await
                    .map_err(|e| format!("Failed to ensure Unity Catalog schema exists: {e}"))?;
            }
            DatabricksVariant::Lakebase => {
                self.ensure_uc_schema_exists()
                    .await
                    .map_err(|e| format!("Failed to ensure Unity Catalog schema exists: {e}"))?;
            }
        }

        let table_format = self
            .table_format_from_setup_metadata(variant, &metadata)
            .map_err(|e| format!("Invalid setup metadata: {e}"))?;

        let started_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.runs.insert(
            run_id,
            RunState {
                table_format,
                variant,
                scenario_slug: scenario_slug.clone(),
                created_tables: Vec::new(),
                cluster_id: cluster_id.clone(),
                cluster_created_by_adapter,
                started_at_ms,
            },
        );

        let mut created_tables = Vec::with_capacity(datasets.len());
        let mut table_locations: HashMap<String, String> = HashMap::with_capacity(datasets.len());

        eprintln!(
            "[databricks-adapter] setup: creating tables...: {:#?}",
            metadata
        );

        if metadata.get("etl_sink") == Some(&Value::String("hive".to_string())) {
            eprintln!("[databricks-adapter] Initialization for hive sink");

            // Create UC tables via CTAS (common to all variants).
            for (table_name, dataset_cfg) in &datasets {
                let location = dataset_cfg.location.as_deref().ok_or_else(|| {
                    format!("Dataset '{table_name}' is missing required 'location' field")
                })?;
                let drop_sql = format!("DROP TABLE IF EXISTS {}", self.table_full_name(table_name));
                self.execute_sql_statement(&drop_sql).await.map_err(|e| {
                    format!(
                        "Failed to drop existing table '{table_name}' during create_tables: {e}"
                    )
                })?;

                let create_sql = self.create_table_ctas(table_name, location);

                eprintln!("[databricks-adapter] create_table '{table_name}': {create_sql}");

                self.execute_sql_statement(&create_sql)
                    .await
                    .map_err(|e| format!("Failed to create table '{table_name}': {e}"))?;

                table_locations.insert(table_name.clone(), location.to_string());
                created_tables.push(table_name.clone());
            }

            self.ensure_notebook(&scenario_slug, &table_locations)
                .await
                .map_err(|e| format!("Failed to upload sync notebook: {e}"))?;

            self.ensure_notebook_sync_job(&scenario_slug)
                .await
                .map_err(|e| format!("Failed to create scheduled notebook sync job: {e}"))?;
        } else {
            eprintln!("[databricks-adapter] Initialization for adbc sink");

            let create_schema_sql = format!(
                "CREATE SCHEMA IF NOT EXISTS {}.{}",
                Self::quoted_identifier(&self.config.catalog),
                Self::quoted_identifier(&self.config.schema)
            );

            eprintln!("[databricks-adapter] Initialize schema: {create_schema_sql}");

            self.execute_sql_statement(&create_schema_sql)
                .await
                .map_err(|e| format!("Failed to initialize schema: {e}"))?;

            // Create managed UC tables (sources for synced tables) via SQL Warehouse.
            for (table_name, dataset_cfg) in &datasets {
                let ddl = self
                    .create_table_ddl(table_name, dataset_cfg, TableFormat::Delta)
                    .map_err(|e| {
                        format!("Failed to build DDL for managed table '{table_name}': {e}")
                    })?;
                eprintln!("[databricks-adapter] creating managed table '{table_name}': {ddl}");
                self.execute_sql_statement(&ddl)
                    .await
                    .map_err(|e| format!("Failed to create managed table '{table_name}': {e}"))?;
                created_tables.push(table_name.clone());
            }
        }

        // Variant-specific post-processing.
        match variant {
            DatabricksVariant::Databricks => {}
            DatabricksVariant::Lakebase => {
                eprintln!("[databricks-adapter] Waiting 2 minutes for schema to initialize");
                std::thread::sleep(Duration::from_secs(120));

                let lakebase_config = match &self.config.compute_target {
                    ComputeTarget::Lakebase(cfg) => cfg,
                    _ => {
                        return Err("create_synced_table called without Lakebase compute target"
                            .to_string());
                    }
                };

                let create_schema_sql = format!(
                    "CREATE SCHEMA IF NOT EXISTS {}.{}",
                    Self::quoted_identifier(&self.config.catalog),
                    Self::quoted_identifier(&lakebase_config.schema),
                );

                eprintln!("[databricks-adapter] Initialize schema: {create_schema_sql}");

                self.execute_sql_statement(&create_schema_sql)
                    .await
                    .map_err(|e| format!("Failed to initialize schema': {e}"))?;

                // Parallel synced table creation + wait for ONLINE
                let this = &*self;
                let sync_futs: Vec<_> = datasets
                    .iter()
                    .map(|(table_name, dataset_cfg)| {
                        let table_name = table_name.clone();
                        let pks = dataset_cfg.primary_key_columns.clone();
                        async move {
                            let alter_table_sql = format!(
                                "ALTER TABLE {}.{}.{} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)",
                                Self::quoted_identifier(&this.config.catalog),
                                Self::quoted_identifier(&this.config.schema),
                                Self::quoted_identifier(&table_name),
                            );

                            eprintln!("[databricks-adapter] alter table: {alter_table_sql}");

                            this.execute_sql_statement(&alter_table_sql).await.map_err(|e| {
                                format!("Failed to alter table '{table_name}': {e}")
                            })?;

                            this.create_synced_table(&table_name, &pks)
                                .await
                                .map_err(|e| {
                                    format!(
                                        "Failed to create synced table for '{table_name}': {e}"
                                    )
                                })?;
                            Ok::<_, String>(table_name)
                        }
                    })
                    .collect();

                created_tables = futures::future::try_join_all(sync_futs).await?;

                // Reset PG cumulative statistics so metrics reflect only this run.
                self.reset_lakebase_pg_stats()
                    .await
                    .map_err(|e| format!("Failed to reset Lakebase PG stats: {e}"))?;
            }
        }

        if let Some(state) = self.runs.get_mut(&run_id) {
            state.created_tables = created_tables;
        }

        match &self.config.compute_target {
            // For Lakebase, return the Databricks ADBC driver for ingestion and PostgreSQL driver for reading.
            ComputeTarget::Lakebase(_) => {
                let pg_uri = self
                    .lakebase_pg_uri()
                    .await
                    .map_err(|e| format!("Failed to build Lakebase PostgreSQL URI: {e}"))?;
                Ok(SetupResponse {
                    driver: AdbcDriver::Databricks,
                    db_kwargs: HashMap::from([
                        ("uri".to_string(), Value::String(self.databricks_uri())),
                        (
                            "databricks.staging.volume_path".to_string(),
                            Value::String(self.config.staging_volume_path.clone()),
                        ),
                    ]),
                    catalog_namespace: None,
                    read_driver: Some((
                        AdbcDriver::Postgresql,
                        HashMap::from([("uri".to_string(), Value::String(pg_uri))]),
                    )),
                })
            }
            // For other variants, return a single Databricks ADBC driver.
            _ => Ok(SetupResponse {
                driver: AdbcDriver::Databricks,
                db_kwargs: HashMap::from([
                    ("uri".to_string(), Value::String(self.databricks_uri())),
                    (
                        "databricks.staging.volume_path".to_string(),
                        Value::String(self.config.staging_volume_path.clone()),
                    ),
                ]),
                catalog_namespace: Some(format!("{}.{}", self.config.catalog, self.config.schema)),
                read_driver: None,
            }),
        }
    }

    async fn teardown(&mut self, run_id: Uuid) -> std::result::Result<TeardownResponse, String> {
        eprintln!("[databricks-adapter] teardown: run_id={run_id}");

        let Some(state) = self.runs.remove(&run_id) else {
            return Ok(TeardownResponse { ok: true });
        };

        // 1. Delete the sync job if it exists.
        let job_name = Self::job_name_for_scenario(&state.scenario_slug);
        match self.find_job_id_by_name(&job_name).await {
            Ok(Some(job_id)) => {
                self.delete_job(job_id).await.map_err(|e| {
                    format!("Failed to delete sync job '{job_name}' (id={job_id}): {e}")
                })?;
            }
            Ok(None) => {
                eprintln!("[databricks-adapter] no sync job to delete: job_name={job_name}");
            }
            Err(e) => {
                eprintln!(
                    "[databricks-adapter] warning: failed to look up sync job '{job_name}': {e}"
                );
            }
        }

        // 2. Delete the notebook if it exists.
        let notebook_path = Self::notebook_path_for_scenario(&state.scenario_slug);
        match self.find_notebook(&notebook_path).await {
            Ok(true) => {
                self.delete_notebook(&notebook_path)
                    .await
                    .map_err(|e| format!("Failed to delete notebook '{notebook_path}': {e}"))?;
            }
            Ok(false) => {
                eprintln!("[databricks-adapter] no notebook to delete: path={notebook_path}");
            }
            Err(e) => {
                eprintln!(
                    "[databricks-adapter] warning: failed to look up notebook '{notebook_path}': {e}"
                );
            }
        }

        // 3. Clean up tables.
        match state.variant {
            DatabricksVariant::Lakebase => {
                let lakebase_config = match &self.config.compute_target {
                    ComputeTarget::Lakebase(cfg) => cfg,
                    _ => {
                        return Err("Lakebase variant requires Lakebase compute target".to_string());
                    }
                };
                for table_name in &state.created_tables {
                    // a) Delete synced table from Lakebase (via synced tables API).
                    self.delete_synced_table(table_name).await.map_err(|e| {
                        format!("Failed to delete synced table '{table_name}': {e}")
                    })?;

                    // b) Drop the synced table's UC catalog entry (lakebase schema).
                    let synced_full = format!(
                        "{}.{}.{}",
                        Self::quoted_identifier(&self.config.catalog),
                        Self::quoted_identifier(&lakebase_config.schema),
                        Self::quoted_identifier(table_name)
                    );
                    if let Err(e) = self
                        .execute_sql_statement(&format!("DROP TABLE IF EXISTS {synced_full}"))
                        .await
                    {
                        eprintln!(
                            "[databricks-adapter] warning: failed to drop synced table UC entry '{table_name}': {e}"
                        );
                    }

                    // c) Drop the managed source table (adapter schema).
                    let sql = format!("DROP TABLE IF EXISTS {}", self.table_full_name(table_name));
                    self.execute_sql_statement(&sql)
                        .await
                        .map_err(|e| format!("Failed to drop managed table '{table_name}': {e}"))?;
                }

                // Drop tables directly from Lakebase PG.
                self.delete_lakebase_pg_tables(&state.created_tables, lakebase_config)
                    .await
                    .map_err(|e| format!("Failed to delete Lakebase PG tables: {e}"))?;

                eprintln!(
                    "[databricks-adapter] cleaned up {} table(s)",
                    state.created_tables.len()
                );
            }
            DatabricksVariant::Databricks => {
                if self.config.drop_tables_on_teardown {
                    for table_name in &state.created_tables {
                        let sql =
                            format!("DROP TABLE IF EXISTS {}", self.table_full_name(table_name));
                        self.execute_sql_statement(&sql)
                            .await
                            .map_err(|e| format!("Failed to drop table '{table_name}': {e}"))?;
                    }
                    eprintln!(
                        "[databricks-adapter] cleaned up {} table(s)",
                        state.created_tables.len()
                    );
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

    async fn metrics(
        &mut self,
        run_id: Uuid,
        final_scrape: bool,
    ) -> std::result::Result<MetricsResponse, String> {
        match &self.config.compute_target {
            ComputeTarget::SqlWarehouse => {
                let info = self
                    .get_warehouse_info()
                    .await
                    .map_err(|e| format!("Failed to get warehouse info: {e}"))?;

                eprintln!("[databricks-adapter] SUT metrics: warehouse_info={info:?}");

                let mut resource = ResourceMetrics {
                    num_compute_nodes: info.num_clusters,
                    ..Default::default()
                };

                let ingestion = IngestionMetrics {
                    active_connections: info.num_active_sessions,
                    ..Default::default()
                };

                // Get run start time
                let started_at_ms = self.runs.get(&run_id).map(|s| s.started_at_ms).unwrap_or(0);

                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                if final_scrape {
                    // Fire marker query and wait for it to appear in Query History.
                    // Once visible, all earlier queries from this warehouse are also available.
                    let marker_tag = format!("{run_id}_{now_ms}");
                    if let Err(e) = self.fire_marker_and_wait(&marker_tag).await {
                        eprintln!("[databricks-adapter] warning: marker wait failed: {e}");
                    }
                }

                // Sum read_bytes and write_bytes from all queries since the run started.
                // On periodic scrapes this is best-effort (Query History has ~5 min lag).
                // On final scrape the marker wait above ensures completeness.
                match self.sum_query_history_io(started_at_ms).await {
                    Ok((total_read, total_write)) => {
                        eprintln!(
                            "[databricks-adapter] query history totals: read_bytes={total_read} write_remote_bytes={total_write}"
                        );
                        resource.disk_read_bytes = Some(total_read);
                        resource.disk_write_bytes = Some(total_write);
                    }
                    Err(e) => {
                        eprintln!(
                            "[databricks-adapter] warning: query history I/O sum failed: {e}"
                        );
                    }
                }

                Ok(MetricsResponse {
                    resource,
                    ingestion,
                })
            }
            ComputeTarget::Lakebase(_) => {
                match self.scrape_lakebase_pg_metrics().await {
                    Ok((resource, ingestion)) => Ok(MetricsResponse {
                        resource,
                        ingestion,
                    }),
                    Err(e) => {
                        eprintln!("[databricks-adapter] warning: lakebase metrics scrape failed: {e}");
                        Ok(MetricsResponse::default())
                    }
                }
            }
            _ => Ok(MetricsResponse::default()),
        }
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
