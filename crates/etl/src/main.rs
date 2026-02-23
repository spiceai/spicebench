/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::sync::Arc;

use clap::Parser;
use data_generation::config::{TargetConfig, build_version_prefix};
use data_generation::storage::DataStorage;
use data_generation::storage::s3::S3Storage;
use etl::sink::adbc::AdbcSink;
use etl::sink::s3_hive::S3HiveSink;
use etl::sink::Sink;
use etl::{DatasetSource, ETLPipeline, PipelineState, StopReason};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    about = "Run an ETL pipeline that reads from S3, rehydrates data, and writes to either S3 Hive Parquet or an ADBC target"
)]
struct Cli {
    /// Scenario name (e.g. "tpch") — used in the storage path `{prefix}/{scenario}/{version}/`
    #[arg(long, default_value = "tpch")]
    scenario: String,

    /// Version identifier for the data generation to read from.
    #[arg(long)]
    version: String,

    /// S3 bucket name (used for both source and target)
    #[arg(long)]
    bucket: String,

    /// S3 key prefix (the `{prefix}` portion of `{prefix}/{scenario}/{version}/`)
    #[arg(long, default_value = "")]
    prefix: String,

    /// AWS region
    #[arg(long)]
    region: Option<String>,

    /// S3 endpoint URL (for MinIO/LocalStack)
    #[arg(long)]
    endpoint: Option<String>,

    /// Base S3 key prefix for the ETL target (hive-partitioned output).
    /// Defaults to the source prefix if not specified.
    #[arg(long, default_value = "")]
    target_prefix: String,

    /// Ordered list of columns used for hive-style partitioning.
    ///
    /// Example: `--partition-by __created_at,product_type`
    #[arg(long, value_delimiter = ',', default_value = "__created_at")]
    partition_by: Vec<String>,

    /// ADBC driver name (for example: "databricks" or "flightsql").
    /// Provide with `--adbc-uri` to write to an ADBC target.
    #[arg(long)]
    adbc_driver: Option<String>,

    /// Connection URI passed as ADBC database option `uri`.
    /// Provide with `--adbc-driver` to write to an ADBC target.
    #[arg(long)]
    adbc_uri: Option<String>,

    /// Optional target database schema for bulk ingest
    #[arg(long)]
    adbc_schema: Option<String>,

    /// Additional ADBC database options as `key=value`.
    ///
    /// May be specified multiple times.
    /// Example: `--adbc-option username=token --adbc-option password=...`
    #[arg(long = "adbc-option")]
    adbc_options: Vec<String>,
}

impl Cli {
    /// Builds the source config with the versioned prefix:
    /// `{prefix}/{scenario}/{version}`
    fn source_config(&self) -> TargetConfig {
        let version_prefix = build_version_prefix(&self.prefix, &self.scenario, &self.version);
        TargetConfig {
            bucket: self.bucket.clone(),
            prefix: version_prefix,
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
            partition_columns: vec![],
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    let source_config = cli.source_config();
    let version_prefix = source_config.prefix.clone();
    let source = Arc::new(S3Storage::new(&source_config)?);

    // Read version metadata to derive dataset config and mutations.
    let version_metadata = source.read_version_metadata().await?.ok_or_else(|| {
        anyhow::anyhow!(
            "No version.json found at {version_prefix}. Was data generation run for this version?"
        )
    })?;

    let dataset_source = DatasetSource::from_dataset_type(&version_metadata.dataset_type)?;
    let dataset_config = version_metadata.dataset_config();
    let mutations = version_metadata.mutation_config();

    let (target, target_config, target_kind): (Arc<dyn Sink>, Option<TargetConfig>, String) =
        match (&cli.adbc_driver, &cli.adbc_uri) {
            (Some(driver), Some(uri)) => {
                let mut db_kwargs = std::collections::HashMap::new();
                db_kwargs.insert("uri".to_string(), serde_json::Value::String(uri.clone()));

                for option in &cli.adbc_options {
                    let (key, value) = option.split_once('=').ok_or_else(|| {
                        anyhow::anyhow!(
                            "Invalid --adbc-option '{option}'. Expected key=value"
                        )
                    })?;

                    let key = key.trim();
                    if key.is_empty() {
                        anyhow::bail!(
                            "Invalid --adbc-option '{option}'. Option key cannot be empty"
                        );
                    }

                    db_kwargs.insert(
                        key.to_string(),
                        serde_json::Value::String(value.to_string()),
                    );
                }

                (
                    Arc::new(AdbcSink::new(driver, db_kwargs, cli.adbc_schema.clone())?),
                    None,
                    "adbc".to_string(),
                )
            }
            (None, None) => {
                let hive_prefix = if cli.target_prefix.is_empty() {
                    format!(
                        "{}/{}/{}",
                        cli.prefix.trim_matches('/'),
                        cli.scenario,
                        cli.version
                    )
                } else {
                    format!(
                        "{}/{}/{}",
                        cli.target_prefix.trim_matches('/'),
                        cli.scenario,
                        cli.version
                    )
                };

                let hive_config = TargetConfig {
                    bucket: cli.bucket.clone(),
                    prefix: hive_prefix,
                    region: cli.region.clone(),
                    endpoint: cli.endpoint.clone(),
                    partition_columns: cli.partition_by.clone(),
                };

                (
                    Arc::new(S3HiveSink::new(&hive_config)?),
                    Some(hive_config),
                    "s3-hive".to_string(),
                )
            }
            _ => {
                anyhow::bail!(
                    "ADBC target requires both --adbc-driver and --adbc-uri. Omit both to use the S3 Hive sink."
                );
            }
        };

    let mut pipeline = ETLPipeline::new(dataset_source, &dataset_config, source, target, &mutations)?;
    if let Some(target_config) = target_config {
        pipeline = pipeline.with_target_config(target_config);
    }

    tracing::info!(
        scenario = %cli.scenario,
        version = %cli.version,
        dataset = %version_metadata.dataset_type,
        bucket = %cli.bucket,
        prefix = %cli.prefix,
        target = %target_kind,
        adbc_driver = ?cli.adbc_driver,
        adbc_schema = ?cli.adbc_schema,
        target_prefix = %cli.target_prefix,
        partition_by = ?cli.partition_by,
        scale_factor = version_metadata.scale_factor,
        num_steps = version_metadata.num_steps,
        "Starting ETL pipeline"
    );

    pipeline.initialize().await?;
    pipeline.start().await?;

    let final_state = pipeline.wait().await;
    match &final_state {
        PipelineState::Stopped(StopReason::Completed) => {
            tracing::info!("ETL pipeline completed successfully");
        }
        PipelineState::Stopped(StopReason::Cancelled) => {
            tracing::warn!("ETL pipeline was cancelled");
        }
        PipelineState::Stopped(StopReason::Error(e)) => {
            tracing::error!(error = %e, "ETL pipeline stopped with error");
            anyhow::bail!("ETL pipeline failed: {e}");
        }
        other => {
            anyhow::bail!("Unexpected final pipeline state: {other:?}");
        }
    }

    Ok(())
}
