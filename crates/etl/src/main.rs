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

use adbc_client::AdbcConnection;
use clap::Parser;
use data_generation::config::{TargetConfig, build_version_prefix};
use data_generation::storage::DataStorage;
use data_generation::storage::s3::S3Storage;
use etl::sink::adbc::AdbcSink;
use etl::{DatasetSource, ETLPipeline, PipelineState, StopReason};
use serde_json::Value;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    about = "Run an ETL pipeline that reads from S3, rehydrates data, and writes directly to a SUT via ADBC"
)]
struct Cli {
    /// Scenario name (e.g. "tpch") — used in the storage path `{prefix}/{scenario}/{version}/`
    #[arg(long, default_value = "tpch")]
    scenario: String,

    /// Version identifier for the data generation to read from.
    #[arg(long)]
    version: u64,

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

    /// ADBC driver name (for example: databricks, flightsql)
    #[arg(long)]
    adbc_driver: String,

    /// ADBC connection URI passed as db option `uri`
    #[arg(long)]
    adbc_uri: String,

    /// Optional schema name to prefix destination table names
    #[arg(long)]
    adbc_schema: Option<String>,

    /// Append a `__created_at` timestamp column to every batch written to the sink.
    #[arg(long, default_value_t = false)]
    with_created_at: bool,
}

impl Cli {
    /// Builds the source config with the versioned prefix:
    /// `{prefix}/{scenario}/{version}`
    fn source_config(&self) -> TargetConfig {
        let version_prefix = build_version_prefix(&self.prefix, &self.scenario, self.version);
        TargetConfig {
            bucket: self.bucket.clone(),
            prefix: version_prefix,
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
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

    let adbc_conn = AdbcConnection::create(
        &cli.adbc_driver,
        std::collections::HashMap::from([("uri".to_string(), Value::String(cli.adbc_uri.clone()))]),
    )?;
    let target = Arc::new(AdbcSink::new(adbc_conn, cli.adbc_schema.clone()));

    let mut pipeline =
        ETLPipeline::new(dataset_source, &dataset_config, source, target, &mutations)?
            .with_created_at(cli.with_created_at);

    tracing::info!(
        scenario = %cli.scenario,
        version = cli.version,
        dataset = %version_metadata.dataset_type,
        bucket = %cli.bucket,
        prefix = %cli.prefix,
        adbc_driver = %cli.adbc_driver,
        adbc_schema = ?cli.adbc_schema,
        scale_factor = version_metadata.scale_factor,
        num_steps = version_metadata.num_steps,
        "Starting ETL pipeline"
    );

    // Log the tables and schemas that will be processed.
    let datasets = pipeline.create_tables_request_datasets();
    for (name, config) in &datasets {
        tracing::info!(table = %name, schema = ?config.schema, "Dataset table registered");
    }

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
