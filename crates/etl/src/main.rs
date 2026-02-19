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
use data_generation::config::{DatasetConfig, TargetConfig};
use data_generation::dataset::MutationConfig;
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
    /// Dataset type: "tpch" or "simple_sequence"
    #[arg(long, default_value = "tpch")]
    dataset: String,

    /// Scale factor for data generation
    #[arg(long, default_value_t = 1.0)]
    scale_factor: f64,

    /// Number of data generation steps (partitions)
    #[arg(long, default_value_t = 25)]
    num_steps: u16,

    /// S3 bucket name (used for both source and target)
    #[arg(long)]
    bucket: String,

    /// S3 key prefix for source data
    #[arg(long, default_value = "")]
    source_prefix: String,
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
}

impl Cli {
    fn dataset_source(&self) -> anyhow::Result<DatasetSource> {
        match self.dataset.as_str() {
            "tpch" => Ok(DatasetSource::Tpch),
            "simple_sequence" => Ok(DatasetSource::SimpleSequence),
            other => {
                anyhow::bail!("Unknown dataset type: {other}. Use 'tpch' or 'simple_sequence'.")
            }
        }
    }

    fn dataset_config(&self) -> DatasetConfig {
        DatasetConfig {
            dataset_type: self.dataset.clone(),
            scale_factor: self.scale_factor,
            num_steps: self.num_steps,
        }
    }

    fn source_config(&self) -> TargetConfig {
        TargetConfig {
            bucket: self.bucket.clone(),
            prefix: self.source_prefix.clone(),
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

    let dataset_source = cli.dataset_source()?;
    let dataset_config = cli.dataset_config();

    let source = Arc::new(S3Storage::new(&cli.source_config())?);

    let adbc_conn = AdbcConnection::create(
        &cli.adbc_driver,
        std::collections::HashMap::from([("uri".to_string(), Value::String(cli.adbc_uri.clone()))]),
    )?;
    let target = Arc::new(AdbcSink::new(adbc_conn, cli.adbc_schema.clone()));

    let mutations = MutationConfig::new(0.1, 0.1);

    let mut pipeline =
        ETLPipeline::new(dataset_source, &dataset_config, source, target, &mutations)?;

    tracing::info!(
        dataset = %cli.dataset,
        bucket = %cli.bucket,
        source_prefix = %cli.source_prefix,
        adbc_driver = %cli.adbc_driver,
        adbc_schema = ?cli.adbc_schema,
        scale_factor = cli.scale_factor,
        num_steps = cli.num_steps,
        "Starting ETL pipeline"
    );

    // Log the tables and schemas that will be processed.
    let datasets = pipeline.create_tables_request_datasets();
    for (name, config) in &datasets {
        tracing::info!(table = %name, schema = ?config.schema, "Dataset table registered");
    }

    pipeline.initialize().await?;
    pipeline.start()?;

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
