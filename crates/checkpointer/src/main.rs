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

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::RecordBatch;
use checkpointer::CheckpointStore;
use clap::Parser;
use data_generation::config::{DatasetConfig, TargetConfig};
use data_generation::dataset::MutationConfig;
use data_generation::storage::s3::S3Storage;
use etl::sink::duckdb::DuckDBSink;
use etl::{DatasetSource, ETLPipeline, PipelineState, StopReason};
use parquet::arrow::ArrowWriter;
use tracing_subscriber::EnvFilter;

/// Static scenario name used until we derive it from the scenario configuration.
const SCENARIO_NAME: &str = "default";

/// Static list of checkpoint queries to run against the DuckDB database at
/// each checkpoint boundary.
const CHECKPOINT_QUERIES: &[&str] = &[
    "SELECT COUNT(*) AS cnt FROM lineitem",
    "SELECT COUNT(*) AS cnt FROM orders",
    "SELECT COUNT(*) AS cnt FROM customer",
    "SELECT * FROM lineitem ORDER BY l_orderkey LIMIT 1000",
];

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

    /// Path to the local DuckDB database file to sink data into
    #[arg(long)]
    duckdb_path: PathBuf,

    /// Every N steps to take a checkpoint
    #[arg(long, default_value_t = 100)]
    checkpoint_interval_steps: u64,

    /// Directory to write checkpoint parquet files into
    #[arg(long, default_value = "./checkpoints")]
    checkpoint_dir: PathBuf,
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

/// Run all checkpoint queries against the DuckDB sink and write each result
/// set to a parquet file at `<checkpoint_dir>/<checkpoint_idx>/<query_idx>.parquet`.
async fn run_checkpoint_queries(
    sink: &DuckDBSink,
    checkpoint_dir: &Path,
    checkpoint_idx: usize,
) -> anyhow::Result<()> {
    let resolved_checkpoint_dir = checkpoint_dir.join(checkpoint_idx.to_string());
    fs::create_dir_all(&resolved_checkpoint_dir)?;

    for (query_idx, sql) in CHECKPOINT_QUERIES.iter().enumerate() {
        tracing::info!(
            checkpoint = checkpoint_idx,
            query = query_idx,
            sql = %sql,
            "Running checkpoint query"
        );

        let batches = sink.query(sql).await?;
        let out_path = resolved_checkpoint_dir.join(format!("{query_idx}.parquet"));
        write_batches_to_parquet(&batches, &out_path)?;

        tracing::info!(
            checkpoint = checkpoint_idx,
            query = query_idx,
            path = %out_path.display(),
            "Checkpoint query result written"
        );
    }

    Ok(())
}

/// Write a slice of `RecordBatch`es to a single parquet file.
fn write_batches_to_parquet(batches: &[RecordBatch], path: &Path) -> anyhow::Result<()> {
    if batches.is_empty() {
        anyhow::bail!("No record batches to write to parquet");
    }
    let schema = batches[0].schema();
    let file = fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(())
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

    let target = Arc::new(DuckDBSink::new(&cli.duckdb_path)?);
    let target_sink: Arc<dyn etl::sink::Sink> = Arc::clone(&target) as Arc<dyn etl::sink::Sink>;

    let mutations = MutationConfig::new(0.1, 0.1);

    let mut pipeline = ETLPipeline::new(
        dataset_source,
        &dataset_config,
        source,
        target_sink,
        &mutations,
    )?;

    tracing::info!(
        dataset = %cli.dataset,
        bucket = %cli.bucket,
        source_prefix = %cli.source_prefix,
        duckdb_path = %cli.duckdb_path.display(),
        scale_factor = cli.scale_factor,
        num_steps = cli.num_steps,
        checkpoint_interval = cli.checkpoint_interval_steps,
        checkpoint_dir = %cli.checkpoint_dir.display(),
        "Starting Checkpointer"
    );

    // Log the tables and schemas that will be processed.
    let datasets = pipeline.setup_request_datasets();
    for (name, config) in &datasets {
        tracing::info!(table = %name, schema = ?config.schema, "Dataset table registered");
    }

    pipeline.initialize().await?;
    pipeline.run(cli.checkpoint_interval_steps as usize)?;

    let mut checkpoint_idx: usize = 0;

    loop {
        let state = pipeline.wait().await;

        match state {
            PipelineState::Paused => {
                // Pipeline paused after a batch of steps — take a checkpoint.
                tracing::info!(
                    checkpoint = checkpoint_idx,
                    "Pipeline paused, running checkpoint queries"
                );
                run_checkpoint_queries(&target, &cli.checkpoint_dir, checkpoint_idx).await?;
                checkpoint_idx += 1;

                // Resume the pipeline for the next batch of steps.
                pipeline.continue_pipeline()?;
            }
            PipelineState::Stopped(StopReason::Completed) => {
                // Take a final checkpoint at completion.
                tracing::info!(
                    checkpoint = checkpoint_idx,
                    "Pipeline completed, running final checkpoint queries"
                );
                run_checkpoint_queries(&target, &cli.checkpoint_dir, checkpoint_idx).await?;

                // Upload all checkpoints to S3.
                let checkpoint_store = CheckpointStore::new(
                    &cli.bucket,
                    &cli.source_prefix,
                    cli.region.as_deref(),
                    cli.endpoint.as_deref(),
                )?;
                checkpoint_store
                    .upload_checkpoints(SCENARIO_NAME, &cli.checkpoint_dir)
                    .await?;

                tracing::info!("Checkpointer completed successfully");
                break;
            }
            PipelineState::Stopped(StopReason::Cancelled) => {
                tracing::warn!("Checkpointer was cancelled");
                break;
            }
            PipelineState::Stopped(StopReason::Error(e)) => {
                tracing::error!(error = %e, "Checkpointer stopped with error");
                anyhow::bail!("Checkpointer failed: {e}");
            }
            other => {
                anyhow::bail!("Unexpected final pipeline state: {other:?}");
            }
        }
    }

    Ok(())
}
