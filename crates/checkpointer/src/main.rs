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

#[cfg(feature = "duckdb")]
use std::fs;
#[cfg(feature = "duckdb")]
use std::path::{Path, PathBuf};
#[cfg(feature = "duckdb")]
use std::sync::Arc;

#[cfg(feature = "duckdb")]
use arrow::array::RecordBatch;
#[cfg(feature = "duckdb")]
use checkpointer::CheckpointStore;
#[cfg(feature = "duckdb")]
use clap::Parser;
#[cfg(feature = "duckdb")]
use data_generation::config::{TargetConfig, build_version_prefix};
#[cfg(feature = "duckdb")]
use data_generation::storage::DataStorage;
#[cfg(feature = "duckdb")]
use data_generation::storage::s3::S3Storage;
#[cfg(feature = "duckdb")]
use etl::sink::duckdb::DuckDBSink;
#[cfg(feature = "duckdb")]
use etl::{DatasetSource, ETLPipeline, PipelineState, StopReason};
#[cfg(feature = "duckdb")]
use parquet::arrow::ArrowWriter;
#[cfg(feature = "duckdb")]
use test_framework::Scenario;
use tracing_subscriber::EnvFilter;

#[cfg(feature = "duckdb")]
#[derive(Parser)]
#[command(
    about = "Run an ETL pipeline that reads from S3, rehydrates data, and writes directly to a SUT via ADBC"
)]
struct Cli {
    /// The scenario to run, which determines the dataset type and checkpoint queries.
    #[arg(long, value_enum, default_value = "tpch")]
    scenario: Scenario,

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

#[cfg(feature = "duckdb")]
impl Cli {
    /// Builds the source config with the versioned prefix:
    /// `{prefix}/{scenario}/{version}`
    fn source_config(&self) -> TargetConfig {
        let version_prefix =
            build_version_prefix(&self.prefix, &self.scenario.to_string(), &self.version);
        TargetConfig {
            bucket: self.bucket.clone(),
            prefix: version_prefix,
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
        }
    }
}

/// Run all checkpoint queries against the DuckDB sink and write each result
/// set to a parquet file at `<checkpoint_dir>/<checkpoint_idx>/<query_idx>.parquet`.
#[cfg(feature = "duckdb")]
async fn run_checkpoint_queries(
    sink: &DuckDBSink,
    checkpoint_queries: &[String],
    checkpoint_dir: &Path,
    checkpoint_idx: usize,
) -> anyhow::Result<()> {
    let resolved_checkpoint_dir = checkpoint_dir.join(checkpoint_idx.to_string());
    fs::create_dir_all(&resolved_checkpoint_dir)?;

    for (query_idx, sql) in checkpoint_queries.iter().enumerate() {
        tracing::info!(
            checkpoint = checkpoint_idx,
            query = query_idx,
            "Running checkpoint query"
        );

        let batches = sink.query(sql).await?;
        let out_path = resolved_checkpoint_dir.join(format!("{query_idx}.parquet"));

        // Derive the result schema. If the query returned rows, use the first
        // batch's schema. Otherwise, ask DuckDB for the schema directly — this
        // works even when zero rows are returned.
        let result_schema = if let Some(first) = batches.first() {
            first.schema()
        } else {
            sink.query_schema(sql).await?
        };

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        write_batches_to_parquet(&batches, &out_path, &result_schema)?;

        tracing::info!(
            checkpoint = checkpoint_idx,
            query = query_idx,
            rows = total_rows,
            path = %out_path.display(),
            "Checkpoint query result written"
        );
    }

    Ok(())
}

/// Write a slice of `RecordBatch`es to a single parquet file.
///
/// If `batches` is empty (the query returned zero rows) an empty parquet file
/// containing only the schema from `result_schema` is written.
#[cfg(feature = "duckdb")]
fn write_batches_to_parquet(
    batches: &[RecordBatch],
    path: &Path,
    result_schema: &arrow::datatypes::SchemaRef,
) -> anyhow::Result<()> {
    let file = fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(result_schema), None)?;
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

    #[cfg(not(feature = "duckdb"))]
    {
        tracing::error!(
            "The Checkpointer currently only supports DuckDB as a sink. Please re-run with the `duckdb` feature enabled."
        );
    }

    #[cfg(feature = "duckdb")]
    {
        let cli = Cli::parse();

        let scenario_name = cli.scenario.to_string();
        let query_set = cli.scenario.load_query_set()?;
        let checkpoint_queries: Vec<String> = query_set
            .get_queries(None, None, None)
            .await?
            .iter()
            .map(|q| q.sql.to_string())
            .collect();

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
        let target = Arc::new(DuckDBSink::new(&cli.duckdb_path)?);
        let target_sink: Arc<dyn etl::sink::Sink> = Arc::clone(&target) as Arc<dyn etl::sink::Sink>;

        let mut pipeline = ETLPipeline::new(
            dataset_source,
            &dataset_config,
            source,
            target_sink,
            &mutations,
        )?;

        tracing::info!(
            scenario = %scenario_name,
            version = %cli.version,
            dataset = %version_metadata.dataset_type,
            bucket = %cli.bucket,
            prefix = %cli.prefix,
            version_prefix = %version_prefix,
            duckdb_path = %cli.duckdb_path.display(),
            scale_factor = version_metadata.scale_factor,
            num_steps = version_metadata.num_steps,
            checkpoint_interval = cli.checkpoint_interval_steps,
            checkpoint_dir = %cli.checkpoint_dir.display(),
            "Starting Checkpointer"
        );

        pipeline.initialize().await?;
        pipeline.run(cli.checkpoint_interval_steps as usize).await?;

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
                    run_checkpoint_queries(
                        &target,
                        &checkpoint_queries,
                        &cli.checkpoint_dir,
                        checkpoint_idx,
                    )
                    .await?;
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
                    run_checkpoint_queries(
                        &target,
                        &checkpoint_queries,
                        &cli.checkpoint_dir,
                        checkpoint_idx,
                    )
                    .await?;

                    // Upload all checkpoints to S3 under the version prefix.
                    let checkpoint_store = CheckpointStore::new(
                        &cli.bucket,
                        &version_prefix,
                        cli.region.as_deref(),
                        cli.endpoint.as_deref(),
                    )?;
                    checkpoint_store
                        .upload_checkpoints(
                            &scenario_name,
                            &cli.checkpoint_dir,
                            cli.checkpoint_interval_steps as usize,
                        )
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
    }

    Ok(())
}
