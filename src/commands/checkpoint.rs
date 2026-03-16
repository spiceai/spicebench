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
use std::path::Path;
#[cfg(feature = "duckdb")]
use std::sync::Arc;

#[cfg(feature = "duckdb")]
use arrow::array::RecordBatch;
#[cfg(feature = "duckdb")]
use checkpointer::CheckpointStore;
#[cfg(feature = "duckdb")]
use data_generation::config::{TargetConfig, build_version_prefix};
#[cfg(feature = "duckdb")]
use data_generation::storage::DataStorage;
#[cfg(feature = "duckdb")]
use data_generation::storage::file::FileStorage;
#[cfg(feature = "duckdb")]
use data_generation::storage::s3::S3Storage;
#[cfg(feature = "duckdb")]
use etl::sink::duckdb::DuckDBSink;
#[cfg(feature = "duckdb")]
use etl::{DatasetSource, ETLPipeline, PipelineState, StopReason};
#[cfg(feature = "duckdb")]
use parquet::arrow::ArrowWriter;

use test_framework::anyhow;

use crate::args::checkpoint::CheckpointArgs;

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

pub async fn execute(args: &CheckpointArgs) -> anyhow::Result<()> {
    #[cfg(not(feature = "duckdb"))]
    {
        let _ = args;
        anyhow::bail!(
            "The checkpoint command requires the `duckdb` feature. Please rebuild with `--features duckdb`."
        );
    }

    #[cfg(feature = "duckdb")]
    execute_duckdb(args).await
}

#[cfg(feature = "duckdb")]
async fn execute_duckdb(args: &CheckpointArgs) -> anyhow::Result<()> {
    let scenario_name = args.scenario.to_string();
    let query_set = args.scenario.load_query_set()?;
    let checkpoint_queries: Vec<String> = query_set
        .get_queries(None, None, None)
        .await?
        .iter()
        .map(|q| q.sql.to_string())
        .collect();

    let version_prefix = build_version_prefix(&args.prefix, &scenario_name, &args.version);
    let source_config = TargetConfig {
        bucket: args.bucket.clone(),
        prefix: version_prefix.clone(),
        region: args.region.clone(),
        endpoint: args.endpoint.clone(),
        partition_columns: vec![],
    };
    let archive_storage: Arc<dyn DataStorage> = Arc::new(S3Storage::new(&source_config)?);

    // Download and extract the archive to a temporary local directory.
    let extract_dir = tempfile::tempdir()?;
    ETLPipeline::download(archive_storage, extract_dir.path()).await?;
    let source: Arc<dyn DataStorage> = Arc::new(FileStorage::new(extract_dir.path()));

    // Read version metadata to derive dataset config and mutations.
    let version_metadata = source.read_version_metadata().await?.ok_or_else(|| {
        anyhow::anyhow!(
            "No version.json found in extracted data at {}. Was data generation run for this version?",
            extract_dir.path().display()
        )
    })?;

    let dataset_source = DatasetSource::from_dataset_type(&version_metadata.dataset_type)?;
    let dataset_config = version_metadata.dataset_config();
    let mutations = version_metadata.mutation_config();
    let target = Arc::new(DuckDBSink::new(&args.duckdb_path)?);
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
        version = %args.version,
        dataset = %version_metadata.dataset_type,
        bucket = %args.bucket,
        prefix = %args.prefix,
        version_prefix = %version_prefix,
        extract_dir = %extract_dir.path().display(),
        duckdb_path = %args.duckdb_path.display(),
        scale_factor = version_metadata.scale_factor,
        num_steps = version_metadata.num_steps,
        checkpoint_interval = args.checkpoint_interval_steps,
        checkpoint_dir = %args.checkpoint_dir.display(),
        "Starting Checkpointer"
    );

    pipeline.initialize().await?;
    pipeline
        .run(args.checkpoint_interval_steps as usize)
        .await?;

    let mut checkpoint_idx: usize = 0;

    loop {
        let state = pipeline.wait().await;

        match state {
            PipelineState::Paused => {
                tracing::info!(
                    checkpoint = checkpoint_idx,
                    "Pipeline paused, running checkpoint queries"
                );
                run_checkpoint_queries(
                    &target,
                    &checkpoint_queries,
                    &args.checkpoint_dir,
                    checkpoint_idx,
                )
                .await?;
                checkpoint_idx += 1;

                pipeline.continue_pipeline()?;
            }
            PipelineState::Stopped(StopReason::Completed) => {
                tracing::info!(
                    checkpoint = checkpoint_idx,
                    "Pipeline completed, running final checkpoint queries"
                );
                run_checkpoint_queries(
                    &target,
                    &checkpoint_queries,
                    &args.checkpoint_dir,
                    checkpoint_idx,
                )
                .await?;

                let checkpoint_store = CheckpointStore::new(
                    &args.bucket,
                    &version_prefix,
                    args.region.as_deref(),
                    args.endpoint.as_deref(),
                )?;
                checkpoint_store
                    .upload_checkpoints(
                        &scenario_name,
                        &args.checkpoint_dir,
                        args.checkpoint_interval_steps as usize,
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

    Ok(())
}
