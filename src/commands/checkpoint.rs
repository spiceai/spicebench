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
use data_generation::dataset::Dataset;
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

#[cfg(feature = "duckdb")]
fn extract_row_count_value(batches: &[RecordBatch]) -> anyhow::Result<usize> {
    let batch = batches
        .first()
        .ok_or_else(|| anyhow::anyhow!("row count query returned no batches"))?;
    if batch.num_rows() == 0 || batch.num_columns() == 0 {
        anyhow::bail!("row count query returned an empty result set");
    }

    let value =
        test_framework::queries::validation::array_value_to_string(batch.column(0).as_ref(), 0)?
            .ok_or_else(|| anyhow::anyhow!("row count query returned NULL"))?;

    value
        .parse::<usize>()
        .map_err(|e| anyhow::anyhow!("failed to parse row count '{value}': {e}"))
}

#[cfg(feature = "duckdb")]
async fn write_checkpoint_row_counts(
    sink: &DuckDBSink,
    table_names: &[String],
    checkpoint_dir: &Path,
    checkpoint_idx: usize,
) -> anyhow::Result<()> {
    let resolved_checkpoint_dir = checkpoint_dir.join(checkpoint_idx.to_string());
    fs::create_dir_all(&resolved_checkpoint_dir)?;

    let mut row_counts = std::collections::BTreeMap::new();
    for table_name in table_names {
        let sql = format!("SELECT COUNT(*) AS row_count FROM \"{table_name}\"");
        let batches = sink.query(&sql).await?;
        let row_count = extract_row_count_value(&batches)?;
        row_counts.insert(table_name.clone(), row_count);
    }

    let out_path = resolved_checkpoint_dir.join("row_counts.json");
    fs::write(&out_path, serde_json::to_vec_pretty(&row_counts)?)?;

    tracing::info!(
        checkpoint = checkpoint_idx,
        tables = row_counts.len(),
        path = %out_path.display(),
        "Checkpoint table row counts written"
    );

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
    // The checkpoint command replays all ETL steps from scratch, so a pre-existing
    // DuckDB file or checkpoint directory would silently accumulate stale data.
    if args.duckdb_path.exists() {
        tracing::info!(path = %args.duckdb_path.display(), "Removing existing DuckDB file");
        std::fs::remove_file(&args.duckdb_path)?;
    }
    let wal_path = args.duckdb_path.with_extension("duckdb.wal");
    if wal_path.exists() {
        std::fs::remove_file(&wal_path)?;
    }
    if args.checkpoint_dir.exists() {
        tracing::info!(path = %args.checkpoint_dir.display(), "Removing existing checkpoint directory");
        std::fs::remove_dir_all(&args.checkpoint_dir)?;
    }

    let scenario_name = args.scenario.to_string();
    let query_set = args.scenario.load_query_set()?;
    let checkpoint_queries: Vec<String> = query_set
        .get_queries(None, None, None)
        .await?
        .iter()
        .map(|q| q.sql.to_string())
        .collect();

    // Download (or extract from local archive) to a temporary directory.
    let extract_dir = tempfile::tempdir()?;
    let version_prefix = build_version_prefix(&args.prefix, &scenario_name, &args.version);

    if let Some(local_archive) = &args.etl_source_archive {
        tracing::info!(archive = %local_archive, "Extracting local archive (skipping S3)");
        let archive_path = std::path::Path::new(local_archive);
        anyhow::ensure!(
            archive_path.exists(),
            "Local archive not found: {local_archive}"
        );
        data_generation::archive::extract_archive(archive_path, extract_dir.path())?;
    } else {
        let bucket = args.bucket.as_deref().ok_or_else(|| {
            anyhow::anyhow!("--bucket is required when --etl-source-archive is not set")
        })?;
        let source_config = TargetConfig {
            bucket: bucket.to_string(),
            prefix: version_prefix.clone(),
            region: args.region.clone(),
            endpoint: args.endpoint.clone(),
            partition_columns: vec![],
        };
        let archive_storage: Arc<dyn DataStorage> = Arc::new(S3Storage::new(&source_config)?);
        ETLPipeline::download(archive_storage, extract_dir.path()).await?;
    }

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
    let dataset = dataset_source.create(&dataset_config, &mutations, Arc::clone(&source))?;
    let mut table_names: Vec<String> = dataset.tables().keys().cloned().collect();
    table_names.sort();

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
        bucket = args.bucket.as_deref().unwrap_or("(local)"),
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

    let interval = args.checkpoint_interval_steps as usize;
    pipeline.initialize().await?;
    if mutations.bootstrap {
        // Phase identically to the bootstrap run so checkpoint indices align:
        // checkpoint 0 = the full base (all base steps), then one checkpoint every
        // `interval` mutation steps.
        let base_remainder = usize::from(version_metadata.num_steps).saturating_sub(1);
        pipeline.run(base_remainder.max(1)).await?;
    } else {
        pipeline.run(interval).await?;
    }

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
                write_checkpoint_row_counts(
                    &target,
                    &table_names,
                    &args.checkpoint_dir,
                    checkpoint_idx,
                )
                .await?;
                checkpoint_idx += 1;

                // Bootstrap: after the full-base checkpoint (cp0), switch the pause
                // cadence to the mutation checkpoint interval.
                if mutations.bootstrap {
                    pipeline.set_batch_budget(interval);
                }
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
                write_checkpoint_row_counts(
                    &target,
                    &table_names,
                    &args.checkpoint_dir,
                    checkpoint_idx,
                )
                .await?;

                // Skip S3 upload when using a local archive — write the
                // manifest directly to checkpoint_dir/checkpoints.json instead.
                if args.etl_source_archive.is_none() {
                    let checkpoint_store = CheckpointStore::new(
                        args.bucket.as_deref().unwrap_or_default(),
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
                } else {
                    CheckpointStore::write_local_manifest(
                        &scenario_name,
                        &args.checkpoint_dir,
                        args.checkpoint_interval_steps as usize,
                    )?;
                    tracing::info!(
                        dir = %args.checkpoint_dir.display(),
                        "Local archive mode — manifest written to checkpoint dir"
                    );
                }

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
