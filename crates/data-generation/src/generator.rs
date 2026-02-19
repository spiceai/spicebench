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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::config::IngestorConfig;
use super::dataset::Dataset;
use super::metrics::{IngestResult, Metrics};
use super::storage::DataStorage;
use super::version::{MutationsMetadata, TableMetadata, VersionMetadata, arrow_schema_to_json};

/// Configuration for the version metadata that will be written at the end
/// of a data generation run.
pub struct VersionConfig {
    pub version: u64,
    pub scenario: String,
    pub scale_factor: f64,
    pub num_steps: u16,
    pub dataset_type: String,
    pub update_ratio: f64,
    pub delete_ratio: f64,
}

pub struct DataGenerator {
    dataset: Arc<dyn Dataset>,
    target: Arc<dyn DataStorage>,
    metrics: Metrics,
    semaphore: Arc<Semaphore>,
    version_config: VersionConfig,
}

impl DataGenerator {
    pub fn new(
        dataset: Arc<dyn Dataset>,
        target: Arc<dyn DataStorage>,
        config: &IngestorConfig,
        metrics: Metrics,
        version_config: VersionConfig,
    ) -> Self {
        Self {
            dataset,
            target,
            metrics,
            semaphore: Arc::new(Semaphore::new(config.max_concurrency)),
            version_config,
        }
    }

    /// Seed the target with initial data — writes at least one batch per table.
    ///
    /// Pulls one batch per table from the dataset using `next_batches()`, then writes
    /// them sequentially so the data is guaranteed to be present when this returns.
    pub async fn initialize(&self) -> anyhow::Result<IngestResult> {
        let table_count = self.dataset.tables().len();

        tracing::info!(
            table_count,
            "Initializing target with seed data for all tables"
        );

        match self.dataset.next_batches().await {
            Ok(Some(batches)) => {
                // Write all tables concurrently within this step.
                let mut join_set = JoinSet::new();
                for (table_name, batch) in batches {
                    self.metrics.record_generation(&batch);

                    let target = self.target.clone();
                    let metrics = self.metrics.clone();
                    join_set.spawn(async move {
                        let start = Instant::now();
                        let result = target.write(&table_name, 0, batch).await?;
                        metrics.record_write(&result, start.elapsed());
                        Ok::<_, anyhow::Error>(())
                    });
                }
                while let Some(result) = join_set.join_next().await {
                    result??;
                }
            }
            Ok(None) => {
                tracing::warn!("Dataset exhausted during initialization");
            }
            Err(e) => return Err(e),
        }

        let summary = self.metrics.summary();
        tracing::info!(
            rows = summary.rows_written,
            batches = summary.batches_written,
            "Initialization complete"
        );

        Ok(summary)
    }

    /// Skip the initial batches that `initialize()` would have written.
    ///
    /// Consumes one round of batches (one per table) from the dataset without writing
    /// them to the target. This advances the dataset past the initialization records
    /// so that `run()` only processes new data.
    pub async fn skip_initial_batches(&self) -> anyhow::Result<()> {
        let table_count = self.dataset.tables().len();

        tracing::info!(table_count, "Skipping initial batches for all tables");

        match self.dataset.next_batches().await {
            Ok(Some(_)) => {}
            Ok(None) => {
                tracing::warn!("Dataset exhausted before all tables were skipped");
            }
            Err(e) => return Err(e),
        }

        tracing::info!("Initial batches skipped");

        Ok(())
    }

    /// Ingest the remaining data from the dataset into the target.
    ///
    /// Pulls batches sequentially from the dataset and dispatches writes concurrently,
    /// bounded by the configured `max_concurrency`. This continues from wherever the
    /// dataset was left after `initialize()`.
    pub async fn run(&self) -> anyhow::Result<IngestResult> {
        let mut join_set = JoinSet::new();

        // Spawn periodic metrics logger
        let metrics_logger = self.metrics.clone();
        let logger_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                metrics_logger.log_progress();
            }
        });

        let mut batch_ids = HashMap::new();
        for table in self.dataset.tables().keys() {
            batch_ids.insert(table.clone(), self.dataset.clone().batch_ids(table).await);
        }

        // Track which batch IDs were successfully written per table so we can
        // persist them in the table metadata at the end of the run.
        let written_batch_ids: Arc<std::sync::Mutex<HashMap<String, Vec<u64>>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));

        loop {
            let source_batches = match self.dataset.next_batches().await {
                Ok(Some(batches)) => batches,
                Ok(None) => break,
                Err(e) => {
                    tracing::error!("Dataset error: {e}");
                    break;
                }
            };

            for (table_name, batch) in source_batches {
                self.metrics.record_generation(&batch);

                // Acquire semaphore permit — creates backpressure if all write slots are busy
                let permit = Arc::clone(&self.semaphore).acquire_owned().await?;

                let target = self.target.clone();
                let metrics = self.metrics.clone();
                let next_batch_id = batch_ids
                    .get_mut(&table_name)
                    .and_then(|ids| ids.pop_front())
                    .unwrap_or_else(|| {
                        tracing::warn!(table = %table_name, "No more batch IDs available for this table");
                        0
                    });

                let written_ids = Arc::clone(&written_batch_ids);
                join_set.spawn(async move {
                    let start = Instant::now();
                    match target.write(&table_name, next_batch_id, batch).await {
                        Ok(result) => {
                            metrics.record_write(&result, start.elapsed());
                            written_ids
                                .lock()
                                .expect("written_batch_ids lock poisoned")
                                .entry(table_name)
                                .or_default()
                                .push(next_batch_id);
                        }
                        Err(e) => {
                            metrics.record_error();
                            tracing::error!(batch_id = next_batch_id, "Write failed: {e}");
                        }
                    }
                    drop(permit);
                });
            }
        }

        // Wait for all in-flight writes
        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                tracing::error!("Write task panicked: {e}");
            }
        }

        logger_handle.abort();

        // Build and persist the consolidated version metadata (version.json).
        let written = Arc::try_unwrap(written_batch_ids)
            .expect("all tasks should be finished")
            .into_inner()
            .expect("mutex should not be poisoned");

        let dataset_tables = self.dataset.tables();
        let mut tables_metadata = HashMap::new();
        for (table_name, mut ids) in written {
            ids.sort_unstable();
            let key_columns = self.dataset.primary_key(&table_name);
            let dataset_table = dataset_tables.get(&table_name);
            let schema_json = dataset_table
                .map(|t| arrow_schema_to_json(&t.rehydrated_schema()))
                .unwrap_or_else(|| serde_json::Value::Array(Vec::new()));
            let time_column = dataset_table
                .map(|t| t.time_column.clone())
                .unwrap_or_default();

            tables_metadata.insert(
                table_name.clone(),
                TableMetadata {
                    name: table_name.clone(),
                    schema: schema_json,
                    time_column,
                    key_columns,
                    batch_ids: ids.clone(),
                },
            );

            tracing::info!(
                table = %table_name,
                batch_count = ids.len(),
                "Table metadata collected"
            );
        }

        let version_metadata = VersionMetadata {
            version: self.version_config.version,
            scenario: self.version_config.scenario.clone(),
            scale_factor: self.version_config.scale_factor,
            num_steps: self.version_config.num_steps,
            dataset_type: self.version_config.dataset_type.clone(),
            mutations: MutationsMetadata {
                update_ratio: self.version_config.update_ratio,
                delete_ratio: self.version_config.delete_ratio,
            },
            tables: tables_metadata,
        };

        self.target
            .write_version_metadata(&version_metadata)
            .await?;
        tracing::info!("version.json written");

        let summary = self.metrics.summary();
        tracing::info!(
            elapsed = ?summary.elapsed,
            batches_generated = summary.batches_generated,
            batches_written = summary.batches_written,
            rows = summary.rows_written,
            creates = summary.rows_created,
            updates = summary.rows_updated,
            deletes = summary.rows_deleted,
            bytes = summary.bytes_written,
            errors = summary.write_errors,
            rows_per_sec = format!("{:.0}", summary.rows_per_sec),
            avg_write_latency = ?summary.avg_write_latency,
            "Ingestion complete"
        );

        Ok(summary)
    }
}
