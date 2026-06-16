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

use tokio::task::JoinSet;

use super::dataset::Dataset;
use super::metrics::{IngestResult, Metrics};
use super::storage::DataStorage;
use super::version::{MutationsMetadata, TableMetadata, VersionMetadata, arrow_schema_to_json};
use crate::config::format_scale_factor;

/// Configuration for the version metadata that will be written at the end
/// of a data generation run.
pub struct VersionConfig {
    pub scenario: String,
    pub scale_factor: f64,
    pub num_steps: u16,
    pub dataset_type: String,
    pub update_ratio: f64,
    pub delete_ratio: f64,
    pub bootstrap: bool,
    pub num_mutation_steps: u16,
    pub churn_fraction: f64,
}

pub struct DataGenerator {
    dataset: Arc<dyn Dataset>,
    target: Arc<dyn DataStorage>,
    metrics: Metrics,
    version_config: VersionConfig,
}

type WrittenBatches = HashMap<String, HashMap<u64, Vec<usize>>>;

impl DataGenerator {
    pub fn new(
        dataset: Arc<dyn Dataset>,
        target: Arc<dyn DataStorage>,
        metrics: Metrics,
        version_config: VersionConfig,
    ) -> Self {
        Self {
            dataset,
            target,
            metrics,
            version_config,
        }
    }

    /// Ingest data from the dataset into the target.
    ///
    /// Spawns one task per table. Each table task generates the next batch,
    /// writes it directly to file storage, and moves to the next batch.
    /// All table tasks run concurrently. With file-based storage there is no
    /// upload backpressure, so channels are not needed.
    pub async fn run(&self) -> anyhow::Result<IngestResult> {
        // Spawn periodic metrics logger (every 1 second)
        let metrics_logger = self.metrics.clone();
        let logger_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                interval.tick().await;
                metrics_logger.log_progress();
            }
        });

        // Track which logical batch IDs were successfully written per table,
        // plus any split part IDs for each logical batch, so we can persist
        // both in table metadata at the end of the run.
        let written_batches: Arc<std::sync::Mutex<WrittenBatches>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));

        // For each table, spawn a single task that generates and writes inline.
        let mut join_set = JoinSet::new();
        for table_name in self.dataset.tables().keys() {
            let dataset = Arc::clone(&self.dataset);
            let target = self.target.clone();
            let metrics = self.metrics.clone();
            let written_ids = Arc::clone(&written_batches);

            let table_name = table_name.clone();
            join_set.spawn(async move {
                let mut batch_id: u64 = 0;
                loop {
                    let batch = match dataset.next_batch(&table_name).await {
                        Ok(Some(b)) => b,
                        Ok(None) => break,
                        Err(e) => {
                            return Err(anyhow::anyhow!(
                                "Dataset error for table {table_name}: {e}"
                            ));
                        }
                    };
                    metrics.record_generation(&batch);

                    let start = Instant::now();
                    match target.write(&table_name, batch_id, batch).await {
                        Ok(result) => {
                            metrics.record_write(&result, start.elapsed());
                            written_ids
                                .lock()
                                .expect("written_batches lock poisoned")
                                .entry(table_name.clone())
                                .or_default()
                                .insert(batch_id, result.part_ids.clone());
                        }
                        Err(e) => {
                            metrics.record_error();
                            tracing::error!(batch_id, table = %table_name, "Write failed: {e}");
                        }
                    }
                    batch_id += 1;
                }
                Ok::<(), anyhow::Error>(())
            });
        }

        // Wait for all generator and uploader tasks, propagating any errors.
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    logger_handle.abort();
                    return Err(e);
                }
                Err(e) => {
                    logger_handle.abort();
                    return Err(anyhow::anyhow!("Task panicked: {e}"));
                }
            }
        }

        logger_handle.abort();

        // Build and persist the consolidated version metadata (version.json).
        let written = Arc::try_unwrap(written_batches)
            .expect("all tasks should be finished")
            .into_inner()
            .expect("mutex should not be poisoned");

        let dataset_tables = self.dataset.tables();
        let mut tables_metadata = HashMap::new();
        for (table_name, batch_parts) in written {
            let mut ids: Vec<u64> = batch_parts.keys().copied().collect();
            ids.sort_unstable();

            let mut normalized_batch_parts: HashMap<u64, Vec<usize>> = HashMap::new();
            for (batch_id, mut part_ids) in batch_parts {
                if part_ids.is_empty() {
                    continue;
                }
                part_ids.sort_unstable();
                normalized_batch_parts.insert(batch_id, part_ids);
            }

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
                    batch_parts: normalized_batch_parts,
                },
            );

            tracing::info!(
                table = %table_name,
                batch_count = ids.len(),
                "Table metadata collected"
            );
        }

        let version_metadata = VersionMetadata {
            version: format_scale_factor(self.version_config.scale_factor),
            scenario: self.version_config.scenario.clone(),
            scale_factor: self.version_config.scale_factor,
            num_steps: self.version_config.num_steps,
            dataset_type: self.version_config.dataset_type.clone(),
            mutations: MutationsMetadata {
                update_ratio: self.version_config.update_ratio,
                delete_ratio: self.version_config.delete_ratio,
                bootstrap: self.version_config.bootstrap,
                num_mutation_steps: self.version_config.num_mutation_steps,
                churn_fraction: self.version_config.churn_fraction,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use async_trait::async_trait;

    use crate::config::DatasetConfig;
    use crate::dataset::{Dataset, DatasetTable, MutationConfig};
    use crate::storage::{ReadResult, WriteResult};

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("_op", DataType::Utf8, false),
        ]))
    }

    fn test_batch(start: i64, len: usize) -> RecordBatch {
        let ids = Int64Array::from_iter_values((start..start + len as i64).collect::<Vec<_>>());
        let ops = StringArray::from(vec!["c"; len]);
        RecordBatch::try_new(test_schema(), vec![Arc::new(ids), Arc::new(ops)])
            .expect("test batch should be valid")
    }

    struct MockDataset {
        tables: HashMap<String, DatasetTable>,
        /// Per-table queue of batches. Each entry is the next batch to return
        /// for that table; `None` is never stored — the queue simply becomes
        /// empty when exhausted.
        table_batches: HashMap<String, Mutex<Vec<RecordBatch>>>,
    }

    impl MockDataset {
        fn new() -> Self {
            let schema = test_schema();
            let tables = HashMap::from([
                (
                    "a".to_string(),
                    DatasetTable {
                        name: "a".to_string(),
                        schema: schema.clone(),
                        time_column: "a_created_at".to_string(),
                    },
                ),
                (
                    "b".to_string(),
                    DatasetTable {
                        name: "b".to_string(),
                        schema,
                        time_column: "b_created_at".to_string(),
                    },
                ),
            ]);

            // Table "a": 3 batches; table "b": 2 batches.
            let table_batches = HashMap::from([
                (
                    "a".to_string(),
                    Mutex::new(vec![test_batch(0, 2), test_batch(10, 2), test_batch(20, 2)]),
                ),
                (
                    "b".to_string(),
                    Mutex::new(vec![test_batch(100, 3), test_batch(110, 3)]),
                ),
            ]);

            Self {
                tables,
                table_batches,
            }
        }
    }

    #[async_trait]
    impl Dataset for MockDataset {
        fn create(
            _config: &DatasetConfig,
            _mutations: &MutationConfig,
            _storage: Arc<dyn DataStorage>,
        ) -> anyhow::Result<Arc<dyn Dataset>>
        where
            Self: Sized + 'static,
        {
            anyhow::bail!("not used in tests")
        }

        fn storage(self: Arc<Self>) -> Arc<dyn DataStorage> {
            Arc::new(MockStorage::new())
        }

        fn primary_key(&self, _table: &str) -> Vec<String> {
            vec![]
        }

        fn num_batches(&self, table: &str) -> u64 {
            self.table_batches
                .get(table)
                .map_or(0, |q| q.lock().expect("queue lock poisoned").len() as u64)
        }

        async fn raw_next_batch(&self, table: &str) -> anyhow::Result<Option<RecordBatch>> {
            let batch = self.table_batches.get(table).and_then(|q| {
                let mut queue = q.lock().expect("queue lock poisoned");
                if queue.is_empty() {
                    None
                } else {
                    Some(queue.remove(0))
                }
            });
            Ok(batch)
        }

        fn tables(&self) -> HashMap<String, DatasetTable> {
            self.tables.clone()
        }
    }

    #[derive(Default)]
    struct MockStorageState {
        seen_ids: HashMap<String, HashSet<u64>>,
        rows_written: u64,
        table_batch_ids: HashMap<String, Vec<u64>>,
        version_written: bool,
    }

    #[derive(Clone, Default)]
    struct MockStorage {
        state: Arc<Mutex<MockStorageState>>,
    }

    impl MockStorage {
        fn new() -> Self {
            Self::default()
        }

        fn snapshot(&self) -> MockStorageState {
            let s = self.state.lock().expect("state lock poisoned");
            MockStorageState {
                seen_ids: s.seen_ids.clone(),
                rows_written: s.rows_written,
                table_batch_ids: s.table_batch_ids.clone(),
                version_written: s.version_written,
            }
        }
    }

    #[async_trait]
    impl DataStorage for MockStorage {
        async fn list_batches(&self, _table_name: &str) -> anyhow::Result<Vec<String>> {
            Ok(Vec::new())
        }

        async fn read_batch(
            &self,
            _table_name: &str,
            _batch_id: u64,
            _part_id: Option<usize>,
        ) -> anyhow::Result<Option<ReadResult>> {
            Ok(None)
        }

        async fn write(
            &self,
            table_name: &str,
            batch_id: u64,
            batch: RecordBatch,
        ) -> anyhow::Result<WriteResult> {
            let mut state = self.state.lock().expect("state lock poisoned");
            let inserted = state
                .seen_ids
                .entry(table_name.to_string())
                .or_default()
                .insert(batch_id);
            if !inserted {
                anyhow::bail!("duplicate batch id {batch_id} for table {table_name}");
            }

            state
                .table_batch_ids
                .entry(table_name.to_string())
                .or_default()
                .push(batch_id);
            state.rows_written += batch.num_rows() as u64;

            Ok(WriteResult {
                rows_written: batch.num_rows() as u64,
                bytes_written: 0,
                part_ids: Vec::new(),
            })
        }

        async fn write_version_metadata(&self, _metadata: &VersionMetadata) -> anyhow::Result<()> {
            let mut state = self.state.lock().expect("state lock poisoned");
            state.version_written = true;
            Ok(())
        }

        fn table_params(&self, _table_name: &str) -> HashMap<String, serde_json::Value> {
            HashMap::new()
        }

        fn expected_files(&self, _table_name: &str, _batch_ids: &[u64]) -> Vec<String> {
            Vec::new()
        }
    }

    #[tokio::test]
    async fn run_writes_all_batches_with_unique_sequential_ids() {
        let dataset: Arc<dyn Dataset> = Arc::new(MockDataset::new());
        let storage = Arc::new(MockStorage::new());
        let target: Arc<dyn DataStorage> = storage.clone();

        let generator = DataGenerator::new(
            dataset,
            target,
            Metrics::new(),
            VersionConfig {
                scenario: "test".to_string(),
                scale_factor: 1.0,
                num_steps: 3,
                dataset_type: "mock".to_string(),
                update_ratio: 0.0,
                delete_ratio: 0.0,
                bootstrap: false,
                num_mutation_steps: 0,
                churn_fraction: 0.0,
            },
        );

        let result = generator.run().await.expect("run should succeed");
        assert_eq!(result.write_errors, 0);

        let snapshot = storage.snapshot();
        assert_eq!(snapshot.rows_written, 12);
        assert_eq!(snapshot.table_batch_ids.get("a"), Some(&vec![0, 1, 2]));
        assert_eq!(snapshot.table_batch_ids.get("b"), Some(&vec![0, 1]));
        assert!(snapshot.version_written);
    }
}
