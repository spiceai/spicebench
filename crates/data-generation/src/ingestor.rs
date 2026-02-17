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
use std::time::Instant;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::config::IngestorConfig;
use crate::dataset::Dataset;
use crate::metrics::{IngestResult, Metrics};
use crate::target::Target;

use std::collections::HashSet;

pub struct Ingestor<S: Dataset, T: Target> {
    dataset: S,
    target: T,
    metrics: Metrics,
    semaphore: Arc<Semaphore>,
    batch_id: u64,
}

impl<S: Dataset, T: Target> Ingestor<S, T> {
    pub fn new(dataset: S, target: T, config: &IngestorConfig, metrics: Metrics) -> Self {
        Self {
            dataset,
            target,
            metrics,
            semaphore: Arc::new(Semaphore::new(config.max_concurrency)),
            batch_id: 0,
        }
    }

    /// Seed the target with initial data — writes at least one batch per table.
    ///
    /// Pulls batches from the dataset until every table has been written at least once,
    /// then returns. Writes are performed sequentially (no concurrency) so the data is
    /// guaranteed to be present when this returns.
    ///
    /// If `table_location_fn` is provided, prints a JSON object mapping each table to
    /// its connector and location, e.g.:
    /// `{"customer": {"connector": "s3", "location": "s3://bucket/prefix/customer/"}, ...}`
    pub async fn initialize(
        &mut self,
        table_location_fn: Option<&dyn Fn(&str) -> String>,
    ) -> anyhow::Result<IngestResult> {
        // Print table locations as JSON
        if let Some(loc_fn) = table_location_fn {
            let mut map = serde_json::Map::new();
            for table in self.dataset.tables() {
                let mut entry = serde_json::Map::new();
                entry.insert(
                    "connector".to_string(),
                    serde_json::Value::String("s3".to_string()),
                );
                entry.insert(
                    "location".to_string(),
                    serde_json::Value::String(loc_fn(&table)),
                );
                if let Some(time_col) = self.dataset.time_column(&table) {
                    entry.insert(
                        "time_column".to_string(),
                        serde_json::Value::String(time_col),
                    );
                }
                map.insert(table, serde_json::Value::Object(entry));
            }
            println!("{}", serde_json::Value::Object(map));
        }

        let all_tables: HashSet<String> = self.dataset.tables().into_iter().collect();
        let mut tables_written: HashSet<String> = HashSet::new();

        tracing::info!(
            table_count = all_tables.len(),
            "Initializing target with seed data for all tables"
        );

        while tables_written.len() < all_tables.len() {
            let source_batch = match self.dataset.next_batch() {
                Ok(Some(batch)) => batch,
                Ok(None) => {
                    let missing: Vec<_> = all_tables.difference(&tables_written).collect();
                    tracing::warn!(?missing, "Dataset exhausted before all tables were written");
                    break;
                }
                Err(e) => return Err(e),
            };
            self.metrics.record_generation();

            tables_written.insert(source_batch.table_name.clone());

            let start = Instant::now();
            let result = self
                .target
                .write(&source_batch.table_name, self.batch_id, source_batch.batch)
                .await?;
            self.metrics.record_write(&result, start.elapsed());
            self.batch_id += 1;
        }

        let summary = self.metrics.summary();
        tracing::info!(
            rows = summary.rows_written,
            batches = summary.batches_written,
            tables = tables_written.len(),
            "Initialization complete"
        );

        Ok(summary)
    }

    /// Skip the initial batches that `initialize()` would have written.
    ///
    /// Consumes batches from the dataset until every table has been seen at least once,
    /// without writing them to the target. This advances the dataset past the
    /// initialization records so that `run()` only processes new data.
    pub fn skip_initial_batches(&mut self) -> anyhow::Result<()> {
        let all_tables: HashSet<String> = self.dataset.tables().into_iter().collect();
        let mut tables_seen: HashSet<String> = HashSet::new();

        tracing::info!(
            table_count = all_tables.len(),
            "Skipping initial batches for all tables"
        );

        while tables_seen.len() < all_tables.len() {
            match self.dataset.next_batch() {
                Ok(Some(batch)) => {
                    tables_seen.insert(batch.table_name);
                    self.batch_id += 1;
                }
                Ok(None) => {
                    let missing: Vec<_> = all_tables.difference(&tables_seen).collect();
                    tracing::warn!(?missing, "Dataset exhausted before all tables were skipped");
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        tracing::info!(batches_skipped = self.batch_id, "Initial batches skipped");

        Ok(())
    }

    /// Ingest the remaining data from the dataset into the target.
    ///
    /// Pulls batches sequentially from the dataset and dispatches writes concurrently,
    /// bounded by the configured `max_concurrency`. This continues from wherever the
    /// dataset was left after `initialize()`.
    pub async fn run(&mut self) -> anyhow::Result<IngestResult> {
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

        loop {
            let source_batch = match self.dataset.next_batch() {
                Ok(Some(batch)) => batch,
                Ok(None) => break,
                Err(e) => {
                    tracing::error!("Dataset error: {e}");
                    break;
                }
            };
            self.metrics.record_generation();

            // Acquire semaphore permit — creates backpressure if all write slots are busy
            let permit = Arc::clone(&self.semaphore).acquire_owned().await?;

            let target = self.target.clone();
            let metrics = self.metrics.clone();
            let current_batch_id = self.batch_id;
            let table_name = source_batch.table_name;
            let batch = source_batch.batch;
            self.batch_id += 1;

            join_set.spawn(async move {
                let start = Instant::now();
                match target.write(&table_name, current_batch_id, batch).await {
                    Ok(result) => {
                        metrics.record_write(&result, start.elapsed());
                    }
                    Err(e) => {
                        metrics.record_error();
                        tracing::error!(batch_id = current_batch_id, "Write failed: {e}");
                    }
                }
                drop(permit);
            });
        }

        // Wait for all in-flight writes
        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                tracing::error!("Write task panicked: {e}");
            }
        }

        logger_handle.abort();

        let summary = self.metrics.summary();
        tracing::info!(
            elapsed = ?summary.elapsed,
            batches_generated = summary.batches_generated,
            batches_written = summary.batches_written,
            rows = summary.rows_written,
            bytes = summary.bytes_written,
            errors = summary.write_errors,
            rows_per_sec = format!("{:.0}", summary.rows_per_sec),
            avg_write_latency = ?summary.avg_write_latency,
            "Ingestion complete"
        );

        Ok(summary)
    }
}
