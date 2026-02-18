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
use super::target::Target;

pub struct Ingestor {
    dataset: Arc<dyn Dataset>,
    target: Arc<dyn Target>,
    metrics: Metrics,
    semaphore: Arc<Semaphore>,
}

impl Ingestor {
    pub fn new(
        dataset: Arc<dyn Dataset>,
        target: Arc<dyn Target>,
        config: &IngestorConfig,
        metrics: Metrics,
    ) -> Self {
        Self {
            dataset,
            target,
            metrics,
            semaphore: Arc::new(Semaphore::new(config.max_concurrency)),
        }
    }

    /// Seed the target with initial data — writes at least one batch per table.
    ///
    /// Pulls one batch per table from the dataset using `next_batches()`, then writes
    /// them sequentially so the data is guaranteed to be present when this returns.
    ///
    /// If `table_location_fn` is provided, prints a JSON object mapping each table to
    /// its connector and location, e.g.:
    /// `{"customer": {"connector": "s3", "location": "s3://bucket/prefix/customer/"}, ...}`
    pub async fn initialize(
        &self,
        table_location_fn: Option<&dyn Fn(&str) -> String>,
    ) -> anyhow::Result<IngestResult> {
        // Print table locations as JSON
        if let Some(loc_fn) = table_location_fn {
            let mut map = serde_json::Map::new();
            for (name, table) in self.dataset.tables() {
                let mut entry = serde_json::Map::new();
                entry.insert(
                    "connector".to_string(),
                    serde_json::Value::String("s3".to_string()),
                );
                entry.insert(
                    "location".to_string(),
                    serde_json::Value::String(loc_fn(&name)),
                );
                if let Some(ref time_col) = table.time_column {
                    entry.insert(
                        "time_column".to_string(),
                        serde_json::Value::String(time_col.clone()),
                    );
                }
                map.insert(name, serde_json::Value::Object(entry));
            }
            println!("{}", serde_json::Value::Object(map));
        }

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
                    self.metrics.record_generation();

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
            batch_ids.insert(table.clone(), self.dataset.batch_ids(table));
        }

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
                self.metrics.record_generation();

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

                join_set.spawn(async move {
                    let start = Instant::now();
                    match target.write(&table_name, next_batch_id, batch).await {
                        Ok(result) => {
                            metrics.record_write(&result, start.elapsed());
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
