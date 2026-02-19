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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{Array, RecordBatch, StringArray};

use crate::storage::WriteResult;

#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    start_time: Instant,
    batches_generated: AtomicU64,
    batches_written: AtomicU64,
    rows_written: AtomicU64,
    bytes_written: AtomicU64,
    write_errors: AtomicU64,
    total_write_latency_us: AtomicU64,
    rows_created: AtomicU64,
    rows_updated: AtomicU64,
    rows_deleted: AtomicU64,
}

pub struct IngestResult {
    pub elapsed: Duration,
    pub batches_generated: u64,
    pub batches_written: u64,
    pub rows_written: u64,
    pub bytes_written: u64,
    pub write_errors: u64,
    pub rows_per_sec: f64,
    pub batches_per_sec: f64,
    pub bytes_per_sec: f64,
    pub avg_write_latency: Duration,
    pub rows_created: u64,
    pub rows_updated: u64,
    pub rows_deleted: u64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                start_time: Instant::now(),
                batches_generated: AtomicU64::new(0),
                batches_written: AtomicU64::new(0),
                rows_written: AtomicU64::new(0),
                bytes_written: AtomicU64::new(0),
                write_errors: AtomicU64::new(0),
                total_write_latency_us: AtomicU64::new(0),
                rows_created: AtomicU64::new(0),
                rows_updated: AtomicU64::new(0),
                rows_deleted: AtomicU64::new(0),
            }),
        }
    }

    pub fn record_generation(&self, batch: &RecordBatch) {
        self.inner.batches_generated.fetch_add(1, Ordering::Relaxed);

        // Count inserts, updates, and deletes from the `_op` column.
        if let Ok(idx) = batch.schema().index_of("_op")
            && let Some(op_array) = batch.column(idx).as_any().downcast_ref::<StringArray>()
        {
            let mut creates = 0u64;
            let mut updates = 0u64;
            let mut deletes = 0u64;
            for i in 0..op_array.len() {
                match op_array.value(i) {
                    "c" => creates += 1,
                    "u" => updates += 1,
                    "d" => deletes += 1,
                    _ => {}
                }
            }
            self.inner
                .rows_created
                .fetch_add(creates, Ordering::Relaxed);
            self.inner
                .rows_updated
                .fetch_add(updates, Ordering::Relaxed);
            self.inner
                .rows_deleted
                .fetch_add(deletes, Ordering::Relaxed);
        }
    }

    pub fn record_write(&self, result: &WriteResult, latency: Duration) {
        self.inner.batches_written.fetch_add(1, Ordering::Relaxed);
        self.inner
            .rows_written
            .fetch_add(result.rows_written, Ordering::Relaxed);
        self.inner
            .bytes_written
            .fetch_add(result.bytes_written, Ordering::Relaxed);
        self.inner
            .total_write_latency_us
            .fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.inner.write_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn log_progress(&self) {
        let elapsed = self.inner.start_time.elapsed();
        let secs = elapsed.as_secs_f64();
        if secs < 0.001 {
            return;
        }

        let batches_written = self.inner.batches_written.load(Ordering::Relaxed);
        let rows_written = self.inner.rows_written.load(Ordering::Relaxed);
        let bytes_written = self.inner.bytes_written.load(Ordering::Relaxed);
        let errors = self.inner.write_errors.load(Ordering::Relaxed);
        let creates = self.inner.rows_created.load(Ordering::Relaxed);
        let updates = self.inner.rows_updated.load(Ordering::Relaxed);
        let deletes = self.inner.rows_deleted.load(Ordering::Relaxed);

        tracing::info!(
            elapsed_secs = format!("{secs:.1}"),
            batches = batches_written,
            rows = rows_written,
            creates = creates,
            updates = updates,
            deletes = deletes,
            bytes = bytes_written,
            errors = errors,
            rows_per_sec = format!("{:.0}", rows_written as f64 / secs),
            mb_per_sec = format!("{:.2}", bytes_written as f64 / secs / 1_048_576.0),
            "Progress"
        );
    }

    pub fn summary(&self) -> IngestResult {
        let elapsed = self.inner.start_time.elapsed();
        let secs = elapsed.as_secs_f64().max(0.001);

        let batches_generated = self.inner.batches_generated.load(Ordering::Relaxed);
        let batches_written = self.inner.batches_written.load(Ordering::Relaxed);
        let rows_written = self.inner.rows_written.load(Ordering::Relaxed);
        let bytes_written = self.inner.bytes_written.load(Ordering::Relaxed);
        let write_errors = self.inner.write_errors.load(Ordering::Relaxed);
        let total_latency_us = self.inner.total_write_latency_us.load(Ordering::Relaxed);

        let avg_write_latency = if batches_written > 0 {
            Duration::from_micros(total_latency_us / batches_written)
        } else {
            Duration::ZERO
        };

        let rows_created = self.inner.rows_created.load(Ordering::Relaxed);
        let rows_updated = self.inner.rows_updated.load(Ordering::Relaxed);
        let rows_deleted = self.inner.rows_deleted.load(Ordering::Relaxed);

        IngestResult {
            elapsed,
            batches_generated,
            batches_written,
            rows_written,
            bytes_written,
            write_errors,
            rows_created,
            rows_updated,
            rows_deleted,
            rows_per_sec: rows_written as f64 / secs,
            batches_per_sec: batches_written as f64 / secs,
            bytes_per_sec: bytes_written as f64 / secs,
            avg_write_latency,
        }
    }
}
