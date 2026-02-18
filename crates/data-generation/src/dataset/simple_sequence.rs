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
use std::sync::atomic::{AtomicI64, AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use rand::Rng;

use crate::config::DatasetConfig;
use crate::dataset::key_set::IndexedKeySet;
use crate::dataset::MutationConfig;

use super::{Dataset, DatasetTable};

/// A simple dataset that generates a sequence of integers in a single table,
/// with support for change-tracking operations (create, update, delete).
///
/// Each call to `raw_next_batch` returns `batch_size` rows distributed across
/// create, update, and delete operations according to the configured
/// [`MutationConfig`] ratios. On the first batch (when no keys exist yet) all
/// rows are creates; subsequent batches mix operations against the accumulated
/// key set.
///
/// After `num_steps` batches the dataset is exhausted.
///
/// ## Output schema
///
/// | Column       | Type  | Nullable | Description                                   |
/// |-------------|-------|----------|-----------------------------------------------|
/// | `id`        | Int64 | No       | Primary key                                   |
/// | `value`     | Int64 | Yes      | Payload (`null` for deletes)                  |
/// | `_op`       | Utf8  | No       | Operation: `"c"` create, `"u"` update, `"d"` delete |
/// | `_op_index` | Int64 | No       | Monotonically increasing replay counter       |
pub struct SimpleSequenceDataset {
    batch_size: usize,
    num_steps: u16,
    /// Next id to assign for create operations.
    current_offset: AtomicI64,
    remaining_steps: AtomicU16,
    mutations: MutationConfig,
    /// Tracks currently live primary keys for update / delete targeting.
    key_set: Mutex<IndexedKeySet<i64>>,
    /// Global monotonically increasing operation counter for replay ordering.
    op_counter: AtomicI64,
}

impl SimpleSequenceDataset {
    pub fn new(config: &DatasetConfig, mutations: &MutationConfig) -> Self {
        let batch_size = (config.scale_factor * 1000.0) as usize;
        Self {
            batch_size,
            num_steps: config.num_steps,
            mutations: mutations.clone(),
            current_offset: AtomicI64::new(0),
            remaining_steps: AtomicU16::new(config.num_steps),
            key_set: Mutex::new(IndexedKeySet::new()),
            op_counter: AtomicI64::new(0),
        }
    }

    /// Returns the static Arrow schema for the `integer_sequence` table.
    ///
    /// Includes change-tracking columns (`_op`, `_op_index`). The time column
    /// (`inserted_at`) is not included; it will be added during ETL
    /// rehydration.
    pub fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, true), // nullable for deletes
            Field::new("_op", DataType::Utf8, false),
            Field::new("_op_index", DataType::Int64, false),
        ]))
    }
}

#[async_trait]
impl Dataset for SimpleSequenceDataset {
    fn create(
        config: &DatasetConfig,
        mutations: &MutationConfig,
    ) -> anyhow::Result<Arc<dyn Dataset>>
    where
        Self: Sized + 'static,
    {
        Ok(Arc::new(Self::new(config, mutations)))
    }

    fn primary_key(&self, _table: &str) -> Vec<String> {
        vec!["id".to_string()]
    }

    fn num_batches(&self, _table: &str) -> u64 {
        // One batch per step for the single table.
        u64::from(self.num_steps)
    }

    async fn raw_next_batch(&self, _table: &str) -> anyhow::Result<Option<RecordBatch>> {
        let prev = self.remaining_steps.fetch_sub(1, Ordering::SeqCst);
        if prev == 0 {
            // Was already 0, restore it.
            self.remaining_steps.store(0, Ordering::SeqCst);
            return Ok(None);
        }

        let total_rows = self.batch_size;
        let mut rng = rand::rng();

        let mut key_set = self
            .key_set
            .lock()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {e}"))?;

        let existing_count = key_set.len();

        // Determine operation counts. On the first batch (no existing keys),
        // all rows must be creates since there is nothing to update or delete.
        let (num_creates, num_updates, num_deletes) = if existing_count == 0 {
            (total_rows, 0, 0)
        } else {
            let mut num_updates =
                ((total_rows as f64) * self.mutations.update_ratio).round() as usize;
            let mut num_deletes =
                ((total_rows as f64) * self.mutations.delete_ratio).round() as usize;

            // Cap mutations to the number of available distinct keys.
            if num_updates + num_deletes > existing_count {
                let scale = existing_count as f64 / (num_updates + num_deletes) as f64;
                num_updates = (num_updates as f64 * scale).floor() as usize;
                num_deletes = (num_deletes as f64 * scale).floor() as usize;
            }

            let num_creates = total_rows.saturating_sub(num_updates + num_deletes);
            (num_creates, num_updates, num_deletes)
        };

        // Sample distinct keys for updates and deletes so no key is both
        // updated *and* deleted within the same batch.
        let mutation_keys = key_set.sample_keys(num_updates + num_deletes, &mut rng);
        let update_keys = &mutation_keys[..num_updates];
        let delete_keys = &mutation_keys[num_updates..];

        // Reserve the global operation counter range for this batch.
        let op_base = self
            .op_counter
            .fetch_add(total_rows as i64, Ordering::SeqCst);

        // Reserve the id range for new create rows.
        let id_offset = self
            .current_offset
            .fetch_add(num_creates as i64, Ordering::SeqCst);

        let mut ids: Vec<i64> = Vec::with_capacity(total_rows);
        let mut values: Vec<Option<i64>> = Vec::with_capacity(total_rows);
        let mut ops: Vec<&str> = Vec::with_capacity(total_rows);
        let mut op_indices: Vec<i64> = Vec::with_capacity(total_rows);
        let mut op_idx = op_base;

        // --- Creates: new sequential ids with deterministic values ---
        for i in 0..num_creates as i64 {
            let id = id_offset + i;
            ids.push(id);
            values.push(Some(id * 10));
            ops.push("c");
            op_indices.push(op_idx);
            op_idx += 1;
            key_set.insert(id);
        }

        // --- Updates: existing keys with new random values ---
        for &key in update_keys {
            ids.push(key);
            values.push(Some(rng.random_range(0..1_000_000i64)));
            ops.push("u");
            op_indices.push(op_idx);
            op_idx += 1;
        }

        // --- Deletes: existing keys with null payload ---
        for &key in delete_keys {
            ids.push(key);
            values.push(None);
            ops.push("d");
            op_indices.push(op_idx);
            op_idx += 1;
            key_set.remove(&key);
        }

        // Build the RecordBatch.
        let id_array = Int64Array::from(ids);
        let value_array = Int64Array::from(values);
        let op_array = StringArray::from(ops);
        let op_index_array = Int64Array::from(op_indices);

        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(id_array),
                Arc::new(value_array),
                Arc::new(op_array),
                Arc::new(op_index_array),
            ],
        )?;

        Ok(Some(batch))
    }

    fn tables(&self) -> HashMap<String, DatasetTable> {
        HashMap::from([(
            "integer_sequence".to_string(),
            DatasetTable {
                name: "integer_sequence".to_string(),
                schema: Self::schema(),
                time_column: Some("inserted_at".to_string()),
            },
        )])
    }
}
