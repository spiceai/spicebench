/*
Copyright 2026 The Spice.ai OSS Authors

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
use std::sync::atomic::{AtomicI64, AtomicU16, Ordering};

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;

use crate::config::DatasetConfig;

use super::{Dataset, DatasetTable};

/// A simple dataset that generates a sequence of integers in a single table.
///
/// Each call to `raw_next_batch` returns `batch_size` rows with sequential `id`
/// and `value = id * 10`. After `num_steps` batches the dataset is exhausted.
pub struct SimpleSequenceDataset {
    batch_size: usize,
    num_steps: u16,
    current_offset: AtomicI64,
    remaining_steps: AtomicU16,
}

impl SimpleSequenceDataset {
    pub fn new(config: &DatasetConfig) -> Self {
        let batch_size = (config.scale_factor * 1000.0) as usize;
        Self {
            batch_size,
            num_steps: config.num_steps,
            current_offset: AtomicI64::new(0),
            remaining_steps: AtomicU16::new(config.num_steps),
        }
    }

    /// Returns the static Arrow schema for the `integer_sequence` table.
    ///
    /// The time column (`inserted_at`) is not included; it will be added during
    /// ETL rehydration.
    pub fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]))
    }
}

#[async_trait]
impl Dataset for SimpleSequenceDataset {
    fn create(config: &DatasetConfig) -> anyhow::Result<Arc<dyn Dataset>>
    where
        Self: Sized + 'static,
    {
        Ok(Arc::new(Self::new(config)))
    }

    fn num_batches(&self, _table: &str) -> u64 {
        // One batch per step for the single table.
        u64::from(self.num_steps)
    }

    async fn raw_next_batch(&self, _table: &str) -> anyhow::Result<Option<RecordBatch>> {
        let prev = self.remaining_steps.fetch_sub(1, Ordering::SeqCst);
        if prev == 0 {
            // Was already 0, restore it
            self.remaining_steps.store(0, Ordering::SeqCst);
            return Ok(None);
        }

        let offset = self.current_offset.fetch_add(self.batch_size as i64, Ordering::SeqCst);

        let ids: Int64Array = (offset..offset + self.batch_size as i64)
            .collect();
        let values: Int64Array = (offset..offset + self.batch_size as i64)
            .map(|id| id * 10)
            .collect();

        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![Arc::new(ids), Arc::new(values)],
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