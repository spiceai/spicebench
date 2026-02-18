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
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{Int64Array, RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;

use crate::config::DatasetConfig;

use super::{Dataset, DatasetTable};

/// A simple dataset that generates a sequence of integers in a single table.
///
/// Each call to `raw_next_batch` returns `batch_size` rows with sequential `id`
/// and `value = id * 10`. After `num_steps` batches the dataset is exhausted.
pub struct SimpleSequenceDataset {
    batch_size: usize,
    current_offset: AtomicI64,
    remaining_steps: AtomicU16,
}

impl SimpleSequenceDataset {
    pub fn new(config: &DatasetConfig) -> Self {
        let batch_size = (config.scale_factor * 1000.0) as usize;
        Self {
            batch_size,
            current_offset: AtomicI64::new(0),
            remaining_steps: AtomicU16::new(config.num_steps),
        }
    }

    /// Returns the static Arrow schema for the `integer_sequence` table.
    pub fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
            Field::new(
                "inserted_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]))
    }
}

#[async_trait]
impl Dataset for SimpleSequenceDataset {
    async fn raw_next_batch(&self, _table: &str) -> anyhow::Result<Option<RecordBatch>> {
        let prev = self.remaining_steps.fetch_sub(1, Ordering::SeqCst);
        if prev == 0 {
            // Was already 0, restore it
            self.remaining_steps.store(0, Ordering::SeqCst);
            return Ok(None);
        }

        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before UNIX epoch")
            .as_micros() as i64;

        let offset = self.current_offset.fetch_add(self.batch_size as i64, Ordering::SeqCst);

        let ids: Int64Array = (offset..offset + self.batch_size as i64)
            .collect();
        let values: Int64Array = (offset..offset + self.batch_size as i64)
            .map(|id| id * 10)
            .collect();
        let timestamps = TimestampMicrosecondArray::from(vec![Some(now_us); self.batch_size])
            .with_timezone("UTC");

        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![Arc::new(ids), Arc::new(values), Arc::new(timestamps)],
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