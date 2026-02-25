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

use async_trait::async_trait;

use super::{InsertOp, Sink};

/// ETL sink that intentionally discards all writes.
///
/// Useful for measuring source + ETL transform throughput without any target
/// write overhead.
pub struct NullSink;

impl NullSink {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Sink for NullSink {
    async fn write(
        &self,
        _table_name: &str,
        _batch_id: u64,
        _batch: arrow::array::RecordBatch,
        _op: InsertOp,
        _partition_columns: Vec<String>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
