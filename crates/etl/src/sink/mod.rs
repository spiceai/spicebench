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

use arrow::array::RecordBatch;
use async_trait::async_trait;

pub mod adbc;
pub mod null;

#[cfg(feature = "duckdb")]
pub mod duckdb;

#[cfg(feature = "dynamodb")]
pub mod dynamodb;

#[cfg(feature = "mongodb")]
pub mod mongodb;

#[derive(Debug, Clone)]
pub enum InsertOp {
    Insert,
    Update { key_columns: Vec<String> },
    Delete { key_columns: Vec<String> },
}

#[async_trait]
pub trait Sink: Send + Sync + 'static {
    async fn write(
        &self,
        table_name: &str,
        batch_id: u64,
        batch: RecordBatch,
        op: InsertOp,
        partition_columns: Vec<String>,
    ) -> anyhow::Result<()>;

    /// Flush any internally buffered data to the underlying storage.
    ///
    /// Sinks that buffer writes should implement this to ensure all data is
    /// persisted. The default implementation is a no-op for sinks that write
    /// immediately.
    async fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

