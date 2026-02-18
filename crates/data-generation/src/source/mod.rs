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

pub mod s3;

use arrow::array::RecordBatch;
use async_trait::async_trait;

pub struct ReadResult {
    pub batches: Vec<RecordBatch>,
    pub rows_read: u64,
    pub bytes_read: u64,
}

#[async_trait]
pub trait Source: Send + Sync + Clone + 'static {
    /// List available batch object paths for a given table.
    async fn list_batches(&self, table_name: &str) -> anyhow::Result<Vec<String>>;

    /// Read a single batch from the source by its batch ID and table name.
    ///
    /// The concrete implementation is responsible for mapping `(table_name,
    /// batch_id)` to the underlying storage path.
    async fn read_batch(&self, table_name: &str, batch_id: u64) -> anyhow::Result<ReadResult>;
}
