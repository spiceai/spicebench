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
use std::collections::HashMap;
use std::collections::VecDeque;

pub struct ReadResult {
    pub batches: Vec<RecordBatch>,
    pub rows_read: u64,
    pub bytes_read: u64,
    pub key_columns: Vec<String>,
}

pub struct WriteResult {
    pub rows_written: u64,
    pub bytes_written: u64,
}

#[async_trait]
pub trait DataStorage: Send + Sync + 'static {
    /// List available batch object paths for a given table.
    async fn list_batches(&self, table_name: &str) -> anyhow::Result<Vec<String>>;

    /// Read a single batch from the source by its batch ID and table name.
    ///
    /// Returns `Ok(None)` when the batch does not exist in the underlying
    /// storage (e.g. the table has fewer batches than others). The caller
    /// should treat this as the table having no more data.
    ///
    /// The concrete implementation is responsible for mapping `(table_name,
    /// batch_id)` to the underlying storage path.
    async fn read_batch(
        &self,
        table_name: &str,
        batch_id: u64,
    ) -> anyhow::Result<Option<ReadResult>>;

    async fn write(
        &self,
        table_name: &str,
        batch_id: u64,
        batch: RecordBatch,
    ) -> anyhow::Result<WriteResult>;

    /// Writes table-level metadata, including the key columns used for
    /// update/delete operations and the batch IDs that were written.
    ///
    /// The default implementation is a no-op.  Backends that persist
    /// metadata (e.g. S3) override this to write `metadata.json`.
    async fn write_table_metadata(
        &self,
        _table_name: &str,
        _key_columns: &[String],
        _batch_ids: &[u64],
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Reads the key columns from the table-level metadata.
    ///
    /// Returns `Ok(Vec::new())` if no key columns are defined (pure inserts).
    async fn read_key_columns(&self, _table_name: &str) -> anyhow::Result<Vec<String>> {
        Ok(Vec::new())
    }

    fn table_params(&self, table_name: &str) -> HashMap<String, serde_json::Value>;

    /// Returns the list of file paths/URIs that would exist after a successful
    /// generation for the given table and batch IDs.
    ///
    /// This is a planning method — no I/O is performed. Each implementation
    /// maps `(table_name, batch_id)` to its own path scheme (e.g. an S3 URI).
    fn expected_files(&self, table_name: &str, batch_ids: &[u64]) -> Vec<String>;

    /// Reads the batch IDs recorded in the table-level metadata file.
    ///
    /// Returns the batch IDs in ascending order. If no metadata file exists
    /// (or the implementation does not support metadata), the default
    /// returns an empty `VecDeque`.
    async fn read_batch_ids(&self, _table_name: &str) -> anyhow::Result<VecDeque<u64>> {
        Ok(VecDeque::new())
    }
}
