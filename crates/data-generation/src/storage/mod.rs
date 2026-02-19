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

use crate::version::VersionMetadata;

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
    ///
    /// Batches are stored under `tables/{table_name}/batch-NNNNNN.parquet`.
    async fn list_batches(&self, table_name: &str) -> anyhow::Result<Vec<String>>;

    /// Read a single batch from the source by its batch ID and table name.
    ///
    /// Returns `Ok(None)` when the batch does not exist in the underlying
    /// storage (e.g. the table has fewer batches than others). The caller
    /// should treat this as the table having no more data.
    ///
    /// Batches are stored at `tables/{table_name}/batch-{batch_id:06}.parquet`.
    async fn read_batch(
        &self,
        table_name: &str,
        batch_id: u64,
    ) -> anyhow::Result<Option<ReadResult>>;

    /// Write a single batch to storage for the given table and batch ID.
    ///
    /// Batches are written to `tables/{table_name}/batch-{batch_id:06}.parquet`.
    async fn write(
        &self,
        table_name: &str,
        batch_id: u64,
        batch: RecordBatch,
    ) -> anyhow::Result<WriteResult>;

    /// Writes the consolidated version metadata (`version.json`) for this
    /// generation version. Contains scale factor, mutations config, and
    /// per-table metadata (schemas, key columns, batch IDs).
    async fn write_version_metadata(
        &self,
        _metadata: &VersionMetadata,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Reads the version metadata (`version.json`) from storage.
    ///
    /// Returns `Ok(None)` if no version metadata exists.
    async fn read_version_metadata(&self) -> anyhow::Result<Option<VersionMetadata>> {
        Ok(None)
    }

    /// Returns key columns for a table by reading from the version metadata.
    ///
    /// Returns `Ok(Vec::new())` if no key columns are defined (pure inserts)
    /// or if version metadata is not available.
    async fn read_key_columns(&self, table_name: &str) -> anyhow::Result<Vec<String>> {
        if let Some(metadata) = self.read_version_metadata().await? {
            if let Some(table_meta) = metadata.tables.get(table_name) {
                return Ok(table_meta.key_columns.clone());
            }
        }
        Ok(Vec::new())
    }

    /// Reads the batch IDs for a table from the version metadata.
    ///
    /// Returns the batch IDs in ascending order. If no version metadata exists
    /// or the table is not found, returns an empty `VecDeque`.
    async fn read_batch_ids(&self, table_name: &str) -> anyhow::Result<VecDeque<u64>> {
        if let Some(metadata) = self.read_version_metadata().await? {
            if let Some(table_meta) = metadata.tables.get(table_name) {
                let mut ids = table_meta.batch_ids.clone();
                ids.sort_unstable();
                return Ok(VecDeque::from(ids));
            }
        }
        Ok(VecDeque::new())
    }

    fn table_params(&self, table_name: &str) -> HashMap<String, serde_json::Value>;

    /// Returns the list of file paths/URIs that would exist after a successful
    /// generation for the given table and batch IDs.
    ///
    /// This is a planning method — no I/O is performed. Each implementation
    /// maps `(table_name, batch_id)` to its own path scheme (e.g. an S3 URI).
    fn expected_files(&self, table_name: &str, batch_ids: &[u64]) -> Vec<String>;
}
