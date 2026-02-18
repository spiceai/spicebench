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

use std::collections::HashMap;

use arrow::array::RecordBatch;
use async_trait::async_trait;

pub struct WriteResult {
    pub rows_written: u64,
    pub bytes_written: u64,
}

#[async_trait]
pub trait Target: Send + Sync + 'static {
    async fn write(
        &self,
        table_name: &str,
        batch_id: u64,
        batch: RecordBatch,
    ) -> anyhow::Result<WriteResult>;

    fn table_params(&self, table_name: &str) -> HashMap<String, serde_json::Value>;

    /// Returns the list of file paths/URIs that would exist after a successful
    /// generation for the given table and batch IDs.
    ///
    /// This is a planning method — no I/O is performed. Each implementation
    /// maps `(table_name, batch_id)` to its own path scheme (e.g. an S3 URI).
    fn expected_files(&self, table_name: &str, batch_ids: &[u64]) -> Vec<String>;
}
