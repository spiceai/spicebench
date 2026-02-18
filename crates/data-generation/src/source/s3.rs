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

use async_trait::async_trait;
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::storage::s3::S3Storage;

use super::{ReadResult, Source};

#[async_trait]
impl Source for S3Storage {
    async fn list_batches(&self, table_name: &str) -> anyhow::Result<Vec<String>> {
        let prefix = self.table_object_prefix(table_name);

        let objects: Vec<_> = self.store.list(Some(&prefix)).try_collect().await?;

        let paths: Vec<String> = objects
            .into_iter()
            .filter(|meta| meta.location.as_ref().ends_with(".parquet"))
            .map(|meta| meta.location.to_string())
            .collect();

        Ok(paths)
    }

    async fn read_batch(
        &self,
        table_name: &str,
        batch_id: u64,
    ) -> anyhow::Result<Option<ReadResult>> {
        let location = self.batch_object_path(table_name, batch_id);

        let get_result = match self.store.get(&location).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let bytes = get_result.bytes().await?;
        let bytes_read = bytes.len() as u64;

        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;

        let mut batches = Vec::new();
        let mut rows_read = 0u64;
        for batch in reader {
            let batch = batch?;
            rows_read += batch.num_rows() as u64;
            batches.push(batch);
        }

        Ok(Some(ReadResult {
            batches,
            rows_read,
            bytes_read,
        }))
    }
}
