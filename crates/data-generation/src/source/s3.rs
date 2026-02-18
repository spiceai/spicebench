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

use std::sync::Arc;

use async_trait::async_trait;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::config::TargetConfig;

use super::{ReadResult, Source};

/// Reads Parquet data from S3 (or S3-compatible storage).
#[derive(Clone)]
pub struct S3Source {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl S3Source {
    /// Create a new [`S3Source`] from the same config used for [`S3Target`].
    ///
    /// The source and target typically share the same bucket/prefix, so they
    /// reuse [`TargetConfig`] to avoid duplicating configuration structs.
    pub fn new(config: &TargetConfig) -> anyhow::Result<Self> {
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(&config.bucket);

        if let Some(region) = &config.region {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = &config.endpoint
            && !endpoint.is_empty()
        {
            builder = builder.with_endpoint(endpoint);
            if endpoint.starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
        }

        let store = Arc::new(builder.build()?);
        Ok(Self {
            store,
            prefix: config.prefix.clone(),
        })
    }
}

#[async_trait]
impl Source for S3Source {
    async fn list_batches(&self, table_name: &str) -> anyhow::Result<Vec<String>> {
        let prefix = if self.prefix.is_empty() {
            ObjectPath::from(format!("{table_name}/"))
        } else {
            ObjectPath::from(format!("{}/{table_name}/", self.prefix))
        };

        let objects: Vec<_> = self
            .store
            .list(Some(&prefix))
            .try_collect()
            .await?;

        let paths: Vec<String> = objects
            .into_iter()
            .filter(|meta| meta.location.as_ref().ends_with(".parquet"))
            .map(|meta| meta.location.to_string())
            .collect();

        Ok(paths)
    }

    async fn read_batch(&self, table_name: &str, batch_id: u64) -> anyhow::Result<Option<ReadResult>> {
        let location = if self.prefix.is_empty() {
            ObjectPath::from(format!("{table_name}/batch-{batch_id:06}.parquet"))
        } else {
            ObjectPath::from(format!(
                "{}/{table_name}/batch-{batch_id:06}.parquet",
                self.prefix
            ))
        };

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
