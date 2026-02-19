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

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;

use crate::config::TargetConfig;
use crate::storage::DataStorage;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use futures::TryStreamExt;
use object_store::PutPayload;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::collections::VecDeque;

use super::{ReadResult, WriteResult};

/// Unified S3 storage backend that implements both [`Source`] and [`Target`].
///
/// The trait implementations live in their respective modules
/// (`source/s3.rs` and `target/s3.rs`) to keep concerns separated.
#[derive(Clone)]
pub struct S3Storage {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) bucket: String,
    pub(crate) prefix: String,
    pub(crate) region: Option<String>,
}

impl S3Storage {
    pub fn new(config: &TargetConfig) -> anyhow::Result<Self> {
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(&config.bucket);

        if let Some(region) = &config.region {
            tracing::info!("S3 storage with region: {region}");
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
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
            region: config.region.clone(),
        })
    }

    /// Returns the S3 URI for a given table name (e.g. `s3://bucket/prefix/customer/`).
    pub fn table_s3_path(&self, table_name: &str) -> String {
        if self.prefix.is_empty() {
            format!("s3://{}/{table_name}/", self.bucket)
        } else {
            format!("s3://{}/{}/{table_name}/", self.bucket, self.prefix)
        }
    }

    /// Returns the [`ObjectPath`] for a batch file within a table directory.
    pub(crate) fn batch_object_path(&self, table_name: &str, batch_id: u64) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(format!("{table_name}/batch-{batch_id:06}.parquet"))
        } else {
            ObjectPath::from(format!(
                "{}/{table_name}/batch-{batch_id:06}.parquet",
                self.prefix
            ))
        }
    }

    /// Returns the [`ObjectPath`] prefix for listing objects in a table directory.
    pub(crate) fn table_object_prefix(&self, table_name: &str) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(format!("{table_name}/"))
        } else {
            ObjectPath::from(format!("{}/{table_name}/", self.prefix))
        }
    }

    pub(crate) fn table_metadata_object_path(&self, table_name: &str) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(format!("{table_name}/metadata.json"))
        } else {
            ObjectPath::from(format!("{}/{table_name}/metadata.json", self.prefix))
        }
    }

    /// Reads the key columns from the table-level metadata.
    ///
    /// Returns the `key_columns` array from `metadata.json`.
    /// Returns an empty `Vec` if no metadata exists or key columns are not set.
    async fn read_key_columns_from_metadata(
        &self,
        table_name: &str,
    ) -> anyhow::Result<Vec<String>> {
        let metadata_path = self.table_metadata_object_path(table_name);
        let get_result = match self.store.get(&metadata_path).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };

        let bytes = get_result.bytes().await?;
        let table_meta: serde_json::Value = serde_json::from_slice(&bytes)?;

        let Some(keys) = table_meta.get("key_columns").and_then(|v| v.as_array()) else {
            return Ok(Vec::new());
        };

        Ok(keys
            .iter()
            .filter_map(|v| v.as_str().map(ToOwned::to_owned))
            .collect())
    }
}

#[async_trait]
impl DataStorage for S3Storage {
    fn expected_files(&self, table_name: &str, batch_ids: &[u64]) -> Vec<String> {
        batch_ids
            .iter()
            .map(|id| {
                if self.prefix.is_empty() {
                    format!("s3://{}/{table_name}/batch-{id:06}.parquet", self.bucket)
                } else {
                    format!(
                        "s3://{}/{}/{table_name}/batch-{id:06}.parquet",
                        self.bucket, self.prefix
                    )
                }
            })
            .collect()
    }

    fn table_params(&self, table_name: &str) -> HashMap<String, serde_json::Value> {
        let mut params = HashMap::new();
        params.insert(
            "connector".to_string(),
            serde_json::Value::String("s3".to_string()),
        );
        params.insert(
            "from".to_string(),
            serde_json::Value::String(self.table_s3_path(table_name)),
        );
        params.insert(
            "file_format".to_string(),
            serde_json::Value::String("parquet".to_string()),
        );

        if let Some(region) = &self.region {
            params.insert(
                "s3_region".to_string(),
                serde_json::Value::String(region.clone()),
            );
        }

        params
    }

    async fn write(
        &self,
        table_name: &str,
        batch_id: u64,
        batch: RecordBatch,
    ) -> anyhow::Result<WriteResult> {
        let rows = batch.num_rows() as u64;
        let schema = batch.schema();

        // Serialize RecordBatch to Parquet bytes in memory
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        let bytes_written = buf.len() as u64;

        // Upload to S3 with per-table directory structure
        let path = self.batch_object_path(table_name, batch_id);

        self.store.put(&path, PutPayload::from(buf)).await?;

        Ok(WriteResult {
            rows_written: rows,
            bytes_written,
        })
    }

    async fn write_table_metadata(
        &self,
        table_name: &str,
        key_columns: &[String],
        batch_ids: &[u64],
    ) -> anyhow::Result<()> {
        let path = self.table_metadata_object_path(table_name);

        let table_meta = serde_json::json!({
            "key_columns": key_columns,
            "batch_ids": batch_ids,
        });

        let bytes = serde_json::to_vec_pretty(&table_meta)?;
        self.store.put(&path, PutPayload::from(bytes)).await?;
        Ok(())
    }

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

    async fn read_batch_ids(&self, table_name: &str) -> anyhow::Result<VecDeque<u64>> {
        let metadata_path = self.table_metadata_object_path(table_name);
        let get_result = match self.store.get(&metadata_path).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(VecDeque::new()),
            Err(e) => return Err(e.into()),
        };

        let bytes = get_result.bytes().await?;
        let table_meta: serde_json::Value = serde_json::from_slice(&bytes)?;

        let Some(ids_array) = table_meta.get("batch_ids").and_then(|v| v.as_array()) else {
            return Ok(VecDeque::new());
        };

        let mut ids: Vec<u64> = ids_array.iter().filter_map(|v| v.as_u64()).collect();
        ids.sort_unstable();
        Ok(VecDeque::from(ids))
    }

    async fn read_key_columns(&self, table_name: &str) -> anyhow::Result<Vec<String>> {
        self.read_key_columns_from_metadata(table_name).await
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

        let key_columns = self.read_key_columns_from_metadata(table_name).await?;

        Ok(Some(ReadResult {
            batches,
            rows_read,
            bytes_read,
            key_columns,
        }))
    }
}
