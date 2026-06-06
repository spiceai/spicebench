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
use object_store::ObjectStoreExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{BackoffConfig, ClientOptions, RetryConfig};

use crate::archive;
use crate::config::TargetConfig;
use crate::storage::DataStorage;
use crate::version::VersionMetadata;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use futures::TryStreamExt;
use object_store::PutPayload;
use object_store::WriteMultipart;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::env;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{OnceCell, RwLock};

use super::{ReadResult, WriteResult};

const MIN_ROWS_PER_FILE: usize = 32_000;
const MAX_ROWS_PER_FILE: usize = 64_000;
const DEFAULT_MAX_ROWS_PER_FILE: usize = 48_000;
const ARCHIVE_TRANSFER_CHUNK_SIZE: usize = 8 * 1024 * 1024;
const ARCHIVE_UPLOAD_MAX_IN_FLIGHT_PARTS: usize = 8;

fn max_rows_per_file() -> usize {
    env::var("SPICEBENCH_TPCH_MAX_ROWS_PER_FILE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .map(|v| v.clamp(MIN_ROWS_PER_FILE, MAX_ROWS_PER_FILE))
        .unwrap_or(DEFAULT_MAX_ROWS_PER_FILE)
}

fn split_record_batch(batch: &RecordBatch, max_rows: usize) -> Vec<RecordBatch> {
    if batch.num_rows() <= max_rows {
        return vec![batch.clone()];
    }

    let mut out = Vec::new();
    let mut offset = 0usize;
    while offset < batch.num_rows() {
        let len = std::cmp::min(max_rows, batch.num_rows() - offset);
        out.push(batch.slice(offset, len));
        offset += len;
    }
    out
}

/// Unified S3 storage backend for versioned data generation.
///
/// Storage layout under the version prefix (`{prefix}/{scenario}/{version}/`):
///
/// ```text
/// version.json
/// tables/{table_name}/batch-000000.parquet
/// tables/{table_name}/batch-000001.parquet
/// checkpoints/
///   checkpoints.json
///   {checkpoint_idx}/{query_idx}.parquet
/// ```
#[derive(Clone)]
pub struct S3Storage {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) bucket: String,
    /// The fully-qualified prefix including scenario and version:
    /// `{prefix}/{scenario}/{version}`
    pub(crate) prefix: String,
    pub(crate) region: Option<String>,
    key_columns_cache: Arc<RwLock<HashMap<String, Vec<String>>>>,
    version_metadata_cache: Arc<OnceCell<Option<Arc<VersionMetadata>>>>,
}

impl S3Storage {
    pub fn new(config: &TargetConfig) -> anyhow::Result<Self> {
        let request_timeout_secs = env::var("SPICEBENCH_S3_REQUEST_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(180);
        let connect_timeout_secs = env::var("SPICEBENCH_S3_CONNECT_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(10);
        let max_retries = env::var("SPICEBENCH_S3_MAX_RETRIES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(15);
        let retry_timeout_secs = env::var("SPICEBENCH_S3_RETRY_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(600);
        let pool_max_idle_per_host = env::var("SPICEBENCH_S3_POOL_MAX_IDLE_PER_HOST")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(64);

        let client_options = ClientOptions::new()
            .with_timeout(Duration::from_secs(request_timeout_secs))
            .with_connect_timeout(Duration::from_secs(connect_timeout_secs))
            .with_pool_idle_timeout(Duration::from_secs(90))
            .with_pool_max_idle_per_host(pool_max_idle_per_host);

        let retry_config = RetryConfig {
            backoff: BackoffConfig::default(),
            max_retries,
            retry_timeout: Duration::from_secs(retry_timeout_secs),
        };

        tracing::info!(
            request_timeout_secs,
            connect_timeout_secs,
            max_retries,
            retry_timeout_secs,
            pool_max_idle_per_host,
            "Configured S3 client timeout/retry settings"
        );

        let mut builder = AmazonS3Builder::from_env().with_bucket_name(&config.bucket);
        builder = builder
            .with_client_options(client_options)
            .with_retry(retry_config);

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
            key_columns_cache: Arc::new(RwLock::new(HashMap::new())),
            version_metadata_cache: Arc::new(OnceCell::new()),
        })
    }

    /// Returns the S3 URI for a given table's batch directory.
    ///
    /// e.g. `s3://bucket/{prefix}/tables/{table_name}/`
    pub fn table_s3_path(&self, table_name: &str) -> String {
        if self.prefix.is_empty() {
            format!("s3://{}/tables/{table_name}/", self.bucket)
        } else {
            format!("s3://{}/{}/tables/{table_name}/", self.bucket, self.prefix)
        }
    }

    /// Returns the [`ObjectPath`] for a batch file within a table directory.
    ///
    /// Path: `{prefix}/tables/{table_name}/batch-{batch_id:06}.parquet`
    pub(crate) fn batch_object_path(&self, table_name: &str, batch_id: u64) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(format!("tables/{table_name}/batch-{batch_id:06}.parquet"))
        } else {
            ObjectPath::from(format!(
                "{}/tables/{table_name}/batch-{batch_id:06}.parquet",
                self.prefix
            ))
        }
    }

    /// Returns the [`ObjectPath`] prefix for listing objects in a table directory.
    pub(crate) fn table_object_prefix(&self, table_name: &str) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(format!("tables/{table_name}/"))
        } else {
            ObjectPath::from(format!("{}/tables/{table_name}/", self.prefix))
        }
    }

    /// Returns the [`ObjectPath`] for the version metadata file.
    pub(crate) fn version_metadata_object_path(&self) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from("version.json")
        } else {
            ObjectPath::from(format!("{}/version.json", self.prefix))
        }
    }

    /// Returns the [`ObjectPath`] for a split batch part file.
    ///
    /// Path: `{prefix}/tables/{table_name}/batch-{batch_id:06}-part-{part_idx:03}.parquet`
    pub(crate) fn batch_part_object_path(
        &self,
        table_name: &str,
        batch_id: u64,
        part_idx: usize,
    ) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(format!(
                "tables/{table_name}/batch-{batch_id:06}-part-{part_idx:03}.parquet"
            ))
        } else {
            ObjectPath::from(format!(
                "{}/tables/{table_name}/batch-{batch_id:06}-part-{part_idx:03}.parquet",
                self.prefix
            ))
        }
    }

    /// Returns the [`ObjectPath`] for the data archive file.
    pub(crate) fn archive_object_path(&self) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(archive::ARCHIVE_FILENAME)
        } else {
            ObjectPath::from(format!("{}/{}", self.prefix, archive::ARCHIVE_FILENAME))
        }
    }

    async fn cached_key_columns(&self, table_name: &str) -> anyhow::Result<Vec<String>> {
        if let Some(cached) = self.key_columns_cache.read().await.get(table_name).cloned() {
            return Ok(cached);
        }

        let key_columns = self.read_key_columns(table_name).await?;

        let mut cache = self.key_columns_cache.write().await;
        let cached = cache
            .entry(table_name.to_string())
            .or_insert_with(|| key_columns.clone())
            .clone();

        Ok(cached)
    }

    async fn cached_version_metadata(&self) -> anyhow::Result<Option<Arc<VersionMetadata>>> {
        let cached = self
            .version_metadata_cache
            .get_or_try_init(|| async {
                let path = self.version_metadata_object_path();
                let get_result = match self.store.get(&path).await {
                    Ok(r) => r,
                    Err(object_store::Error::NotFound { .. }) => return Ok(None),
                    Err(e) => return Err(anyhow::anyhow!(e)),
                };

                let bytes = get_result.bytes().await?;
                let metadata = Arc::new(serde_json::from_slice::<VersionMetadata>(&bytes)?);
                Ok(Some(metadata))
            })
            .await?;

        Ok(cached.clone())
    }
}

#[async_trait]
impl DataStorage for S3Storage {
    fn expected_files(&self, table_name: &str, batch_ids: &[u64]) -> Vec<String> {
        batch_ids
            .iter()
            .map(|id| {
                if self.prefix.is_empty() {
                    format!(
                        "s3://{}/tables/{table_name}/batch-{id:06}.parquet",
                        self.bucket
                    )
                } else {
                    format!(
                        "s3://{}/{}/tables/{table_name}/batch-{id:06}.parquet",
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
        let start = Instant::now();
        let max_rows = max_rows_per_file();
        let chunks = split_record_batch(&batch, max_rows);

        tracing::debug!(
            table = %table_name,
            batch_id,
            rows,
            num_parts = chunks.len(),
            max_rows_per_file = max_rows,
            "S3 write batch split planning"
        );

        let props = WriterProperties::builder()
            .set_compression(Compression::LZ4)
            .build();

        let mut bytes_written: u64 = 0;
        let mut serialize_elapsed = Duration::ZERO;
        let mut upload_elapsed = Duration::ZERO;
        let part_ids: Vec<usize> = if chunks.len() > 1 {
            (0..chunks.len()).collect()
        } else {
            Vec::new()
        };
        for (part_idx, chunk) in chunks.iter().enumerate() {
            let serialize_start = Instant::now();
            let mut buf = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut buf, chunk.schema(), Some(props.clone()))?;
            writer.write(chunk)?;
            writer.close()?;
            serialize_elapsed += serialize_start.elapsed();
            bytes_written += buf.len() as u64;

            let path = if chunks.len() == 1 {
                self.batch_object_path(table_name, batch_id)
            } else {
                self.batch_part_object_path(table_name, batch_id, part_idx)
            };

            let upload_start = Instant::now();
            self.store.put(&path, PutPayload::from(buf)).await?;
            upload_elapsed += upload_start.elapsed();
        }
        let total_elapsed = start.elapsed();

        if total_elapsed.as_secs() >= 5 {
            tracing::warn!(
                table = %table_name,
                batch_id,
                rows,
                bytes = bytes_written,
                serialize_ms = serialize_elapsed.as_millis(),
                upload_ms = upload_elapsed.as_millis(),
                total_ms = total_elapsed.as_millis(),
                mb_per_sec = format!(
                    "{:.2}",
                    if total_elapsed.is_zero() {
                        0.0
                    } else {
                        bytes_written as f64 / total_elapsed.as_secs_f64() / 1_048_576.0
                    }
                ),
                "Slow S3 batch write"
            );
        } else {
            tracing::debug!(
                table = %table_name,
                batch_id,
                rows,
                bytes = bytes_written,
                serialize_ms = serialize_elapsed.as_millis(),
                upload_ms = upload_elapsed.as_millis(),
                total_ms = total_elapsed.as_millis(),
                "S3 batch write"
            );
        }

        Ok(WriteResult {
            rows_written: rows,
            bytes_written,
            part_ids,
        })
    }

    async fn write_version_metadata(&self, metadata: &VersionMetadata) -> anyhow::Result<()> {
        let path = self.version_metadata_object_path();
        let bytes = serde_json::to_vec_pretty(metadata)?;
        self.store.put(&path, PutPayload::from(bytes)).await?;
        let _ = self
            .version_metadata_cache
            .set(Some(Arc::new(metadata.clone())));
        Ok(())
    }

    async fn read_version_metadata(&self) -> anyhow::Result<Option<Arc<VersionMetadata>>> {
        self.cached_version_metadata().await
    }

    async fn read_key_columns(&self, table_name: &str) -> anyhow::Result<Vec<String>> {
        if let Some(metadata) = self.cached_version_metadata().await?
            && let Some(table_meta) = metadata.tables.get(table_name)
        {
            return Ok(table_meta.key_columns.clone());
        }
        Ok(Vec::new())
    }

    async fn read_batch_ids(
        &self,
        table_name: &str,
    ) -> anyhow::Result<std::collections::VecDeque<u64>> {
        if let Some(metadata) = self.cached_version_metadata().await?
            && let Some(table_meta) = metadata.tables.get(table_name)
        {
            let mut ids = table_meta.batch_ids.clone();
            ids.sort_unstable();
            return Ok(std::collections::VecDeque::from(ids));
        }
        Ok(std::collections::VecDeque::new())
    }

    async fn read_batch_parts(
        &self,
        table_name: &str,
        batch_id: u64,
    ) -> anyhow::Result<Vec<usize>> {
        if let Some(metadata) = self.cached_version_metadata().await?
            && let Some(table_meta) = metadata.tables.get(table_name)
            && let Some(part_ids) = table_meta.batch_parts.get(&batch_id)
        {
            let mut sorted = part_ids.clone();
            sorted.sort_unstable();
            return Ok(sorted);
        }

        Ok(Vec::new())
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

    async fn read_batch(
        &self,
        table_name: &str,
        batch_id: u64,
        part_id: Option<usize>,
    ) -> anyhow::Result<Option<ReadResult>> {
        let location = match part_id {
            Some(part_id) => self.batch_part_object_path(table_name, batch_id, part_id),
            None => self.batch_object_path(table_name, batch_id),
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

        let key_columns = self.cached_key_columns(table_name).await?;

        Ok(Some(ReadResult {
            batches,
            rows_read,
            bytes_read,
            key_columns,
        }))
    }

    async fn download_archive(&self, local_path: &std::path::Path) -> anyhow::Result<()> {
        let path = self.archive_object_path();
        tracing::info!(
            s3_path = %path,
            local_path = %local_path.display(),
            "Downloading data archive from S3"
        );

        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let get_result = self.store.get(&path).await?;
        let mut stream = get_result.into_stream();
        let mut file = tokio::fs::File::create(local_path).await?;
        let mut total_bytes: u64 = 0;

        while let Some(chunk) = stream.try_next().await? {
            total_bytes += chunk.len() as u64;
            file.write_all(&chunk).await?;
        }
        file.flush().await?;

        tracing::info!(
            bytes = total_bytes,
            mb = format!("{:.2}", total_bytes as f64 / 1_048_576.0),
            "Archive downloaded"
        );
        Ok(())
    }

    async fn upload_archive(&self, local_path: &std::path::Path) -> anyhow::Result<()> {
        let path = self.archive_object_path();
        let size = tokio::fs::metadata(local_path).await?.len();

        tracing::info!(
            local_path = %local_path.display(),
            s3_path = %path,
            bytes = size,
            mb = format!("{:.2}", size as f64 / 1_048_576.0),
            "Uploading data archive to S3"
        );

        let upload = self.store.put_multipart(&path).await?;
        let mut writer = WriteMultipart::new_with_chunk_size(upload, ARCHIVE_TRANSFER_CHUNK_SIZE);
        let mut file = tokio::fs::File::open(local_path).await?;
        let mut buffer = vec![0u8; ARCHIVE_TRANSFER_CHUNK_SIZE];
        let mut uploaded_bytes: u64 = 0;

        loop {
            let read = match file.read(&mut buffer).await {
                Ok(read) => read,
                Err(e) => {
                    let _ = writer.abort().await;
                    return Err(e.into());
                }
            };

            if read == 0 {
                break;
            }

            if let Err(e) = writer
                .wait_for_capacity(ARCHIVE_UPLOAD_MAX_IN_FLIGHT_PARTS)
                .await
            {
                let _ = writer.abort().await;
                return Err(e.into());
            }

            writer.write(&buffer[..read]);
            uploaded_bytes += read as u64;
        }

        writer.finish().await?;

        tracing::info!(
            bytes = uploaded_bytes,
            mb = format!("{:.2}", uploaded_bytes as f64 / 1_048_576.0),
            "Archive uploaded"
        );
        Ok(())
    }
}
