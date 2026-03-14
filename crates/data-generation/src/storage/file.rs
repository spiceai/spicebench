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

//! Local file-system implementation of [`DataStorage`].
//!
//! This storage backend mirrors the same directory layout as the S3 backend:
//!
//! ```text
//! {base_dir}/
//!   version.json
//!   tables/{table_name}/batch-000000.parquet
//!   tables/{table_name}/batch-000001.parquet
//!   …
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use arrow::array::RecordBatch;
use async_trait::async_trait;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::storage::{DataStorage, ReadResult, WriteResult};
use crate::version::VersionMetadata;

/// File-system backed data storage.
///
/// All data is stored under a single `base_dir`. The layout is identical to
/// the S3 key hierarchy so archives produced by [`crate::archive::create_archive`]
/// are directly consumable after extraction.
pub struct FileStorage {
    base_dir: PathBuf,
    version_metadata: OnceLock<Arc<VersionMetadata>>,
}

impl FileStorage {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
            version_metadata: OnceLock::new(),
        }
    }

    /// Returns the base directory for this storage.
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Returns the directory for a table's batch files.
    fn table_dir(&self, table_name: &str) -> PathBuf {
        self.base_dir.join("tables").join(table_name)
    }

    /// Returns the path for a single-file batch.
    fn batch_path(&self, table_name: &str, batch_id: u64) -> PathBuf {
        self.table_dir(table_name)
            .join(format!("batch-{batch_id:06}.parquet"))
    }

    /// Returns the path for a split-part batch file.
    fn batch_part_path(&self, table_name: &str, batch_id: u64, part_idx: usize) -> PathBuf {
        self.table_dir(table_name)
            .join(format!("batch-{batch_id:06}-part-{part_idx:03}.parquet"))
    }

    /// Returns the path for `version.json`.
    fn version_metadata_path(&self) -> PathBuf {
        self.base_dir.join("version.json")
    }

    /// Caches deserialized version metadata if the cache is not yet populated.
    fn cache_version_metadata_if_unset(&self, metadata: Arc<VersionMetadata>) {
        let _ = self.version_metadata.set(metadata);
    }
}

#[async_trait]
impl DataStorage for FileStorage {
    async fn list_batches(&self, table_name: &str) -> anyhow::Result<Vec<String>> {
        let dir = self.table_dir(table_name);
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut paths = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "parquet") {
                paths.push(path.display().to_string());
            }
        }
        paths.sort();
        Ok(paths)
    }

    async fn read_batch(
        &self,
        table_name: &str,
        batch_id: u64,
        part_id: Option<usize>,
    ) -> anyhow::Result<Option<ReadResult>> {
        let path = match part_id {
            Some(pid) => self.batch_part_path(table_name, batch_id, pid),
            None => self.batch_path(table_name, batch_id),
        };

        // Read key columns from the cached version metadata (essentially free
        // after the first call since it uses OnceLock).
        let key_columns = self.read_key_columns(table_name).await?;

        // Offload the blocking file read + parquet decode to the blocking
        // thread pool so the async worker is not stalled during I/O.
        let result = tokio::task::spawn_blocking(move || -> anyhow::Result<Option<ReadResult>> {
            let raw_bytes = match std::fs::read(&path) {
                Ok(b) => b,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e.into()),
            };

            let bytes_read = raw_bytes.len() as u64;
            let bytes = bytes::Bytes::from(raw_bytes);
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
                key_columns,
            }))
        })
        .await
        .map_err(|e| anyhow::anyhow!("spawn_blocking panicked reading parquet: {e}"))??;

        if table_name == "lineitem"
            && let Some(result) = result.as_ref()
        {
            eprintln!(
                "[etl-read] table={table_name} batch_id={batch_id} rows={}",
                result.rows_read,
            );
        }

        Ok(result)
    }

    async fn write(
        &self,
        table_name: &str,
        batch_id: u64,
        batch: RecordBatch,
    ) -> anyhow::Result<WriteResult> {
        let path = self.batch_path(table_name, batch_id);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .build();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        let rows_written = batch.num_rows() as u64;
        let bytes_written = buf.len() as u64;
        std::fs::write(&path, &buf)?;

        Ok(WriteResult {
            rows_written,
            bytes_written,
            part_ids: Vec::new(),
        })
    }

    async fn write_version_metadata(&self, metadata: &VersionMetadata) -> anyhow::Result<()> {
        let path = self.version_metadata_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_vec_pretty(metadata)?;
        std::fs::write(&path, &json)?;
        self.cache_version_metadata_if_unset(Arc::new(metadata.clone()));
        Ok(())
    }

    async fn read_version_metadata(&self) -> anyhow::Result<Option<Arc<VersionMetadata>>> {
        if let Some(cached_metadata) = self.version_metadata.get() {
            return Ok(Some(cached_metadata.clone()));
        }

        let path = self.version_metadata_path();
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&path)?;
        let metadata = Arc::new(serde_json::from_slice::<VersionMetadata>(&bytes)?);
        self.cache_version_metadata_if_unset(metadata.clone());
        Ok(Some(metadata))
    }

    fn table_params(&self, table_name: &str) -> HashMap<String, serde_json::Value> {
        let mut params = HashMap::new();
        params.insert(
            "connector".to_string(),
            serde_json::Value::String("file".to_string()),
        );
        params.insert(
            "from".to_string(),
            serde_json::Value::String(self.table_dir(table_name).display().to_string()),
        );
        params.insert(
            "file_format".to_string(),
            serde_json::Value::String("parquet".to_string()),
        );
        params
    }

    fn expected_files(&self, table_name: &str, batch_ids: &[u64]) -> Vec<String> {
        batch_ids
            .iter()
            .map(|id| self.batch_path(table_name, *id).display().to_string())
            .collect()
    }
}
