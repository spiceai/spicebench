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

use std::collections::HashMap;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use object_store::PutPayload;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::storage::s3::S3Storage;

use super::{Target, WriteResult};

#[async_trait]
impl Target for S3Storage {
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
}
