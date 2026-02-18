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
use data_generation::storage::{DataStorage, s3::S3Storage};

pub enum InsertOp {
    Overwrite,
    Append,
    Delete
}

#[async_trait]
pub trait Sink: Send + Sync + 'static {
    async fn write(&self, table_name: &str, batch_id: u64, batch: RecordBatch, insert_op: InsertOp) -> anyhow::Result<()>;
    fn table_params(&self, table_name: &str) -> HashMap<String, serde_json::Value>;
}

#[async_trait]
impl Sink for S3Storage {
    async fn write(&self, table_name: &str, batch_id: u64, batch: RecordBatch, insert_op: InsertOp) -> anyhow::Result<()> {
        // For simplicity, S3Storage only supports Append (i.e. writing new batches, create operations)
        
        match insert_op {
            InsertOp::Append => {
                DataStorage::write(self, table_name, batch_id, batch).await?;
                Ok(())
            },
            _ => anyhow::bail!("S3Storage only supports Append insert operations"),
        }
    }

    fn table_params(&self, table_name: &str) -> HashMap<String, serde_json::Value> {
        DataStorage::table_params(self, table_name)
    }
}