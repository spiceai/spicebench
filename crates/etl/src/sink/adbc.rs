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

use std::collections::HashMap;
use std::sync::Mutex;

use adbc_client::{AdbcConnection, IngestMode};
use arrow::array::RecordBatch;
use async_trait::async_trait;

use super::{InsertOp, Sink};

/// ETL sink that writes transformed batches directly to an ADBC target using
/// the bulk ingest protocol.
pub struct AdbcSink {
    conn: Mutex<AdbcConnection>,
    target_db_schema: Option<String>,
}

impl AdbcSink {
    /// Creates a new [`AdbcSink`] backed by a single ADBC connection.
    pub fn new(
        driver_name: &str,
        db_kwargs: HashMap<String, serde_json::Value>,
        target_db_schema: Option<String>,
    ) -> anyhow::Result<Self> {
        let conn = AdbcConnection::create(driver_name, db_kwargs)
            .map_err(|e| anyhow::anyhow!("Failed to create ADBC connection: {e}"))?;

        Ok(Self {
            conn: Mutex::new(conn),
            target_db_schema,
        })
    }
}

#[async_trait]
impl Sink for AdbcSink {
    async fn write(
        &self,
        table_name: &str,
        _batch_id: u64,
        batch: RecordBatch,
        op: InsertOp,
        _partition_columns: Vec<String>,
    ) -> anyhow::Result<()> {
        match op {
            InsertOp::Insert => {}
            InsertOp::Update { .. } | InsertOp::Delete { .. } => {
                anyhow::bail!(
                    "ADBC sink only supports Insert operations with bulk ingest, got {op:?}"
                );
            }
        }

        if batch.num_rows() == 0 {
            return Ok(());
        }

        let mut conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("ADBC connection lock poisoned: {e}"))?;

        conn.bulk_ingest(
            table_name,
            self.target_db_schema.as_deref(),
            IngestMode::CreateAppend,
            batch,
        )
        .map_err(|e| anyhow::anyhow!("ADBC bulk ingest failed for '{table_name}': {e}"))?;

        Ok(())
    }
}
