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
#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use adbc_client::AdbcConnection;
use test_framework::{
    anyhow,
    execution::{ExecutionResult, QueryExecutor},
};

/// Executes queries directly against a database via ADBC.
///
/// `AdbcConnection` is not `Send`/`Sync` (the underlying `ManagedConnection`
/// uses raw FFI pointers), so we wrap it in `Arc<Mutex<>>` to satisfy the
/// `Send + Sync` bounds required by [`QueryExecutor`] and to allow cloning
/// the executor across concurrent tasks.
pub(crate) struct AdbcDirectQueryExecutor {
    conn: Arc<Mutex<AdbcConnection>>,
}

impl AdbcDirectQueryExecutor {
    pub(crate) fn new(conn: AdbcConnection) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
        }
    }

    pub(crate) fn from_shared(conn: Arc<Mutex<AdbcConnection>>) -> Self {
        Self { conn }
    }
}

impl Clone for AdbcDirectQueryExecutor {
    fn clone(&self) -> Self {
        Self {
            conn: Arc::clone(&self.conn),
        }
    }
}

#[async_trait::async_trait]
impl QueryExecutor for AdbcDirectQueryExecutor {
    async fn execute(
        &self,
        query: &test_framework::queries::Query,
    ) -> anyhow::Result<ExecutionResult> {
        let mut sql = query.to_sql_with_inlined_params().to_string();
        sql = sql
            .trim_end()
            .strip_suffix(';')
            .unwrap_or(&sql)
            .trim_end()
            .to_string();

        let conn = Arc::clone(&self.conn);

        // `AdbcConnection::query()` is synchronous (ADBC has no async API),
        // so we run it on the blocking thread pool to avoid stalling the tokio runtime.
        let (duration, batches) = tokio::task::spawn_blocking(move || {
            let start = std::time::Instant::now();
            let mut guard = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {e}"))?;
            let batches = guard.query(&sql).map_err(|e| anyhow::anyhow!("{e}"))?;
            Ok::<_, anyhow::Error>((start.elapsed(), batches))
        })
        .await??;

        let row_count: usize = batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();

        Ok(ExecutionResult {
            duration,
            row_count,
            batches: Some(batches),
        })
    }

    fn name(&self) -> &'static str {
        "adbc_direct"
    }

    fn supports_validation(&self) -> bool {
        true
    }

    fn clone_box(&self) -> Box<dyn QueryExecutor> {
        Box::new(self.clone())
    }
}
