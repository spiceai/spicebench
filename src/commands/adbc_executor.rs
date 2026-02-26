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

use adbc_client::AdbcConnectionPool;
use test_framework::{
    anyhow,
    execution::{ExecutionResult, QueryExecutor},
};

/// Executes queries directly against a database via ADBC.
///
/// Each call to [`execute`] checks out a connection from the pool,
/// runs the query, and returns the connection when the guard is dropped.
/// This allows concurrent workers to issue queries in parallel.
#[derive(Clone)]
pub(crate) struct AdbcDirectQueryExecutor {
    pool: AdbcConnectionPool,
}

impl AdbcDirectQueryExecutor {
    pub(crate) fn new(pool: AdbcConnectionPool) -> Self {
        Self { pool }
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

        let pool = self.pool.clone();

        // `AdbcConnection::query()` is synchronous (ADBC has no async API),
        // so we run it on the blocking thread pool to avoid stalling the tokio runtime.
        let (duration, batches) = tokio::task::spawn_blocking(move || {
            let start = std::time::Instant::now();
            let mut conn = pool.get().map_err(|e| anyhow::anyhow!("{e}"))?;
            let batches = conn.query(&sql).map_err(|e| anyhow::anyhow!("{e}"))?;
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
