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

use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema};
use async_trait::async_trait;
use duckdb::arrow::record_batch::RecordBatch as DuckDBRecordBatch;
use tokio::sync::Mutex as TokioMutex;

use super::{InsertOp, Sink};

/// ETL sink that writes transformed batches into a local DuckDB database file.
///
/// This sink creates destination tables on first write (`CREATE TABLE IF NOT EXISTS`)
/// and uses the DuckDB Appender API for fast bulk operations. Inserts use the
/// Appender directly, while updates and deletes stage data into a temporary table
/// via the Appender, apply a single SQL statement, then drop the staging table.
pub struct DuckDBSink {
    conn: Arc<Mutex<duckdb::Connection>>,
    created_tables: TokioMutex<HashSet<String>>,
}

impl DuckDBSink {
    /// Opens (or creates) a DuckDB database at the given `path` and returns a new sink.
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let conn = duckdb::Connection::open(path.as_ref())
            .map_err(|e| anyhow::anyhow!("Failed to open DuckDB database: {e}"))?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            created_tables: TokioMutex::new(HashSet::new()),
        })
    }

    fn create_table_sql(table_name: &str, schema: &Schema) -> anyhow::Result<String> {
        let columns = schema
            .fields()
            .iter()
            .map(|f| {
                let col_type = sql_type_for_arrow(f.data_type())?;
                Ok::<_, anyhow::Error>(format!("{} {col_type}", quote_identifier(f.name())))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .join(", ");

        Ok(format!(
            "CREATE TABLE IF NOT EXISTS {} ({columns})",
            quote_identifier(table_name)
        ))
    }

    async fn execute_sql_batch(&self, statements: Vec<String>) -> anyhow::Result<()> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let guard = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("DuckDB connection lock poisoned: {e}"))?;
            for sql in &statements {
                guard
                    .execute(sql, [])
                    .map_err(|e| anyhow::anyhow!("DuckDB SQL execution failed: {e}"))?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await?
    }

    /// Uses the DuckDB Appender to efficiently bulk-insert a `RecordBatch`.
    async fn append_batch(&self, table_name: &str, batch: RecordBatch) -> anyhow::Result<()> {
        let conn = Arc::clone(&self.conn);
        let table = table_name.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("DuckDB connection lock poisoned: {e}"))?;
            let mut appender = guard.appender(&table).map_err(|e| {
                anyhow::anyhow!("Failed to create DuckDB appender for '{table}': {e}")
            })?;
            appender
                .append_record_batch(batch)
                .map_err(|e| anyhow::anyhow!("Failed to append record batch to '{table}': {e}"))?;
            appender.flush().map_err(|e| {
                anyhow::anyhow!("Failed to flush DuckDB appender for '{table}': {e}")
            })?;
            Ok::<_, anyhow::Error>(())
        })
        .await?
    }

    fn create_temp_table_sql(table_name: &str, schema: &Schema) -> anyhow::Result<String> {
        let columns = schema
            .fields()
            .iter()
            .map(|f| {
                let col_type = sql_type_for_arrow(f.data_type())?;
                Ok::<_, anyhow::Error>(format!("{} {col_type}", quote_identifier(f.name())))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .join(", ");

        Ok(format!(
            "CREATE TEMPORARY TABLE IF NOT EXISTS {} ({columns})",
            quote_identifier(table_name)
        ))
    }

    fn update_from_staging_sql(
        target: &str,
        staging: &str,
        schema: &Schema,
        key_columns: &[String],
    ) -> anyhow::Result<String> {
        let target_ident = quote_identifier(target);
        let staging_ident = quote_identifier(staging);

        let set_clauses: Vec<String> = schema
            .fields()
            .iter()
            .filter(|f| !key_columns.contains(f.name()))
            .map(|f| {
                let col = quote_identifier(f.name());
                format!("{col} = __stg.{col}")
            })
            .collect();

        if set_clauses.is_empty() {
            anyhow::bail!("Update requires at least one non-key column in batch schema");
        }

        let join_predicates: Vec<String> = key_columns
            .iter()
            .map(|k| {
                let col = quote_identifier(k);
                format!("{target_ident}.{col} IS NOT DISTINCT FROM __stg.{col}")
            })
            .collect();

        Ok(format!(
            "UPDATE {target_ident} SET {} FROM {staging_ident} AS __stg WHERE {}",
            set_clauses.join(", "),
            join_predicates.join(" AND ")
        ))
    }

    fn delete_using_staging_sql(
        target: &str,
        staging: &str,
        key_columns: &[String],
    ) -> anyhow::Result<String> {
        let target_ident = quote_identifier(target);
        let staging_ident = quote_identifier(staging);

        let join_predicates: Vec<String> = key_columns
            .iter()
            .map(|k| {
                let col = quote_identifier(k);
                format!("{target_ident}.{col} IS NOT DISTINCT FROM __stg.{col}")
            })
            .collect();

        Ok(format!(
            "DELETE FROM {target_ident} USING {staging_ident} AS __stg WHERE {}",
            join_predicates.join(" AND ")
        ))
    }

    /// Applies an update or delete by staging batch data into a temporary table
    /// via the Appender, executing a single SQL statement, then cleaning up.
    async fn apply_via_staging(
        &self,
        table_name: &str,
        batch_id: u64,
        batch: RecordBatch,
        key_columns: &[String],
        is_update: bool,
    ) -> anyhow::Result<()> {
        if key_columns.is_empty() {
            anyhow::bail!("Update/Delete requires at least one key column");
        }

        let schema = batch.schema();
        for key in key_columns {
            schema
                .index_of(key)
                .map_err(|_| anyhow::anyhow!("Key column '{key}' not found in batch schema"))?;
        }

        let staging = format!("__etl_staging_{batch_id}");
        let create_sql = Self::create_temp_table_sql(&staging, &schema)?;
        let apply_sql = if is_update {
            Self::update_from_staging_sql(table_name, &staging, &schema, key_columns)?
        } else {
            Self::delete_using_staging_sql(table_name, &staging, key_columns)?
        };
        let drop_sql = format!("DROP TABLE IF EXISTS {}", quote_identifier(&staging));

        let conn = Arc::clone(&self.conn);
        let staging_name = staging.clone();
        tokio::task::spawn_blocking(move || {
            let guard = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("DuckDB connection lock poisoned: {e}"))?;

            // Create temporary staging table.
            guard.execute(&create_sql, []).map_err(|e| {
                anyhow::anyhow!("Failed to create staging table '{staging_name}': {e}")
            })?;

            // Bulk-insert the batch into the staging table via the Appender.
            {
                let mut appender = guard.appender(&staging_name).map_err(|e| {
                    anyhow::anyhow!("Failed to create appender for staging table: {e}")
                })?;
                appender
                    .append_record_batch(batch)
                    .map_err(|e| anyhow::anyhow!("Failed to append to staging table: {e}"))?;
                appender
                    .flush()
                    .map_err(|e| anyhow::anyhow!("Failed to flush staging appender: {e}"))?;
            }

            // Apply the update or delete from the staging table to the target.
            guard
                .execute(&apply_sql, [])
                .map_err(|e| anyhow::anyhow!("Failed to apply staged operation: {e}"))?;

            // Clean up the staging table.
            guard
                .execute(&drop_sql, [])
                .map_err(|e| anyhow::anyhow!("Failed to drop staging table: {e}"))?;

            Ok::<_, anyhow::Error>(())
        })
        .await?
    }
}

impl DuckDBSink {
    /// Executes an arbitrary SQL query against the underlying DuckDB connection
    /// and returns all result rows collected into `RecordBatch`es.
    pub async fn query(&self, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("DuckDB connection lock poisoned: {e}"))?;
            let mut stmt = guard
                .prepare(&sql)
                .map_err(|e| anyhow::anyhow!("Failed to prepare DuckDB query: {e}"))?;
            let duckdb_batches: Vec<DuckDBRecordBatch> = stmt
                .query_arrow([])
                .map_err(|e| anyhow::anyhow!("Failed to execute DuckDB query: {e}"))?
                .collect();

            // Convert from duckdb::arrow RecordBatch to arrow::array::RecordBatch
            // via IPC serialization round-trip for crate compatibility.
            let mut batches = Vec::with_capacity(duckdb_batches.len());
            for db_batch in duckdb_batches {
                let mut buf = Vec::new();
                {
                    let mut writer = duckdb::arrow::ipc::writer::FileWriter::try_new(
                        &mut buf,
                        &db_batch.schema(),
                    )
                    .map_err(|e| anyhow::anyhow!("IPC write init failed: {e}"))?;
                    writer
                        .write(&db_batch)
                        .map_err(|e| anyhow::anyhow!("IPC write failed: {e}"))?;
                    writer
                        .finish()
                        .map_err(|e| anyhow::anyhow!("IPC finish failed: {e}"))?;
                }
                let reader =
                    arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(buf), None)
                        .map_err(|e| anyhow::anyhow!("IPC read failed: {e}"))?;
                for batch in reader {
                    batches.push(batch.map_err(|e| anyhow::anyhow!("IPC batch read failed: {e}"))?);
                }
            }
            Ok::<_, anyhow::Error>(batches)
        })
        .await?
    }
}

#[async_trait]
impl Sink for DuckDBSink {
    async fn write(
        &self,
        table_name: &str,
        batch_id: u64,
        batch: RecordBatch,
        op: InsertOp,
    ) -> anyhow::Result<()> {
        // Ensure the target table exists for insert/update operations.
        let should_ensure_table = matches!(op, InsertOp::Insert | InsertOp::Update { .. });
        let mut newly_created = false;

        if should_ensure_table {
            let created = self.created_tables.lock().await;
            if !created.contains(table_name) {
                let create_sql = Self::create_table_sql(table_name, &batch.schema())?;
                self.execute_sql_batch(vec![create_sql]).await?;
                newly_created = true;
            }
        }

        let num_rows = batch.num_rows();
        if num_rows > 0 {
            match &op {
                InsertOp::Insert => {
                    // Use the DuckDB Appender API for fast bulk inserts.
                    self.append_batch(table_name, batch).await?;
                }
                InsertOp::Update { key_columns } => {
                    // Stage into a temp table, apply a single UPDATE...FROM, then drop.
                    self.apply_via_staging(table_name, batch_id, batch, key_columns, true)
                        .await?;
                }
                InsertOp::Delete { key_columns } => {
                    // Stage into a temp table, apply a single DELETE...USING, then drop.
                    self.apply_via_staging(table_name, batch_id, batch, key_columns, false)
                        .await?;
                }
            }
        }

        if newly_created {
            let mut created = self.created_tables.lock().await;
            created.insert(table_name.to_string());
        }

        Ok(())
    }
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn sql_type_for_arrow(data_type: &DataType) -> anyhow::Result<String> {
    match data_type {
        DataType::Boolean => Ok("BOOLEAN".to_string()),
        DataType::Int8 => Ok("TINYINT".to_string()),
        DataType::Int16 => Ok("SMALLINT".to_string()),
        DataType::Int32 => Ok("INTEGER".to_string()),
        DataType::UInt8 => Ok("UTINYINT".to_string()),
        DataType::UInt16 => Ok("USMALLINT".to_string()),
        DataType::UInt32 => Ok("UINTEGER".to_string()),
        DataType::Int64 => Ok("BIGINT".to_string()),
        DataType::UInt64 => Ok("UBIGINT".to_string()),
        DataType::Float32 => Ok("FLOAT".to_string()),
        DataType::Float64 => Ok("DOUBLE".to_string()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok("VARCHAR".to_string()),
        DataType::Date32 => Ok("DATE".to_string()),
        DataType::Timestamp(_, _) => Ok("TIMESTAMP".to_string()),
        DataType::Decimal128(p, s) => Ok(format!("DECIMAL({p}, {s})")),
        other => anyhow::bail!("Unsupported Arrow data type for DuckDB sink: {other:?}"),
    }
}
