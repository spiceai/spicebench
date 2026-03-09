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

use adbc_client::{AdbcConnection, AdbcConnectionPool, IngestMode, create_pool};
use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, Schema};
use arrow_cast::display::array_value_to_string;
use async_trait::async_trait;
use system_adapter_protocol::DatasetConfig;

use super::{InsertOp, Sink};

/// Conservative cap for per-ingest Arrow payload size to avoid exceeding
/// FlightSQL/gRPC max message limits (commonly 16 MiB).
const MAX_ADBC_INGEST_BATCH_BYTES: usize = 75 * 1024 * 1024;
const MAX_ADBC_INGEST_BATCH_BYTES_ENV: &str = "SPICEBENCH_ADBC_MAX_INGEST_BATCH_BYTES";

/// Default number of connections in the ADBC sink pool.
const DEFAULT_ADBC_SINK_POOL_SIZE: u32 = 8;
const ADBC_SINK_POOL_SIZE_ENV: &str = "SPICEBENCH_ADBC_SINK_POOL_SIZE";

/// ETL sink that writes transformed batches directly to an ADBC target.
///
/// Inserts use ADBC bulk ingest, while updates and deletes execute row-level
/// SQL statements derived from key columns in each batch.
///
/// Backed by a connection pool to allow concurrent writes across tables.
pub struct AdbcSink {
    pool: AdbcConnectionPool,
    target_db_catalog: Option<String>,
    target_db_schema: Option<String>,
}

impl AdbcSink {
    fn max_ingest_batch_bytes() -> usize {
        std::env::var(MAX_ADBC_INGEST_BATCH_BYTES_ENV)
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(MAX_ADBC_INGEST_BATCH_BYTES)
    }

    fn pool_size() -> u32 {
        std::env::var(ADBC_SINK_POOL_SIZE_ENV)
            .ok()
            .and_then(|raw| raw.parse::<u32>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_ADBC_SINK_POOL_SIZE)
    }

    /// Creates a new [`AdbcSink`] backed by a connection pool.
    pub fn new(
        driver_name: &str,
        db_kwargs: HashMap<String, serde_json::Value>,
        target_db_catalog: Option<String>,
        target_db_schema: Option<String>,
    ) -> anyhow::Result<Self> {
        let pool_size = Self::pool_size();
        let pool = create_pool(driver_name, db_kwargs, Some(pool_size))
            .map_err(|e| anyhow::anyhow!("Failed to create ADBC connection pool: {e}"))?;
        eprintln!("[adbc] Connection pool created (driver: {driver_name}, size: {pool_size})");

        Ok(Self {
            pool,
            target_db_catalog,
            target_db_schema,
        })
    }

    fn quote_identifier(value: &str) -> String {
        format!("\"{}\"", value.replace('"', "\"\""))
    }

    fn postgres_type_for_arrow(data_type: &DataType) -> anyhow::Result<String> {
        let ty = match data_type {
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::Int8 | DataType::Int16 => "SMALLINT".to_string(),
            DataType::Int32 => "INTEGER".to_string(),
            DataType::Int64 => "BIGINT".to_string(),
            DataType::UInt8 => "SMALLINT".to_string(),
            DataType::UInt16 => "INTEGER".to_string(),
            DataType::UInt32 => "BIGINT".to_string(),
            DataType::UInt64 => "NUMERIC(20,0)".to_string(),
            DataType::Float16 | DataType::Float32 => "REAL".to_string(),
            DataType::Float64 => "DOUBLE PRECISION".to_string(),
            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                format!("NUMERIC({precision},{scale})")
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "TEXT".to_string(),
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "BYTEA".to_string(),
            DataType::Date32 | DataType::Date64 => "DATE".to_string(),
            DataType::Time32(_) | DataType::Time64(_) => "TIME".to_string(),
            DataType::Timestamp(_, timezone) => {
                if timezone.is_some() {
                    "TIMESTAMPTZ".to_string()
                } else {
                    "TIMESTAMP".to_string()
                }
            }
            DataType::Duration(_) | DataType::Interval(_) => "INTERVAL".to_string(),
            other => {
                anyhow::bail!(
                    "Cannot map Arrow type '{other:?}' to a PostgreSQL-compatible CREATE TABLE type"
                )
            }
        };

        Ok(ty)
    }

    fn target_table_identifier(&self, table_name: &str) -> String {
        let mut parts = Vec::with_capacity(3);

        if let Some(catalog) = self.target_db_catalog.as_deref()
            && !catalog.is_empty()
        {
            parts.push(Self::quote_identifier(catalog));
        }

        if let Some(schema) = self.target_db_schema.as_deref()
            && !schema.is_empty()
        {
            parts.push(Self::quote_identifier(schema));
        }

        parts.push(Self::quote_identifier(table_name));
        parts.join(".")
    }

    fn target_table_ingest_name(&self, table_name: &str) -> String {
        self.target_table_identifier_unquoted(table_name)
    }

    fn target_table_identifier_unquoted(&self, table_name: &str) -> String {
        let mut parts = Vec::with_capacity(3);

        if let Some(catalog) = self.target_db_catalog.as_deref()
            && !catalog.is_empty()
        {
            parts.push(catalog.to_string());
        }

        if let Some(schema) = self.target_db_schema.as_deref()
            && !schema.is_empty()
        {
            parts.push(schema.to_string());
        }

        parts.push(table_name.to_string());
        parts.join(".")
    }

    fn create_table_sql(&self, table_name: &str, schema: &Schema) -> anyhow::Result<String> {
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                let col_ident = Self::quote_identifier(field.name());
                let col_type = Self::postgres_type_for_arrow(field.data_type())?;
                let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
                Ok::<_, anyhow::Error>(format!("{col_ident} {col_type}{nullable}"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .join(", ");

        Ok(format!(
            "CREATE TABLE IF NOT EXISTS {} ({columns})",
            self.target_table_identifier(table_name)
        ))
    }

    pub fn create_tables_from_dataset_configs(
        &self,
        datasets: &HashMap<String, DatasetConfig>,
    ) -> anyhow::Result<()> {
        let mut statements = Vec::with_capacity(datasets.len());
        let mut table_names: Vec<_> = datasets.keys().cloned().collect();
        table_names.sort();

        for table_name in table_names {
            let config = datasets.get(&table_name).ok_or_else(|| {
                anyhow::anyhow!("Missing dataset config for table '{table_name}'")
            })?;
            statements.push(self.create_table_sql(&table_name, config.schema.as_ref())?);
        }

        let mut conn = self
            .pool
            .get()
            .map_err(|e| anyhow::anyhow!("Failed to get ADBC connection from pool: {e}"))?;

        for sql in statements {
            conn.execute_update(&sql)
                .map_err(|e| anyhow::anyhow!("ADBC create table execution failed: {e}"))?;
        }

        Ok(())
    }

    fn serialized_batch_size(batch: &RecordBatch) -> anyhow::Result<usize> {
        let mut buf = Vec::new();
        {
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
                    .map_err(|e| anyhow::anyhow!("Failed to create Arrow stream writer: {e}"))?;
            writer
                .write(batch)
                .map_err(|e| anyhow::anyhow!("Failed to serialize batch for size estimate: {e}"))?;
            writer
                .finish()
                .map_err(|e| anyhow::anyhow!("Failed to finish Arrow stream writer: {e}"))?;
        }
        Ok(buf.len())
    }

    fn split_for_size(batch: &RecordBatch, max_bytes: usize) -> anyhow::Result<Vec<RecordBatch>> {
        let mut out = Vec::new();
        let mut start = 0usize;
        let total_rows = batch.num_rows();

        while start < total_rows {
            let mut end = total_rows;
            let mut accepted: Option<RecordBatch> = None;

            while end > start {
                let candidate = batch.slice(start, end - start);
                let size = Self::serialized_batch_size(&candidate)?;
                if size <= max_bytes {
                    accepted = Some(candidate);
                    break;
                }

                let span = end - start;
                end = start + (span / 2);
            }

            let accepted = accepted.ok_or_else(|| {
                anyhow::anyhow!(
                    "Single-row batch exceeds max ingest payload ({max_bytes} bytes). Consider increasing FlightSQL max message size."
                )
            })?;

            start += accepted.num_rows();
            out.push(accepted);
        }

        Ok(out)
    }

    fn is_message_too_large_error(message: &str) -> bool {
        message.contains("ResourceExhausted") || message.contains("message larger than max")
    }

    fn bulk_ingest_with_retry(
        &self,
        conn: &mut AdbcConnection,
        table_name: &str,
        batch: RecordBatch,
    ) -> anyhow::Result<()> {
        let ingest_table_name = self.target_table_ingest_name(table_name);
        let target_db_catalog = self
            .target_db_catalog
            .as_deref()
            .and_then(|catalog| (!catalog.is_empty()).then_some(catalog));
        let target_db_schema = self
            .target_db_schema
            .as_deref()
            .and_then(|schema| (!schema.is_empty()).then_some(schema));

        let ingest_result = if target_db_catalog.is_some() || target_db_schema.is_some() {
            match conn.bulk_ingest(
                &ingest_table_name,
                None,
                None,
                IngestMode::CreateAppend,
                batch.clone(),
            ) {
                Ok(result) => Ok(result),
                Err(qualified_err) => {
                    let qualified_message = qualified_err.to_string();
                    if Self::is_message_too_large_error(&qualified_message) {
                        Err(qualified_err)
                    } else {
                        conn.bulk_ingest(
                            table_name,
                            target_db_catalog,
                            target_db_schema,
                            IngestMode::CreateAppend,
                            batch.clone(),
                        )
                    }
                }
            }
        } else {
            conn.bulk_ingest(
                table_name,
                target_db_catalog,
                target_db_schema,
                IngestMode::CreateAppend,
                batch.clone(),
            )
        };

        match ingest_result {
            Ok(_) => Ok(()),
            Err(e) => {
                let message = e.to_string();
                if Self::is_message_too_large_error(&message) {
                    if batch.num_rows() <= 1 {
                        anyhow::bail!(
                            "ADBC bulk ingest failed for source table '{table_name}' (ingest target '{ingest_table_name}'): single-row batch still exceeds FlightSQL message limit: {message}. Configure a larger FlightSQL max message size (e.g. adbc.flight.sql.client_option.with_max_msg_size)."
                        );
                    }

                    let mid = batch.num_rows() / 2;
                    let left = batch.slice(0, mid);
                    let right = batch.slice(mid, batch.num_rows() - mid);
                    self.bulk_ingest_with_retry(conn, table_name, left)?;
                    self.bulk_ingest_with_retry(conn, table_name, right)?;
                    Ok(())
                } else {
                    anyhow::bail!(
                        "ADBC bulk ingest failed for source table '{table_name}' (ingest target '{ingest_table_name}'): {message}"
                    )
                }
            }
        }
    }

    fn ingest_insert_batch(
        &self,
        conn: &mut AdbcConnection,
        table_name: &str,
        batch: RecordBatch,
    ) -> anyhow::Result<()> {
        let max_ingest_bytes = Self::max_ingest_batch_bytes();
        let size = Self::serialized_batch_size(&batch)?;
        if size <= max_ingest_bytes {
            self.bulk_ingest_with_retry(conn, table_name, batch)?;

            return Ok(());
        }

        let split_batches = Self::split_for_size(&batch, max_ingest_bytes)?;
        for sub_batch in split_batches {
            self.bulk_ingest_with_retry(conn, table_name, sub_batch)?;
        }

        Ok(())
    }

    fn sql_literal(array: &dyn Array, row: usize) -> anyhow::Result<String> {
        if array.is_null(row) {
            return Ok("NULL".to_string());
        }

        let raw = array_value_to_string(array, row)
            .map_err(|e| anyhow::anyhow!("Failed to render key value at row {row}: {e}"))?;

        let value = match array.data_type() {
            DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => raw,
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::Interval(_) => format!("'{}'", raw.replace('\'', "''")),
            other => {
                anyhow::bail!(
                    "Update/Delete is not supported for column type '{other:?}'. Use primitive/string/date/time columns."
                )
            }
        };

        Ok(value)
    }

    fn null_safe_predicate_for_literal(identifier: &str, literal: &str) -> String {
        if literal == "NULL" {
            format!("{identifier} IS NULL")
        } else {
            format!("{identifier} = {literal}")
        }
    }

    fn delete_sql_for_row(
        &self,
        table_name: &str,
        batch: &RecordBatch,
        row: usize,
        key_columns: &[String],
    ) -> anyhow::Result<String> {
        let mut predicates = Vec::with_capacity(key_columns.len());
        let schema = batch.schema();
        for key in key_columns {
            let idx = schema.index_of(key).map_err(|_| {
                anyhow::anyhow!("Key column '{key}' not found in delete batch schema")
            })?;
            let column = batch.column(idx);
            let key_ident = Self::quote_identifier(key);
            let literal = Self::sql_literal(column.as_ref(), row)?;
            predicates.push(Self::null_safe_predicate_for_literal(&key_ident, &literal));
        }

        Ok(format!(
            "DELETE FROM {} WHERE {}",
            self.target_table_identifier(table_name),
            predicates.join(" AND ")
        ))
    }

    fn update_sql_for_row(
        &self,
        table_name: &str,
        batch: &RecordBatch,
        row: usize,
        key_columns: &[String],
    ) -> anyhow::Result<String> {
        let schema = batch.schema();

        let mut predicates = Vec::with_capacity(key_columns.len());
        for key in key_columns {
            let key_idx = schema.index_of(key).map_err(|_| {
                anyhow::anyhow!("Key column '{key}' not found in update batch schema")
            })?;
            let key_col = batch.column(key_idx);
            let key_ident = Self::quote_identifier(key);
            let key_literal = Self::sql_literal(key_col.as_ref(), row)?;
            predicates.push(Self::null_safe_predicate_for_literal(
                &key_ident,
                &key_literal,
            ));
        }

        let mut set_clauses = Vec::new();
        for (column_idx, field) in schema.fields().iter().enumerate() {
            if key_columns.iter().any(|key| key == field.name()) {
                continue;
            }

            let col_ident = Self::quote_identifier(field.name());
            let literal = Self::sql_literal(batch.column(column_idx).as_ref(), row)?;
            set_clauses.push(format!("{col_ident} = {literal}"));
        }

        if set_clauses.is_empty() {
            anyhow::bail!(
                "Update requires at least one non-key column in batch schema for table '{table_name}'"
            );
        }

        Ok(format!(
            "UPDATE {} SET {} WHERE {}",
            self.target_table_identifier(table_name),
            set_clauses.join(", "),
            predicates.join(" AND ")
        ))
    }

    fn delete_sql_statements(
        &self,
        table_name: &str,
        batch: &RecordBatch,
        key_columns: &[String],
    ) -> anyhow::Result<Vec<String>> {
        if key_columns.is_empty() {
            anyhow::bail!("Delete requires at least one key column");
        }

        (0..batch.num_rows())
            .map(|row| self.delete_sql_for_row(table_name, batch, row, key_columns))
            .collect()
    }

    fn update_sql_statements(
        &self,
        table_name: &str,
        batch: &RecordBatch,
        key_columns: &[String],
    ) -> anyhow::Result<Vec<String>> {
        if key_columns.is_empty() {
            anyhow::bail!("Update requires at least one key column");
        }

        (0..batch.num_rows())
            .map(|row| self.update_sql_for_row(table_name, batch, row, key_columns))
            .collect()
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
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let mut conn = self
            .pool
            .get()
            .map_err(|e| anyhow::anyhow!("Failed to get ADBC connection from pool: {e}"))?;

        match op {
            InsertOp::Insert => {
                self.ingest_insert_batch(&mut conn, table_name, batch)?;
            }
            InsertOp::Delete { key_columns } => {
                let statements = self.delete_sql_statements(table_name, &batch, &key_columns)?;
                for sql in statements {
                    conn.execute_update(&sql).map_err(|e| {
                        anyhow::anyhow!("ADBC delete execution failed for '{table_name}': {e}")
                    })?;
                }
            }
            InsertOp::Update { key_columns } => {
                let statements = self.update_sql_statements(table_name, &batch, &key_columns)?;
                for sql in statements {
                    conn.execute_update(&sql).map_err(|e| {
                        anyhow::anyhow!("ADBC update execution failed for '{table_name}': {e}")
                    })?;
                }
            }
        }

        Ok(())
    }
}
