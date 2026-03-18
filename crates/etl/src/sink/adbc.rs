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
use std::time::Instant;

use adbc_client::{
    AdbcConnection, AdbcConnectionManager, AdbcConnectionPool, IngestMode, create_pool,
};
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

/// When set to a positive integer, deletes are batched into
/// `DELETE … WHERE key IN (…)` statements of at most this many rows.
/// When unset or empty, deletes use the original row-by-row approach.
const ADBC_DELETE_BATCH_SIZE_ENV: &str = "SPICEBENCH_ADBC_DELETE_BATCH_SIZE";

/// Controls how UPDATE operations are executed.
///
/// - `statement`          — row-by-row `UPDATE … SET … WHERE …` statements (default)
/// - `staging_table`      — bulk ingest into temp staging table + single `MERGE INTO`
/// - `bulk_ingest_upsert` — bulk ingest directly into the target table (relies on the
///                          target system's `on_conflict: upsert` or equivalent to merge)
const ADBC_UPDATE_STRATEGY_ENV: &str = "SPICEBENCH_ADBC_UPDATE_STRATEGY";

/// Strategy for executing UPDATE operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpdateStrategy {
    /// Row-by-row `UPDATE … SET … WHERE …` SQL statements.
    Statement,
    /// Bulk ingest into a temporary staging table, then `MERGE INTO target USING staging …`.
    StagingTable,
    /// Bulk ingest directly into the target table, relying on the target system
    /// to handle upsert semantics (e.g. Spice Cloud `on_conflict: upsert`).
    BulkIngestUpsert,
}

impl UpdateStrategy {
    fn from_env() -> anyhow::Result<Self> {
        match std::env::var(ADBC_UPDATE_STRATEGY_ENV).ok().as_deref() {
            Some(val) => match val.to_lowercase().as_str() {
                "statement" => Ok(Self::Statement),
                "staging_table" => Ok(Self::StagingTable),
                "bulk_ingest_upsert" => Ok(Self::BulkIngestUpsert),
                other => anyhow::bail!(
                    "Unknown update strategy '{other}'. Valid values for {ADBC_UPDATE_STRATEGY_ENV}: statement, staging_table, bulk_ingest_upsert"
                ),
            },
            None => Ok(Self::Statement),
        }
    }
}

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
    row_counts: Mutex<HashMap<String, u64>>,
    /// Character used to quote SQL identifiers (e.g. '"' for ANSI, '`' for Databricks).
    identifier_quote_char: char,
    /// Whether Int64/UInt64 literals need an `L` suffix (Databricks).
    bigint_suffix: bool,
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

        let identifier_quote_char = AdbcConnectionManager::identifier_quote_style(driver_name);
        let bigint_suffix = AdbcConnectionManager::bigint_suffix(driver_name);

        Ok(Self {
            pool,
            target_db_catalog,
            target_db_schema,
            row_counts: Mutex::new(HashMap::new()),
            identifier_quote_char,
            bigint_suffix,
        })
    }

    fn quote_identifier(&self, value: &str) -> String {
        Self::quote_ident(value, self.identifier_quote_char)
    }

    fn quote_ident(value: &str, quote_char: char) -> String {
        let escaped = value.replace(quote_char, &format!("{quote_char}{quote_char}"));
        format!("{quote_char}{escaped}{quote_char}")
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
            parts.push(self.quote_identifier(catalog));
        }

        if let Some(schema) = self.target_db_schema.as_deref()
            && !schema.is_empty()
        {
            parts.push(self.quote_identifier(schema));
        }

        parts.push(self.quote_identifier(table_name));
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
                let col_ident = self.quote_identifier(field.name());
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

    /// Render a cell value as a SQL literal.
    ///
    /// When `bigint_suffix` is `true`, integer literals are suffixed with `L` so
    /// that Databricks treats them as BIGINT instead of INT (avoids
    /// `DATATYPE_MISMATCH` in composite-key tuple comparisons).
    fn sql_literal(array: &dyn Array, row: usize, bigint_suffix: bool) -> anyhow::Result<String> {
        if array.is_null(row) {
            return Ok("NULL".to_string());
        }

        let raw = array_value_to_string(array, row)
            .map_err(|e| anyhow::anyhow!("Failed to render key value at row {row}: {e}"))?;

        let value = match array.data_type() {
            DataType::Boolean
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => raw,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32 => raw,
            DataType::Int64 | DataType::UInt64 => {
                if bigint_suffix {
                    format!("{raw}L")
                } else {
                    raw
                }
            }
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
            let key_ident = self.quote_identifier(key);
            let literal = Self::sql_literal(column.as_ref(), row, false)?;
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
            let key_ident = self.quote_identifier(key);
            let key_literal = Self::sql_literal(key_col.as_ref(), row, false)?;
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

            let col_ident = self.quote_identifier(field.name());
            let literal = Self::sql_literal(batch.column(column_idx).as_ref(), row, false)?;
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

    /// Returns the delete batch size from the env var, or `None` if unset/empty
    /// (meaning row-by-row mode).
    fn delete_batch_size() -> Option<usize> {
        std::env::var(ADBC_DELETE_BATCH_SIZE_ENV)
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|value| *value > 0)
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

        let batch_size = match Self::delete_batch_size() {
            Some(size) => size,
            None => {
                // Original row-by-row approach.
                return (0..batch.num_rows())
                    .map(|row| self.delete_sql_for_row(table_name, batch, row, key_columns))
                    .collect();
            }
        };

        let schema = batch.schema();
        let key_indices: Vec<usize> = key_columns
            .iter()
            .map(|key| {
                schema.index_of(key).map_err(|_| {
                    anyhow::anyhow!("Key column '{key}' not found in delete batch schema")
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        Self::batched_delete_sql(
            &self.target_table_identifier(table_name),
            batch,
            batch_size,
            key_columns,
            &key_indices,
            self.identifier_quote_char,
            self.bigint_suffix,
        )
    }

    /// Build batched `DELETE` statements, chunked by `batch_size`.
    ///
    /// Single key:    `DELETE FROM t WHERE k IN (v1, v2, …)`
    /// Composite key: `DELETE FROM t WHERE (k1, k2) IN ((v1, v2), …)`
    ///
    /// When `bigint_suffix` is `true` (Databricks), Int64/UInt64 literals are
    /// suffixed with `L` to force BIGINT and avoid `DATATYPE_MISMATCH`.
    fn batched_delete_sql(
        table_ident: &str,
        batch: &RecordBatch,
        batch_size: usize,
        key_columns: &[String],
        key_indices: &[usize],
        identifier_quote_char: char,
        bigint_suffix: bool,
    ) -> anyhow::Result<Vec<String>> {
        let num_rows = batch.num_rows();
        let mut statements = Vec::new();
        let mut start = 0;

        while start < num_rows {
            let end = (start + batch_size).min(num_rows);

            let sql = if key_columns.len() == 1 {
                let key_ident = Self::quote_ident(&key_columns[0], identifier_quote_char);
                let values: Vec<String> = (start..end)
                    .map(|row| Self::sql_literal(batch.column(key_indices[0]).as_ref(), row, false))
                    .collect::<anyhow::Result<Vec<_>>>()?;
                format!(
                    "DELETE FROM {table_ident} WHERE {key_ident} IN ({})",
                    values.join(", ")
                )
            } else {
                let key_idents: Vec<String> = key_columns
                    .iter()
                    .map(|k| Self::quote_ident(k, identifier_quote_char))
                    .collect();
                let tuples: Vec<String> = (start..end)
                    .map(|row| {
                        let vals: Vec<String> = key_indices
                            .iter()
                            .map(|&idx| {
                                Self::sql_literal(batch.column(idx).as_ref(), row, bigint_suffix)
                            })
                            .collect::<anyhow::Result<Vec<_>>>()?;
                        Ok(format!("({})", vals.join(", ")))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                format!(
                    "DELETE FROM {table_ident} WHERE ({}) IN ({})",
                    key_idents.join(", "),
                    tuples.join(", ")
                )
            };

            statements.push(sql);
            start = end;
        }

        Ok(statements)
    }

    /// Perform an UPDATE via a temporary staging table:
    ///
    /// 1. Bulk-ingest the update batch into a staging table.
    /// 2. `MERGE INTO target USING staging ON … WHEN MATCHED THEN UPDATE SET …`
    /// 3. `DROP TABLE staging`.
    fn staging_merge_update(
        &self,
        conn: &mut AdbcConnection,
        table_name: &str,
        batch: RecordBatch,
        key_columns: &[String],
    ) -> anyhow::Result<()> {
        if key_columns.is_empty() {
            anyhow::bail!("Update requires at least one key column");
        }

        let schema = batch.schema();
        let has_non_key = schema
            .fields()
            .iter()
            .any(|f| !key_columns.iter().any(|k| k == f.name()));
        if !has_non_key {
            anyhow::bail!(
                "Update requires at least one non-key column in batch schema for table '{table_name}'"
            );
        }

        // Generate a unique staging table name.
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let staging_table = format!("_spicebench_stg_{table_name}_{ts}");

        // 1. Bulk-ingest batch into the staging table.
        if let Err(e) = self.ingest_insert_batch(conn, &staging_table, batch) {
            self.drop_staging_table(conn, &staging_table);
            return Err(e.context(format!(
                "Failed to ingest update data into staging table '{staging_table}'"
            )));
        }

        // 2. MERGE INTO target from staging.
        let merge_sql = Self::build_staging_merge_sql(
            &self.target_table_identifier(table_name),
            &self.target_table_identifier(&staging_table),
            &schema,
            key_columns,
            self.identifier_quote_char,
        );
        let merge_result = conn
            .execute_update(&merge_sql)
            .map_err(|e| anyhow::anyhow!("MERGE INTO update failed for '{table_name}': {e}"));

        // 3. Drop staging table (always, even on merge failure).
        self.drop_staging_table(conn, &staging_table);

        merge_result?;
        Ok(())
    }

    /// Best-effort drop of a staging table.
    fn drop_staging_table(&self, conn: &mut AdbcConnection, staging_table: &str) {
        let drop_sql = format!(
            "DROP TABLE IF EXISTS {}",
            self.target_table_identifier(staging_table)
        );

        if let Err(e) = conn.execute_update(&drop_sql) {
            tracing::error!(
                staging_table = %staging_table,
                error = %e,
                "Failed to drop staging table. Manual cleanup may be required."
            );
        }
    }

    /// Build a `MERGE INTO` statement that reads from a staging table.
    ///
    /// ```sql
    /// MERGE INTO target t
    /// USING staging s
    /// ON t.key1 = s.key1
    /// WHEN MATCHED THEN UPDATE SET val1 = s.val1, val2 = s.val2
    /// ```
    fn build_staging_merge_sql(
        target_ident: &str,
        staging_ident: &str,
        schema: &Schema,
        key_columns: &[String],
        identifier_quote_char: char,
    ) -> String {
        let on_clause: String = key_columns
            .iter()
            .map(|k| {
                let q = Self::quote_ident(k, identifier_quote_char);
                format!("t.{q} = s.{q}")
            })
            .collect::<Vec<_>>()
            .join(" AND ");

        let set_clause: String = schema
            .fields()
            .iter()
            .filter(|f| !key_columns.iter().any(|k| k == f.name()))
            .map(|f| {
                let q = Self::quote_ident(f.name(), identifier_quote_char);
                format!("{q} = s.{q}")
            })
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "MERGE INTO {target_ident} t \
             USING {staging_ident} s \
             ON {on_clause} \
             WHEN MATCHED THEN UPDATE SET {set_clause}"
        )
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

        let rows_current = batch.num_rows() as u64;
        let op_label = match &op {
            InsertOp::Insert => "insert",
            InsertOp::Update { .. } => "update",
            InsertOp::Delete { .. } => "delete",
        };

        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f UTC");
        tracing::info!("[adbc] {now} | {table_name} | {op_label} | rows: {rows_current}");

        match op {
            InsertOp::Insert => {
                self.ingest_insert_batch(&mut conn, table_name, batch)?;
            }
            InsertOp::Delete { key_columns } => {
                let statements = self.delete_sql_statements(table_name, &batch, &key_columns)?;
                let num_statements = statements.len();
                let start = Instant::now();
                for sql in statements {
                    conn.execute_update(&sql).map_err(|e| {
                        anyhow::anyhow!("ADBC delete execution failed for '{table_name}': {e}")
                    })?;
                }
                let elapsed = start.elapsed();
                let rows_per_sec = if elapsed.as_secs_f64() > 0.0 {
                    batch.num_rows() as f64 / elapsed.as_secs_f64()
                } else {
                    0.0
                };
                tracing::debug!(
                    table = %table_name,
                    rows = batch.num_rows(),
                    num_statements,
                    elapsed_ms = elapsed.as_millis(),
                    rows_per_sec = format!("{rows_per_sec:.1}"),
                    "DELETE executed"
                );
            }
            InsertOp::Update { key_columns } => {
                let strategy = UpdateStrategy::from_env()?;
                let num_rows = batch.num_rows();
                let start = Instant::now();

                match strategy {
                    UpdateStrategy::StagingTable => {
                        self.staging_merge_update(&mut conn, table_name, batch, &key_columns)?;
                    }
                    UpdateStrategy::BulkIngestUpsert => {
                        self.ingest_insert_batch(&mut conn, table_name, batch)?;
                    }
                    UpdateStrategy::Statement => {
                        let statements: Vec<String> = (0..num_rows)
                            .map(|row| {
                                self.update_sql_for_row(table_name, &batch, row, &key_columns)
                            })
                            .collect::<anyhow::Result<Vec<_>>>()?;
                        for sql in &statements {
                            conn.execute_update(sql).map_err(|e| {
                                anyhow::anyhow!(
                                    "ADBC update execution failed for '{table_name}': {e}"
                                )
                            })?;
                        }
                    }
                }

                let elapsed = start.elapsed();
                let rows_per_sec = if elapsed.as_secs_f64() > 0.0 {
                    num_rows as f64 / elapsed.as_secs_f64()
                } else {
                    0.0
                };
                tracing::debug!(
                    table = %table_name,
                    rows = num_rows,
                    strategy = ?strategy,
                    elapsed_ms = elapsed.as_millis(),
                    rows_per_sec = format!("{rows_per_sec:.1}"),
                    "UPDATE executed"
                );
            }
        }

        let rows_total = {
            let mut counts = self.row_counts.lock().unwrap();
            let total = counts.entry(table_name.to_string()).or_insert(0);
            match op_label {
                "insert" => *total += rows_current,
                "delete" => *total = total.saturating_sub(rows_current),
                _ => {} // updates don't change row count
            }
            *total
        };

        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f UTC");
        tracing::info!(
            "[adbc] WRITTEN {now} | {table_name} | {op_label} | rows: {rows_current} | total: {rows_total}"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn make_int_batch(ids: &[i64]) -> (RecordBatch, Vec<String>, Vec<usize>) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids.to_vec()))]).unwrap();
        let key_columns = vec!["id".to_string()];
        let key_indices = vec![0];
        (batch, key_columns, key_indices)
    }

    #[test]
    fn single_key_all_rows_in_one_batch() {
        let (batch, key_columns, key_indices) = make_int_batch(&[10, 20, 30]);
        let stmts = AdbcSink::batched_delete_sql(
            r#""t""#,
            &batch,
            1000,
            &key_columns,
            &key_indices,
            '"',
            false,
        )
        .unwrap();
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], r#"DELETE FROM "t" WHERE "id" IN (10, 20, 30)"#);
    }

    #[test]
    fn single_key_chunked() {
        let (batch, key_columns, key_indices) = make_int_batch(&[1, 2, 3, 4, 5]);
        let stmts = AdbcSink::batched_delete_sql(
            r#""t""#,
            &batch,
            2,
            &key_columns,
            &key_indices,
            '"',
            false,
        )
        .unwrap();
        assert_eq!(stmts.len(), 3);
        assert_eq!(stmts[0], r#"DELETE FROM "t" WHERE "id" IN (1, 2)"#);
        assert_eq!(stmts[1], r#"DELETE FROM "t" WHERE "id" IN (3, 4)"#);
        assert_eq!(stmts[2], r#"DELETE FROM "t" WHERE "id" IN (5)"#);
    }

    #[test]
    fn composite_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();
        let key_columns = vec!["a".to_string(), "b".to_string()];
        let key_indices = vec![0, 1];
        let stmts = AdbcSink::batched_delete_sql(
            r#""t""#,
            &batch,
            1000,
            &key_columns,
            &key_indices,
            '"',
            false,
        )
        .unwrap();
        assert_eq!(stmts.len(), 1);
        assert_eq!(
            stmts[0],
            r#"DELETE FROM "t" WHERE ("a", "b") IN ((1, 'x'), (2, 'y'))"#
        );
    }

    #[test]
    fn qualified_table_ident() {
        let (batch, key_columns, key_indices) = make_int_batch(&[42]);
        let stmts = AdbcSink::batched_delete_sql(
            r#""catalog"."schema"."orders""#,
            &batch,
            1000,
            &key_columns,
            &key_indices,
            '"',
            false,
        )
        .unwrap();
        assert_eq!(
            stmts[0],
            r#"DELETE FROM "catalog"."schema"."orders" WHERE "id" IN (42)"#
        );
    }

    #[test]
    fn composite_key_databricks_bigint_suffix() {
        // partsupp: both keys are Int64 (BIGINT) → both get L suffix
        let schema = Arc::new(Schema::new(vec![
            Field::new("ps_partkey", DataType::Int64, false),
            Field::new("ps_suppkey", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![59, 1688])),
                Arc::new(Int64Array::from(vec![560, 940])),
            ],
        )
        .unwrap();
        let key_columns = vec!["ps_partkey".to_string(), "ps_suppkey".to_string()];
        let key_indices = vec![0, 1];
        let stmts = AdbcSink::batched_delete_sql(
            "`t`",
            &batch,
            1000,
            &key_columns,
            &key_indices,
            '`',
            true,
        )
        .unwrap();
        assert_eq!(stmts.len(), 1);
        assert_eq!(
            stmts[0],
            "DELETE FROM `t` WHERE (`ps_partkey`, `ps_suppkey`) IN ((59L, 560L), (1688L, 940L))"
        );
    }

    #[test]
    fn composite_key_databricks_mixed_int64_int32() {
        // lineitem: l_orderkey is Int64 (BIGINT) → L suffix, l_linenumber is Int32 (INT) → no suffix
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_linenumber", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![40194, 55937])),
                Arc::new(Int32Array::from(vec![1, 3])),
            ],
        )
        .unwrap();
        let key_columns = vec!["l_orderkey".to_string(), "l_linenumber".to_string()];
        let key_indices = vec![0, 1];
        let stmts = AdbcSink::batched_delete_sql(
            "`t`",
            &batch,
            1000,
            &key_columns,
            &key_indices,
            '`',
            true,
        )
        .unwrap();
        assert_eq!(stmts.len(), 1);
        // l_orderkey gets L (Int64), l_linenumber does NOT (Int32)
        assert_eq!(
            stmts[0],
            "DELETE FROM `t` WHERE (`l_orderkey`, `l_linenumber`) IN ((40194L, 1), (55937L, 3))"
        );
    }

    #[test]
    fn empty_batch_returns_no_statements() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::new_empty(schema);
        let key_columns = vec!["id".to_string()];
        let key_indices = vec![0];
        let stmts = AdbcSink::batched_delete_sql(
            r#""t""#,
            &batch,
            100,
            &key_columns,
            &key_indices,
            '"',
            false,
        )
        .unwrap();
        assert!(stmts.is_empty());
    }

    // ── STAGING MERGE UPDATE tests ───────────────────────────────────────

    #[test]
    fn staging_merge_sql_single_key() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]);
        let key_columns = vec!["id".to_string()];
        let sql = AdbcSink::build_staging_merge_sql(
            r#""target""#,
            r#""staging""#,
            &schema,
            &key_columns,
            '"',
        );
        assert_eq!(
            sql,
            "MERGE INTO \"target\" t \
             USING \"staging\" s \
             ON t.\"id\" = s.\"id\" \
             WHEN MATCHED THEN UPDATE SET \"name\" = s.\"name\", \"value\" = s.\"value\""
        );
    }

    #[test]
    fn staging_merge_sql_composite_key() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int32, false),
            Field::new("val", DataType::Utf8, false),
        ]);
        let key_columns = vec!["a".to_string(), "b".to_string()];
        let sql =
            AdbcSink::build_staging_merge_sql(r#""t""#, r#""stg""#, &schema, &key_columns, '"');
        assert!(sql.contains(r#"ON t."a" = s."a" AND t."b" = s."b""#));
        assert!(sql.contains(r#"UPDATE SET "val" = s."val""#));
    }

    #[test]
    fn staging_merge_sql_databricks_backticks() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let key_columns = vec!["id".to_string()];
        let sql = AdbcSink::build_staging_merge_sql(
            "`catalog`.`schema`.`target`",
            "`catalog`.`schema`.`staging`",
            &schema,
            &key_columns,
            '`',
        );
        assert!(sql.starts_with("MERGE INTO `catalog`.`schema`.`target` t"));
        assert!(sql.contains("USING `catalog`.`schema`.`staging` s"));
        assert!(sql.contains("ON t.`id` = s.`id`"));
        assert!(sql.contains("`name` = s.`name`"));
    }
}
