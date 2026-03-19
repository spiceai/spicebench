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

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinHandle;

use adbc_client::{
    AdbcConnection, AdbcConnectionManager, AdbcConnectionPool, IngestMode, create_pool,
};
use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatchIterator;
use arrow_cast::display::array_value_to_string;
use async_trait::async_trait;
use system_adapter_protocol::DatasetConfig;

use super::{InsertOp, Sink};

/// Conservative cap for per-ingest Arrow payload size to avoid exceeding
/// FlightSQL/gRPC max message limits (commonly 16 MiB).
const MAX_ADBC_INGEST_BATCH_BYTES: usize = 75 * 1024 * 1024;
const MAX_ADBC_INGEST_BATCH_BYTES_ENV: &str = "SPICEBENCH_ADBC_MAX_INGEST_BATCH_BYTES";

/// Approximate-size thresholds used to avoid expensive exact Arrow IPC serialization
/// for every split decision.
const APPROX_SAFE_FRACTION_NUM: usize = 8;
const APPROX_SAFE_FRACTION_DEN: usize = 10;
const APPROX_LARGE_FRACTION_NUM: usize = 12;
const APPROX_LARGE_FRACTION_DEN: usize = 10;

/// Default number of connections in the ADBC sink pool.
const DEFAULT_ADBC_SINK_POOL_SIZE: u32 = 8;
const ADBC_SINK_POOL_SIZE_ENV: &str = "SPICEBENCH_ADBC_SINK_POOL_SIZE";

/// When set to a positive integer, deletes are batched into
/// `DELETE … WHERE key IN (…)` statements of at most this many rows.
/// When unset or empty, deletes use the original row-by-row approach.
const ADBC_DELETE_BATCH_SIZE_ENV: &str = "SPICEBENCH_ADBC_DELETE_BATCH_SIZE";

/// Enables reuse of a single long-lived ADBC bulk ingest stream per table for
/// insert/update operations that use bulk ingest. When disabled, writes use the
/// existing per-batch ingest behavior.
const ADBC_REUSE_BULK_INGEST_STREAMS_ENV: &str = "SPICEBENCH_ADBC_REUSE_BULK_INGEST_STREAMS";

/// Bounded channel capacity per table for queued bulk ingest batches.
const DEFAULT_ADBC_BULK_INGEST_STREAM_BUFFER: usize = 1;
const ADBC_BULK_INGEST_STREAM_BUFFER_ENV: &str = "SPICEBENCH_ADBC_BULK_INGEST_STREAM_BUFFER";

/// Controls how UPDATE operations are executed.
///
/// - `statement`          — row-by-row `UPDATE … SET … WHERE …` statements (default)
/// - `staging_table`      — bulk ingest into temp staging table + single `MERGE INTO`
/// - `bulk_ingest_upsert` — bulk ingest directly into the target table (relies on the
///   target system's `on_conflict: upsert` or equivalent to merge)
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

/// Reader backed by a tokio mpsc channel so a background worker can keep a
/// single bulk ingest stream open and receive batches over time.
struct ChannelRecordBatchReader {
    schema: std::sync::Arc<Schema>,
    receiver: mpsc::Receiver<RecordBatch>,
}

impl ChannelRecordBatchReader {
    fn new(schema: std::sync::Arc<Schema>, receiver: mpsc::Receiver<RecordBatch>) -> Self {
        Self { schema, receiver }
    }
}

impl Iterator for ChannelRecordBatchReader {
    type Item = arrow::error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.blocking_recv().map(Ok)
    }
}

impl arrow::record_batch::RecordBatchReader for ChannelRecordBatchReader {
    fn schema(&self) -> std::sync::Arc<Schema> {
        self.schema.clone()
    }
}

struct TableBulkIngestStream {
    schema: std::sync::Arc<Schema>,
    sender: mpsc::Sender<RecordBatch>,
    worker: JoinHandle<anyhow::Result<()>>,
}

impl TableBulkIngestStream {
    async fn close_and_wait(self, table_name: &str) -> anyhow::Result<()> {
        drop(self.sender);
        self.worker.await.map_err(|e| {
            anyhow::anyhow!("Bulk ingest worker task join failed for table '{table_name}': {e}")
        })?
    }
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
    row_counts: RwLock<HashMap<String, AtomicU64>>,
    bulk_ingest_streams: RwLock<HashMap<String, TableBulkIngestStream>>,
    /// Character used to quote SQL identifiers (e.g. '"' for ANSI, '`' for Databricks).
    identifier_quote_char: char,
    /// Whether Int64/UInt64 literals need an `L` suffix (Databricks).
    bigint_suffix: bool,
    update_strategy: UpdateStrategy,
    reuse_bulk_ingest_streams: bool,
    bulk_ingest_stream_buffer: usize,
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

    fn reuse_bulk_ingest_streams() -> bool {
        std::env::var(ADBC_REUSE_BULK_INGEST_STREAMS_ENV)
            .ok()
            .and_then(|raw| {
                let val = raw.trim().to_ascii_lowercase();
                match val.as_str() {
                    "1" | "true" | "yes" | "on" => Some(true),
                    "0" | "false" | "no" | "off" => Some(false),
                    _ => None,
                }
            })
            .unwrap_or(true)
    }

    fn bulk_ingest_stream_buffer() -> usize {
        std::env::var(ADBC_BULK_INGEST_STREAM_BUFFER_ENV)
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_ADBC_BULK_INGEST_STREAM_BUFFER)
    }

    /// Creates a new [`AdbcSink`] backed by a connection pool.
    pub fn new(
        driver_name: &str,
        db_kwargs: HashMap<String, serde_json::Value>,
        target_db_catalog: Option<String>,
        target_db_schema: Option<String>,
    ) -> anyhow::Result<Self> {
        let update_strategy = UpdateStrategy::from_env()?;
        let pool_size = Self::pool_size();
        let pool = create_pool(driver_name, db_kwargs, Some(pool_size))
            .map_err(|e| anyhow::anyhow!("Failed to create ADBC connection pool: {e}"))?;
        eprintln!("[adbc] Connection pool created (driver: {driver_name}, size: {pool_size})");

        let identifier_quote_char = AdbcConnectionManager::identifier_quote_style(driver_name);
        let bigint_suffix = AdbcConnectionManager::bigint_suffix(driver_name);
        let reuse_bulk_ingest_streams = Self::reuse_bulk_ingest_streams();
        let bulk_ingest_stream_buffer = Self::bulk_ingest_stream_buffer();

        if reuse_bulk_ingest_streams {
            eprintln!(
                "[adbc] Reusable bulk ingest streams enabled (buffer size: {bulk_ingest_stream_buffer})"
            );
        }

        Ok(Self {
            pool,
            target_db_catalog,
            target_db_schema,
            row_counts: RwLock::new(HashMap::new()),
            bulk_ingest_streams: RwLock::new(HashMap::new()),
            identifier_quote_char,
            bigint_suffix,
            update_strategy,
            reuse_bulk_ingest_streams,
            bulk_ingest_stream_buffer,
        })
    }

    fn split_insert_batch_for_ingest(
        &self,
        batch: RecordBatch,
    ) -> anyhow::Result<Vec<RecordBatch>> {
        let max_ingest_bytes = Self::max_ingest_batch_bytes();
        let approx_size = Self::approx_serialized_batch_size(&batch);

        if Self::approx_likely_safe(approx_size, max_ingest_bytes) {
            return Ok(vec![batch]);
        }

        if !Self::approx_likely_too_large(approx_size, max_ingest_bytes) {
            let exact_size = Self::serialized_batch_size(&batch)?;
            if exact_size <= max_ingest_bytes {
                return Ok(vec![batch]);
            }
        }

        Self::split_for_size(&batch, max_ingest_bytes)
    }

    fn spawn_table_bulk_ingest_stream(
        &self,
        table_name: &str,
        schema: std::sync::Arc<Schema>,
    ) -> TableBulkIngestStream {
        let (sender, receiver) = mpsc::channel(self.bulk_ingest_stream_buffer);
        let pool = self.pool.clone();
        let ingest_table_name = self.target_table_ingest_name(table_name);
        let source_table_name = table_name.to_string();
        let worker_schema = schema.clone();
        let target_db_catalog = self.target_db_catalog.clone();
        let target_db_schema = self.target_db_schema.clone();

        let worker = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let mut conn = pool
                .get()
                .map_err(|e| anyhow::anyhow!("Failed to get ADBC connection from pool: {e}"))?;

            conn.bulk_ingest_stream(
                &ingest_table_name,
                target_db_catalog.as_deref(),
                target_db_schema.as_deref(),
                IngestMode::CreateAppend,
                Box::new(ChannelRecordBatchReader::new(worker_schema, receiver)),
            )
            .map_err(|e| {
                anyhow::anyhow!(
                    "ADBC stream bulk ingest failed for source table '{source_table_name}' (ingest target '{ingest_table_name}'): {e}"
                )
            })?;

            Ok(())
        });

        TableBulkIngestStream {
            schema,
            sender,
            worker,
        }
    }

    async fn send_batch_via_reused_bulk_ingest_stream(
        &self,
        table_name: &str,
        batch: RecordBatch,
    ) -> anyhow::Result<()> {
        let sub_batches = self.split_insert_batch_for_ingest(batch)?;
        let schema = sub_batches
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| anyhow::anyhow!("Expected at least one insert batch"))?;

        let sender = {
            let streams = self.bulk_ingest_streams.read().await;
            streams.get(table_name).and_then(|stream| {
                if stream.schema.as_ref() == schema.as_ref() && !stream.worker.is_finished() {
                    Some(stream.sender.clone())
                } else {
                    None
                }
            })
        };

        let sender = if let Some(sender) = sender {
            sender
        } else {
            self.end_bulk_ingest_stream_for_table(table_name).await?;
            let mut streams = self.bulk_ingest_streams.write().await;
            let stream = self.spawn_table_bulk_ingest_stream(table_name, schema.clone());
            let sender = stream.sender.clone();
            streams.insert(table_name.to_string(), stream);
            sender
        };

        for sub_batch in sub_batches {
            sender.send(sub_batch).await.map_err(|_| {
                anyhow::anyhow!(
                    "Bulk ingest stream for table '{table_name}' is no longer available"
                )
            })?;
        }

        Ok(())
    }

    async fn end_bulk_ingest_stream_for_table(&self, table_name: &str) -> anyhow::Result<()> {
        let stream = {
            let mut streams = self.bulk_ingest_streams.write().await;
            streams.remove(table_name)
        };

        if let Some(stream) = stream {
            stream.close_and_wait(table_name).await?;
        }

        Ok(())
    }

    async fn end_all_bulk_ingest_streams(&self) -> anyhow::Result<()> {
        let streams: Vec<(String, TableBulkIngestStream)> = {
            let mut guard = self.bulk_ingest_streams.write().await;
            guard.drain().collect()
        };

        for (table_name, stream) in streams {
            stream.close_and_wait(&table_name).await?;
        }

        Ok(())
    }

    fn saturating_fetch_sub(counter: &AtomicU64, value: u64) -> u64 {
        let mut current = counter.load(Ordering::Relaxed);
        loop {
            let updated = current.saturating_sub(value);
            match counter.compare_exchange_weak(
                current,
                updated,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return updated,
                Err(observed) => current = observed,
            }
        }
    }

    fn apply_row_count_delta(counter: &AtomicU64, op_label: &str, rows_current: u64) -> u64 {
        match op_label {
            "insert" => counter.fetch_add(rows_current, Ordering::Relaxed) + rows_current,
            "delete" => Self::saturating_fetch_sub(counter, rows_current),
            _ => counter.load(Ordering::Relaxed), // updates don't change row count
        }
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
        table_name.to_string()
    }

    fn create_table_sql(
        &self,
        table_name: &str,
        schema: &Schema,
        partition_by: Vec<String>,
        primary_keys: &[String],
    ) -> anyhow::Result<String> {
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

        let partition_clause = if !partition_by.is_empty() {
            format!("PARTITION BY ({})", partition_by.join(", "))
        } else {
            String::new()
        };

        let primary_key_statement = if !primary_keys.is_empty() {
            let key_idents: Vec<String> = primary_keys
                .iter()
                .map(|k| self.quote_identifier(k))
                .collect();
            format!(", PRIMARY KEY ({})", key_idents.join(", "))
        } else {
            String::new()
        };

        Ok(format!(
            "CREATE TABLE IF NOT EXISTS {} ({columns}{primary_key_statement}) {partition_clause}",
            self.target_table_identifier(table_name)
        ))
    }

    pub fn create_tables_from_dataset_configs(
        &self,
        datasets: &HashMap<String, DatasetConfig>,
    ) -> anyhow::Result<()> {
        let statements = datasets
            .iter()
            .map(|(table_name, config)| {
                self.create_table_sql(
                    table_name,
                    config.schema.as_ref(),
                    config.partition_columns.clone(),
                    &config.primary_key_columns,
                )
            })
            .collect::<Result<Vec<String>, anyhow::Error>>()?;

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

    fn approx_serialized_batch_size(batch: &RecordBatch) -> usize {
        batch
            .columns()
            .iter()
            .map(|col| col.get_array_memory_size())
            .sum()
    }

    fn approx_likely_safe(approx_bytes: usize, max_bytes: usize) -> bool {
        approx_bytes
            <= max_bytes.saturating_mul(APPROX_SAFE_FRACTION_NUM) / APPROX_SAFE_FRACTION_DEN
    }

    fn approx_likely_too_large(approx_bytes: usize, max_bytes: usize) -> bool {
        approx_bytes
            >= max_bytes.saturating_mul(APPROX_LARGE_FRACTION_NUM) / APPROX_LARGE_FRACTION_DEN
    }

    fn split_for_size(batch: &RecordBatch, max_bytes: usize) -> anyhow::Result<Vec<RecordBatch>> {
        let mut out = Vec::new();
        let mut start = 0usize;
        let total_rows = batch.num_rows();
        let total_approx_bytes = Self::approx_serialized_batch_size(batch);

        if total_rows == 0 {
            return Ok(out);
        }

        while start < total_rows {
            let mut end = total_rows;
            let mut accepted: Option<RecordBatch> = None;

            while end > start {
                let span = end - start;
                let candidate_approx_bytes = total_approx_bytes.saturating_mul(span) / total_rows;

                if Self::approx_likely_too_large(candidate_approx_bytes, max_bytes) {
                    end = start + (span / 2);
                    continue;
                }

                let candidate = batch.slice(start, span);
                if Self::approx_likely_safe(candidate_approx_bytes, max_bytes) {
                    accepted = Some(candidate);
                    break;
                }

                let exact_size = Self::serialized_batch_size(&candidate)?;
                if exact_size <= max_bytes {
                    accepted = Some(candidate);
                    break;
                }

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

    fn bulk_ingest_stream_for_batches(
        &self,
        conn: &mut AdbcConnection,
        table_name: &str,
        batches: Vec<RecordBatch>,
    ) -> anyhow::Result<()> {
        let Some(first_batch) = batches.first() else {
            return Ok(());
        };

        let schema = first_batch.schema();
        let make_reader = |batches: Vec<RecordBatch>| {
            Box::new(RecordBatchIterator::new(
                batches.into_iter().map(Ok),
                schema.clone(),
            ))
        };

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
            match conn.bulk_ingest_stream(
                &ingest_table_name,
                None,
                None,
                IngestMode::CreateAppend,
                make_reader(batches.clone()),
            ) {
                Ok(result) => Ok(result),
                Err(qualified_err) => {
                    let qualified_message = qualified_err.to_string();
                    if Self::is_message_too_large_error(&qualified_message) {
                        Err(qualified_err)
                    } else {
                        conn.bulk_ingest_stream(
                            table_name,
                            target_db_catalog,
                            target_db_schema,
                            IngestMode::CreateAppend,
                            make_reader(batches.clone()),
                        )
                    }
                }
            }
        } else {
            conn.bulk_ingest_stream(
                table_name,
                target_db_catalog,
                target_db_schema,
                IngestMode::CreateAppend,
                make_reader(batches.clone()),
            )
        };

        match ingest_result {
            Ok(_) => Ok(()),
            Err(e) => {
                let message = e.to_string();
                if Self::is_message_too_large_error(&message) {
                    for sub_batch in batches {
                        self.bulk_ingest_with_retry(conn, table_name, sub_batch)?;
                    }
                    Ok(())
                } else {
                    anyhow::bail!(
                        "ADBC stream bulk ingest failed for source table '{table_name}' (ingest target '{ingest_table_name}'): {message}"
                    )
                }
            }
        }
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
                target_db_catalog,
                target_db_schema,
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
        let approx_size = Self::approx_serialized_batch_size(&batch);

        if Self::approx_likely_safe(approx_size, max_ingest_bytes) {
            self.bulk_ingest_with_retry(conn, table_name, batch)?;
            return Ok(());
        }

        if !Self::approx_likely_too_large(approx_size, max_ingest_bytes) {
            let exact_size = Self::serialized_batch_size(&batch)?;
            if exact_size <= max_ingest_bytes {
                self.bulk_ingest_with_retry(conn, table_name, batch)?;
                return Ok(());
            }
        }

        let split_batches = Self::split_for_size(&batch, max_ingest_bytes)?;
        self.bulk_ingest_stream_for_batches(conn, table_name, split_batches)?;

        Ok(())
    }

    fn resolve_key_columns(
        &self,
        schema: &Schema,
        key_columns: &[String],
        context: &str,
    ) -> anyhow::Result<(Vec<usize>, Vec<String>)> {
        let key_indices = key_columns
            .iter()
            .map(|key| {
                schema.index_of(key).map_err(|_| {
                    anyhow::anyhow!("Key column '{key}' not found in {context} batch schema")
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        let key_idents = key_columns
            .iter()
            .map(|key| self.quote_identifier(key))
            .collect();

        Ok((key_indices, key_idents))
    }

    fn non_key_update_columns(
        &self,
        schema: &Schema,
        key_columns: &[String],
    ) -> Vec<(usize, String)> {
        let key_set: HashSet<&str> = key_columns.iter().map(String::as_str).collect();
        schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| !key_set.contains(field.name().as_str()))
            .map(|(idx, field)| (idx, self.quote_identifier(field.name())))
            .collect()
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
        table_ident: &str,
        batch: &RecordBatch,
        row: usize,
        key_indices: &[usize],
        key_idents: &[String],
    ) -> anyhow::Result<String> {
        let mut predicates = Vec::with_capacity(key_indices.len());
        for (&idx, key_ident) in key_indices.iter().zip(key_idents.iter()) {
            let column = batch.column(idx);
            let literal = Self::sql_literal(column.as_ref(), row, false)?;
            predicates.push(Self::null_safe_predicate_for_literal(key_ident, &literal));
        }

        Ok(format!(
            "DELETE FROM {table_ident} WHERE {}",
            predicates.join(" AND ")
        ))
    }

    #[expect(clippy::too_many_arguments)]
    fn update_sql_for_row(
        &self,
        table_name: &str,
        table_ident: &str,
        batch: &RecordBatch,
        row: usize,
        key_indices: &[usize],
        key_idents: &[String],
        non_key_columns: &[(usize, String)],
    ) -> anyhow::Result<String> {
        let mut predicates = Vec::with_capacity(key_indices.len());
        for (&key_idx, key_ident) in key_indices.iter().zip(key_idents.iter()) {
            let key_col = batch.column(key_idx);
            let key_literal = Self::sql_literal(key_col.as_ref(), row, false)?;
            predicates.push(Self::null_safe_predicate_for_literal(
                key_ident,
                &key_literal,
            ));
        }

        let mut set_clauses = Vec::with_capacity(non_key_columns.len());
        for (column_idx, col_ident) in non_key_columns {
            let literal = Self::sql_literal(batch.column(*column_idx).as_ref(), row, false)?;
            set_clauses.push(format!("{col_ident} = {literal}"));
        }

        if set_clauses.is_empty() {
            anyhow::bail!(
                "Update requires at least one non-key column in batch schema for table '{table_name}'"
            );
        }

        Ok(format!(
            "UPDATE {table_ident} SET {} WHERE {}",
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

        let table_ident = self.target_table_identifier(table_name);
        let schema = batch.schema();
        let (key_indices, key_idents) =
            self.resolve_key_columns(schema.as_ref(), key_columns, "delete")?;

        let batch_size = match Self::delete_batch_size() {
            Some(size) => size,
            None => {
                // Original row-by-row approach.
                return (0..batch.num_rows())
                    .map(|row| {
                        self.delete_sql_for_row(&table_ident, batch, row, &key_indices, &key_idents)
                    })
                    .collect();
            }
        };

        Self::batched_delete_sql(
            &table_ident,
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
        let fn_start = Instant::now();
        let num_rows = batch.num_rows();
        let mut statements = Vec::new();
        let mut start = 0;
        let key_idents: Vec<String> = key_columns
            .iter()
            .map(|k| Self::quote_ident(k, identifier_quote_char))
            .collect();

        while start < num_rows {
            let end = (start + batch_size).min(num_rows);

            let sql = if key_columns.len() == 1 {
                let key_ident = &key_idents[0];
                let values: Vec<String> = (start..end)
                    .map(|row| Self::sql_literal(batch.column(key_indices[0]).as_ref(), row, false))
                    .collect::<anyhow::Result<Vec<_>>>()?;
                format!(
                    "DELETE FROM {table_ident} WHERE {key_ident} IN ({})",
                    values.join(", ")
                )
            } else {
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

        tracing::debug!(
            table = table_ident,
            rows = num_rows,
            statements = statements.len(),
            batch_size,
            elapsed_ms = fn_start.elapsed().as_millis(),
            "batched_delete_sql completed"
        );

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
        let fn_start = Instant::now();
        let rows = batch.num_rows();
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
        tracing::debug!(
            table = %table_name,
            rows,
            elapsed_ms = fn_start.elapsed().as_millis(),
            "staging_merge_update completed"
        );
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
        let write_start = Instant::now();
        if batch.num_rows() == 0 {
            tracing::debug!(
                table = %table_name,
                op = "empty",
                elapsed_ms = write_start.elapsed().as_millis(),
                "Sink::write completed"
            );
            return Ok(());
        }

        let rows_current = batch.num_rows() as u64;
        let op_label = match &op {
            InsertOp::Insert => "insert",
            InsertOp::Update { .. } => "update",
            InsertOp::Delete { .. } => "delete",
        };

        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f UTC");
        tracing::debug!("[adbc] {now} | {table_name} | {op_label} | rows: {rows_current}");

        if self.reuse_bulk_ingest_streams
            && matches!(&op, InsertOp::Delete { .. } | InsertOp::Update { .. })
        {
            // Ensure all queued bulk ingest data is flushed before mutation SQL/update flows.
            self.end_all_bulk_ingest_streams().await?;
        }

        match op {
            InsertOp::Insert => {
                if self.reuse_bulk_ingest_streams {
                    self.send_batch_via_reused_bulk_ingest_stream(table_name, batch)
                        .await?;
                } else {
                    let mut conn = self.pool.get().map_err(|e| {
                        anyhow::anyhow!("Failed to get ADBC connection from pool: {e}")
                    })?;
                    self.ingest_insert_batch(&mut conn, table_name, batch)?;
                }
            }
            InsertOp::Delete { key_columns } => {
                let mut conn = self
                    .pool
                    .get()
                    .map_err(|e| anyhow::anyhow!("Failed to get ADBC connection from pool: {e}"))?;
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
                let num_rows = batch.num_rows();
                let start = Instant::now();

                match self.update_strategy {
                    UpdateStrategy::StagingTable => {
                        let mut conn = self.pool.get().map_err(|e| {
                            anyhow::anyhow!("Failed to get ADBC connection from pool: {e}")
                        })?;
                        self.staging_merge_update(&mut conn, table_name, batch, &key_columns)?;
                    }
                    UpdateStrategy::BulkIngestUpsert => {
                        if self.reuse_bulk_ingest_streams {
                            self.send_batch_via_reused_bulk_ingest_stream(table_name, batch)
                                .await?;
                        } else {
                            let mut conn = self.pool.get().map_err(|e| {
                                anyhow::anyhow!("Failed to get ADBC connection from pool: {e}")
                            })?;
                            self.ingest_insert_batch(&mut conn, table_name, batch)?;
                        }
                    }
                    UpdateStrategy::Statement => {
                        let mut conn = self.pool.get().map_err(|e| {
                            anyhow::anyhow!("Failed to get ADBC connection from pool: {e}")
                        })?;
                        let table_ident = self.target_table_identifier(table_name);
                        let schema = batch.schema();
                        let (key_indices, key_idents) =
                            self.resolve_key_columns(schema.as_ref(), &key_columns, "update")?;
                        let non_key_columns =
                            self.non_key_update_columns(schema.as_ref(), &key_columns);

                        if non_key_columns.is_empty() {
                            anyhow::bail!(
                                "Update requires at least one non-key column in batch schema for table '{table_name}'"
                            );
                        }

                        let statements: Vec<String> = (0..num_rows)
                            .map(|row| {
                                self.update_sql_for_row(
                                    table_name,
                                    &table_ident,
                                    &batch,
                                    row,
                                    &key_indices,
                                    &key_idents,
                                    &non_key_columns,
                                )
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
                    strategy = ?self.update_strategy,
                    elapsed_ms = elapsed.as_millis(),
                    rows_per_sec = format!("{rows_per_sec:.1}"),
                    "UPDATE executed"
                );
            }
        }

        let existing_total = {
            let counts = self.row_counts.read().await;
            counts
                .get(table_name)
                .map(|counter| Self::apply_row_count_delta(counter, op_label, rows_current))
        };

        let rows_total = if let Some(total) = existing_total {
            total
        } else {
            let mut counts = self.row_counts.write().await;
            let counter = counts
                .entry(table_name.to_string())
                .or_insert_with(|| AtomicU64::new(0));
            Self::apply_row_count_delta(counter, op_label, rows_current)
        };

        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f UTC");
        tracing::debug!(
            "[adbc] WRITTEN {now} | {table_name} | {op_label} | rows: {rows_current} | total: {rows_total}"
        );

        tracing::debug!(
            table = %table_name,
            op = op_label,
            rows = rows_current,
            elapsed_ms = write_start.elapsed().as_millis(),
            "Sink::write completed"
        );

        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        if self.reuse_bulk_ingest_streams {
            self.end_all_bulk_ingest_streams().await?;
        }

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
