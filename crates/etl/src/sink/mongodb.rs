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

//! Native MongoDB sink using the mongodb crate directly.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Float64Type, Int64Type, TimeUnit};
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use mongodb::bson::{Bson, Document};
use mongodb::options::{ReplaceOneModel, WriteModel};

use super::{InsertOp, Sink};

const INSERT_PARALLELISM_ENV: &str = "SPICEBENCH_MONGO_INSERT_PARALLELISM";
const INSERT_BATCH_SIZE_ENV: &str = "SPICEBENCH_MONGO_INSERT_BATCH_SIZE";
const UPDATE_PARALLELISM_ENV: &str = "SPICEBENCH_MONGO_UPDATE_PARALLELISM";
const DELETE_PARALLELISM_ENV: &str = "SPICEBENCH_MONGO_DELETE_PARALLELISM";
const UPDATE_BATCH_SIZE_ENV: &str = "SPICEBENCH_MONGO_UPDATE_BATCH_SIZE";
const DELETE_BATCH_SIZE_ENV: &str = "SPICEBENCH_MONGO_DELETE_BATCH_SIZE";

const DEFAULT_INSERT_BATCH_SIZE: usize = 5_000;
const DEFAULT_UPDATE_BATCH_SIZE: usize = 5_000;
const DEFAULT_DELETE_BATCH_SIZE: usize = 5_000;

pub struct MongoDbSink {
    db: mongodb::Database,
    /// Primary key columns per table, used to compute `_id` for each document.
    /// Change stream delete events only carry `_id`, so we store the PK as `_id`.
    primary_key_columns: HashMap<String, Vec<String>>,
    /// Running row counts per table, updated after each write.
    row_counts: Arc<Mutex<HashMap<String, u64>>>,
    /// Max concurrent insert_many requests for inserts.
    insert_parallelism: usize,
    /// Chunk size for splitting insert batches before parallelising.
    insert_batch_size: usize,
    /// Max concurrent bulk_write requests for updates.
    update_parallelism: usize,
    /// Chunk size for splitting update batches before parallelising.
    update_batch_size: usize,
    /// Max concurrent delete_many requests for deletes.
    delete_parallelism: usize,
    /// Chunk size for splitting delete batches before parallelising.
    delete_batch_size: usize,
}

impl MongoDbSink {
    pub async fn new(
        uri: &str,
        primary_key_columns: HashMap<String, Vec<String>>,
    ) -> anyhow::Result<Self> {
        let mut options = mongodb::options::ClientOptions::parse(uri)
            .await
            .map_err(|e| anyhow::anyhow!("MongoDB URI parse error: {e}"))?;
        let db_name = options
            .default_database
            .clone()
            .unwrap_or_else(|| "spicebench".to_string());
        options.max_pool_size = Some(200);
        let client = mongodb::Client::with_options(options)
            .map_err(|e| anyhow::anyhow!("MongoDB client creation error: {e}"))?;

        let insert_parallelism = std::env::var(INSERT_PARALLELISM_ENV)
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(8);
        let insert_batch_size = std::env::var(INSERT_BATCH_SIZE_ENV)
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(DEFAULT_INSERT_BATCH_SIZE);
        let update_parallelism = std::env::var(UPDATE_PARALLELISM_ENV)
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(8);
        let update_batch_size = std::env::var(UPDATE_BATCH_SIZE_ENV)
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(DEFAULT_UPDATE_BATCH_SIZE);
        let delete_parallelism = std::env::var(DELETE_PARALLELISM_ENV)
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(16);
        let delete_batch_size = std::env::var(DELETE_BATCH_SIZE_ENV)
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(DEFAULT_DELETE_BATCH_SIZE);

        tracing::info!(
            insert_parallelism,
            insert_batch_size,
            update_parallelism,
            update_batch_size,
            delete_parallelism,
            delete_batch_size,
            "MongoDB sink configured"
        );

        Ok(Self {
            db: client.database(&db_name),
            primary_key_columns,
            row_counts: Arc::new(Mutex::new(HashMap::new())),
            insert_parallelism,
            insert_batch_size,
            update_parallelism,
            update_batch_size,
            delete_parallelism,
            delete_batch_size,
        })
    }

    fn update_row_count(&self, table_name: &str, op_label: &str, rows: u64) -> u64 {
        let mut counts = self.row_counts.lock().expect("row_counts lock poisoned");
        let count = counts.entry(table_name.to_string()).or_insert(0);
        match op_label {
            "insert" => *count = count.saturating_add(rows),
            "delete" => *count = count.saturating_sub(rows),
            _ => {} // update: row count unchanged
        }
        *count
    }

    /// Compute the `_id` for a row as a colon-joined string of primary key values.
    /// For a single key this is just the value; for compound keys it's "v1:v2:...".
    fn compute_id(
        &self,
        table_name: &str,
        batch: &RecordBatch,
        row: usize,
    ) -> anyhow::Result<Bson> {
        let pk_cols = self
            .primary_key_columns
            .get(table_name)
            .map(Vec::as_slice)
            .unwrap_or(&[]);

        if pk_cols.is_empty() {
            // No PK configured — let MongoDB auto-generate _id
            return Ok(Bson::Null);
        }

        let schema = batch.schema();
        let parts: Vec<String> = pk_cols
            .iter()
            .map(|pk| {
                let idx = schema.index_of(pk).map_err(|_| {
                    anyhow::anyhow!("PK column '{pk}' not in schema for '{table_name}'")
                })?;
                let col = batch.column(idx);
                Ok(bson_to_string(&arrow_col_to_bson(col.as_ref(), row)))
            })
            .collect::<anyhow::Result<_>>()?;

        Ok(if parts.len() == 1 {
            Bson::String(parts.into_iter().next().unwrap())
        } else {
            Bson::String(parts.join(":"))
        })
    }
}

fn bson_to_string(b: &Bson) -> String {
    match b {
        Bson::Int32(v) => v.to_string(),
        Bson::Int64(v) => v.to_string(),
        Bson::Double(v) => v.to_string(),
        Bson::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[async_trait]
impl Sink for MongoDbSink {
    async fn write(
        &self,
        table_name: &str,
        _batch_id: u64,
        batch: RecordBatch,
        op: InsertOp,
        _partition_columns: Vec<String>,
    ) -> anyhow::Result<()> {
        let write_start = Instant::now();
        let rows = batch.num_rows();
        let op_label = match &op {
            InsertOp::Insert => "insert",
            InsertOp::Update { .. } => "update",
            InsertOp::Delete { .. } => "delete",
        };

        tracing::debug!(
            table = %table_name,
            op = %op_label,
            rows,
            "Sink::write started"
        );

        match op {
            InsertOp::Insert => {
                let collection = self.db.collection::<Document>(table_name);
                let schema = batch.schema();
                let mut all_docs = Vec::with_capacity(batch.num_rows());
                for row in 0..batch.num_rows() {
                    let mut doc = Document::new();
                    let id = self.compute_id(table_name, &batch, row)?;
                    if id != Bson::Null {
                        doc.insert("_id", id);
                    }
                    for (col_idx, field) in schema.fields().iter().enumerate() {
                        let col = batch.column(col_idx);
                        if !col.is_null(row) {
                            doc.insert(field.name().clone(), arrow_col_to_bson(col.as_ref(), row));
                        }
                    }
                    all_docs.push(doc);
                }
                if !all_docs.is_empty() {
                    let n_chunks = (all_docs.len() + self.insert_batch_size - 1) / self.insert_batch_size;
                    tracing::debug!(
                        table = %table_name,
                        rows,
                        chunk_size = self.insert_batch_size,
                        n_chunks,
                        parallelism = self.insert_parallelism,
                        "insert_many starting"
                    );
                    let table_name_owned = table_name.to_string();
                    run_chunked_parallel(
                        all_docs,
                        self.insert_batch_size,
                        self.insert_parallelism,
                        |chunk| {
                            let collection = collection.clone();
                            let table = table_name_owned.clone();
                            async move {
                                let n = chunk.len();
                                let t = Instant::now();
                                tracing::debug!(table = %table, rows = n, "insert_many subbatch started");
                                let r = collection.insert_many(chunk).await.map(|_| ()).map_err(|e| {
                                    anyhow::anyhow!("MongoDB insert_many failed for '{table}': {e}")
                                });
                                tracing::debug!(table = %table, rows = n, elapsed_ms = t.elapsed().as_millis(), "insert_many subbatch done");
                                r
                            }
                        },
                    )
                    .await?;
                    tracing::debug!(table = %table_name, rows, "insert_many done");
                }
            }
            InsertOp::Update { .. } => {
                let collection = self.db.collection::<Document>(table_name);
                let namespace = collection.namespace();
                let schema = batch.schema();

                // Build all WriteModels upfront.
                let mut all_models: Vec<WriteModel> = Vec::with_capacity(batch.num_rows());
                for row in 0..batch.num_rows() {
                    let id = self.compute_id(table_name, &batch, row)?;
                    let mut doc = Document::new();
                    if id != Bson::Null {
                        doc.insert("_id", id.clone());
                    }
                    for (col_idx, field) in schema.fields().iter().enumerate() {
                        let col = batch.column(col_idx);
                        if !col.is_null(row) {
                            doc.insert(field.name().clone(), arrow_col_to_bson(col.as_ref(), row));
                        }
                    }
                    let filter = mongodb::bson::doc! { "_id": id };
                    all_models.push(
                        ReplaceOneModel::builder()
                            .namespace(namespace.clone())
                            .filter(filter)
                            .replacement(doc)
                            .upsert(true)
                            .build()
                            .into(),
                    );
                }

                if !all_models.is_empty() {
                    let n_chunks = (all_models.len() + self.update_batch_size - 1) / self.update_batch_size;
                    tracing::debug!(
                        table = %table_name,
                        rows,
                        chunk_size = self.update_batch_size,
                        n_chunks,
                        parallelism = self.update_parallelism,
                        "bulk_write updates starting"
                    );
                    let client = self.db.client().clone();
                    let table_name_owned = table_name.to_string();
                    run_chunked_parallel(
                        all_models,
                        self.update_batch_size,
                        self.update_parallelism,
                        move |chunk| {
                            let client = client.clone();
                            let table = table_name_owned.clone();
                            async move {
                                let n = chunk.len();
                                let t = Instant::now();
                                tracing::debug!(table = %table, rows = n, "bulk_write subbatch started");
                                let r = client.bulk_write(chunk).ordered(false).await.map(|_| ()).map_err(|e| {
                                    anyhow::anyhow!("MongoDB bulk_write (update) failed: {e}")
                                });
                                tracing::debug!(table = %table, rows = n, elapsed_ms = t.elapsed().as_millis(), "bulk_write subbatch done");
                                r
                            }
                        },
                    )
                    .await?;
                }
            }
            InsertOp::Delete { .. } => {
                let collection = self.db.collection::<Document>(table_name);

                let mut all_ids: Vec<Bson> = Vec::with_capacity(batch.num_rows());
                for row in 0..batch.num_rows() {
                    let id = self.compute_id(table_name, &batch, row)?;
                    if id != Bson::Null {
                        all_ids.push(id);
                    }
                }

                if !all_ids.is_empty() {
                    let n_chunks = (all_ids.len() + self.delete_batch_size - 1) / self.delete_batch_size;
                    tracing::debug!(
                        table = %table_name,
                        rows,
                        chunk_size = self.delete_batch_size,
                        n_chunks,
                        parallelism = self.delete_parallelism,
                        "delete_many starting"
                    );
                    let table_name_owned = table_name.to_string();
                    run_chunked_parallel(
                        all_ids,
                        self.delete_batch_size,
                        self.delete_parallelism,
                        |chunk| {
                            let collection = collection.clone();
                            let table = table_name_owned.clone();
                            async move {
                                let n = chunk.len();
                                let t = Instant::now();
                                tracing::debug!(table = %table, rows = n, "delete_many subbatch started");
                                let filter = mongodb::bson::doc! { "_id": { "$in": chunk } };
                                let r = collection.delete_many(filter).await.map(|_| ()).map_err(|e| {
                                    anyhow::anyhow!("MongoDB delete_many failed: {e}")
                                });
                                tracing::debug!(table = %table, rows = n, elapsed_ms = t.elapsed().as_millis(), "delete_many subbatch done");
                                r
                            }
                        },
                    )
                    .await?;
                }
            }
        }

        let rows_total = self.update_row_count(table_name, op_label, rows as u64);
        let elapsed = write_start.elapsed();
        let rows_per_sec = if elapsed.as_secs_f64() > 0.0 {
            rows as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };
        tracing::debug!(
            table = %table_name,
            op = %op_label,
            rows,
            rows_total,
            elapsed_ms = elapsed.as_millis(),
            rows_per_sec = format!("{rows_per_sec:.1}"),
            "Sink::write completed"
        );

        Ok(())
    }
}

/// Split `items` into chunks of `chunk_size` and dispatch up to `max_parallel`
/// concurrently using a sliding window. Only parallelises when there is more
/// than one chunk, so small batches always use a single request.
async fn run_chunked_parallel<T, Fut>(
    items: Vec<T>,
    chunk_size: usize,
    max_parallel: usize,
    f: impl Fn(Vec<T>) -> Fut,
) -> anyhow::Result<()>
where
    T: Send + 'static,
    Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let mut remaining = items;
    let mut pending: FuturesUnordered<tokio::task::JoinHandle<anyhow::Result<()>>> =
        FuturesUnordered::new();

    while !remaining.is_empty() {
        // Drain completed tasks to stay within max_parallel in-flight.
        while pending.len() >= max_parallel {
            if let Some(result) = pending.next().await {
                result.map_err(|e| anyhow::anyhow!("parallel write task panicked: {e}"))??;
            }
        }

        let split_at = chunk_size.min(remaining.len());
        let rest = remaining.split_off(split_at);
        let chunk = remaining;
        remaining = rest;
        pending.push(tokio::spawn(f(chunk)));
    }

    while let Some(result) = pending.next().await {
        result.map_err(|e| anyhow::anyhow!("parallel write task panicked: {e}"))??;
    }

    Ok(())
}

fn arrow_col_to_bson(col: &dyn Array, row: usize) -> Bson {
    match col.data_type() {
        DataType::Boolean => Bson::Boolean(col.as_boolean().value(row)),
        DataType::Int8 => Bson::Int32(
            col.as_primitive::<arrow::datatypes::Int8Type>()
                .value(row)
                .into(),
        ),
        DataType::Int16 => Bson::Int32(
            col.as_primitive::<arrow::datatypes::Int16Type>()
                .value(row)
                .into(),
        ),
        DataType::Int32 => {
            Bson::Int32(col.as_primitive::<arrow::datatypes::Int32Type>().value(row))
        }
        DataType::Int64 => Bson::Int64(col.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => Bson::Int32(
            col.as_primitive::<arrow::datatypes::UInt8Type>()
                .value(row)
                .into(),
        ),
        DataType::UInt16 => Bson::Int32(
            col.as_primitive::<arrow::datatypes::UInt16Type>()
                .value(row)
                .into(),
        ),
        DataType::UInt32 => Bson::Int64(
            col.as_primitive::<arrow::datatypes::UInt32Type>()
                .value(row)
                .into(),
        ),
        DataType::UInt64 => Bson::Int64(
            col.as_primitive::<arrow::datatypes::UInt64Type>()
                .value(row)
                .cast_signed(),
        ),
        DataType::Float32 => Bson::Double(
            col.as_primitive::<arrow::datatypes::Float32Type>()
                .value(row)
                .into(),
        ),
        DataType::Float64 => Bson::Double(col.as_primitive::<Float64Type>().value(row)),
        DataType::Decimal128(_, scale) => {
            let raw = col
                .as_primitive::<arrow::datatypes::Decimal128Type>()
                .value(row);
            #[allow(clippy::cast_sign_loss)]
            let scale = *scale as u32;
            let divisor = 10i128.pow(scale);
            #[allow(clippy::cast_precision_loss)]
            let f = (raw as f64) / (divisor as f64);
            Bson::Double(f)
        }
        DataType::Utf8 => Bson::String(col.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => Bson::String(col.as_string::<i64>().value(row).to_string()),
        DataType::Binary => Bson::Binary(mongodb::bson::Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: col.as_binary::<i32>().value(row).to_vec(),
        }),
        DataType::LargeBinary => Bson::Binary(mongodb::bson::Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: col.as_binary::<i64>().value(row).to_vec(),
        }),
        DataType::Date32 => {
            let days = i64::from(
                col.as_primitive::<arrow::datatypes::Date32Type>()
                    .value(row),
            );
            Bson::DateTime(mongodb::bson::DateTime::from_millis(days * 86_400 * 1_000))
        }
        DataType::Date64 => {
            let millis = col
                .as_primitive::<arrow::datatypes::Date64Type>()
                .value(row);
            Bson::DateTime(mongodb::bson::DateTime::from_millis(millis))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let secs = col
                .as_primitive::<arrow::datatypes::TimestampSecondType>()
                .value(row);
            Bson::DateTime(mongodb::bson::DateTime::from_millis(secs * 1_000))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let millis = col
                .as_primitive::<arrow::datatypes::TimestampMillisecondType>()
                .value(row);
            Bson::DateTime(mongodb::bson::DateTime::from_millis(millis))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let micros = col
                .as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
                .value(row);
            Bson::DateTime(mongodb::bson::DateTime::from_millis(micros / 1_000))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let nanos = col
                .as_primitive::<arrow::datatypes::TimestampNanosecondType>()
                .value(row);
            Bson::DateTime(mongodb::bson::DateTime::from_millis(nanos / 1_000_000))
        }
        _ => {
            Bson::String(arrow::util::display::array_value_to_string(col, row).unwrap_or_default())
        }
    }
}
