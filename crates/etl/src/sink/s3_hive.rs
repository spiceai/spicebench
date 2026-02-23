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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use arrow::array::{Array, RecordBatch, StringArray, TimestampMicrosecondArray, UInt32Array};
use arrow::compute;
use arrow::datatypes::Schema;
use async_trait::async_trait;
use data_generation::config::TargetConfig;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression};
use parquet::file::properties::WriterProperties;
use tokio::sync::{Mutex, Notify, Semaphore, mpsc};
use tracing::Instrument;

use super::{InsertOp, Sink};

/// Default partitioning column used when no explicit scheme is configured.
const DEFAULT_PARTITION_COLUMN: &str = "__created_at";
const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// Maximum number of concurrent encode+upload tasks allowed **per S3 table
/// path prefix** (i.e. per `{prefix}/{table_name}`).
///
/// S3 rate limits are applied per-prefix, so we scope the concurrency limiter
/// to each table-level prefix rather than each partition path.
const MAX_CONCURRENT_UPLOADS_PER_PREFIX: usize = 8;

/// Capacity of the bounded encode -> upload queue.
const UPLOAD_QUEUE_CAPACITY: usize = 64;

/// Number of background upload workers consuming from the queue.
const MAX_UPLOAD_WORKERS: usize = 32;

/// ETL sink that writes batches as hive-partitioned Parquet files in S3.
///
/// Incoming batches are partitioned by the configured columns and written
/// to S3 immediately during each [`write()`](Sink::write) call.
///
/// Each partition produces a single Parquet file at:
/// ```text
/// {prefix}/{table_name}/{col1}={value1}/{col2}={value2}/part-{seq:08}.parquet
/// ```
///
/// Only `Insert` operations are supported. `Update` and `Delete` operations
/// will return an error.
pub struct S3HiveSink {
    prefix: String,
    partition_columns: Vec<String>,
    /// Per-table-prefix upload concurrency limiters. Each unique S3 table path
    /// prefix (`{prefix}/{table}`) gets its own semaphore so that partition
    /// fanout for a table is rate-limited together.
    prefix_semaphores: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
    upload_tx: mpsc::Sender<QueuedUpload>,
    pending_uploads: Arc<AtomicU64>,
    upload_error: Arc<Mutex<Option<String>>>,
    flush_notify: Arc<Notify>,
    /// Monotonic counter for unique output file names.
    file_seq: Arc<AtomicU64>,
}

struct QueuedUpload {
    path: ObjectPath,
    payload: Vec<u8>,
    table_name: String,
    seq: u64,
    semaphore: Arc<Semaphore>,
}

impl S3HiveSink {
    /// Creates a new [`S3HiveSink`] from a [`TargetConfig`].
    pub fn new(config: &TargetConfig) -> anyhow::Result<Self> {
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(&config.bucket);

        if let Some(region) = &config.region {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = &config.endpoint
            && !endpoint.is_empty()
        {
            builder = builder.with_endpoint(endpoint);
            if endpoint.starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
        }

        let store = Arc::new(builder.build()?);
        let partition_columns = if config.partition_columns.is_empty() {
            vec![DEFAULT_PARTITION_COLUMN.to_string()]
        } else {
            let columns: Vec<String> = config
                .partition_columns
                .iter()
                .map(|c| c.trim())
                .filter(|c| !c.is_empty())
                .map(ToOwned::to_owned)
                .collect();
            if columns.is_empty() {
                vec![DEFAULT_PARTITION_COLUMN.to_string()]
            } else {
                columns
            }
        };

        let file_seq = Arc::new(AtomicU64::new(0));
        let prefix_semaphores: Arc<Mutex<HashMap<String, Arc<Semaphore>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let pending_uploads = Arc::new(AtomicU64::new(0));
        let upload_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let flush_notify = Arc::new(Notify::new());
        let (upload_tx, upload_rx) = mpsc::channel::<QueuedUpload>(UPLOAD_QUEUE_CAPACITY);

        let workers = std::thread::available_parallelism()
            .map_or(8, |parallelism| parallelism.get())
            .clamp(4, MAX_UPLOAD_WORKERS);

        let upload_rx = Arc::new(Mutex::new(upload_rx));
        for worker_id in 0..workers {
            let store = Arc::clone(&store);
            let upload_rx = Arc::clone(&upload_rx);
            let pending_uploads = Arc::clone(&pending_uploads);
            let upload_error = Arc::clone(&upload_error);
            let flush_notify = Arc::clone(&flush_notify);
            tokio::spawn(async move {
                loop {
                    let next_item = {
                        let mut rx = upload_rx.lock().await;
                        rx.recv().await
                    };

                    let Some(item) = next_item else {
                        break;
                    };

                    let put_span = tracing::debug_span!(
                        "etl.s3_hive.s3_put",
                        table = %item.table_name,
                        seq = item.seq,
                        path = %item.path,
                        bytes = item.payload.len(),
                        worker = worker_id,
                    );
                    let put_start = Instant::now();

                    let put_result = async {
                        let _permit = item
                            .semaphore
                            .acquire_owned()
                            .await
                            .map_err(|_| anyhow::anyhow!("upload semaphore closed"))?;

                        store
                            .put(&item.path, item.payload.into())
                            .await
                            .map_err(|e| anyhow::anyhow!("S3 PUT failed for {}: {e}", item.path))
                    }
                    .instrument(put_span.clone())
                    .await;

                    match put_result {
                        Ok(_) => {
                            tracing::debug!(
                                parent: &put_span,
                                elapsed_ms = put_start.elapsed().as_secs_f64() * 1000.0,
                                "S3 PUT completed"
                            );
                        }
                        Err(err) => {
                            let mut shared_err = upload_error.lock().await;
                            if shared_err.is_none() {
                                *shared_err = Some(err.to_string());
                            }
                            tracing::error!(
                                parent: &put_span,
                                error = %err,
                                "S3 PUT failed"
                            );
                        }
                    }

                    pending_uploads.fetch_sub(1, Ordering::AcqRel);
                    flush_notify.notify_waiters();
                }
            });
        }

        Ok(Self {
            prefix: config.prefix.clone(),
            partition_columns,
            prefix_semaphores,
            upload_tx,
            pending_uploads,
            upload_error,
            flush_notify,
            file_seq,
        })
    }

    async fn check_upload_error(&self) -> anyhow::Result<()> {
        let err = { self.upload_error.lock().await.clone() };
        if let Some(err) = err {
            anyhow::bail!("S3 upload worker failed: {err}");
        }
        Ok(())
    }
}

/// Encode partition buffers and enqueue each as a single Parquet upload.
async fn encode_and_queue_partitions(
    upload_tx: &mpsc::Sender<QueuedUpload>,
    pending_uploads: &Arc<AtomicU64>,
    prefix_semaphores: &Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
    prefix: &str,
    partitions: Vec<(String, RecordBatch)>,
    table_name: &str,
    file_seq: &Arc<AtomicU64>,
) -> anyhow::Result<()> {
    if partitions.is_empty() {
        return Ok(());
    }

    let flushed_partitions = partitions.len();
    let flushed_rows: usize = partitions.iter().map(|(_, b)| b.num_rows()).sum();
    tracing::debug!(
        partitions = flushed_partitions,
        rows = flushed_rows,
        "Encoding and queueing partition buffers"
    );

    for (partition_path, batch) in partitions {
        let seq = file_seq.fetch_add(1, Ordering::Relaxed);

        // Resolve (or create) the per-table-prefix semaphore for this table.
        let sem_key = if prefix.is_empty() {
            table_name.to_string()
        } else {
            format!("{prefix}/{table_name}")
        };
        let semaphore = {
            let mut sems = prefix_semaphores.lock().await;
            Arc::clone(
                sems.entry(sem_key)
                    .or_insert_with(|| Arc::new(Semaphore::new(MAX_CONCURRENT_UPLOADS_PER_PREFIX))),
            )
        };

        let path = if prefix.is_empty() {
            if partition_path.is_empty() {
                ObjectPath::from(format!("{table_name}/part-{seq:08}.parquet"))
            } else {
                ObjectPath::from(format!("{table_name}/{partition_path}/part-{seq:08}.parquet"))
            }
        } else if partition_path.is_empty() {
            ObjectPath::from(format!("{prefix}/{table_name}/part-{seq:08}.parquet",))
        } else {
            ObjectPath::from(format!(
                "{prefix}/{table_name}/{partition_path}/part-{seq:08}.parquet",
            ))
        };

        let encode_start = Instant::now();
        let encode_span = tracing::debug_span!(
            "etl.s3_hive.parquet_encode",
            table = %table_name,
            seq,
            partition_path = %partition_path,
            rows = batch.num_rows(),
        );
        let payload = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<u8>> {
            let props = WriterProperties::builder()
                .set_compression(Compression::LZ4)
                .build();
            let mut buf = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
            writer.write(&batch)?;
            writer.close()?;
            Ok(buf)
        })
        .instrument(encode_span.clone())
        .await
        .map_err(|e| anyhow::anyhow!("Parquet encoding task panicked: {e}"))??;
        tracing::debug!(
            parent: &encode_span,
            elapsed_ms = encode_start.elapsed().as_secs_f64() * 1000.0,
            parquet_bytes = payload.len(),
            "Parquet encode completed"
        );

        pending_uploads.fetch_add(1, Ordering::AcqRel);
        if let Err(send_err) = upload_tx
            .send(QueuedUpload {
                path,
                payload,
                table_name: table_name.to_string(),
                seq,
                semaphore,
            })
            .await
        {
            pending_uploads.fetch_sub(1, Ordering::AcqRel);
            anyhow::bail!("Failed to queue S3 upload: {send_err}");
        }
    }

    Ok(())
}

#[async_trait]
impl Sink for S3HiveSink {
    async fn write(
        &self,
        table_name: &str,
        _batch_id: u64,
        batch: RecordBatch,
        op: InsertOp,
        partition_columns: Vec<String>,
    ) -> anyhow::Result<()> {
        match op {
            InsertOp::Insert => {}
            InsertOp::Update { .. } | InsertOp::Delete { .. } => {
                anyhow::bail!(
                    "S3 hive-partitioned sink only supports Insert operations, got {op:?}"
                );
            }
        }

        if batch.num_rows() == 0 {
            return Ok(());
        }

        self.check_upload_error().await?;

        let schema = batch.schema();
        let effective_partition_columns = if partition_columns.is_empty() {
            self.partition_columns.clone()
        } else {
            partition_columns
        };

        let partition_columns_with_idx: Vec<(String, usize)> = effective_partition_columns
            .iter()
            .map(|column_name| {
                let idx = schema.index_of(column_name).map_err(|_| {
                    anyhow::anyhow!(
                        "Batch for table '{table_name}' is missing required partition column '{column_name}'"
                    )
                })?;
                Ok((column_name.clone(), idx))
            })
            .collect::<anyhow::Result<_>>()?;

        let partition_index_set: std::collections::HashSet<usize> = partition_columns_with_idx
            .iter()
            .map(|(_, idx)| *idx)
            .collect();
        let projected_column_indices: Vec<usize> = (0..schema.fields().len())
            .filter(|idx| !partition_index_set.contains(idx))
            .collect();

        if projected_column_indices.is_empty() {
            anyhow::bail!(
                "Cannot write table '{table_name}' with partition columns {effective_partition_columns:?}: \
                 no columns would remain in parquet output"
            );
        }

        // Group rows by distinct partition tuples in configured column order.
        let partitions = partition_batch(
            &batch,
            &partition_columns_with_idx,
            &projected_column_indices,
        )?;

        // Encode all partitions and enqueue S3 uploads.
        encode_and_queue_partitions(
            &self.upload_tx,
            &self.pending_uploads,
            &self.prefix_semaphores,
            &self.prefix,
            partitions,
            table_name,
            &self.file_seq,
        )
        .await?;

        self.check_upload_error().await?;

        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        self.check_upload_error().await?;

        while self.pending_uploads.load(Ordering::Acquire) > 0 {
            self.flush_notify.notified().await;
            self.check_upload_error().await?;
        }

        self.check_upload_error().await?;
        Ok(())
    }
}

/// Splits a [`RecordBatch`] into groups based on distinct values of the column
/// set in `partition_columns_with_idx`. Returns `(partition_path, sub_batch)`
/// pairs where `partition_path` is `col1=v1/col2=v2` in input order.
fn partition_batch(
    batch: &RecordBatch,
    partition_columns_with_idx: &[(String, usize)],
    projected_column_indices: &[usize],
) -> anyhow::Result<Vec<(String, RecordBatch)>> {
    if batch.num_rows() > u32::MAX as usize {
        anyhow::bail!("Batch row count exceeds u32 range for partition indexing");
    }

    // Build a mapping from partition path → row indices.
    let mut groups: indexmap::IndexMap<String, Vec<u32>> = indexmap::IndexMap::new();

    let partition_arrays: Vec<(String, PartitionColumnValues)> = partition_columns_with_idx
        .iter()
        .map(|(name, idx)| {
            let col = batch.column(*idx);
            if let Some(ts_array) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                Ok((
                    name.clone(),
                    PartitionColumnValues::TimestampMicrosecond(ts_array.clone()),
                ))
            } else {
                let string_repr = arrow::array::cast::as_string_array(
                    &compute::cast(col, &arrow::datatypes::DataType::Utf8).map_err(|e| {
                        anyhow::anyhow!("Failed to cast partition column '{name}' to string: {e}")
                    })?,
                )
                .clone();
                Ok((name.clone(), PartitionColumnValues::Utf8(string_repr)))
            }
        })
        .collect::<anyhow::Result<_>>()?;

    let mut key_prefixes: Vec<String> = Vec::with_capacity(partition_arrays.len());
    for (name, _) in &partition_arrays {
        key_prefixes.push(format!("{name}="));
    }

    for row in 0..batch.num_rows() {
        let mut key = String::new();
        for (idx, (_, values)) in partition_arrays.iter().enumerate() {
            if idx > 0 {
                key.push('/');
            }
            key.push_str(&key_prefixes[idx]);
            values.push_partition_key(row, &mut key);
        }
        groups.entry(key).or_default().push(row as u32);
    }

    let projected_fields: Vec<_> = projected_column_indices
        .iter()
        .map(|idx| batch.schema().field(*idx).clone())
        .collect();
    let projected_schema = Arc::new(Schema::new(projected_fields));

    let mut result = Vec::with_capacity(groups.len());
    for (partition_value, row_indices) in groups {
        let indices = UInt32Array::from(row_indices);
        let columns: Vec<Arc<dyn Array>> = projected_column_indices
            .iter()
            .map(|idx| compute::take(batch.column(*idx).as_ref(), &indices, None))
            .collect::<Result<_, _>>()
            .map_err(|e| anyhow::anyhow!("Failed to take rows for partition: {e}"))?;
        let sub_batch = RecordBatch::try_new(Arc::clone(&projected_schema), columns)?;
        result.push((partition_value, sub_batch));
    }

    Ok(result)
}

enum PartitionColumnValues {
    TimestampMicrosecond(TimestampMicrosecondArray),
    Utf8(StringArray),
}

impl PartitionColumnValues {
    fn push_partition_key(&self, row: usize, out: &mut String) {
        match self {
            Self::TimestampMicrosecond(values) => {
                if values.is_null(row) {
                    out.push_str(HIVE_DEFAULT_PARTITION);
                } else {
                    // Truncate to millisecond precision so that rows sharing
                    // the same millisecond coalesce into a single partition,
                    // while preserving enough resolution for downstream
                    // consumers that filter on `__created_at`.
                    let us = values.value(row);
                    let ms = us / 1_000;
                    out.push_str(&ms.to_string());
                }
            }
            Self::Utf8(values) => {
                if values.is_null(row) {
                    out.push_str(HIVE_DEFAULT_PARTITION);
                } else {
                    out.push_str(values.value(row));
                }
            }
        }
    }
}
