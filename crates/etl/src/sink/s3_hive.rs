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

use std::sync::Arc;
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
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::Instrument;

use super::{InsertOp, Sink};

/// Default partitioning column used when no explicit scheme is configured.
const DEFAULT_PARTITION_COLUMN: &str = "__created_at";
const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// Maximum number of partition encode+upload tasks that may run concurrently
/// across all active `write()` calls on a single [`S3HiveSink`] instance.
///
/// This prevents unbounded fan-out (e.g. a TPC-H `lineitem` batch spanning
/// hundreds of date partitions multiplied by several tables initialising in
/// parallel) from exhausting the S3 connection pool or the blocking-thread
/// pool on slow machines / networks.
const MAX_CONCURRENT_UPLOADS: usize = 8;

/// ETL sink that writes batches as hive-partitioned Parquet files in S3.
///
/// Each batch is written to a path of the form:
/// ```text
/// {prefix}/{table_name}/{col1}={value1}/{col2}={value2}/batch-{batch_id:06}.parquet
/// ```
///
/// Only `Insert` operations are supported. `Update` and `Delete` operations
/// will return an error.
pub struct S3HiveSink {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    partition_columns: Vec<String>,
    /// Limits how many partition encode+upload tasks run at once.
    upload_semaphore: Arc<Semaphore>,
}

impl S3HiveSink {
    /// Creates a new [`S3HiveSink`] from a [`TargetConfig`].
    ///
    /// The `prefix` field of the config specifies the destination bucket prefix
    /// that tables will be placed into.
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

        Ok(Self {
            store,
            prefix: config.prefix.clone(),
            partition_columns,
            upload_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_UPLOADS)),
        })
    }
}

/// Writes a single partition batch to S3 as a Parquet file.
///
/// Parquet encoding (CPU-bound) is offloaded to `spawn_blocking` so that the
/// async executor is not blocked during compression. The resulting bytes are
/// then uploaded with a single `PUT`.
async fn write_partition_task(
    store: Arc<dyn ObjectStore>,
    semaphore: Arc<Semaphore>,
    prefix: String,
    table_name: String,
    batch_id: u64,
    partition_path: String,
    batch: RecordBatch,
    partition_idx: usize,
) -> anyhow::Result<()> {
    let task_span = tracing::debug_span!(
        "etl.s3_hive.partition_write",
        table = %table_name,
        batch_id,
        partition_idx,
        partition_path = %partition_path,
    );

    // Acquire a concurrency slot before doing any work. This bounds the number
    // of simultaneous encode+upload operations sink-wide, preventing connection
    // pool exhaustion on slow networks when many partitions fan out at once.
    let _permit = semaphore
        .acquire_owned()
        .await
        .map_err(|_| anyhow::anyhow!("upload semaphore closed"))?;

    let path = if prefix.is_empty() {
        if partition_path.is_empty() {
            ObjectPath::from(format!(
                "{table_name}/batch-{batch_id:06}-{partition_idx:04}.parquet"
            ))
        } else {
            ObjectPath::from(format!(
                "{table_name}/{partition_path}/batch-{batch_id:06}-{partition_idx:04}.parquet"
            ))
        }
    } else if partition_path.is_empty() {
        ObjectPath::from(format!(
            "{prefix}/{table_name}/batch-{batch_id:06}-{partition_idx:04}.parquet",
        ))
    } else {
        ObjectPath::from(format!(
            "{prefix}/{table_name}/{partition_path}/batch-{batch_id:06}-{partition_idx:04}.parquet",
        ))
    };

    // Encode to Parquet + Snappy on a blocking thread so the async executor
    // is not stalled during CPU-intensive compression.
    let encode_start = Instant::now();
    let encode_span = tracing::debug_span!(
        parent: &task_span,
        "etl.s3_hive.parquet_encode",
        table = %table_name,
        batch_id,
        partition_idx,
    );
    let buf = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<u8>> {
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
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
        parquet_bytes = buf.len(),
        "Parquet encode completed"
    );

    let put_start = Instant::now();
    let put_span = tracing::debug_span!(
        parent: &task_span,
        "etl.s3_hive.s3_put",
        table = %table_name,
        batch_id,
        partition_idx,
        path = %path,
        bytes = buf.len(),
    );
    store
        .put(&path, buf.into())
        .instrument(put_span.clone())
        .await
        .map_err(|e| anyhow::anyhow!("S3 PUT failed for {path}: {e}"))?;
    tracing::debug!(
        parent: &put_span,
        elapsed_ms = put_start.elapsed().as_secs_f64() * 1000.0,
        "S3 PUT completed"
    );

    Ok(())
}

#[async_trait]
impl Sink for S3HiveSink {
    async fn write(
        &self,
        table_name: &str,
        batch_id: u64,
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
        let partitioning_span = tracing::debug_span!(
            "etl.s3_hive.partitioning",
            table = %table_name,
            batch_id,
            rows = batch.num_rows(),
            partition_columns = effective_partition_columns.len(),
        );
        let partitioning_start = Instant::now();
        let partitions = partition_batch(
            &batch,
            &partition_columns_with_idx,
            &projected_column_indices,
        )
        .inspect(|partitions| {
            tracing::debug!(
                parent: &partitioning_span,
                elapsed_ms = partitioning_start.elapsed().as_secs_f64() * 1000.0,
                partition_count = partitions.len(),
                "Partitioning completed"
            );
        })?;

        // Spawn all partition writes concurrently. Each task owns its data
        // so there is no contention, and S3 PUTs for different paths are
        // fully independent.
        let mut join_set: JoinSet<anyhow::Result<()>> = JoinSet::new();

        for (idx, (partition_path, partition_batch)) in partitions.into_iter().enumerate() {
            join_set.spawn(write_partition_task(
                Arc::clone(&self.store),
                Arc::clone(&self.upload_semaphore),
                self.prefix.clone(),
                table_name.to_string(),
                batch_id,
                partition_path,
                partition_batch,
                idx,
            ));
        }

        // Collect results; propagate the first error encountered.
        while let Some(result) = join_set.join_next().await {
            result.map_err(|e| anyhow::anyhow!("Partition write task panicked: {e}"))??;
        }

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
                    out.push_str(&values.value(row).to_string());
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
