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

use arrow::array::{Array, RecordBatch, StringArray, TimestampMicrosecondArray, UInt64Array};
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
use tokio::task::JoinSet;

use super::{InsertOp, Sink};

/// Default partitioning column used when no explicit scheme is configured.
const DEFAULT_PARTITION_COLUMN: &str = "__created_at";
const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

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
    prefix: String,
    table_name: String,
    batch_id: u64,
    partition_path: String,
    batch: RecordBatch,
    effective_partition_columns: Vec<String>,
    partition_idx: usize,
) -> anyhow::Result<()> {
    // Strip partition columns — they are encoded in the path.
    let batch_without_partition = strip_columns(&batch, &effective_partition_columns)?;

    if batch_without_partition.num_columns() == 0 {
        anyhow::bail!(
            "Cannot write table '{table_name}' with partition columns {effective_partition_columns:?}: \
             no columns would remain in parquet output"
        );
    }

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
    let buf = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<u8>> {
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut buf = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buf, batch_without_partition.schema(), Some(props))?;
        writer.write(&batch_without_partition)?;
        writer.close()?;
        Ok(buf)
    })
    .await
    .map_err(|e| anyhow::anyhow!("Parquet encoding task panicked: {e}"))??;

    store
        .put(&path, buf.into())
        .await
        .map_err(|e| anyhow::anyhow!("S3 PUT failed for {path}: {e}"))?;

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

        // Group rows by distinct partition tuples in configured column order.
        let partitions = partition_batch(&batch, &partition_columns_with_idx)?;

        // Spawn all partition writes concurrently. Each task owns its data
        // so there is no contention, and S3 PUTs for different paths are
        // fully independent.
        let mut join_set: JoinSet<anyhow::Result<()>> = JoinSet::new();

        for (idx, (partition_path, partition_batch)) in partitions.into_iter().enumerate() {
            join_set.spawn(write_partition_task(
                Arc::clone(&self.store),
                self.prefix.clone(),
                table_name.to_string(),
                batch_id,
                partition_path,
                partition_batch,
                effective_partition_columns.clone(),
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
) -> anyhow::Result<Vec<(String, RecordBatch)>> {
    // Build a mapping from partition path → row indices.
    let mut groups: indexmap::IndexMap<String, Vec<u64>> = indexmap::IndexMap::new();

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

    for row in 0..batch.num_rows() {
        let mut path_segments = Vec::with_capacity(partition_arrays.len());
        for (name, values) in &partition_arrays {
            let value = values.value_as_partition_key(row);
            path_segments.push(format!("{name}={value}"));
        }
        let key = path_segments.join("/");
        groups.entry(key).or_default().push(row as u64);
    }

    let mut result = Vec::with_capacity(groups.len());
    for (partition_value, row_indices) in groups {
        let indices = UInt64Array::from(row_indices);
        let columns: Vec<Arc<dyn Array>> = batch
            .columns()
            .iter()
            .map(|col| compute::take(col.as_ref(), &indices, None))
            .collect::<Result<_, _>>()
            .map_err(|e| anyhow::anyhow!("Failed to take rows for partition: {e}"))?;
        let sub_batch = RecordBatch::try_new(batch.schema(), columns)?;
        result.push((partition_value, sub_batch));
    }

    Ok(result)
}

enum PartitionColumnValues {
    TimestampMicrosecond(TimestampMicrosecondArray),
    Utf8(StringArray),
}

impl PartitionColumnValues {
    fn value_as_partition_key(&self, row: usize) -> String {
        match self {
            Self::TimestampMicrosecond(values) => {
                if values.is_null(row) {
                    HIVE_DEFAULT_PARTITION.to_string()
                } else {
                    values.value(row).to_string()
                }
            }
            Self::Utf8(values) => {
                if values.is_null(row) {
                    HIVE_DEFAULT_PARTITION.to_string()
                } else {
                    values.value(row).to_string()
                }
            }
        }
    }
}

/// Removes named columns from a [`RecordBatch`].
fn strip_columns(batch: &RecordBatch, column_names: &[String]) -> anyhow::Result<RecordBatch> {
    let schema = batch.schema();
    let indices_to_strip: std::collections::HashSet<usize> = column_names
        .iter()
        .filter_map(|name| schema.index_of(name).ok())
        .collect();

    if indices_to_strip.is_empty() {
        return Ok(batch.clone());
    }

    let new_fields: Vec<_> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(i, _)| !indices_to_strip.contains(i))
        .map(|(_, f)| f.clone())
        .collect();
    let new_columns: Vec<_> = batch
        .columns()
        .iter()
        .enumerate()
        .filter(|(i, _)| !indices_to_strip.contains(i))
        .map(|(_, c)| c.clone())
        .collect();

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(new_fields)),
        new_columns,
    )?)
}
