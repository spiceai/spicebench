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

use arrow::array::{Array, RecordBatch, TimestampMicrosecondArray};
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

use super::{InsertOp, Sink};

/// The column used for hive-style partitioning.
const PARTITION_COLUMN: &str = "__created_at";

/// ETL sink that writes batches as hive-partitioned Parquet files in S3.
///
/// Each batch is written to a path of the form:
/// ```text
/// {prefix}/{table_name}/__created_at={value}/batch-{batch_id:06}.parquet
/// ```
///
/// Only `Insert` operations are supported. `Update` and `Delete` operations
/// will return an error.
pub struct S3HiveSink {
    store: Arc<dyn ObjectStore>,
    prefix: String,
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
        Ok(Self {
            store,
            prefix: config.prefix.clone(),
        })
    }

    /// Writes a single partition's batch to S3 as a Parquet file.
    async fn write_partition(
        &self,
        table_name: &str,
        batch_id: u64,
        partition_value: &str,
        batch: &RecordBatch,
        partition_idx: usize,
    ) -> anyhow::Result<()> {
        // Strip the partition column from the written data — it's encoded in the path.
        let batch_without_partition = strip_column(batch, PARTITION_COLUMN)?;

        let path = if self.prefix.is_empty() {
            ObjectPath::from(format!(
                "{table_name}/{PARTITION_COLUMN}={partition_value}/batch-{batch_id:06}-{partition_idx:04}.parquet"
            ))
        } else {
            ObjectPath::from(format!(
                "{}/{table_name}/{PARTITION_COLUMN}={partition_value}/batch-{batch_id:06}-{partition_idx:04}.parquet",
                self.prefix
            ))
        };

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut buf = Vec::new();
        {
            let mut writer =
                ArrowWriter::try_new(&mut buf, batch_without_partition.schema(), Some(props))?;
            writer.write(&batch_without_partition)?;
            writer.close()?;
        }

        self.store
            .put(&path, buf.into())
            .await
            .map_err(|e| anyhow::anyhow!("S3 PUT failed for {path}: {e}"))?;

        Ok(())
    }
}

#[async_trait]
impl Sink for S3HiveSink {
    async fn write(
        &self,
        table_name: &str,
        batch_id: u64,
        batch: RecordBatch,
        op: InsertOp,
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
        let partition_col_idx = schema.index_of(PARTITION_COLUMN).map_err(|_| {
            anyhow::anyhow!(
                "Batch for table '{table_name}' is missing required partition column '{PARTITION_COLUMN}'"
            )
        })?;

        // Group rows by distinct partition values.
        let partitions = partition_batch(&batch, partition_col_idx)?;

        for (idx, (partition_value, partition_batch)) in partitions.iter().enumerate() {
            self.write_partition(table_name, batch_id, partition_value, partition_batch, idx)
                .await?;
        }

        Ok(())
    }
}

/// Splits a [`RecordBatch`] into groups based on distinct values of the column
/// at `col_idx`. Returns `(partition_value_string, sub_batch)` pairs.
fn partition_batch(
    batch: &RecordBatch,
    col_idx: usize,
) -> anyhow::Result<Vec<(String, RecordBatch)>> {
    let col = batch.column(col_idx);

    // Build a mapping from partition value → row indices.
    let mut groups: indexmap::IndexMap<String, Vec<u64>> = indexmap::IndexMap::new();

    if let Some(ts_array) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        for row in 0..batch.num_rows() {
            let key = if ts_array.is_null(row) {
                "__HIVE_DEFAULT_PARTITION__".to_string()
            } else {
                ts_array.value(row).to_string()
            };
            groups.entry(key).or_default().push(row as u64);
        }
    } else {
        // Fallback — use Display formatting for the array element.
        let string_repr = arrow::array::cast::as_string_array(
            &compute::cast(col, &arrow::datatypes::DataType::Utf8)
                .map_err(|e| anyhow::anyhow!("Failed to cast partition column to string: {e}"))?,
        )
        .clone();
        for row in 0..batch.num_rows() {
            let key = if string_repr.is_null(row) {
                "__HIVE_DEFAULT_PARTITION__".to_string()
            } else {
                string_repr.value(row).to_string()
            };
            groups.entry(key).or_default().push(row as u64);
        }
    }

    let mut result = Vec::with_capacity(groups.len());
    for (partition_value, row_indices) in groups {
        let indices =
            arrow::array::UInt64Array::from(row_indices);
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

/// Removes a named column from a [`RecordBatch`].
fn strip_column(batch: &RecordBatch, column_name: &str) -> anyhow::Result<RecordBatch> {
    let schema = batch.schema();
    let idx = match schema.index_of(column_name) {
        Ok(i) => i,
        Err(_) => return Ok(batch.clone()), // column not present — nothing to strip
    };

    let new_fields: Vec<_> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != idx)
        .map(|(_, f)| f.clone())
        .collect();
    let new_columns: Vec<_> = batch
        .columns()
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != idx)
        .map(|(_, c)| c.clone())
        .collect();

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(new_fields)),
        new_columns,
    )?)
}
