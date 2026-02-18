/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

pub mod simple_sequence;
pub mod tpch;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit};
use async_trait::async_trait;

use crate::config::DatasetConfig;

/// Metadata about a table in a dataset.
#[derive(Debug, Clone)]
pub struct DatasetTable {
    /// The name of the table.
    pub name: String,
    /// The Arrow schema for the table (without the time column).
    pub schema: SchemaRef,
    /// The time column for the table, if any.
    ///
    /// When set, this column is *not* included in [`schema`] — it is appended
    /// during rehydration via [`DatasetTable::rehydrate`].
    pub time_column: Option<String>,
}

impl DatasetTable {
    /// Returns the full schema including the time column, if one is configured.
    ///
    /// If `time_column` is `None`, this returns the same schema as [`schema`].
    pub fn rehydrated_schema(&self) -> SchemaRef {
        let Some(ref time_col) = self.time_column else {
            return Arc::clone(&self.schema);
        };

        let ts_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let mut fields: Vec<_> = self.schema.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(time_col, ts_type, true)));
        Arc::new(arrow::datatypes::Schema::new(fields))
    }

    /// Rehydrate a batch by appending the time column with the current timestamp.
    ///
    /// If this table has no `time_column`, the batch is returned unchanged.
    /// The batch schema must match [`schema`] (i.e. without the time column).
    pub fn rehydrate(&self, batch: &RecordBatch) -> anyhow::Result<RecordBatch> {
        if self.time_column.is_none() {
            anyhow::bail!(
                "Cannot rehydrate table '{}' without a time column",
                self.name
            );
        }

        if batch.schema() != self.schema {
            let mut diffs = Vec::new();
            let expected_fields = self.schema.fields();
            let actual_schema = batch.schema();
            let actual_fields = actual_schema.fields();

            for (i, expected) in expected_fields.iter().enumerate() {
                match actual_fields.get(i) {
                    Some(actual) if actual != expected => {
                        if actual.name() != expected.name() {
                            diffs.push(format!(
                                "  column {i}: expected name '{}', got '{}'",
                                expected.name(),
                                actual.name()
                            ));
                        }
                        if actual.data_type() != expected.data_type() {
                            diffs.push(format!(
                                "  column '{}' (index {i}): expected type {:?}, got {:?}",
                                expected.name(),
                                expected.data_type(),
                                actual.data_type()
                            ));
                        }
                        if actual.is_nullable() != expected.is_nullable() {
                            diffs.push(format!(
                                "  column '{}' (index {i}): expected nullable={}, got nullable={}",
                                expected.name(),
                                expected.is_nullable(),
                                actual.is_nullable()
                            ));
                        }
                    }
                    None => {
                        diffs.push(format!(
                            "  column '{}' (index {i}): missing from batch",
                            expected.name()
                        ));
                    }
                    _ => {}
                }
            }
            for i in expected_fields.len()..actual_fields.len() {
                diffs.push(format!(
                    "  column '{}' (index {i}): unexpected extra column in batch",
                    actual_fields[i].name()
                ));
            }
            if expected_fields.len() != actual_fields.len() {
                diffs.push(format!(
                    "  expected {} columns, got {}",
                    expected_fields.len(),
                    actual_fields.len()
                ));
            }

            anyhow::bail!(
                "Schema mismatch for table '{}':\n{}",
                self.name,
                diffs.join("\n")
            );
        }

        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before UNIX epoch")
            .as_micros() as i64;

        let num_rows = batch.num_rows();
        let timestamps =
            TimestampMicrosecondArray::from(vec![Some(now_us); num_rows]).with_timezone("UTC");

        let rehydrated_schema = self.rehydrated_schema();
        let mut columns: Vec<_> = batch.columns().to_vec();
        columns.push(Arc::new(timestamps));

        Ok(RecordBatch::try_new(rehydrated_schema, columns)?)
    }
}

#[async_trait]
pub trait Dataset: Send + Sync {
    /// Creates a new instance of this dataset from the given configuration.
    ///
    /// This is a factory method that returns an `Arc<dyn Dataset>` without any
    /// external side-effects beyond initialising in-memory state.
    ///
    /// The default implementation returns an error; concrete dataset types
    /// should override this.
    fn create(config: &DatasetConfig) -> anyhow::Result<Arc<dyn Dataset>>
    where
        Self: Sized + 'static,
    {
        let _ = config;
        anyhow::bail!("create() is not implemented for this dataset type")
    }

    /// Returns the batch IDs that would be produced for a given table after a
    /// successful generation run.
    ///
    /// The default implementation returns `0..num_batches(table)`, but
    /// implementations may override this to customise the ID scheme.
    fn batch_ids(&self, table: &str) -> VecDeque<u64> {
        (0..self.num_batches(table)).collect()
    }

    /// Returns the total number of batches this dataset will produce for the
    /// given table. Implementations must provide this so that [`batch_ids`]
    /// and downstream planning (e.g. `Target::expected_files`) can work.
    fn num_batches(&self, table: &str) -> u64;

    /// Returns the next raw batch of data for the given table, or `None` if exhausted.
    ///
    /// Most callers should use [`next_batch`]
    /// instead, which validates the table name and output schema.
    async fn raw_next_batch(&self, table: &str) -> anyhow::Result<Option<RecordBatch>>;

    /// Returns the next batch of data for the given table, or `None` if exhausted.
    ///
    /// Validates that `table` is a known table for this dataset, delegates to
    /// [`raw_next_batch`], and verifies the returned batch matches the expected schema.
    async fn next_batch(&self, table: &str) -> anyhow::Result<Option<RecordBatch>> {
        let tables = self.tables();
        let dataset_table = tables
            .get(table)
            .ok_or_else(|| anyhow::anyhow!("Unknown table: {table}"))?;
        let expected_schema = Arc::clone(&dataset_table.schema);

        let Some(batch) = self.raw_next_batch(table).await? else {
            return Ok(None);
        };

        if batch.schema() != expected_schema {
            let mut diffs = Vec::new();
            let expected_fields = expected_schema.fields();
            let actual_schema = batch.schema();
            let actual_fields = actual_schema.fields();

            for (i, expected) in expected_fields.iter().enumerate() {
                match actual_fields.get(i) {
                    Some(actual) if actual != expected => {
                        if actual.name() != expected.name() {
                            diffs.push(format!(
                                "  column {i}: expected name '{}', got '{}'",
                                expected.name(),
                                actual.name()
                            ));
                        }
                        if actual.data_type() != expected.data_type() {
                            diffs.push(format!(
                                "  column '{}' (index {i}): expected type {:?}, got {:?}",
                                expected.name(),
                                expected.data_type(),
                                actual.data_type()
                            ));
                        }
                        if actual.is_nullable() != expected.is_nullable() {
                            diffs.push(format!(
                                "  column '{}' (index {i}): expected nullable={}, got nullable={}",
                                expected.name(),
                                expected.is_nullable(),
                                actual.is_nullable()
                            ));
                        }
                    }
                    None => {
                        diffs.push(format!(
                            "  column '{}' (index {i}): missing from batch",
                            expected.name()
                        ));
                    }
                    _ => {}
                }
            }
            for i in expected_fields.len()..actual_fields.len() {
                diffs.push(format!(
                    "  column '{}' (index {i}): unexpected extra column in batch",
                    actual_fields[i].name()
                ));
            }
            if expected_fields.len() != actual_fields.len() {
                diffs.push(format!(
                    "  expected {} columns, got {}",
                    expected_fields.len(),
                    actual_fields.len()
                ));
            }

            anyhow::bail!("Schema mismatch for table '{table}':\n{}", diffs.join("\n"));
        }

        Ok(Some(batch))
    }

    /// Returns a batch for every table. Returns `None` if all tables are exhausted.
    async fn next_batches(&self) -> anyhow::Result<Option<HashMap<String, RecordBatch>>> {
        let tables = self.tables();
        let mut batches = HashMap::new();
        for (name, _) in &tables {
            if let Some(batch) = self.next_batch(name).await? {
                batches.insert(name.clone(), batch);
            }
        }
        if batches.is_empty() {
            Ok(None)
        } else {
            Ok(Some(batches))
        }
    }

    /// Returns the tables this dataset produces, including metadata, keyed by table name.
    fn tables(&self) -> HashMap<String, DatasetTable>;

    /// Rehydrate a batch for the given table by appending the time column.
    ///
    /// Uses the table metadata from [`tables()`] to look up the time column name
    /// and delegates to [`DatasetTable::rehydrate`]. If the table has no time column,
    /// a rehydration error is returned.
    fn rehydrate(&self, table: &str, batch: &RecordBatch) -> anyhow::Result<RecordBatch> {
        let tables = self.tables();
        let dataset_table = tables
            .get(table)
            .ok_or_else(|| anyhow::anyhow!("Unknown table: {table}"))?;
        dataset_table.rehydrate(batch)
    }
}

#[async_trait]
impl Dataset for Arc<dyn Dataset> {
    fn batch_ids(&self, table: &str) -> VecDeque<u64> {
        (**self).batch_ids(table)
    }

    fn num_batches(&self, table: &str) -> u64 {
        (**self).num_batches(table)
    }

    async fn raw_next_batch(&self, table: &str) -> anyhow::Result<Option<RecordBatch>> {
        (**self).raw_next_batch(table).await
    }

    async fn next_batch(&self, table: &str) -> anyhow::Result<Option<RecordBatch>> {
        (**self).next_batch(table).await
    }

    async fn next_batches(&self) -> anyhow::Result<Option<HashMap<String, RecordBatch>>> {
        (**self).next_batches().await
    }

    fn tables(&self) -> HashMap<String, DatasetTable> {
        (**self).tables()
    }

    fn rehydrate(&self, table: &str, batch: &RecordBatch) -> anyhow::Result<RecordBatch> {
        (**self).rehydrate(table, batch)
    }
}
