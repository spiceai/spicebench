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

pub mod tpch;
pub mod simple_sequence;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

/// Metadata about a table in a dataset.
#[derive(Debug, Clone)]
pub struct DatasetTable {
    /// The name of the table.
    pub name: String,
    /// The Arrow schema for the table.
    pub schema: SchemaRef,
    /// The time column for the table, if any.
    pub time_column: Option<String>,
}

pub trait Dataset: Send {
    /// Returns the next raw batch of data for the given table, or `None` if exhausted.
    ///
    /// Most callers should use [`next_batch`]
    /// instead, which validates the table name and output schema.
    fn raw_next_batch(&mut self, table: &str) -> anyhow::Result<Option<RecordBatch>>;

    /// Returns the next batch of data for the given table, or `None` if exhausted.
    ///
    /// Validates that `table` is a known table for this dataset, delegates to
    /// [`raw_next_batch`], and verifies the returned batch matches the expected schema.
    fn next_batch(&mut self, table: &str) -> anyhow::Result<Option<RecordBatch>> {
        let tables = self.tables();
        let dataset_table = tables
            .get(table)
            .ok_or_else(|| anyhow::anyhow!("Unknown table: {table}"))?;
        let expected_schema = Arc::clone(&dataset_table.schema);

        let Some(batch) = self.raw_next_batch(table)? else {
            return Ok(None);
        };

        if batch.schema() != expected_schema {
            anyhow::bail!(
                "Schema mismatch for table '{table}': expected {expected_schema}, got {}",
                batch.schema()
            );
        }

        Ok(Some(batch))
    }

    /// Returns a batch for every table. Returns `None` if all tables are exhausted.
    fn next_batches(&mut self) -> anyhow::Result<Option<HashMap<String, RecordBatch>>> {
        let tables = self.tables();
        let mut batches = HashMap::new();
        for (name, _) in &tables {
            if let Some(batch) = self.next_batch(name)? {
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
}

impl Dataset for Box<dyn Dataset> {
    fn raw_next_batch(&mut self, table: &str) -> anyhow::Result<Option<RecordBatch>> {
        (**self).raw_next_batch(table)
    }

    fn next_batch(&mut self, table: &str) -> anyhow::Result<Option<RecordBatch>> {
        (**self).next_batch(table)
    }

    fn next_batches(&mut self) -> anyhow::Result<Option<HashMap<String, RecordBatch>>> {
        (**self).next_batches()
    }

    fn tables(&self) -> HashMap<String, DatasetTable> {
        (**self).tables()
    }
}
