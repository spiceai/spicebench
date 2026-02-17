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

use arrow::array::RecordBatch;

/// A batch of data from a dataset, tagged with its table name.
pub struct DatasetBatch {
    pub table_name: String,
    pub batch: RecordBatch,
}

pub trait Dataset: Send {
    /// Returns the next batch of data, or `None` if exhausted.
    fn next_batch(&mut self) -> anyhow::Result<Option<DatasetBatch>>;

    /// Returns the list of table names this dataset produces.
    fn tables(&self) -> Vec<String>;

    /// Returns the time column name for the given table, if any.
    fn time_column(&self, table: &str) -> Option<String>;
}

impl Dataset for Box<dyn Dataset> {
    fn next_batch(&mut self) -> anyhow::Result<Option<DatasetBatch>> {
        (**self).next_batch()
    }

    fn tables(&self) -> Vec<String> {
        (**self).tables()
    }

    fn time_column(&self, table: &str) -> Option<String> {
        (**self).time_column(table)
    }
}
