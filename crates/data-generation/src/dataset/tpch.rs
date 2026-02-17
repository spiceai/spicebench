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

use arrow::array::RecordBatch;
use duckdb::Connection;
use tracing::info;

use crate::config::DatasetConfig;

use super::{Dataset, DatasetBatch};

/// TPC-H tables with their corresponding time column names.
/// Matches the convention in `test-framework/src/queries/mod.rs`.
const TPCH_TABLE_TIME_COLUMNS: &[(&str, &str)] = &[
    ("region", "r_created_at"),
    ("nation", "n_created_at"),
    ("supplier", "s_created_at"),
    ("customer", "c_created_at"),
    ("part", "p_created_at"),
    ("partsupp", "ps_created_at"),
    ("orders", "o_created_at"),
    ("lineitem", "l_created_at"),
];

/// Generates TPC-H data using DuckDB's built-in `dbgen` and yields Arrow `RecordBatch`es.
///
/// Data is partitioned into `num_steps` steps using `dbgen(children=N, step=S)`.
/// Step 0 is generated on construction. Each subsequent step generates non-overlapping
/// data into temporary `_new` tables that are read and then dropped.
///
/// Each call to `next_batch()` returns all rows from one table for the current step.
pub struct TpchDataset {
    conn: Connection,
    scale_factor: f64,

    /// Index into `TPCH_TABLE_TIME_COLUMNS` for the current table being read.
    table_index: usize,

    /// Step-based generation for continuous appends.
    /// Step 0 is the initial `dbgen` call; steps 1+ generate new non-overlapping data.
    current_step: u16,
    /// Total number of step partitions for `dbgen(children=...)`.
    num_steps: u16,
}

impl TpchDataset {
    pub fn new(config: &DatasetConfig) -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory()?;

        // Generate initial TPC-H data (step 0)
        let mut sql = format!(
            "INSTALL tpch; LOAD tpch; CALL dbgen(sf={sf}, children={num_steps}, step=0);",
            sf = config.scale_factor,
            num_steps = config.num_steps,
        );

        // Add time columns to each table
        for (table, time_col) in TPCH_TABLE_TIME_COLUMNS {
            sql.push_str(&format!(
                "ALTER TABLE {table} ADD COLUMN {time_col} TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;"
            ));
        }

        conn.execute_batch(&sql)?;

        info!(
            scale_factor = config.scale_factor,
            num_steps = config.num_steps,
            "DuckDB TPC-H dataset initialized (step 0)"
        );

        Ok(Self {
            conn,
            scale_factor: config.scale_factor,
            table_index: 0,
            current_step: 0,
            num_steps: config.num_steps,
        })
    }

    /// Advance to the next step of TPC-H data generation.
    /// Generates new data into `_new` tables (read by `next_batch`, dropped when exhausted).
    /// Returns `false` if all steps are exhausted.
    fn advance_step(&mut self) -> anyhow::Result<bool> {
        self.current_step += 1;
        if self.current_step >= self.num_steps {
            return Ok(false);
        }

        let mut sql = format!(
            "CALL dbgen(sf={sf}, children={num_steps}, step={step}, suffix='_new');",
            sf = self.scale_factor,
            num_steps = self.num_steps,
            step = self.current_step,
        );

        // Add time columns to _new tables
        for (table, time_col) in TPCH_TABLE_TIME_COLUMNS {
            sql.push_str(&format!(
                "ALTER TABLE {table}_new ADD COLUMN {time_col} TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;"
            ));
        }

        self.conn.execute_batch(&sql)?;

        info!(step = self.current_step, "Generated new TPC-H data step");

        self.table_index = 0;

        Ok(true)
    }

    /// Drop the `_new` tables created by `advance_step()`.
    fn drop_step_tables(&self) -> anyhow::Result<()> {
        let mut sql = String::new();
        for (table, _) in TPCH_TABLE_TIME_COLUMNS {
            sql.push_str(&format!("DROP TABLE IF EXISTS {table}_new;"));
        }
        self.conn.execute_batch(&sql)?;
        Ok(())
    }
}

impl Dataset for TpchDataset {
    fn next_batch(&mut self) -> anyhow::Result<Option<DatasetBatch>> {
        loop {
            if self.table_index >= TPCH_TABLE_TIME_COLUMNS.len() {
                // All tables exhausted for current step — drop _new tables and advance
                if self.current_step > 0 {
                    self.drop_step_tables()?;
                }
                if !self.advance_step()? {
                    return Ok(None); // all steps exhausted
                }
                continue;
            }

            let (table, _) = TPCH_TABLE_TIME_COLUMNS[self.table_index];
            let source_table = if self.current_step == 0 {
                table.to_string()
            } else {
                format!("{table}_new")
            };

            let sql = format!("SELECT * FROM {source_table}");
            let mut stmt = self.conn.prepare(&sql)?;
            let batches: Vec<RecordBatch> = stmt.query_arrow([])?.collect();

            self.table_index += 1;

            if batches.is_empty() || batches[0].num_rows() == 0 {
                continue;
            }

            return Ok(Some(DatasetBatch {
                table_name: table.to_string(),
                batch: batches.into_iter().next().expect("checked non-empty"),
            }));
        }
    }

    fn tables(&self) -> Vec<String> {
        TPCH_TABLE_TIME_COLUMNS
            .iter()
            .map(|(name, _)| (*name).to_string())
            .collect()
    }

    fn time_column(&self, table: &str) -> Option<String> {
        TPCH_TABLE_TIME_COLUMNS
            .iter()
            .find(|(name, _)| *name == table)
            .map(|(_, col)| (*col).to_string())
    }
}
