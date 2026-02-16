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

use crate::config::SourceConfig;

use super::{Source, SourceBatch};

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
/// On initialization, runs `CALL dbgen(sf=...)` to create TPC-H tables in an in-memory
/// DuckDB database, then adds a `TIMESTAMPTZ` time column to each table. Yields batches
/// by querying each table in sequence with `LIMIT/OFFSET` pagination. When all rows from
/// all tables are exhausted, generates a new step of data for continuous append-based
/// generation.
pub struct DuckdbSource {
    conn: Connection,
    scale_factor: f64,
    batch_size: usize,
    total_batches: Option<u64>,
    batches_yielded: u64,

    /// Index into `TPCH_TABLE_TIME_COLUMNS` for the current table being read.
    table_index: usize,
    /// Current row offset within the current table.
    row_offset: usize,

    /// Step-based generation for continuous appends.
    /// Step 0 is the initial `dbgen` call; steps 1+ generate new non-overlapping data.
    current_step: u16,
    /// Total number of step partitions for `dbgen(children=...)`.
    load_steps: u16,
}

impl DuckdbSource {
    pub fn new(config: &SourceConfig) -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory()?;

        let load_steps = 100; // partition into 100 steps for continuous generation

        // Generate initial TPC-H data (step 0)
        let mut sql = format!(
            "INSTALL tpch; LOAD tpch; CALL dbgen(sf={sf}, children={load_steps}, step=0);",
            sf = config.scale_factor,
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
            batch_size = config.batch_size,
            "DuckDB TPC-H source initialized (step 0)"
        );

        Ok(Self {
            conn,
            scale_factor: config.scale_factor,
            batch_size: config.batch_size,
            total_batches: config.total_batches,
            batches_yielded: 0,
            table_index: 0,
            row_offset: 0,
            current_step: 0,
            load_steps,
        })
    }

    /// Advance to the next step of TPC-H data generation.
    /// Returns `false` if all steps are exhausted.
    fn advance_step(&mut self) -> anyhow::Result<bool> {
        self.current_step += 1;
        if self.current_step >= self.load_steps {
            return Ok(false);
        }

        let mut sql = format!(
            "INSTALL tpch; LOAD tpch; CALL dbgen(sf={sf}, children={load_steps}, step={step}, suffix='_new');",
            sf = self.scale_factor,
            load_steps = self.load_steps,
            step = self.current_step,
        );

        // Add time columns to new tables, merge into main tables, then drop
        for (table, time_col) in TPCH_TABLE_TIME_COLUMNS {
            sql.push_str(&format!(
                "ALTER TABLE {table}_new ADD COLUMN {time_col} TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;\
                 INSERT INTO {table} SELECT * FROM {table}_new;\
                 DROP TABLE {table}_new;"
            ));
        }

        self.conn.execute_batch(&sql)?;

        info!(step = self.current_step, "Generated new TPC-H data step");

        // Reset table iteration
        self.table_index = 0;
        self.row_offset = 0;

        Ok(true)
    }
}

impl Source for DuckdbSource {
    fn next_batch(&mut self) -> anyhow::Result<Option<SourceBatch>> {
        // Check batch limit
        if let Some(limit) = self.total_batches
            && self.batches_yielded >= limit
        {
            return Ok(None);
        }

        loop {
            if self.table_index >= TPCH_TABLE_TIME_COLUMNS.len() {
                // All tables exhausted for current step — generate next step
                if !self.advance_step()? {
                    return Ok(None); // all steps exhausted
                }
                continue;
            }

            let (table, _) = TPCH_TABLE_TIME_COLUMNS[self.table_index];
            let sql = format!(
                "SELECT * FROM {table} LIMIT {limit} OFFSET {offset}",
                limit = self.batch_size,
                offset = self.row_offset,
            );

            let mut stmt = self.conn.prepare(&sql)?;
            let batches: Vec<RecordBatch> = stmt.query_arrow([])?.collect();

            if batches.is_empty() || batches[0].num_rows() == 0 {
                // Table exhausted — move to next table
                self.table_index += 1;
                self.row_offset = 0;
                continue;
            }

            self.row_offset += batches[0].num_rows();
            self.batches_yielded += 1;

            return Ok(Some(SourceBatch {
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
