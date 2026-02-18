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

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use duckdb::Connection;
use tracing::info;

use crate::config::DatasetConfig;

use super::{Dataset, DatasetTable};

/// TPC-H table definitions: (table_name, time_column, schema_fn).
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

/// Returns the static Arrow schema for a TPC-H table (including the appended time column).
fn tpch_schema(table: &str, time_col: &str) -> SchemaRef {
    let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let fields: Vec<Field> = match table {
        "region" => vec![
            Field::new("r_regionkey", DataType::Int32, false),
            Field::new("r_name", DataType::Utf8, false),
            Field::new("r_comment", DataType::Utf8, true),
            Field::new(time_col, ts, true),
        ],
        "nation" => vec![
            Field::new("n_nationkey", DataType::Int32, false),
            Field::new("n_name", DataType::Utf8, false),
            Field::new("n_regionkey", DataType::Int32, false),
            Field::new("n_comment", DataType::Utf8, true),
            Field::new(time_col, ts, true),
        ],
        "supplier" => vec![
            Field::new("s_suppkey", DataType::Int32, false),
            Field::new("s_name", DataType::Utf8, false),
            Field::new("s_address", DataType::Utf8, false),
            Field::new("s_nationkey", DataType::Int32, false),
            Field::new("s_phone", DataType::Utf8, false),
            Field::new("s_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("s_comment", DataType::Utf8, true),
            Field::new(time_col, ts, true),
        ],
        "customer" => vec![
            Field::new("c_custkey", DataType::Int32, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_nationkey", DataType::Int32, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_comment", DataType::Utf8, true),
            Field::new(time_col, ts, true),
        ],
        "part" => vec![
            Field::new("p_partkey", DataType::Int32, false),
            Field::new("p_name", DataType::Utf8, false),
            Field::new("p_mfgr", DataType::Utf8, false),
            Field::new("p_brand", DataType::Utf8, false),
            Field::new("p_type", DataType::Utf8, false),
            Field::new("p_size", DataType::Int32, false),
            Field::new("p_container", DataType::Utf8, false),
            Field::new("p_retailprice", DataType::Decimal128(15, 2), false),
            Field::new("p_comment", DataType::Utf8, true),
            Field::new(time_col, ts, true),
        ],
        "partsupp" => vec![
            Field::new("ps_partkey", DataType::Int32, false),
            Field::new("ps_suppkey", DataType::Int32, false),
            Field::new("ps_availqty", DataType::Int32, false),
            Field::new("ps_supplycost", DataType::Decimal128(15, 2), false),
            Field::new("ps_comment", DataType::Utf8, true),
            Field::new(time_col, ts, true),
        ],
        "orders" => vec![
            Field::new("o_orderkey", DataType::Int32, false),
            Field::new("o_custkey", DataType::Int32, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Decimal128(15, 2), false),
            Field::new("o_orderdate", DataType::Date32, false),
            Field::new("o_orderpriority", DataType::Utf8, false),
            Field::new("o_clerk", DataType::Utf8, false),
            Field::new("o_shippriority", DataType::Int32, false),
            Field::new("o_comment", DataType::Utf8, true),
            Field::new(time_col, ts, true),
        ],
        "lineitem" => vec![
            Field::new("l_orderkey", DataType::Int32, false),
            Field::new("l_partkey", DataType::Int32, false),
            Field::new("l_suppkey", DataType::Int32, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Decimal128(15, 2), false),
            Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
            Field::new("l_discount", DataType::Decimal128(15, 2), false),
            Field::new("l_tax", DataType::Decimal128(15, 2), false),
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Date32, false),
            Field::new("l_commitdate", DataType::Date32, false),
            Field::new("l_receiptdate", DataType::Date32, false),
            Field::new("l_shipinstruct", DataType::Utf8, false),
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("l_comment", DataType::Utf8, true),
            Field::new(time_col, ts, true),
        ],
        _ => unreachable!("unknown TPC-H table: {table}"),
    };
    Arc::new(Schema::new(fields))
}

/// Generates TPC-H data using DuckDB's built-in `dbgen` and yields Arrow `RecordBatch`es.
///
/// Data is partitioned into `num_steps` steps using `dbgen(children=N, step=S)`.
/// Step 0 is generated on construction. Each subsequent step generates non-overlapping
/// data into temporary `_new` tables that are read and then dropped.
///
/// Each call to `next_batch()` returns all rows from one table for the current step.
pub struct TpchDataset {
    conn: Mutex<Connection>,
    scale_factor: f64,

    /// Tables that have already been consumed in the current step.
    consumed_tables: RwLock<HashSet<String>>,

    /// Step-based generation for continuous appends.
    /// Step 0 is the initial `dbgen` call; steps 1+ generate new non-overlapping data.
    current_step: AtomicU16,
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
            conn: Mutex::new(conn),
            scale_factor: config.scale_factor,
            consumed_tables: RwLock::new(HashSet::new()),
            current_step: AtomicU16::new(0),
            num_steps: config.num_steps,
        })
    }

    /// Advance to the next step of TPC-H data generation.
    /// Generates new data into `_new` tables (read by `next_batch`, dropped when exhausted).
    /// Returns `false` if all steps are exhausted.
    fn advance_step(&self) -> anyhow::Result<bool> {
        let new_step = self.current_step.fetch_add(1, Ordering::SeqCst) + 1;
        if new_step >= self.num_steps {
            return Ok(false);
        }

        let mut sql = format!(
            "CALL dbgen(sf={sf}, children={num_steps}, step={step}, suffix='_new');",
            sf = self.scale_factor,
            num_steps = self.num_steps,
            step = new_step,
        );

        // Add time columns to _new tables
        for (table, time_col) in TPCH_TABLE_TIME_COLUMNS {
            sql.push_str(&format!(
                "ALTER TABLE {table}_new ADD COLUMN {time_col} TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;"
            ));
        }

        let conn = self.conn.lock().map_err(|e| anyhow::anyhow!("lock poisoned: {e}"))?;
        conn.execute_batch(&sql)?;

        info!(step = new_step, "Generated new TPC-H data step");

        Ok(true)
    }

    /// Drop the `_new` tables created by `advance_step()`.
    fn drop_step_tables(&self) -> anyhow::Result<()> {
        let mut sql = String::new();
        for (table, _) in TPCH_TABLE_TIME_COLUMNS {
            sql.push_str(&format!("DROP TABLE IF EXISTS {table}_new;"));
        }
        let conn = self.conn.lock().map_err(|e| anyhow::anyhow!("lock poisoned: {e}"))?;
        conn.execute_batch(&sql)?;
        Ok(())
    }
}

#[async_trait]
impl Dataset for TpchDataset {
    async fn raw_next_batch(&self, table: &str) -> anyhow::Result<Option<RecordBatch>> {
        // If all tables consumed for current step, advance to next step
        {
            let consumed = self.consumed_tables.read().map_err(|e| anyhow::anyhow!("lock poisoned: {e}"))?;
            if consumed.len() >= TPCH_TABLE_TIME_COLUMNS.len() {
                drop(consumed);
                if self.current_step.load(Ordering::SeqCst) > 0 {
                    self.drop_step_tables()?;
                }
                if !self.advance_step()? {
                    return Ok(None); // all steps exhausted
                }
                let mut consumed = self.consumed_tables.write().map_err(|e| anyhow::anyhow!("lock poisoned: {e}"))?;
                consumed.clear();
            }
        }

        // If this table was already consumed in the current step
        {
            let consumed = self.consumed_tables.read().map_err(|e| anyhow::anyhow!("lock poisoned: {e}"))?;
            if consumed.contains(table) {
                return Ok(None);
            }
        }

        // Validate the table name
        if !TPCH_TABLE_TIME_COLUMNS
            .iter()
            .any(|(name, _)| *name == table)
        {
            anyhow::bail!("Unknown TPC-H table: {table}");
        }

        let current_step = self.current_step.load(Ordering::SeqCst);
        let source_table = if current_step == 0 {
            table.to_string()
        } else {
            format!("{table}_new")
        };

        let batches = {
            let conn = self.conn.lock().map_err(|e| anyhow::anyhow!("lock poisoned: {e}"))?;
            let sql = format!("SELECT * FROM {source_table}");
            let mut stmt = conn.prepare(&sql)?;
            let batches: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
            batches
        };

        {
            let mut consumed = self.consumed_tables.write().map_err(|e| anyhow::anyhow!("lock poisoned: {e}"))?;
            consumed.insert(table.to_string());
        }

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(None);
        }

        Ok(Some(
            batches.into_iter().next().expect("checked non-empty"),
        ))
    }

    fn tables(&self) -> HashMap<String, DatasetTable> {
        TPCH_TABLE_TIME_COLUMNS
            .iter()
            .map(|(name, time_col)| {
                (
                    (*name).to_string(),
                    DatasetTable {
                        name: (*name).to_string(),
                        schema: tpch_schema(name, time_col),
                        time_column: Some((*time_col).to_string()),
                    },
                )
            })
            .collect()
    }
}
