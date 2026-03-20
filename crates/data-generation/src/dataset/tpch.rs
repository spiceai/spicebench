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

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, Date32Array, Decimal128Array, Int32Array, Int64Array, RecordBatch,
    StringArray, StringViewArray, new_null_array,
};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use rand::Rng;
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};
use tpchgen_arrow::{
    CustomerArrow, LineItemArrow, NationArrow, OrderArrow, PartArrow, PartSuppArrow, RegionArrow,
    SupplierArrow,
};
use tracing::info;

use crate::config::DatasetConfig;
use crate::dataset::MutationConfig;
use crate::dataset::key_set::{IndexedKeySet, PrimaryKeyValue};
use crate::storage::DataStorage;

use super::{Dataset, DatasetTable};

/// TPC-H table definitions: `(table_name, time_column)`.
const TPCH_TABLES: &[(&str, &str)] = &[
    ("region", "r_created_at"),
    ("nation", "n_created_at"),
    ("supplier", "s_created_at"),
    ("customer", "c_created_at"),
    ("part", "p_created_at"),
    ("partsupp", "ps_created_at"),
    ("orders", "o_created_at"),
    ("lineitem", "l_created_at"),
];

/// Number of rows each TPC-H table produces at Scale Factor 1 (SF1).
///
/// These counts are used to calculate how many rows to emit per step when
/// partitioning the full dataset into `num_steps` batches. The total rows for
/// a given scale factor is `SF1_ROW_COUNTS[table] * scale_factor` (except for
/// region and nation which are fixed).
const SF1_ROW_COUNTS: &[(&str, u64)] = &[
    ("region", 5),
    ("nation", 25),
    ("supplier", 10_000),
    ("customer", 150_000),
    ("part", 200_000),
    ("partsupp", 800_000),
    ("orders", 1_500_000),
    ("lineitem", 6_001_215),
];

/// Returns the expected total number of rows for a given table at the
/// specified scale factor.
fn total_rows_for_table(table: &str, scale_factor: f64) -> u64 {
    let sf1_count = SF1_ROW_COUNTS
        .iter()
        .find(|(name, _)| *name == table)
        .map(|(_, count)| *count)
        .unwrap_or(0);

    // Region and nation are fixed regardless of scale factor.
    match table {
        "region" | "nation" => sf1_count,
        _ => (sf1_count as f64 * scale_factor).round() as u64,
    }
}

/// Returns the native Arrow schema for a TPC-H table (without `_op`/`_op_index`),
/// derived from `tpchgen-arrow`'s generated schema.
///
/// All fields are set to nullable so that delete-mutation rows (which contain
/// nulls for non-PK columns) can be represented in the same batch.
fn native_tpch_schema(table: &str) -> SchemaRef {
    use tpchgen_arrow::RecordBatchIterator as _;

    let schema: SchemaRef = match table {
        "region" => RegionArrow::new(RegionGenerator::new(1.0, 1, 1))
            .schema()
            .clone(),
        "nation" => NationArrow::new(NationGenerator::new(1.0, 1, 1))
            .schema()
            .clone(),
        "supplier" => SupplierArrow::new(SupplierGenerator::new(1.0, 1, 1))
            .schema()
            .clone(),
        "customer" => CustomerArrow::new(CustomerGenerator::new(1.0, 1, 1))
            .schema()
            .clone(),
        "part" => PartArrow::new(PartGenerator::new(1.0, 1, 1))
            .schema()
            .clone(),
        "partsupp" => PartSuppArrow::new(PartSuppGenerator::new(1.0, 1, 1))
            .schema()
            .clone(),
        "orders" => OrderArrow::new(OrderGenerator::new(1.0, 1, 1))
            .schema()
            .clone(),
        "lineitem" => LineItemArrow::new(LineItemGenerator::new(1.0, 1, 1))
            .schema()
            .clone(),
        _ => unreachable!("unknown TPC-H table: {table}"),
    };

    // Ensure all fields are nullable (needed for delete mutation rows).
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone().with_nullable(true))
        .collect();
    Arc::new(Schema::new(fields))
}

/// Returns the full Arrow schema for a TPC-H table, including the
/// `_op` (operation type) and `_op_index` (replay ordering) columns
/// used for change-tracking.
fn tpch_schema(table: &str) -> SchemaRef {
    let native = native_tpch_schema(table);
    let mut fields: Vec<Field> = native.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new("_op", DataType::Utf8, false));
    fields.push(Field::new("_op_index", DataType::Int64, false));
    Arc::new(Schema::new(fields))
}

// ---------------------------------------------------------------------------
// Helper: convert tpchgen rows to Arrow RecordBatch
// ---------------------------------------------------------------------------

/// Generates a raw [`RecordBatch`] (without `_op`/`_op_index`) from
/// `tpchgen-arrow` by producing `skip + count` rows in one batch and then
/// slicing off the first `skip` rows.
fn generate_raw_batch(table: &str, scale_factor: f64, part: i32, part_count: i32) -> RecordBatch {
    // Use a batch size large enough to capture all rows for this part in one call.
    let batch_size = total_rows_for_table(table, scale_factor) as usize;

    let batch: Option<RecordBatch> = match table {
        "region" => RegionArrow::new(RegionGenerator::new(scale_factor, part, part_count))
            .with_batch_size(batch_size)
            .next(),
        "nation" => NationArrow::new(NationGenerator::new(scale_factor, part, part_count))
            .with_batch_size(batch_size)
            .next(),
        "supplier" => SupplierArrow::new(SupplierGenerator::new(scale_factor, part, part_count))
            .with_batch_size(batch_size)
            .next(),
        "customer" => CustomerArrow::new(CustomerGenerator::new(scale_factor, part, part_count))
            .with_batch_size(batch_size)
            .next(),
        "part" => PartArrow::new(PartGenerator::new(scale_factor, part, part_count))
            .with_batch_size(batch_size)
            .next(),
        "partsupp" => PartSuppArrow::new(PartSuppGenerator::new(scale_factor, part, part_count))
            .with_batch_size(batch_size)
            .next(),
        "orders" => OrderArrow::new(OrderGenerator::new(scale_factor, part, part_count))
            .with_batch_size(batch_size)
            .next(),
        "lineitem" => LineItemArrow::new(LineItemGenerator::new(scale_factor, part, part_count))
            .with_batch_size(batch_size)
            .next(),
        _ => unreachable!("unknown TPC-H table: {table}"),
    };

    batch.unwrap_or_else(|| RecordBatch::new_empty(native_tpch_schema(table)))
}

// ---------------------------------------------------------------------------
// Helper functions for mutation (update / delete) generation
// ---------------------------------------------------------------------------

/// Generates a random alphanumeric string of the specified length.
fn random_alphanumeric_string(rng: &mut impl Rng, len: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    (0..len)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect()
}

/// Extracts an `i64` value from an Arrow array at the given row index.
/// Supports [`Int32Array`] (widened to `i64`) and [`Int64Array`].
fn get_i64_from_array(array: &dyn Array, row: usize) -> i64 {
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        arr.value(row)
    } else if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        i64::from(arr.value(row))
    } else {
        panic!(
            "PK column must be Int32 or Int64, got {:?}",
            array.data_type()
        );
    }
}

/// Extracts primary key values from all rows in a [`RecordBatch`].
///
/// Returns [`PrimaryKeyValue::Single`] for single-column PKs and
/// [`PrimaryKeyValue::Composite`] for multi-column PKs.
fn extract_pk_values(
    batch: &RecordBatch,
    pk_columns: &[String],
) -> anyhow::Result<Vec<PrimaryKeyValue>> {
    let col_indices: Vec<usize> = pk_columns
        .iter()
        .map(|name| {
            batch
                .schema()
                .index_of(name)
                .map_err(|e| anyhow::anyhow!("PK column '{name}' not found in batch: {e}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok((0..batch.num_rows())
        .map(|row| {
            if col_indices.len() == 1 {
                PrimaryKeyValue::single(get_i64_from_array(
                    batch.column(col_indices[0]).as_ref(),
                    row,
                ))
            } else {
                let values: Vec<i64> = col_indices
                    .iter()
                    .map(|&idx| get_i64_from_array(batch.column(idx).as_ref(), row))
                    .collect();
                PrimaryKeyValue::composite(&values)
            }
        })
        .collect())
}

/// Builds an Arrow array containing primary key values extracted from
/// [`PrimaryKeyValue`]s at the given `pk_position` index.
fn build_pk_array(keys: &[PrimaryKeyValue], pk_position: usize, data_type: &DataType) -> ArrayRef {
    let values: Vec<i64> = keys
        .iter()
        .map(|k| match k {
            PrimaryKeyValue::Single(v) => *v,
            PrimaryKeyValue::Composite(v) => v[pk_position],
        })
        .collect();

    match data_type {
        DataType::Int32 => Arc::new(Int32Array::from(
            values.iter().map(|v| *v as i32).collect::<Vec<_>>(),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(values)),
        _ => unreachable!("PK column type must be Int32 or Int64, got {data_type:?}"),
    }
}

/// Generates an array of `num_rows` random values matching the given Arrow
/// [`DataType`]. Used to populate non-PK columns for update mutations.
fn build_random_array(
    num_rows: usize,
    data_type: &DataType,
    rng: &mut impl Rng,
) -> anyhow::Result<ArrayRef> {
    match data_type {
        DataType::Int32 => {
            let values: Vec<i32> = (0..num_rows)
                .map(|_| rng.random_range(1..1_000_000i32))
                .collect();
            Ok(Arc::new(Int32Array::from(values)))
        }
        DataType::Int64 => {
            let values: Vec<i64> = (0..num_rows)
                .map(|_| rng.random_range(1..1_000_000_000i64))
                .collect();
            Ok(Arc::new(Int64Array::from(values)))
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let values: Vec<String> = (0..num_rows)
                .map(|_| random_alphanumeric_string(rng, 10))
                .collect();
            Ok(Arc::new(StringArray::from(
                values.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )))
        }
        DataType::Utf8View => {
            let values: Vec<String> = (0..num_rows)
                .map(|_| random_alphanumeric_string(rng, 10))
                .collect();
            Ok(Arc::new(StringViewArray::from(
                values.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )))
        }
        DataType::Decimal128(precision, scale) => {
            let scale_u32 = u32::try_from(*scale).map_err(|_| {
                anyhow::anyhow!("Decimal128 scale must be non-negative, got {scale}")
            })?;
            // Cap the effective fractional digits to 4 so that DuckDB arithmetic
            // (which sums scales on multiply) stays within its max scale of 38.
            let effective_scale = scale_u32.min(4);
            let effective_mult = 10i64.pow(effective_scale);
            // Zero-pad the remaining scale digits so the value matches the column's
            // declared scale.
            let padding = 10i128.pow(scale_u32 - effective_scale);
            // Limit the whole-number range to the available integer digits
            // (precision - scale), capped at 4 digits.
            let int_digits = ((*precision as i8) - (*scale)).max(1) as u32;
            let max_whole = 10i64.pow(int_digits.min(4));
            let values: Vec<i128> = (0..num_rows)
                .map(|_| {
                    let whole = rng.random_range(0..max_whole);
                    let frac = rng.random_range(0..effective_mult);
                    i128::from(whole * effective_mult + frac) * padding
                })
                .collect();
            Ok(Arc::new(
                Decimal128Array::from_iter_values(values)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "invalid Decimal128 precision/scale ({precision}, {scale}): {e}"
                        )
                    })?,
            ))
        }
        DataType::Date32 => {
            // Random dates roughly between 1992 and 2025 (epoch day offsets).
            let values: Vec<i32> = (0..num_rows)
                .map(|_| rng.random_range(8000..20000i32))
                .collect();
            Ok(Arc::new(Date32Array::from(values)))
        }
        other => Err(anyhow::anyhow!(
            "unsupported data type for random generation: {other:?}"
        )),
    }
}

/// Generates TPC-H data using `tpchgen-rs` iterators and yields Arrow `RecordBatch`es.
///
/// Data is partitioned into `num_steps` steps by dividing the total row count
/// for each table at the configured scale factor. Each call to `next_batch()`
/// returns all rows for one table in the current step.
pub struct TpchDataset {
    scale_factor: f64,

    /// Total number of step partitions (tpchgen `part_count`).
    num_steps: u16,
    /// Configuration for data mutations (update/delete ratios).
    mutations: MutationConfig,
    /// Per-table step counter tracking which part to generate next (0-indexed).
    table_steps: HashMap<String, AtomicU16>,
    /// Per-table primary key tracking for update/delete targeting.
    key_sets: HashMap<String, Mutex<IndexedKeySet<PrimaryKeyValue>>>,
    /// Global monotonically increasing operation counter for replay ordering.
    op_counter: AtomicI64,
    /// The storage backend for reading/writing table metadata.
    storage: Arc<dyn DataStorage>,
}

impl TpchDataset {
    pub fn new(
        config: &DatasetConfig,
        mutations: &MutationConfig,
        storage: Arc<dyn DataStorage>,
    ) -> anyhow::Result<Self> {
        info!(
            scale_factor = config.scale_factor,
            num_steps = config.num_steps,
            "tpchgen-rs TPC-H dataset initialized"
        );

        let key_sets: HashMap<String, Mutex<IndexedKeySet<PrimaryKeyValue>>> = TPCH_TABLES
            .iter()
            .map(|(name, _)| (name.to_string(), Mutex::new(IndexedKeySet::new())))
            .collect();

        let table_steps: HashMap<String, AtomicU16> = TPCH_TABLES
            .iter()
            .map(|(name, _)| (name.to_string(), AtomicU16::new(0)))
            .collect();

        Ok(Self {
            scale_factor: config.scale_factor,
            num_steps: config.num_steps,
            mutations: mutations.clone(),
            table_steps,
            key_sets,
            op_counter: AtomicI64::new(0),
            storage,
        })
    }
}

#[async_trait]
impl Dataset for TpchDataset {
    fn create(
        config: &DatasetConfig,
        mutations: &MutationConfig,
        storage: Arc<dyn DataStorage>,
    ) -> anyhow::Result<Arc<dyn Dataset>>
    where
        Self: Sized + 'static,
    {
        Ok(Arc::new(Self::new(config, mutations, storage)?))
    }

    fn storage(self: Arc<Self>) -> Arc<dyn DataStorage> {
        Arc::clone(&self.storage)
    }

    fn primary_key(&self, table: &str) -> Vec<String> {
        // Partition columns must be included in the primary key for correct
        // on-conflict behavior in distributed (scheduler+executor) mode.
        match table {
            "region" => vec!["r_regionkey".to_string()],
            "nation" => vec!["n_nationkey".to_string(), "n_regionkey".to_string()],
            "supplier" => vec!["s_suppkey".to_string(), "s_nationkey".to_string()],
            "customer" => vec!["c_custkey".to_string(), "c_nationkey".to_string()],
            "part" => vec!["p_partkey".to_string()],
            "partsupp" => vec!["ps_partkey".to_string(), "ps_suppkey".to_string()],
            "orders" => vec!["o_orderkey".to_string()],
            "lineitem" => vec!["l_orderkey".to_string(), "l_linenumber".to_string()],
            _ => vec![],
        }
    }

    fn partition_columns(&self, table: &str) -> Vec<String> {
        match table {
            "lineitem" => vec!["bucket(10, l_linenumber)".to_string()],
            "orders" => vec!["bucket(10, o_orderkey)".to_string()],
            "partsupp" => vec!["bucket(10, ps_partkey)".to_string()],
            "part" => vec!["bucket(10, p_partkey)".to_string()],
            "supplier" => vec!["s_nationkey".to_string()],
            "customer" => vec!["bucket(5, c_nationkey)".to_string()],
            "nation" => vec!["n_regionkey".to_string()],
            "region" => vec!["r_regionkey".to_string()],
            _ => vec![],
        }
    }

    fn num_batches(&self, table: &str) -> u64 {
        if !TPCH_TABLES.iter().any(|(name, _)| *name == table) {
            return 0;
        }

        // Region and nation are fixed-size tables and should be emitted once.
        if matches!(table, "region" | "nation") {
            return 1;
        }

        // One batch per step for all other tables.
        u64::from(self.num_steps)
    }

    async fn raw_next_batch(&self, table: &str) -> anyhow::Result<Option<RecordBatch>> {
        // Each table independently tracks which step (part) it is on.
        let step_counter = self
            .table_steps
            .get(table)
            .ok_or_else(|| anyhow::anyhow!("Unknown TPC-H table: {table}"))?;

        let current_step = step_counter.fetch_add(1, Ordering::SeqCst);
        // Region and nation do not support part/part_count partitioning in tpchgen.
        // Emit them only on the first step to avoid duplicate primary keys.
        if matches!(table, "region" | "nation") {
            if current_step > 0 {
                return Ok(None);
            }
        } else if current_step >= self.num_steps {
            return Ok(None); // all parts exhausted for this table
        }

        // Generate the raw batch using tpchgen part/part_count for correct partitioning.
        // Parts are 1-indexed in tpchgen; part_count = num_steps.
        let (part, part_count) = if matches!(table, "region" | "nation") {
            (1, 1)
        } else {
            (i32::from(current_step) + 1, i32::from(self.num_steps))
        };
        let batch = generate_raw_batch(table, self.scale_factor, part, part_count);
        let num_creates = batch.num_rows();

        if num_creates == 0 {
            return Ok(None);
        }

        // --- Primary key tracking and mutation planning ---
        let pk_columns = self.primary_key(table);
        let create_pks = extract_pk_values(&batch, &pk_columns)?;

        let key_set_mutex = self
            .key_sets
            .get(table)
            .ok_or_else(|| anyhow::anyhow!("No key set for table: {table}"))?;

        let (num_updates, num_deletes, update_keys, delete_keys) = {
            let mut ks = key_set_mutex
                .lock()
                .map_err(|e| anyhow::anyhow!("lock poisoned: {e}"))?;

            // Register all newly created primary keys.
            for pk in &create_pks {
                ks.insert(pk.clone());
            }

            let existing_count = ks.len();

            let mut rng = rand::rng();
            let jitter = |base: usize, rng: &mut rand::rngs::ThreadRng| -> usize {
                if base == 0 {
                    return 0;
                }
                let lo = (base as f64 * 0.75).floor() as usize;
                let hi = (base as f64 * 1.25).ceil() as usize;
                rng.random_range(lo..=hi.max(lo))
            };

            let base_updates =
                ((num_creates as f64) * self.mutations.update_ratio).round() as usize;
            let base_deletes =
                ((num_creates as f64) * self.mutations.delete_ratio).round() as usize;

            let mut num_updates = jitter(base_updates, &mut rng);
            let mut num_deletes = jitter(base_deletes, &mut rng);

            // Cap mutations to available distinct keys.
            if num_updates + num_deletes > existing_count {
                let scale = existing_count as f64 / (num_updates + num_deletes) as f64;
                num_updates = (num_updates as f64 * scale).floor() as usize;
                num_deletes = (num_deletes as f64 * scale).floor() as usize;
            }

            let mutation_keys = ks.sample_keys(num_updates + num_deletes, &mut rng);
            let update_keys = mutation_keys[..num_updates].to_vec();
            let delete_keys = mutation_keys[num_updates..].to_vec();

            for k in &delete_keys {
                ks.remove(k);
            }

            (num_updates, num_deletes, update_keys, delete_keys)
        };

        let total_rows = num_creates + num_updates + num_deletes;

        // Reserve a contiguous range of op indices for the full batch.
        let op_base = self
            .op_counter
            .fetch_add(total_rows as i64, Ordering::SeqCst);

        // --- Build the combined batch (creates + updates + deletes) ---
        let schema = tpch_schema(table);
        let native_field_count = schema.fields().len() - 2; // exclude _op, _op_index
        let mut rng = rand::rng();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

        for col_idx in 0..native_field_count {
            let field = &schema.fields()[col_idx];
            let creates_col = batch.column(col_idx);
            let pk_col_position = pk_columns.iter().position(|n| n == field.name());

            // Update values: PK columns use sampled keys, others get random data.
            let update_arr: ArrayRef = if let Some(pk_pos) = pk_col_position {
                build_pk_array(&update_keys, pk_pos, field.data_type())
            } else {
                build_random_array(num_updates, field.data_type(), &mut rng)?
            };

            // Delete values: PK columns use sampled keys, others are null.
            let delete_arr: ArrayRef = if let Some(pk_pos) = pk_col_position {
                build_pk_array(&delete_keys, pk_pos, field.data_type())
            } else {
                new_null_array(field.data_type(), num_deletes)
            };

            let combined = compute::concat(&[
                creates_col.as_ref(),
                update_arr.as_ref(),
                delete_arr.as_ref(),
            ])?;
            columns.push(combined);
        }

        // _op column: "c" for creates, "u" for updates, "d" for deletes.
        let ops: Vec<&str> = std::iter::repeat_n("c", num_creates)
            .chain(std::iter::repeat_n("u", num_updates))
            .chain(std::iter::repeat_n("d", num_deletes))
            .collect();
        columns.push(Arc::new(StringArray::from(ops)));

        // _op_index column: monotonically increasing replay counter.
        let op_indices: Vec<i64> = (op_base..op_base + total_rows as i64).collect();
        columns.push(Arc::new(Int64Array::from(op_indices)));

        Ok(Some(RecordBatch::try_new(schema, columns)?))
    }

    fn tables(&self) -> HashMap<String, DatasetTable> {
        TPCH_TABLES
            .iter()
            .map(|(name, time_col)| {
                (
                    (*name).to_string(),
                    DatasetTable {
                        name: (*name).to_string(),
                        schema: tpch_schema(name),
                        time_column: (*time_col).to_string(),
                    },
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::sync::Arc;

    use crate::storage::{ReadResult, WriteResult};
    use crate::version::VersionMetadata;

    struct NoopStorage;

    #[async_trait]
    impl DataStorage for NoopStorage {
        async fn list_batches(&self, _table_name: &str) -> anyhow::Result<Vec<String>> {
            Ok(Vec::new())
        }

        async fn read_batch(
            &self,
            _table_name: &str,
            _batch_id: u64,
            _part_id: Option<usize>,
        ) -> anyhow::Result<Option<ReadResult>> {
            Ok(None)
        }

        async fn write(
            &self,
            _table_name: &str,
            _batch_id: u64,
            _batch: RecordBatch,
        ) -> anyhow::Result<WriteResult> {
            Ok(WriteResult {
                rows_written: 0,
                bytes_written: 0,
                part_ids: Vec::new(),
            })
        }

        async fn write_version_metadata(&self, _metadata: &VersionMetadata) -> anyhow::Result<()> {
            Ok(())
        }

        async fn read_version_metadata(&self) -> anyhow::Result<Option<Arc<VersionMetadata>>> {
            Ok(None)
        }

        async fn read_batch_ids(&self, _table_name: &str) -> anyhow::Result<VecDeque<u64>> {
            Ok(VecDeque::new())
        }

        fn table_params(&self, _table_name: &str) -> HashMap<String, serde_json::Value> {
            HashMap::new()
        }

        fn expected_files(&self, _table_name: &str, _batch_ids: &[u64]) -> Vec<String> {
            Vec::new()
        }
    }

    fn build_dataset(scale_factor: f64, num_steps: u16) -> TpchDataset {
        TpchDataset::new(
            &DatasetConfig {
                dataset_type: "tpch".to_string(),
                scale_factor,
                num_steps,
            },
            &MutationConfig::new(0.0, 0.0),
            Arc::new(NoopStorage),
        )
        .expect("failed to construct TpchDataset")
    }

    #[tokio::test]
    async fn tpch_sf1_total_rows_match_expected_per_table() {
        let dataset = build_dataset(1.0, 25);

        for (table, _) in TPCH_TABLES {
            let mut total_rows = 0u64;

            while let Some(batch) = dataset
                .raw_next_batch(table)
                .await
                .expect("raw_next_batch should not fail")
            {
                total_rows += batch.num_rows() as u64;
            }

            assert_eq!(
                total_rows,
                total_rows_for_table(table, 1.0),
                "unexpected total row count for table '{table}' at SF1"
            );
        }
    }

    #[tokio::test]
    async fn tpch_emits_exactly_one_batch_per_step_for_non_static_tables() {
        let dataset = build_dataset(1.0, 7);

        for (table, _) in TPCH_TABLES {
            let mut emitted_batches = 0u64;

            while dataset
                .raw_next_batch(table)
                .await
                .expect("raw_next_batch should not fail")
                .is_some()
            {
                emitted_batches += 1;
            }

            assert_eq!(
                emitted_batches,
                dataset.num_batches(table),
                "emitted batches should match planned logical batches for table '{table}'"
            );
        }
    }

    #[tokio::test]
    async fn tpch_zero_mutation_batches_have_create_only_ops() {
        let dataset = build_dataset(1.0, 10);

        for (table, _) in TPCH_TABLES {
            while let Some(batch) = dataset
                .raw_next_batch(table)
                .await
                .expect("raw_next_batch should not fail")
            {
                let op_col_idx = batch
                    .schema()
                    .index_of("_op")
                    .expect("schema must contain _op column");
                let ops = batch
                    .column(op_col_idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("_op must be Utf8/StringArray");

                for row in 0..ops.len() {
                    assert!(!ops.is_null(row), "_op must not contain nulls");
                    assert_eq!(
                        ops.value(row),
                        "c",
                        "expected create op only for zero-mutation batch in table '{table}'"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn tpch_row_counts_match_expected_for_varied_scale_factors() {
        let scale_factors = [0.1, 1.0, 10.0];
        // Keep this test fast while still validating both fixed-size and scale-dependent tables.
        let tables = ["region", "nation", "supplier"];

        for scale_factor in scale_factors {
            let dataset = build_dataset(scale_factor, 25);

            for table in tables {
                let mut total_rows = 0u64;

                while let Some(batch) = dataset
                    .raw_next_batch(table)
                    .await
                    .expect("raw_next_batch should not fail")
                {
                    total_rows += batch.num_rows() as u64;
                }

                assert_eq!(
                    total_rows,
                    total_rows_for_table(table, scale_factor),
                    "unexpected total row count for table '{table}' at SF={scale_factor}"
                );
            }
        }
    }

    #[tokio::test]
    async fn tpch_sf1_lineitem_composite_primary_key_is_unique() {
        let dataset = build_dataset(1.0, 25);

        let mut seen_keys = HashSet::new();
        let mut total_rows = 0u64;

        while let Some(batch) = dataset
            .raw_next_batch("lineitem")
            .await
            .expect("raw_next_batch should not fail")
        {
            let order_idx = batch
                .schema()
                .index_of("l_orderkey")
                .expect("lineitem must contain l_orderkey");
            let line_idx = batch
                .schema()
                .index_of("l_linenumber")
                .expect("lineitem must contain l_linenumber");

            for row in 0..batch.num_rows() {
                let orderkey = get_i64_from_array(batch.column(order_idx).as_ref(), row) as u64;
                let linenumber = get_i64_from_array(batch.column(line_idx).as_ref(), row) as u64;
                let composite = (orderkey << 8) | linenumber;
                assert!(
                    seen_keys.insert(composite),
                    "duplicate lineitem PK detected: (l_orderkey={orderkey}, l_linenumber={linenumber})"
                );
            }

            total_rows += batch.num_rows() as u64;
        }

        assert_eq!(total_rows, total_rows_for_table("lineitem", 1.0));
    }
}
