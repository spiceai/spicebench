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

use std::sync::{Arc, Mutex};

use adbc_client::{AdbcConnection, IngestMode};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray, StringViewArray,
    TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use chrono::{Duration, NaiveDate};

use super::{InsertOp, Sink};

/// ETL sink that writes transformed batches directly into the SUT via ADBC.
///
/// For `Insert` operations this sink uses the ADBC bulk ingest API, which
/// binds Arrow `RecordBatch` data directly to a statement – avoiding the
/// overhead of constructing individual SQL INSERT statements.
///
/// Tables are expected to be pre-created (e.g. via a system adapter
/// `create_table` RPC), so the sink always uses [`IngestMode::Append`].
///
/// `Update` and `Delete` operations fall back to per-row SQL because
/// the ADBC bulk ingest API only supports append semantics.
pub struct AdbcSink {
    conn: Arc<Mutex<AdbcConnection>>,
    schema_name: Option<String>,
}

impl AdbcSink {
    #[must_use]
    pub fn new(conn: AdbcConnection, schema_name: Option<String>) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
            schema_name,
        }
    }

    fn table_identifier(&self, table_name: &str) -> String {
        match &self.schema_name {
            Some(schema) if !schema.is_empty() => {
                format!("{}.{table_name}", quote_identifier(schema))
            }
            _ => quote_identifier(table_name),
        }
    }

    async fn execute_sql_batch(&self, statements: Vec<String>) -> anyhow::Result<()> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let mut guard = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("ADBC connection lock poisoned: {e}"))?;
            for sql in statements {
                guard
                    .query(&sql)
                    .map_err(|e| anyhow::anyhow!("ADBC SQL execution failed: {e}"))?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await?
    }

    async fn bulk_ingest_batch(&self, table_name: &str, batch: RecordBatch) -> anyhow::Result<()> {
        let conn = Arc::clone(&self.conn);
        let target_table = table_name.to_string();
        let target_schema = self.schema_name.clone();
        let batch = normalize_utf8view_to_utf8(batch)?;
        tokio::task::spawn_blocking(move || {
            let mut guard = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("ADBC connection lock poisoned: {e}"))?;
            guard
                .bulk_ingest(
                    &target_table,
                    target_schema.as_deref(),
                    IngestMode::Append,
                    batch,
                )
                .map_err(|e| anyhow::anyhow!("ADBC bulk ingest failed: {e}"))?;
            Ok::<_, anyhow::Error>(())
        })
        .await?
    }
}

#[async_trait]
impl Sink for AdbcSink {
    async fn write(
        &self,
        table_name: &str,
        _batch_id: u64,
        batch: RecordBatch,
        op: InsertOp,
    ) -> anyhow::Result<()> {
        match op {
            InsertOp::Insert => {
                if batch.num_rows() == 0 {
                    return Ok(());
                }

                self.bulk_ingest_batch(table_name, batch).await?;
            }
            InsertOp::Update { ref key_columns } => {
                if batch.num_rows() == 0 {
                    return Ok(());
                }
                let key_indexes = key_column_indexes(&batch, key_columns)?;
                let mut statements = Vec::new();
                for row_idx in 0..batch.num_rows() {
                    statements.push(self.update_sql_for_row(
                        table_name,
                        &batch,
                        row_idx,
                        &key_indexes,
                    )?);
                }
                self.execute_sql_batch(statements).await?;
            }
            InsertOp::Delete { ref key_columns } => {
                if batch.num_rows() == 0 {
                    return Ok(());
                }
                let key_indexes = key_column_indexes(&batch, key_columns)?;
                let mut statements = Vec::new();
                for row_idx in 0..batch.num_rows() {
                    statements.push(self.delete_sql_for_row(
                        table_name,
                        &batch,
                        row_idx,
                        &key_indexes,
                    )?);
                }
                self.execute_sql_batch(statements).await?;
            }
        }

        Ok(())
    }
}

fn key_column_indexes(batch: &RecordBatch, key_columns: &[String]) -> anyhow::Result<Vec<usize>> {
    if key_columns.is_empty() {
        anyhow::bail!("Update/Delete requires at least one key column");
    }

    let schema = batch.schema();
    key_columns
        .iter()
        .map(|col| {
            schema
                .index_of(col)
                .map_err(|_| anyhow::anyhow!("Key column '{col}' not found in batch schema"))
        })
        .collect()
}

impl AdbcSink {
    fn update_sql_for_row(
        &self,
        table_name: &str,
        batch: &RecordBatch,
        row_idx: usize,
        key_indexes: &[usize],
    ) -> anyhow::Result<String> {
        let schema = batch.schema();
        let fields = schema.fields();
        let mut set_clauses = Vec::new();
        for col_idx in 0..batch.num_columns() {
            if key_indexes.contains(&col_idx) {
                continue;
            }
            let field = &fields[col_idx];
            let value =
                sql_literal_for_value(&batch.columns()[col_idx], field.data_type(), row_idx)?;
            set_clauses.push(format!("{} = {value}", quote_identifier(field.name())));
        }

        if set_clauses.is_empty() {
            anyhow::bail!("Update requires at least one non-key column in batch schema");
        }

        let where_clause = where_clause_for_row(batch, row_idx, key_indexes)?;
        Ok(format!(
            "UPDATE {} SET {} WHERE {where_clause}",
            self.table_identifier(table_name),
            set_clauses.join(", ")
        ))
    }

    fn delete_sql_for_row(
        &self,
        table_name: &str,
        batch: &RecordBatch,
        row_idx: usize,
        key_indexes: &[usize],
    ) -> anyhow::Result<String> {
        let where_clause = where_clause_for_row(batch, row_idx, key_indexes)?;
        Ok(format!(
            "DELETE FROM {} WHERE {where_clause}",
            self.table_identifier(table_name)
        ))
    }
}

fn where_clause_for_row(
    batch: &RecordBatch,
    row_idx: usize,
    key_indexes: &[usize],
) -> anyhow::Result<String> {
    let schema = batch.schema();
    let fields = schema.fields();
    let mut predicates = Vec::with_capacity(key_indexes.len());
    for &col_idx in key_indexes {
        let field = &fields[col_idx];
        let column = &batch.columns()[col_idx];
        let col_ident = quote_identifier(field.name());
        if column.is_null(row_idx) {
            predicates.push(format!("{col_ident} IS NULL"));
        } else {
            let value = sql_literal_for_value(column, field.data_type(), row_idx)?;
            predicates.push(format!("{col_ident} = {value}"));
        }
    }
    Ok(predicates.join(" AND "))
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

/// Cast any `Utf8View` columns to `Utf8` so the batch schema matches the
/// DDL-created table schema (which uses `VARCHAR` / `Utf8`).
fn normalize_utf8view_to_utf8(batch: RecordBatch) -> anyhow::Result<RecordBatch> {
    let schema = batch.schema();
    if !schema
        .fields()
        .iter()
        .any(|f| f.data_type() == &DataType::Utf8View)
    {
        return Ok(batch);
    }

    let mut new_fields = Vec::with_capacity(schema.fields().len());
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

    for (i, field) in schema.fields().iter().enumerate() {
        if field.data_type() == &DataType::Utf8View {
            new_fields.push(Arc::new(Field::new(
                field.name(),
                DataType::Utf8,
                field.is_nullable(),
            )));
            let casted = cast(batch.column(i), &DataType::Utf8).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to cast Utf8View to Utf8 for column '{}': {e}",
                    field.name()
                )
            })?;
            new_columns.push(casted);
        } else {
            new_fields.push(Arc::clone(field));
            new_columns.push(Arc::clone(batch.column(i)));
        }
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns).map_err(|e| {
        anyhow::anyhow!("Failed to rebuild RecordBatch after Utf8View normalization: {e}")
    })
}

fn quote_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_literal_for_value(
    column: &ArrayRef,
    data_type: &DataType,
    row_idx: usize,
) -> anyhow::Result<String> {
    if column.is_null(row_idx) {
        return Ok("NULL".to_string());
    }

    match data_type {
        DataType::Boolean => Ok(as_array::<BooleanArray>(column, data_type)?
            .value(row_idx)
            .to_string()),
        DataType::Int8 => Ok(as_array::<Int8Array>(column, data_type)?
            .value(row_idx)
            .to_string()),
        DataType::Int16 => Ok(as_array::<Int16Array>(column, data_type)?
            .value(row_idx)
            .to_string()),
        DataType::Int32 => Ok(as_array::<Int32Array>(column, data_type)?
            .value(row_idx)
            .to_string()),
        DataType::Int64 => Ok(as_array::<Int64Array>(column, data_type)?
            .value(row_idx)
            .to_string()),
        DataType::UInt8 => Ok(as_array::<UInt8Array>(column, data_type)?
            .value(row_idx)
            .to_string()),
        DataType::UInt16 => Ok(as_array::<UInt16Array>(column, data_type)?
            .value(row_idx)
            .to_string()),
        DataType::UInt32 => Ok(as_array::<UInt32Array>(column, data_type)?
            .value(row_idx)
            .to_string()),
        DataType::UInt64 => Ok(as_array::<UInt64Array>(column, data_type)?
            .value(row_idx)
            .to_string()),
        DataType::Float32 => {
            let value = as_array::<Float32Array>(column, data_type)?.value(row_idx);
            if value.is_finite() {
                Ok(value.to_string())
            } else {
                Ok("NULL".to_string())
            }
        }
        DataType::Float64 => {
            let value = as_array::<Float64Array>(column, data_type)?.value(row_idx);
            if value.is_finite() {
                Ok(value.to_string())
            } else {
                Ok("NULL".to_string())
            }
        }
        DataType::Utf8 => {
            let value = as_array::<StringArray>(column, data_type)?.value(row_idx);
            Ok(quote_string_literal(value))
        }
        DataType::Utf8View => {
            let value = as_array::<StringViewArray>(column, data_type)?.value(row_idx);
            Ok(quote_string_literal(value))
        }
        DataType::LargeUtf8 => {
            let value = column
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast LargeUtf8 array"))?
                .value(row_idx);
            Ok(quote_string_literal(value))
        }
        DataType::Date32 => {
            let days = as_array::<Date32Array>(column, data_type)?.value(row_idx);
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                .ok_or_else(|| anyhow::anyhow!("Invalid epoch date"))?;
            let date = epoch
                .checked_add_signed(Duration::days(i64::from(days)))
                .ok_or_else(|| anyhow::anyhow!("Date32 out of range: {days}"))?;
            Ok(format!(
                "DATE {}",
                quote_string_literal(&date.format("%Y-%m-%d").to_string())
            ))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
            let micros = as_array::<TimestampMicrosecondArray>(column, data_type)?.value(row_idx);
            let ts = chrono::DateTime::from_timestamp_micros(micros)
                .ok_or_else(|| anyhow::anyhow!("Timestamp out of range: {micros}"))?;
            Ok(format!(
                "TIMESTAMP {}",
                quote_string_literal(&ts.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            ))
        }
        DataType::Decimal128(_, scale) => {
            let raw = as_array::<Decimal128Array>(column, data_type)?.value(row_idx);
            Ok(decimal128_to_sql_string(raw, *scale))
        }
        other => anyhow::bail!("Unsupported Arrow data type in row serialization: {other:?}"),
    }
}

fn as_array<'a, T: Array + 'static>(
    column: &'a ArrayRef,
    data_type: &DataType,
) -> anyhow::Result<&'a T> {
    column.as_any().downcast_ref::<T>().ok_or_else(|| {
        anyhow::anyhow!(
            "Failed to downcast array for data type {data_type:?} to {}",
            std::any::type_name::<T>()
        )
    })
}

fn decimal128_to_sql_string(value: i128, scale: i8) -> String {
    if scale <= 0 {
        return value.to_string();
    }

    let sign = if value < 0 { "-" } else { "" };
    let abs = value.unsigned_abs().to_string();
    let scale_usize = scale as usize;

    if abs.len() <= scale_usize {
        let padded = format!("{:0>width$}", abs, width = scale_usize);
        return format!("{sign}0.{padded}");
    }

    let split = abs.len() - scale_usize;
    let (int_part, frac_part) = abs.split_at(split);
    format!("{sign}{int_part}.{frac_part}")
}
