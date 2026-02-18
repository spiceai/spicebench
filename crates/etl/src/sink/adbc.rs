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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use adbc_client::AdbcConnection;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Schema};
use async_trait::async_trait;
use chrono::{Duration, NaiveDate};
use data_generation::target::{Target, WriteResult};
use tokio::sync::Mutex as TokioMutex;

const DEFAULT_INSERT_ROWS_PER_STATEMENT: usize = 256;

/// ETL sink that writes transformed batches directly into the SUT via ADBC SQL.
///
/// This sink creates destination tables on first write (`CREATE TABLE IF NOT EXISTS`)
/// and appends rows with batched `INSERT INTO ... VALUES` statements.
pub struct AdbcSink {
    conn: Arc<Mutex<AdbcConnection>>,
    created_tables: TokioMutex<HashSet<String>>,
    schema_name: Option<String>,
    insert_rows_per_statement: usize,
}

impl AdbcSink {
    #[must_use]
    pub fn new(conn: AdbcConnection, schema_name: Option<String>) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
            created_tables: TokioMutex::new(HashSet::new()),
            schema_name,
            insert_rows_per_statement: DEFAULT_INSERT_ROWS_PER_STATEMENT,
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

    fn create_table_sql(&self, table_name: &str, schema: &Schema) -> anyhow::Result<String> {
        let columns = schema
            .fields()
            .iter()
            .map(|f| {
                let col_type = sql_type_for_arrow(f.data_type())?;
                Ok::<_, anyhow::Error>(format!("{} {col_type}", quote_identifier(f.name())))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .join(", ");

        Ok(format!(
            "CREATE TABLE IF NOT EXISTS {} ({columns})",
            self.table_identifier(table_name)
        ))
    }

    async fn execute_sql(&self, sql: String) -> anyhow::Result<()> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let mut guard = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("ADBC connection lock poisoned: {e}"))?;
            guard
                .query(&sql)
                .map_err(|e| anyhow::anyhow!("ADBC SQL execution failed: {e}"))?;
            Ok::<_, anyhow::Error>(())
        })
        .await?
    }

    async fn ensure_table_created(&self, table_name: &str, schema: &Schema) -> anyhow::Result<()> {
        {
            let created = self.created_tables.lock().await;
            if created.contains(table_name) {
                return Ok(());
            }
        }

        let sql = self.create_table_sql(table_name, schema)?;
        self.execute_sql(sql).await?;

        let mut created = self.created_tables.lock().await;
        created.insert(table_name.to_string());

        Ok(())
    }

    fn insert_sql_for_rows(
        &self,
        table_name: &str,
        batch: &RecordBatch,
        row_range: std::ops::Range<usize>,
    ) -> anyhow::Result<String> {
        let mut tuples = Vec::with_capacity(row_range.len());
        for row_idx in row_range {
            let mut values = Vec::with_capacity(batch.num_columns());
            for (column, field) in batch.columns().iter().zip(batch.schema().fields()) {
                values.push(sql_literal_for_value(column, field.data_type(), row_idx)?);
            }
            tuples.push(format!("({})", values.join(", ")));
        }

        Ok(format!(
            "INSERT INTO {} VALUES {}",
            self.table_identifier(table_name),
            tuples.join(", ")
        ))
    }
}

#[async_trait]
impl Target for AdbcSink {
    async fn write(
        &self,
        table_name: &str,
        _batch_id: u64,
        batch: RecordBatch,
    ) -> anyhow::Result<WriteResult> {
        self.ensure_table_created(table_name, &batch.schema()).await?;

        let num_rows = batch.num_rows();
        if num_rows > 0 {
            let mut start = 0usize;
            while start < num_rows {
                let end = std::cmp::min(start + self.insert_rows_per_statement, num_rows);
                let sql = self.insert_sql_for_rows(table_name, &batch, start..end)?;
                self.execute_sql(sql).await?;
                start = end;
            }
        }

        Ok(WriteResult {
            rows_written: num_rows as u64,
            bytes_written: batch.get_array_memory_size() as u64,
        })
    }

    fn table_params(&self, _table_name: &str) -> HashMap<String, serde_json::Value> {
        HashMap::new()
    }

    fn expected_files(&self, _table_name: &str, _batch_ids: &[u64]) -> Vec<String> {
        Vec::new()
    }
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn quote_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_type_for_arrow(data_type: &DataType) -> anyhow::Result<&'static str> {
    match data_type {
        DataType::Boolean => Ok("BOOLEAN"),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16 => {
            Ok("INT")
        }
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => Ok("BIGINT"),
        DataType::Float32 => Ok("FLOAT"),
        DataType::Float64 => Ok("DOUBLE"),
        DataType::Utf8 | DataType::LargeUtf8 => Ok("STRING"),
        DataType::Date32 => Ok("DATE"),
        DataType::Timestamp(_, _) => Ok("TIMESTAMP"),
        DataType::Decimal128(_, _) => Ok("DECIMAL(38, 18)"),
        other => anyhow::bail!("Unsupported Arrow data type for ADBC sink: {other:?}"),
    }
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
        DataType::Boolean => Ok(as_array::<BooleanArray>(column, data_type)?.value(row_idx).to_string()),
        DataType::Int8 => Ok(as_array::<Int8Array>(column, data_type)?.value(row_idx).to_string()),
        DataType::Int16 => Ok(as_array::<Int16Array>(column, data_type)?.value(row_idx).to_string()),
        DataType::Int32 => Ok(as_array::<Int32Array>(column, data_type)?.value(row_idx).to_string()),
        DataType::Int64 => Ok(as_array::<Int64Array>(column, data_type)?.value(row_idx).to_string()),
        DataType::UInt8 => Ok(as_array::<UInt8Array>(column, data_type)?.value(row_idx).to_string()),
        DataType::UInt16 => Ok(as_array::<UInt16Array>(column, data_type)?.value(row_idx).to_string()),
        DataType::UInt32 => Ok(as_array::<UInt32Array>(column, data_type)?.value(row_idx).to_string()),
        DataType::UInt64 => Ok(as_array::<UInt64Array>(column, data_type)?.value(row_idx).to_string()),
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
            Ok(format!("DATE {}", quote_string_literal(&date.format("%Y-%m-%d").to_string())))
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
