// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use adbc_core::error::Result;
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray,
    RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, Schema, TimeUnit};
use datafusion::prelude::SessionContext;
use datafusion_table_providers::mongodb::connection_pool::MongoDBConnectionPool;
use mongodb::bson::{Bson, Document};
use tokio::runtime::Runtime;

use crate::error::{adbc_err, invalid_state, io_err, not_implemented};

pub struct MongoDBStatement {
    runtime: Arc<Runtime>,
    ctx: SessionContext,
    pool: Arc<MongoDBConnectionPool>,
    sql_query: Option<String>,
    bound_reader: Option<Box<dyn RecordBatchReader + Send>>,
    bound_schema: Option<Arc<Schema>>,
    target_table: Option<String>,
    ingest_mode: Option<String>,
}

impl MongoDBStatement {
    pub fn new(
        runtime: Arc<Runtime>,
        ctx: SessionContext,
        pool: Arc<MongoDBConnectionPool>,
    ) -> Self {
        Self {
            runtime,
            ctx,
            pool,
            sql_query: None,
            bound_reader: None,
            bound_schema: None,
            target_table: None,
            ingest_mode: None,
        }
    }

    fn execute_ingest(&mut self) -> Result<Option<i64>> {
        let table_name = self
            .target_table
            .as_ref()
            .ok_or_else(|| invalid_state("no target table set"))?
            .clone();

        // bound_schema is kept for execute_schema() but not needed for the write itself
        // since we use each batch's own schema for BSON conversion.
        let _ = self.bound_schema.take();

        let reader = self
            .bound_reader
            .take()
            .ok_or_else(|| invalid_state("no data bound; call bind() or bind_stream() first"))?;

        let pool = Arc::clone(&self.pool);
        let overwrite = self
            .ingest_mode
            .as_deref()
            .map(|m| m.eq_ignore_ascii_case("replace"))
            .unwrap_or(false);

        self.runtime
            .block_on(async {
                let conn = pool
                    .connect()
                    .await
                    .map_err(|e| io_err(format!("failed to connect to MongoDB: {e}")))?;

                let collection = conn
                    .client
                    .database(&conn.db_name)
                    .collection::<Document>(&table_name);

                if overwrite {
                    collection
                        .delete_many(Document::new())
                        .await
                        .map_err(|e| io_err(format!("failed to clear collection '{table_name}': {e}")))?;
                }

                let mut row_count = 0i64;
                for batch_result in reader {
                    let batch = batch_result
                        .map_err(|e| io_err(format!("failed to read batch: {e}")))?;
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let docs = record_batch_to_documents(&batch);
                    row_count += docs.len() as i64;
                    collection
                        .insert_many(docs)
                        .await
                        .map_err(|e| io_err(format!("failed to insert into '{table_name}': {e}")))?;
                }

                Ok::<i64, adbc_core::error::Error>(row_count)
            })
            .map(Some)
    }

    fn execute_sql(&self, query: &str) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
        let ctx = self.ctx.clone();
        self.runtime.block_on(async {
            let df = ctx
                .sql(query)
                .await
                .map_err(|e| io_err(format!("SQL execution failed: {e}")))?;

            let schema = Arc::new(df.schema().as_arrow().clone());
            let batches = df
                .collect()
                .await
                .map_err(|e| io_err(format!("failed to collect results: {e}")))?;

            Ok((schema, batches))
        })
    }
}

// ── Arrow-to-BSON conversion ──────────────────────────────────────────────────

fn record_batch_to_documents(batch: &RecordBatch) -> Vec<Document> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();

    let columns: Vec<Vec<Bson>> = batch
        .columns()
        .iter()
        .enumerate()
        .map(|(col_idx, array)| {
            let field = schema.field(col_idx);
            (0..num_rows)
                .map(|row_idx| arrow_value_to_bson(array.as_ref(), field.data_type(), row_idx))
                .collect()
        })
        .collect();

    (0..num_rows)
        .map(|row_idx| {
            let mut doc = Document::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let bson_val = columns[col_idx][row_idx].clone();
                // Skip null _id so MongoDB auto-generates it
                if field.name() == "_id" && bson_val == Bson::Null {
                    continue;
                }
                doc.insert(field.name().clone(), bson_val);
            }
            doc
        })
        .collect()
}

fn arrow_value_to_bson(array: &dyn Array, data_type: &DataType, row_idx: usize) -> Bson {
    if array.is_null(row_idx) {
        return Bson::Null;
    }
    match data_type {
        DataType::Boolean => Bson::Boolean(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row_idx),
        ),
        DataType::Int8 => Bson::Int32(i32::from(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(row_idx),
        )),
        DataType::Int16 => Bson::Int32(i32::from(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row_idx),
        )),
        DataType::Int32 => Bson::Int32(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row_idx),
        ),
        DataType::Int64 => Bson::Int64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row_idx),
        ),
        DataType::UInt8 => Bson::Int32(i32::from(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(row_idx),
        )),
        DataType::UInt16 => Bson::Int32(i32::from(
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(row_idx),
        )),
        DataType::UInt32 => Bson::Int64(i64::from(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(row_idx),
        )),
        DataType::UInt64 => Bson::Int64(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row_idx) as i64,
        ),
        DataType::Float32 => Bson::Double(f64::from(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row_idx),
        )),
        DataType::Float64 => Bson::Double(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row_idx),
        ),
        DataType::Utf8 => Bson::String(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row_idx)
                .to_string(),
        ),
        DataType::LargeUtf8 => Bson::String(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(row_idx)
                .to_string(),
        ),
        DataType::Binary => Bson::Binary(mongodb::bson::Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(row_idx)
                .to_vec(),
        }),
        DataType::LargeBinary => Bson::Binary(mongodb::bson::Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(row_idx)
                .to_vec(),
        }),
        DataType::Date32 => {
            let millis = i64::from(
                array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .unwrap()
                    .value(row_idx),
            ) * 86_400
                * 1_000;
            Bson::DateTime(mongodb::bson::DateTime::from_millis(millis))
        }
        DataType::Date64 => {
            let millis = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .unwrap()
                .value(row_idx);
            Bson::DateTime(mongodb::bson::DateTime::from_millis(millis))
        }
        DataType::Timestamp(unit, _) => {
            let millis = match unit {
                TimeUnit::Second => {
                    array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap()
                        .value(row_idx)
                        * 1_000
                }
                TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .value(row_idx),
                TimeUnit::Microsecond => {
                    array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .value(row_idx)
                        / 1_000
                }
                TimeUnit::Nanosecond => {
                    array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap()
                        .value(row_idx)
                        / 1_000_000
                }
            };
            Bson::DateTime(mongodb::bson::DateTime::from_millis(millis))
        }
        _ => Bson::Null,
    }
}

// ── VecBatchReader ────────────────────────────────────────────────────────────

struct VecBatchReader {
    schema: Arc<Schema>,
    batches: std::vec::IntoIter<RecordBatch>,
}

impl Iterator for VecBatchReader {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.batches.next().map(Ok)
    }
}

impl RecordBatchReader for VecBatchReader {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}

// ── Optionable ────────────────────────────────────────────────────────────────

impl Optionable for MongoDBStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        let OptionValue::String(val) = value else {
            return Err(adbc_err(
                adbc_core::error::Status::InvalidArguments,
                "expected string option value",
            ));
        };

        match key {
            OptionStatement::TargetTable => {
                self.target_table = Some(val);
                Ok(())
            }
            OptionStatement::IngestMode => {
                self.ingest_mode = Some(val);
                Ok(())
            }
            OptionStatement::TargetCatalog
            | OptionStatement::TargetDbSchema
            | OptionStatement::Temporary => Ok(()),
            _ => Err(not_implemented(&format!(
                "statement option {:?}",
                key.as_ref()
            ))),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionStatement::TargetTable => Ok(self.target_table.clone().unwrap_or_default()),
            OptionStatement::IngestMode => Ok(self.ingest_mode.clone().unwrap_or_default()),
            _ => Err(not_implemented(&format!(
                "statement option {:?}",
                key.as_ref()
            ))),
        }
    }

    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        Err(not_implemented("get_option_bytes"))
    }

    fn get_option_int(&self, _key: Self::Option) -> Result<i64> {
        Err(not_implemented("get_option_int"))
    }

    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        Err(not_implemented("get_option_double"))
    }
}

// ── Statement ─────────────────────────────────────────────────────────────────

impl Statement for MongoDBStatement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        let schema = batch.schema();
        self.bound_schema = Some(Arc::clone(&schema));
        self.bound_reader = Some(Box::new(RecordBatchIterator::new(
            std::iter::once(Ok(batch)),
            schema,
        )));
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        self.bound_schema = Some(reader.schema());
        self.bound_reader = Some(reader);
        Ok(())
    }

    fn execute(&mut self) -> Result<impl RecordBatchReader + Send> {
        let query = self
            .sql_query
            .as_ref()
            .ok_or_else(|| invalid_state("no SQL query set; call set_sql_query first"))?;
        let (schema, batches) = self.execute_sql(query)?;
        Ok(VecBatchReader {
            schema,
            batches: batches.into_iter(),
        })
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        if self.target_table.is_some() && self.bound_schema.is_some() {
            return self.execute_ingest();
        }

        let query = self
            .sql_query
            .as_ref()
            .ok_or_else(|| invalid_state("no SQL query or target table set"))?;
        let (_, batches) = self.execute_sql(query)?;
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        Ok(Some(row_count as i64))
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        if let Some(ref schema) = self.bound_schema {
            return Ok(schema.as_ref().clone());
        }
        let query = self
            .sql_query
            .as_ref()
            .ok_or_else(|| invalid_state("no SQL query set"))?;
        let ctx = self.ctx.clone();
        let query = query.clone();
        self.runtime.block_on(async {
            let df = ctx
                .sql(&query)
                .await
                .map_err(|e| io_err(format!("SQL execution failed: {e}")))?;
            Ok(df.schema().as_arrow().clone())
        })
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        Err(not_implemented("execute_partitions"))
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        Err(not_implemented("get_parameter_schema"))
    }

    fn prepare(&mut self) -> Result<()> {
        Ok(())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.sql_query = Some(query.as_ref().to_string());
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(not_implemented("set_substrait_plan"))
    }

    fn cancel(&mut self) -> Result<()> {
        Ok(())
    }
}
