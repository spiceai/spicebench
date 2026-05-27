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

//! Native MongoDB sink using the mongodb crate directly.

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Float64Type, Int64Type, TimeUnit};
use async_trait::async_trait;
use mongodb::bson::{Bson, Document};
use mongodb::options::ReplaceOptions;

use super::{InsertOp, Sink};

pub struct MongoDbSink {
    db: mongodb::Database,
}

impl MongoDbSink {
    pub async fn new(uri: &str) -> anyhow::Result<Self> {
        let options = mongodb::options::ClientOptions::parse(uri)
            .await
            .map_err(|e| anyhow::anyhow!("MongoDB URI parse error: {e}"))?;
        let db_name = options
            .default_database
            .clone()
            .unwrap_or_else(|| "spicebench".to_string());
        let client = mongodb::Client::with_options(options)
            .map_err(|e| anyhow::anyhow!("MongoDB client creation error: {e}"))?;
        Ok(Self {
            db: client.database(&db_name),
        })
    }
}

#[async_trait]
impl Sink for MongoDbSink {
    async fn write(
        &self,
        table_name: &str,
        _batch_id: u64,
        batch: RecordBatch,
        op: InsertOp,
        _partition_columns: Vec<String>,
    ) -> anyhow::Result<()> {
        match op {
            InsertOp::Insert => {
                let collection = self.db.collection::<Document>(table_name);
                let docs = batch_to_docs(&batch)?;
                if !docs.is_empty() {
                    collection.insert_many(docs).await.map_err(|e| {
                        anyhow::anyhow!("MongoDB insert_many failed for '{table_name}': {e}")
                    })?;
                }
            }
            InsertOp::Update { key_columns } => {
                let collection = self.db.collection::<Document>(table_name);
                let schema = batch.schema();

                for row in 0..batch.num_rows() {
                    let mut filter = Document::new();
                    for pk_col in &key_columns {
                        let col_idx = schema
                            .index_of(pk_col)
                            .map_err(|_| anyhow::anyhow!("PK column '{pk_col}' not in schema"))?;
                        let col = batch.column(col_idx);
                        filter.insert(pk_col.clone(), arrow_col_to_bson(col.as_ref(), row));
                    }

                    let mut replacement = Document::new();
                    for (col_idx, field) in schema.fields().iter().enumerate() {
                        let col = batch.column(col_idx);
                        if !col.is_null(row) {
                            replacement
                                .insert(field.name().clone(), arrow_col_to_bson(col.as_ref(), row));
                        }
                    }

                    collection
                        .replace_one(filter, replacement)
                        .with_options(ReplaceOptions::builder().upsert(true).build())
                        .await
                        .map_err(|e| {
                            anyhow::anyhow!(
                                "MongoDB replace_one (upsert) failed for '{table_name}': {e}"
                            )
                        })?;
                }
            }
            InsertOp::Delete { key_columns } => {
                let collection = self.db.collection::<Document>(table_name);
                let schema = batch.schema();

                for row in 0..batch.num_rows() {
                    let mut filter = Document::new();
                    for pk_col in &key_columns {
                        let col_idx = schema
                            .index_of(pk_col)
                            .map_err(|_| anyhow::anyhow!("PK column '{pk_col}' not in schema"))?;
                        let col = batch.column(col_idx);
                        filter.insert(pk_col.clone(), arrow_col_to_bson(col.as_ref(), row));
                    }

                    collection.delete_one(filter).await.map_err(|e| {
                        anyhow::anyhow!("MongoDB delete_one failed for '{table_name}': {e}")
                    })?;
                }
            }
        }
        Ok(())
    }
}

fn batch_to_docs(batch: &RecordBatch) -> anyhow::Result<Vec<Document>> {
    let schema = batch.schema();
    let mut docs = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut doc = Document::new();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            if !col.is_null(row) {
                doc.insert(field.name().clone(), arrow_col_to_bson(col.as_ref(), row));
            }
        }
        docs.push(doc);
    }
    Ok(docs)
}

fn arrow_col_to_bson(col: &dyn Array, row: usize) -> Bson {
    match col.data_type() {
        DataType::Boolean => Bson::Boolean(col.as_boolean().value(row)),
        DataType::Int8 => Bson::Int32(
            col.as_primitive::<arrow::datatypes::Int8Type>()
                .value(row)
                .into(),
        ),
        DataType::Int16 => Bson::Int32(
            col.as_primitive::<arrow::datatypes::Int16Type>()
                .value(row)
                .into(),
        ),
        DataType::Int32 => {
            Bson::Int32(col.as_primitive::<arrow::datatypes::Int32Type>().value(row))
        }
        DataType::Int64 => Bson::Int64(col.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => Bson::Int32(
            col.as_primitive::<arrow::datatypes::UInt8Type>()
                .value(row)
                .into(),
        ),
        DataType::UInt16 => Bson::Int32(
            col.as_primitive::<arrow::datatypes::UInt16Type>()
                .value(row)
                .into(),
        ),
        DataType::UInt32 => Bson::Int64(
            col.as_primitive::<arrow::datatypes::UInt32Type>()
                .value(row)
                .into(),
        ),
        DataType::UInt64 => Bson::Int64(
            col.as_primitive::<arrow::datatypes::UInt64Type>()
                .value(row)
                .cast_signed(),
        ),
        DataType::Float32 => Bson::Double(
            col.as_primitive::<arrow::datatypes::Float32Type>()
                .value(row)
                .into(),
        ),
        DataType::Float64 => Bson::Double(col.as_primitive::<Float64Type>().value(row)),
        DataType::Decimal128(_, scale) => {
            let raw = col
                .as_primitive::<arrow::datatypes::Decimal128Type>()
                .value(row);
            #[allow(clippy::cast_sign_loss)]
            let scale = *scale as u32;
            let divisor = 10i128.pow(scale);
            #[allow(clippy::cast_precision_loss)]
            let f = (raw as f64) / (divisor as f64);
            Bson::Double(f)
        }
        DataType::Utf8 => Bson::String(col.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => Bson::String(col.as_string::<i64>().value(row).to_string()),
        DataType::Binary => Bson::Binary(mongodb::bson::Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: col.as_binary::<i32>().value(row).to_vec(),
        }),
        DataType::LargeBinary => Bson::Binary(mongodb::bson::Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: col.as_binary::<i64>().value(row).to_vec(),
        }),
        DataType::Date32 => {
            let days = i64::from(
                col.as_primitive::<arrow::datatypes::Date32Type>()
                    .value(row),
            );
            Bson::DateTime(mongodb::bson::DateTime::from_millis(days * 86_400 * 1_000))
        }
        DataType::Date64 => {
            let millis = col
                .as_primitive::<arrow::datatypes::Date64Type>()
                .value(row);
            Bson::DateTime(mongodb::bson::DateTime::from_millis(millis))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let secs = col
                .as_primitive::<arrow::datatypes::TimestampSecondType>()
                .value(row);
            Bson::DateTime(mongodb::bson::DateTime::from_millis(secs * 1_000))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let millis = col
                .as_primitive::<arrow::datatypes::TimestampMillisecondType>()
                .value(row);
            Bson::DateTime(mongodb::bson::DateTime::from_millis(millis))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let micros = col
                .as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
                .value(row);
            Bson::DateTime(mongodb::bson::DateTime::from_millis(micros / 1_000))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let nanos = col
                .as_primitive::<arrow::datatypes::TimestampNanosecondType>()
                .value(row);
            Bson::DateTime(mongodb::bson::DateTime::from_millis(nanos / 1_000_000))
        }
        _ => {
            Bson::String(arrow::util::display::array_value_to_string(col, row).unwrap_or_default())
        }
    }
}
