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

//! Native DynamoDB sink using the AWS SDK directly.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use async_trait::async_trait;
use aws_sdk_dynamodb::types::AttributeValue;
use tokio::sync::Semaphore;

use super::{InsertOp, Sink};

const BATCH_WRITE_MAX_ITEMS: usize = 25;
const DEFAULT_PARALLELISM: usize = 10;

pub struct DynamoDbSink {
    client: aws_sdk_dynamodb::Client,
    /// Prefix for physical DynamoDB table names (e.g. "sb_1a2b3c").
    /// Physical name = "{catalog_namespace}.{logical_name}".
    catalog_namespace: Option<String>,
    /// Running row counts per table, updated after each write.
    row_counts: Arc<Mutex<HashMap<String, u64>>>,
}

/// Per-table write parallelism (max concurrent `BatchWriteItem` calls).
/// Matches the values previously configured on the DynamoDB ADBC driver.
fn write_parallelism(table_name: &str) -> usize {
    match table_name {
        "lineitem" => 30,
        "orders" => 20,
        _ => DEFAULT_PARALLELISM,
    }
}

impl DynamoDbSink {
    pub async fn new(
        region: String,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        session_token: Option<String>,
        catalog_namespace: Option<String>,
    ) -> Self {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_dynamodb::config::Region::new(region));

        if let (Some(key_id), Some(secret)) = (access_key_id, secret_access_key) {
            let credentials = aws_sdk_dynamodb::config::Credentials::new(
                key_id,
                secret,
                session_token,
                None,
                "spicebench",
            );
            loader = loader.credentials_provider(credentials);
        }

        let sdk_config = loader.load().await;
        let client = aws_sdk_dynamodb::Client::new(&sdk_config);
        Self {
            client,
            catalog_namespace,
            row_counts: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn update_row_count(&self, table_name: &str, op_label: &str, rows: u64) -> u64 {
        let mut counts = self.row_counts.lock().expect("row_counts lock poisoned");
        let count = counts.entry(table_name.to_string()).or_insert(0);
        match op_label {
            "insert" => *count = count.saturating_add(rows),
            "delete" => *count = count.saturating_sub(rows),
            _ => {} // update: row count unchanged (PutItem replaces in-place)
        }
        *count
    }

    fn physical_name(&self, logical: &str) -> String {
        match &self.catalog_namespace {
            Some(ns) => format!("{ns}.{logical}"),
            None => logical.to_string(),
        }
    }

    /// Dispatch pre-built chunks concurrently, bounded by `parallelism`.
    async fn dispatch_chunks(
        &self,
        physical: String,
        chunks: Vec<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
        parallelism: usize,
        op_label: &'static str,
    ) -> anyhow::Result<()> {
        let sem = Arc::new(Semaphore::new(parallelism));
        let mut join_set: tokio::task::JoinSet<anyhow::Result<()>> =
            tokio::task::JoinSet::new();

        for chunk in chunks {
            let client = self.client.clone();
            let physical = physical.clone();
            let permit = Arc::clone(&sem)
                .acquire_owned()
                .await
                .expect("semaphore closed");
            join_set.spawn(async move {
                let _permit = permit;
                client
                    .batch_write_item()
                    .request_items(physical, chunk)
                    .send()
                    .await
                    .map_err(|e| anyhow::anyhow!("DynamoDB BatchWriteItem ({op_label}) failed: {e}"))?;
                Ok(())
            });
        }

        while let Some(result) = join_set.join_next().await {
            result.map_err(|e| anyhow::anyhow!("DynamoDB task panicked: {e}"))??;
        }
        Ok(())
    }
}

#[async_trait]
impl Sink for DynamoDbSink {
    async fn write(
        &self,
        table_name: &str,
        _batch_id: u64,
        batch: RecordBatch,
        op: InsertOp,
        _partition_columns: Vec<String>,
    ) -> anyhow::Result<()> {
        let write_start = Instant::now();
        let rows = batch.num_rows();

        if rows == 0 {
            tracing::debug!(
                table = %table_name,
                op = "empty",
                elapsed_ms = write_start.elapsed().as_millis(),
                "Sink::write completed"
            );
            return Ok(());
        }

        let op_label = match &op {
            InsertOp::Insert => "insert",
            InsertOp::Update { .. } => "update",
            InsertOp::Delete { .. } => "delete",
        };

        match op {
            InsertOp::Insert | InsertOp::Update { .. } => {
                // DynamoDB PutItem always upserts by partition + sort key.
                self.upsert(table_name, batch).await?;
            }
            InsertOp::Delete { key_columns } => {
                self.delete_rows(table_name, batch, &key_columns).await?;
            }
        }

        let elapsed = write_start.elapsed();
        let rows_per_sec = if elapsed.as_secs_f64() > 0.0 {
            rows as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };
        let rows_total = self.update_row_count(table_name, op_label, rows as u64);

        tracing::debug!(
            table = %table_name,
            op = op_label,
            rows,
            rows_total,
            elapsed_ms = elapsed.as_millis(),
            rows_per_sec = format!("{rows_per_sec:.1}"),
            "Sink::write completed"
        );

        Ok(())
    }
}

impl DynamoDbSink {
    async fn upsert(&self, table_name: &str, batch: RecordBatch) -> anyhow::Result<()> {
        use aws_sdk_dynamodb::types::{PutRequest, WriteRequest};

        let physical = self.physical_name(table_name);
        let schema = batch.schema();

        let mut chunks: Vec<Vec<WriteRequest>> = Vec::new();
        for chunk_start in (0..batch.num_rows()).step_by(BATCH_WRITE_MAX_ITEMS) {
            let chunk_end = (chunk_start + BATCH_WRITE_MAX_ITEMS).min(batch.num_rows());
            let mut requests = Vec::with_capacity(chunk_end - chunk_start);
            for row in chunk_start..chunk_end {
                let mut item: HashMap<String, AttributeValue> = HashMap::new();
                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let col = batch.column(col_idx);
                    if !col.is_null(row) {
                        item.insert(
                            field.name().clone(),
                            arrow_col_to_attribute_value(col.as_ref(), row),
                        );
                    }
                }
                requests.push(
                    WriteRequest::builder()
                        .put_request(PutRequest::builder().set_item(Some(item)).build()?)
                        .build(),
                );
            }
            chunks.push(requests);
        }

        self.dispatch_chunks(physical, chunks, write_parallelism(table_name), "upsert")
            .await
    }

    async fn delete_rows(
        &self,
        table_name: &str,
        batch: RecordBatch,
        pk_columns: &[String],
    ) -> anyhow::Result<()> {
        use aws_sdk_dynamodb::types::{DeleteRequest, WriteRequest};

        let physical = self.physical_name(table_name);
        let schema = batch.schema();

        // Resolve PK column indices once — same for every row.
        let pk_col_indices: Vec<usize> = pk_columns
            .iter()
            .map(|pk| {
                schema
                    .index_of(pk)
                    .map_err(|_| anyhow::anyhow!("PK column '{pk}' not in batch schema"))
            })
            .collect::<anyhow::Result<_>>()?;

        let mut chunks: Vec<Vec<WriteRequest>> = Vec::new();
        for chunk_start in (0..batch.num_rows()).step_by(BATCH_WRITE_MAX_ITEMS) {
            let chunk_end = (chunk_start + BATCH_WRITE_MAX_ITEMS).min(batch.num_rows());
            let mut requests = Vec::with_capacity(chunk_end - chunk_start);
            for row in chunk_start..chunk_end {
                let mut key: HashMap<String, AttributeValue> = HashMap::new();
                for (pk_col, &col_idx) in pk_columns.iter().zip(&pk_col_indices) {
                    let col = batch.column(col_idx);
                    if !col.is_null(row) {
                        key.insert(pk_col.clone(), arrow_col_to_attribute_value(col.as_ref(), row));
                    }
                }
                requests.push(
                    WriteRequest::builder()
                        .delete_request(DeleteRequest::builder().set_key(Some(key)).build()?)
                        .build(),
                );
            }
            chunks.push(requests);
        }

        self.dispatch_chunks(physical, chunks, write_parallelism(table_name), "delete")
            .await
    }
}

fn arrow_col_to_attribute_value(col: &dyn Array, row: usize) -> AttributeValue {
    match col.data_type() {
        DataType::Boolean => AttributeValue::Bool(col.as_boolean().value(row)),
        DataType::Int8 => AttributeValue::N(
            col.as_primitive::<arrow::datatypes::Int8Type>()
                .value(row)
                .to_string(),
        ),
        DataType::Int16 => AttributeValue::N(
            col.as_primitive::<arrow::datatypes::Int16Type>()
                .value(row)
                .to_string(),
        ),
        DataType::Int32 => AttributeValue::N(
            col.as_primitive::<arrow::datatypes::Int32Type>()
                .value(row)
                .to_string(),
        ),
        DataType::Int64 => {
            AttributeValue::N(col.as_primitive::<Int64Type>().value(row).to_string())
        }
        DataType::UInt8 => AttributeValue::N(
            col.as_primitive::<arrow::datatypes::UInt8Type>()
                .value(row)
                .to_string(),
        ),
        DataType::UInt16 => AttributeValue::N(
            col.as_primitive::<arrow::datatypes::UInt16Type>()
                .value(row)
                .to_string(),
        ),
        DataType::UInt32 => AttributeValue::N(
            col.as_primitive::<arrow::datatypes::UInt32Type>()
                .value(row)
                .to_string(),
        ),
        DataType::UInt64 => AttributeValue::N(
            col.as_primitive::<arrow::datatypes::UInt64Type>()
                .value(row)
                .to_string(),
        ),
        DataType::Float32 => AttributeValue::N(
            col.as_primitive::<arrow::datatypes::Float32Type>()
                .value(row)
                .to_string(),
        ),
        DataType::Float64 => {
            AttributeValue::N(col.as_primitive::<Float64Type>().value(row).to_string())
        }
        DataType::Decimal128(_, scale) => {
            let raw = col
                .as_primitive::<arrow::datatypes::Decimal128Type>()
                .value(row);
            #[allow(clippy::cast_sign_loss)]
            let scale = *scale as u32;
            let divisor = 10i128.pow(scale);
            // Stored as N; column is declared Float64 so the value is read back as f64.
            let float_val = raw as f64 / divisor as f64;
            AttributeValue::N(float_val.to_string())
        }
        DataType::Timestamp(_, _) => {
            // Stored as ISO 8601 string; column is declared Utf8 in the spicepod.
            AttributeValue::S(
                arrow::util::display::array_value_to_string(col, row).unwrap_or_default(),
            )
        }
        DataType::Utf8 => AttributeValue::S(col.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => AttributeValue::S(col.as_string::<i64>().value(row).to_string()),
        DataType::Binary => AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(
            col.as_binary::<i32>().value(row).to_vec(),
        )),
        DataType::LargeBinary => AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(
            col.as_binary::<i64>().value(row).to_vec(),
        )),
        _ => AttributeValue::S(
            arrow::util::display::array_value_to_string(col, row).unwrap_or_default(),
        ),
    }
}
