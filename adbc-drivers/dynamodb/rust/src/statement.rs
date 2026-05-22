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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use adbc_core::error::Result;
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{Schema, SchemaRef};
use aws_config::SdkConfig;
use data_components::dynamodb::provider::DynamoDBTableProvider;
use datafusion::catalog::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::prelude::SessionContext;
use futures::stream;
use tokio::runtime::Runtime;

use crate::error::{adbc_err, invalid_state, io_err, not_implemented};

pub struct DynamoDBStatement {
    runtime: Arc<Runtime>,
    ctx: SessionContext,
    sdk_config: SdkConfig,
    sql_query: Option<String>,
    bound_reader: Option<Box<dyn RecordBatchReader + Send>>,
    bound_schema: Option<Arc<Schema>>,

    // Ingest options
    target_table: Option<String>,
    ingest_mode: Option<String>,
    parallelism_override: Option<usize>,
    default_parallelism: usize,
    table_parallelism: HashMap<String, usize>,
}

impl DynamoDBStatement {
    pub fn new(
        runtime: Arc<Runtime>,
        ctx: SessionContext,
        sdk_config: SdkConfig,
        default_parallelism: usize,
        table_parallelism: HashMap<String, usize>,
    ) -> Self {
        Self {
            runtime,
            ctx,
            sdk_config,
            sql_query: None,
            bound_reader: None,
            bound_schema: None,
            target_table: None,
            ingest_mode: None,
            parallelism_override: None,
            default_parallelism,
            table_parallelism,
        }
    }

    fn resolve_parallelism(&self, table_name: &str) -> usize {
        if let Some(p) = self.parallelism_override {
            return p;
        }
        if let Some(p) = self.table_parallelism.get(table_name) {
            return *p;
        }
        self.default_parallelism
    }

    fn execute_ingest(&mut self) -> Result<Option<i64>> {
        let table_name = self
            .target_table
            .as_ref()
            .ok_or_else(|| invalid_state("no target table set"))?
            .clone();

        let schema = self
            .bound_schema
            .take()
            .ok_or_else(|| invalid_state("no data bound; call bind() or bind_stream() first"))?;

        let reader = self
            .bound_reader
            .take()
            .ok_or_else(|| invalid_state("no data bound; call bind() or bind_stream() first"))?;

        let sdk_config = self.sdk_config.clone();
        let write_parallelism = self.resolve_parallelism(&table_name);
        let ctx = self.ctx.clone();

        self.runtime
            .block_on(async {
                // Create provider with the bound schema (works for empty tables).
                // Keys are fetched internally via describe_table.
                let provider = DynamoDBTableProvider::try_new_with_schema(
                    sdk_config,
                    Arc::from(table_name.as_str()),
                    Arc::clone(&schema),
                    None,                                        // config_partitions
                    Duration::from_secs(1),                      // scan_interval
                    "2006-01-02T15:04:05.000Z07:00".to_string(), // time_format
                    Duration::ZERO,                              // ready_lag
                    Arc::new(Default::default()),                // metrics_collector
                    write_parallelism,
                )
                .await
                .map_err(|e| io_err(format!("failed to create table provider: {e}")))?;

                // Convert the RecordBatchReader to a SendableRecordBatchStream lazily —
                // no buffering, batches are pulled one at a time as the sink demands them.
                let schema_ref: SchemaRef = Arc::clone(&schema);
                let arrow_stream = stream::iter(
                    reader.map(|r| r.map_err(|e| DataFusionError::ArrowError(Box::new(e), None))),
                );
                let sendable: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&schema_ref),
                    arrow_stream,
                ));

                let input = Arc::new(
                    StreamingTableExec::try_new(
                        Arc::clone(&schema_ref),
                        vec![Arc::new(OneShotStream::new(
                            Arc::clone(&schema_ref),
                            sendable,
                        ))],
                        None,
                        vec![],
                        false,
                        None,
                    )
                    .map_err(|e| io_err(format!("failed to build streaming exec: {e}")))?,
                );

                let state = ctx.state();
                let insert_plan = provider
                    .insert_into(&state, input, InsertOp::Append)
                    .await
                    .map_err(|e| io_err(format!("insert_into failed: {e}")))?;

                let task_ctx = state.task_ctx();
                let result_batches = collect(insert_plan, task_ctx)
                    .await
                    .map_err(|e| io_err(format!("ingest execution failed: {e}")))?;

                // DataSinkExec returns a single batch with a "count" column
                let row_count: i64 = result_batches
                    .iter()
                    .map(|b| {
                        b.column(0)
                            .as_any()
                            .downcast_ref::<arrow_array::UInt64Array>()
                            .map(|a| a.iter().flatten().sum::<u64>() as i64)
                            .unwrap_or(0)
                    })
                    .sum();

                Ok::<i64, adbc_core::error::Error>(row_count)
            })
            .map(Some)
    }

    /// SQL query execution via DataFusion.
    fn execute_sql(&self, query: &str) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
        let ctx = self.ctx.clone();
        let query_preview: String = query.chars().take(400).collect();
        self.runtime.block_on(async {
            let df = ctx.sql(query).await.map_err(|e| {
                eprintln!("SQL execution failed: {e}\nQuery: {query_preview}");
                io_err(format!("SQL execution failed: {e}"))
            })?;

            let schema = Arc::new(df.schema().as_arrow().clone());

            let batches = df.collect().await.map_err(|e| {
                eprintln!("Failed to collect results: {e}\nQuery: {query_preview}");
                io_err(format!("failed to collect results: {e}"))
            })?;

            Ok((schema, batches))
        })
    }
}

// ── OneShotStream ─────────────────────────────────────────────────────────────
//
// A one-shot PartitionStream that yields a pre-built SendableRecordBatchStream.
// Batches are pulled lazily from the underlying RecordBatchReader — nothing is
// buffered in memory before the DataSink requests it.

struct OneShotStream {
    schema: SchemaRef,
    stream: Mutex<Option<SendableRecordBatchStream>>,
}

impl OneShotStream {
    fn new(schema: SchemaRef, stream: SendableRecordBatchStream) -> Self {
        Self {
            schema,
            stream: Mutex::new(Some(stream)),
        }
    }
}

impl std::fmt::Debug for OneShotStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OneShotStream")
    }
}

impl PartitionStream for OneShotStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        self.stream
            .lock()
            .expect("OneShotStream lock poisoned")
            .take()
            .expect("OneShotStream already consumed")
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

impl Optionable for DynamoDBStatement {
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
            OptionStatement::Other(ref key_str) => match key_str.as_str() {
                "adbc.driver.dynamodb.parallelism" => {
                    self.parallelism_override = Some(val.parse::<usize>().map_err(|_| {
                        adbc_err(
                            adbc_core::error::Status::InvalidArguments,
                            format!("invalid parallelism value: {val}"),
                        )
                    })?);
                    Ok(())
                }
                _ => Err(not_implemented(&format!("statement option {key_str}"))),
            },
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

impl Statement for DynamoDBStatement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        let schema = batch.schema();
        self.bound_schema = Some(Arc::clone(&schema));
        // Wrap the single batch in a reader so all paths go through ReaderExec
        self.bound_reader = Some(Box::new(RecordBatchIterator::new(
            std::iter::once(Ok(batch)),
            schema,
        )));
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        // Store the reader directly — batches are pulled lazily during execute_ingest
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
