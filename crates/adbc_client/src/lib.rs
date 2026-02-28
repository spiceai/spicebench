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

mod pool;
pub use pool::{AdbcConnectionManager, AdbcConnectionPool, create_pool};

pub use adbc_core::options::IngestMode;

use adbc_core::options::{self, AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, LOAD_FLAG_DEFAULT, Optionable, Statement};
use adbc_driver_manager::ManagedDriver;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Schema};
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::Field;
use snafu::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to load ADBC driver: {reason}"))]
    LoadDriver { reason: String },

    #[snafu(display("Failed to create database handle: {reason}"))]
    CreateDatabase { reason: String },

    #[snafu(display("Failed to create connection: {reason}"))]
    CreateConnection { reason: String },

    #[snafu(display("Failed to execute query: {reason}"))]
    ExecuteQuery { reason: String },

    #[snafu(display("Failed to read result batch: {source}"))]
    ReadBatch { source: arrow::error::ArrowError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A generic ADBC connection wrapping [`adbc_driver_manager::ManagedConnection`].
///
/// Use a connector-specific builder (e.g. [`databricks::connect`]) to obtain an instance.
pub struct AdbcConnection {
    conn: adbc_driver_manager::ManagedConnection,
    downcast_utf8view: bool,
    resolve_opaque_numerics: bool,
}

impl AdbcConnection {
    /// Create an `AdbcConnection` from an already-established [`ManagedConnection`].
    #[must_use]
    pub fn new(conn: adbc_driver_manager::ManagedConnection, downcast_utf8view: bool, resolve_opaque_numerics: bool) -> Self {
        Self {
            conn,
            downcast_utf8view,
            resolve_opaque_numerics,
        }
    }

    /// Create an `AdbcConnection` from a driver name and a map of key-value options.
    ///
    /// Each key in `kwargs` is converted to an [`OptionDatabase`] variant
    /// (matching canonical keys like `"uri"`, `"username"`, `"password"`,
    /// or falling back to [`OptionDatabase::Other`]).
    pub fn create(driver_name: &str, kwargs: HashMap<String, serde_json::Value>) -> Result<Self> {
        let mut driver = ManagedDriver::load_from_name(
            driver_name,
            None,
            AdbcVersion::default(),
            LOAD_FLAG_DEFAULT,
            None,
        )
        .map_err(|e| Error::LoadDriver {
            reason: e.to_string(),
        })?;

        let opts: Vec<(OptionDatabase, OptionValue)> = kwargs
            .into_iter()
            .map(|(k, v)| {
                let val = match v {
                    serde_json::Value::String(s) => s,
                    other => other.to_string(),
                };
                (OptionDatabase::from(k.as_str()), OptionValue::from(val))
            })
            .collect();

        let db = driver
            .new_database_with_opts(opts)
            .map_err(|e| Error::CreateDatabase {
                reason: e.to_string(),
            })?;

        let conn = db.new_connection().map_err(|e| Error::CreateConnection {
            reason: e.to_string(),
        })?;

        Ok(Self::new(
            conn,
            driver_name == "databricks",
            driver_name == "postgresql",
        ))
    }

    /// Lightweight check that the connection is still usable.
    ///
    /// Used by the r2d2 pool to validate connections before handing them out.
    pub fn check_valid(&mut self) -> Result<()> {
        self.conn
            .new_statement()
            .map(|_| ())
            .map_err(|e| Error::ExecuteQuery {
                reason: e.to_string(),
            })
    }

    /// Execute a SQL query and collect all result batches.
    pub fn query(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        let mut stmt = self.conn.new_statement().map_err(|e| Error::ExecuteQuery {
            reason: e.to_string(),
        })?;

        stmt.set_sql_query(sql).map_err(|e| Error::ExecuteQuery {
            reason: e.to_string(),
        })?;

        let reader = stmt.execute().map_err(|e| Error::ExecuteQuery {
            reason: e.to_string(),
        })?;

        let mut batches = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .context(ReadBatchSnafu)?;

        if self.resolve_opaque_numerics {
            batches = resolve_opaque_numerics(batches);
        }

        Ok(batches)
    }

    /// Execute a SQL data-modification statement and return the affected row count when provided by the driver.
    pub fn execute_update(&mut self, sql: &str) -> Result<Option<i64>> {
        let mut stmt = self.conn.new_statement().map_err(|e| Error::ExecuteQuery {
            reason: e.to_string(),
        })?;

        stmt.set_sql_query(sql).map_err(|e| Error::ExecuteQuery {
            reason: e.to_string(),
        })?;

        stmt.execute_update().map_err(|e| Error::ExecuteQuery {
            reason: e.to_string(),
        })
    }

    /// Bulk-ingest a [`RecordBatch`] into a target table using the ADBC bulk
    /// ingest API.
    ///
    /// This binds the batch directly to a statement configured with the
    /// target table and ingest mode, avoiding the overhead of constructing
    /// individual SQL INSERT statements.
    pub fn bulk_ingest(
        &mut self,
        target_table: &str,
        target_db_catalog: Option<&str>,
        target_db_schema: Option<&str>,
        mode: options::IngestMode,
        batch: RecordBatch,
    ) -> Result<Option<i64>> {
        let batch = if self.downcast_utf8view {
            downcast_utf8view(&batch)
        } else {
            batch
        };

        self.bulk_ingest_stream(
            target_table,
            target_db_catalog,
            target_db_schema,
            mode,
            Box::new(arrow_array::RecordBatchIterator::new(
                std::iter::once(Ok(batch.clone())),
                batch.schema(),
            )),
        )
    }

    /// Bulk-ingest a stream of [`RecordBatch`]es into a target table using a
    /// single ADBC statement with `bind_stream`.
    ///
    /// This is more efficient than calling [`bulk_ingest`](Self::bulk_ingest)
    /// per batch because it reuses the same statement and network connection.
    pub fn bulk_ingest_stream(
        &mut self,
        target_table: &str,
        target_db_catalog: Option<&str>,
        target_db_schema: Option<&str>,
        mode: options::IngestMode,
        reader: Box<dyn arrow_array::RecordBatchReader + Send>,
    ) -> Result<Option<i64>> {
        let mut stmt = self.conn.new_statement().map_err(|e| Error::ExecuteQuery {
            reason: e.to_string(),
        })?;

        stmt.set_option(
            options::OptionStatement::TargetTable,
            OptionValue::from(target_table),
        )
        .map_err(|e| Error::ExecuteQuery {
            reason: format!("Failed to set target table: {e}"),
        })?;

        if let Some(catalog) = target_db_catalog {
            stmt.set_option(
                options::OptionStatement::TargetCatalog,
                OptionValue::from(catalog),
            )
            .map_err(|e| Error::ExecuteQuery {
                reason: format!("Failed to set target db catalog: {e}"),
            })?;
        }

        if let Some(schema) = target_db_schema {
            stmt.set_option(
                options::OptionStatement::TargetDbSchema,
                OptionValue::from(schema),
            )
            .map_err(|e| Error::ExecuteQuery {
                reason: format!("Failed to set target db schema: {e}"),
            })?;
        }

        stmt.set_option(options::OptionStatement::IngestMode, mode.into())
            .map_err(|e| Error::ExecuteQuery {
                reason: format!("Failed to set ingest mode: {e}"),
            })?;

        stmt.bind_stream(reader).map_err(|e| Error::ExecuteQuery {
            reason: format!("Failed to bind stream for bulk ingest: {e}"),
        })?;

        stmt.execute_update().map_err(|e| Error::ExecuteQuery {
            reason: format!(
                "Bulk ingest execution failed for target_table='{target_table}', target_db_catalog={target_db_catalog:?}, target_db_schema={target_db_schema:?}: {e}"
            ),
        })
    }
}

/// Cast Utf8View columns to Utf8 since the Databricks ADBC driver
/// does not support STRING_VIEW.
fn downcast_utf8view(batch: &RecordBatch) -> RecordBatch {
    let schema = batch.schema();
    let mut fields = Vec::with_capacity(schema.fields().len());
    let mut columns = Vec::with_capacity(schema.fields().len());

    for (i, field) in schema.fields().iter().enumerate() {
        match field.data_type() {
            DataType::Utf8View => {
                fields.push(Arc::new(Field::new(
                    field.name(),
                    DataType::Utf8,
                    field.is_nullable(),
                )));
                columns.push(cast(batch.column(i), &DataType::Utf8).unwrap());
            }
            _ => {
                fields.push(field.clone());
                columns.push(batch.column(i).clone());
            }
        }
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

/// Returns `true` if the field uses the Arrow opaque extension type for
/// PostgreSQL `numeric`.
fn is_opaque_numeric(field: &Field) -> bool {
    if !matches!(field.data_type(), DataType::Utf8) {
        return false;
    }
    let metadata = field.metadata();
    let Some(ext_name) = metadata.get("ARROW:extension:name") else {
        return false;
    };
    if ext_name != "arrow.opaque" {
        return false;
    }
    let Some(ext_meta) = metadata.get("ARROW:extension:metadata") else {
        return false;
    };
    serde_json::from_str::<serde_json::Value>(ext_meta)
        .ok()
        .and_then(|v| v.get("type_name")?.as_str().map(|s| s == "numeric"))
        .unwrap_or(false)
}

/// Determine the maximum decimal scale (digits after the decimal point)
/// across all non-null values in a string array.
fn max_decimal_scale(array: &StringArray) -> i8 {
    let mut scale: i8 = 0;
    for i in 0..array.len() {
        if array.is_null(i) {
            continue;
        }
        let val = array.value(i);
        if let Some(dot_pos) = val.find('.') {
            let s = (val.len() - dot_pos - 1) as i8;
            scale = scale.max(s);
        }
    }
    scale
}

/// Convert columns returned by the PostgreSQL ADBC driver as
/// `Utf8` with `arrow.opaque` extension metadata for `numeric` to
/// `Decimal128`, matching the representation used in checkpoint
/// parquet files.
///
/// The scale for each column is inferred from the actual data across
/// all batches. If casting fails (e.g. the column contains `NaN` or
/// `inf`), the original `Utf8` column is kept.
fn resolve_opaque_numerics(batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    if batches.is_empty() {
        return batches;
    }

    let schema = batches[0].schema();
    let opaque_cols: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, f)| if is_opaque_numeric(f) { Some(i) } else { None })
        .collect();

    if opaque_cols.is_empty() {
        return batches;
    }

    // Determine the max scale for each opaque numeric column across
    // all batches so every batch uses a consistent Decimal128 type.
    let mut scales: Vec<i8> = vec![0; opaque_cols.len()];
    for batch in &batches {
        for (j, &col_idx) in opaque_cols.iter().enumerate() {
            if let Some(arr) = batch.column(col_idx).as_any().downcast_ref::<StringArray>() {
                scales[j] = scales[j].max(max_decimal_scale(arr));
            }
        }
    }

    batches
        .into_iter()
        .map(|batch| {
            let schema = batch.schema();
            let mut fields = Vec::with_capacity(schema.fields().len());
            let mut columns = Vec::with_capacity(schema.fields().len());

            for (i, field) in schema.fields().iter().enumerate() {
                if let Some(j) = opaque_cols.iter().position(|&idx| idx == i) {
                    let target_type = DataType::Decimal128(38, scales[j]);
                    match cast(batch.column(i), &target_type) {
                        Ok(converted) => {
                            fields.push(Arc::new(Field::new(
                                field.name(),
                                target_type,
                                field.is_nullable(),
                            )));
                            columns.push(converted);
                            continue;
                        }
                        Err(_) => { /* fall through to keep original */ }
                    }
                }
                fields.push(field.clone());
                columns.push(batch.column(i).clone());
            }

            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
        })
        .collect()
}