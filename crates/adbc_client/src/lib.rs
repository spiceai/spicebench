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

pub mod databricks;

use std::collections::HashMap;

use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Statement, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::ManagedDriver;
use arrow_array::RecordBatch;
use snafu::prelude::*;

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
}

impl AdbcConnection {
    /// Create an `AdbcConnection` from an already-established [`ManagedConnection`].
    #[must_use]
    pub fn new(conn: adbc_driver_manager::ManagedConnection) -> Self {
        Self { conn }
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

        Ok(Self::new(conn))
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

        reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .context(ReadBatchSnafu)
    }
}
