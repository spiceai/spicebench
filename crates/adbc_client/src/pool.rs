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

//! r2d2-based ADBC connection pool.

use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Database, Driver, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::{ManagedDatabase, ManagedDriver};

use crate::{AdbcConnection, Error};

/// Connection pool backed by `r2d2`.
///
/// Each call to `pool.get()` checks out an [`AdbcConnection`] that is
/// automatically returned when the [`r2d2::PooledConnection`] is dropped.
pub type AdbcConnectionPool = r2d2::Pool<AdbcConnectionManager>;

/// Default pool size when none is specified.
const DEFAULT_POOL_SIZE: u32 = 10;

/// Manages ADBC connections for the `r2d2` connection pool.
///
/// Wraps a [`ManagedDatabase`] behind an `Arc` so that `r2d2` can
/// create new connections on demand from any thread.
pub struct AdbcConnectionManager {
    database: Arc<ManagedDatabase>,
    downcast_utf8view: bool,
}

impl AdbcConnectionManager {
    /// Create a new manager from an existing [`ManagedDatabase`].
    pub fn new(database: ManagedDatabase, downcast_utf8view: bool) -> Self {
        Self {
            database: Arc::new(database),
            downcast_utf8view,
        }
    }
}

impl r2d2::ManageConnection for AdbcConnectionManager {
    type Connection = AdbcConnection;
    type Error = Error;

    fn connect(&self) -> std::result::Result<Self::Connection, Self::Error> {
        let conn = self
            .database
            .new_connection()
            .map_err(|e| Error::CreateConnection {
                reason: e.to_string(),
            })?;
        Ok(AdbcConnection::new(conn, self.downcast_utf8view))
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> std::result::Result<(), Self::Error> {
        conn.check_valid()
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

/// Create a connection pool of ADBC connections.
///
/// * `driver_name` – ADBC driver library name (e.g. `"databricks"`).
/// * `kwargs`      – Driver-specific key-value options (uri, credentials, …).
/// * `size`        – Maximum number of connections. Defaults to [`DEFAULT_POOL_SIZE`] when `None`.
///
/// All connections are eagerly created during pool construction so failures
/// surface immediately rather than at first query time.
pub fn create_pool(
    driver_name: &str,
    kwargs: HashMap<String, serde_json::Value>,
    size: Option<u32>,
) -> crate::Result<AdbcConnectionPool> {
    let pool_size = size.unwrap_or(DEFAULT_POOL_SIZE);

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

    let manager = AdbcConnectionManager::new(db, driver_name == "databricks");

    r2d2::Pool::builder()
        .max_size(pool_size)
        .min_idle(Some(pool_size))
        .build(manager)
        .map_err(|e| Error::CreateConnection {
            reason: format!("Failed to build connection pool: {e}"),
        })
}
