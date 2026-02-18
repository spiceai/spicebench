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

use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Database, Driver, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::ManagedDriver;

use crate::{AdbcConnection, Error, Result};

/// Configuration for connecting to a Databricks instance.
pub enum DatabricksConnectionConfig<'a> {
    /// Connect using a personal access token.
    ///
    /// - `endpoint`: e.g. `"dbc-a1b2345c-d6e7.cloud.databricks.com"`
    /// - `token`: Databricks personal access token
    /// - `http_path`: SQL warehouse HTTP path, e.g. `"sql/protocolv1/o/123/0123-456789-abcdef01"`
    Token {
        endpoint: &'a str,
        token: &'a str,
        http_path: &'a str,
    },
}

/// Connect to Databricks using a URI.
///
/// # URI formats
///
/// - **OAuth U2M** (browser-based):
///   `databricks://<server-hostname>:<port>/<http-path>?authType=OauthU2M`
///
/// - **OAuth M2M** (client credentials):
///   `databricks://<server-hostname>:<port>/<http-path>?authType=OAuthM2M&clientID=<id>&clientSecret=<secret>`
///
/// - **Personal access token**:
///   `databricks://token:<pat>@<server-hostname>:<port>/<http-path>`
fn connect_from_uri(uri: &str) -> Result<AdbcConnection> {
    let mut driver = ManagedDriver::load_from_name(
        "databricks",
        None,
        AdbcVersion::default(),
        LOAD_FLAG_DEFAULT,
        None,
    )
    .map_err(|e| Error::LoadDriver {
        reason: e.to_string(),
    })?;

    let opts = [(OptionDatabase::Uri, uri.into())];
    let db = driver
        .new_database_with_opts(opts)
        .map_err(|e| Error::CreateDatabase {
            reason: e.to_string(),
        })?;

    let conn = db.new_connection().map_err(|e| Error::CreateConnection {
        reason: e.to_string(),
    })?;

    Ok(AdbcConnection::new(conn))
}

/// Connect to Databricks with the given configuration.
///
/// # Example
///
/// ```rust,no_run
/// use adbc_client::databricks::{connect, DatabricksConnectionConfig};
///
/// let mut conn = connect(DatabricksConnectionConfig::Token {
///     endpoint: "dbc-a1b2345c-d6e7.cloud.databricks.com",
///     token: "dapi0123456789abcdef",
///     http_path: "sql/protocolv1/o/1234567890123456/0123-456789-abcdef01",
/// }).expect("Failed to connect");
///
/// let batches = conn.query("SELECT 1").expect("query failed");
/// ```
pub fn connect(config: DatabricksConnectionConfig<'_>) -> Result<AdbcConnection> {
    match config {
        DatabricksConnectionConfig::Token {
            endpoint,
            token,
            http_path,
        } => {
            let uri = format!("databricks://token:{token}@{endpoint}:443/{http_path}");
            connect_from_uri(&uri)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Requires `DATABRICKS_ENDPOINT`, `DATABRICKS_TOKEN`, and `DATABRICKS_HTTP_PATH` env vars.
    #[test]
    #[ignore = "requires Databricks credentials in env"]
    fn test_tpch_query() {
        let endpoint =
            std::env::var("DATABRICKS_ENDPOINT").expect("DATABRICKS_ENDPOINT must be set");
        let token = std::env::var("DATABRICKS_TOKEN").expect("DATABRICKS_TOKEN must be set");
        let http_path =
            std::env::var("DATABRICKS_HTTP_PATH").expect("DATABRICKS_HTTP_PATH must be set");

        let mut conn = connect(DatabricksConnectionConfig::Token {
            endpoint: &endpoint,
            token: &token,
            http_path: &http_path,
        })
        .expect("Failed to connect to Databricks");

        let batches = conn
            .query(
                "SELECT n_nationkey, n_name, n_regionkey \
                 FROM spiceai_sandbox.tpch.nation \
                 ORDER BY n_nationkey \
                 LIMIT 5",
            )
            .expect("Failed to execute TPC-H query");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(
            total_rows > 0,
            "Expected at least one row from nation table"
        );
        assert!(total_rows <= 5, "Expected at most 5 rows, got {total_rows}");

        // Verify schema has the expected columns
        let schema = batches[0].schema();
        let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            col_names.contains(&"n_nationkey"),
            "Missing n_nationkey column, got: {col_names:?}"
        );
        assert!(
            col_names.contains(&"n_name"),
            "Missing n_name column, got: {col_names:?}"
        );
        assert!(
            col_names.contains(&"n_regionkey"),
            "Missing n_regionkey column, got: {col_names:?}"
        );
    }
}
