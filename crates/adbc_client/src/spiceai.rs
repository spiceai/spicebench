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

use std::collections::HashMap;

use crate::{AdbcConnection, Result};

/// Connect to a Spice.ai Flight SQL endpoint.
///
/// Uses [`AdbcConnection::create`] with the `"flightsql"` ADBC driver,
/// passing `uri`, `username`, and `password` options.
///
/// # Arguments
///
/// - `flight_url`: The Flight SQL endpoint URI (e.g. `"grpc://localhost:50051"`).
/// - `api_key`: Optional API key (passed as the `password` option; `username` is left empty).
///
/// # Example
///
/// ```rust,no_run
/// use adbc_client::spiceai;
///
/// let mut conn = spiceai::connect("grpc://localhost:50051", Some("my-api-key"))
///     .expect("Failed to connect");
///
/// let batches = conn.query("SELECT 1").expect("query failed");
/// ```
pub fn connect(flight_url: &str, api_key: Option<&str>) -> Result<AdbcConnection> {
    let mut kwargs = HashMap::from([(
        "uri".to_string(),
        serde_json::Value::String(flight_url.to_string()),
    )]);

    if let Some(key) = api_key {
        kwargs.insert(
            "username".to_string(),
            serde_json::Value::String(String::new()),
        );
        kwargs.insert(
            "password".to_string(),
            serde_json::Value::String(key.to_string()),
        );
    }

    AdbcConnection::create("flightsql", kwargs)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Requires `SPICEAI_FLIGHT_URL` and `SPICEAI_API_KEY` env vars.
    #[test]
    #[ignore = "requires Spice.ai credentials in env"]
    fn test_spiceai_query() {
        let flight_url =
            std::env::var("SPICEAI_FLIGHT_URL").expect("SPICEAI_FLIGHT_URL must be set");
        let api_key = std::env::var("SPICEAI_API_KEY").ok();

        let mut conn =
            connect(&flight_url, api_key.as_deref()).expect("Failed to connect to Spice.ai");

        let batches = conn
            .query("SELECT 1 AS one")
            .expect("Failed to execute query");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(
            total_rows > 0,
            "Expected at least one row from Spice.ai query"
        );
    }

    /// TPC-H Q22 – Global Sales Opportunity
    ///
    /// Requires `SPICEAI_FLIGHT_URL` and `SPICEAI_API_KEY` env vars.
    /// SPICEAI_FLIGHT_URL="grpc://127.0.0.1:50051" cargo test -p adbc_client -- --ignored
    #[test]
    #[ignore = "requires Spice.ai credentials in env"]
    fn test_tpch_q22() {
        let flight_url =
            std::env::var("SPICEAI_FLIGHT_URL").expect("SPICEAI_FLIGHT_URL must be set");
        let api_key = std::env::var("SPICEAI_API_KEY").ok();

        let mut conn =
            connect(&flight_url, api_key.as_deref()).expect("Failed to connect to Spice.ai");

        let batches = conn
            .query(
                "select \
                    cntrycode, \
                    count(*) as numcust, \
                    sum(c_acctbal) as totacctbal \
                from \
                    ( \
                        select \
                            substring(c_phone from 1 for 2) as cntrycode, \
                            c_acctbal \
                        from \
                            customer \
                        where \
                                substring(c_phone from 1 for 2) in \
                                ('13', '31', '23', '29', '30', '18', '17') \
                          and c_acctbal > ( \
                            select \
                                avg(c_acctbal) \
                            from \
                                customer \
                            where \
                                    c_acctbal > 0.00 \
                              and substring(c_phone from 1 for 2) in \
                                  ('13', '31', '23', '29', '30', '18', '17') \
                        ) \
                          and not exists ( \
                                select \
                                    * \
                                from \
                                    orders \
                                where \
                                        o_custkey = c_custkey \
                            ) \
                    ) as custsale \
                group by \
                    cntrycode \
                order by \
                    cntrycode",
            )
            .expect("Failed to execute TPC-H Q22");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0, "Expected at least one row from TPC-H Q22");
    }
}
