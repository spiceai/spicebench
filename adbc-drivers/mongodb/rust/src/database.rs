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
use std::sync::Arc;

use adbc_core::error::Result;
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Database, Optionable};
use datafusion_table_providers::mongodb::connection_pool::MongoDBConnectionPool;
use secrecy::SecretString;
use tokio::runtime::Runtime;

use crate::connection::MongoDBConnection;
use crate::error::{adbc_err, io_err, not_implemented};

// Option key constants
pub const OPT_CONNECTION_STRING: &str = "adbc.driver.mongodb.connection_string";
pub const OPT_HOST: &str = "adbc.driver.mongodb.host";
pub const OPT_PORT: &str = "adbc.driver.mongodb.port";
pub const OPT_DB: &str = "adbc.driver.mongodb.db";
pub const OPT_USER: &str = "adbc.driver.mongodb.user";
pub const OPT_PASS: &str = "adbc.driver.mongodb.pass";
pub const OPT_AUTH_SOURCE: &str = "adbc.driver.mongodb.auth_source";
pub const OPT_SRV: &str = "adbc.driver.mongodb.srv";
pub const OPT_DIRECT_CONNECTION: &str = "adbc.driver.mongodb.direct_connection";
pub const OPT_SSL_MODE: &str = "adbc.driver.mongodb.sslmode";
pub const OPT_SSL_ROOT_CERT: &str = "adbc.driver.mongodb.sslrootcert";
pub const OPT_POOL_MIN: &str = "adbc.driver.mongodb.pool_min";
pub const OPT_POOL_MAX: &str = "adbc.driver.mongodb.pool_max";
pub const OPT_TIME_ZONE: &str = "adbc.driver.mongodb.time_zone";
pub const OPT_UNNEST_DEPTH: &str = "adbc.driver.mongodb.unnest_depth";
pub const OPT_SCHEMA_INFER_MAX_RECORDS: &str = "adbc.driver.mongodb.schema_infer_max_records";

pub struct MongoDBDatabase {
    runtime: Arc<Runtime>,
    // Raw option values keyed by their driver-specific param name
    // (without the "adbc.driver.mongodb." prefix, matching MongoDBConnectionPool::new() expectations)
    params: HashMap<String, String>,
}

impl MongoDBDatabase {
    pub fn new(runtime: Arc<Runtime>) -> Self {
        Self {
            runtime,
            params: HashMap::new(),
        }
    }

    fn set_param(&mut self, key: &str, value: String) {
        // Strip "adbc.driver.mongodb." prefix to get the raw param name
        let param_key = key
            .strip_prefix("adbc.driver.mongodb.")
            .unwrap_or(key)
            .to_string();
        self.params.insert(param_key, value);
    }

    fn build_pool(&self) -> Result<Arc<MongoDBConnectionPool>> {
        let secret_params: HashMap<String, SecretString> = self
            .params
            .iter()
            .map(|(k, v)| (k.clone(), SecretString::new(v.clone().into_boxed_str())))
            .collect();

        self.runtime.block_on(async {
            MongoDBConnectionPool::new(secret_params)
                .await
                .map(Arc::new)
                .map_err(|e| io_err(format!("failed to connect to MongoDB: {e}")))
        })
    }

    fn build_connection(&self) -> Result<MongoDBConnection> {
        let pool = self.build_pool()?;
        MongoDBConnection::new(Arc::clone(&self.runtime), pool)
    }
}

impl Optionable for MongoDBDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        let OptionValue::String(val) = value else {
            return Err(adbc_err(
                adbc_core::error::Status::InvalidArguments,
                "expected string option value",
            ));
        };

        match key {
            OptionDatabase::Uri => {
                // Accept a mongodb:// or mongodb+srv:// URI as the connection string
                self.params.insert("connection_string".to_string(), val);
                Ok(())
            }
            OptionDatabase::Other(ref key_str) => match key_str.as_str() {
                OPT_CONNECTION_STRING => {
                    self.params.insert("connection_string".to_string(), val);
                    Ok(())
                }
                OPT_HOST => {
                    self.params.insert("host".to_string(), val);
                    Ok(())
                }
                OPT_PORT => {
                    self.params.insert("port".to_string(), val);
                    Ok(())
                }
                OPT_DB => {
                    self.params.insert("db".to_string(), val);
                    Ok(())
                }
                OPT_USER => {
                    self.params.insert("user".to_string(), val);
                    Ok(())
                }
                OPT_PASS => {
                    self.params.insert("pass".to_string(), val);
                    Ok(())
                }
                OPT_AUTH_SOURCE => {
                    self.params.insert("auth_source".to_string(), val);
                    Ok(())
                }
                OPT_SRV => {
                    self.params.insert("srv".to_string(), val);
                    Ok(())
                }
                OPT_DIRECT_CONNECTION => {
                    self.params.insert("direct_connection".to_string(), val);
                    Ok(())
                }
                OPT_SSL_MODE => {
                    self.params.insert("sslmode".to_string(), val);
                    Ok(())
                }
                OPT_SSL_ROOT_CERT => {
                    self.params.insert("sslrootcert".to_string(), val);
                    Ok(())
                }
                OPT_POOL_MIN => {
                    val.parse::<u32>().map_err(|_| {
                        adbc_err(
                            adbc_core::error::Status::InvalidArguments,
                            format!("invalid pool_min value: {val}"),
                        )
                    })?;
                    self.params.insert("pool_min".to_string(), val);
                    Ok(())
                }
                OPT_POOL_MAX => {
                    val.parse::<u32>().map_err(|_| {
                        adbc_err(
                            adbc_core::error::Status::InvalidArguments,
                            format!("invalid pool_max value: {val}"),
                        )
                    })?;
                    self.params.insert("pool_max".to_string(), val);
                    Ok(())
                }
                OPT_TIME_ZONE => {
                    self.params.insert("time_zone".to_string(), val);
                    Ok(())
                }
                OPT_UNNEST_DEPTH => {
                    val.parse::<usize>().map_err(|_| {
                        adbc_err(
                            adbc_core::error::Status::InvalidArguments,
                            format!("invalid unnest_depth value: {val}"),
                        )
                    })?;
                    self.params.insert("unnest_depth".to_string(), val);
                    Ok(())
                }
                OPT_SCHEMA_INFER_MAX_RECORDS => {
                    val.parse::<u32>().map_err(|_| {
                        adbc_err(
                            adbc_core::error::Status::InvalidArguments,
                            format!("invalid schema_infer_max_records value: {val}"),
                        )
                    })?;
                    self.params
                        .insert("schema_infer_max_records".to_string(), val);
                    Ok(())
                }
                _ => {
                    // Accept any unknown option with the "adbc.driver.mongodb." prefix
                    if let Some(param) = key_str.strip_prefix("adbc.driver.mongodb.") {
                        self.set_param(param, val);
                        Ok(())
                    } else {
                        Err(not_implemented(&format!("option {key_str}")))
                    }
                }
            },
            _ => Err(not_implemented(&format!("option {:?}", key.as_ref()))),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionDatabase::Other(ref key_str) => {
                let param = key_str
                    .strip_prefix("adbc.driver.mongodb.")
                    .unwrap_or(key_str.as_str());
                self.params
                    .get(param)
                    .cloned()
                    .ok_or_else(|| not_implemented(&format!("option {key_str}")))
            }
            _ => Err(not_implemented(&format!("option {:?}", key.as_ref()))),
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

impl Database for MongoDBDatabase {
    type ConnectionType = MongoDBConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        self.build_connection()
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let mut conn = self.build_connection()?;
        for (key, value) in opts {
            conn.set_option(key, value)?;
        }
        Ok(conn)
    }
}
