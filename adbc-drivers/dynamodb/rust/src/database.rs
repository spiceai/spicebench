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
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::config::Region;
use tokio::runtime::Runtime;

use crate::connection::DynamoDBConnection;
use crate::error::not_implemented;

pub const OPT_REGION: &str = "adbc.driver.dynamodb.region";
pub const OPT_ENDPOINT: &str = "adbc.driver.dynamodb.endpoint";
pub const OPT_ACCESS_KEY_ID: &str = "adbc.driver.dynamodb.access_key_id";
pub const OPT_SECRET_ACCESS_KEY: &str = "adbc.driver.dynamodb.secret_access_key";
pub const OPT_SESSION_TOKEN: &str = "adbc.driver.dynamodb.session_token";
pub const OPT_PROFILE: &str = "adbc.driver.dynamodb.profile";
pub const OPT_PARALLELISM: &str = "adbc.driver.dynamodb.parallelism";
const OPT_PARALLELISM_PREFIX: &str = "adbc.driver.dynamodb.parallelism.";

pub struct DynamoDBDatabase {
    runtime: Arc<Runtime>,
    region: String,
    endpoint: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
    session_token: Option<String>,
    profile: Option<String>,
    pub(crate) default_parallelism: usize,
    pub(crate) table_parallelism: HashMap<String, usize>,
}

impl DynamoDBDatabase {
    pub fn new(runtime: Arc<Runtime>) -> Self {
        Self {
            runtime,
            region: "us-east-1".to_string(),
            endpoint: None,
            access_key: None,
            secret_key: None,
            session_token: None,
            profile: None,
            default_parallelism: 10,
            table_parallelism: HashMap::new(),
        }
    }

    fn build_connection(&self) -> Result<DynamoDBConnection> {
        let runtime = Arc::clone(&self.runtime);
        let region = self.region.clone();
        let endpoint = self.endpoint.clone();
        let access_key = self.access_key.clone();
        let secret_key = self.secret_key.clone();
        let session_token = self.session_token.clone();
        let profile = self.profile.clone();

        let (sdk_config, client) = runtime.block_on(async {
            let mut loader =
                aws_config::defaults(BehaviorVersion::latest()).region(Region::new(region));

            if let Some(profile) = profile {
                loader = loader.profile_name(profile);
            }

            if let (Some(ak), Some(sk)) = (access_key, secret_key) {
                loader = loader.credentials_provider(Credentials::new(
                    ak,
                    sk,
                    session_token,
                    None,
                    "adbc-dynamodb",
                ));
            }

            if let Some(ref ep) = endpoint {
                loader = loader.endpoint_url(ep);
            }

            let sdk_config = loader.load().await;

            let mut dynamo_config = aws_sdk_dynamodb::config::Builder::from(&sdk_config);
            if let Some(ep) = endpoint {
                dynamo_config = dynamo_config.endpoint_url(ep);
            }

            let client = aws_sdk_dynamodb::Client::from_conf(dynamo_config.build());
            (sdk_config, client)
        });

        DynamoDBConnection::new(
            runtime,
            sdk_config,
            client,
            self.default_parallelism,
            self.table_parallelism.clone(),
        )
    }
}

impl Optionable for DynamoDBDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        let OptionValue::String(val) = value else {
            return Err(crate::error::adbc_err(
                adbc_core::error::Status::InvalidArguments,
                "expected string option value",
            ));
        };

        match key {
            OptionDatabase::Uri => {
                if let Some(rest) = val.strip_prefix("dynamodb://") {
                    if let Some((host, query)) = rest.split_once('?') {
                        if !host.is_empty() {
                            self.endpoint = Some(format!("http://{host}"));
                        }
                        for param in query.split('&') {
                            if let Some((k, v)) = param.split_once('=') {
                                match k {
                                    "region" => self.region = v.to_string(),
                                    "access_key_id" => self.access_key = Some(v.to_string()),
                                    "secret_access_key" => {
                                        self.secret_key = Some(v.to_string());
                                    }
                                    "session_token" => {
                                        self.session_token = Some(v.to_string());
                                    }
                                    "profile" => self.profile = Some(v.to_string()),
                                    _ => {}
                                }
                            }
                        }
                    } else if !rest.is_empty() {
                        self.endpoint = Some(format!("http://{rest}"));
                    }
                }
                Ok(())
            }
            OptionDatabase::Other(ref key_str) => match key_str.as_str() {
                OPT_REGION => {
                    self.region = val;
                    Ok(())
                }
                OPT_ENDPOINT => {
                    self.endpoint = Some(val);
                    Ok(())
                }
                OPT_ACCESS_KEY_ID => {
                    self.access_key = Some(val);
                    Ok(())
                }
                OPT_SECRET_ACCESS_KEY => {
                    self.secret_key = Some(val);
                    Ok(())
                }
                OPT_SESSION_TOKEN => {
                    self.session_token = Some(val);
                    Ok(())
                }
                OPT_PROFILE => {
                    self.profile = Some(val);
                    Ok(())
                }
                OPT_PARALLELISM => {
                    self.default_parallelism = val.parse::<usize>().map_err(|_| {
                        crate::error::adbc_err(
                            adbc_core::error::Status::InvalidArguments,
                            format!("invalid parallelism value: {val}"),
                        )
                    })?;
                    Ok(())
                }
                key if key.starts_with(OPT_PARALLELISM_PREFIX) => {
                    let table = &key[OPT_PARALLELISM_PREFIX.len()..];
                    let p = val.parse::<usize>().map_err(|_| {
                        crate::error::adbc_err(
                            adbc_core::error::Status::InvalidArguments,
                            format!("invalid parallelism value for table '{table}': {val}"),
                        )
                    })?;
                    self.table_parallelism.insert(table.to_string(), p);
                    Ok(())
                }
                _ => Err(not_implemented(&format!("option {key_str}"))),
            },
            _ => Err(not_implemented(&format!("option {:?}", key.as_ref()))),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionDatabase::Other(ref key_str) => match key_str.as_str() {
                OPT_REGION => Ok(self.region.clone()),
                OPT_ENDPOINT => Ok(self.endpoint.clone().unwrap_or_default()),
                _ => Err(not_implemented(&format!("option {key_str}"))),
            },
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

impl Database for DynamoDBDatabase {
    type ConnectionType = DynamoDBConnection;

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
