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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_sdk_dynamodb::Client as DbClient;
use data_components::dynamodb::provider::DynamoDBTableProvider;
use datafusion::catalog::SchemaProvider;
use datafusion::catalog::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct DynamoDBSchemaProvider {
    sdk_config: SdkConfig,
    #[allow(dead_code)]
    client: Arc<DbClient>,
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
    default_parallelism: usize,
    table_parallelism: HashMap<String, usize>,
}

impl DynamoDBSchemaProvider {
    pub fn new(
        sdk_config: SdkConfig,
        client: Arc<DbClient>,
        default_parallelism: usize,
        table_parallelism: HashMap<String, usize>,
    ) -> Self {
        Self {
            sdk_config,
            client,
            tables: RwLock::new(HashMap::new()),
            default_parallelism,
            table_parallelism,
        }
    }
}

#[async_trait]
impl SchemaProvider for DynamoDBSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let tables = self.tables.blocking_read();
        tables.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        // Check cache first
        {
            let tables = self.tables.read().await;
            if let Some(provider) = tables.get(name) {
                return Ok(Some(Arc::clone(provider)));
            }
        }

        let write_parallelism = self
            .table_parallelism
            .get(name)
            .copied()
            .unwrap_or(self.default_parallelism);

        // Create provider on demand
        let provider = DynamoDBTableProvider::try_new(
            self.sdk_config.clone(),
            Arc::from(name),
            None,                                        // unnest_depth
            100,                                         // schema_infer_max_records
            None,                                        // config_partitions (auto)
            Duration::from_secs(1),                      // scan_interval
            "2006-01-02T15:04:05.000Z07:00".to_string(), // time_format (ISO 8601)
            Duration::ZERO,                              // ready_lag
            Arc::new(Default::default()),                // metrics_collector
            None,                                        // json_nesting
            write_parallelism,
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let provider: Arc<dyn TableProvider> = Arc::new(provider);

        // Cache it
        {
            let mut tables = self.tables.write().await;
            tables.insert(name.to_string(), Arc::clone(&provider));
        }

        Ok(Some(provider))
    }

    fn table_exist(&self, name: &str) -> bool {
        let tables = self.tables.blocking_read();
        tables.contains_key(name)
    }
}
