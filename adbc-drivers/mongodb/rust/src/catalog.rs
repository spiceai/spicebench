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

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::catalog::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion_table_providers::mongodb::connection_pool::MongoDBConnectionPool;
use datafusion_table_providers::mongodb::MongoDBTableFactory;
use tokio::sync::RwLock;

pub struct MongoDBSchemaProvider {
    factory: MongoDBTableFactory,
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl std::fmt::Debug for MongoDBSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MongoDBSchemaProvider")
    }
}

impl MongoDBSchemaProvider {
    pub fn new(pool: Arc<MongoDBConnectionPool>) -> Self {
        Self {
            factory: MongoDBTableFactory::new(pool),
            tables: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl SchemaProvider for MongoDBSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let tables = self.tables.blocking_read();
        tables.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        // Return from cache if available
        {
            let tables = self.tables.read().await;
            if let Some(provider) = tables.get(name) {
                return Ok(Some(Arc::clone(provider)));
            }
        }

        // Create provider on demand
        let provider = self
            .factory
            .table_provider(datafusion::sql::TableReference::bare(name.to_string()))
            .await
            .map_err(DataFusionError::External)?;

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
