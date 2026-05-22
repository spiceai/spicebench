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

use std::collections::HashSet;
use std::sync::Arc;

use adbc_core::error::Result;
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::{Connection, Optionable};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::mongodb::connection_pool::MongoDBConnectionPool;
use tokio::runtime::Runtime;

use crate::catalog::MongoDBSchemaProvider;
use crate::error::{io_err, not_implemented};
use crate::statement::MongoDBStatement;

pub struct MongoDBConnection {
    runtime: Arc<Runtime>,
    ctx: SessionContext,
    pool: Arc<MongoDBConnectionPool>,
}

impl MongoDBConnection {
    pub fn new(runtime: Arc<Runtime>, pool: Arc<MongoDBConnectionPool>) -> Result<Self> {
        let ctx = SessionContext::new();

        let schema_provider = Arc::new(MongoDBSchemaProvider::new(Arc::clone(&pool)));

        let catalog = ctx
            .catalog("datafusion")
            .ok_or_else(|| io_err("default catalog 'datafusion' not found"))?;
        catalog
            .register_schema("public", schema_provider)
            .map_err(|e| io_err(format!("failed to register schema: {e}")))?;

        Ok(Self { runtime, ctx, pool })
    }
}

impl Optionable for MongoDBConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, _value: OptionValue) -> Result<()> {
        Err(not_implemented(&format!(
            "connection option {:?}",
            key.as_ref()
        )))
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionConnection::AutoCommit => Ok("true".to_string()),
            _ => Err(not_implemented(&format!(
                "connection option {:?}",
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

struct VecBatchReader {
    schema: Arc<Schema>,
    batches: std::vec::IntoIter<RecordBatch>,
}

impl VecBatchReader {
    fn new(schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches: batches.into_iter(),
        }
    }

    fn empty(schema: Arc<Schema>) -> Self {
        Self::new(schema, vec![])
    }
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

impl Connection for MongoDBConnection {
    type StatementType = MongoDBStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(MongoDBStatement::new(
            Arc::clone(&self.runtime),
            self.ctx.clone(),
            Arc::clone(&self.pool),
        ))
    }

    fn cancel(&mut self) -> Result<()> {
        Ok(())
    }

    fn get_info(&self, _codes: Option<HashSet<InfoCode>>) -> Result<impl RecordBatchReader + Send> {
        let schema = Arc::new(Schema::empty());
        Ok(VecBatchReader::empty(schema))
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> Result<impl RecordBatchReader + Send> {
        let schema = Arc::new(Schema::empty());
        Ok(VecBatchReader::empty(schema))
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        let table_name = table_name.to_string();
        let ctx = self.ctx.clone();
        self.runtime.block_on(async {
            let catalog = ctx
                .catalog("datafusion")
                .ok_or_else(|| io_err("default catalog not found"))?;
            let schema_provider = catalog
                .schema("public")
                .ok_or_else(|| io_err("default schema not found"))?;
            let table = schema_provider
                .table(&table_name)
                .await
                .map_err(|e| io_err(format!("failed to get table: {e}")))?;
            let table = table.ok_or_else(|| {
                crate::error::adbc_err(
                    adbc_core::error::Status::NotFound,
                    format!("table '{table_name}' not found"),
                )
            })?;
            Ok(table.schema().as_ref().clone())
        })
    }

    fn get_table_types(&self) -> Result<impl RecordBatchReader + Send> {
        use arrow_array::StringArray;
        let schema = Arc::new(Schema::new(vec![arrow_schema::Field::new(
            "table_type",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["TABLE"]))],
        )
        .map_err(|e| io_err(format!("failed to build table_types: {e}")))?;
        Ok(VecBatchReader::new(schema, vec![batch]))
    }

    fn get_statistic_names(&self) -> Result<impl RecordBatchReader + Send> {
        let schema = Arc::new(Schema::empty());
        Ok(VecBatchReader::empty(schema))
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<impl RecordBatchReader + Send> {
        let schema = Arc::new(Schema::empty());
        Ok(VecBatchReader::empty(schema))
    }

    fn commit(&mut self) -> Result<()> {
        Err(not_implemented("commit"))
    }

    fn rollback(&mut self) -> Result<()> {
        Err(not_implemented("rollback"))
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<impl RecordBatchReader + Send> {
        Err::<VecBatchReader, _>(not_implemented("read_partition"))
    }
}
