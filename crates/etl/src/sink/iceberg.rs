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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::DataFileFormat;
use iceberg::transaction::ApplyTransactionAction;
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use parquet::file::properties::WriterProperties;
use tokio::sync::Mutex as TokioMutex;

use super::{InsertOp, Sink};

/// Configuration for [`IcebergSink`].
#[derive(Debug, Clone)]
pub struct IcebergObjectStoreConfig {
    pub warehouse_uri: String,
    pub namespace: Vec<String>,
    pub s3_region: Option<String>,
    pub s3_endpoint: Option<String>,
}

/// ETL sink that writes batches as Iceberg tables in object storage.
///
/// This sink currently supports append-only (`Insert`) operations.
pub struct IcebergSink {
    catalog: Arc<iceberg::MemoryCatalog>,
    namespace: NamespaceIdent,
    warehouse_uri: String,
    created_tables: TokioMutex<HashSet<String>>,
}

impl IcebergSink {
    pub async fn new(config: IcebergObjectStoreConfig) -> anyhow::Result<Self> {
        let mut props = HashMap::from([(
            MEMORY_CATALOG_WAREHOUSE.to_string(),
            config.warehouse_uri.clone(),
        )]);

        if let Some(region) = &config.s3_region {
            props.insert("s3.region".to_string(), region.clone());
        }
        if let Some(endpoint) = &config.s3_endpoint {
            let endpoint_with_scheme =
                if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
                    endpoint.clone()
                } else {
                    format!("https://{endpoint}")
                };
            props.insert("s3.endpoint".to_string(), endpoint_with_scheme);
            props.insert("s3.path-style-access".to_string(), "true".to_string());
        }

        let catalog: Arc<iceberg::MemoryCatalog> = Arc::new(
            MemoryCatalogBuilder::default()
                .load("spicebench", props)
                .await?,
        );

        let namespace = NamespaceIdent::from_vec(config.namespace)?;
        if !catalog.namespace_exists(&namespace).await? {
            catalog.create_namespace(&namespace, HashMap::new()).await?;
        }

        Ok(Self {
            catalog,
            namespace,
            warehouse_uri: config.warehouse_uri,
            created_tables: TokioMutex::new(HashSet::new()),
        })
    }

    async fn ensure_table(&self, table_name: &str, batch: &RecordBatch) -> anyhow::Result<()> {
        {
            let created = self.created_tables.lock().await;
            if created.contains(table_name) {
                return Ok(());
            }
        }

        let table_ident = TableIdent::new(self.namespace.clone(), table_name.to_string());
        let exists = self.catalog.table_exists(&table_ident).await?;

        if !exists {
            let schema = arrow_schema_to_schema_auto_assign_ids(batch.schema().as_ref())?;
            let location = format!(
                "{}/{}",
                self.warehouse_uri.trim_end_matches('/'),
                table_name
            );
            let table_creation = TableCreation::builder()
                .name(table_name.to_string())
                .location(location)
                .schema(schema)
                .build();

            self.catalog
                .create_table(&self.namespace, table_creation)
                .await?;
        }

        let mut created = self.created_tables.lock().await;
        created.insert(table_name.to_string());
        Ok(())
    }

    fn batch_with_field_ids(
        &self,
        table: &iceberg::table::Table,
        batch: RecordBatch,
    ) -> anyhow::Result<RecordBatch> {
        let target_schema = schema_to_arrow_schema(table.metadata().current_schema())?;
        let normalized = RecordBatch::try_new(Arc::new(target_schema), batch.columns().to_vec())?;
        Ok(normalized)
    }
}

#[async_trait]
impl Sink for IcebergSink {
    async fn write(
        &self,
        table_name: &str,
        batch_id: u64,
        batch: RecordBatch,
        op: InsertOp,
    ) -> anyhow::Result<()> {
        match op {
            InsertOp::Insert => {}
            InsertOp::Update { .. } | InsertOp::Delete { .. } => {
                anyhow::bail!(
                    "Iceberg sink currently supports append-only writes. Unsupported operation: {op:?}"
                );
            }
        }

        if batch.num_rows() == 0 {
            return Ok(());
        }

        self.ensure_table(table_name, &batch).await?;

        let table_ident = TableIdent::new(self.namespace.clone(), table_name.to_string());
        let table = self.catalog.load_table(&table_ident).await?;
        let batch = self.batch_with_field_ids(&table, batch)?;

        let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
        let file_name_generator = DefaultFileNameGenerator::new(
            format!("{table_name}-{batch_id}"),
            None,
            DataFileFormat::Parquet,
        );
        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            table.current_schema_ref(),
        );
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );

        let mut data_file_writer = DataFileWriterBuilder::new(rolling_writer_builder)
            .build(None)
            .await?;

        data_file_writer.write(batch).await?;
        let data_files = data_file_writer.close().await?;

        if data_files.is_empty() {
            return Ok(());
        }

        let tx = Transaction::new(&table);
        let tx = tx.fast_append().add_data_files(data_files).apply(tx)?;
        tx.commit(self.catalog.as_ref()).await?;

        Ok(())
    }
}
