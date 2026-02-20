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
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema};
use async_trait::async_trait;
use duckdb::arrow::record_batch::RecordBatch as DuckDBRecordBatch;
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
/// This sink supports `Insert`, `Update`, and `Delete` operations by applying
/// batched mutations into an in-memory DuckDB state table and rewriting the
/// Iceberg table snapshot from that state.
pub struct IcebergSink {
    catalog: Arc<iceberg::MemoryCatalog>,
    namespace: NamespaceIdent,
    warehouse_uri: String,
    created_tables: TokioMutex<HashSet<String>>,
    state_tables: TokioMutex<HashSet<String>>,
    state_conn: Arc<Mutex<duckdb::Connection>>,
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
            state_tables: TokioMutex::new(HashSet::new()),
            state_conn: Arc::new(Mutex::new(
                duckdb::Connection::open_in_memory()
                    .map_err(|e| anyhow::anyhow!("Failed to open DuckDB state database: {e}"))?,
            )),
        })
    }

    async fn ensure_table(&self, table_name: &str, batch: &RecordBatch) -> anyhow::Result<()> {
        {
            let created = self.created_tables.lock().await;
            if created.contains(table_name) {
                self.ensure_state_table(table_name, batch.schema().as_ref())
                    .await?;
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
        drop(created);

        self.ensure_state_table(table_name, batch.schema().as_ref())
            .await?;
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

    fn state_table_name(table_name: &str) -> String {
        format!("__iceberg_state_{}", table_name.replace('-', "_"))
    }

    async fn ensure_state_table(&self, table_name: &str, schema: &Schema) -> anyhow::Result<()> {
        {
            let created = self.state_tables.lock().await;
            if created.contains(table_name) {
                return Ok(());
            }
        }

        let state_table = Self::state_table_name(table_name);
        let create_sql = Self::create_table_sql(&state_table, schema)?;
        self.execute_duckdb_sql_batch(vec![create_sql]).await?;

        let mut created = self.state_tables.lock().await;
        created.insert(table_name.to_string());
        Ok(())
    }

    fn create_table_sql(table_name: &str, schema: &Schema) -> anyhow::Result<String> {
        let columns = schema
            .fields()
            .iter()
            .map(|f| {
                let col_type = sql_type_for_arrow(f.data_type())?;
                Ok::<_, anyhow::Error>(format!("{} {col_type}", quote_identifier(f.name())))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .join(", ");

        Ok(format!(
            "CREATE TABLE IF NOT EXISTS {} ({columns})",
            quote_identifier(table_name)
        ))
    }

    async fn execute_duckdb_sql_batch(&self, statements: Vec<String>) -> anyhow::Result<()> {
        let conn = Arc::clone(&self.state_conn);
        tokio::task::spawn_blocking(move || {
            let guard = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("DuckDB connection lock poisoned: {e}"))?;
            for sql in &statements {
                guard
                    .execute(sql, [])
                    .map_err(|e| anyhow::anyhow!("DuckDB SQL execution failed: {e}"))?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await?
    }

    async fn append_state_batch(&self, table_name: &str, batch: RecordBatch) -> anyhow::Result<()> {
        let conn = Arc::clone(&self.state_conn);
        let state_table = Self::state_table_name(table_name);
        tokio::task::spawn_blocking(move || {
            let guard = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("DuckDB connection lock poisoned: {e}"))?;
            let mut appender = guard.appender(&state_table).map_err(|e| {
                anyhow::anyhow!("Failed to create DuckDB appender for '{state_table}': {e}")
            })?;
            appender.append_record_batch(batch).map_err(|e| {
                anyhow::anyhow!("Failed to append record batch to '{state_table}': {e}")
            })?;
            appender.flush().map_err(|e| {
                anyhow::anyhow!("Failed to flush DuckDB appender for '{state_table}': {e}")
            })?;
            Ok::<_, anyhow::Error>(())
        })
        .await?
    }

    fn create_temp_table_sql(table_name: &str, schema: &Schema) -> anyhow::Result<String> {
        let columns = schema
            .fields()
            .iter()
            .map(|f| {
                let col_type = sql_type_for_arrow(f.data_type())?;
                Ok::<_, anyhow::Error>(format!("{} {col_type}", quote_identifier(f.name())))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .join(", ");

        Ok(format!(
            "CREATE TEMPORARY TABLE IF NOT EXISTS {} ({columns})",
            quote_identifier(table_name)
        ))
    }

    fn update_from_staging_sql(
        target: &str,
        staging: &str,
        schema: &Schema,
        key_columns: &[String],
    ) -> anyhow::Result<String> {
        let target_ident = quote_identifier(target);
        let staging_ident = quote_identifier(staging);

        let set_clauses: Vec<String> = schema
            .fields()
            .iter()
            .filter(|f| !key_columns.contains(f.name()))
            .map(|f| {
                let col = quote_identifier(f.name());
                format!("{col} = __stg.{col}")
            })
            .collect();

        if set_clauses.is_empty() {
            anyhow::bail!("Update requires at least one non-key column in batch schema");
        }

        let join_predicates: Vec<String> = key_columns
            .iter()
            .map(|k| {
                let col = quote_identifier(k);
                format!("{target_ident}.{col} IS NOT DISTINCT FROM __stg.{col}")
            })
            .collect();

        Ok(format!(
            "UPDATE {target_ident} SET {} FROM {staging_ident} AS __stg WHERE {}",
            set_clauses.join(", "),
            join_predicates.join(" AND ")
        ))
    }

    fn delete_using_staging_sql(
        target: &str,
        staging: &str,
        key_columns: &[String],
    ) -> anyhow::Result<String> {
        let target_ident = quote_identifier(target);
        let staging_ident = quote_identifier(staging);

        let join_predicates: Vec<String> = key_columns
            .iter()
            .map(|k| {
                let col = quote_identifier(k);
                format!("{target_ident}.{col} IS NOT DISTINCT FROM __stg.{col}")
            })
            .collect();

        Ok(format!(
            "DELETE FROM {target_ident} USING {staging_ident} AS __stg WHERE {}",
            join_predicates.join(" AND ")
        ))
    }

    fn key_column_indexes(
        batch: &RecordBatch,
        key_columns: &[String],
    ) -> anyhow::Result<Vec<usize>> {
        if key_columns.is_empty() {
            anyhow::bail!("Update/Delete requires at least one key column");
        }

        let schema = batch.schema();
        key_columns
            .iter()
            .map(|col| {
                schema
                    .index_of(col)
                    .map_err(|_| anyhow::anyhow!("Key column '{col}' not found in batch schema"))
            })
            .collect()
    }

    async fn apply_via_staging(
        &self,
        table_name: &str,
        batch_id: u64,
        batch: RecordBatch,
        key_columns: &[String],
        is_update: bool,
    ) -> anyhow::Result<()> {
        let _ = Self::key_column_indexes(&batch, key_columns)?;

        let state_table = Self::state_table_name(table_name);
        let schema = batch.schema();
        let staging = format!("__iceberg_staging_{batch_id}");
        let create_sql = Self::create_temp_table_sql(&staging, &schema)?;
        let apply_sql = if is_update {
            Self::update_from_staging_sql(&state_table, &staging, &schema, key_columns)?
        } else {
            Self::delete_using_staging_sql(&state_table, &staging, key_columns)?
        };
        let drop_sql = format!("DROP TABLE IF EXISTS {}", quote_identifier(&staging));

        let conn = Arc::clone(&self.state_conn);
        tokio::task::spawn_blocking(move || {
            let guard = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("DuckDB connection lock poisoned: {e}"))?;

            guard
                .execute(&create_sql, [])
                .map_err(|e| anyhow::anyhow!("Failed to create staging table '{staging}': {e}"))?;

            {
                let mut appender = guard.appender(&staging).map_err(|e| {
                    anyhow::anyhow!("Failed to create appender for staging table: {e}")
                })?;
                appender
                    .append_record_batch(batch)
                    .map_err(|e| anyhow::anyhow!("Failed to append to staging table: {e}"))?;
                appender
                    .flush()
                    .map_err(|e| anyhow::anyhow!("Failed to flush staging appender: {e}"))?;
            }

            guard
                .execute(&apply_sql, [])
                .map_err(|e| anyhow::anyhow!("Failed to apply staged operation: {e}"))?;

            guard
                .execute(&drop_sql, [])
                .map_err(|e| anyhow::anyhow!("Failed to drop staging table: {e}"))?;

            Ok::<_, anyhow::Error>(())
        })
        .await?
    }

    async fn query_state_batches(&self, table_name: &str) -> anyhow::Result<Vec<RecordBatch>> {
        let conn = Arc::clone(&self.state_conn);
        let sql = format!(
            "SELECT * FROM {}",
            quote_identifier(&Self::state_table_name(table_name))
        );
        tokio::task::spawn_blocking(move || {
            let guard = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("DuckDB connection lock poisoned: {e}"))?;
            let mut stmt = guard
                .prepare(&sql)
                .map_err(|e| anyhow::anyhow!("Failed to prepare DuckDB query: {e}"))?;
            let duckdb_batches: Vec<DuckDBRecordBatch> = stmt
                .query_arrow([])
                .map_err(|e| anyhow::anyhow!("Failed to execute DuckDB query: {e}"))?
                .collect();

            let mut batches = Vec::with_capacity(duckdb_batches.len());
            for db_batch in duckdb_batches {
                let mut buf = Vec::new();
                {
                    let mut writer = duckdb::arrow::ipc::writer::FileWriter::try_new(
                        &mut buf,
                        &db_batch.schema(),
                    )
                    .map_err(|e| anyhow::anyhow!("IPC write init failed: {e}"))?;
                    writer
                        .write(&db_batch)
                        .map_err(|e| anyhow::anyhow!("IPC write failed: {e}"))?;
                    writer
                        .finish()
                        .map_err(|e| anyhow::anyhow!("IPC finish failed: {e}"))?;
                }
                let reader =
                    arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(buf), None)
                        .map_err(|e| anyhow::anyhow!("IPC read failed: {e}"))?;
                for batch in reader {
                    batches.push(batch.map_err(|e| anyhow::anyhow!("IPC batch read failed: {e}"))?);
                }
            }
            Ok::<_, anyhow::Error>(batches)
        })
        .await?
    }

    async fn rewrite_iceberg_table_from_state(
        &self,
        table_name: &str,
        fallback_schema: &RecordBatch,
    ) -> anyhow::Result<()> {
        let state_batches = self.query_state_batches(table_name).await?;
        let create_schema_batch = state_batches.first().unwrap_or(fallback_schema);

        let table_ident = TableIdent::new(self.namespace.clone(), table_name.to_string());
        if self.catalog.table_exists(&table_ident).await? {
            self.catalog.drop_table(&table_ident).await?;
        }

        let schema = arrow_schema_to_schema_auto_assign_ids(create_schema_batch.schema().as_ref())?;
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

        let table = self.catalog.load_table(&table_ident).await?;
        if state_batches.is_empty() {
            return Ok(());
        }

        let mut data_files = Vec::new();
        for (chunk_idx, batch) in state_batches.into_iter().enumerate() {
            if batch.num_rows() == 0 {
                continue;
            }

            let normalized_batch = self.batch_with_field_ids(&table, batch)?;
            let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
            let file_name_generator = DefaultFileNameGenerator::new(
                format!("{table_name}-rewrite-{chunk_idx}"),
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
            data_file_writer.write(normalized_batch).await?;
            data_files.extend(data_file_writer.close().await?);
        }

        if data_files.is_empty() {
            return Ok(());
        }

        let tx = Transaction::new(&table);
        let tx = tx.fast_append().add_data_files(data_files).apply(tx)?;
        tx.commit(self.catalog.as_ref()).await?;

        Ok(())
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
        if batch.num_rows() == 0 {
            return Ok(());
        }

        self.ensure_table(table_name, &batch).await?;

        let table_ident = TableIdent::new(self.namespace.clone(), table_name.to_string());
        let table = self.catalog.load_table(&table_ident).await?;
        let normalized_batch = self.batch_with_field_ids(&table, batch)?;

        match &op {
            InsertOp::Insert => {
                self.append_state_batch(table_name, normalized_batch)
                    .await?;
            }
            InsertOp::Update { key_columns } => {
                self.apply_via_staging(table_name, batch_id, normalized_batch, key_columns, true)
                    .await?;
            }
            InsertOp::Delete { key_columns } => {
                self.apply_via_staging(table_name, batch_id, normalized_batch, key_columns, false)
                    .await?;
            }
        }

        self.rewrite_iceberg_table_from_state(table_name, &table.current_schema_as_batch()?)
            .await?;

        Ok(())
    }
}

trait TableSchemaAsBatch {
    fn current_schema_as_batch(&self) -> anyhow::Result<RecordBatch>;
}

impl TableSchemaAsBatch for iceberg::table::Table {
    fn current_schema_as_batch(&self) -> anyhow::Result<RecordBatch> {
        let schema = schema_to_arrow_schema(self.metadata().current_schema())?;
        let empty_columns = schema
            .fields()
            .iter()
            .map(|f| arrow::array::new_empty_array(f.data_type()))
            .collect::<Vec<_>>();
        Ok(RecordBatch::try_new(Arc::new(schema), empty_columns)?)
    }
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn sql_type_for_arrow(data_type: &DataType) -> anyhow::Result<String> {
    match data_type {
        DataType::Boolean => Ok("BOOLEAN".to_string()),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16 => {
            Ok("INT".to_string())
        }
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => Ok("BIGINT".to_string()),
        DataType::Float32 => Ok("FLOAT".to_string()),
        DataType::Float64 => Ok("DOUBLE".to_string()),
        DataType::Utf8 | DataType::LargeUtf8 => Ok("STRING".to_string()),
        DataType::Date32 => Ok("DATE".to_string()),
        DataType::Timestamp(_, _) => Ok("TIMESTAMP".to_string()),
        DataType::Decimal128(p, s) => Ok(format!("DECIMAL({p}, {s})")),
        other => {
            anyhow::bail!("Unsupported Arrow data type for Iceberg sink state table: {other:?}")
        }
    }
}
