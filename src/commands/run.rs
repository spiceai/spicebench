/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
use std::sync::Arc;

use checkpointer::CheckpointStore;
use data_generation::config::{TargetConfig, build_version_prefix, format_scale_factor};
use data_generation::storage::DataStorage;
use data_generation::storage::file::FileStorage;
use data_generation::storage::s3::S3Storage;
use data_generation::version::VersionMetadata;
use etl::sink::Sink;
use etl::sink::adbc::AdbcSink;
use etl::{DatasetSource, ETLPipeline, PipelineState, StopReason};
use test_framework::anyhow;
use tokio::sync::Mutex;

use crate::args::RunArgs;
use crate::commands::connect_system_adapter;

const FLIGHTSQL_MAX_MSG_SIZE_OPTION: &str = "adbc.flight.sql.client_option.with_max_msg_size";
const DEFAULT_FLIGHTSQL_MAX_MSG_SIZE_BYTES: &str = "78643200";

fn infer_adbc_target_namespace(
    catalog_namespace: Option<&str>,
) -> (Option<String>, Option<String>) {
    let Some(namespace) = catalog_namespace.map(str::trim).filter(|ns| !ns.is_empty()) else {
        return (None, None);
    };

    let mut parts = namespace.rsplitn(2, '.');
    let schema = parts.next().map(str::trim).filter(|v| !v.is_empty());
    let catalog = parts.next().map(str::trim).filter(|v| !v.is_empty());

    (catalog.map(str::to_string), schema.map(str::to_string))
}

async fn run_benchmark(
    common: &RunArgs,
    system_adapter_client: Arc<Mutex<system_adapter_protocol::Client>>,
    run_id: uuid::Uuid,
    setup_metadata: HashMap<String, serde_json::Value>,
    version_metadata: &VersionMetadata,
    file_storage: Arc<FileStorage>,
) -> anyhow::Result<()> {
    // --- Download checkpoints from S3 ---
    let scenario_name = common.scenario.to_string();
    let checkpoint_dir = tempfile::tempdir()?;

    let derived_version = format_scale_factor(common.scale_factor);
    let version_prefix = build_version_prefix(&common.etl_prefix, &scenario_name, &derived_version);
    let checkpoint_store = CheckpointStore::new(
        &common.etl_bucket,
        &version_prefix,
        common.etl_region.as_deref(),
        common.etl_endpoint.as_deref(),
    )?;

    let manifest = checkpoint_store
        .download_manifest()
        .await
        .map_err(|e| {
            tracing::warn!("Failed to download checkpoint manifest - results validation will not be enabled: {e}");
            e
        })
        .ok();
    let mut checkpoint_steps: Option<usize> = None;
    if let Some(manifest) = manifest
        && let Some(scenario_info) = manifest.scenarios.get(&scenario_name)
    {
        tracing::info!(
            scenario = %scenario_name,
            num_checkpoints = scenario_info.checkpoint_indexes.len(),
            num_queries = scenario_info.query_indexes.len(),
            checkpoint_interval_steps = scenario_info.checkpoint_interval_steps,
            path = %checkpoint_dir.path().display(),
            "Downloading checkpoints"
        );
        if scenario_info.checkpoint_interval_steps > 0 {
            checkpoint_steps = Some(scenario_info.checkpoint_interval_steps);
        }
        if let Err(e) = checkpoint_store
            .download_checkpoints(&scenario_name, scenario_info, checkpoint_dir.path())
            .await
        {
            tracing::warn!(
                "Failed to download checkpoints - results validation will not be enabled: {e}"
            );
        } else {
            tracing::info!(scenario = %scenario_name, "Checkpoints downloaded");
        }
    } else {
        tracing::warn!(
            scenario = %scenario_name,
            "No checkpoints found for scenario in manifest"
        );
    }

    let dataset_source = DatasetSource::from_dataset_type(&version_metadata.dataset_type)?;
    let generation_config = version_metadata.dataset_config();
    let mutations = version_metadata.mutation_config();
    let data_source: Arc<dyn DataStorage> = file_storage.clone();

    let etl_sink_type = system_adapter_protocol::EtlSinkType::Adbc;
    let target_config = None;

    let mut datasets = ETLPipeline::create_tables_request_datasets(
        dataset_source.clone(),
        &generation_config,
        Arc::clone(&data_source),
        &mutations,
        target_config.clone(),
    )?;

    // When using the staging_table update strategy, tables must NOT have
    // primary keys. MERGE INTO handles matching via the ON clause and uses
    // delete+insert execution, which conflicts with Cayenne's automatic
    // on_conflict: Upsert behavior on primary-key tables.
    let uses_staging_table = std::env::var("SPICEBENCH_ADBC_UPDATE_STRATEGY")
        .ok()
        .map(|v| v.eq_ignore_ascii_case("staging_table"))
        .unwrap_or(false);
    if uses_staging_table {
        for config in datasets.values_mut() {
            config.primary_key_columns.clear();
        }
    }
    let seed_data = generate_seed_data(&datasets, 5)?;
    let (setup_response, mut pipeline) = {
        let setup_response = system_adapter_client
            .lock()
            .await
            .setup(
                run_id,
                setup_metadata.clone(),
                datasets.clone(),
                Some(etl_sink_type),
                seed_data,
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to setup system adapter: {e}"))?;

        let driver_name = setup_response.driver.to_string();
        let mut db_kwargs = setup_response.db_kwargs.clone();
        if driver_name.eq_ignore_ascii_case("flightsql") {
            db_kwargs
                .entry(FLIGHTSQL_MAX_MSG_SIZE_OPTION.to_string())
                .or_insert_with(|| {
                    serde_json::Value::String(DEFAULT_FLIGHTSQL_MAX_MSG_SIZE_BYTES.to_string())
                });
        }

        let write_schema = db_kwargs
            .remove("spicebench.write_schema")
            .and_then(|v| v.as_str().map(str::to_string));
        let (target_db_catalog, target_db_schema) = infer_adbc_target_namespace(
            write_schema
                .as_deref()
                .or(setup_response.catalog_namespace.as_deref()),
        );

        let target_sink: Arc<dyn Sink> = Arc::new(AdbcSink::new(
            &driver_name,
            db_kwargs,
            target_db_catalog,
            target_db_schema,
            Some((Arc::clone(&system_adapter_client), run_id)),
            setup_response.table_name_map.clone(),
        )?);

        let mut pipeline = ETLPipeline::new(
            dataset_source,
            &generation_config,
            Arc::clone(&data_source),
            target_sink,
            &mutations,
        )?;

        pipeline.initialize().await?;

        (setup_response, pipeline)
    };

    let driver_name = setup_response.driver.to_string();
    let mut db_kwargs = setup_response.db_kwargs.clone();
    let query_catalog_namespace = setup_response.catalog_namespace.clone();
    let read_driver = setup_response.read_driver.clone();

    if driver_name.eq_ignore_ascii_case("flightsql") {
        db_kwargs
            .entry(FLIGHTSQL_MAX_MSG_SIZE_OPTION.to_string())
            .or_insert_with(|| {
                serde_json::Value::String(DEFAULT_FLIGHTSQL_MAX_MSG_SIZE_BYTES.to_string())
            });
    }

    // Allow system adapter to optionally provide read connection different from the write connection.
    // If not specified - use the same connection as the write connection.
    let (read_driver_name, read_db_kwargs) = match read_driver {
        None => (driver_name.clone(), db_kwargs.clone()),
        Some((read_driver, read_db_kwargs)) => (read_driver.to_string(), read_db_kwargs.clone()),
    };

    // Size the read pool to accommodate both the test workers and the
    // checkpoint validation executor running concurrently.  Each test worker
    // holds one connection for the duration of a query, and during checkpoint
    // validation `validate_full_query_set` runs up to `concurrency` queries
    // in parallel via a *separate* executor backed by the same pool.  Without
    // enough headroom the excess `pool.get()` calls block and eventually hit
    // r2d2's 30-second connection timeout ("timed out waiting for connection").
    let read_pool_size: u32 = (common.concurrency * 2 + 1).try_into().unwrap_or(u32::MAX);
    let read_pool =
        match adbc_client::create_pool(&read_driver_name, read_db_kwargs, Some(read_pool_size)) {
            Ok(pool) => pool,
            Err(e) => {
                pipeline.cancel();
                return Err(anyhow::anyhow!(
                    "Failed to create ADBC connection pool for driver {read_driver_name}: {e}"
                ));
            }
        };
    tracing::info!(
        "ADBC connection pool created (driver: {read_driver_name}, size: {read_pool_size})",
    );

    super::load::run(
        system_adapter_client,
        run_id,
        &common.scenario,
        common,
        version_metadata,
        read_pool,
        &mut pipeline,
        checkpoint_steps,
        Some(checkpoint_dir.path()),
        query_catalog_namespace,
    )
    .await?;

    // --- Wait for ETL to finish ---
    let final_state = pipeline.wait().await;
    match &final_state {
        PipelineState::Stopped(StopReason::Completed) => {
            tracing::info!("ETL pipeline completed successfully");
        }
        PipelineState::Stopped(StopReason::Cancelled) => {
            tracing::warn!("ETL pipeline was cancelled");
        }
        PipelineState::Stopped(StopReason::Error(e)) => {
            tracing::error!(error = %e, "ETL pipeline stopped with error");
        }
        other => {
            tracing::warn!("Unexpected final pipeline state: {other:?}");
        }
    }

    Ok(())
}

pub async fn execute(args: &RunArgs) -> anyhow::Result<()> {
    let scenario_name = args.scenario.to_string();
    let derived_version = format_scale_factor(args.scale_factor);
    let version_prefix = build_version_prefix(&args.etl_prefix, &scenario_name, &derived_version);
    tracing::info!(
        etl_source = %format!("s3://{}/{}/", args.etl_bucket, version_prefix),
        etl_bucket = %args.etl_bucket,
        etl_prefix = %args.etl_prefix,
        scale_factor = args.scale_factor,
        derived_version = %derived_version,
        etl_region = ?args.etl_region,
        table_format = %args.table_format,
        etl_sink = ?args.etl_sink,
        scenario = %scenario_name,
        concurrency = args.concurrency,
        "ETL configuration"
    );

    // Step 1: Download and extract the data archive to a TempDir.
    // This happens before benchmark timing begins.
    let extract_dir = tempfile::tempdir()?;

    let source_config = TargetConfig {
        bucket: args.etl_bucket.clone(),
        prefix: version_prefix.clone(),
        region: args.etl_region.clone(),
        endpoint: args.etl_endpoint.clone(),
        partition_columns: vec![],
    };
    let archive_storage: Arc<dyn DataStorage> = Arc::new(S3Storage::new(&source_config)?);

    tracing::info!(
        extract_dir = %extract_dir.path().display(),
        "Downloading and extracting data archive"
    );
    ETLPipeline::download(archive_storage, extract_dir.path()).await?;

    // Step 2: Create FileStorage from extracted data and read version metadata.
    let file_storage = Arc::new(FileStorage::new(extract_dir.path()));
    let version_metadata = file_storage.read_version_metadata().await?.ok_or_else(|| {
        anyhow::anyhow!(
            "No version.json found in extracted data at {}. Was data generation run?",
            extract_dir.path().display(),
        )
    })?;

    // --- Connect to the system adapter ---
    let system_adapter_client = match connect_system_adapter(args).await {
        Ok(client) => client,
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to connect to system adapter: {e}"));
        }
    };

    let run_id = uuid::Uuid::new_v4();
    let scenario_name = args.scenario.to_string();

    let serde_json::Value::Object(setup_map) = serde_json::json!({
        "executor_instance_type": args.executor_instance_type,
        "table_format": args.table_format.to_string(),
        "scenario": scenario_name,
        "etl_bucket": args.etl_bucket,
        "etl_prefix": args.etl_prefix,
        "etl_version": derived_version,
        "etl_region": args.etl_region,
        "etl_endpoint": args.etl_endpoint,
        "etl_sink": "adbc",
        "etl_type": version_metadata.etl_type().to_string(),
    }) else {
        unreachable!()
    };
    let mut setup_metadata: HashMap<String, serde_json::Value> = setup_map.into_iter().collect();

    if let Ok(system_under_test) = std::env::var("SYSTEM_UNDER_TEST") {
        setup_metadata.insert(
            "system_under_test".to_string(),
            serde_json::json!(system_under_test),
        );

        if let Some((prefix, variant)) = system_under_test.split_once('-') {
            setup_metadata.insert(
                "system_adapter_prefix".to_string(),
                serde_json::json!(prefix),
            );
            setup_metadata.insert(
                "system_adapter_variant".to_string(),
                serde_json::json!(variant),
            );
        } else {
            setup_metadata.insert(
                "system_adapter_prefix".to_string(),
                serde_json::json!(system_under_test),
            );
        }
    }

    let system_adapter_client = Arc::new(Mutex::new(system_adapter_client));
    let result = run_benchmark(
        args,
        Arc::clone(&system_adapter_client),
        run_id,
        setup_metadata,
        &version_metadata,
        file_storage,
    )
    .await;

    // After successful setup, always teardown even if there are errors in between,
    // unless --no-teardown was requested (e.g. to inspect cloud state after a run).
    if args.no_teardown {
        tracing::info!("Skipping teardown (--no-teardown flag is set).");
    } else if let Err(e) = system_adapter_client.lock().await.teardown(run_id).await {
        tracing::error!("Failed to teardown system adapter: {e}");
    }

    result
}

/// Generate a base64-encoded Arrow IPC stream for each dataset containing
/// `n_rows` zero-value rows. Used to bootstrap schema inference in adapters
/// (e.g. DynamoDB, MongoDB) that require pre-existing records before the
/// system under test can start.
fn generate_seed_data(
    datasets: &HashMap<String, system_adapter_protocol::DatasetConfig>,
    n_rows: usize,
) -> anyhow::Result<HashMap<String, String>> {
    use base64::Engine as _;
    datasets
        .iter()
        .map(|(name, config)| {
            let batch = make_zero_batch(&config.schema, n_rows)?;
            let ipc_bytes = batch_to_ipc_bytes(&batch)?;
            let encoded = base64::engine::general_purpose::STANDARD.encode(&ipc_bytes);
            Ok((name.clone(), encoded))
        })
        .collect()
}

fn make_zero_batch(
    schema: &arrow_schema::SchemaRef,
    n_rows: usize,
) -> anyhow::Result<arrow::record_batch::RecordBatch> {
    use arrow::array::*;
    use arrow::datatypes::DataType;
    use std::sync::Arc;

    let arrays = schema
        .fields()
        .iter()
        .map(|field| -> anyhow::Result<arrow::array::ArrayRef> {
            let arr: ArrayRef = match field.data_type() {
                DataType::Boolean => Arc::new(BooleanArray::from(vec![false; n_rows])),
                DataType::Int8 => Arc::new(Int8Array::from(vec![0i8; n_rows])),
                DataType::Int16 => Arc::new(Int16Array::from(vec![0i16; n_rows])),
                DataType::Int32 => Arc::new(Int32Array::from(vec![0i32; n_rows])),
                DataType::Int64 => Arc::new(Int64Array::from(vec![0i64; n_rows])),
                DataType::UInt8 => Arc::new(UInt8Array::from(vec![0u8; n_rows])),
                DataType::UInt16 => Arc::new(UInt16Array::from(vec![0u16; n_rows])),
                DataType::UInt32 => Arc::new(UInt32Array::from(vec![0u32; n_rows])),
                DataType::UInt64 => Arc::new(UInt64Array::from(vec![0u64; n_rows])),
                DataType::Float32 => Arc::new(Float32Array::from(vec![0.0f32; n_rows])),
                DataType::Float64 => Arc::new(Float64Array::from(vec![0.0f64; n_rows])),
                DataType::Utf8 => Arc::new(StringArray::from(vec![""; n_rows])),
                DataType::LargeUtf8 => Arc::new(LargeStringArray::from(vec![""; n_rows])),
                DataType::Utf8View => Arc::new(StringViewArray::from(vec![""; n_rows])),
                DataType::Date32 => Arc::new(Date32Array::from(vec![0i32; n_rows])),
                DataType::Date64 => Arc::new(Date64Array::from(vec![0i64; n_rows])),
                DataType::Timestamp(unit, tz) => {
                    use arrow::datatypes::TimeUnit;
                    let arr: ArrayRef = match unit {
                        TimeUnit::Second => Arc::new(
                            arrow::array::TimestampSecondArray::from(vec![0i64; n_rows])
                                .with_timezone_opt(tz.clone()),
                        ),
                        TimeUnit::Millisecond => Arc::new(
                            arrow::array::TimestampMillisecondArray::from(vec![0i64; n_rows])
                                .with_timezone_opt(tz.clone()),
                        ),
                        TimeUnit::Microsecond => Arc::new(
                            arrow::array::TimestampMicrosecondArray::from(vec![0i64; n_rows])
                                .with_timezone_opt(tz.clone()),
                        ),
                        TimeUnit::Nanosecond => Arc::new(
                            arrow::array::TimestampNanosecondArray::from(vec![0i64; n_rows])
                                .with_timezone_opt(tz.clone()),
                        ),
                    };
                    arr
                }
                DataType::Decimal128(p, s) => Arc::new(
                    Decimal128Array::from(vec![0i128; n_rows])
                        .with_precision_and_scale(*p, *s)
                        .map_err(|e| anyhow::anyhow!("invalid decimal128 precision/scale: {e}"))?,
                ),
                dt => anyhow::bail!("unsupported data type in seed batch: {dt}"),
            };
            Ok(arr)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(arrow::record_batch::RecordBatch::try_new(
        Arc::clone(schema),
        arrays,
    )?)
}

fn batch_to_ipc_bytes(batch: &arrow::record_batch::RecordBatch) -> anyhow::Result<Vec<u8>> {
    use arrow_ipc::writer::StreamWriter;
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(buf)
}
