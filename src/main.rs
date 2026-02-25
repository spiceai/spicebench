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

use adbc_client::AdbcConnection;
use checkpointer::CheckpointStore;
use clap::Parser;
use data_generation::config::{TargetConfig, build_version_prefix};
use data_generation::storage::DataStorage;
use data_generation::storage::s3::S3Storage;
use data_generation::version::VersionMetadata;
use etl::sink::Sink;
use etl::sink::adbc::AdbcSink;
use etl::sink::s3_hive::S3HiveSink;
use etl::{DatasetSource, ETLPipeline, PipelineState, StopReason};
use test_framework::{anyhow, rustls};
use tokio::sync::Mutex;
use tracing::Level;
use tracing_subscriber::EnvFilter;
mod args;
mod commands;
mod metrics;
mod scenario;

use crate::args::{CommonArgs, EtlSink};
use crate::commands::connect_system_adapter;

const FLIGHTSQL_MAX_MSG_SIZE_OPTION: &str = "adbc.flight.sql.client_option.with_max_msg_size";
const DEFAULT_FLIGHTSQL_MAX_MSG_SIZE_BYTES: &str = "78643200";

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(flatten)]
    common: args::CommonArgs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum SystemAdapterExecutionMode {
    AdapterCommand,
    DirectQuery,
}

fn s3_hive_target_prefix(common: &CommonArgs, scenario_name: &str, run_id: uuid::Uuid) -> String {
    let mut target_prefix = common.etl_target_base_prefix.trim_matches('/').to_string();
    if target_prefix.is_empty() {
        target_prefix = "etl-hive-output".to_string();
    }
    format!("{target_prefix}/{scenario_name}/{run_id}")
}

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
    common: &CommonArgs,
    system_adapter_client: Arc<Mutex<system_adapter_protocol::Client>>,
    run_id: uuid::Uuid,
    setup_metadata: HashMap<String, serde_json::Value>,
    version_metadata: &VersionMetadata,
    source: Arc<S3Storage>,
) -> anyhow::Result<()> {
    // --- Download checkpoints from S3 ---
    let scenario_name = common.scenario.to_string();
    let checkpoint_dir = tempfile::tempdir()?;

    let version_prefix =
        build_version_prefix(&common.etl_prefix, &scenario_name, &common.etl_version);
    let checkpoint_store = CheckpointStore::new(
        &common.etl_bucket,
        &version_prefix,
        common.etl_region.as_deref(),
        common.etl_endpoint.as_deref(),
    )?;

    let manifest = checkpoint_store.download_manifest().await.map_err(|e| {
        tracing::warn!("Failed to download checkpoint manifest - results validation will not be enabled: {e}");
        e
    }).ok();
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
    let data_source: Arc<dyn DataStorage> = source.clone();

    let mut setup_response_for_run: Option<system_adapter_protocol::SetupResponse> = None;
    let etl_sink_type = match common.etl_sink {
        EtlSink::Hive => system_adapter_protocol::EtlSinkType::Hive,
        EtlSink::Adbc => system_adapter_protocol::EtlSinkType::Adbc,
    };

    let (target, target_config, target_kind, adbc_sink): (
        Arc<dyn Sink>,
        Option<TargetConfig>,
        &'static str,
        Option<Arc<AdbcSink>>,
    ) = match common.etl_sink {
        EtlSink::Hive => {
            let hive_prefix = s3_hive_target_prefix(common, &scenario_name, run_id);
            let hive_config = TargetConfig {
                bucket: common.etl_bucket.clone(),
                prefix: hive_prefix,
                region: common.etl_region.clone(),
                endpoint: common.etl_endpoint.clone(),
                partition_columns: common.etl_partition_by.clone(),
            };

            (
                Arc::new(S3HiveSink::new(&hive_config)?),
                Some(hive_config),
                "hive",
                None,
            )
        }
        EtlSink::Adbc => {
            let setup_hive_prefix = s3_hive_target_prefix(common, &scenario_name, run_id);
            let setup_hive_config = TargetConfig {
                bucket: common.etl_bucket.clone(),
                prefix: setup_hive_prefix,
                region: common.etl_region.clone(),
                endpoint: common.etl_endpoint.clone(),
                partition_columns: common.etl_partition_by.clone(),
            };

            let setup_pipeline = ETLPipeline::new(
                dataset_source.clone(),
                &generation_config,
                Arc::clone(&data_source),
                Arc::new(S3HiveSink::new(&setup_hive_config)?),
                &mutations,
            )?
            .with_target_config(setup_hive_config);

            let setup_response = system_adapter_client
                .lock()
                .await
                .setup(
                    run_id,
                    setup_metadata.clone(),
                    setup_pipeline.create_tables_request_datasets(),
                    Some(etl_sink_type),
                )
                .await
                .map_err(|e| anyhow::anyhow!("Failed to setup system adapter: {e}"))?;

            let driver_name = setup_response.driver.to_string();
            let mut db_kwargs = setup_response.db_kwargs.clone();

            if !db_kwargs.contains_key("uri") {
                anyhow::bail!(
                    "No ADBC URI available for --etl-sink adbc. Ensure adapter setup returns db_kwargs.uri"
                );
            }

            if driver_name.eq_ignore_ascii_case("flightsql") {
                db_kwargs
                    .entry(FLIGHTSQL_MAX_MSG_SIZE_OPTION.to_string())
                    .or_insert_with(|| {
                        serde_json::Value::String(DEFAULT_FLIGHTSQL_MAX_MSG_SIZE_BYTES.to_string())
                    });
            }

            let (target_db_catalog, target_db_schema) =
                infer_adbc_target_namespace(setup_response.catalog_namespace.as_deref());

            let adbc_sink = Arc::new(AdbcSink::new(
                &driver_name,
                db_kwargs,
                target_db_catalog,
                target_db_schema,
            )?);

            setup_response_for_run = Some(setup_response);

            (
                adbc_sink.clone() as Arc<dyn Sink>,
                None,
                "adbc",
                Some(adbc_sink),
            )
        }
    };

    let mut pipeline = ETLPipeline::new(
        dataset_source,
        &generation_config,
        Arc::clone(&data_source),
        target,
        &mutations,
    )?;

    if let Some(target_config) = target_config {
        pipeline = pipeline.with_target_config(target_config);
    }

    if let Some(adbc_sink) = &adbc_sink {
        adbc_sink.create_tables_from_dataset_configs(&pipeline.create_tables_request_datasets())?;
    }

    tracing::info!(etl_sink = %target_kind, "Selected ETL sink");

    // --- Initialize: ETL the first batch so the target has data ---
    tracing::info!("Initializing ETL pipeline (first batch)...");
    pipeline.initialize().await?;
    tracing::info!("ETL pipeline initialized");

    // --- Call setup with datasets to provision the SUT ---
    let setup_response = if let Some(setup_response) = setup_response_for_run {
        setup_response
    } else {
        system_adapter_client
            .lock()
            .await
            .setup(
                run_id,
                setup_metadata,
                pipeline.create_tables_request_datasets(),
                Some(etl_sink_type),
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to setup system adapter: {e}"))?
    };

    let driver_name = setup_response.driver.to_string();
    let query_catalog_namespace = setup_response.catalog_namespace.clone();
    let db_kwargs = setup_response.db_kwargs;

    let load_conn = match AdbcConnection::create(&driver_name, db_kwargs) {
        Ok(conn) => conn,
        Err(e) => {
            pipeline.cancel();
            return Err(anyhow::anyhow!(
                "Failed to create benchmark ADBC connection for driver {driver_name}: {e}"
            ));
        }
    };
    tracing::info!("ADBC read connection established (driver: {driver_name})");

    commands::load::run(
        system_adapter_client,
        run_id,
        &common.scenario,
        common,
        load_conn,
        &mut pipeline,
        checkpoint_steps,
        Some(checkpoint_dir.path()),
        query_catalog_namespace,
    )
    .await?;

    // --- Wait for ETL to finish ---
    // If checkpoint_steps was set, the load runner already handled
    // the pause/resume loop internally, so the pipeline should be
    // in a stopped state by now. If it was started without checkpoints
    // (.start()), the pipeline may still be running.
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );

    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    let scenario_name = cli.common.scenario.to_string();
    let version_prefix = build_version_prefix(
        &cli.common.etl_prefix,
        &scenario_name,
        &cli.common.etl_version,
    );
    tracing::info!(
        etl_source = %format!("s3://{}/{}/tables/", cli.common.etl_bucket, version_prefix),
        etl_bucket = %cli.common.etl_bucket,
        etl_prefix = %cli.common.etl_prefix,
        etl_version = %cli.common.etl_version,
        etl_region = ?cli.common.etl_region,
        table_format = %cli.common.table_format,
        etl_sink = ?cli.common.etl_sink,
        scenario = %scenario_name,
        concurrency = cli.common.concurrency,
        "ETL configuration"
    );

    let source_config = TargetConfig {
        bucket: cli.common.etl_bucket.clone(),
        prefix: version_prefix,
        region: cli.common.etl_region.clone(),
        endpoint: cli.common.etl_endpoint.clone(),
        partition_columns: vec![],
    };

    let source = Arc::new(S3Storage::new(&source_config)?);

    // Read version metadata to derive dataset config and mutations.
    let version_metadata = source.read_version_metadata().await?.ok_or_else(|| {
        anyhow::anyhow!(
            "No version.json found at {}. Was data generation run for this version?",
            source_config.prefix,
        )
    })?;

    // --- Connect to the system adapter ---
    let system_adapter_client = match connect_system_adapter(&cli.common).await {
        Ok(system_adapter_client) => system_adapter_client,
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to connect to system adapter: {e}"));
        }
    };

    let run_id = uuid::Uuid::new_v4();
    let scenario_name = cli.common.scenario.to_string();

    let mut setup_metadata: std::collections::HashMap<String, serde_json::Value> = HashMap::from([
        (
            "executor_instance_type".to_string(),
            serde_json::Value::String(cli.common.executor_instance_type.clone()),
        ),
        (
            "table_format".to_string(),
            serde_json::Value::String(cli.common.table_format.to_string()),
        ),
        (
            "scenario".to_string(),
            serde_json::Value::String(scenario_name.clone()),
        ),
        (
            "etl_bucket".to_string(),
            serde_json::Value::String(cli.common.etl_bucket.clone()),
        ),
        (
            "etl_prefix".to_string(),
            serde_json::Value::String(cli.common.etl_prefix.clone()),
        ),
        (
            "etl_version".to_string(),
            serde_json::Value::String(cli.common.etl_version.clone()),
        ),
        (
            "etl_region".to_string(),
            cli.common
                .etl_region
                .as_ref()
                .map_or(serde_json::Value::Null, |v| {
                    serde_json::Value::String(v.clone())
                }),
        ),
        (
            "etl_endpoint".to_string(),
            cli.common
                .etl_endpoint
                .as_ref()
                .map_or(serde_json::Value::Null, |v| {
                    serde_json::Value::String(v.clone())
                }),
        ),
        (
            "etl_sink".to_string(),
            serde_json::Value::String(match cli.common.etl_sink {
                EtlSink::Hive => "hive".to_string(),
                EtlSink::Adbc => "adbc".to_string(),
            }),
        ),
    ]);

    {
        if matches!(cli.common.etl_sink, EtlSink::Hive) {
            let hive_prefix = s3_hive_target_prefix(&cli.common, &scenario_name, run_id);
            setup_metadata.insert(
                "etl_s3_hive_target_prefix".to_string(),
                serde_json::Value::String(hive_prefix.clone()),
            );
            setup_metadata.insert(
                "etl_s3_hive_uri".to_string(),
                serde_json::Value::String(format!(
                    "s3://{}/{}",
                    cli.common.etl_bucket, hive_prefix
                )),
            );
        }
    }

    if let Ok(system_under_test) = std::env::var("SYSTEM_UNDER_TEST") {
        setup_metadata.insert(
            "system_under_test".to_string(),
            serde_json::Value::String(system_under_test.clone()),
        );

        if let Some((prefix, variant)) = system_under_test.split_once('-') {
            setup_metadata.insert(
                "system_adapter_prefix".to_string(),
                serde_json::Value::String(prefix.to_string()),
            );
            setup_metadata.insert(
                "system_adapter_variant".to_string(),
                serde_json::Value::String(variant.to_string()),
            );
        } else {
            setup_metadata.insert(
                "system_adapter_prefix".to_string(),
                serde_json::Value::String(system_under_test),
            );
        }
    }

    let system_adapter_client = Arc::new(Mutex::new(system_adapter_client));
    let result = run_benchmark(
        &cli.common,
        Arc::clone(&system_adapter_client),
        run_id,
        setup_metadata,
        &version_metadata,
        source,
    )
    .await;

    // After successful setup, always teardown even if there are errors in between.
    if let Err(e) = system_adapter_client.lock().await.teardown(run_id).await {
        tracing::error!("Failed to teardown system adapter: {e}");
    }

    result
}
