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
use etl::sink::QuoteStyle;
use etl::sink::Sink;
use etl::sink::adbc::AdbcSink;
use etl::sink::iceberg::{IcebergObjectStoreConfig, IcebergSink};
use etl::{DatasetSource, ETLPipeline, PipelineState, StopReason};
use system_adapter_protocol::AdbcDriver;
use test_framework::{anyhow, rustls};
use tokio::sync::Mutex;
use tracing::Level;
use tracing_subscriber::EnvFilter;
mod args;
mod commands;
mod metrics;
mod scenario;

use crate::args::{CommonArgs, EtlSinkMode};
use crate::commands::connect_system_adapter;

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

fn iceberg_target_prefix(common: &CommonArgs, scenario_name: &str, run_id: uuid::Uuid) -> String {
    let mut target_prefix = common.etl_target_base_prefix.trim_matches('/').to_string();
    if target_prefix.is_empty() {
        target_prefix = "etl-iceberg-output".to_string();
    }
    format!("{target_prefix}/{scenario_name}/{run_id}")
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

    // --- Call setup with datasets to provision the SUT ---
    let setup_response = system_adapter_client
        .lock()
        .await
        .setup(
            run_id,
            setup_metadata,
            ETLPipeline::create_tables_request_datasets(
                false,
                dataset_source.create(&generation_config, &mutations, Arc::clone(&data_source))?,
            ),
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to setup system adapter: {e}"))?;

    let driver_name = setup_response.driver.to_string();
    let db_kwargs = setup_response.db_kwargs;
    let quote_style = match setup_response.driver {
        AdbcDriver::Databricks => QuoteStyle::Backtick,
        AdbcDriver::Flightsql => QuoteStyle::default(),
    };

    let target: Arc<dyn Sink> = match common.etl_sink_mode {
        EtlSinkMode::Adbc => {
            let adbc_conn =
                AdbcConnection::create(&driver_name, db_kwargs.clone()).map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to create ADBC connection for driver {driver_name}: {e}"
                    )
                })?;
            println!("ADBC sink connection established (driver: {driver_name})");
            Arc::new(
                AdbcSink::new_without_table_creation(adbc_conn, None).with_quote_style(quote_style),
            )
        }
        EtlSinkMode::IcebergObjectStore => {
            let iceberg_prefix = iceberg_target_prefix(common, &scenario_name, run_id);
            let sink = IcebergSink::new(IcebergObjectStoreConfig {
                warehouse_uri: format!("s3://{}/{}", common.etl_bucket, iceberg_prefix),
                namespace: vec!["spicebench".to_string(), "etl".to_string()],
                s3_region: common.etl_region.clone(),
                s3_endpoint: common.etl_endpoint.clone(),
            })
            .await?;
            println!(
                "Iceberg sink initialized at s3://{}/{}",
                common.etl_bucket, iceberg_prefix
            );
            Arc::new(sink)
        }
    };

    let mut pipeline = ETLPipeline::new(
        dataset_source,
        &generation_config,
        Arc::clone(&data_source),
        target,
        &mutations,
    )?
    .with_created_at(common.with_created_at);

    // --- Initialize: ETL the first batch so the target has data ---
    tracing::info!("Initializing ETL pipeline (first batch)...");
    pipeline.initialize().await?;
    tracing::info!("ETL pipeline initialized");

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
        etl_sink_mode = %cli.common.etl_sink_mode,
        table_format = %cli.common.table_format,
        scenario = %scenario_name,
        concurrency = cli.common.concurrency,
        with_created_at = cli.common.with_created_at,
        "ETL configuration"
    );

    let source_config = TargetConfig {
        bucket: cli.common.etl_bucket.clone(),
        prefix: version_prefix,
        region: cli.common.etl_region.clone(),
        endpoint: cli.common.etl_endpoint.clone(),
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
            "etl_sink_mode".to_string(),
            serde_json::Value::String(cli.common.etl_sink_mode.to_string()),
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
    ]);

    if matches!(cli.common.etl_sink_mode, EtlSinkMode::IcebergObjectStore) {
        let iceberg_prefix = iceberg_target_prefix(&cli.common, &scenario_name, run_id);
        setup_metadata.insert(
            "etl_iceberg_target_prefix".to_string(),
            serde_json::Value::String(iceberg_prefix.clone()),
        );
        setup_metadata.insert(
            "etl_iceberg_warehouse_uri".to_string(),
            serde_json::Value::String(format!("s3://{}/{}", cli.common.etl_bucket, iceberg_prefix)),
        );
        setup_metadata.insert(
            "etl_iceberg_namespace".to_string(),
            serde_json::Value::String("spicebench.etl".to_string()),
        );
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
