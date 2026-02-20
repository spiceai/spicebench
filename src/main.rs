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

use std::sync::Arc;

use adbc_client::AdbcConnection;
use checkpointer::CheckpointStore;
use clap::Parser;
use data_generation::config::{TargetConfig, build_version_prefix};
use data_generation::storage::DataStorage;
use data_generation::storage::s3::S3Storage;
use data_generation::version::VersionMetadata;
use etl::sink::Sink;
use etl::sink::s3_hive::S3HiveSink;
use etl::{DatasetSource, ETLPipeline, PipelineState, StopReason};
use test_framework::{anyhow, rustls};
use tracing::Level;
use tracing_subscriber::EnvFilter;

mod args;
mod commands;
mod metrics;
mod scenario;

use crate::args::CommonArgs;
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

fn s3_hive_target_prefix(common: &CommonArgs, scenario_name: &str, run_id: uuid::Uuid) -> String {
    let mut target_prefix = common.etl_target_base_prefix.trim_matches('/').to_string();
    if target_prefix.is_empty() {
        target_prefix = "etl-hive-output".to_string();
    }
    format!("{target_prefix}/{scenario_name}/{run_id}")
}

async fn run_benchmark(
    common: &CommonArgs,
    system_adapter_client: &mut system_adapter_protocol::Client,
    run_id: uuid::Uuid,
    setup_response: system_adapter_protocol::SetupResponse,
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

    let read_driver_name = setup_response.read_driver.driver.to_string();
    let read_kwargs = setup_response.read_driver.db_kwargs;

    let dataset_source = DatasetSource::from_dataset_type(&version_metadata.dataset_type)?;
    let generation_config = version_metadata.dataset_config();
    let mutations = version_metadata.mutation_config();
    let data_source: Arc<dyn DataStorage> = source.clone();

    let target: Arc<dyn Sink> = {
        let hive_prefix = s3_hive_target_prefix(common, &scenario_name, run_id);
        let hive_config = TargetConfig {
            bucket: common.etl_bucket.clone(),
            prefix: hive_prefix.clone(),
            region: common.etl_region.clone(),
            endpoint: common.etl_endpoint.clone(),
        };
        let sink = S3HiveSink::new(&hive_config)?;
        println!(
            "S3 hive sink initialized at s3://{}/{}",
            common.etl_bucket, hive_prefix
        );
        Arc::new(sink)
    };

    let mut pipeline = ETLPipeline::new(
        dataset_source,
        &generation_config,
        Arc::clone(&data_source),
        target,
        &mutations,
    )?;

    let datasets = pipeline.create_tables_request_datasets();

    if let Err(e) = system_adapter_client.create_tables(run_id, datasets).await {
        pipeline.cancel();
        return Err(anyhow::anyhow!(
            "Failed to create tables via system adapter: {e}"
        ));
    }

    // --- Initialize: ETL the first batch so the target has data ---
    tracing::info!("Initializing ETL pipeline (first batch)...");
    pipeline.initialize().await?;
    tracing::info!("ETL pipeline initialized");

    let load_conn = match AdbcConnection::create(&read_driver_name, read_kwargs) {
        Ok(conn) => conn,
        Err(e) => {
            pipeline.cancel();
            return Err(anyhow::anyhow!(
                "Failed to create benchmark ADBC connection for driver {}: {e}",
                read_driver_name
            ));
        }
    };
    tracing::info!(
        "ADBC read connection established (driver: {})",
        read_driver_name
    );

    commands::load::run(
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

    // --- Connect to S3 and read version metadata ---
    let source_config = TargetConfig {
        bucket: cli.common.etl_bucket.clone(),
        prefix: build_version_prefix(
            &cli.common.etl_prefix,
            &cli.common.scenario.to_string(),
            &cli.common.etl_version,
        ),
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
    let mut system_adapter_client = match connect_system_adapter(&cli.common).await {
        Ok(system_adapter_client) => system_adapter_client,
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to connect to system adapter: {e}"));
        }
    };

    let run_id = uuid::Uuid::new_v4();
    let scenario_name = cli.common.scenario.to_string();

    let mut setup_metadata = std::collections::HashMap::new();
    setup_metadata.insert(
        "executor_instance_type".to_string(),
        serde_json::Value::String(cli.common.executor_instance_type.clone()),
    );
    setup_metadata.insert(
        "table_format".to_string(),
        serde_json::Value::String(cli.common.table_format.to_string()),
    );
    setup_metadata.insert(
        "etl_sink_mode".to_string(),
        serde_json::Value::String("s3-hive".to_string()),
    );
    setup_metadata.insert(
        "scenario".to_string(),
        serde_json::Value::String(scenario_name.clone()),
    );
    setup_metadata.insert(
        "etl_bucket".to_string(),
        serde_json::Value::String(cli.common.etl_bucket.clone()),
    );
    setup_metadata.insert(
        "etl_prefix".to_string(),
        serde_json::Value::String(cli.common.etl_prefix.clone()),
    );
    setup_metadata.insert(
        "etl_version".to_string(),
        serde_json::Value::String(cli.common.etl_version.clone()),
    );
    setup_metadata.insert(
        "etl_region".to_string(),
        cli.common
            .etl_region
            .as_ref()
            .map_or(serde_json::Value::Null, |v| {
                serde_json::Value::String(v.clone())
            }),
    );
    setup_metadata.insert(
        "etl_endpoint".to_string(),
        cli.common
            .etl_endpoint
            .as_ref()
            .map_or(serde_json::Value::Null, |v| {
                serde_json::Value::String(v.clone())
            }),
    );

    {
        let hive_prefix = s3_hive_target_prefix(&cli.common, &scenario_name, run_id);
        setup_metadata.insert(
            "etl_s3_hive_target_prefix".to_string(),
            serde_json::Value::String(hive_prefix.clone()),
        );
        setup_metadata.insert(
            "etl_s3_hive_uri".to_string(),
            serde_json::Value::String(format!("s3://{}/{}", cli.common.etl_bucket, hive_prefix)),
        );
    }

    let setup_response = match system_adapter_client.setup(run_id, setup_metadata).await {
        Ok(response) => response,
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to setup system adapter: {e}"));
        }
    };

    let result = run_benchmark(
        &cli.common,
        &mut system_adapter_client,
        run_id,
        setup_response,
        &version_metadata,
        source,
    )
    .await;

    // After successful setup, always teardown even if there are errors in between.
    if let Err(e) = system_adapter_client.teardown(run_id).await {
        tracing::error!("Failed to teardown system adapter: {e}");
    }

    result
}
