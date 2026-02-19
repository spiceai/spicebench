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

use std::{collections::HashMap, sync::Arc};

use adbc_client::AdbcConnection;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use checkpointer::CheckpointStore;
use clap::Parser;
use data_generation::config::{DatasetConfig as GenerationDatasetConfig, TargetConfig};
use data_generation::dataset::Dataset;
use data_generation::dataset::MutationConfig;
use data_generation::storage::DataStorage;
use data_generation::storage::s3::S3Storage;
use etl::sink::adbc::AdbcSink;
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
use crate::scenario::Scenario;

fn create_tables_request_datasets(
    dataset: &Arc<dyn Dataset>,
    with_created_at: bool,
) -> HashMap<String, system_adapter_protocol::DatasetConfig> {
    dataset
        .tables()
        .into_iter()
        .map(|(name, table)| {
            let schema = if with_created_at {
                let mut fields: Vec<_> = table.schema.fields().iter().cloned().collect();
                fields.push(Arc::new(Field::new(
                    "__created_at",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                )));
                Arc::new(Schema::new(fields))
            } else {
                table.schema.clone()
            };

            (name, system_adapter_protocol::DatasetConfig { schema })
        })
        .collect()
}

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

async fn run_benchmark(
    common: &CommonArgs,
    system_adapter_client: &mut system_adapter_protocol::Client,
    run_id: uuid::Uuid,
    adbc_driver: system_adapter_protocol::SetupResponse,
    dataset_source: DatasetSource,
    generation_config: &GenerationDatasetConfig,
    mutations: &MutationConfig,
    source: Arc<S3Storage>,
    datasets: HashMap<String, system_adapter_protocol::DatasetConfig>,
) -> anyhow::Result<()> {
    // --- Download checkpoints from S3 ---
    let scenario_name = common.scenario.to_string();
    let checkpoint_dir = tempfile::tempdir()?;

    let checkpoint_store = CheckpointStore::new(
        &common.etl_bucket,
        &common.etl_source_prefix,
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
            num_checkpoints = scenario_info.num_checkpoints,
            num_queries = scenario_info.num_queries,
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

    let driver_name = adbc_driver.driver.to_string();
    let sink_kwargs = adbc_driver.db_kwargs.clone();
    let load_kwargs = adbc_driver.db_kwargs;

    let adbc_conn = AdbcConnection::create(&driver_name, sink_kwargs).map_err(|e| {
        anyhow::anyhow!(
            "Failed to create ADBC connection for driver {}: {e}",
            driver_name
        )
    })?;
    println!("ADBC connection established (driver: {})", driver_name);

    let target = Arc::new(AdbcSink::new_without_table_creation(adbc_conn, None));
    let mut pipeline =
        ETLPipeline::new(dataset_source, generation_config, source, target, mutations)?
            .with_created_at(common.with_created_at);

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

    let load_conn = match AdbcConnection::create(&driver_name, load_kwargs) {
        Ok(conn) => conn,
        Err(e) => {
            pipeline.cancel();
            return Err(anyhow::anyhow!(
                "Failed to create benchmark ADBC connection for driver {}: {e}",
                driver_name
            ));
        }
    };

    commands::load::run(
        &common.scenario,
        common,
        load_conn,
        &mut pipeline,
        checkpoint_steps,
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

    // --- Construct the ETL pipeline ---
    let dataset_source = match &cli.common.scenario {
        Scenario::TPCH => DatasetSource::Tpch,
    };

    let generation_config = GenerationDatasetConfig {
        dataset_type: match &dataset_source {
            DatasetSource::Tpch => "tpch".to_string(),
            DatasetSource::SimpleSequence => "simple_sequence".to_string(),
        },
        scale_factor: cli.common.scale_factor,
        num_steps: cli.common.etl_num_steps,
    };

    let source_config = TargetConfig {
        bucket: cli.common.etl_bucket.clone(),
        prefix: cli.common.etl_source_prefix.clone(),
        region: cli.common.etl_region.clone(),
        endpoint: cli.common.etl_endpoint.clone(),
    };

    let source = Arc::new(S3Storage::new(&source_config)?);

    // --- Connect to the system adapter ---
    let mut system_adapter_client = match connect_system_adapter(&cli.common).await {
        Ok(system_adapter_client) => system_adapter_client,
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to connect to system adapter: {e}"));
        }
    };

    let run_id = uuid::Uuid::new_v4();
    let mutations = MutationConfig::new(0.0, 0.0);

    let setup_dataset = dataset_source.create(
        &generation_config,
        &mutations,
        Arc::clone(&source) as Arc<dyn DataStorage>,
    )?;
    let datasets = create_tables_request_datasets(&setup_dataset, cli.common.with_created_at);

    let setup_metadata = std::collections::HashMap::from([
        (
            "executor_instance_type".to_string(),
            serde_json::Value::String(cli.common.executor_instance_type.clone()),
        ),
        (
            "table_format".to_string(),
            serde_json::Value::String(cli.common.table_format.to_string()),
        ),
    ]);

    let adbc_driver = match system_adapter_client.setup(run_id, setup_metadata).await {
        Ok(response) => response,
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to setup system adapter: {e}"));
        }
    };

    let result = run_benchmark(
        &cli.common,
        &mut system_adapter_client,
        run_id,
        adbc_driver,
        dataset_source,
        &generation_config,
        &mutations,
        source,
        datasets,
    )
    .await;

    // After successful setup, always teardown even if there are errors in between.
    if let Err(e) = system_adapter_client.teardown(run_id).await {
        tracing::error!("Failed to teardown system adapter: {e}");
    }

    result
}
