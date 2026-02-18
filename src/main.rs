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
use clap::Parser;
use data_generation::config::{DatasetConfig as GenerationDatasetConfig, TargetConfig};
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

use crate::commands::connect_system_adapter;
use crate::scenario::Scenario;

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

    // --- Query method from system adapter ---
    let adbc_driver = match system_adapter_client.query_method(run_id).await {
        Ok(method) => method,
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to query system adapter: {e}"));
        }
    };

    let driver_name = adbc_driver.driver.to_string();
    let sink_kwargs = adbc_driver.db_kwargs.clone();
    let load_kwargs = adbc_driver.db_kwargs;

    let adbc_conn: Option<AdbcConnection> = match AdbcConnection::create(&driver_name, sink_kwargs)
    {
        Ok(conn) => {
            println!(
                "ADBC connection established (driver: {})",
                adbc_driver.driver
            );
            Some(conn)
        }
        Err(e) => {
            eprintln!(
                "Failed to create ADBC connection for driver {}: {e}",
                adbc_driver.driver
            );
            None
        }
    };

    let Some(adbc_conn) = adbc_conn else {
        return Err(anyhow::anyhow!(
            "ADBC connection is required to run benchmarks"
        ));
    };

    let target = Arc::new(AdbcSink::new(adbc_conn, None));
    let mut pipeline = ETLPipeline::new(dataset_source, &generation_config, source, target)?;

    // --- Initialize: ETL the first batch so the target has data ---
    tracing::info!("Initializing ETL pipeline (first batch)...");
    pipeline.initialize().await?;
    tracing::info!("ETL pipeline initialized");

    // --- Setup the system adapter after initial data load ---
    let datasets = pipeline.setup_request_datasets();
    let setup_metadata = std::collections::HashMap::from([(
        "executor_instance_type".to_string(),
        serde_json::Value::String(cli.common.executor_instance_type.clone()),
    )]);

    if let Err(e) = system_adapter_client
        .setup(run_id, datasets, setup_metadata)
        .await
    {
        pipeline.cancel();
        return Err(anyhow::anyhow!("Failed to setup system adapter: {e}"));
    }

    let load_conn = match AdbcConnection::create(&driver_name, load_kwargs) {
        Ok(conn) => conn,
        Err(e) => {
            pipeline.cancel();
            return Err(anyhow::anyhow!(
                "Failed to create benchmark ADBC connection for driver {}: {e}",
                adbc_driver.driver
            ));
        }
    };

    commands::load::run(&cli.common.scenario, &cli.common, load_conn, &mut pipeline).await?;

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

    if let Err(e) = system_adapter_client.teardown(run_id).await {
        return Err(anyhow::anyhow!("Failed to teardown system adapter: {e}"));
    }

    Ok(())
}
