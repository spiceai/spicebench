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

use adbc_client::AdbcConnection;
use clap::Parser;
use test_framework::{anyhow, rustls};
use uuid::Uuid;

mod args;
mod commands;
mod metrics;
mod scenario;

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let cli = Cli::parse();

    let mut system_adapter_client = match connect_system_adapter(&cli.common).await {
        Ok(system_adapter_client) => system_adapter_client,
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to connect to system adapter: {e}"));
        }
    };

    let run_id = Uuid::new_v4();
    let datasets: std::collections::HashMap<String, system_adapter_protocol::DatasetConfig> =
        [].into_iter().collect();

    if let Err(e) = system_adapter_client.setup(run_id, datasets).await {
        return Err(anyhow::anyhow!("Failed to setup system adapter: {e}"));
    }

    let adbc_driver = match system_adapter_client.query_method(run_id).await {
        Ok(method) => method,
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to query system adapter: {e}"));
        }
    };

    let adbc_conn: Option<AdbcConnection> =
        match AdbcConnection::create(&adbc_driver.driver.to_string(), adbc_driver.db_kwargs) {
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
        return Err(anyhow::anyhow!("ADBC connection is required to run benchmarks"));
    };

    commands::load::run(&cli.common.scenario, &cli.common, adbc_conn).await?;

    if let Err(e) = system_adapter_client.teardown(run_id).await {
        return Err(anyhow::anyhow!("Failed to teardown system adapter: {e}"));
    }

    Ok(())
}
