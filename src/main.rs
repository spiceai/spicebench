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

use clap::Parser;
use test_framework::{anyhow, rustls};

mod args;
mod commands;
mod health;
mod metrics;
mod spiced_metrics;

use args::Commands;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    subcommand: Commands,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let raw_cli_args: Vec<String> = std::env::args().skip(1).collect();
    let cli = Cli::parse();

    match cli.subcommand {
        Commands::Run(args) => {
            if commands::maybe_dispatch_run_to_system_adapter(&raw_cli_args, &args.test_args.common)
                .await?
            {
                return Ok(());
            }

            commands::load::run(&args).await?
        }
    }

    Ok(())
}
