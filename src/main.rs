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
use tracing::Level;
use tracing_subscriber::EnvFilter;

mod args;
mod commands;
mod metrics;
mod scenario;

use crate::args::{Cli, Command};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    match cli.command {
        Command::Run(args) => commands::run::execute(&args).await,
        Command::Generate(args) => commands::generate::execute(&args).await,
        Command::Etl(args) => commands::etl_cmd::execute(&args).await,
        Command::Checkpoint(args) => commands::checkpoint::execute(&args).await,
    }
}
