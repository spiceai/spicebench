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

use clap::{ArgAction, Parser, Subcommand, ValueEnum};

mod dataset;
use crate::scenario::Scenario;

pub mod checkpoint;
pub mod etl;
pub mod generate;

#[derive(Clone, Debug, ValueEnum)]
#[value(rename_all = "lower")]
pub enum TableFormat {
    Iceberg,
    Parquet,
    Delta,
}

#[derive(Clone, Debug, ValueEnum)]
#[value(rename_all = "lower")]
pub enum EtlSink {
    Adbc,
}

impl std::fmt::Display for TableFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Iceberg => "iceberg",
            Self::Parquet => "parquet",
            Self::Delta => "delta",
        };
        write!(f, "{value}")
    }
}

/// Top-level CLI with subcommands.
#[derive(Parser)]
#[command(author, version, about = "SpiceBench — benchmark for data & AI platforms", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Run the full benchmark lifecycle (setup → ETL + queries → teardown)
    Run(Box<RunArgs>),

    /// Generate a dataset archive and upload to S3 or write locally
    Generate(generate::GenerateArgs),

    /// Run a standalone ETL pipeline (S3/local → ADBC or null sink)
    Etl(etl::EtlArgs),

    /// Capture checkpoint query results at ETL boundaries for validation
    Checkpoint(checkpoint::CheckpointArgs),
}

/// Arguments for the `run` subcommand (full benchmark lifecycle).
#[derive(Parser, Debug, Clone)]
pub struct RunArgs {
    /// The scenario to use for the benchmark run, which determines the query set and other parameters.
    #[arg(long)]
    pub(crate) scenario: Scenario,

    /// The number of clients to run simultaneously.
    ///
    /// Each client runs query-set iterations independently and dispatches its
    /// queries asynchronously.
    #[arg(long, default_value = "2")]
    pub(crate) concurrency: usize,

    /// Executor instance type used for this run (for cross-run comparison and dashboarding).
    #[arg(long, default_value = "unknown")]
    pub(crate) executor_instance_type: String,

    /// Whether to collect SUT metrics via the system adapter JSON-RPC command.
    #[arg(long)]
    pub(crate) scrape_sut_metrics: bool,

    /// OTLP metrics collector endpoint (HTTP or gRPC). If unset, falls back to Arrow telemetry.
    #[arg(long)]
    pub(crate) otlp_endpoint: Option<String>,

    /// Additional OTLP headers in key=value form. Can be repeated.
    #[arg(long, value_parser = parse_key_val, action = ArgAction::Append, requires = "otlp_endpoint", value_name = "KEY=VALUE")]
    pub(crate) otlp_header: Vec<(String, String)>,

    /// Logical name for the system adapter connection.
    #[arg(long, default_value = "system_adapter", env = "SYSTEM_ADAPTER")]
    pub(crate) system_adapter_name: String,

    /// Optional suffix added as a separate `run_tag` metric label for differentiating
    /// runs in dashboards without changing the adapter_name dimension.
    #[arg(long, default_value = "", env = "RUN_TAG")]
    pub(crate) run_tag: String,

    /// How to execute when a system adapter transport is configured.
    /// - adapter-command: dispatch spicebench run as a JSON-RPC command (e.g. run.load)
    /// - direct-query: execute load/query path in spicebench directly (ADBC path)
    #[arg(long, value_enum, default_value = "adapter-command")]
    pub(crate) system_adapter_execution_mode: SystemAdapterExecutionMode,

    /// Command to run for a stdio JSON-RPC system adapter.
    #[arg(long, group = "system_adapter_option")]
    pub(crate) system_adapter_stdio_cmd: Option<String>,

    /// Space-delimited argument string passed to the stdio system adapter command.
    #[arg(long, requires = "system_adapter_stdio_cmd")]
    pub(crate) system_adapter_stdio_args: Option<String>,

    /// HTTP URL for a remote JSON-RPC system adapter.
    #[arg(
        long,
        conflicts_with = "system_adapter_stdio_cmd",
        group = "system_adapter_option"
    )]
    pub(crate) system_adapter_http_url: Option<String>,

    /// Additional system adapter parameters in key=value form. Can be repeated.
    /// Reserved for adapter-specific JSON-RPC usage.
    #[arg(long, value_parser = parse_key_val, action = ArgAction::Append, value_name = "KEY=VALUE", requires = "system_adapter_option")]
    pub(crate) system_adapter_param: Vec<(String, String)>,

    /// Environment variables for stdio system adapter in key=value form. Can be repeated.
    #[arg(long, value_parser = parse_key_val, action = ArgAction::Append, value_name = "KEY=VALUE", requires = "system_adapter_stdio_cmd")]
    pub(crate) system_adapter_env: Vec<(String, String)>,

    /// S3 bucket name for the ETL source and target
    #[arg(long, default_value = "spiceai-public-datasets")]
    pub(crate) etl_bucket: String,

    /// S3 key prefix (the `{prefix}` portion of `{prefix}/{scenario}/{version}/`)
    #[arg(long, default_value = "data-gen")]
    pub(crate) etl_prefix: String,

    /// TPC-H scale factor. The ETL version path segment is derived
    /// automatically as `format_scale_factor(scale_factor)` (e.g. 1.0 → "1.0").
    #[arg(long, default_value_t = 1.0)]
    pub(crate) scale_factor: f64,

    /// ETL sink implementation used for loading generated data.
    #[arg(long, value_enum, default_value = "adbc")]
    pub(crate) etl_sink: EtlSink,

    /// AWS region for the ETL S3 bucket
    #[arg(long, default_value = "us-east-1")]
    pub(crate) etl_region: Option<String>,

    /// S3 endpoint URL for the ETL bucket (for MinIO/LocalStack)
    #[arg(long)]
    pub(crate) etl_endpoint: Option<String>,

    /// Path to a locally generated data archive (e.g. from `spicebench generate --output-archive`).
    /// When set, skips the S3 download and uses this local .tar.zst file directly.
    /// --etl-bucket and --etl-endpoint are not required when this is set.
    #[arg(long)]
    pub(crate) etl_source_archive: Option<String>,

    /// Path to a local directory containing pre-generated checkpoints (checkpoints.json +
    /// checkpoints/ sub-tree). When set, skips the S3 checkpoint download.
    /// Combine with --validate-results for fully offline validation.
    #[arg(long)]
    pub(crate) checkpoint_local_dir: Option<String>,

    /// Table format propagated through ETL dataset metadata and adapters.
    #[arg(long, value_enum, default_value = "parquet")]
    pub(crate) table_format: TableFormat,

    /// Enable checkpoint-based results validation during load tests.
    ///
    /// When enabled and checkpoint data is available, the load runner will
    /// validate query results against pre-computed checkpoint snapshots at
    /// each ETL pause boundary.
    #[arg(long, default_value_t = false)]
    pub(crate) validate_results: bool,

    /// Period in seconds between checkpoint validation probe batches.
    ///
    /// During checkpoint validation, the first query in the scenario is
    /// dispatched at `--concurrency` copies every this many seconds.
    /// Overlapping batches allow more precise E2E latency measurement
    /// when individual query latency is high.
    #[arg(long, default_value_t = 5)]
    pub(crate) checkpoint_validation_period: u64,

    /// Maximum time in seconds to wait for a single checkpoint to converge
    /// before aborting the run with `validation_timeout`.
    #[arg(long, default_value_t = 600)]
    pub(crate) checkpoint_validation_timeout: u64,

    /// Skip the teardown RPC call to the system adapter after the benchmark completes.
    ///
    /// Useful when you want to inspect the system state after a run without
    /// triggering adapter-side cleanup (e.g. for debugging spice_cloud deployments).
    #[arg(long, default_value_t = false)]
    pub(crate) no_teardown: bool,

    /// Bootstrap mode: seed the base dataset into the source, then start the SUT
    /// (which snapshots the seeded data) before streaming the mutation workload.
    ///
    /// Requires an adapter that supports the `activate` RPC (e.g. spidapter
    /// mongodb-streams). The base load runs unthrottled; only the streaming phase
    /// is rate-limited (see `SPICEBENCH_SINK_MAX_RECORDS_PER_SEC`).
    #[arg(long, env = "SPICEBENCH_BOOTSTRAP", default_value_t = false)]
    pub(crate) bootstrap: bool,
}

fn parse_key_val(s: &str) -> Result<(String, String), String> {
    let pos = s
        .find('=')
        .ok_or_else(|| "expected KEY=VALUE formatted header".to_string())?;
    Ok((s[..pos].to_string(), s[pos + 1..].to_string()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SystemAdapterExecutionMode {
    AdapterCommand,
    DirectQuery,
}
