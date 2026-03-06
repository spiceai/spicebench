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

use clap::{ArgAction, Parser, ValueEnum};

mod dataset;
use crate::scenario::Scenario;

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
    Hive,
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

/// Arguments Common to all [`TestCommands`].
#[derive(Parser, Debug, Clone)]
pub struct CommonArgs {
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

    /// Version identifier for the data generation to read from.
    #[arg(long, default_value = "1")]
    pub(crate) etl_version: String,

    /// Base S3 key prefix for the ETL target (rehydrated) data.
    /// A random suffix is appended automatically to create a unique destination per run.
    #[arg(long, default_value = "")]
    pub(crate) etl_target_base_prefix: String,

    /// ETL sink implementation used for loading generated data.
    #[arg(long, value_enum, default_value = "hive")]
    pub(crate) etl_sink: EtlSink,

    /// AWS region for the ETL S3 bucket
    #[arg(long, default_value = "us-east-1")]
    pub(crate) etl_region: Option<String>,

    /// S3 endpoint URL for the ETL bucket (for MinIO/LocalStack)
    #[arg(long)]
    pub(crate) etl_endpoint: Option<String>,

    /// S3 URI for shared scheduler state (e.g. `s3://bucket/scheduler-state/`).
    /// Passed to the system adapter as `scheduler_state_location` metadata.
    #[arg(long)]
    pub(crate) scheduler_state_location: Option<String>,

    /// Ordered list of columns used for hive-style partitioning of ETL output.
    ///
    /// Example: `--etl-partition-by __created_at,product_type`
    #[arg(long, value_delimiter = ',', default_value = "__created_at")]
    pub(crate) etl_partition_by: Vec<String>,

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
