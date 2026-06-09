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

use crate::scenario::Scenario;

/// Capture checkpoint query results at ETL boundaries for validation
#[derive(Parser, Debug, Clone)]
pub struct CheckpointArgs {
    /// The scenario to run, which determines the dataset type and checkpoint queries.
    #[arg(long, value_enum, default_value = "tpch")]
    pub scenario: Scenario,

    /// Version identifier for the data generation to read from.
    #[arg(long)]
    pub version: String,

    /// S3 bucket name (used for both source and target). Not required when --etl-source-archive is set.
    #[arg(long, required_unless_present = "etl_source_archive")]
    pub bucket: Option<String>,

    /// S3 key prefix (the `{prefix}` portion of `{prefix}/{scenario}/{version}/`)
    #[arg(long, default_value = "")]
    pub prefix: String,

    /// AWS region
    #[arg(long)]
    pub region: Option<String>,

    /// S3 endpoint URL (for MinIO/LocalStack)
    #[arg(long)]
    pub endpoint: Option<String>,

    /// Path to the local DuckDB database file to sink data into
    #[arg(long)]
    pub duckdb_path: std::path::PathBuf,

    /// Every N steps to take a checkpoint
    #[arg(long, default_value_t = 100)]
    pub checkpoint_interval_steps: u64,

    /// Directory to write checkpoint parquet files into
    #[arg(long, default_value = "./checkpoints")]
    pub checkpoint_dir: std::path::PathBuf,

    /// Path to a locally generated data archive. When set, skips S3 download.
    #[arg(long)]
    pub etl_source_archive: Option<String>,
}
