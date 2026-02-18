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

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(about = "Spice.ai data generation tool - generates Arrow data and writes to S3")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Seed the target with a small initial batch of data (10 rows)
    Initialize(CommonArgs),
    /// Run the full data generation pipeline from scratch
    Run(RunArgs),
}

#[derive(Parser, Clone)]
pub struct RunArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    /// Skip records that would be written during initialization (use after running `initialize` separately)
    #[arg(long, default_value_t = false)]
    pub skip_initial: bool,
}

#[derive(Parser, Clone)]
pub struct CommonArgs {
    /// Dataset type
    #[arg(long, default_value = "tpch")]
    pub dataset: String,

    /// TPC-H scale factor
    #[arg(long, default_value_t = 1.0)]
    pub scale_factor: f64,

    /// Number of data generation steps (partitions for TPC-H dbgen)
    #[arg(long, default_value_t = 25)]
    pub num_steps: u16,

    /// S3 bucket name
    #[arg(long)]
    pub bucket: String,

    /// S3 key prefix for generated files
    #[arg(long, default_value = "")]
    pub prefix: String,

    /// Logical table format propagated to system adapters
    #[arg(long, value_enum, default_value = "parquet")]
    pub table_format: TableFormat,

    /// Executor instance type label propagated to adapters for dashboarding
    #[arg(long, default_value = "unknown")]
    pub executor_instance_type: String,

    /// AWS region
    #[arg(long)]
    pub region: Option<String>,

    /// S3 endpoint URL (for MinIO/LocalStack)
    #[arg(long)]
    pub endpoint: Option<String>,

    /// Maximum number of concurrent S3 writes
    #[arg(long, default_value_t = 16)]
    pub max_concurrency: usize,
}

pub struct DatasetConfig {
    pub dataset_type: String,
    pub scale_factor: f64,
    pub num_steps: u16,
}

pub struct TargetConfig {
    pub bucket: String,
    pub prefix: String,
    pub table_format: TableFormat,
    pub executor_instance_type: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
}

#[derive(Clone, Debug, ValueEnum)]
#[value(rename_all = "lower")]
pub enum TableFormat {
    Iceberg,
    Parquet,
    Delta,
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

pub struct IngestorConfig {
    pub max_concurrency: usize,
}

impl CommonArgs {
    pub fn dataset_config(&self) -> DatasetConfig {
        DatasetConfig {
            dataset_type: self.dataset.clone(),
            scale_factor: self.scale_factor,
            num_steps: self.num_steps,
        }
    }

    pub fn target_config(&self) -> TargetConfig {
        TargetConfig {
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
            table_format: self.table_format.clone(),
            executor_instance_type: self.executor_instance_type.clone(),
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
        }
    }

    pub fn ingestor_config(&self) -> IngestorConfig {
        IngestorConfig {
            max_concurrency: self.max_concurrency,
        }
    }
}
