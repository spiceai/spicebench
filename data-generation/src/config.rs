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

use clap::{Parser, Subcommand};

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
    Run(CommonArgs),
}

#[derive(Parser, Clone)]
pub struct CommonArgs {
    /// Source type
    #[arg(long, default_value = "duckdb")]
    pub source_type: String,

    /// TPC-H scale factor
    #[arg(long, default_value_t = 1.0)]
    pub scale_factor: f64,

    /// Number of rows per batch
    #[arg(long, default_value_t = 10_000)]
    pub batch_size: usize,

    /// Total number of batches to generate (omit for unlimited)
    #[arg(long)]
    pub total_batches: Option<u64>,

    /// S3 bucket name
    #[arg(long)]
    pub bucket: String,

    /// S3 key prefix for generated files
    #[arg(long, default_value = "")]
    pub prefix: String,

    /// AWS region
    #[arg(long)]
    pub region: Option<String>,

    /// S3 endpoint URL (for MinIO/LocalStack)
    #[arg(long)]
    pub endpoint: Option<String>,

    /// Maximum number of concurrent S3 writes
    #[arg(long, default_value_t = 8)]
    pub max_concurrency: usize,
}

pub struct SourceConfig {
    pub source_type: String,
    pub scale_factor: f64,
    pub batch_size: usize,
    pub total_batches: Option<u64>,
}

pub struct TargetConfig {
    pub bucket: String,
    pub prefix: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
}

pub struct IngestorConfig {
    pub max_concurrency: usize,
}

impl CommonArgs {
    pub fn source_config(&self) -> SourceConfig {
        SourceConfig {
            source_type: self.source_type.clone(),
            scale_factor: self.scale_factor,
            batch_size: self.batch_size,
            total_batches: self.total_batches,
        }
    }

    pub fn target_config(&self) -> TargetConfig {
        TargetConfig {
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
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
