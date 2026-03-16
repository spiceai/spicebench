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

/// Generate a dataset archive and upload to S3 or write locally
#[derive(Parser, Debug, Clone)]
pub struct GenerateArgs {
    /// Dataset type
    #[arg(long, default_value = "tpch")]
    pub dataset: String,

    /// TPC-H scale factor
    #[arg(long, default_value_t = 1.0)]
    pub scale_factor: f64,

    /// Number of data generation steps (partitions for TPC-H dbgen)
    #[arg(long, default_value_t = 25)]
    pub num_steps: u16,

    /// Scenario name (e.g. "tpch") — used as `{scenario}` in the storage path `{prefix}/{scenario}/{version}/`
    #[arg(long, default_value = "tpch")]
    pub scenario: String,

    /// Write the generated archive to this local path instead of uploading to S3.
    /// When specified, --bucket and S3 options are not required.
    #[arg(long)]
    pub output_archive: Option<String>,

    /// S3 bucket name (required unless --output-archive is specified)
    #[arg(long)]
    pub bucket: Option<String>,

    /// S3 key prefix for generated files (the `{prefix}` portion of the path)
    #[arg(long, default_value = "")]
    pub prefix: String,

    /// AWS region
    #[arg(long)]
    pub region: Option<String>,

    /// S3 endpoint URL (for MinIO/LocalStack)
    #[arg(long)]
    pub endpoint: Option<String>,

    /// Ratio of update mutations per batch (0.0 to 1.0)
    #[arg(long, default_value_t = 0.0)]
    pub update_ratio: f64,

    /// Ratio of delete mutations per batch (0.0 to 1.0)
    #[arg(long, default_value_t = 0.0)]
    pub delete_ratio: f64,
}
