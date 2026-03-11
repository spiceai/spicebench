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
    /// Run the full data generation pipeline from scratch
    Run(CommonArgs),
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

pub struct DatasetConfig {
    pub dataset_type: String,
    pub scale_factor: f64,
    pub num_steps: u16,
}

#[derive(Clone)]
pub struct TargetConfig {
    pub bucket: String,
    /// The fully-qualified prefix: `{prefix}/{scenario}/{version}`
    pub prefix: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    /// Ordered list of columns used for hive-style partitioning.
    ///
    /// Path segments are created in this same order, e.g. `a=.../b=...`.
    pub partition_columns: Vec<String>,
}

impl CommonArgs {
    pub fn dataset_config(&self) -> DatasetConfig {
        DatasetConfig {
            dataset_type: self.dataset.clone(),
            scale_factor: self.scale_factor,
            num_steps: self.num_steps,
        }
    }

    /// Returns the derived version string from the scale factor.
    ///
    /// The version is `format_scale_factor(scale_factor)`, e.g. `"1.0"`.
    pub fn derived_version(&self) -> String {
        format_scale_factor(self.scale_factor)
    }

    /// Builds the target config with the version-based storage path.
    ///
    /// The resulting prefix is `{prefix}/{scenario}/{version}` where
    /// version is derived from the scale factor.
    ///
    /// Requires `--bucket` to be specified; returns an error otherwise.
    pub fn target_config(&self) -> anyhow::Result<TargetConfig> {
        let bucket = self
            .bucket
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("--bucket is required for S3 storage"))?;
        let version = self.derived_version();
        let prefix = build_version_prefix(&self.prefix, &self.scenario, &version);
        Ok(TargetConfig {
            bucket: bucket.clone(),
            prefix,
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
            partition_columns: vec![],
        })
    }
}

/// Builds the versioned storage prefix: `{prefix}/{scenario}/{version}`.
///
/// If `prefix` is empty, the result is `{scenario}/{version}`.
pub fn build_version_prefix(prefix: &str, scenario: &str, version: &str) -> String {
    if prefix.is_empty() {
        format!("{scenario}/{version}")
    } else {
        format!("{prefix}/{scenario}/{version}")
    }
}

/// Formats a scale factor for use in S3 key paths.
///
/// Uses Rust's default `Display` formatting which preserves all significant
/// digits (e.g. `0.01` stays `"0.01"`), then appends `.0` for whole numbers
/// so that `1` becomes `"1.0"` to match github workflow values.
pub fn format_scale_factor(sf: f64) -> String {
    let s = format!("{sf}");
    if s.contains('.') { s } else { format!("{s}.0") }
}
