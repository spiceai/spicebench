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

use clap::{Parser, ValueEnum};

/// Run a standalone ETL pipeline (S3/local → ADBC or null sink)
#[derive(Parser, Debug, Clone)]
pub struct EtlArgs {
    /// Scenario name (e.g. "tpch") — used in the storage path `{prefix}/{scenario}/{version}/`
    #[arg(long, default_value = "tpch")]
    pub scenario: String,

    /// Scale factor for the dataset. The version is derived automatically as
    /// `format_scale_factor(scale_factor)` (e.g. 1.0 → "1.0").
    #[arg(long, default_value_t = 1.0)]
    pub scale_factor: f64,

    /// Path to a local archive file (`.tar.zst`). When specified, the archive
    /// is extracted locally without downloading from S3.
    #[arg(long)]
    pub archive_file: Option<std::path::PathBuf>,

    /// Directory to extract the archive into. Defaults to a temporary directory.
    #[arg(long)]
    pub extract_dir: Option<std::path::PathBuf>,

    /// S3 bucket name (required unless --archive-file is specified)
    #[arg(long)]
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

    /// ETL sink target.
    ///
    /// - adbc: write via ADBC bulk ingest
    /// - null: discard all writes (throughput benchmark mode)
    #[arg(long, value_enum, default_value_t = EtlSinkType::Adbc)]
    pub sink: EtlSinkType,

    /// ADBC driver name (for example: "databricks" or "flightsql").
    /// Provide with `--adbc-uri` to write to an ADBC target.
    #[arg(long)]
    pub adbc_driver: Option<String>,

    /// Connection URI passed as ADBC database option `uri`.
    /// Provide with `--adbc-driver` to write to an ADBC target.
    #[arg(long)]
    pub adbc_uri: Option<String>,

    /// Optional target database catalog for ADBC bulk ingest inserts
    #[arg(long)]
    pub adbc_catalog: Option<String>,

    /// Optional target database schema for bulk ingest
    #[arg(long)]
    pub adbc_schema: Option<String>,

    /// When writing to an ADBC target, send PostgreSQL-compatible CREATE TABLE
    /// statements before ETL starts, based on dataset table schemas (including
    /// `__created_at`).
    #[arg(long, default_value_t = false)]
    pub adbc_create_tables: bool,

    /// Additional ADBC database options as `key=value`.
    ///
    /// May be specified multiple times.
    /// Example: `--adbc-option username=token --adbc-option password=...`
    #[arg(long = "adbc-option")]
    pub adbc_options: Vec<String>,
}

#[derive(Clone, Debug, Default, ValueEnum)]
pub enum EtlSinkType {
    #[default]
    #[value(name = "adbc")]
    Adbc,
    #[value(name = "null")]
    Null,
}
