/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Archive creation and extraction for data generation artifacts.
//!
//! Data generation writes individual Parquet files and a `version.json` to a
//! local directory. This module packages that directory into a single
//! `.tar.zst` archive for upload to object storage and extracts such archives
//! for consumption by the ETL pipeline.

use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

/// Default archive file name used within a version prefix.
pub const ARCHIVE_FILENAME: &str = "data.tar.zst";

/// Default zstd compression level.
const ZSTD_COMPRESSION_LEVEL: i32 = 3;

/// Creates a `.tar.zst` archive from the contents of `source_dir`.
///
/// The archive preserves the directory structure relative to `source_dir`.
/// After extraction, the directory layout will match the original
/// `FileStorage` layout:
///
/// ```text
/// version.json
/// tables/{table_name}/batch-000000.parquet
/// tables/{table_name}/batch-000001.parquet
/// …
/// ```
pub fn create_archive(source_dir: &Path, archive_path: &Path) -> anyhow::Result<()> {
    if let Some(parent) = archive_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = File::create(archive_path)?;
    let writer = BufWriter::new(file);
    let encoder = zstd::Encoder::new(writer, ZSTD_COMPRESSION_LEVEL)?;
    let mut tar_builder = tar::Builder::new(encoder);
    tar_builder.append_dir_all(".", source_dir)?;

    let encoder = tar_builder.into_inner()?;
    encoder.finish()?;

    let meta = std::fs::metadata(archive_path)?;
    tracing::info!(
        source_dir = %source_dir.display(),
        archive = %archive_path.display(),
        bytes = meta.len(),
        mb = format!("{:.2}", meta.len() as f64 / 1_048_576.0),
        "Archive created"
    );
    Ok(())
}

/// Extracts a `.tar.zst` archive to `target_dir`.
///
/// The target directory is created if it does not exist. After extraction the
/// directory will contain the same layout that was archived by
/// [`create_archive`].
pub fn extract_archive(archive_path: &Path, target_dir: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(target_dir)?;

    let file = File::open(archive_path)?;
    let reader = BufReader::new(file);
    let decoder = zstd::Decoder::new(reader)?;
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(target_dir)?;

    tracing::info!(
        archive = %archive_path.display(),
        target_dir = %target_dir.display(),
        "Archive extracted"
    );
    Ok(())
}
