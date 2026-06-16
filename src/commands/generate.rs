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

use std::path::PathBuf;
use std::sync::Arc;

use data_generation::archive;
use data_generation::config::{DatasetConfig, TargetConfig, format_scale_factor};
use data_generation::dataset::{Dataset, MutationConfig};
use data_generation::generator::{DataGenerator, VersionConfig};
use data_generation::metrics::{IngestResult, Metrics};
use data_generation::storage::DataStorage;
use data_generation::storage::file::FileStorage;
use data_generation::storage::s3::S3Storage;
use test_framework::anyhow;

use crate::args::generate::GenerateArgs;

fn print_summary(result: &IngestResult) {
    println!("  Duration:          {:?}", result.elapsed);
    println!("  Batches generated: {}", result.batches_generated);
    println!("  Batches written:   {}", result.batches_written);
    println!("  Rows written:      {}", result.rows_written);
    println!(
        "  Bytes written:     {} ({:.2} MB)",
        result.bytes_written,
        result.bytes_written as f64 / 1_048_576.0
    );
    println!("  Write errors:      {}", result.write_errors);
    println!("  Throughput:        {:.0} rows/sec", result.rows_per_sec);
    println!(
        "                     {:.1} batches/sec",
        result.batches_per_sec
    );
    println!(
        "                     {:.2} MB/sec",
        result.bytes_per_sec / 1_048_576.0
    );
    println!("  Avg write latency: {:?}", result.avg_write_latency);
}

fn build_version_prefix(prefix: &str, scenario: &str, version: &str) -> String {
    if prefix.is_empty() {
        format!("{scenario}/{version}")
    } else {
        format!("{prefix}/{scenario}/{version}")
    }
}

pub async fn execute(args: &GenerateArgs) -> anyhow::Result<()> {
    let dataset_config = DatasetConfig {
        dataset_type: args.dataset.clone(),
        scale_factor: args.scale_factor,
        num_steps: args.num_steps,
    };
    let version = format_scale_factor(args.scale_factor);

    tracing::info!(
        dataset_type = dataset_config.dataset_type,
        num_steps = dataset_config.num_steps,
        version = %version,
        scenario = %args.scenario,
        scale_factor = args.scale_factor,
        "Configuration"
    );

    let mutations_config = if args.bootstrap {
        MutationConfig::new(args.update_ratio, args.delete_ratio)
            .with_bootstrap(args.bootstrap_mutation_steps, args.bootstrap_churn_fraction)
    } else {
        MutationConfig::new(args.update_ratio, args.delete_ratio)
    };

    // Generate data to a temporary working directory on disk.
    let work_dir = tempfile::tempdir()?;
    let file_storage = Arc::new(FileStorage::new(work_dir.path()));

    tracing::info!(
        work_dir = %work_dir.path().display(),
        "Generating data to local directory"
    );

    let storage: Arc<dyn DataStorage> = file_storage.clone() as Arc<dyn DataStorage>;
    let dataset: Arc<dyn Dataset> = Arc::create(&dataset_config, &mutations_config, storage)?;
    let metrics = Metrics::new();

    let version_config = VersionConfig {
        scenario: args.scenario.clone(),
        scale_factor: args.scale_factor,
        num_steps: args.num_steps,
        dataset_type: args.dataset.clone(),
        update_ratio: mutations_config.update_ratio,
        delete_ratio: mutations_config.delete_ratio,
    };

    let ingestor = DataGenerator::new(
        dataset,
        file_storage.clone() as Arc<dyn DataStorage>,
        metrics,
        version_config,
    );
    let result = ingestor.run().await?;

    println!("\n=== Ingestion Summary ===");
    print_summary(&result);

    if result.write_errors > 0 {
        anyhow::bail!("Ingestion completed with {} errors", result.write_errors);
    }

    // Create the archive from the working directory.
    let mut archive_temp_dir: Option<tempfile::TempDir> = None;
    let archive_path = if let Some(ref output_path) = args.output_archive {
        PathBuf::from(output_path)
    } else {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join(archive::ARCHIVE_FILENAME);
        archive_temp_dir = Some(dir);
        path
    };

    tracing::info!(
        archive_path = %archive_path.display(),
        "Creating archive"
    );
    archive::create_archive(work_dir.path(), &archive_path)?;

    // If --output-archive was specified, we're done; archive is at the
    // requested path. Otherwise, upload to S3.
    if args.output_archive.is_some() {
        tracing::info!(
            output = %archive_path.display(),
            "Archive written to local path (no S3 upload)"
        );
    } else {
        let bucket = args
            .bucket
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("--bucket is required for S3 storage"))?;
        let prefix = build_version_prefix(&args.prefix, &args.scenario, &version);
        let target_config = TargetConfig {
            bucket: bucket.clone(),
            prefix,
            region: args.region.clone(),
            endpoint: args.endpoint.clone(),
            partition_columns: vec![],
        };
        let s3_storage = S3Storage::new(&target_config)?;

        tracing::info!(
            bucket = %target_config.bucket,
            prefix = %target_config.prefix,
            "Uploading archive to S3"
        );
        s3_storage.upload_archive(&archive_path).await?;

        // Clean up temp archive directory
        drop(archive_temp_dir);
    }

    Ok(())
}
