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
use data_generation::archive;
use data_generation::config::format_scale_factor;
use data_generation::generator::{DataGenerator, VersionConfig};
use data_generation::storage::DataStorage;
use data_generation::storage::file::FileStorage;
use data_generation::storage::s3::S3Storage;
use tracing_subscriber::EnvFilter;

use std::path::PathBuf;
use std::sync::Arc;

use data_generation::config::{Cli, Command, CommonArgs};
use data_generation::dataset::{Dataset, MutationConfig};
use data_generation::metrics::{IngestResult, Metrics};

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

fn build(args: &CommonArgs, file_storage: Arc<FileStorage>) -> anyhow::Result<DataGenerator> {
    let dataset_config = args.dataset_config();
    let ingestor_config = args.ingestor_config();
    let version = format_scale_factor(args.scale_factor);

    tracing::info!(
        dataset_type = dataset_config.dataset_type,
        num_steps = dataset_config.num_steps,
        max_concurrency = ingestor_config.max_concurrency,
        version = %version,
        scenario = %args.scenario,
        scale_factor = args.scale_factor,
        "Configuration"
    );

    let mutations_config = MutationConfig::new(args.update_ratio, args.delete_ratio);

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
        file_storage as Arc<dyn DataStorage>,
        &ingestor_config,
        metrics,
        version_config,
    );
    Ok(ingestor)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Run(args) => {
            // Generate data to a temporary working directory on disk.
            let work_dir = tempfile::tempdir()?;
            let file_storage = Arc::new(FileStorage::new(work_dir.path()));

            tracing::info!(
                work_dir = %work_dir.path().display(),
                "Generating data to local directory"
            );

            let ingestor = build(&args, file_storage)?;
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
                let target_config = args.target_config()?;
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
        }
    }

    Ok(())
}
