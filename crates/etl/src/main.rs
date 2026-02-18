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
use tracing_subscriber::EnvFilter;

use std::sync::Arc;

use data_generation::config::{Cli, Command, CommonArgs};
use data_generation::dataset;
use data_generation::dataset::tpch::TpchDataset;
use data_generation::metrics::{IngestResult, Metrics};
use data_generation::target::s3::S3Target;
use etl::ingestor::Ingestor;

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

fn build(
    args: &CommonArgs,
) -> anyhow::Result<(Ingestor<Arc<dyn dataset::Dataset>, S3Target>, S3Target)> {
    let dataset_config = args.dataset_config();
    let target_config = args.target_config();
    let ingestor_config = args.ingestor_config();

    tracing::info!(
        dataset_type = dataset_config.dataset_type,
        num_steps = dataset_config.num_steps,
        max_concurrency = ingestor_config.max_concurrency,
        bucket = target_config.bucket,
        prefix = target_config.prefix,
        "Configuration"
    );

    let dataset: Arc<dyn dataset::Dataset> = match dataset_config.dataset_type.as_str() {
        "tpch" => Arc::new(TpchDataset::new(&dataset_config)?),
        other => anyhow::bail!("Unknown dataset type: {other}. Supported: tpch"),
    };

    let target = S3Target::new(&target_config)?;
    let metrics = Metrics::new();

    let ingestor = Ingestor::new(dataset, target.clone(), &ingestor_config, metrics);
    Ok((ingestor, target))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Initialize(args) => {
            let (mut ingestor, target) = build(&args)?;
            let loc_fn = |table: &str| target.table_s3_path(table);
            let result = ingestor.initialize(Some(&loc_fn)).await?;

            if result.write_errors > 0 {
                anyhow::bail!("Initialization failed with {} errors", result.write_errors);
            }
        }
        Command::Run(run_args) => {
            let (mut ingestor, _target) = build(&run_args.common)?;
            if run_args.skip_initial {
                ingestor.skip_initial_batches().await?;
            }
            let result = ingestor.run().await?;

            println!("\n=== Ingestion Summary ===");
            print_summary(&result);

            if result.write_errors > 0 {
                anyhow::bail!("Ingestion completed with {} errors", result.write_errors);
            }
        }
    }

    Ok(())
}
