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

use std::sync::Arc;

use data_generation::config::{TargetConfig, build_version_prefix, format_scale_factor};
use data_generation::storage::DataStorage;
use data_generation::storage::file::FileStorage;
use data_generation::storage::s3::S3Storage;
use etl::sink::Sink;
use etl::sink::adbc::AdbcSink;
use etl::sink::null::NullSink;
use etl::{DatasetSource, ETLPipeline, PipelineState, StopReason};
use test_framework::anyhow;

use crate::args::etl::{EtlArgs, EtlSinkType};

const FLIGHTSQL_MAX_MSG_SIZE_OPTION: &str = "adbc.flight.sql.client_option.with_max_msg_size";
const DEFAULT_FLIGHTSQL_MAX_MSG_SIZE_BYTES: &str = "78643200";

pub async fn execute(args: &EtlArgs) -> anyhow::Result<()> {
    let version = format_scale_factor(args.scale_factor);

    // Determine where to extract the archive data.
    let extract_temp_dir;
    let extract_dir = if let Some(ref dir) = args.extract_dir {
        dir.clone()
    } else {
        extract_temp_dir = tempfile::tempdir()?;
        extract_temp_dir.path().to_path_buf()
    };

    // Step 1: Obtain the data archive and extract it.
    if let Some(ref archive_file) = args.archive_file {
        // Local archive mode — extract directly, no S3 required.
        tracing::info!(
            archive_file = %archive_file.display(),
            extract_dir = %extract_dir.display(),
            "Extracting local archive"
        );
        data_generation::archive::extract_archive(archive_file, &extract_dir)?;
    } else {
        // Download from S3.
        let bucket = args
            .bucket
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("--bucket is required when not using --archive-file"))?;
        let version_prefix = build_version_prefix(&args.prefix, &args.scenario, &version);
        let source_config = TargetConfig {
            bucket: bucket.clone(),
            prefix: version_prefix,
            region: args.region.clone(),
            endpoint: args.endpoint.clone(),
            partition_columns: vec![],
        };
        let s3_storage = Arc::new(S3Storage::new(&source_config)?);
        ETLPipeline::download(s3_storage as Arc<dyn DataStorage>, &extract_dir).await?;
    }

    // Step 2: Create FileStorage from extracted data and read version metadata.
    let file_storage: Arc<dyn DataStorage> = Arc::new(FileStorage::new(&extract_dir));
    let version_metadata = file_storage.read_version_metadata().await?.ok_or_else(|| {
        anyhow::anyhow!(
            "No version.json found in extracted data at {}. Was data generation run?",
            extract_dir.display()
        )
    })?;

    let dataset_source = DatasetSource::from_dataset_type(&version_metadata.dataset_type)?;
    let dataset_config = version_metadata.dataset_config();
    let mutations = version_metadata.mutation_config();

    if args.adbc_create_tables && !matches!(args.sink, EtlSinkType::Adbc) {
        anyhow::bail!("--adbc-create-tables requires --sink adbc");
    }

    let (target, target_config, target_kind, adbc_sink): (
        Arc<dyn Sink>,
        Option<TargetConfig>,
        String,
        Option<Arc<AdbcSink>>,
    ) = match args.sink {
        EtlSinkType::Adbc => {
            let driver = args
                .adbc_driver
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("--sink adbc requires --adbc-driver"))?;
            let uri = args
                .adbc_uri
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("--sink adbc requires --adbc-uri"))?;

            let mut db_kwargs = std::collections::HashMap::new();
            db_kwargs.insert(
                "uri".to_string(),
                serde_json::Value::String(uri.to_string()),
            );

            for option in &args.adbc_options {
                let (key, value) = option.split_once('=').ok_or_else(|| {
                    anyhow::anyhow!("Invalid --adbc-option '{option}'. Expected key=value")
                })?;

                let key = key.trim();
                if key.is_empty() {
                    anyhow::bail!("Invalid --adbc-option '{option}'. Option key cannot be empty");
                }

                db_kwargs.insert(
                    key.to_string(),
                    serde_json::Value::String(value.to_string()),
                );
            }

            if driver.eq_ignore_ascii_case("flightsql") {
                db_kwargs
                    .entry(FLIGHTSQL_MAX_MSG_SIZE_OPTION.to_string())
                    .or_insert_with(|| {
                        serde_json::Value::String(DEFAULT_FLIGHTSQL_MAX_MSG_SIZE_BYTES.to_string())
                    });
            }

            let adbc_sink = Arc::new(AdbcSink::new(
                driver,
                db_kwargs,
                args.adbc_catalog.clone(),
                args.adbc_schema.clone(),
            )?);

            (
                adbc_sink.clone() as Arc<dyn Sink>,
                None,
                "adbc".to_string(),
                Some(adbc_sink),
            )
        }
        EtlSinkType::Null => {
            if args.adbc_driver.is_some()
                || args.adbc_uri.is_some()
                || !args.adbc_options.is_empty()
                || args.adbc_catalog.is_some()
                || args.adbc_schema.is_some()
                || args.adbc_create_tables
            {
                anyhow::bail!(
                    "ADBC options are only valid with --sink adbc. Remove ADBC flags when using --sink null."
                );
            }

            (Arc::new(NullSink::new()), None, "null".to_string(), None)
        }
    };

    if args.adbc_create_tables
        && let Some(adbc_sink) = &adbc_sink
    {
        let datasets = ETLPipeline::create_tables_request_datasets(
            dataset_source.clone(),
            &dataset_config,
            file_storage.clone(),
            &mutations,
            target_config.clone(),
        )?;
        adbc_sink.create_tables_from_dataset_configs(&datasets)?;
    }

    let mut pipeline = ETLPipeline::new(
        dataset_source,
        &dataset_config,
        file_storage,
        target,
        &mutations,
    )?;
    if let Some(target_config) = target_config {
        pipeline = pipeline.with_target_config(target_config);
    }

    tracing::info!(
        scenario = %args.scenario,
        version = %version,
        dataset = %version_metadata.dataset_type,
        bucket = ?args.bucket,
        prefix = %args.prefix,
        extract_dir = %extract_dir.display(),
        target = %target_kind,
        adbc_driver = ?args.adbc_driver,
        adbc_catalog = ?args.adbc_catalog,
        adbc_schema = ?args.adbc_schema,
        adbc_create_tables = args.adbc_create_tables,
        scale_factor = version_metadata.scale_factor,
        num_steps = version_metadata.num_steps,
        "Starting ETL pipeline"
    );

    pipeline.initialize().await?;
    pipeline.start().await?;

    let final_state = pipeline.wait().await;
    match &final_state {
        PipelineState::Stopped(StopReason::Completed) => {
            tracing::info!("ETL pipeline completed successfully");
        }
        PipelineState::Stopped(StopReason::Cancelled) => {
            tracing::warn!("ETL pipeline was cancelled");
        }
        PipelineState::Stopped(StopReason::Error(e)) => {
            tracing::error!(error = %e, "ETL pipeline stopped with error");
            anyhow::bail!("ETL pipeline failed: {e}");
        }
        other => {
            anyhow::bail!("Unexpected final pipeline state: {other:?}");
        }
    }

    Ok(())
}
