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

use std::collections::HashMap;
use std::sync::Arc;

use checkpointer::CheckpointStore;
use data_generation::config::{TargetConfig, build_version_prefix, format_scale_factor};
use data_generation::storage::DataStorage;
use data_generation::storage::file::FileStorage;
use data_generation::storage::s3::S3Storage;
use data_generation::version::VersionMetadata;
use etl::sink::Sink;
use etl::sink::adbc::AdbcSink;
use etl::sink::{dynamodb::DynamoDbSink, mongodb::MongoDbSink};
use etl::{DatasetSource, ETLPipeline, PipelineState, StopReason};
use system_adapter_protocol::SinkConfig;
use test_framework::anyhow;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::args::RunArgs;
use crate::commands::connect_system_adapter;

const FLIGHTSQL_MAX_MSG_SIZE_OPTION: &str = "adbc.flight.sql.client_option.with_max_msg_size";
const DEFAULT_FLIGHTSQL_MAX_MSG_SIZE_BYTES: &str = "78643200";

fn infer_adbc_target_namespace(
    catalog_namespace: Option<&str>,
) -> (Option<String>, Option<String>) {
    let Some(namespace) = catalog_namespace.map(str::trim).filter(|ns| !ns.is_empty()) else {
        return (None, None);
    };

    let mut parts = namespace.rsplitn(2, '.');
    let schema = parts.next().map(str::trim).filter(|v| !v.is_empty());
    let catalog = parts.next().map(str::trim).filter(|v| !v.is_empty());

    (catalog.map(str::to_string), schema.map(str::to_string))
}

async fn run_benchmark(
    common: &RunArgs,
    system_adapter_client: Arc<Mutex<system_adapter_protocol::Client>>,
    run_id: uuid::Uuid,
    setup_metadata: HashMap<String, serde_json::Value>,
    version_metadata: &VersionMetadata,
    file_storage: Arc<FileStorage>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    // --- Load checkpoints (from local dir or S3) ---
    let scenario_name = common.scenario.to_string();
    let checkpoint_dir = tempfile::tempdir()?;

    let derived_version = format_scale_factor(common.scale_factor);
    let version_prefix = build_version_prefix(&common.etl_prefix, &scenario_name, &derived_version);

    let checkpoint_store = if let Some(local_dir) = &common.checkpoint_local_dir {
        tracing::info!(dir = %local_dir, "Using local checkpoint directory (skipping S3)");
        CheckpointStore::new_local(std::path::Path::new(local_dir))
    } else {
        CheckpointStore::new(
            &common.etl_bucket,
            &version_prefix,
            common.etl_region.as_deref(),
            common.etl_endpoint.as_deref(),
        )
    }?;

    let manifest = checkpoint_store
        .download_manifest()
        .await
        .map_err(|e| {
            tracing::warn!("Failed to download checkpoint manifest - results validation will not be enabled: {e}");
            e
        })
        .ok();
    let mut checkpoint_steps: Option<usize> = None;
    if let Some(manifest) = manifest
        && let Some(scenario_info) = manifest.scenarios.get(&scenario_name)
    {
        if common.checkpoint_local_dir.is_some() {
            // Local checkpoints: files are already in checkpoint_local_dir/{idx}/{q}.parquet.
            // Reuse that directory directly instead of downloading into a temp dir.
            tracing::info!(
                scenario = %scenario_name,
                num_checkpoints = scenario_info.checkpoint_indexes.len(),
                num_queries = scenario_info.query_indexes.len(),
                "Using local checkpoints (no download)"
            );
            if scenario_info.checkpoint_interval_steps > 0 {
                checkpoint_steps = Some(scenario_info.checkpoint_interval_steps);
            }
        } else {
            tracing::info!(
                scenario = %scenario_name,
                num_checkpoints = scenario_info.checkpoint_indexes.len(),
                num_queries = scenario_info.query_indexes.len(),
                checkpoint_interval_steps = scenario_info.checkpoint_interval_steps,
                path = %checkpoint_dir.path().display(),
                "Downloading checkpoints"
            );
            if scenario_info.checkpoint_interval_steps > 0 {
                checkpoint_steps = Some(scenario_info.checkpoint_interval_steps);
            }
            if let Err(e) = checkpoint_store
                .download_checkpoints(&scenario_name, scenario_info, checkpoint_dir.path())
                .await
            {
                tracing::warn!(
                    "Failed to download checkpoints - results validation will not be enabled: {e}"
                );
            } else {
                tracing::info!(scenario = %scenario_name, "Checkpoints downloaded");
            }
        }
    } else {
        tracing::warn!(
            scenario = %scenario_name,
            "No checkpoints found for scenario in manifest"
        );
    }

    let dataset_source = DatasetSource::from_dataset_type(&version_metadata.dataset_type)?;
    let generation_config = version_metadata.dataset_config();
    let mutations = version_metadata.mutation_config();
    let data_source: Arc<dyn DataStorage> = file_storage.clone();

    let mut datasets = ETLPipeline::create_tables_request_datasets(
        dataset_source.clone(),
        &generation_config,
        Arc::clone(&data_source),
        &mutations,
        None,
    )?;

    // When using the staging_table update strategy, tables must NOT have
    // primary keys. MERGE INTO handles matching via the ON clause and uses
    // delete+insert execution, which conflicts with Cayenne's automatic
    // on_conflict: Upsert behavior on primary-key tables.
    // Note: preserve PKs for system adapter setup (e.g. Lakebase synced tables
    // require primary_key_columns) and only strip them for the ETL pipeline.
    let uses_staging_table = std::env::var("SPICEBENCH_ADBC_UPDATE_STRATEGY")
        .ok()
        .map(|v| v.eq_ignore_ascii_case("staging_table"))
        .unwrap_or(false);
    let setup_datasets = datasets.clone();
    if uses_staging_table {
        for config in datasets.values_mut() {
            config.primary_key_columns.clear();
        }
    }

    // --- Step 1: setup — create tables/collections, start spiced, get write + read config ---
    let setup_response = system_adapter_client
        .lock()
        .await
        .setup(run_id, setup_metadata.clone(), setup_datasets)
        .await
        .map_err(|e| anyhow::anyhow!("setup failed: {e}"))?;

    tracing::info!(sink_type = ?setup_response.sink, "Setup complete");

    // --- Step 2: build the write sink from SinkConfig ---
    let target_sink: Arc<dyn Sink> = match setup_response.sink {
        SinkConfig::Adbc {
            driver,
            mut db_kwargs,
        } => {
            let driver_name = driver.to_string();
            if driver_name.eq_ignore_ascii_case("flightsql") {
                db_kwargs
                    .entry(FLIGHTSQL_MAX_MSG_SIZE_OPTION.to_string())
                    .or_insert_with(|| {
                        serde_json::Value::String(DEFAULT_FLIGHTSQL_MAX_MSG_SIZE_BYTES.to_string())
                    });
            }

            let write_schema = db_kwargs
                .remove("spicebench.write_schema")
                .and_then(|v| v.as_str().map(str::to_string));

            let (target_db_catalog, target_db_schema) =
                infer_adbc_target_namespace(write_schema.as_deref());

            Arc::new(AdbcSink::new(
                &driver_name,
                db_kwargs,
                target_db_catalog,
                target_db_schema,
                Some((Arc::clone(&system_adapter_client), run_id)),
            )?)
        }
        SinkConfig::DynamoDb {
            region,
            access_key_id,
            secret_access_key,
            session_token,
        } => Arc::new(
            DynamoDbSink::new(
                region,
                access_key_id,
                secret_access_key,
                session_token,
                setup_response.catalog_namespace.clone(),
            )
            .await,
        ),
        SinkConfig::MongoDb { uri } => {
            let pk_cols = datasets
                .iter()
                .map(|(name, cfg)| (name.clone(), cfg.primary_key_columns.clone()))
                .collect();
            Arc::new(
                MongoDbSink::new(&uri, pk_cols)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to create MongoDB sink: {e}"))?,
            )
        }
    };

    // --- Step 3: initialize ETL pipeline (writes batch 0 via the write sink) ---
    let mut pipeline = ETLPipeline::new(
        dataset_source,
        &generation_config,
        Arc::clone(&data_source),
        target_sink,
        &mutations,
    )?;
    tokio::select! {
        r = pipeline.initialize() => r?,
        _ = shutdown.cancelled() => {
            pipeline.cancel();
            return Err(anyhow::anyhow!("Interrupted during ETL initialization"));
        }
    }

    let read_driver_name = setup_response.read_driver.to_string();
    let mut read_db_kwargs = setup_response.read_db_kwargs;
    let query_catalog_namespace = setup_response.catalog_namespace;

    if read_driver_name.eq_ignore_ascii_case("flightsql") {
        read_db_kwargs
            .entry(FLIGHTSQL_MAX_MSG_SIZE_OPTION.to_string())
            .or_insert_with(|| {
                serde_json::Value::String(DEFAULT_FLIGHTSQL_MAX_MSG_SIZE_BYTES.to_string())
            });
    }

    // Size the read pool to accommodate both the test workers and the
    // checkpoint validation executor running concurrently.
    let read_pool_size: u32 = (common.concurrency * 2 + 1).try_into().unwrap_or(u32::MAX);
    let read_pool =
        match adbc_client::create_pool(&read_driver_name, read_db_kwargs, Some(read_pool_size)) {
            Ok(pool) => pool,
            Err(e) => {
                pipeline.cancel();
                return Err(anyhow::anyhow!(
                    "Failed to create ADBC connection pool for driver {read_driver_name}: {e}"
                ));
            }
        };
    tracing::info!(
        "ADBC connection pool created (driver: {read_driver_name}, size: {read_pool_size})",
    );

    super::load::run(
        system_adapter_client,
        run_id,
        &common.scenario,
        common,
        version_metadata,
        read_pool,
        &mut pipeline,
        checkpoint_steps,
        common.checkpoint_local_dir
            .as_deref()
            .map(std::path::Path::new)
            .or_else(|| Some(checkpoint_dir.path())),
        query_catalog_namespace,
        shutdown,
    )
    .await?;

    // --- Wait for ETL to finish ---
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
        }
        other => {
            tracing::warn!("Unexpected final pipeline state: {other:?}");
        }
    }

    Ok(())
}

/// Best-effort lookup of the machine's public egress IP, logged at run start so
/// operators can allowlist it in a source/database firewall (e.g. the MongoDB
/// Atlas Network Access List). Non-fatal — a failure just logs a warning.
async fn log_public_egress_ip() {
    const ENDPOINTS: [&str; 3] = [
        "https://api.ipify.org",
        "https://checkip.amazonaws.com",
        "https://ifconfig.me/ip",
    ];
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("Could not build HTTP client to detect public egress IP: {e}");
            return;
        }
    };
    for url in ENDPOINTS {
        if let Ok(resp) = client.get(url).send().await
            && let Ok(body) = resp.text().await
        {
            let ip = body.trim();
            if !ip.is_empty() {
                tracing::info!(
                    public_egress_ip = %ip,
                    "Public egress IP — allowlist this in your source/database firewall (e.g. MongoDB Atlas Network Access List)"
                );
                return;
            }
        }
    }
    tracing::warn!("Could not determine public egress IP (all lookup endpoints failed)");
}

pub async fn execute(args: &RunArgs) -> anyhow::Result<()> {
    log_public_egress_ip().await;

    let scenario_name = args.scenario.to_string();
    let derived_version = format_scale_factor(args.scale_factor);
    let version_prefix = build_version_prefix(&args.etl_prefix, &scenario_name, &derived_version);
    tracing::info!(
        etl_source = %format!("s3://{}/{}/", args.etl_bucket, version_prefix),
        etl_bucket = %args.etl_bucket,
        etl_prefix = %args.etl_prefix,
        scale_factor = args.scale_factor,
        derived_version = %derived_version,
        etl_region = ?args.etl_region,
        table_format = %args.table_format,
        etl_sink = ?args.etl_sink,
        scenario = %scenario_name,
        concurrency = args.concurrency,
        "ETL configuration"
    );

    // Step 1: Download (or copy from local) and extract the data archive.
    // This happens before benchmark timing begins.
    let extract_dir = tempfile::tempdir()?;

    if let Some(local_archive) = &args.etl_source_archive {
        tracing::info!(
            archive = %local_archive,
            extract_dir = %extract_dir.path().display(),
            "Extracting local data archive (skipping S3 download)"
        );
        let archive_path = std::path::Path::new(local_archive);
        anyhow::ensure!(
            archive_path.exists(),
            "Local archive not found: {local_archive}"
        );
        data_generation::archive::extract_archive(archive_path, extract_dir.path())?;
    } else {
        let source_config = TargetConfig {
            bucket: args.etl_bucket.clone(),
            prefix: version_prefix.clone(),
            region: args.etl_region.clone(),
            endpoint: args.etl_endpoint.clone(),
            partition_columns: vec![],
        };
        let archive_storage: Arc<dyn DataStorage> = Arc::new(S3Storage::new(&source_config)?);

        tracing::info!(
            extract_dir = %extract_dir.path().display(),
            "Downloading and extracting data archive"
        );
        ETLPipeline::download(archive_storage, extract_dir.path()).await?;
    }

    // Step 2: Create FileStorage from extracted data and read version metadata.
    let file_storage = Arc::new(FileStorage::new(extract_dir.path()));
    let version_metadata = file_storage.read_version_metadata().await?.ok_or_else(|| {
        anyhow::anyhow!(
            "No version.json found in extracted data at {}. Was data generation run?",
            extract_dir.path().display(),
        )
    })?;

    // --- Connect to the system adapter ---
    let system_adapter_client = match connect_system_adapter(args).await {
        Ok(client) => client,
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to connect to system adapter: {e}"));
        }
    };

    let run_id = uuid::Uuid::new_v4();
    let scenario_name = args.scenario.to_string();

    let serde_json::Value::Object(setup_map) = serde_json::json!({
        "executor_instance_type": args.executor_instance_type,
        "table_format": args.table_format.to_string(),
        "scenario": scenario_name,
        "etl_bucket": args.etl_bucket,
        "etl_prefix": args.etl_prefix,
        "etl_version": derived_version,
        "etl_region": args.etl_region,
        "etl_endpoint": args.etl_endpoint,
        "etl_sink": "adbc",
        "etl_type": version_metadata.etl_type().to_string(),
    }) else {
        unreachable!()
    };
    let mut setup_metadata: HashMap<String, serde_json::Value> = setup_map.into_iter().collect();

    if let Ok(system_under_test) = std::env::var("SYSTEM_UNDER_TEST") {
        setup_metadata.insert(
            "system_under_test".to_string(),
            serde_json::json!(system_under_test),
        );

        if let Some((prefix, variant)) = system_under_test.split_once('-') {
            setup_metadata.insert(
                "system_adapter_prefix".to_string(),
                serde_json::json!(prefix),
            );
            setup_metadata.insert(
                "system_adapter_variant".to_string(),
                serde_json::json!(variant),
            );
        } else {
            setup_metadata.insert(
                "system_adapter_prefix".to_string(),
                serde_json::json!(system_under_test),
            );
        }
    }

    let system_adapter_client = Arc::new(Mutex::new(system_adapter_client));

    let shutdown = CancellationToken::new();
    {
        let token = shutdown.clone();
        tokio::spawn(async move {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{SignalKind, signal};
                let mut sigterm =
                    signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = sigterm.recv() => {},
                }
            }
            #[cfg(not(unix))]
            tokio::signal::ctrl_c().await.ok();
            token.cancel();
        });
    }

    // Wrap run_benchmark in a select so that SIGTERM/SIGINT during any blocking
    // call inside it (setup, sink creation, pool creation) cancels cleanly and
    // teardown still runs.  The token is also passed into run_benchmark so that
    // initialize() and load::run() can cancel themselves from within.
    let result = tokio::select! {
        r = run_benchmark(
            args,
            Arc::clone(&system_adapter_client),
            run_id,
            setup_metadata,
            &version_metadata,
            file_storage,
            shutdown.clone(),
        ) => r,
        _ = shutdown.cancelled() => {
            Err(anyhow::anyhow!("Interrupted"))
        }
    };

    // Always call teardown so spidapter can clean up its run state and disarm
    // RAII guards. When --no-teardown is set, pass preserve_resources=true so
    // provisioned cloud resources (EC2 instances, DynamoDB tables, SCP app) are
    // kept alive for post-run inspection instead of being deleted.
    let preserve = args.no_teardown;
    if preserve {
        tracing::info!(
            "--no-teardown: calling teardown with preserve_resources=true to keep cloud resources alive."
        );
    }
    if let Err(e) = system_adapter_client
        .lock()
        .await
        .teardown(run_id, preserve)
        .await
    {
        tracing::error!("Failed to teardown system adapter: {e}");
    }

    result
}
