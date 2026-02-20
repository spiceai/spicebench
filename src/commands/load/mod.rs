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
#![allow(dead_code)]

use crate::{args::CommonArgs, commands::adbc_executor, scenario::Scenario};
use arrow::{
    array::{Array, RecordBatch, TimestampMicrosecondArray},
    util::pretty::pretty_format_batches,
};
use etl::{ETLPipeline, PipelineState, StopReason};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;
use system_adapter_protocol::MetricsResponse;
use test_framework::{
    TestType, anyhow,
    arrow::util::pretty::print_batches,
    metrics::{MetricCollector, NoExtendedMetrics, QueryMetrics, QueryStatus, StatisticsCollector},
    opentelemetry::KeyValue,
    opentelemetry_sdk::Resource,
    spicetest::datasets::{ValidationCommand, ValidationStatus, create_validation_channels},
    spicetest::{SpiceTest, datasets::NotStarted},
    telemetry::streaming::StreamingOtlpExporter,
};
use tokio::signal;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

fn run_metric_attributes(common_args: &CommonArgs) -> Vec<KeyValue> {
    vec![KeyValue::new(
        "executor_instance_type",
        common_args.executor_instance_type.clone(),
    )]
}

/// Record the latest SUT metrics snapshot as OTel gauge values.
fn record_sut_metrics(response: &MetricsResponse, attributes: &[KeyValue]) {
    // Resource metrics
    if let Some(cpu) = response.resource.cpu_usage_percent {
        crate::metrics::SUT_CPU_USAGE_PERCENT.record(cpu, attributes);
    }
    if let Some(mem) = response.resource.memory_usage_bytes {
        crate::metrics::SUT_MEMORY_USAGE_BYTES.record(mem, attributes);
    }
    if let Some(v) = response.resource.disk_read_bytes {
        crate::metrics::SUT_DISK_READ_BYTES.record(v, attributes);
    }
    if let Some(v) = response.resource.disk_write_bytes {
        crate::metrics::SUT_DISK_WRITE_BYTES.record(v, attributes);
    }
    if let Some(v) = response.resource.disk_read_iops {
        crate::metrics::SUT_DISK_READ_IOPS.record(v, attributes);
    }
    if let Some(v) = response.resource.disk_write_iops {
        crate::metrics::SUT_DISK_WRITE_IOPS.record(v, attributes);
    }

    // Ingestion metrics
    if let Some(v) = response.ingestion.rows_ingested {
        crate::metrics::INGESTION_ROWS_TOTAL.record(v, attributes);
    }
    if let Some(v) = response.ingestion.bytes_ingested {
        crate::metrics::INGESTION_BYTES_TOTAL.record(v, attributes);
    }
    if let Some(v) = response.ingestion.rows_per_sec {
        crate::metrics::INGESTION_ROWS_PER_SEC.record(v, attributes);
    }
    if let Some(v) = response.ingestion.active_connections {
        crate::metrics::ACTIVE_CONNECTIONS.record(v, attributes);
    }
}

/// Spawn a task that periodically scrapes SUT metrics from the system adapter.
///
/// Returns a `JoinHandle` that resolves to the last `MetricsResponse` received
/// (or `None` if no successful scrape occurred).
fn spawn_sut_metrics_scraper(
    adapter: Arc<Mutex<system_adapter_protocol::Client>>,
    run_id: uuid::Uuid,
    token: CancellationToken,
    interval: Duration,
    attributes: Vec<KeyValue>,
) -> tokio::task::JoinHandle<Option<MetricsResponse>> {
    tokio::spawn(async move {
        let mut last_response: Option<MetricsResponse> = None;
        let mut ticker = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let metrics_result = adapter.lock().await.metrics(run_id).await;
                    match metrics_result {
                        Ok(resp) => {
                            println!("Metrics={:?}", resp);
                            record_sut_metrics(&resp, &attributes);
                            last_response = Some(resp);
                        }
                        Err(e) => {
                            eprintln!("SUT metrics scrape failed: {e}");
                        }
                    }
                }
                () = token.cancelled() => {
                    // Final scrape before exiting
                    if let Ok(resp) = adapter.lock().await.metrics(run_id).await {
                        record_sut_metrics(&resp, &attributes);
                        last_response = Some(resp);
                    }
                    break;
                }
            }
        }
        last_response
    })
}

/// Spawn a task that periodically queries `SELECT MAX(__created_at)` for each
/// table and records the freshness delay (`now − max_created_at`).
///
/// Returns a map of table name → vec of freshness samples (in milliseconds).
fn spawn_e2e_latency_check(
    conn: Arc<std::sync::Mutex<adbc_client::AdbcConnection>>,
    table_names: Vec<String>,
    token: CancellationToken,
    interval: Duration,
    last_created_at_us: Arc<HashMap<String, AtomicI64>>,
) -> tokio::task::JoinHandle<HashMap<String, Vec<f64>>> {
    tokio::spawn(async move {
        println!("[spawn_e2e_latency_check] Starting...");
        let mut samples_by_table: HashMap<String, Vec<f64>> = table_names
            .iter()
            .map(|t| (t.clone(), Vec::new()))
            .collect();
        let mut ticker = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                () = token.cancelled() => break,
            }
            println!("[spawn_e2e_latency_check] after tick.");

            let conn = Arc::clone(&conn);
            let tables = table_names.clone();
            let timestamps = Arc::clone(&last_created_at_us);
            let results = tokio::task::spawn_blocking(move || {
                println!("[spawn_e2e_latency_check] spawn_blocking.");
                let mut out: Vec<(String, Option<f64>)> = Vec::new();
                let mut guard = match conn.lock() {
                    Ok(g) => g,
                    Err(e) => {
                        println!("E2E latency scraper: lock poisoned: {e}");
                        return out;
                    }
                };
                for table in &tables {
                    let last_written_us = timestamps
                        .get(table.as_str())
                        .map_or(0, |a| a.load(Ordering::Relaxed));
                    if last_written_us == 0 {
                        out.push((table.clone(), None));
                        continue;
                    }
                    let sql = format!("SELECT MAX(__created_at) FROM {table}");
                    println!("[spawn_e2e_latency_check] sql={sql}");
                    match guard.query(&sql) {
                        Ok(batches) => {
                            if let Ok(v) = pretty_format_batches(&batches) {
                                println!("[Spicebench] pretty formatted batches:\n{v}");
                            } else {
                                println!("[Spicebench] failed to pretty format batches");
                            };
                            let sample = batches.first().and_then(|batch| {
                                let col = batch.column(0);
                                let ts_array =
                                    col.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
                                if ts_array.is_null(0) {
                                    return None;
                                }
                                let max_ts_us = ts_array.value(0);
                                Some((last_written_us - max_ts_us) as f64 / 1000.0)
                            });
                            println!(
                                "E2E latency checker: table={table}, freshness_ms={:?}",
                                sample
                            );
                            out.push((table.clone(), sample));
                        }
                        Err(e) => {
                            eprintln!("E2E latency checker: query failed for {table}: {e}");
                            out.push((table.clone(), None));
                        }
                    }
                }
                out
            })
            .await;

            if let Ok(results) = results {
                for (table, sample) in results {
                    if let Some(ms) = sample {
                        samples_by_table.entry(table).or_default().push(ms);
                    }
                }
            }
        }
        samples_by_table
    })
}

/// Load checkpoint expected results from parquet files on disk for a given
/// checkpoint index.
///
/// The checkpoints directory is laid out as:
/// ```text
/// {checkpoint_dir}/{checkpoint_idx}/{query_idx}.parquet
/// ```
///
/// `query_names` provides the ordered mapping from `query_idx` to the query
/// name used as keys in the returned map.
///
/// Returns a map of query name → expected `RecordBatch`es. Queries for which
/// the parquet file does not exist are silently skipped.
fn load_checkpoint_results(
    checkpoint_dir: &Path,
    checkpoint_idx: usize,
    query_names: &[Arc<str>],
) -> anyhow::Result<HashMap<Arc<str>, Vec<RecordBatch>>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let idx_dir = checkpoint_dir.join(checkpoint_idx.to_string());
    if !idx_dir.is_dir() {
        anyhow::bail!("Checkpoint directory does not exist: {}", idx_dir.display());
    }

    let mut results: HashMap<Arc<str>, Vec<RecordBatch>> = HashMap::new();

    for (query_idx, query_name) in query_names.iter().enumerate() {
        let parquet_path = idx_dir.join(format!("{query_idx}.parquet"));
        if !parquet_path.exists() {
            tracing::debug!(
                checkpoint = checkpoint_idx,
                query = query_idx,
                name = query_name.as_ref(),
                "Checkpoint parquet not found, skipping"
            );
            continue;
        }

        let file = std::fs::File::open(&parquet_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;
        let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;

        tracing::debug!(
            checkpoint = checkpoint_idx,
            query = query_idx,
            name = query_name.as_ref(),
            rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "Loaded checkpoint expected results"
        );

        results.insert(Arc::clone(query_name), batches);
    }

    Ok(results)
}

#[expect(clippy::too_many_lines)]
#[expect(clippy::too_many_arguments)]
pub(crate) async fn run(
    system_adapter_client: Arc<Mutex<system_adapter_protocol::Client>>,
    run_id: uuid::Uuid,
    scenario: &Scenario,
    common_args: &CommonArgs,
    adbc_conn: adbc_client::AdbcConnection,
    etl_pipeline: &mut ETLPipeline,
    checkpoint_steps: Option<usize>,
    checkpoint_dir: Option<&Path>,
) -> anyhow::Result<()> {
    let metric_attributes = run_metric_attributes(common_args);

    scenario.load_query_set()?;

    let load_resource = Resource::builder_empty()
        .with_attributes(vec![
            KeyValue::new("service.name", "spicebench"),
            KeyValue::new("type", "spicebench"),
            KeyValue::new("adapter_name", common_args.system_adapter_name.clone()),
            KeyValue::new("scenario", scenario.to_string()),
        ])
        .build();

    // Create telemetry with resource upfront, before any metrics calls
    let telemetry = super::create_telemetry_with_resource(common_args, load_resource);

    // Create the appropriate query executor based on args, sharing the ADBC connection
    // so the freshness scraper can also query through it.
    let shared_conn = Arc::new(std::sync::Mutex::new(adbc_conn));
    let executor = Box::new(adbc_executor::AdbcDirectQueryExecutor::from_shared(
        Arc::clone(&shared_conn),
    ));

    println!("Running benchmark");

    let load_end_condition = scenario.end_condition();

    // Create streaming OTLP exporter if OTLP endpoint is configured
    let streaming_exporter = common_args
        .otlp_endpoint
        .as_ref()
        .map(|endpoint| StreamingOtlpExporter::spawn(endpoint.clone()));

    // Spawn SUT metrics scraper if --scrape-sut-metrics is enabled and a system adapter is configured
    let sut_scraper_token = CancellationToken::new();
    let sut_scraper_handle = if common_args.scrape_sut_metrics
        && (common_args.system_adapter_stdio_cmd.is_some()
            || common_args.system_adapter_http_url.is_some())
    {
        println!("SUT metrics scraping enabled (run_id={run_id})");
        Some(spawn_sut_metrics_scraper(
            system_adapter_client,
            run_id,
            sut_scraper_token.clone(),
            Duration::from_secs(5),
            metric_attributes.clone(),
        ))
    } else {
        None
    };

    // Spawn e2e checker
    let table_names: Vec<String> = etl_pipeline.dataset().tables().keys().cloned().collect();
    let e2e_latency_token = CancellationToken::new();
    let e2e_latency_handle = spawn_e2e_latency_check(
        Arc::clone(&shared_conn),
        table_names,
        e2e_latency_token.clone(),
        Duration::from_secs(5),
        etl_pipeline.last_created_at_us(),
    );

    // Record client concurrency as a gauge
    crate::metrics::ACTIVE_CONNECTIONS.record(
        common_args.concurrency.try_into().unwrap_or(0),
        &metric_attributes,
    );

    let mut test_builder = NotStarted::new()
        .with_parallel_count(common_args.concurrency)
        .with_end_condition(load_end_condition)
        .with_query_executor(executor);

    // Add streaming metrics sender if exporter is configured
    if let Some(exporter) = &streaming_exporter {
        test_builder = test_builder.with_streaming_metrics(exporter.sender());
    }

    // Create checkpoint validation channels when --validate-results is enabled
    // and checkpoint data is available.
    let validation_controller =
        if common_args.validate_results && checkpoint_steps.is_some() && checkpoint_dir.is_some() {
            let (controller, worker_handles) = create_validation_channels();
            test_builder = test_builder.with_checkpoint_validation(worker_handles);
            Some(controller)
        } else {
            None
        };

    let (query_set, test_builder) =
        super::build_test_with_validation(scenario, test_builder).await?;

    // Build ordered query names for mapping checkpoint query_idx → query name.
    let queries = query_set.get_queries(None, None, None).await?;
    let query_names: Vec<Arc<str>> = queries.iter().map(|q| Arc::clone(&q.name)).collect();

    let throughput_test = SpiceTest::<NotStarted>::new(scenario.to_string(), test_builder)
        .with_progress_bars(false)
        .start()?;
    let shutdown_token = throughput_test.cancellation_token();

    // --- Start the ETL pipeline (remaining batches) ---
    // If checkpoint_steps is set, use `.run(steps)` so the pipeline pauses
    // at checkpoint boundaries. Otherwise fall back to `.start()` which runs
    // all remaining batches without pausing.
    tracing::info!("Starting ETL pipeline (remaining batches)...");
    let mut etl_state_rx = etl_pipeline.state_watch();
    if let Some(steps) = checkpoint_steps {
        tracing::info!(checkpoint_steps = steps, "Using checkpoint-aware ETL mode");
        etl_pipeline.run(steps).await?;
    } else {
        etl_pipeline.start().await?;
    }

    let test_future = throughput_test.wait();
    tokio::pin!(test_future);

    // Wait for ETL pipeline state changes, handling both pauses (checkpoint
    // boundaries) and stops (completion / error / cancellation).
    //
    // When the pipeline pauses at a checkpoint boundary we immediately
    // continue it. TODO: In the future this is where checkpoint-based query result
    // validation would be triggered before resuming.
    //
    // If interrupted (ctrl-c), cancel both the test and the ETL pipeline.
    let etl_error: Option<String> = loop {
        tokio::select! {
            // ETL state changed — check if stopped or paused
            _ = etl_state_rx.changed() => {
                let state = etl_state_rx.borrow_and_update().clone();
                match state {
                    PipelineState::Paused => {
                        let checkpoint_idx = etl_pipeline.checkpoint_idx();
                        tracing::info!(
                            checkpoint_idx,
                            "ETL pipeline paused at checkpoint boundary"
                        );

                        // --- Checkpoint validation window ---
                        if let Some(ref validation_controller) = validation_controller
                            && let Some(cp_dir) = checkpoint_dir
                        {
                            match load_checkpoint_results(cp_dir, checkpoint_idx, &query_names) {
                                Ok(expected_results) if !expected_results.is_empty() => {
                                    tracing::info!(
                                        checkpoint_idx,
                                        num_queries = expected_results.len(),
                                        "Enabling checkpoint validation"
                                    );

                                    // Tell worker 0 to start validating.
                                    let _ = validation_controller.command_tx.send(Some(
                                        ValidationCommand::Enable {
                                            checkpoint_idx,
                                            expected_results,
                                        },
                                    ));

                                    // Poll the validation status until at least
                                    // `target_iterations` complete query-set iterations
                                    // have finished, or a maximum timeout is reached.
                                    const TARGET_ITERATIONS: usize = 2;
                                    const POLL_INTERVAL: Duration = Duration::from_secs(5);
                                    const MAX_WAIT: Duration = Duration::from_secs(600);
                                    let wait_start = tokio::time::Instant::now();
                                    loop {
                                        let status =
                                            validation_controller.status_rx.borrow().clone();
                                        if status.completed_iterations() >= TARGET_ITERATIONS {
                                            break;
                                        }
                                        if wait_start.elapsed() >= MAX_WAIT {
                                            tracing::warn!(
                                                checkpoint_idx,
                                                completed = status.completed_iterations(),
                                                target = TARGET_ITERATIONS,
                                                "Validation window timed out before reaching target iterations"
                                            );
                                            break;
                                        }
                                        tokio::time::sleep(POLL_INTERVAL).await;
                                    }

                                    // Read the validation status before disabling.
                                    let status = validation_controller.status_rx.borrow().clone();
                                    match &status {
                                        ValidationStatus::Active {
                                            checkpoint_idx: idx,
                                            outcomes,
                                            completed_iterations: iters,
                                        } => {
                                            let total_pass: usize =
                                                outcomes.iter().map(|o| o.pass_count).sum();
                                            let total_fail: usize =
                                                outcomes.iter().map(|o| o.fail_count).sum();
                                            println!(
                                                "Checkpoint {idx} validation ({iters} iterations): {} queries, {total_pass} pass, {total_fail} fail",
                                                outcomes.len()
                                            );
                                            if total_fail > 0 {
                                                for o in outcomes {
                                                    if o.fail_count > 0 {
                                                        eprintln!(
                                                            "  FAIL - query '{}': {} pass, {} fail, last failure: {:?}",
                                                            o.query_name,
                                                            o.pass_count,
                                                            o.fail_count,
                                                            o.last_failure
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                        ValidationStatus::Inactive => {
                                            tracing::warn!(
                                                "Validation status is unexpectedly Inactive"
                                            );
                                        }
                                    }

                                    // Disable validation before resuming ETL.
                                    let _ = validation_controller
                                        .command_tx
                                        .send(Some(ValidationCommand::Disable));
                                }
                                Ok(_) => {
                                    tracing::info!(
                                        checkpoint_idx,
                                        "No checkpoint results found, skipping validation"
                                    );
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        checkpoint_idx,
                                        error = %e,
                                        "Failed to load checkpoint results, skipping validation"
                                    );
                                }
                            }
                        }

                        if let Err(e) = etl_pipeline.continue_pipeline() {
                            eprintln!("Failed to continue ETL pipeline after pause: {e}");
                            shutdown_token.cancel();
                            break Some(format!("Failed to continue ETL pipeline: {e}"));
                        }
                        tracing::info!("ETL pipeline resumed");
                    }
                    PipelineState::Stopped(StopReason::Completed) => {
                        println!("ETL pipeline completed, stopping benchmark...");
                        shutdown_token.cancel();
                        break None;
                    }
                    PipelineState::Stopped(StopReason::Error(ref e)) => {
                        eprintln!("ETL pipeline failed: {e}");
                        shutdown_token.cancel();
                        break Some(e.clone());
                    }
                    PipelineState::Stopped(StopReason::Cancelled) => {
                        println!("ETL pipeline was cancelled, stopping benchmark...");
                        shutdown_token.cancel();
                        break None;
                    }
                    _ => { /* still running, keep waiting */ }
                }
            }
            // ctrl-c: stop everything
            _ = signal::ctrl_c() => {
                println!("Interrupt received, stopping benchmark...");
                shutdown_token.cancel();
                etl_pipeline.cancel();
                break None;
            }
        }
    };

    let test = match test_future.await {
        Ok(test) => test,
        Err(e) => {
            return Err(e);
        }
    };

    // Propagate ETL error after collecting the test result
    if let Some(etl_err) = etl_error {
        return Err(anyhow::anyhow!("ETL pipeline failed: {etl_err}"));
    }
    test.get_query_durations().statistical_set()?;

    // Get all query durations for overall statistics before ending the test
    let all_durations = test.get_query_durations().clone();
    let all_duration_values: Vec<_> = all_durations.values().flatten().copied().collect();

    let metrics: QueryMetrics<_, NoExtendedMetrics> = test.collect(TestType::Load)?;
    let _ = test.end();

    // Record per-query metrics for load test
    for query in &metrics.metrics {
        let query_name = &query.query_name;
        let mut attributes = metric_attributes.clone();
        attributes.push(KeyValue::new("query_name", query_name.to_string()));

        let status: u64 = u64::from(match &query.query_status {
            QueryStatus::Passed => true,
            QueryStatus::Failed(_) => false,
        });

        crate::metrics::QUERY_STATUS.record(status, &attributes);
        crate::metrics::MEDIAN_DURATION.record(query.median_duration_ms, &attributes);
        crate::metrics::MIN_DURATION.record(query.min_duration_ms, &attributes);
        crate::metrics::MAX_DURATION.record(query.max_duration_ms, &attributes);
        crate::metrics::ITERATIONS.record(query.iterations.try_into()?, &attributes);
        crate::metrics::P99_DURATION.record(query.percentile_99_duration_ms, &attributes);
    }

    // Calculate and record overall load test P99
    if !all_duration_values.is_empty() {
        let overall_p99 = all_duration_values.percentile(99.0)?;
        crate::metrics::P99_DURATION
            .record(overall_p99.as_millis().try_into()?, &metric_attributes);
    }
    crate::metrics::TEST_DURATION.record(
        (metrics.finished_at - metrics.started_at).try_into()?,
        &metric_attributes,
    );

    // Query throughput metrics
    let total_iterations: u64 = metrics.metrics.iter().map(|q| q.iterations as u64).sum();
    let test_duration_secs = (metrics.finished_at - metrics.started_at) as f64 / 1000.0;
    crate::metrics::QUERIES_TOTAL.add(total_iterations, &metric_attributes);
    if test_duration_secs > 0.0 {
        let qps = total_iterations as f64 / test_duration_secs;
        crate::metrics::QUERIES_PER_SEC.record(qps, &metric_attributes);

        // Efficiency: queries/s normalized by CPU core count
        let cpu_cores = std::thread::available_parallelism()
            .map(|n| n.get() as f64)
            .unwrap_or(1.0);
        if cpu_cores > 0.0 {
            crate::metrics::EFFICIENCY_QUERIES_PER_CORE.record(qps / cpu_cores, &metric_attributes);
        }
    }

    // Stop SUT metrics scraper
    sut_scraper_token.cancel();
    if let Some(handle) = sut_scraper_handle
        && let Ok(Some(last_sut_metrics)) = handle.await
    {
        println!(
            "Final SUT metrics: cpu={:?}%, mem={:?}B, ingested_rows={:?}, ingested_bytes={:?}",
            last_sut_metrics.resource.cpu_usage_percent,
            last_sut_metrics.resource.memory_usage_bytes,
            last_sut_metrics.ingestion.rows_ingested,
            last_sut_metrics.ingestion.bytes_ingested,
        );
    }

    // Stop freshness scraper and emit P99 metrics
    e2e_latency_token.cancel();
    if let Ok(samples_by_table) = e2e_latency_handle.await {
        println!("!!!! e2e samples_by_table: {:#?}", samples_by_table);
        let mut all_samples: Vec<f64> = Vec::new();
        for (table_name, samples) in &samples_by_table {
            if !samples.is_empty() {
                all_samples.extend(samples);
                let mut sorted = samples.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let idx = ((sorted.len() as f64 * 0.99) as usize).min(sorted.len() - 1);
                let p99 = sorted[idx];
                let attrs = vec![KeyValue::new("table_name", table_name.clone())];
                crate::metrics::E2E_LATENCY_P99_MS.record(p99, &attrs);
            }
        }
        if !all_samples.is_empty() {
            all_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let idx = ((all_samples.len() as f64 * 0.99) as usize).min(all_samples.len() - 1);
            let p99 = all_samples[idx];
            crate::metrics::E2E_LATENCY_P99_MS.record(p99, &[KeyValue::new("table_name", "")]);
            println!(
                "Data freshness P99: {p99:.1}ms ({} samples)",
                all_samples.len()
            );
        }
    } else {
        println!("!!!!!! e2e empty");
    }

    println!("{}", vec!["-"; 30].join(""));
    println!("Benchmark metrics:");
    let records = metrics.build_records()?;
    print_batches(&records)?;

    // Shutdown streaming exporter before emitting final telemetry
    if let Some(exporter) = streaming_exporter {
        exporter.shutdown().await;
    }

    println!("Benchmark completed");

    telemetry.emit().await?;

    Ok(())
}
