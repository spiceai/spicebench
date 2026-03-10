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
use arrow::array::{Array, RecordBatch, TimestampMicrosecondArray};
use data_generation::version::VersionMetadata;
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
    opentelemetry::metrics::{Counter, Gauge},
    opentelemetry_sdk::Resource,
    spicetest::datasets::{ValidationCommand, ValidationStatus, create_validation_channels},
    spicetest::{SpiceTest, datasets::NotStarted},
    telemetry::SutMetricsPipeline,
    telemetry::streaming::StreamingOtlpExporter,
};
use tokio::signal;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// Instruments for recording SUT resource metrics on the streaming pipeline.
struct SutInstruments {
    cpu_usage_percent: Counter<f64>,
    memory_bytes: Gauge<u64>,
    disk_read_bytes: Counter<u64>,
    disk_write_bytes: Counter<u64>,
    disk_read_ops: Counter<u64>,
    disk_write_ops: Counter<u64>,
    ingestion_rows_total: Gauge<u64>,
    ingestion_bytes_total: Gauge<u64>,
}

fn run_metric_attributes(common_args: &CommonArgs) -> Vec<KeyValue> {
    vec![KeyValue::new(
        "executor_instance_type",
        common_args.executor_instance_type.clone(),
    )]
}

fn log_sut_metrics_snapshot(response: &MetricsResponse) {
    tracing::debug!(
        resource = ?response.resource,
        ingestion = ?response.ingestion,
        "SUT metrics snapshot retrieved before export"
    );
}

/// Record the latest SUT metrics snapshot on the given streaming instruments.
#[expect(clippy::too_many_arguments)]
fn record_sut_metrics(
    response: &MetricsResponse,
    instruments: &SutInstruments,
    attributes: &[KeyValue],
    prev_cpu_usage_seconds: &mut Option<f64>,
    prev_disk_read_bytes: &mut Option<u64>,
    prev_disk_write_bytes: &mut Option<u64>,
    prev_disk_read_iops: &mut Option<u64>,
    prev_disk_write_iops: &mut Option<u64>,
    prev_rows_ingested: &mut Option<u64>,
    last_scrape_time: &mut Option<std::time::Instant>,
) {
    // Resource metrics are cumulative counters; record the delta since last scrape
    if let Some(cpu) = response.resource.cpu_usage_percent {
        if let Some(prev) = prev_cpu_usage_seconds {
            instruments
                .cpu_usage_percent
                .add((cpu - *prev).max(0.0), attributes);
        }
        *prev_cpu_usage_seconds = Some(cpu);
    }
    if let Some(mem) = response.resource.memory_usage_bytes {
        instruments.memory_bytes.record(mem, attributes);
    }
    // Disk metrics are cumulative counters; record the delta since last scrape
    if let Some(v) = response.resource.disk_read_bytes {
        if let Some(prev) = prev_disk_read_bytes {
            let delta = v.saturating_sub(*prev);
            instruments.disk_read_bytes.add(delta, attributes);
        }
        *prev_disk_read_bytes = Some(v);
    }
    if let Some(v) = response.resource.disk_write_bytes {
        if let Some(prev) = prev_disk_write_bytes {
            let delta = v.saturating_sub(*prev);
            instruments.disk_write_bytes.add(delta, attributes);
        }
        *prev_disk_write_bytes = Some(v);
    }
    if let Some(v) = response.resource.disk_read_iops {
        if let Some(prev) = prev_disk_read_iops {
            let delta = v.saturating_sub(*prev);
            instruments.disk_read_ops.add(delta, attributes);
        }
        *prev_disk_read_iops = Some(v);
    }
    if let Some(v) = response.resource.disk_write_iops {
        if let Some(prev) = prev_disk_write_iops {
            let delta = v.saturating_sub(*prev);
            instruments.disk_write_ops.add(delta, attributes);
        }
        *prev_disk_write_iops = Some(v);
    }

    // Ingestion metrics
    if let Some(v) = response.ingestion.rows_ingested {
        instruments.ingestion_rows_total.record(v, attributes);
    }
    if let Some(v) = response.ingestion.bytes_ingested {
        instruments.ingestion_bytes_total.record(v, attributes);
    }
    // Use adapter-provided rows_per_sec if available; otherwise derive it
    // from the delta in rows_ingested since the last scrape.
    if let Some(v) = response.ingestion.rows_per_sec {
        crate::metrics::INGESTION_ROWS_PER_SEC.record(v, attributes);
    } else if let Some(current_rows) = response.ingestion.rows_ingested
        && let Some(prev_rows) = *prev_rows_ingested
        && let Some(prev_time) = *last_scrape_time
    {
        let elapsed_secs = prev_time.elapsed().as_secs_f64();
        if elapsed_secs > 0.0 {
            let rows_per_sec = current_rows.saturating_sub(prev_rows) as f64 / elapsed_secs;
            crate::metrics::INGESTION_ROWS_PER_SEC.record(rows_per_sec, attributes);
        }
    }
    // Update tracking state for the next scrape
    if let Some(v) = response.ingestion.rows_ingested {
        *prev_rows_ingested = Some(v);
    }
    *last_scrape_time = Some(std::time::Instant::now());
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
    instruments: SutInstruments,
) -> tokio::task::JoinHandle<Option<MetricsResponse>> {
    tokio::spawn(async move {
        let mut last_response: Option<MetricsResponse> = None;
        let mut prev_disk_read_bytes: Option<u64> = None;
        let mut prev_disk_write_bytes: Option<u64> = None;
        let mut prev_cpu_usage_seconds: Option<f64> = None;
        let mut prev_disk_read_iops: Option<u64> = None;
        let mut prev_disk_write_iops: Option<u64> = None;
        let mut prev_rows_ingested: Option<u64> = None;
        let mut last_scrape_time: Option<std::time::Instant> = None;
        let mut ticker = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let metrics_result = adapter.lock().await.metrics(run_id).await;
                    match metrics_result {
                        Ok(resp) => {
                            log_sut_metrics_snapshot(&resp);
                            record_sut_metrics(
                                &resp,
                                &instruments,
                                &attributes,
                                &mut prev_cpu_usage_seconds,
                                &mut prev_disk_read_bytes,
                                &mut prev_disk_write_bytes,
                                &mut prev_disk_read_iops,
                                &mut prev_disk_write_iops,
                                &mut prev_rows_ingested,
                                &mut last_scrape_time,
                            );
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
                        log_sut_metrics_snapshot(&resp);
                        record_sut_metrics(
                            &resp,
                            &instruments,
                            &attributes,
                            &mut prev_cpu_usage_seconds,
                            &mut prev_disk_read_bytes,
                            &mut prev_disk_write_bytes,
                            &mut prev_disk_read_iops,
                            &mut prev_disk_write_iops,
                            &mut prev_rows_ingested,
                            &mut last_scrape_time,
                        );
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
    pool: adbc_client::AdbcConnectionPool,
    table_names: Vec<String>,
    query_catalog_namespace: Option<String>,
    token: CancellationToken,
    interval: Duration,
    last_created_at_us: Arc<HashMap<String, AtomicI64>>,
) -> tokio::task::JoinHandle<HashMap<String, Vec<f64>>> {
    tokio::spawn(async move {
        println!(
            "E2E latency checker started (interval={}s)",
            interval.as_secs()
        );
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

            let pool = pool.clone();
            let tables = table_names.clone();
            let timestamps = Arc::clone(&last_created_at_us);
            let query_catalog_namespace = query_catalog_namespace.clone();
            let results = tokio::task::spawn_blocking(move || {
                let mut out: Vec<(String, Option<f64>)> = Vec::new();
                let mut conn = match pool.get() {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("E2E latency checker: failed to get connection: {e}");
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
                    let table_ref = if let Some(namespace) = query_catalog_namespace
                        .as_deref()
                        .map(str::trim)
                        .filter(|ns| !ns.is_empty())
                    {
                        if table.contains('.') {
                            table.to_string()
                        } else {
                            format!("{namespace}.{table}")
                        }
                    } else {
                        table.to_string()
                    };

                    let sql = format!("SELECT MAX(__created_at) FROM {table_ref}");
                    match conn.query(&sql) {
                        Ok(batches) => {
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
                let mut sampled_count = 0usize;
                let mut missing_count = 0usize;
                let mut min_ms = f64::INFINITY;
                let mut max_ms = f64::NEG_INFINITY;
                let mut sum_ms = 0.0;

                for (table, sample) in results {
                    if let Some(ms) = sample {
                        samples_by_table.entry(table).or_default().push(ms);
                        sampled_count += 1;
                        sum_ms += ms;
                        min_ms = min_ms.min(ms);
                        max_ms = max_ms.max(ms);
                    } else {
                        missing_count += 1;
                    }
                }

                if sampled_count > 0 {
                    println!(
                        "E2E latency checker: tables={} sampled={} missing={} min_ms={:.2} avg_ms={:.2} max_ms={:.2}",
                        sampled_count + missing_count,
                        sampled_count,
                        missing_count,
                        min_ms,
                        sum_ms / sampled_count as f64,
                        max_ms
                    );
                } else {
                    println!(
                        "E2E latency checker: tables={} sampled=0 missing={}",
                        missing_count, missing_count
                    );
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
    version_metadata: &VersionMetadata,
    read_pool: adbc_client::AdbcConnectionPool,
    etl_pipeline: &mut ETLPipeline,
    checkpoint_steps: Option<usize>,
    checkpoint_dir: Option<&Path>,
    query_catalog_namespace: Option<String>,
) -> anyhow::Result<()> {
    let metric_attributes = run_metric_attributes(common_args);

    scenario.load_query_set()?;

    let load_resource = Resource::builder_empty()
        .with_attributes(vec![
            KeyValue::new("service.name", "spicebench"),
            KeyValue::new("type", "spicebench"),
            KeyValue::new("adapter_name", common_args.system_adapter_name.clone()),
            KeyValue::new("scenario", scenario.to_string()),
            KeyValue::new(
                "data_gen_version",
                data_generation::config::format_scale_factor(common_args.scale_factor),
            ),
            KeyValue::new("scale_factor", version_metadata.scale_factor.to_string()),
        ])
        .build();

    // Create telemetry with resource upfront, before any metrics calls
    let telemetry = super::create_telemetry_with_resource(common_args, load_resource.clone());

    // Create the appropriate query executor based on args.
    // Each worker gets its own connection from the pool.
    let executor = Box::new(adbc_executor::AdbcDirectQueryExecutor::new(
        read_pool.clone(),
    ));

    println!("Running benchmark");

    let load_end_condition = scenario.end_condition();

    // Create streaming OTLP exporter if OTLP endpoint is configured
    let streaming_exporter = common_args
        .otlp_endpoint
        .as_ref()
        .map(|endpoint| StreamingOtlpExporter::spawn(endpoint.clone()));

    // Spawn SUT metrics scraper if --scrape-sut-metrics is enabled and a system adapter is configured.
    // SUT metrics are always periodically exported to the Arrow backend (SPICEAI_BENCHMARK_METRICS_KEY).
    // When --otlp-endpoint is configured, they are also exported there.
    let sut_scraper_token = CancellationToken::new();
    let (sut_scraper_handle, sut_pipeline) = if common_args.scrape_sut_metrics
        && (common_args.system_adapter_stdio_cmd.is_some()
            || common_args.system_adapter_http_url.is_some())
    {
        let sut_pipeline = SutMetricsPipeline::new(
            "SPICEAI_BENCHMARK_METRICS_KEY",
            common_args.otlp_endpoint.as_deref(),
            load_resource.clone(),
        )
        .await?;
        let m = sut_pipeline.meter();
        let instruments = SutInstruments {
            cpu_usage_percent: m.f64_counter("sut_cpu_usage_percent").build(),
            memory_bytes: m.u64_gauge("sut_memory_usage_bytes").build(),
            disk_read_bytes: m.u64_counter("sut_disk_read_bytes").build(),
            disk_write_bytes: m.u64_counter("sut_disk_write_bytes").build(),
            disk_read_ops: m.u64_counter("sut_disk_read_ops").build(),
            disk_write_ops: m.u64_counter("sut_disk_write_ops").build(),
            ingestion_rows_total: m.u64_gauge("ingestion_rows_total").build(),
            ingestion_bytes_total: m.u64_gauge("ingestion_bytes_total").build(),
        };
        let mut sut_attributes = metric_attributes.clone();
        sut_attributes.push(KeyValue::new("run_id", run_id.to_string()));
        println!("SUT metrics scraping enabled (run_id={run_id})");
        (
            Some(spawn_sut_metrics_scraper(
                system_adapter_client,
                run_id,
                sut_scraper_token.clone(),
                Duration::from_secs(5),
                sut_attributes,
                instruments,
            )),
            Some(sut_pipeline),
        )
    } else {
        (None, None)
    };

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

    // Always create validation channels so we can track query-set iteration
    // completions. When --validate-results is enabled with checkpoint data,
    // these channels are also used for checkpoint-based results validation.
    let (mut validation_controller, validation_worker_handles) = create_validation_channels();
    test_builder = test_builder.with_checkpoint_validation(validation_worker_handles);

    let has_checkpoint_validation =
        common_args.validate_results && checkpoint_steps.is_some() && checkpoint_dir.is_some();

    // Spawn e2e checker — only when checkpoint validation is NOT enabled,
    // since checkpoint validation provides its own e2e latency measurement.
    let e2e_latency_token = CancellationToken::new();
    let e2e_latency_handle = if !has_checkpoint_validation {
        let table_names: Vec<String> = etl_pipeline.dataset().tables().keys().cloned().collect();
        Some(spawn_e2e_latency_check(
            read_pool.clone(),
            table_names,
            query_catalog_namespace.clone(),
            e2e_latency_token.clone(),
            Duration::from_secs(5),
            etl_pipeline.last_created_at_us(),
        ))
    } else {
        None
    };

    let (query_set, test_builder) = super::build_test_with_validation(
        scenario,
        test_builder,
        query_catalog_namespace.as_deref(),
    )
    .await?;

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
                        if has_checkpoint_validation
                            && let Some(cp_dir) = checkpoint_dir
                        {
                            match load_checkpoint_results(cp_dir, checkpoint_idx, &query_names) {
                                Ok(expected_results) if !expected_results.is_empty() => {
                                    tracing::info!(
                                        checkpoint_idx,
                                        num_queries = expected_results.len(),
                                        "Enabling checkpoint validation"
                                    );

                                    let etl_pause_time = tokio::time::Instant::now();
                                    // Capture std::time::Instant at the same point so
                                    // we can compare against the worker's
                                    // first_pass_instant (which uses std::time::Instant).
                                    let etl_pause_time_std = std::time::Instant::now();

                                    // Tell worker 0 to start validating.
                                    let _ = validation_controller.command_tx.send(Some(
                                        ValidationCommand::Enable {
                                            checkpoint_idx,
                                            expected_results,
                                        },
                                    ));

                                    // Poll the validation status until convergence
                                    // (a complete iteration where every query passes)
                                    // or until the timeout is reached.
                                    const MAX_WAIT: Duration = Duration::from_secs(600);
                                    let deadline =
                                        tokio::time::Instant::now() + MAX_WAIT;
                                    let mut timed_out = false;
                                    let interrupted = false;
                                    loop {
                                        let status =
                                            validation_controller.status_rx.borrow().clone();
                                        if status.converged() {
                                            // Use the instant the first query passed
                                            // as the latency reference point.  This
                                            // measures when the data was fully ingested
                                            // (first correct answer), not when the full
                                            // validation sweep finished.
                                            let latency_ms = status
                                                .first_pass_instant()
                                                .map_or_else(
                                                    || etl_pause_time_std.elapsed(),
                                                    |fpi| fpi.duration_since(etl_pause_time_std),
                                                )
                                                .as_secs_f64()
                                                * 1000.0;
                                            println!(
                                                "Checkpoint {} converged in {:.1}s ({} iterations)",
                                                checkpoint_idx,
                                                latency_ms / 1000.0,
                                                status.completed_iterations(),
                                            );
                                            crate::metrics::E2E_LATENCY_MS
                                                .record(latency_ms, &metric_attributes);
                                            break;
                                        }
                                        if etl_pause_time.elapsed() >= MAX_WAIT {
                                            timed_out = true;
                                            break;
                                        }
                                        // Wait for the worker to publish a new status
                                        // update rather than polling on a fixed interval.
                                        if tokio::time::timeout_at(
                                            deadline,
                                            validation_controller.status_rx.changed(),
                                        )
                                        .await
                                        .is_err()
                                        {
                                            timed_out = true;
                                            break;
                                        }
                                    }

                                    // Read the validation status before disabling.
                                    let status = validation_controller.status_rx.borrow().clone();
                                    if let ValidationStatus::Active {
                                        checkpoint_idx: idx,
                                        outcomes,
                                        completed_iterations: iters,
                                        converged,
                                        ..
                                    } = &status
                                    {
                                        let total_pass: usize =
                                            outcomes.iter().map(|o| o.pass_count).sum();
                                        let total_fail: usize =
                                            outcomes.iter().map(|o| o.fail_count).sum();
                                        println!(
                                            "Checkpoint {idx} validation ({iters} iterations, converged={converged}): {} queries, {total_pass} pass, {total_fail} fail",
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

                                    // Disable validation before resuming ETL.
                                    let _ = validation_controller
                                        .command_tx
                                        .send(Some(ValidationCommand::Disable));

                                    if interrupted {
                                        eprintln!("Interrupt received during checkpoint validation, stopping...");
                                        shutdown_token.cancel();
                                        etl_pipeline.cancel();
                                        break Some("Interrupted by user".to_string());
                                    }

                                    if timed_out {
                                        eprintln!(
                                            "Checkpoint {} validation timed out after {}s without convergence, aborting run",
                                            checkpoint_idx, MAX_WAIT.as_secs()
                                        );
                                        shutdown_token.cancel();
                                        etl_pipeline.cancel();
                                        break Some(format!(
                                            "Checkpoint {checkpoint_idx} validation timed out after {}s",
                                            MAX_WAIT.as_secs()
                                        ));
                                    }
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
                        println!("ETL pipeline completed");
                        if !has_checkpoint_validation {
                            // When results validation is not enabled, wait for
                            // at least 1 query set iteration to complete so we
                            // collect meaningful query metrics before stopping.
                            const POLL_INTERVAL: Duration = Duration::from_secs(1);
                            const MAX_WAIT: Duration = Duration::from_secs(300);
                            let wait_start = tokio::time::Instant::now();
                            loop {
                                let status =
                                    validation_controller.status_rx.borrow().clone();
                                if status.completed_iterations() >= 1 {
                                    break;
                                }
                                if wait_start.elapsed() >= MAX_WAIT {
                                    tracing::warn!(
                                        completed = status.completed_iterations(),
                                        "Timed out waiting for at least 1 query set iteration after ETL completion"
                                    );
                                    break;
                                }
                                tokio::time::sleep(POLL_INTERVAL).await;
                            }
                        }
                        println!("Stopping benchmark...");
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

    // Stop SUT metrics scraper and flush its pipeline
    sut_scraper_token.cancel();
    if let Some(handle) = sut_scraper_handle
        && let Ok(Some(last_sut_metrics)) = handle.await
    {
        println!(
            "Final SUT metrics: cpu_sec={:?}, mem={:?}B, ingested_rows={:?}, ingested_bytes={:?}",
            last_sut_metrics.resource.cpu_usage_percent,
            last_sut_metrics.resource.memory_usage_bytes,
            last_sut_metrics.ingestion.rows_ingested,
            last_sut_metrics.ingestion.bytes_ingested,
        );
    }
    if let Some(pipeline) = sut_pipeline {
        pipeline.shutdown();
    }

    // Stop freshness scraper and emit raw E2E latency samples.
    // Percentile calculation is performed in dashboard queries.
    // Only active when checkpoint validation is NOT enabled.
    e2e_latency_token.cancel();
    if let Some(handle) = e2e_latency_handle
        && let Ok(samples_by_table) = handle.await
    {
        let mut total_samples = 0usize;
        for (table_name, samples) in &samples_by_table {
            if !samples.is_empty() {
                total_samples += samples.len();
                let attrs = vec![KeyValue::new("table_name", table_name.clone())];
                for sample in samples {
                    crate::metrics::E2E_LATENCY_MS.record(*sample, &attrs);
                }
            }
        }
        if total_samples > 0 {
            println!("Recorded {total_samples} E2E latency samples");
        }
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
