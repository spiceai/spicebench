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

use crate::{args::RunArgs, commands::adbc_executor, scenario::Scenario};
use arrow::array::RecordBatch;
use data_generation::version::VersionMetadata;
use etl::{ETLPipeline, PipelineState, StopReason};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use system_adapter_protocol::MetricsResponse;
use test_framework::{
    TestType, anyhow,
    arrow::util::pretty::print_batches,
    execution::QueryExecutor,
    metrics::{
        MetricCollector, NoExtendedMetrics, QueryMetrics, QueryStatus, RunOutcome,
        StatisticsCollector,
    },
    opentelemetry::KeyValue,
    opentelemetry::metrics::{Counter, Gauge},
    opentelemetry_sdk::Resource,
    queries::validation::{self, QueryValidationResult},
    spicetest::datasets::create_validation_channels,
    spicetest::{SpiceTest, datasets::NotStarted},
    telemetry::SutMetricsPipeline,
    telemetry::streaming::StreamingOtlpExporter,
};
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
    ingestion_rows_per_sec: Gauge<f64>,
}

fn run_metric_attributes(
    common_args: &RunArgs,
    run_id: uuid::Uuid,
    etl_type: &str,
) -> Vec<KeyValue> {
    vec![
        KeyValue::new(
            "executor_instance_type",
            common_args.executor_instance_type.clone(),
        ),
        KeyValue::new("run_id", run_id.to_string()),
        KeyValue::new("etl_type", etl_type.to_string()),
    ]
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
        instruments.ingestion_rows_per_sec.record(v, attributes);
    } else if let Some(current_rows) = response.ingestion.rows_ingested
        && let Some(prev_rows) = *prev_rows_ingested
        && let Some(prev_time) = *last_scrape_time
    {
        let elapsed_secs = prev_time.elapsed().as_secs_f64();
        if elapsed_secs > 0.0 {
            let rows_per_sec = current_rows.saturating_sub(prev_rows) as f64 / elapsed_secs;
            instruments
                .ingestion_rows_per_sec
                .record(rows_per_sec, attributes);
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
    if let Some(v) = response.resource.num_compute_nodes {
        crate::metrics::NUM_COMPUTE_NODES.record(v, attributes);
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
    attributes: Arc<std::sync::RwLock<Vec<KeyValue>>>,
    instruments: SutInstruments,
) -> tokio::task::JoinHandle<Option<MetricsResponse>> {
    // Bound every scrape so a wedged adapter call can neither hold the adapter
    // lock indefinitely nor stop this task from observing shutdown. A periodic
    // scrape should be quick; the final scrape may legitimately run a Query
    // History marker-wait before summing I/O, so it gets a larger budget.
    const PERIODIC_SCRAPE_TIMEOUT: Duration = Duration::from_secs(180);
    const FINAL_SCRAPE_TIMEOUT: Duration = Duration::from_secs(780);
    // A periodic scrape slower than this holds the shared adapter connection long
    // enough to perturb ingest; treat it as "slow" and back off (below).
    const SLOW_SCRAPE_THRESHOLD: Duration = Duration::from_millis(500);
    // Cap the Fibonacci backoff so SUT metrics never go dark for more than this
    // many ticks between samples.
    const MAX_BACKOFF_TICKS: u64 = 13;

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
        // Fibonacci backoff state: after a slow scrape, skip an increasing number
        // of ticks (fib_a) before scraping again; a fast scrape resets it.
        let mut fib_a: u64 = 1;
        let mut fib_b: u64 = 1;
        let mut skip_remaining: u64 = 0;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if skip_remaining > 0 {
                        // Backing off after a slow scrape — skip this tick so
                        // metrics collection stops adding latency to the run.
                        skip_remaining -= 1;
                        continue;
                    }
                    let scrape_started = std::time::Instant::now();
                    let metrics_result = tokio::time::timeout(
                        PERIODIC_SCRAPE_TIMEOUT,
                        async { adapter.lock().await.metrics(run_id, false).await },
                    )
                    .await;
                    match metrics_result {
                        Ok(Ok(resp)) => {
                            let attrs = attributes.read().expect("SUT attributes lock poisoned");
                            log_sut_metrics_snapshot(&resp);
                            record_sut_metrics(
                                &resp,
                                &instruments,
                                &attrs,
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
                        Ok(Err(e)) => {
                            eprintln!("SUT metrics scrape failed: {e}");
                        }
                        Err(_) => {
                            eprintln!(
                                "SUT metrics scrape timed out after {}s, skipping",
                                PERIODIC_SCRAPE_TIMEOUT.as_secs()
                            );
                        }
                    }
                    // A slow scrape holds the adapter connection the whole time,
                    // adding latency to ingest. Skip an increasing (Fibonacci)
                    // number of subsequent ticks before scraping again; a fast
                    // scrape resets the backoff.
                    if scrape_started.elapsed() > SLOW_SCRAPE_THRESHOLD {
                        skip_remaining = fib_a;
                        let next = fib_a.saturating_add(fib_b).min(MAX_BACKOFF_TICKS);
                        fib_a = fib_b;
                        fib_b = next;
                    } else {
                        fib_a = 1;
                        fib_b = 1;
                    }
                }
                () = token.cancelled() => {
                    // Final scrape before exiting. Bounded so cancellation can
                    // never leave this task (and the shutdown join) hanging.
                    let final_result = tokio::time::timeout(
                        FINAL_SCRAPE_TIMEOUT,
                        async { adapter.lock().await.metrics(run_id, true).await },
                    )
                    .await;
                    match final_result {
                        Ok(Ok(resp)) => {
                            let attrs = attributes.read().expect("SUT attributes lock poisoned");
                            log_sut_metrics_snapshot(&resp);
                            record_sut_metrics(
                                &resp,
                                &instruments,
                                &attrs,
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
                        Ok(Err(e)) => {
                            eprintln!("Final SUT metrics scrape failed: {e}");
                        }
                        Err(_) => {
                            eprintln!(
                                "Final SUT metrics scrape timed out after {}s, abandoning",
                                FINAL_SCRAPE_TIMEOUT.as_secs()
                            );
                        }
                    }
                    break;
                }
            }
        }
        last_response
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

fn load_checkpoint_row_counts(
    checkpoint_dir: &Path,
    checkpoint_idx: usize,
) -> anyhow::Result<HashMap<String, usize>> {
    let idx_dir = checkpoint_dir.join(checkpoint_idx.to_string());
    let row_counts_path = idx_dir.join("row_counts.json");
    if !row_counts_path.exists() {
        return Ok(HashMap::new());
    }

    Ok(serde_json::from_slice(&std::fs::read(row_counts_path)?)?)
}

fn checkpoint_count_query(
    table_name: &str,
    query_catalog_namespace: Option<&str>,
) -> anyhow::Result<test_framework::queries::Query> {
    let query = test_framework::queries::Query::new(
        format!("checkpoint_row_count_{table_name}").into(),
        format!("SELECT COUNT(*) AS row_count FROM {table_name}").into(),
        false,
    );

    match query_catalog_namespace.map(str::trim) {
        Some(namespace) if !namespace.is_empty() => query.rewrite_with_reference_schema(namespace),
        _ => Ok(query),
    }
}

fn extract_row_count_from_batches(batches: &[RecordBatch]) -> anyhow::Result<usize> {
    let batch = batches
        .first()
        .ok_or_else(|| anyhow::anyhow!("row count query returned no batches"))?;
    if batch.num_rows() == 0 || batch.num_columns() == 0 {
        anyhow::bail!("row count query returned an empty result set");
    }

    let value = validation::array_value_to_string(batch.column(0).as_ref(), 0)?
        .ok_or_else(|| anyhow::anyhow!("row count query returned NULL"))?;
    value
        .parse::<usize>()
        .map_err(|e| anyhow::anyhow!("failed to parse row count '{value}': {e}"))
}

async fn validate_checkpoint_table_row_counts(
    executor: &dyn QueryExecutor,
    expected_row_counts: &HashMap<String, usize>,
    checkpoint_idx: usize,
    query_catalog_namespace: Option<&str>,
) -> bool {
    if expected_row_counts.is_empty() {
        println!(
            "Checkpoint {checkpoint_idx}: no table row counts found, skipping row-count validation"
        );
        return true;
    }

    let mut tables: Vec<_> = expected_row_counts.iter().collect();
    tables.sort_by(|(left, _), (right, _)| left.cmp(right));

    for (table_name, expected_count) in tables {
        let query = match checkpoint_count_query(table_name, query_catalog_namespace) {
            Ok(query) => query,
            Err(err) => {
                eprintln!(
                    "Checkpoint {checkpoint_idx}: failed to build row count query for table '{table_name}': {err}"
                );
                return false;
            }
        };
        let result = match executor.execute(&query).await {
            Ok(result) => result,
            Err(err) => {
                eprintln!(
                    "Checkpoint {checkpoint_idx}: row count query for table '{table_name}' failed: {err}"
                );
                return false;
            }
        };

        let Some(batches) = result.batches.as_ref() else {
            eprintln!(
                "Checkpoint {checkpoint_idx}: row count query for table '{table_name}' did not return batches"
            );
            return false;
        };

        let actual_count = match extract_row_count_from_batches(batches) {
            Ok(count) => count,
            Err(err) => {
                eprintln!(
                    "Checkpoint {checkpoint_idx}: failed to read row count for table '{table_name}': {err}"
                );
                return false;
            }
        };

        if actual_count != *expected_count {
            eprintln!(
                "Checkpoint {checkpoint_idx}: table '{table_name}' row count mismatch: expected {expected_count}, actual {actual_count}"
            );
            return false;
        }
    }

    println!("Checkpoint {checkpoint_idx}: table row counts passed, validating full query set");
    true
}

/// Result of a checkpoint validation window.
enum CheckpointValidationResult {
    /// Validation converged — full query set passed.
    Converged {
        /// E2E latency in milliseconds (send_time of first passing Q1 batch
        /// relative to checkpoint pause time).
        e2e_latency_ms: f64,
    },
    /// Validation timed out before convergence.
    TimedOut,
    /// Validation was interrupted by the user (ctrl-c).
    Interrupted,
}

/// Outcome of a single probe phase iteration.
enum ProbeOutcome {
    /// A probe query returned correct results; carries the send_time of the
    /// earliest passing dispatch.
    Passed(std::time::Instant),
    /// The deadline was reached before any probe passed.
    TimedOut,
    /// The user pressed ctrl-c.
    Interrupted,
}

/// An in-flight probe query carrying the instant it was dispatched.
struct ProbeFlight {
    send_time: std::time::Instant,
    result: tokio::task::JoinHandle<anyhow::Result<(bool, Vec<RecordBatch>)>>,
}

/// Maximum number of probe queries that may be in-flight simultaneously.
///
/// Probes run via `spawn_blocking` and each one holds a pool connection for
/// the duration of its query.  Capping in-flight probes prevents them from
/// saturating the connection pool and starving the test workers.  When the cap
/// is reached, new dispatches are skipped until an existing probe completes —
/// a natural form of backoff when the system is under load.
const MAX_PROBE_IN_FLIGHT: usize = 5;

/// Dispatch probe queries until one returns correct results, then return its
/// send_time.
///
/// Every tick of `ticker`, a single probe query is dispatched **if** fewer than
/// [`MAX_PROBE_IN_FLIGHT`] probes are currently outstanding.  When the system
/// is responsive, probes are dispatched at the configured period; when the
/// system is slow (probes take longer), the effective rate naturally decreases
/// because we won't dispatch past the cap.
///
/// `probe_count` is carried across retries so log output is monotonic.
#[expect(clippy::too_many_arguments)]
async fn probe_until_pass(
    executor: &dyn QueryExecutor,
    probe_query: &test_framework::queries::Query,
    probe_expected: &[RecordBatch],
    checkpoint_idx: usize,
    ticker: &mut tokio::time::Interval,
    deadline: tokio::time::Instant,
    probe_count: &mut u64,
    shutdown: &CancellationToken,
) -> ProbeOutcome {
    let mut in_flight: Vec<ProbeFlight> = Vec::new();

    loop {
        if tokio::time::Instant::now() >= deadline {
            return ProbeOutcome::TimedOut;
        }

        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return ProbeOutcome::Interrupted,
            _ = ticker.tick() => {}
        };

        // Only dispatch a new probe if below the in-flight cap.
        if in_flight.len() < MAX_PROBE_IN_FLIGHT {
            let send_time = std::time::Instant::now();
            *probe_count += 1;
            let exec = executor.clone_box();
            let query = probe_query.clone();
            let handle = tokio::spawn(async move {
                match exec.execute(&query).await {
                    Ok(result) => Ok((true, result.batches.unwrap_or_default())),
                    Err(_) => Ok((false, Vec::new())),
                }
            });
            in_flight.push(ProbeFlight {
                send_time,
                result: handle,
            });

            println!(
                "Checkpoint {checkpoint_idx}: probe #{} dispatched, {} in-flight",
                *probe_count,
                in_flight.len()
            );
        }

        // Drain completed probes.
        let mut i = 0;
        while i < in_flight.len() {
            if in_flight[i].result.is_finished() {
                let flight = in_flight.swap_remove(i);
                if let Ok(Ok((executed, batches))) = flight.result.await
                    && executed
                {
                    let valid = validation::validate_with_expected_batches(
                        &probe_query.name,
                        &batches,
                        probe_expected,
                    );
                    if matches!(valid, Ok(QueryValidationResult::Pass)) {
                        // At most MAX_PROBE_IN_FLIGHT - 1 probes remain;
                        // wait for them so their pool connections are returned
                        // before the caller starts full query-set validation.
                        for probe in in_flight {
                            let _ = probe.result.await;
                        }
                        return ProbeOutcome::Passed(flight.send_time);
                    }
                }
                // Don't increment i — swap_remove moved the last element here
            } else {
                i += 1;
            }
        }
    }
}

/// Run every query in the scenario and validate results.
///
/// At most `concurrency` queries execute in parallel. Returns `true` if all
/// queries with expected results pass validation. On failure, logs details to
/// stderr and returns `false`.
async fn validate_full_query_set(
    executor: &dyn QueryExecutor,
    queries: &[test_framework::queries::Query],
    expected_results: &HashMap<Arc<str>, Vec<RecordBatch>>,
    checkpoint_idx: usize,
    concurrency: usize,
) -> bool {
    use futures::stream::{self, StreamExt};

    let results: Vec<(Arc<str>, _)> = stream::iter(queries)
        .map(|query| {
            let query_name = Arc::clone(&query.name);
            let exec = executor.clone_box();
            let q = query.clone();
            async move {
                let result = exec.execute(&q).await;
                (query_name, result)
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    let mut all_passed = true;
    let mut fail_details: Vec<String> = Vec::new();

    // tpch_q6 uses decimal arithmetic that produces Float64 from Cayenne but
    // Decimal128 from the reference; skip result validation to avoid false failures.
    const RESULT_VALIDATION_SKIP: &[&str] = &["tpch_q6"];

    for (query_name, result) in &results {
        if RESULT_VALIDATION_SKIP.contains(&query_name.as_ref()) {
            continue;
        }
        match result {
            Ok(exec_result) => {
                if let Some(expected) = expected_results.get(query_name) {
                    let batches = exec_result.batches.as_deref().unwrap_or_default();
                    let valid =
                        validation::validate_with_expected_batches(query_name, batches, expected);
                    match valid {
                        Ok(QueryValidationResult::Pass) => {}
                        Ok(QueryValidationResult::Fail(reason)) => {
                            all_passed = false;
                            fail_details.push(format!("  FAIL - query '{query_name}': {reason:?}"));
                        }
                        Err(e) => {
                            all_passed = false;
                            fail_details.push(format!("  ERROR - query '{query_name}': {e}"));
                        }
                    }
                }
                // Queries without expected results are skipped (pass by default)
            }
            Err(e) => {
                all_passed = false;
                fail_details.push(format!("  ERROR - query '{query_name}': {e}"));
            }
        }
    }

    if !all_passed {
        eprintln!(
            "Checkpoint {checkpoint_idx}: full query set validation failed, returning to probe phase",
        );
        for detail in &fail_details {
            eprintln!("{detail}");
        }
    }

    all_passed
}

/// Run checkpoint validation for a single checkpoint boundary.
///
/// Alternates between two phases until convergence or timeout:
///
/// **Phase 1 (probe):** Every `probe_period`, dispatch a single probe of the
/// first scenario query, up to [`MAX_PROBE_IN_FLIGHT`] concurrent probes.
/// When the cap is reached, new dispatches are skipped until an existing probe
/// completes — naturally backing off when the system is slow.  When a probe
/// returns correct results, record its **send time**.
///
/// **Phase 2 (validate):** Run the full query set concurrently. If every query
/// passes, the checkpoint has converged and E2E latency =
/// `send_time − checkpoint_pause_time`. If any query fails, return to phase 1
/// to re-probe for a new send_time.
#[expect(clippy::too_many_arguments)]
async fn run_checkpoint_validation(
    executor: &dyn QueryExecutor,
    queries: &[test_framework::queries::Query],
    expected_results: &HashMap<Arc<str>, Vec<RecordBatch>>,
    expected_row_counts: &HashMap<String, usize>,
    checkpoint_idx: usize,
    concurrency: usize,
    probe_period: Duration,
    max_wait: Duration,
    checkpoint_pause_time: std::time::Instant,
    query_catalog_namespace: Option<&str>,
    shutdown: &CancellationToken,
) -> CheckpointValidationResult {
    let deadline = tokio::time::Instant::now() + max_wait;

    // Use query2 as the probe query.  Query1 is too slow.
    let probe_query = &queries[1];
    let Some(probe_expected) = expected_results.get(&probe_query.name) else {
        eprintln!(
            "Checkpoint {checkpoint_idx}: no expected results for probe query '{}', skipping validation",
            probe_query.name
        );
        return CheckpointValidationResult::TimedOut;
    };

    // Phase 0: validate table row counts first as a fast correctness probe.
    // Row count queries are cheap and immediately surface data loss/duplication
    // without waiting for expensive analytical queries to converge.
    println!("Checkpoint {checkpoint_idx}: validating table row counts before probing queries",);
    {
        let mut row_count_ticker = tokio::time::interval(probe_period);
        let mut row_count_attempt = 0u64;
        loop {
            if tokio::time::Instant::now() >= deadline {
                println!(
                    "Checkpoint {checkpoint_idx}: row count validation timed out after {} attempts",
                    row_count_attempt
                );
                return CheckpointValidationResult::TimedOut;
            }

            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return CheckpointValidationResult::Interrupted,
                _ = row_count_ticker.tick() => {}
            };

            row_count_attempt += 1;
            if validate_checkpoint_table_row_counts(
                executor,
                expected_row_counts,
                checkpoint_idx,
                query_catalog_namespace,
            )
            .await
            {
                break;
            }

            println!(
                "Checkpoint {checkpoint_idx}: row count validation attempt {row_count_attempt} failed, retrying",
            );
        }
    }

    println!(
        "Checkpoint {checkpoint_idx}: row counts passed, probing '{}' every {}s",
        probe_query.name,
        probe_period.as_secs()
    );

    let mut ticker = tokio::time::interval(probe_period);
    let mut probe_count = 0u64;

    loop {
        // Phase 1: probe query until it passes
        let e2e_send_time = match probe_until_pass(
            executor,
            probe_query,
            probe_expected,
            checkpoint_idx,
            &mut ticker,
            deadline,
            &mut probe_count,
            shutdown,
        )
        .await
        {
            ProbeOutcome::Passed(send_time) => {
                println!(
                    "Checkpoint {checkpoint_idx}: probe query '{}' passed, validating full query set",
                    probe_query.name
                );
                send_time
            }
            ProbeOutcome::TimedOut => return CheckpointValidationResult::TimedOut,
            ProbeOutcome::Interrupted => return CheckpointValidationResult::Interrupted,
        };

        // Phase 2: validate the full query set
        if tokio::time::Instant::now() >= deadline {
            return CheckpointValidationResult::TimedOut;
        }

        if validate_full_query_set(
            executor,
            queries,
            expected_results,
            checkpoint_idx,
            concurrency,
        )
        .await
        {
            let latency_ms = e2e_send_time
                .duration_since(checkpoint_pause_time)
                .as_secs_f64()
                * 1000.0;
            println!(
                "Checkpoint {checkpoint_idx} converged: E2E latency = {:.1}s (send_time of first passing probe)",
                latency_ms / 1000.0
            );
            return CheckpointValidationResult::Converged {
                e2e_latency_ms: latency_ms,
            };
        }
    }
}

#[expect(clippy::too_many_lines)]
#[expect(clippy::too_many_arguments)]
pub(crate) async fn run(
    system_adapter_client: Arc<Mutex<system_adapter_protocol::Client>>,
    run_id: uuid::Uuid,
    scenario: &Scenario,
    common_args: &RunArgs,
    version_metadata: &VersionMetadata,
    read_pool: adbc_client::AdbcConnectionPool,
    etl_pipeline: &mut ETLPipeline,
    checkpoint_steps: Option<usize>,
    checkpoint_dir: Option<&Path>,
    query_catalog_namespace: Option<String>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let metric_attributes = run_metric_attributes(common_args, run_id, version_metadata.etl_type());

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
            KeyValue::new("etl_type", version_metadata.etl_type()),
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
    let (sut_scraper_handle, sut_pipeline, sut_shared_attributes) = if common_args
        .scrape_sut_metrics
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
            ingestion_rows_per_sec: m.f64_gauge("ingestion_rows_per_sec").build(),
        };
        let sut_attributes = Arc::new(std::sync::RwLock::new(metric_attributes.clone()));
        println!("SUT metrics scraping enabled (run_id={run_id})");
        (
            Some(spawn_sut_metrics_scraper(
                system_adapter_client,
                run_id,
                sut_scraper_token.clone(),
                Duration::from_secs(5),
                Arc::clone(&sut_attributes),
                instruments,
            )),
            Some(sut_pipeline),
            Some(sut_attributes),
        )
    } else {
        (None, None, None)
    };

    // ACTIVE_CONNECTIONS is recorded post-loop so it carries the `outcome` dimension.

    let mut test_builder = NotStarted::new()
        .with_parallel_count(common_args.concurrency)
        .with_end_condition(load_end_condition)
        .with_query_executor(executor);

    // Add streaming metrics sender if exporter is configured
    if let Some(exporter) = &streaming_exporter {
        test_builder = test_builder.with_streaming_metrics(exporter.sender());
    }

    // Always create validation channels so we can track query-set iteration
    // completions (used to wait for at least 1 iteration before stopping).
    let (validation_controller, validation_worker_handles) = create_validation_channels();
    test_builder = test_builder.with_checkpoint_validation(validation_worker_handles);

    let has_checkpoint_validation =
        common_args.validate_results && checkpoint_steps.is_some() && checkpoint_dir.is_some();

    // Create a dedicated executor for checkpoint validation (separate from the test workers).
    let validation_executor = adbc_executor::AdbcDirectQueryExecutor::new(read_pool.clone());

    let (query_set, test_builder) = super::build_test_with_validation(
        scenario,
        test_builder,
        query_catalog_namespace.as_deref(),
    )
    .await?;

    // Build ordered query names for mapping checkpoint query_idx → query name.
    // Rewrite queries with catalog namespace so the validation executor
    // uses the same table references as the test workers.
    let queries = super::rewrite_queries_with_catalog_namespace(
        query_set.get_queries(None, None, None).await?,
        query_catalog_namespace.as_deref(),
    )?;
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
    // Collect checkpoint E2E latency samples during the loop, then emit
    // them post-loop so they carry the final `outcome` dimension.
    let mut checkpoint_e2e_latency_samples: Vec<f64> = Vec::new();

    let run_outcome: Option<RunOutcome> = loop {
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
                                    let expected_row_counts =
                                        load_checkpoint_row_counts(cp_dir, checkpoint_idx)
                                            .unwrap_or_default();
                                    tracing::info!(
                                        checkpoint_idx,
                                        num_queries = expected_results.len(),
                                        num_tables = expected_row_counts.len(),
                                        "Running checkpoint validation"
                                    );

                                    let checkpoint_pause_time = std::time::Instant::now();

                                    let result = run_checkpoint_validation(
                                        &validation_executor,
                                        &queries,
                                        &expected_results,
                                        &expected_row_counts,
                                        checkpoint_idx,
                                        common_args.concurrency,
                                        Duration::from_secs(common_args.checkpoint_validation_period),
                                        Duration::from_secs(common_args.checkpoint_validation_timeout),
                                        checkpoint_pause_time,
                                        query_catalog_namespace.as_deref(),
                                        &shutdown,
                                    )
                                    .await;

                                    match result {
                                        CheckpointValidationResult::Converged { e2e_latency_ms } => {
                                            checkpoint_e2e_latency_samples.push(e2e_latency_ms);
                                        }
                                        CheckpointValidationResult::Interrupted => {
                                            eprintln!("Interrupt received during checkpoint validation, stopping...");
                                            shutdown_token.cancel();
                                            etl_pipeline.cancel();
                                            break Some(RunOutcome::Cancelled);
                                        }
                                        CheckpointValidationResult::TimedOut => {
                                            eprintln!(
                                                "Checkpoint {checkpoint_idx} validation timed out after 600s without convergence, aborting run"
                                            );
                                            shutdown_token.cancel();
                                            etl_pipeline.cancel();
                                            break Some(RunOutcome::ValidationTimeout);
                                        }
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
                            break Some(RunOutcome::PipelineFailure(format!("Failed to continue ETL pipeline: {e}")));
                        }
                        tracing::info!("ETL pipeline resumed");
                    }
                    PipelineState::Stopped(StopReason::Completed) => {
                        println!("ETL pipeline completed");

                        // --- Final checkpoint validation ---
                        // The pipeline transitions directly from Running →
                        // Completed after the last batch, so the final
                        // checkpoint boundary is never seen as a Paused state.
                        // Run validation for it here.
                        if has_checkpoint_validation
                            && let Some(cp_dir) = checkpoint_dir
                        {
                            let checkpoint_idx = etl_pipeline.checkpoint_idx();
                            match load_checkpoint_results(cp_dir, checkpoint_idx, &query_names) {
                                Ok(expected_results) if !expected_results.is_empty() => {
                                    let expected_row_counts =
                                        load_checkpoint_row_counts(cp_dir, checkpoint_idx)
                                            .unwrap_or_default();
                                    tracing::info!(
                                        checkpoint_idx,
                                        num_queries = expected_results.len(),
                                        num_tables = expected_row_counts.len(),
                                        "Running final checkpoint validation"
                                    );

                                    let checkpoint_pause_time = std::time::Instant::now();

                                    let result = run_checkpoint_validation(
                                        &validation_executor,
                                        &queries,
                                        &expected_results,
                                        &expected_row_counts,
                                        checkpoint_idx,
                                        common_args.concurrency,
                                        Duration::from_secs(common_args.checkpoint_validation_period),
                                        Duration::from_secs(common_args.checkpoint_validation_timeout),
                                        checkpoint_pause_time,
                                        query_catalog_namespace.as_deref(),
                                        &shutdown,
                                    )
                                    .await;

                                    match result {
                                        CheckpointValidationResult::Converged { e2e_latency_ms } => {
                                            checkpoint_e2e_latency_samples.push(e2e_latency_ms);
                                        }
                                        CheckpointValidationResult::Interrupted => {
                                            eprintln!("Interrupt received during final checkpoint validation, stopping...");
                                            shutdown_token.cancel();
                                            break Some(RunOutcome::Cancelled);
                                        }
                                        CheckpointValidationResult::TimedOut => {
                                            eprintln!(
                                                "Final checkpoint {checkpoint_idx} validation timed out after 600s without convergence, aborting run"
                                            );
                                            shutdown_token.cancel();
                                            break Some(RunOutcome::ValidationTimeout);
                                        }
                                    }
                                }
                                Ok(_) => {
                                    tracing::info!(
                                        checkpoint_idx,
                                        "No final checkpoint results found, skipping validation"
                                    );
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        checkpoint_idx,
                                        error = %e,
                                        "Failed to load final checkpoint results, skipping validation"
                                    );
                                }
                            }
                        } else if !has_checkpoint_validation {
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
                                tokio::select! {
                                    _ = tokio::time::sleep(POLL_INTERVAL) => {}
                                    _ = shutdown.cancelled() => break,
                                }
                            }
                        }

                        println!("Stopping benchmark...");
                        shutdown_token.cancel();
                        break None;
                    }
                    PipelineState::Stopped(StopReason::Error(ref e)) => {
                        eprintln!("ETL pipeline failed: {e}");
                        shutdown_token.cancel();
                        break Some(RunOutcome::PipelineFailure(e.clone()));
                    }
                    PipelineState::Stopped(StopReason::Cancelled) => {
                        println!("ETL pipeline was cancelled, stopping benchmark...");
                        shutdown_token.cancel();
                        break None;
                    }
                    _ => { /* still running, keep waiting */ }
                }
            }
            // SIGINT/SIGTERM: stop everything
            _ = shutdown.cancelled() => {
                println!("Interrupt received, stopping benchmark...");
                shutdown_token.cancel();
                etl_pipeline.cancel();
                break Some(RunOutcome::Cancelled);
            }
        }
    };

    let test = match test_future.await {
        Ok(test) => test,
        Err(e) => {
            return Err(e);
        }
    };

    // Determine the run outcome from the loop exit reason and test results.
    // This is resolved before recording any metrics so every emitted metric
    // carries the `outcome` dimension.
    let outcome = match &run_outcome {
        Some(outcome) => outcome.clone(),
        None if test.succeeded() => RunOutcome::Success,
        None => RunOutcome::QueryFailure,
    };

    // Add outcome as a dimension on all subsequently recorded metrics.
    let mut metric_attributes = metric_attributes;
    metric_attributes.push(KeyValue::new("outcome", outcome.as_str()));

    // Record deferred metrics now that outcome is available.
    crate::metrics::ACTIVE_CONNECTIONS.record(
        common_args.concurrency.try_into().unwrap_or(0),
        &metric_attributes,
    );
    for sample in &checkpoint_e2e_latency_samples {
        crate::metrics::E2E_LATENCY_MS.record(*sample, &metric_attributes);
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

    // Inject outcome into shared SUT attributes so the final scrape carries it.
    if let Some(ref sut_attrs) = sut_shared_attributes {
        sut_attrs
            .write()
            .expect("SUT attributes lock poisoned")
            .push(KeyValue::new("outcome", outcome.as_str()));
    }
    // Stop SUT metrics scraper and flush its pipeline. The scraper runs a final
    // (bounded) scrape on cancellation, but shutdown must never hang waiting for
    // it: if the join overruns, abort the task and continue without final SUT
    // metrics rather than wedging the whole run.
    sut_scraper_token.cancel();
    if let Some(mut handle) = sut_scraper_handle {
        const SCRAPER_JOIN_TIMEOUT: Duration = Duration::from_secs(840);
        match tokio::time::timeout(SCRAPER_JOIN_TIMEOUT, &mut handle).await {
            Ok(Ok(Some(last_sut_metrics))) => {
                println!(
                    "Final SUT metrics: cpu_sec={:?}, mem={:?}B, ingested_rows={:?}, ingested_bytes={:?}",
                    last_sut_metrics.resource.cpu_usage_percent,
                    last_sut_metrics.resource.memory_usage_bytes,
                    last_sut_metrics.ingestion.rows_ingested,
                    last_sut_metrics.ingestion.bytes_ingested,
                );
            }
            Ok(Ok(None)) => {}
            Ok(Err(e)) => eprintln!("SUT metrics scraper task failed: {e}"),
            Err(_) => {
                eprintln!(
                    "SUT metrics scraper did not finish within {}s; aborting it and continuing shutdown",
                    SCRAPER_JOIN_TIMEOUT.as_secs()
                );
                handle.abort();
            }
        }
    }
    if let Some(pipeline) = sut_pipeline {
        pipeline.shutdown();
    }

    println!("{}", vec!["-"; 30].join(""));
    println!("Benchmark metrics:");
    let records = metrics.build_records()?;
    print_batches(&records)?;

    // Shutdown streaming exporter before emitting final telemetry
    if let Some(exporter) = streaming_exporter {
        exporter.shutdown().await;
    }

    println!("Benchmark completed (outcome: {outcome})");

    // Always emit telemetry — even on failure — so the outcome dimension is recorded.
    telemetry.emit().await?;

    // Propagate failure after telemetry has been emitted.
    if let Some(failure_outcome) = run_outcome {
        return Err(anyhow::anyhow!("Benchmark run failed: {failure_outcome}"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::checkpoint_count_query;

    #[test]
    fn checkpoint_count_query_rewrites_catalog_namespace_without_double_quotes() {
        let query = checkpoint_count_query("customer", Some("spiceai_sandbox.spicebench"))
            .expect("checkpoint row-count query should be built");

        assert_eq!(
            query.sql.as_ref(),
            "SELECT COUNT(*) AS row_count FROM spiceai_sandbox.spicebench.customer"
        );
        assert!(!query.sql.contains('"'));
    }

    #[test]
    fn checkpoint_count_query_without_namespace_uses_plain_table_identifier() {
        let query = checkpoint_count_query("customer", None)
            .expect("checkpoint row-count query should be built");

        assert_eq!(
            query.sql.as_ref(),
            "SELECT COUNT(*) AS row_count FROM customer"
        );
    }
}
