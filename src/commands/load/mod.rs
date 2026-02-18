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

use super::get_app_and_start_request;
use crate::{args::BenchRunArgs, health::HealthMonitor};
use std::sync::Arc;
use std::time::Duration;
use system_adapter_protocol::MetricsResponse;
use test_framework::{
    TestType, anyhow,
    app::AppBuilder,
    arrow::util::pretty::print_batches,
    git,
    metrics::{MetricCollector, NoExtendedMetrics, QueryMetrics, QueryStatus, StatisticsCollector},
    opentelemetry::KeyValue,
    opentelemetry_sdk::Resource,
    spiced::SpicedInstance,
    spicepod::Spicepod,
    spicetest::{
        SpiceTest,
        datasets::{EndCondition, NotStarted},
    },
    telemetry::streaming::StreamingOtlpExporter,
    utils::observe_memory,
};
use tokio::signal;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// Record the latest SUT metrics snapshot as OTel gauge values.
fn record_sut_metrics(response: &MetricsResponse) {
    // Resource metrics
    if let Some(cpu) = response.resource.cpu_usage_percent {
        crate::metrics::SUT_CPU_USAGE_PERCENT.record(cpu, &[]);
    }
    if let Some(mem) = response.resource.memory_usage_bytes {
        crate::metrics::SUT_MEMORY_USAGE_BYTES.record(mem, &[]);
    }
    if let Some(v) = response.resource.disk_read_bytes {
        crate::metrics::SUT_DISK_READ_BYTES.record(v, &[]);
    }
    if let Some(v) = response.resource.disk_write_bytes {
        crate::metrics::SUT_DISK_WRITE_BYTES.record(v, &[]);
    }
    if let Some(v) = response.resource.disk_read_iops {
        crate::metrics::SUT_DISK_READ_IOPS.record(v, &[]);
    }
    if let Some(v) = response.resource.disk_write_iops {
        crate::metrics::SUT_DISK_WRITE_IOPS.record(v, &[]);
    }

    // Ingestion metrics
    if let Some(v) = response.ingestion.rows_ingested {
        crate::metrics::INGESTION_ROWS_TOTAL.record(v, &[]);
    }
    if let Some(v) = response.ingestion.bytes_ingested {
        crate::metrics::INGESTION_BYTES_TOTAL.record(v, &[]);
    }
    if let Some(v) = response.ingestion.rows_per_sec {
        crate::metrics::INGESTION_ROWS_PER_SEC.record(v, &[]);
    }
    if let Some(v) = response.ingestion.active_connections {
        crate::metrics::ACTIVE_CONNECTIONS.record(v, &[]);
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
) -> tokio::task::JoinHandle<Option<MetricsResponse>> {
    tokio::spawn(async move {
        let mut last_response: Option<MetricsResponse> = None;
        let mut ticker = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    match adapter.lock().await.metrics(run_id).await {
                        Ok(resp) => {
                            record_sut_metrics(&resp);
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
                        record_sut_metrics(&resp);
                        last_response = Some(resp);
                    }
                    break;
                }
            }
        }
        last_response
    })
}

#[expect(clippy::too_many_lines)]
pub(crate) async fn run(
    args: &BenchRunArgs,
    adbc_conn: Option<adbc_client::AdbcConnection>,
) -> anyhow::Result<()> {
    if args.test_args.common.concurrency < 2 {
        return Err(anyhow::anyhow!(
            "Concurrency should be greater than 1 for a load test"
        ));
    }

    // Check if connecting to an external instance or starting a new one
    let (app, mut spiced_instance) = if args.test_args.common.is_external_instance() {
        println!(
            "Connecting to external spiced instance at: {}",
            args.test_args.common.spiced_path
        );
        let spicepod = Spicepod::load_exact(args.test_args.common.spicepod_path.clone()).await?;
        let app = AppBuilder::new(spicepod.name.clone())
            .with_spicepod(spicepod)
            .build();
        let instance = SpicedInstance::external(&args.test_args.common.spiced_path);
        (app, instance)
    } else {
        let (app, start_request) = get_app_and_start_request(&args.test_args.common).await?;
        let instance = SpicedInstance::start(start_request).await?;
        (app, instance)
    };

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.test_args.common.ready_wait))
        .await?;

    // Build resource with attributes known upfront, before creating telemetry.
    // This ensures the SdkMeterProvider is created with the correct resource,
    // so all metrics (including HealthMonitor) have proper resource attributes.
    let spiced_version = spiced_instance.version().to_string();
    let spiced_commit_sha =
        std::env::var("SPICED_COMMIT").unwrap_or_else(|_| "unknown".to_string());
    let spicebench_commit_sha = git::get_commit_sha();
    let branch_name = git::get_branch_name();
    let spicepod = args.test_args.common.spicepod_path.display().to_string();

    let query_set = args.test_args.load_query_set()?;
    let load_resource = Resource::builder_empty()
        .with_attributes(vec![
            KeyValue::new("service.name", "spicebench"),
            KeyValue::new("type", "load_test"),
            KeyValue::new("name", app.name.clone()),
            KeyValue::new("spiced_version", spiced_version),
            KeyValue::new("query_set", query_set.to_string()),
            KeyValue::new("spicebench_commit_sha", spicebench_commit_sha),
            KeyValue::new("spiced_commit_sha", spiced_commit_sha),
            KeyValue::new("branch_name", branch_name),
            KeyValue::new("concurrency", args.test_args.common.concurrency.to_string()),
            KeyValue::new("spicepod", spicepod),
            KeyValue::new(
                "param_set_variants",
                args.test_args
                    .random_param_set_count
                    .unwrap_or(1)
                    .to_string(),
            ),
            KeyValue::new(
                "protocol",
                if args.test_args.http_clients {
                    "http"
                } else {
                    "flight"
                },
            ),
        ])
        .build();

    // Create telemetry with resource upfront, before any metrics calls
    let telemetry = super::create_telemetry_with_resource(&args.test_args.common, load_resource);

    let health_monitor = HealthMonitor::spawn()?;

    // Create the appropriate query executor based on args
    let executor = super::create_query_executor(&args.test_args, &spiced_instance, adbc_conn).await?;

    // warm up run
    println!("Performing warm up");

    let (baseline_query_set, test_builder) = super::build_test_with_validation(
        &args.test_args,
        NotStarted::new()
            .with_parallel_count(args.test_args.common.concurrency)
            .with_end_condition(EndCondition::QuerySetCompleted(1))
            .with_query_executor(executor.clone()),
    )
    .await?;

    let warm_up = SpiceTest::<NotStarted>::new(app.name.clone(), test_builder)
        .with_spiced_instance(spiced_instance)
        .with_progress_bars(!args.test_args.common.disable_progress_bars)
        .start()?;

    let spiced_instance = warm_up.wait().await?.end()?;

    let test_duration = Duration::from_secs(args.test_args.common.duration);

    // Calculate baseline duration: 10% of target time, min 1min, max 10min
    let baseline_duration_secs = (test_duration.as_secs() / 10).clamp(60, 600);
    let baseline_duration = Duration::from_secs(baseline_duration_secs);

    // baseline run
    println!("Running baseline throughput test for {baseline_duration_secs}s",);

    let (_, test_builder) = super::build_test_with_validation(
        &args.test_args,
        NotStarted::new()
            .with_parallel_count(args.test_args.common.concurrency)
            .with_end_condition(EndCondition::Duration(baseline_duration))
            .with_query_executor(executor.clone()),
    )
    .await?;

    let baseline_test = SpiceTest::new(app.name.clone(), test_builder)
        .with_spiced_instance(spiced_instance)
        .with_progress_bars(!args.test_args.common.disable_progress_bars)
        .start()?;

    let test = baseline_test.wait().await?;
    let baseline_percentiles = test.get_query_durations().percentile(99.0)?;

    let baseline_metrics: QueryMetrics<_, NoExtendedMetrics> = test.collect(TestType::Load)?;
    println!("Baseline metrics:");
    let records = baseline_metrics.build_records()?;
    print_batches(&records)?;
    let spiced_instance = test.end()?;
    let memory_token = CancellationToken::new();
    // Memory monitoring is only available for owned spiced instances (not external)
    let memory_readings = spiced_instance
        .process()
        .ok()
        .map(|p| p.watch_memory(&memory_token));

    // load test
    println!("Running load test");

    let load_end_condition = if args.run_until_stopped {
        EndCondition::Unlimited
    } else {
        EndCondition::Duration(Duration::from_secs(args.test_args.common.duration))
    };

    // Create streaming OTLP exporter if OTLP endpoint is configured
    let streaming_exporter = args
        .test_args
        .common
        .otlp_endpoint
        .as_ref()
        .map(|endpoint| StreamingOtlpExporter::spawn(endpoint.clone()));

    // Spawn SUT metrics scraper if --scrape-sut-metrics is enabled and a system adapter is configured
    let sut_scraper_token = CancellationToken::new();
    let sut_scraper_handle = if args.test_args.common.scrape_sut_metrics
        && (args.test_args.common.system_adapter_stdio_cmd.is_some()
            || args.test_args.common.system_adapter_http_url.is_some())
    {
        let adapter = super::connect_system_adapter(&args.test_args.common).await?;
        let run_id = uuid::Uuid::new_v4();
        println!("SUT metrics scraping enabled (run_id={run_id})");
        Some(spawn_sut_metrics_scraper(
            Arc::new(Mutex::new(adapter)),
            run_id,
            sut_scraper_token.clone(),
            Duration::from_secs(5),
        ))
    } else {
        None
    };

    // Record client concurrency as a gauge
    crate::metrics::ACTIVE_CONNECTIONS.record(
        args.test_args.common.concurrency.try_into().unwrap_or(0),
        &[],
    );

    let mut test_builder = NotStarted::new()
        .with_parallel_count(args.test_args.common.concurrency)
        .with_end_condition(load_end_condition)
        .with_query_executor(executor)
        .with_query_duration_threshold(args.test_args.mark_query_failed_if_exceeds);

    // Add streaming metrics sender if exporter is configured
    if let Some(exporter) = &streaming_exporter {
        test_builder = test_builder.with_streaming_metrics(exporter.sender());
    }

    let (query_set, test_builder) =
        super::build_test_with_validation(&args.test_args, test_builder).await?;

    // Use the same query overrides that were applied in build_test_with_validation
    let query_overrides = args
        .test_args
        .query_overrides
        .clone()
        .map(test_framework::queries::QueryOverrides::from);
    let _queries = query_set.get_queries(query_overrides, None, None).await?;

    let throughput_test = SpiceTest::<NotStarted>::new(app.name.clone(), test_builder)
        .with_spiced_instance(spiced_instance)
        .with_progress_bars(!args.test_args.common.disable_progress_bars)
        .start()?;
    let shutdown_token = throughput_test.cancellation_token();
    let test_future = throughput_test.wait();
    tokio::pin!(test_future);
    let test = match tokio::select! {
        res = &mut test_future => res,
        _ = signal::ctrl_c() => {
            println!("Interrupt received, stopping load test...");
            shutdown_token.cancel();
            test_future.await
        }
    } {
        Ok(test) => test,
        Err(e) => {
            if let Some(readings) = memory_readings {
                let _ = observe_memory(memory_token, readings).await;
            }
            return Err(e);
        }
    };
    let test_durations = test.get_query_durations().statistical_set()?;

    // Get all query durations for overall statistics before ending the test
    let all_durations = test.get_query_durations().clone();
    let all_duration_values: Vec<_> = all_durations.values().flatten().copied().collect();

    let metrics: QueryMetrics<_, NoExtendedMetrics> = test.collect(TestType::Load)?;
    let mut spiced_instance = test.end()?;
    let (max_memory, median_memory) = if let Some(readings) = memory_readings {
        observe_memory(memory_token, readings).await?
    } else {
        println!("Memory monitoring not available for external spiced instances");
        (0.0, 0.0)
    };

    // Record per-query metrics for load test
    for query in &metrics.metrics {
        let query_name = &query.query_name;
        let attributes = vec![KeyValue::new("query_name", query_name.to_string())];

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
        crate::metrics::P99_DURATION.record(overall_p99.as_millis().try_into()?, &[]);
    }
    crate::metrics::TEST_DURATION
        .record((metrics.finished_at - metrics.started_at).try_into()?, &[]);
    crate::metrics::PEAK_MEMORY_USAGE.record(max_memory * 1024.0, &[]);
    crate::metrics::MEDIAN_MEMORY_USAGE.record(median_memory * 1024.0, &[]);

    // Query throughput metrics
    let total_iterations: u64 = metrics
        .metrics
        .iter()
        .map(|q| q.iterations as u64)
        .sum();
    let test_duration_secs =
        (metrics.finished_at - metrics.started_at) as f64 / 1000.0;
    crate::metrics::QUERIES_TOTAL.add(total_iterations, &[]);
    if test_duration_secs > 0.0 {
        let qps = total_iterations as f64 / test_duration_secs;
        crate::metrics::QUERIES_PER_SEC.record(qps, &[]);

        // Efficiency: queries/s normalized by CPU core count
        let cpu_cores = std::thread::available_parallelism()
            .map(|n| n.get() as f64)
            .unwrap_or(1.0);
        if cpu_cores > 0.0 {
            crate::metrics::EFFICIENCY_QUERIES_PER_CORE
                .record(qps / cpu_cores, &[]);
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

    println!("Baseline metrics:");
    let baseline_records = baseline_metrics.build_records()?;
    print_batches(&baseline_records)?;
    println!("{}", vec!["-"; 30].join(""));
    println!("Load test metrics:");
    let records = metrics.with_memory_usage(max_memory).build_records()?;
    print_batches(&records)?;

    let health_report = health_monitor.stop().await;

    // Shutdown streaming exporter before emitting final telemetry
    if let Some(exporter) = streaming_exporter {
        exporter.shutdown().await;
    }

    telemetry.emit().await?;

    spiced_instance.stop()?;
    let health_report = health_report?;

    let mut test_passed = true;
    let mut yellow_measurements = 0;

    // Use baseline_queries that represent unique query names, otherwise the same failure
    // could be reported multiple times for each parameterized query params set variation
    for query in baseline_query_set
        .get_queries(query_overrides, None, None)
        .await?
    {
        let Some(baseline_percentile) = baseline_percentiles.get(&query.name) else {
            // Query Failed, no percentile statistics recorded
            continue;
        };

        let Some(duration) = test_durations.get(&query.name) else {
            return Err(anyhow::anyhow!(
                "Query {} not found in test durations",
                query.name
            ));
        };

        let percentile_99th = duration.percentile(99.0)?;
        if percentile_99th.as_millis() < 1000 {
            continue; // skip queries that are too fast to be meaningful
        }

        let percentile_ratio =
            ((percentile_99th.as_secs_f64() / baseline_percentile.as_secs_f64()) - 1.0) * 100.0;

        // yellow measurements = 10% to 20% increase
        // red measurements = > 20% increase
        let (yellow, red) = (
            percentile_ratio > 10.0 && percentile_ratio <= 20.0,
            percentile_ratio > 20.0,
        );

        if red {
            println!(
                "FAIL - Query {query} has a 99th percentile that increased {percentile_ratio}% of the baseline 99th percentile",
                query = query.name
            );
            test_passed = false;
        } else if yellow {
            println!(
                "WARN - Query {query} has a 99th percentile that increased {percentile_ratio}% of the baseline 99th percentile",
                query = query.name
            );
            yellow_measurements += 1;
        }
    }

    let mut failure_messages = Vec::new();
    if !args.no_error && yellow_measurements >= 3 {
        failure_messages.push("Load test failed due to too many yellow measurements".to_string());
    }
    if !args.no_error && !test_passed {
        failure_messages.push("Load test failed.".to_string());
    }
    if let Some(message) = health_report.failure_message() {
        failure_messages.push(message);
    }

    if !failure_messages.is_empty() {
        return Err(anyhow::anyhow!(failure_messages.join("\n")));
    }

    println!("Load test completed");
    Ok(())
}
