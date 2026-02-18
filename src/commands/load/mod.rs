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
use std::sync::Arc;
use std::time::Duration;
use etl::ETLPipeline;
use system_adapter_protocol::MetricsResponse;
use test_framework::{
    TestType, anyhow,
    arrow::util::pretty::print_batches,
    metrics::{MetricCollector, NoExtendedMetrics, QueryMetrics, QueryStatus, StatisticsCollector},
    opentelemetry::KeyValue,
    spicetest::{
        SpiceTest,
        datasets::NotStarted,
    },
    telemetry::streaming::StreamingOtlpExporter,
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
    scenario: &Scenario,
    common_args: &CommonArgs,
    adbc_conn: adbc_client::AdbcConnection,
    etl_pipeline: &mut ETLPipeline
) -> anyhow::Result<()> {
    scenario.load_query_set()?;
    // Create the appropriate query executor based on args
    let executor = Box::new(adbc_executor::AdbcDirectQueryExecutor::new(adbc_conn));

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
        let adapter = super::connect_system_adapter(common_args).await?;
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
    crate::metrics::ACTIVE_CONNECTIONS.record(common_args.concurrency.try_into().unwrap_or(0), &[]);

    let mut test_builder = NotStarted::new()
        .with_parallel_count(common_args.concurrency)
        .with_end_condition(load_end_condition)
        .with_query_executor(executor);

    // Add streaming metrics sender if exporter is configured
    if let Some(exporter) = &streaming_exporter {
        test_builder = test_builder.with_streaming_metrics(exporter.sender());
    }

    let (query_set, test_builder) =
        super::build_test_with_validation(scenario, test_builder).await?;

    let _queries = query_set.get_queries(None, None, None).await?;

    let throughput_test = SpiceTest::<NotStarted>::new(scenario.to_string(), test_builder)
        .with_progress_bars(false)
        .start()?;
    let shutdown_token = throughput_test.cancellation_token();

    // --- Start the ETL pipeline (remaining batches) ---
    tracing::info!("Starting ETL pipeline (remaining batches)...");
    etl_pipeline.start()?;

    let test_future = throughput_test.wait();
    tokio::pin!(test_future);
    let test = match tokio::select! {
        res = &mut test_future => res,
        _ = signal::ctrl_c() => {
            println!("Interrupt received, stopping benchmark...");
            shutdown_token.cancel();
            test_future.await
        }
    } {
        Ok(test) => test,
        Err(e) => {
            return Err(e);
        }
    };
    test.get_query_durations().statistical_set()?;

    // Get all query durations for overall statistics before ending the test
    let all_durations = test.get_query_durations().clone();
    let all_duration_values: Vec<_> = all_durations.values().flatten().copied().collect();

    let metrics: QueryMetrics<_, NoExtendedMetrics> = test.collect(TestType::Load)?;
    let _ = test.end();

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

    // Query throughput metrics
    let total_iterations: u64 = metrics.metrics.iter().map(|q| q.iterations as u64).sum();
    let test_duration_secs = (metrics.finished_at - metrics.started_at) as f64 / 1000.0;
    crate::metrics::QUERIES_TOTAL.add(total_iterations, &[]);
    if test_duration_secs > 0.0 {
        let qps = total_iterations as f64 / test_duration_secs;
        crate::metrics::QUERIES_PER_SEC.record(qps, &[]);

        // Efficiency: queries/s normalized by CPU core count
        let cpu_cores = std::thread::available_parallelism()
            .map(|n| n.get() as f64)
            .unwrap_or(1.0);
        if cpu_cores > 0.0 {
            crate::metrics::EFFICIENCY_QUERIES_PER_CORE.record(qps / cpu_cores, &[]);
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

    println!("{}", vec!["-"; 30].join(""));
    println!("Benchmark metrics:");
    let records = metrics.build_records()?;
    print_batches(&records)?;

    // Shutdown streaming exporter before emitting final telemetry
    if let Some(exporter) = streaming_exporter {
        exporter.shutdown().await;
    }

    println!("Benchmark completed");
    Ok(())
}
