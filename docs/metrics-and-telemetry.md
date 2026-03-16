# Metrics & Telemetry

SpiceBench collects comprehensive benchmark metrics via OpenTelemetry and exports them for analysis and visualization.

## Metrics Overview

### Per-Query Metrics

| Metric              | OTel Instrument                     | Description                                             |
| ------------------- | ----------------------------------- | ------------------------------------------------------- |
| Iterations          | `iterations` (Gauge\<u64\>)         | Number of query iterations executed per query           |
| Query Status        | `query_status` (Gauge\<u64\>)       | Pass/fail status per query (1 = pass, 0 = fail)         |
| Query Latency (p50) | `median_duration_ms` (Gauge\<u64\>) | Median (50th percentile) query duration in milliseconds |
| Query Latency (min) | `min_duration_ms` (Gauge\<u64\>)    | Minimum query duration                                  |
| Query Latency (max) | `max_duration_ms` (Gauge\<u64\>)    | Maximum query duration                                  |
| Query Latency (p99) | `p99_duration_ms` (Gauge\<u64\>)    | 99th percentile query duration                          |

All per-query metrics are emitted with a `query_name` attribute identifying the specific query.

### Throughput Metrics

| Metric             | OTel Instrument                              | Description                              |
| ------------------ | -------------------------------------------- | ---------------------------------------- |
| Queries/s          | `queries_per_sec` (Gauge\<f64\>)             | Query throughput under load              |
| Total Queries      | `queries_total` (Counter\<u64\>)             | Total queries executed during the run    |
| Active Connections | `active_connections` (Gauge\<u64\>)          | Number of concurrent connections/clients |
| Efficiency         | `efficiency_queries_per_core` (Gauge\<f64\>) | Query throughput normalized by CPU cores |

### Ingestion Metrics (from SUT adapter)

| Metric          | OTel Instrument                         | Description                    |
| --------------- | --------------------------------------- | ------------------------------ |
| Ingestion Rows  | `ingestion_rows_total` (Gauge\<u64\>)   | Total rows ingested            |
| Ingestion Bytes | `ingestion_bytes_total` (Gauge\<u64\>)  | Total bytes ingested           |
| Ingestion Rate  | `ingestion_rows_per_sec` (Gauge\<f64\>) | Sustained ingestion throughput |

### Resource Metrics (from SUT adapter)

| Metric              | OTel Instrument                         | Description                    |
| ------------------- | --------------------------------------- | ------------------------------ |
| SUT CPU             | `sut_cpu_usage_percent` (Gauge\<f64\>)  | SUT CPU utilization percentage |
| SUT Memory          | `sut_memory_usage_bytes` (Gauge\<u64\>) | SUT memory usage in bytes      |
| SUT Disk Read       | `sut_disk_read_bytes` (Gauge\<u64\>)    | SUT disk read bytes            |
| SUT Disk Write      | `sut_disk_write_bytes` (Gauge\<u64\>)   | SUT disk write bytes           |
| SUT Disk Read IOPS  | `sut_disk_read_iops` (Gauge\<u64\>)     | SUT disk read IOPS             |
| SUT Disk Write IOPS | `sut_disk_write_iops` (Gauge\<u64\>)    | SUT disk write IOPS            |

### System Metrics

| Metric         | OTel Instrument                         | Description                                                                         |
| -------------- | --------------------------------------- | ----------------------------------------------------------------------------------- |
| E2E Duration   | `test_duration_ms` (Gauge\<u64\>)       | Timed benchmark wall-clock duration from test start until stop after ETL completion |
| Peak Memory    | `peak_memory_usage_mb` (Gauge\<f64\>)   | Peak memory usage of the SpiceBench process                                         |
| Median Memory  | `median_memory_usage_mb` (Gauge\<f64\>) | Median memory usage of the SpiceBench process                                       |
| Health Latency | `health_latency_ms` (Histogram\<f64\>)  | Latency of `/health` and `/v1/ready` endpoint probes                                |
| E2E Latency    | `e2e_latency_ms` (Histogram\<f64\>)     | Event-to-queryable freshness (raw samples; percentiles computed in dashboards)      |

### Queue Metrics

| Metric               | OTel Instrument                               | Description                                           |
| -------------------- | --------------------------------------------- | ----------------------------------------------------- |
| Query Queue Length   | `query_queue_length` (Gauge\<u64\>)           | Query worker queue depth at execution start           |
| Query Queue Duration | `query_queue_duration_ms` (Histogram\<f64\>)  | Queue wait time before execution                      |
| Checkpoint In-flight | `checkpoint_in_flight_queries` (Gauge\<u64\>) | Active in-flight queries during checkpoint validation |

Queue metrics include `query_name` and `client_id` attributes.

## Metric Sources

Metrics are collected from three sources:

### 1. Query Driver

Per-query statistics are computed from the test-framework's query execution engine. After the benchmark run completes, SpiceBench calculates median, min, max, p99 latency and iteration counts for each query.

### 2. SUT Metrics Scraper

When `--scrape-sut-metrics` is enabled, SpiceBench calls the adapter's `metrics()` JSON-RPC method every 5 seconds. The adapter returns resource usage (CPU, memory, disk) and ingestion progress (rows, bytes, throughput).

The scraper tracks **cumulative deltas** - if the adapter reports cumulative counters for ingestion rows/bytes, SpiceBench computes the delta since the last scrape.

### 3. Health Monitor

Samples `/health` and `/v1/ready` endpoints every 100ms, recording latency in the `health_latency_ms` histogram. A latency threshold of 125ms is used for health assessment.

## Export Pipelines

### Arrow Flight Export (default)

All metrics are exported to `telemetry.spiceai.io` via Apache Arrow Flight after the benchmark completes. The `otel-arrow` crate converts OTel `ResourceMetrics` to a flattened Arrow `RecordBatch` schema and publishes it via the `telemetry` crate's Flight client.

This is the primary export path - results are ingested by [SpiceBench.com](https://spicebench.com) for leaderboard ranking and run detail views.

### Streaming OTLP Export (optional)

When `--otlp-endpoint` is specified, a separate `StreamingOtlpExporter` sends real-time metrics every 5 seconds via OTLP:

| Metric                                     | Type             | Description                  |
| ------------------------------------------ | ---------------- | ---------------------------- |
| `spicebench.streaming.query.duration_ms`   | Histogram\<f64\> | Per-query execution duration |
| `spicebench.streaming.query.count`         | Counter\<u64\>   | Total queries executed       |
| `spicebench.streaming.query.success_count` | Counter\<u64\>   | Successful queries           |
| `spicebench.streaming.query.failure_count` | Counter\<u64\>   | Failed queries               |

Usage:

```bash
spicebench run \
    --otlp-endpoint http://localhost:4317 \
    --otlp-header "Authorization=Bearer $TOKEN" \
    ...
```

## Metric Attributes

The current benchmark path attaches a mix of resource attributes and per-metric attributes. Not every metric carries every attribute.

| Attribute                | Source                        | Notes                                                                      |
| ------------------------ | ----------------------------- | -------------------------------------------------------------------------- |
| `adapter_name`           | `--system-adapter-name`       | Resource attribute on benchmark metrics                                    |
| `scenario`               | `--scenario`                  | Resource attribute on benchmark metrics                                    |
| `data_gen_version`       | Derived from `--scale-factor` | Resource attribute using `format_scale_factor(scale_factor)`               |
| `scale_factor`           | Version metadata              | Resource attribute on benchmark metrics                                    |
| `executor_instance_type` | `--executor-instance-type`    | Metric attribute on benchmark metrics                                      |
| `query_name`             | Scenario workload             | Metric attribute on per-query metrics                                      |
| `run_id`                 | Auto-generated UUID           | Metric attribute on SUT-scrape metrics                                     |
| `table_name`             | ETL table name                | Metric attribute on `e2e_latency_ms` samples outside checkpoint validation |

## Grafana Dashboard

A prebuilt Grafana dashboard is available at `dashboards/spicebench-benchmarks.grafana.json`.

### Dashboard Features

- **Variables**: Filter by `scenario` and `scale_factor`
- **Client Metrics panels**: `Num Clients`, `P99 Queue Time`, `Query Queue Count`
- **Query latency panels**: Per-query p50, p99, min, max duration
- **Throughput panels**: Queries/s, total queries
- **Resource panels**: CPU, memory, disk I/O from SUT adapter

### Setup

1. Open Grafana → **Dashboards → New → Import**
2. Upload `dashboards/spicebench-benchmarks.grafana.json`
3. Select your InfluxDB datasource (the dashboard queries the `benchmarks-telemetry` bucket)

## SpiceBench.com

Results from every Run are published to [SpiceBench.com](https://spicebench.com), providing:

- **Leaderboard** - Systems ranked by `test_duration_ms`, the timed benchmark wall-clock duration. Secondary sort by query latency and ingestion throughput.
- **Run details** - Per-query latency breakdown, ingestion rates over time, resource utilization charts, and E2E event latency distributions.
- **Cross-system comparison** - Side-by-side views of any two Runs with relative performance ratios.

## Query Status

SpiceBench emits `query_status` for each query. In the current main benchmark path:

| Condition                                        | Result   |
| ------------------------------------------------ | -------- |
| Query execution completed successfully           | **PASS** |
| Query execution failed                           | **FAIL** |
| Checkpoint validation detected incorrect results | **FAIL** |

The current main binary does not run a separate baseline stage or a baseline-regression WARN/FAIL gate. P99 latency is exported as telemetry for comparison across runs instead.
