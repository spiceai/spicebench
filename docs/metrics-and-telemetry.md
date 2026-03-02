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

| Metric         | OTel Instrument                         | Description                                                                    |
| -------------- | --------------------------------------- | ------------------------------------------------------------------------------ |
| E2E Duration   | `test_duration_ms` (Gauge\<u64\>)       | Total wall-clock time for the benchmark phase                                  |
| Peak Memory    | `peak_memory_usage_mb` (Gauge\<f64\>)   | Peak memory usage of the SpiceBench process                                    |
| Median Memory  | `median_memory_usage_mb` (Gauge\<f64\>) | Median memory usage of the SpiceBench process                                  |
| Health Latency | `health_latency_ms` (Histogram\<f64\>)  | Latency of `/health` and `/v1/ready` endpoint probes                           |
| E2E Latency    | `e2e_latency_ms` (Histogram\<f64\>)     | Event-to-queryable freshness (raw samples; percentiles computed in dashboards) |

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

Per-query statistics are computed from the test-framework's query execution engine. After the load test completes, SpiceBench calculates median, min, max, p99 latency and iteration counts for each query.

### 2. SUT Metrics Scraper

When `--scrape-sut-metrics` is enabled, SpiceBench calls the adapter's `metrics()` JSON-RPC method every 5 seconds. The adapter returns resource usage (CPU, memory, disk) and ingestion progress (rows, bytes, throughput).

The scraper tracks **cumulative deltas** — if the adapter reports cumulative counters for ingestion rows/bytes, SpiceBench computes the delta since the last scrape.

### 3. Health Monitor

Samples `/health` and `/v1/ready` endpoints every 100ms, recording latency in the `health_latency_ms` histogram. A latency threshold of 125ms is used for health assessment.

## Export Pipelines

### Arrow Flight Export (default)

All metrics are exported to `telemetry.spiceai.io` via Apache Arrow Flight after the benchmark completes. The `otel-arrow` crate converts OTel `ResourceMetrics` to a flattened Arrow `RecordBatch` schema and publishes it via the `telemetry` crate's Flight client.

This is the primary export path — results are ingested by [SpiceBench.com](https://spicebench.com) for leaderboard ranking and run detail views.

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
spicebench \
    --otlp-endpoint http://localhost:4317 \
    --otlp-header "Authorization=Bearer $TOKEN" \
    ...
```

## OTel Resource Attributes

Every metric export includes these OTel resource attributes:

| Attribute                | Source                     | Description                            |
| ------------------------ | -------------------------- | -------------------------------------- |
| `run_id`                 | Auto-generated UUID        | Unique run identifier                  |
| `scenario`               | `--scenario`               | Benchmark scenario name                |
| `system_under_test`      | `--system-adapter-name`    | Target platform identifier             |
| `executor_instance_type` | `--executor-instance-type` | Hardware class of the executor         |
| `scale_factor`           | From version metadata      | TPC-H scale factor                     |
| `table_format`           | `--table-format`           | Table format (parquet, iceberg, delta) |

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

- **Leaderboard** — Systems ranked by E2E benchmark duration (phase 2 wall-clock time). Secondary sort by query latency and ingestion throughput.
- **Run details** — Per-query latency breakdown, ingestion rates over time, resource utilization charts, and E2E event latency distributions.
- **Cross-system comparison** — Side-by-side views of any two Runs with relative performance ratios.

## Pass/Fail Criteria

After the load test, SpiceBench evaluates each query's performance:

| Condition                               | Result   |
| --------------------------------------- | -------- |
| p99 latency increase >20% vs baseline   | **FAIL** |
| p99 latency increase 10–20% vs baseline | **WARN** |
| ≥3 WARNs across all queries             | **FAIL** |
| Otherwise                               | **PASS** |

The benchmark's overall status is determined by combining individual query results.
