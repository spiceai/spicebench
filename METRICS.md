# Metrics Reference

This document catalogs every benchmark metric listed in the README, its OTel instrument, where it is recorded, and how it flows through the pipeline to `telemetry.spiceai.io`.

## Pipeline Overview

```text
SpiceBench (OTel instruments)
  ├─ Query Driver ──► per-query gauges ──► Telemetry.emit() ──► telemetry.spiceai.io
  ├─ StreamingOtlpExporter ──► real-time histograms/counters ──► --otlp-endpoint
  └─ SUT Adapter (JSON-RPC `metrics`) ──► scraped gauges ──► Telemetry.emit() ──► telemetry.spiceai.io
```

## Metric Inventory

All metrics in this inventory are emitted through `Telemetry.emit()` after the benchmark run completes.

1. **Data Size (total bytes ingested)**
  OTel instrument: `ingestion_bytes_total` (`Gauge\<u64\>`).
  Source: SUT adapter `metrics` -> `ingestion.bytes_ingested`.

2. **Ingestion records/s**
  OTel instrument: `ingestion_rows_per_sec` (`Gauge\<f64\>`).
  Source: SUT adapter `metrics` -> `ingestion.rows_per_sec`.

3. **Ingestion rows total**
  OTel instrument: `ingestion_rows_total` (`Gauge\<u64\>`).
  Source: SUT adapter `metrics` -> `ingestion.rows_ingested`.

4. **Connections / Clients**
  OTel instrument: `active_connections` (`Gauge\<u64\>`).
  Source: CLI `--concurrency` plus SUT adapter `metrics` -> `ingestion.active_connections`.

5. **Queries/s, Requests/s**
  OTel instruments: `queries_per_sec` (`Gauge\<f64\>`), `queries_total` (`Counter\<u64\>`).
  Source: Computed from total iterations and benchmark duration.

6. **Query Latency (p50)**
  OTel instrument: `median_duration_ms` (`Gauge\<u64\>`).
  Source: Query driver per-query statistics.

7. **Query Latency (p99)**
  OTel instrument: `p99_duration_ms` (`Gauge\<u64\>`).
  Source: Query driver per-query statistics.

8. **Efficiency (cores)**
  OTel instrument: `efficiency_queries_per_core` (`Gauge\<f64\>`).
  Source: Computed as `queries_per_sec / cpu_cores`.

9. **Resource Usage - CPU**
  OTel instrument: `sut_cpu_usage_percent` (`Gauge\<f64\>`).
  Source: SUT adapter `metrics` -> `resource.cpu_usage_percent`.

10. **Resource Usage - Memory**
   OTel instruments: `peak_memory_usage_mb` (`Gauge\<f64\>`), `median_memory_usage_mb` (`Gauge\<f64\>`), `sut_memory_usage_bytes` (`Gauge\<u64\>`).
   Source: Local process via `sysinfo` plus SUT adapter `metrics`.

11. **Resource Usage - Disk**
   OTel instruments: `sut_disk_read_bytes` and `sut_disk_write_bytes` (`Gauge\<u64\>`).
   Source: SUT adapter `metrics` -> `resource.disk_read_bytes` and `resource.disk_write_bytes`.

12. **Resource Usage - IOPS**
   OTel instruments: `sut_disk_read_iops` and `sut_disk_write_iops` (`Gauge\<u64\>`).
   Source: SUT adapter `metrics` -> `resource.disk_read_iops` and `resource.disk_write_iops`.

13. **E2E Latency**
   OTel instrument: `e2e_latency_ms` (`Histogram\<f64\>`).
   Source: Raw freshness scraper samples from `MAX(__created_at)` deltas; percentiles are computed in dashboard queries.

14. **E2E Duration**
   OTel instrument: `test_duration_ms` (`Gauge\<u64\>`).
   Source: Wall-clock time of the timed benchmark phase.

15. **Query Queue Length**
   OTel instrument: `query_queue_length` (`Gauge\<u64\>`).
   Source: Query worker queue depth at query execution start, with `query_name` and `client_id` attributes.

16. **Query Queue Duration**
   OTel instrument: `query_queue_duration_ms` (`Histogram\<f64\>`).
   Source: Query worker queue wait time before execution, with `query_name` and `client_id` attributes.

17. **Checkpoint In-flight Queries**
   OTel instrument: `checkpoint_in_flight_queries` (`Gauge\<u64\>`).
   Source: Active in-flight query count while checkpoint validation windows are enabled, with a `client_id` attribute.

## Streaming Metrics (real-time, optional)

When `--otlp-endpoint` is configured, the following are exported every 5 seconds via a separate `PeriodicReader`:

| Metric                                     | OTel Instrument  | Description                  |
| ------------------------------------------ | ---------------- | ---------------------------- |
| `spicebench.streaming.query.duration_ms`   | Histogram\<f64\> | Per-query execution duration |
| `spicebench.streaming.query.count`         | Counter\<u64\>   | Total queries executed       |
| `spicebench.streaming.query.success_count` | Counter\<u64\>   | Successful queries           |
| `spicebench.streaming.query.failure_count` | Counter\<u64\>   | Failed queries               |

## SUT Adapter `metrics` JSON-RPC Method

The system adapter protocol now includes a `metrics` JSON-RPC method that SpiceBench scrapes periodically (every 5s) when `--scrape-sut-metrics` is enabled.

### Request

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "metrics",
  "params": { "run_id": "<uuid>" }
}
```

### Response

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "resource": {
      "cpu_usage_percent": 45.2,
      "memory_usage_bytes": 8589934592,
      "disk_read_bytes": 1073741824,
      "disk_write_bytes": 2147483648,
      "disk_read_iops": 5000,
      "disk_write_iops": 3000
    },
    "ingestion": {
      "rows_ingested": 10000000,
      "bytes_ingested": 5368709120,
      "rows_per_sec": 50000.0,
      "active_connections": 8
    }
  }
}
```

All fields in `resource` and `ingestion` are optional (`null` / omitted if not available from the SUT).

The default `Handler::metrics()` implementation returns empty metrics, so existing adapters remain compatible without changes.

## Remaining Work

- [ ] **E2E Latency dashboard expansion**: Add optional additional percentile panels (e.g., p50/p90/p99.9) computed from `e2e_latency_ms` in Flux.
