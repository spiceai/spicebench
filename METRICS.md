# Metrics Tracking

This document tracks every benchmark metric listed in the README, its OTel instrument, where it is recorded, and whether it is fully wired through the pipeline to `telemetry.spiceai.io`.

## Pipeline Overview

```
spicebench (OTel instruments)
  ├─ Query Driver ──► per-query gauges ──► Telemetry.emit() ──► telemetry.spiceai.io
  ├─ StreamingOtlpExporter ──► real-time histograms/counters ──► --otlp-endpoint
  └─ SUT Adapter (JSON-RPC `metrics`) ──► scraped gauges ──► Telemetry.emit() ──► telemetry.spiceai.io
```

## Metric Checklist

| #   | Metric                               | OTel Instrument                                                                                           | Source                                                                                           | Emitted to telemetry     | Status          |
| --- | ------------------------------------ | --------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ | ------------------------ | --------------- |
| 1   | **Data Size** (total bytes ingested) | `ingestion_bytes_total` (Gauge\<u64\>)                                                                    | SUT adapter `metrics` → `ingestion.bytes_ingested`                                               | ✅ via `Telemetry.emit()` | ✅ Implemented   |
| 2   | **Ingestion records/s**              | `ingestion_rows_per_sec` (Gauge\<f64\>)                                                                   | SUT adapter `metrics` → `ingestion.rows_per_sec`                                                 | ✅ via `Telemetry.emit()` | ✅ Implemented   |
| 3   | **Ingestion rows total**             | `ingestion_rows_total` (Gauge\<u64\>)                                                                     | SUT adapter `metrics` → `ingestion.rows_ingested`                                                | ✅ via `Telemetry.emit()` | ✅ Implemented   |
| 4   | **Connections / Clients**            | `active_connections` (Gauge\<u64\>)                                                                       | CLI `--concurrency` + SUT adapter `metrics` → `ingestion.active_connections`                     | ✅ via `Telemetry.emit()` | ✅ Implemented   |
| 5   | **Queries/s, Requests/s**            | `queries_per_sec` (Gauge\<f64\>), `queries_total` (Counter\<u64\>)                                        | Computed from total iterations / test duration                                                   | ✅ via `Telemetry.emit()` | ✅ Implemented   |
| 6   | **Query Latency (p50)**              | `median_duration_ms` (Gauge\<u64\>)                                                                       | Query driver per-query statistics                                                                | ✅ via `Telemetry.emit()` | ✅ Implemented   |
| 7   | **Query Latency (p99)**              | `p99_duration_ms` (Gauge\<u64\>)                                                                          | Query driver per-query statistics                                                                | ✅ via `Telemetry.emit()` | ✅ Implemented   |
| 8   | **Efficiency (cores)**               | `efficiency_queries_per_core` (Gauge\<f64\>)                                                              | Computed: `queries_per_sec / cpu_cores`                                                          | ✅ via `Telemetry.emit()` | ✅ Implemented   |
| 9   | **Resource Usage – CPU**             | `sut_cpu_usage_percent` (Gauge\<f64\>)                                                                    | SUT adapter `metrics` → `resource.cpu_usage_percent`                                             | ✅ via `Telemetry.emit()` | ✅ Implemented   |
| 10  | **Resource Usage – Memory**          | `peak_memory_usage_mb` / `median_memory_usage_mb` (Gauge\<f64\>), `sut_memory_usage_bytes` (Gauge\<u64\>) | Local process via `sysinfo` + SUT adapter `metrics`                                              | ✅ via `Telemetry.emit()` | ✅ Implemented   |
| 11  | **Resource Usage – Disk**            | `sut_disk_read_bytes` / `sut_disk_write_bytes` (Gauge\<u64\>)                                             | SUT adapter `metrics` → `resource.disk_read_bytes` / `disk_write_bytes`                          | ✅ via `Telemetry.emit()` | ✅ Implemented   |
| 12  | **Resource Usage – IOPS**            | `sut_disk_read_iops` / `sut_disk_write_iops` (Gauge\<u64\>)                                               | SUT adapter `metrics` → `resource.disk_read_iops` / `disk_write_iops`                            | ✅ via `Telemetry.emit()` | ✅ Implemented   |
| 13  | **E2E Latency**                      | `e2e_latency_ms` (Histogram\<f64\>)                                                                       | **Instrument defined; not yet recorded** — requires timestamped events + query-back verification | ⚠️ Instrument only        | 🔲 Not yet wired |
| 14  | **E2E Duration**                     | `test_duration_ms` (Gauge\<u64\>)                                                                         | Wall-clock time of benchmark phase                                                               | ✅ via `Telemetry.emit()` | ✅ Implemented   |

## Streaming Metrics (real-time, optional)

When `--otlp-endpoint` is configured, the following are exported every 5 seconds via a separate `PeriodicReader`:

| Metric                                     | OTel Instrument  | Description                  |
| ------------------------------------------ | ---------------- | ---------------------------- |
| `spicebench.streaming.query.duration_ms`   | Histogram\<f64\> | Per-query execution duration |
| `spicebench.streaming.query.count`         | Counter\<u64\>   | Total queries executed       |
| `spicebench.streaming.query.success_count` | Counter\<u64\>   | Successful queries           |
| `spicebench.streaming.query.failure_count` | Counter\<u64\>   | Failed queries               |

## SUT Adapter `metrics` JSON-RPC Method

The system adapter protocol now includes a `metrics` JSON-RPC method that spicebench scrapes periodically (every 5s) when `--scrape-sut-metrics` is enabled.

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

- [ ] **E2E Latency**: Implement event-creation-to-queryable latency measurement. This requires:
  1. Timestamping generated events at creation time
  2. Querying the SUT for those events after ingestion
  3. Recording the delta as `e2e_latency_ms` histogram observations
