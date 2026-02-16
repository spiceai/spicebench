# Spicebench

A benchmark for data & AI platforms focused on operational data. Unlike static benchmarks such as ClickBench or TPC-H that run queries on pre-created datasets, Spicebench measures end-to-end performance across dynamic real-time data generation, ingestion, indexing/acceleration/materialization, and query execution — all running concurrently.

## Architecture

```mermaid
flowchart TB
    subgraph GHA["GitHub Actions – Workflow Orchestration"]
        direction TB
        trigger["Trigger\n(schedule / manual / PR)"]
        orchestrator["Benchmark Orchestrator"]
        trigger --> orchestrator
    end

    subgraph datagen["Data Generation"]
        generator["Data Generator\n(configurable rate & schema)"]
    end

    subgraph adapters["System Adapters"]
        direction TB
        adapter_iface["Adapter Interface\n(setup / teardown / ingest / query)"]
        spice["Spice Cloud Adapter\n(Management API)"]
        databricks["Databricks Adapter\n(REST API)"]
        snowflake["Snowflake Adapter\n(SQL API)"]
        other["... Other Adapters"]
        adapter_iface --- spice
        adapter_iface --- databricks
        adapter_iface --- snowflake
        adapter_iface --- other
    end

    subgraph sut["System Under Test"]
        direction TB
        ingest_ep["Ingestion Endpoint"]
        query_ep["Query Endpoint"]
    end

    subgraph workload["Concurrent Workload Engine"]
        direction LR
        ingestion_driver["Ingestion Driver\n(continuous writes)"]
        query_driver["Query Driver\n(continuous reads)"]
    end

    subgraph metrics["Metrics Collection (OTel)"]
        direction TB
        collector["Metrics Collector\n(OpenTelemetry SDK)"]
        m1["Data Size"]
        m2["Ingestion records/s"]
        m3["Connections / Clients"]
        m4["Queries/s & Requests/s"]
        m5["Query Latency (p50/p95/p99)"]
        m6["Efficiency (cores)"]
        m7["Resource Usage\n(CPU/Mem/Disk/IOPS)"]
        m8["E2E Latency\n(event creation → query)"]
        m9["E2E Duration"]
        collector --- m1
        collector --- m2
        collector --- m3
        collector --- m4
        collector --- m5
        collector --- m6
        collector --- m7
        collector --- m8
        collector --- m9
    end

    subgraph telemetry["telemetry.spiceai.io"]
        otel_endpoint["OTel Collector Endpoint"]
    end

    subgraph reporting["Reporting"]
        results["Results Store"]
        report["Report Generator\n(comparisons & charts)"]
        results --> report
    end

    orchestrator -->|"configure & launch"| datagen
    orchestrator -->|"setup via adapter"| adapters
    orchestrator -->|"start workloads"| workload

    generator -->|"raw events"| ingestion_driver
    adapter_iface -->|"provision / configure"| sut

    ingestion_driver -->|"write events"| ingest_ep
    query_driver -->|"execute queries"| query_ep

    ingestion_driver -->|"write metrics"| collector
    query_driver -->|"query metrics"| collector
    sut -.->|"resource metrics"| collector

    collector -->|"OTLP export"| otel_endpoint
    collector --> results
```

### Component Overview

| Component                       | Responsibility                                                                                                                                                                       |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **GitHub Actions Orchestrator** | Triggers benchmark runs on schedule, PR, or manual dispatch. Manages the full lifecycle: provision → run → collect → report → teardown.                                              |
| **Data Generator**              | Produces realistic operational data at configurable rates and schemas. Emits timestamped events for E2E latency measurement.                                                         |
| **System Adapters**             | Pluggable interface for provisioning and interacting with each platform. Each adapter implements `setup`, `teardown`, `ingest`, and `query` operations using platform-specific APIs. |
| **Concurrent Workload Engine**  | Drives continuous ingestion and query execution in parallel, simulating real operational workloads where reads and writes happen simultaneously.                                     |
| **Metrics Collector**           | Emits all benchmark metrics via OpenTelemetry (OTLP) to `telemetry.spiceai.io`. Captures data from both the workload drivers and the system under test.                              |
| **Report Generator**            | Aggregates results and produces cross-system comparisons.                                                                                                                            |

### Metrics

| Metric                | Description                                                            |
| --------------------- | ---------------------------------------------------------------------- |
| Data Size             | Total volume of data ingested during the benchmark run                 |
| Ingestion records/s   | Sustained ingestion throughput                                         |
| Connections / Clients | Number of concurrent connections maintained                            |
| Queries/s, Requests/s | Query throughput under concurrent ingestion load                       |
| Query Latency         | Per-query performance breakdown (p50, p95, p99) across the query suite |
| Efficiency (cores)    | Performance normalized by compute resources                            |
| Resource Usage        | CPU, memory, disk, and IOPS utilization during the run                 |
| E2E Latency           | Time from event creation to the event being queryable                  |
| E2E Duration          | Total wall-clock time for the full benchmark run                       |

### Adding a New System Adapter

To benchmark a new platform, implement the adapter interface:

1. **Setup** — Provision infrastructure and configure the target system (e.g., via [Spice Cloud Management API](https://docs.spice.ai/api/management) or Databricks REST API).
2. **Ingest** — Write generated events to the system's ingestion endpoint.
3. **Query** — Execute the benchmark query suite against the system.
4. **Teardown** — Clean up provisioned resources.

## License

See [LICENSE](LICENSE) for details.
