# SpiceBench

A benchmark for data & AI platforms that operate on analytical and operational data, typically on a hybrid data lake + database architecture - where data streams continuously from lakes, databases, and APIs into both object-stores and databases that serves low-latency queries for applications and AI agents. Unlike static benchmarks such as ClickBench or TPC-H that run queries on pre-created datasets, SpiceBench measures end-to-end performance across the full operational lifecycle: real-time data ingestion, indexing/acceleration/materialization, and concurrent query execution.

SpiceBench was created by the team at [Spice AI](https://spice.ai) with contributions from [Columnar](https://columnar.tech/).

## Documentation

Detailed documentation is available in the [`docs/`](docs/) directory:

| Document                                                 | Description                                                                   |
| -------------------------------------------------------- | ----------------------------------------------------------------------------- |
| [Architecture](docs/architecture.md)                     | System architecture, run lifecycle, benchmark phases, and data flow           |
| [Getting Started](docs/getting-started.md)               | Installation, prerequisites, and first run                                    |
| [CLI Reference](docs/cli-reference.md)                   | Complete `spicebench`, `data-generation`, `etl`, and `checkpointer` CLI flags |
| [System Adapters](docs/system-adapters.md)               | JSON-RPC 2.0 protocol, transport modes, and building new adapters             |
| [Data Generation & ETL](docs/data-generation-and-etl.md) | Dataset generation, ETL pipeline, sinks, and checkpointing                    |
| [Metrics & Telemetry](docs/metrics-and-telemetry.md)     | All OTel instruments, streaming metrics, and Grafana dashboards               |
| [Configuration](docs/configuration.md)                   | Scenario, table formats, and run metadata                                     |
| [Crate Reference](docs/crate-reference.md)               | Per-crate API overview for all workspace crates                               |

## Goals

The main goals of SpiceBench are:

### Realism

Modern data platforms don't just run analytical queries on static tables - they combine a data lake (the scalable source of truth) with an acceleration or materialization layer that serves low-latency queries to applications and AI agents. SpiceBench targets this hybrid architecture directly, with an emphasis on systems that have to satisfy both analytical workloads and operational serving paths from the same continuously changing data. It streams pre-generated data continuously into the system under test while concurrently executing query workloads, capturing the real tension between ingestion throughput, materialization freshness, and query latency that operators face every day.

### Reproducibility

Every Run is fully automated and deterministic: a single `spicebench` invocation prepares the system under test, loads data, executes the benchmark, collects metrics, and runs adapter cleanup hooks. Those hooks can provision and tear down infrastructure, or simply connect to a manually prepared system. All results are published to [SpiceBench.com](https://spicebench.com) with run metadata such as executor instance type, scale factor, scenario, table format, and system adapter name so any result can be reproduced on equivalent hardware.

### Extensibility

Adding a new system takes one adapter - a JSON-RPC 2.0 process (stdio or HTTP) implementing three methods (`setup`, `teardown`, `metrics`). The adapter returns the SUT-specific ADBC driver and connection details that SpiceBench primarily uses to standardize query execution and, where supported, ingestion across systems. `setup` and `teardown` can provision and clean up benchmark resources, or remain lightweight/no-op hooks when the system is managed externally. Starter templates are provided in Python, Node.js, Rust, Go, and Java. No source-code changes to SpiceBench are required.

### Transparency

All metrics are emitted via OpenTelemetry with well-defined instruments. Raw per-query latencies, ingestion rates, resource utilization, and query pass/fail status are available for every Run. The scoring methodology (timed benchmark wall-clock duration as primary rank, with latency and correctness metrics as secondary signals) is documented and auditable.

## ADBC

SpiceBench makes a deliberate design choice to standardize primarily on Apache Arrow Database Connectivity (ADBC) for the benchmark data plane. That keeps the core benchmark focused on orchestration, metrics, and reproducibility while using a consistent client boundary for interacting with each system under test.

In practice, the system adapter's `setup` response returns the SUT-specific ADBC driver and connection parameters used for query execution. When the write path supports it, SpiceBench can also use ADBC bulk ingest instead of a system-specific loader. This keeps the benchmark comparable across systems while still allowing each platform to use its own driver and driver-specific options.

This design has a few practical benefits:

- Query execution is standardized around one client interface instead of a growing set of custom per-system executors.
- Ingestion can follow the same boundary when `adbc` ETL sinks are supported, reducing adapter-specific write-path logic.
- System-specific behavior still lives where it belongs: in the adapter-provided driver choice, connection kwargs, and optional read/write separation.

See the [System Adapters guide](docs/system-adapters.md) for how adapters return ADBC configuration, and [Data Generation & ETL](docs/data-generation-and-etl.md) for ADBC bulk ingest examples.

## Primary Metric: E2E Wall-Clock Time

SpiceBench is built around a single primary ranking metric: **end-to-end wall-clock time**. In the current implementation this is the timed benchmark duration recorded as `test_duration_ms`: the interval from `SpiceTest::start()` until the benchmark stops after ETL completion, including concurrent query execution and any checkpoint-validation pause windows.

This is a deliberate design choice. SpiceBench is intended to compare systems that must ingest data continuously, build or maintain acceleration/materialization state, and serve queries from the same live dataset. A single E2E wall-clock metric captures the combined effect of ingest throughput, freshness, query execution, and checkpoint validation overhead in a way isolated query-latency benchmarks cannot.

Archive download/extraction and adapter `setup` / `teardown` happen outside this timer. Query latency p99, ingestion throughput, resource efficiency, and other metrics remain important secondary signals, but the main leaderboard order is determined first by this timed benchmark duration.

## Limitations

SpiceBench focuses on a specific class of workloads - concurrent data ingestion with analytical query execution. Note these limitations:

1. **Hybrid architecture bias.** The benchmark is designed for systems that combine a data lake or federated source layer with an acceleration/materialization layer for low-latency serving. Pure batch-analytical warehouses and pure OLTP databases are not the target workload and may be at an unfair disadvantage.

2. **Dataset coverage.** The data generator currently produces TPC-H tables, with ClickBench and custom dataset support planned. While TPC-H covers common analytical patterns, it does not represent all real-world data shapes (e.g., time-series, JSON, graph) - additional datasets will expand workload diversity over time.

3. **Scale factor range.** Default runs use modest scale factors that complete in minutes. This allows fast iteration but may not surface bottlenecks that appear only at terabyte scale.

4. **Hardware variance.** Results depend heavily on the executor instance type and the SUT deployment. SpiceBench records instance metadata and encourages apples-to-apples comparisons, but cross-hardware conclusions should be drawn carefully.

5. **No cost modeling.** The benchmark does not measure cloud spend, pricing, or cost-efficiency. Two systems may achieve similar throughput at vastly different price points.

*All Benchmarks Are Liars* - use SpiceBench results as one signal among many, not as an absolute verdict.

## Future Ideas: Toward a Fully AI-Native Benchmark

Today, SpiceBench focuses on operational data-plane performance from ingestion to query execution.

The next major extension is to benchmark the full AI-native path from **data ingestion to prompt/RAG outcomes**. Planned areas include:

- **Text-to-SQL evaluation** - measure generation quality, execution success rate, latency, and semantic correctness against ground-truth query intent.
- **Search & retrieval evaluation** - benchmark hybrid retrieval quality (keyword + vector), recall@k / nDCG, retrieval latency, and freshness under continuous ingest.
- **Context engineering evaluation** - measure context assembly quality (chunking, ranking, grounding, citation coverage), token efficiency, and end-to-end response readiness latency.
- **Ingestion-to-answer freshness** - track the time from source event creation to the event being usable in retrieval and reflected in generated answers.

This extends SpiceBench from ingestion-to-query into ingestion-to-prompt/RAG, so teams can evaluate real AI application behavior, not only SQL query speed.

### Metrics

| Metric                  | OTel Instrument                                  | Description                                                                           |
| ----------------------- | ------------------------------------------------ | ------------------------------------------------------------------------------------- |
| Iterations              | `iterations` (Gauge)                             | Number of query iterations per query                                                  |
| Query Status            | `query_status` (Gauge)                           | Pass/fail status per query                                                            |
| Query Latency (p50)     | `median_duration_ms` (Gauge)                     | Median duration per query                                                             |
| Query Latency (min/max) | `min_duration_ms`, `max_duration_ms`             | Min and max duration per query                                                        |
| Query Latency (p99)     | `p99_duration_ms` (Gauge)                        | 99th percentile duration per query                                                    |
| Health Latency          | `health_latency_ms` (Histogram)                  | Latency of `/health` and `/v1/ready` probes                                           |
| E2E Duration            | `test_duration_ms` (Gauge)                       | Timed benchmark wall-clock duration from test start until stop after ETL completion   |
| Peak/Median Memory      | `peak_memory_usage_mb`, `median_memory_usage_mb` | Memory usage of the spiced process                                                    |
| Ingestion Rows/Bytes    | `ingestion_rows_total`, `ingestion_bytes_total`  | Total data ingested (from SUT adapter)                                                |
| Ingestion records/s     | `ingestion_rows_per_sec` (Gauge)                 | Sustained ingestion throughput (from SUT adapter)                                     |
| Queries/s               | `queries_per_sec` (Gauge)                        | Query throughput under load                                                           |
| Total Queries           | `queries_total` (Counter)                        | Total queries executed during the run                                                 |
| Active Connections      | `active_connections` (Gauge)                     | Number of concurrent connections/clients                                              |
| SUT CPU                 | `sut_cpu_usage_percent` (Gauge)                  | SUT CPU utilization (from adapter `metrics`)                                          |
| SUT Memory              | `sut_memory_usage_bytes` (Gauge)                 | SUT memory usage (from adapter `metrics`)                                             |
| SUT Disk I/O            | `sut_disk_{read,write}_bytes` (Gauge)            | SUT disk read/write bytes (from adapter `metrics`)                                    |
| SUT Disk IOPS           | `sut_disk_{read,write}_iops` (Gauge)             | SUT disk IOPS (from adapter `metrics`)                                                |
| Efficiency              | `efficiency_queries_per_core` (Gauge)            | Query throughput normalized by CPU cores                                              |
| E2E Latency             | `e2e_latency_ms` (Histogram)                     | Raw event-to-queryable freshness samples; percentile is computed in dashboard queries |
| Checkpoint In-flight    | `checkpoint_in_flight_queries` (Gauge)           | In-flight query count during checkpoint validation                                    |

#### Grafana Dashboard

A prebuilt Grafana dashboard for these benchmark metrics is available at:

- `dashboards/spicebench-benchmarks.grafana.json`

Included dashboard filters and sections:

- Variables: `scenario`, `scale_factor`
- Client Metrics panels: `Num Clients`, `P99 Queue Time`, `Query Queue Count`

To use it in Grafana:

1. Go to **Dashboards → New → Import**.
2. Upload `dashboards/spicebench-benchmarks.grafana.json`.
3. Select your InfluxDB datasource (the dashboard queries the `benchmarks-telemetry` bucket).

#### Streaming Metrics (optional, `--otlp-endpoint`)

| Metric                                     | Type             | Description                  |
| ------------------------------------------ | ---------------- | ---------------------------- |
| `spicebench.streaming.query.duration_ms`   | Histogram\<f64\> | Per-query execution duration |
| `spicebench.streaming.query.count`         | Counter\<u64\>   | Total queries executed       |
| `spicebench.streaming.query.success_count` | Counter\<u64\>   | Successful queries           |
| `spicebench.streaming.query.failure_count` | Counter\<u64\>   | Failed queries               |

### Benchmark Scenario

The current main `spicebench` binary exposes one built-in benchmark scenario:

| Scenario | Flag              | Description                                |
| -------- | ----------------- | ------------------------------------------ |
| TPC-H    | `--scenario tpch` | Built-in TPC-H scenario and query workload |

Additional query-set and SQL-rewrite plumbing still exists in lower-level crates, but it is not currently surfaced as `spicebench` CLI flags.

### SpiceBench.com

Results from every Run are published to [SpiceBench.com](https://spicebench.com), inspired by [ClickBench](https://clickbench.com/) and [Vortex Bench](https://bench.vortex.dev/). The site provides:

- **Leaderboard** - Systems ranked by `test_duration_ms`, the timed benchmark wall-clock duration. Secondary sort by query latency and ingestion throughput.
- **Run details** - Per-query latency breakdown, ingestion rates over time, resource utilization charts, and E2E event latency distributions.
- **Cross-system comparison** - Side-by-side views of any two Runs with relative performance ratios.

### Supported Systems

SpiceBench currently supports the following systems for benchmark runs:

- **Databricks SQL**
- **Databricks Lakebase**
- **Spice Cloud**

See the [System Adapters guide](docs/system-adapters.md) for configuration and protocol details.

### Rules and Methodology

- **Default configuration.** Systems should be benchmarked with default or recommended settings. Fine-tuned configurations are welcome as separate entries (e.g., `MyDB` and `MyDB-tuned`).
- **No pre-aggregation.** Materialized views, projections, or pre-computed aggregates created specifically for the benchmark queries are not permitted.
- **Standard indexing.** Primary keys and default indexes are allowed. Manually created secondary indexes targeting specific benchmark queries are discouraged.
- **Caching.** Query result caches should be disabled. Data caches (buffer pools, page caches) are allowed as they reflect production behavior.
- **Incomplete results.** If a system cannot execute certain queries (OOM, unsupported SQL), partial results should still be submitted - the benchmark records per-query pass/fail status.
- **Scoring.** The primary ranking metric is **E2E wall-clock time** (`test_duration_ms`). Secondary metrics include query latency p99, ingestion throughput, resource efficiency, and query correctness/status. The current main benchmark path does not apply a separate baseline-regression fail gate.

See the [System Adapters guide](docs/system-adapters.md) for the full JSON-RPC protocol specification, request/response examples, and implementation checklist.

## Similar Projects

Many benchmarks exist for analytical databases, each with different strengths. SpiceBench occupies a distinct niche - concurrent ingestion + query under load - but borrows ideas from several of them.

### ClickBench

[https://benchmark.clickhouse.com](https://benchmark.clickhouse.com/)

A benchmark for analytical databases using a real-world web analytics dataset (100M rows) and 43 queries.

Advantages: real-world data distributions; excellent system coverage (60+ databases); reproducible in ~20 minutes; cold and hot run separation.

Disadvantages: single flat table (no joins); queries run sequentially with no concurrency; static dataset - no ingestion during benchmarking; single-node focused.

### TPC-H

The classic decision-support benchmark from the Transaction Processing Council.

Advantages: well-specified; widely recognized; tests joins, aggregation, and subqueries across a normalized schema.

Disadvantages: requires official certification for published results; synthetic data distributions don't capture real-world skew; many systems are specifically tuned for TPC-H, reducing its discriminative power.

### TPC-DS

A more complex successor to TPC-H with 99 queries, snowflake schemas, and more realistic data distributions.

Advantages: extensive query coverage; tests complex query optimization.

Disadvantages: requires official certification; biased toward complex multi-table joins; no concurrent ingestion.

### TSBS (Time Series Benchmark Suite)

[https://github.com/timescale/tsbs](https://github.com/timescale/tsbs)

A benchmark for time-series databases from InfluxDB / TimescaleDB.

Advantages: tests ingestion and query concurrently; good coverage of time-series systems.

Disadvantages: not applicable for general analytical workloads; limited to time-series data shapes.

### Where SpiceBench Fits

SpiceBench is designed for platforms built on a hybrid data lake + database architecture - systems that continuously ingest streaming data from lakes, databases, and APIs, materialize it into a database layer, and serve low-latency queries to applications and AI agents. This goes beyond analytical dashboards to cover operational workloads: real-time feature serving, agent-driven lookups, and application queries that demand sub-10ms response times while data is actively flowing in.

It complements static benchmarks by measuring what they deliberately exclude: acceleration build times, performance under concurrent write-read pressure, ingestion freshness (E2E latency), and resource efficiency over sustained operational load.

## Further Reading

See the [`docs/`](docs/) directory for detailed documentation on every aspect of SpiceBench, including:

- [Architecture & data flow](docs/architecture.md)
- [Getting started guide](docs/getting-started.md)
- [Full CLI reference](docs/cli-reference.md)
- [Building system adapters](docs/system-adapters.md)
- [Data generation & ETL pipeline](docs/data-generation-and-etl.md)
- [Metrics, telemetry & dashboards](docs/metrics-and-telemetry.md)
- [Configuration & run metadata](docs/configuration.md)
- [Crate API reference](docs/crate-reference.md)

## License

See [LICENSE](LICENSE) for details.
