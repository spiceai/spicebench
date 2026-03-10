# Architecture

## Introduction

SpiceBench is an open-source benchmark for data and AI platforms. It measures the full operational data lifecycle - ingestion, acceleration, and query serving - under the conditions AI applications and agents actually face. Unlike static benchmarks (ClickBench, TPC-H) that run queries on pre-created datasets, SpiceBench benchmarks concurrent ingestion, acceleration/materialization, and query execution against a pre-generated data archive, capturing the real tension between ingestion throughput, materialization freshness, and query latency.

## System Overview

```text
┌──────────────────────────────────────────────────────────────┐
│                    GitHub Actions / CI                       │
│  (schedule, manual dispatch, or PR trigger)                  │
└──────────────────┬───────────────────────────────────────────┘
                   │ starts
                   ▼
┌──────────────────────────────────────────────────────────────┐
│                      SpiceBench Run                          │
│                                                              │
│  ┌──────────┐   ┌────────────────┐   ┌──────────────────┐    │
│  │ 1. Setup │──▶│ 2. Benchmark   │──▶│  3. Teardown     │    │
│  │ (JSON-RPC│   │    (timed)     │   │  (JSON-RPC)      │    │
│  │  adapter)│   │                │   │                  │    │
│  └──────────┘   │ concurrent ETL │   └──────────────────┘    │
│                 │ + query load   │                           │
│                 └────────────────┘                           │
└──────────────────────────────────────────────────────────────┘
        │                    │                    │
        ▼                    ▼                    ▼
┌──────────────┐  ┌─────────────────┐  ┌──────────────────┐
│ System Under │  │  ETL Pipeline   │  │ Metrics (OTel)   │
│    Test      │  │  (S3 → SUT)     │  │ → telemetry      │
│  (via ADBC)  │  │                 │  │   .spiceai.io    │
└──────────────┘  └─────────────────┘  └──────────────────┘
```

## Run Lifecycle

A **Run** is a single end-to-end execution of the benchmark targeting one system. Every Run proceeds through three sequential phases:

### Phase 1: Setup (not timed)

SpiceBench connects to a **system adapter** via JSON-RPC 2.0 (over stdio or HTTP) and calls:

1. **`setup(run_id, metadata, datasets, etl_sink_type)`** - Returns ADBC driver configuration (driver name + connection kwargs) for query execution and can optionally provision the System Under Test (SUT) or create/register benchmark tables.

The adapter response from `setup` tells SpiceBench which ADBC driver to use and how to connect. For manually prepared systems, this phase can be limited to returning connection details.

### Phase 2: Benchmark (timed)

The current main benchmark path runs a single timed stage. SpiceBench starts the query workload, starts ETL, and keeps both running until the pipeline completes, fails, or is cancelled.

Included in the timer:

- concurrent query execution
- ETL processing and sink writes
- checkpoint pause and validation windows when `--validate-results` is enabled
- final shutdown after ETL completion

Excluded from the timer:

- archive download and extraction
- adapter `setup` and `teardown`

The current main binary exports p99 latency as telemetry for comparison across runs, but it does not run a separate baseline stage or a baseline-regression fail gate.

### Phase 3: Teardown (not timed)

SpiceBench calls **`teardown(run_id)`** on the adapter so it can optionally deprovision resources, drop tables, or perform any final cleanup.

Teardown always runs, even if the benchmark phase encounters errors, and adapters may implement it as a no-op when cleanup is handled externally or artifacts should be retained.

## Data Flow

```text
┌─────────────────┐     ┌────────────────┐     ┌──────────────────┐
│ data-generation │────▶│  S3 (Parquet)  │────▶│  ETL Pipeline    │
│   (TPC-H)       │     │  raw batches   │     │  rehydrate +     │
│                 │     │                │     │  timestamp +     │
└─────────────────┘     └────────────────┘     │  partition       │
                                               └────────┬─────────┘
                                                        │
                                          ┌─────────────┼─────────────┐
                                          ▼             ▼             ▼
                                    ┌──────────┐  ┌──────────┐  ┌──────────┐
                                    │ S3 Hive  │  │ ADBC     │  │ Null     │
                                    │ Parquet  │  │ Bulk     │  │ Sink     │
                                    │          │  │ Ingest   │  │ (/dev/   │
                                    │          │  │          │  │  null)   │
                                    └──────────┘  └──────────┘  └──────────┘
```

### Data Generation

The `data-generation` binary produces versioned datasets and writes the resulting archive to S3 or a local file. It supports:

- Configurable scale factors (SF1, SF10, SF100, etc.)
- Multi-step generation for simulating streaming data arrival
- Version metadata (`version.json`) for downstream ETL

The shipped `data-generation run` CLI currently emits create-only batches and records zero mutation ratios in `version.json`.

### ETL Pipeline

The ETL pipeline reads raw batches from S3, processes them, and writes to a configurable sink:

1. **Read** raw Parquet batches from S3
2. **Rehydrate** records (restore from columnar + apply mutations)
3. **Split** by operation type (create/update/delete)
4. **Append** `__created_at` timestamps for freshness tracking
5. **Strip** internal columns (`__op`, `__key_*`)
6. **Write** to the configured sink

See [Data Generation & ETL](data-generation-and-etl.md) for details.

## Query Execution

The current benchmark path uses the ADBC driver returned by the adapter's `setup()` response to execute queries directly against the SUT. Adapter transport is still JSON-RPC over stdio or HTTP, but the benchmark data plane is ADBC-based.

## Future AI-Native Extension

SpiceBench currently measures ingestion-to-query behavior. A planned extension is an ingestion-to-prompt/RAG benchmark pipeline that adds evaluation stages above SQL execution:

1. **Text-to-SQL stage**
      - natural language request → SQL generation
      - evaluate generation validity, execution success, and result correctness

2. **Search & retrieval stage**
      - keyword/vector/hybrid retrieval over continuously ingested data
      - evaluate retrieval quality (`recall@k`, `nDCG`) and retrieval latency

3. **Context engineering stage**
      - chunk/rank/assemble context for model prompts
      - evaluate context quality, citation grounding, and token efficiency

4. **End-to-end AI freshness stage**
      - measure time from source event creation to retrievable context and answer inclusion

This extends SpiceBench from an operational SQL benchmark into an AI-native data benchmark for application and agent workloads.

## Metrics Pipeline

```text
┌────────────────────────────────────────────────────────────┐
│                    SpiceBench Process                       │
│                                                            │
│  ┌──────────────┐  ┌───────────────┐  ┌────────────────┐  │
│  │ Query Driver │  │ SUT Metrics   │  │ Health Monitor │  │
│  │ (per-query   │  │ Scraper       │  │ (/health,      │  │
│  │  stats)      │  │ (every 5s)    │  │  /v1/ready)    │  │
│  └──────┬───────┘  └───────┬───────┘  └───────┬────────┘  │
│         │                  │                   │           │
│         ▼                  ▼                   ▼           │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              OpenTelemetry SDK                       │   │
│  │  (17+ instruments: gauges, counters, histograms)    │   │
│  └──────────┬──────────────────────────┬───────────────┘   │
│             │                          │                   │
│             ▼                          ▼                   │
│  ┌──────────────────┐      ┌───────────────────────┐      │
│  │ OtelArrowExporter│      │ StreamingOtlpExporter │      │
│  │ (Arrow Flight)   │      │ (OTLP, every 5s)      │      │
│  └────────┬─────────┘      └──────────┬────────────┘      │
└───────────┼────────────────────────────┼──────────────────┘
            │                            │
            ▼                            ▼
  telemetry.spiceai.io          --otlp-endpoint
  (Arrow Flight ingest)        (custom OTLP collector)
            │
            ▼
      SpiceBench.com
   (leaderboard + details)
```

Metrics are collected from three sources:

1. **Query driver** - Per-query latency statistics (median, min, max, p99), iteration counts, pass/fail status
2. **SUT metrics scraper** - Resource usage (CPU, memory, disk I/O, IOPS) and ingestion progress (rows, bytes, throughput) obtained by periodically calling the adapter's `metrics()` JSON-RPC method
3. **Health monitor** - Endpoint latency for `/health` and `/v1/ready` probes

See [Metrics & Telemetry](metrics-and-telemetry.md) for the full instrument list.

## System Adapter Protocol

The system adapter protocol is a JSON-RPC 2.0 interface that decouples SpiceBench from any specific data platform. The core runtime methods are:

| Method                                             | Purpose                                                     |
| -------------------------------------------------- | ----------------------------------------------------------- |
| `setup(run_id, metadata, datasets, etl_sink_type)` | Provision the SUT, create tables, return ADBC driver config |
| `teardown(run_id)`                                 | Deprovision resources                                       |
| `metrics(run_id)`                                  | Return resource usage and ingestion stats (optional)        |
| `rpc.methods`                                      | Report supported JSON-RPC methods                           |

Adapters communicate over **stdio** (SpiceBench spawns the adapter as a child process) or **HTTP** (SpiceBench connects to a running adapter server).

See [System Adapters](system-adapters.md) for the full protocol specification.

## Execution Mode Flag

The CLI still accepts `--system-adapter-execution-mode`, but the current main `spicebench` binary does not branch on it yet.

In the shipped benchmark path, SpiceBench:

1. calls adapter `setup`
2. builds the ADBC query path from the returned driver configuration
3. runs ETL and query execution itself
4. optionally scrapes adapter `metrics`
5. calls adapter `teardown`

Custom adapters can still expose additional RPC methods for their own workflows, but those are outside the core benchmark path currently driven by `spicebench`.

## Checkpoint Validation

SpiceBench supports **checkpoint-based result validation** to verify query correctness during active data ingestion:

1. The `checkpointer` binary pre-computes expected query results at specific ETL steps and stores them as Parquet files in S3
2. During a benchmark run, when the ETL pipeline reaches a checkpoint step, it pauses ingestion
3. SpiceBench runs the scenario workload and compares results against the stored expected results
4. After validation, ETL resumes

This ensures the SUT returns correct results under concurrent read/write load.

## Crate Architecture

```text
spicebench (binary)
├── test-framework          Core benchmark engine
├── system-adapter-protocol JSON-RPC client/server
├── adbc_client             ADBC connection pooling
├── flight_client           Arrow Flight client
├── telemetry               OTel metrics + export
│   └── otel-arrow          OTel → Arrow conversion
├── etl                     ETL pipeline + sinks
│   └── data-generation     Dataset generation
├── checkpointer            Checkpoint capture
└── util                    Shared utilities
```

See [Crate Reference](crate-reference.md) for per-crate API details.
