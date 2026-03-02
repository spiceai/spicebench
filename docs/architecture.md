# Architecture

SpiceBench is an end-to-end benchmark for data & AI platforms built on a hybrid data lake + accelerator architecture — systems that continuously ingest data from lakes, databases, and APIs into an acceleration layer that serves low-latency queries to applications and AI agents. Unlike static benchmarks (ClickBench, TPC-H) that run queries on pre-created datasets, SpiceBench runs data generation, ingestion, acceleration/materialization, and query execution **concurrently**.

## System Overview

```
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
│  └──────────┘   │ warm-up        │   └──────────────────┘    │
│                 │ baseline       │                           │
│                 │ load test      │                           │
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

1. **`setup(run_id, metadata)`** — Provisions the System Under Test (SUT) and returns ADBC driver configuration (driver name + connection kwargs) for query execution.
2. **`create_tables(run_id, datasets)`** — Creates or registers destination tables for all benchmark datasets (e.g., TPC-H tables).

The adapter response from `setup` tells SpiceBench which ADBC driver to use and how to connect.

### Phase 2: Benchmark (timed)

The benchmark phase has three sequential stages:

| Stage         | Duration                                 | Purpose                                                                       |
| ------------- | ---------------------------------------- | ----------------------------------------------------------------------------- |
| **Warm-up**   | 1× query set                             | Primes caches, JIT compilation, connection pools                              |
| **Baseline**  | 10% of total duration (clamped 60s–600s) | Establishes per-query p99 latency baselines without concurrent data ingestion |
| **Load test** | Full configured duration                 | Runs concurrent query clients alongside active ETL data ingestion             |

During the load test, the ETL pipeline streams data from S3 into the SUT — simulating continuous data flowing from a data lake into the acceleration layer — while multiple query clients execute the configured query set concurrently, representing application and AI agent workloads.

**Pass/Fail criteria**: After the load test, each query's p99 latency is compared against its baseline:
- **>20% increase** → FAIL
- **10–20% increase** → WARN
- **≥3 WARNs** → FAIL

### Phase 3: Teardown (not timed)

SpiceBench calls **`teardown(run_id)`** on the adapter to deprovision resources, drop tables, and clean up.

Teardown always runs, even if the benchmark phase encounters errors.

## Data Flow

```
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

The `data-generation` binary produces TPC-H datasets as partitioned Parquet batches and writes them to S3. It supports:

- Configurable scale factors (SF1, SF10, SF100, etc.)
- Mutation operations (INSERT, UPDATE, DELETE) with configurable ratios
- Multi-step generation for simulating streaming data arrival
- Version metadata (`version.json`) for downstream ETL

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

SpiceBench supports pluggable query executors:

| Executor        | Transport                      | Use Case                                 |
| --------------- | ------------------------------ | ---------------------------------------- |
| **ADBC Direct** | ADBC driver (adapter-selected) | Primary executor for `direct-query` mode |
| **HTTP**        | `POST /v1/sql`                 | HTTP-based query execution               |

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
| **Distributed** | `POST /v1/queries` + polling       | Async distributed query execution        |

In `direct-query` mode (the most common), SpiceBench uses the ADBC driver returned by the adapter's `setup()` response to execute queries directly against the SUT.

## Metrics Pipeline

```
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

1. **Query driver** — Per-query latency statistics (median, min, max, p99), iteration counts, pass/fail status
2. **SUT metrics scraper** — Resource usage (CPU, memory, disk I/O, IOPS) and ingestion progress (rows, bytes, throughput) obtained by periodically calling the adapter's `metrics()` JSON-RPC method
3. **Health monitor** — Endpoint latency for `/health` and `/v1/ready` probes

See [Metrics & Telemetry](metrics-and-telemetry.md) for the full instrument list.

## System Adapter Protocol

The system adapter protocol is a JSON-RPC 2.0 interface that decouples SpiceBench from any specific data platform. Each adapter implements four methods:

| Method                            | Purpose                                              |
| --------------------------------- | ---------------------------------------------------- |
| `setup(run_id, metadata)`         | Provision the SUT, return ADBC driver config         |
| `create_tables(run_id, datasets)` | Create/register benchmark tables                     |
| `teardown(run_id)`                | Deprovision resources                                |
| `metrics(run_id)`                 | Return resource usage and ingestion stats (optional) |

Adapters communicate over **stdio** (SpiceBench spawns the adapter as a child process) or **HTTP** (SpiceBench connects to a running adapter server).

See [System Adapters](system-adapters.md) for the full protocol specification.

## Execution Modes

SpiceBench supports two execution modes controlled by `--system-adapter-execution-mode`:

| Mode                | Flag                        | Behavior                                                                                        |
| ------------------- | --------------------------- | ----------------------------------------------------------------------------------------------- |
| **adapter-command** | `adapter-command` (default) | Delegates the entire benchmark run to the adapter via `run.load` JSON-RPC                       |
| **direct-query**    | `direct-query`              | SpiceBench drives queries directly via ADBC; adapter handles setup/tables/teardown/metrics only |

`direct-query` is the standard mode for benchmarking external systems where SpiceBench controls the query workload.

## Checkpoint Validation

SpiceBench supports **checkpoint-based result validation** to verify query correctness during active data ingestion:

1. The `checkpointer` binary pre-computes expected query results at specific ETL steps and stores them as Parquet files in S3
2. During a benchmark run, when the ETL pipeline reaches a checkpoint step, it pauses ingestion
3. SpiceBench runs the query set and compares results against the stored expected results
4. After validation, ETL resumes

This ensures the SUT returns correct results under concurrent read/write load.

## Crate Architecture

```
spicebench (binary)
├── test-framework          Core benchmark engine
├── system-adapter-protocol JSON-RPC client/server
├── adbc_client             ADBC connection pooling
├── flight_client           Arrow Flight client
├── telemetry               OTel metrics + export
│   └── otel-arrow          OTel → Arrow conversion
├── app                     Aggregated config
│   └── spicepod            YAML config loader
│       ├── yaml            YAML library
│       └── duration-parse  Duration parsing
├── etl                     ETL pipeline + sinks
│   └── data-generation     Dataset generation
├── checkpointer            Checkpoint capture
└── util                    Shared utilities
```

See [Crate Reference](crate-reference.md) for per-crate API details.
