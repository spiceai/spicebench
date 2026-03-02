# CLI Reference

Complete command-line reference for SpiceBench binaries.

## `spicebench`

The main benchmark binary. Connects to a system adapter, runs setup/benchmark/teardown, and exports metrics.

### Usage

```bash
spicebench [OPTIONS]
```

### Scenario & Query Options

| Flag                    | Type                | Default | Description                                                        |
| ----------------------- | ------------------- | ------- | ------------------------------------------------------------------ |
| `--scenario`            | `Scenario`          | `tpch`  | Benchmark scenario to run                                          |
| `--concurrency`         | `usize`             | `2`     | Number of concurrent query clients during the load test            |
| `--query-set`           | `QuerySetArg`       | —       | Query set to execute (see [Query Sets](#query-sets))               |
| `--query-overrides`     | `QueryOverridesArg` | —       | SQL dialect overrides (see [SQL Dialects](#sql-dialect-overrides)) |
| `--scenario-query-file` | `String`            | —       | Path to a custom query file (used with `--query-set scenario`)     |
| `--validate-results`    | `bool`              | `false` | Enable checkpoint-based query result validation                    |

### System Adapter Options

| Flag                              | Type        | Default           | Description                                                                            |
| --------------------------------- | ----------- | ----------------- | -------------------------------------------------------------------------------------- |
| `--system-adapter-name`           | `String`    | required          | Name identifier for the system adapter                                                 |
| `--system-adapter-execution-mode` | `Enum`      | `adapter-command` | `adapter-command` or `direct-query`                                                    |
| `--system-adapter-stdio-cmd`      | `String`    | —                 | Command to start a stdio adapter (mutually exclusive with `--system-adapter-http-url`) |
| `--system-adapter-stdio-args`     | `String`    | —                 | Arguments passed to the stdio adapter command                                          |
| `--system-adapter-http-url`       | `String`    | —                 | URL of a running HTTP adapter (mutually exclusive with `--system-adapter-stdio-cmd`)   |
| `--system-adapter-param`          | `KEY=VALUE` | —                 | Repeatable. Key-value params passed to the adapter in `setup` metadata                 |
| `--system-adapter-env`            | `KEY=VALUE` | —                 | Repeatable. Environment variables set for stdio adapter processes only                 |

### ETL & Data Options

| Flag                       | Type     | Default                   | Description                                                     |
| -------------------------- | -------- | ------------------------- | --------------------------------------------------------------- |
| `--etl-bucket`             | `String` | `spiceai-public-datasets` | S3 bucket containing source data batches                        |
| `--etl-prefix`             | `String` | `data-gen`                | S3 key prefix for source data                                   |
| `--etl-version`            | `String` | `1`                       | Version identifier of the data generation to read               |
| `--etl-sink`               | `Enum`   | `hive`                    | ETL sink type: `hive` (S3 Parquet) or `adbc` (ADBC bulk ingest) |
| `--etl-target-base-prefix` | `String` | `etl-hive-output`         | Base S3 prefix for Hive sink output                             |
| `--etl-region`             | `String` | `us-east-1`               | AWS region for S3 operations                                    |
| `--etl-endpoint`           | `String` | —                         | Custom S3 endpoint (for MinIO, LocalStack, etc.)                |
| `--etl-partition-by`       | `String` | `__created_at`            | Comma-separated partition columns for Hive sink                 |
| `--table-format`           | `Enum`   | `parquet`                 | Table format: `parquet`, `iceberg`, or `delta`                  |

### Metrics & Telemetry Options

| Flag                       | Type        | Default   | Description                                                             |
| -------------------------- | ----------- | --------- | ----------------------------------------------------------------------- |
| `--scrape-sut-metrics`     | `bool`      | `false`   | Enable periodic SUT metrics scraping via adapter `metrics()` (every 5s) |
| `--otlp-endpoint`          | `String`    | —         | OTLP endpoint for streaming metrics export (every 5s)                   |
| `--otlp-header`            | `KEY=VALUE` | —         | Repeatable. Headers for OTLP export requests                            |
| `--executor-instance-type` | `String`    | `unknown` | Hardware class identifier for cross-system comparison                   |

### Query Sets

| Value               | Flag                              | Description                                        |
| ------------------- | --------------------------------- | -------------------------------------------------- |
| TPC-H               | `--query-set tpch`                | Standard TPC-H query suite (22 queries)            |
| TPC-DS              | `--query-set tpcds`               | Standard TPC-DS query suite                        |
| ClickBench          | `--query-set clickbench`          | ClickBench query suite                             |
| Parameterized TPC-H | `--query-set tpch[parameterized]` | TPC-H with randomized parameter substitution       |
| Scenario            | `--query-set scenario`            | Custom queries loaded from `--scenario-query-file` |

### SQL Dialect Overrides

Use `--query-overrides <dialect>` to apply SQL rewrites for a specific engine:

| Dialect               | Target System                   |
| --------------------- | ------------------------------- |
| `sqlite`              | SQLite                          |
| `postgresql`          | PostgreSQL                      |
| `mysql`               | MySQL                           |
| `dremio`              | Dremio                          |
| `spark`               | Apache Spark SQL                |
| `duckdb`              | DuckDB                          |
| `duckdb-zero-results` | DuckDB (empty result variant)   |
| `duckdb-partitioned`  | DuckDB (partitioned tables)     |
| `snowflake`           | Snowflake                       |
| `oracle`              | Oracle                          |
| `odbc-athena`         | Amazon Athena via ODBC          |
| `odbc-databricks`     | Databricks via ODBC             |
| `iceberg-sf1`         | Iceberg (SF1 variant)           |
| `iceberg-hadoop`      | Iceberg Hadoop catalog          |
| `spicecloud-catalog`  | Spice Cloud with catalog prefix |
| `glue-catalog`        | AWS Glue catalog                |
| `databricks-catalog`  | Databricks Unity Catalog        |
| `spicecloud`          | Spice Cloud                     |
| `dynamodb`            | Amazon DynamoDB                 |

### Examples

**Direct-query with HTTP adapter:**

```bash
spicebench \
    --query-set tpch \
    --system-adapter-name myplatform \
    --system-adapter-execution-mode direct-query \
    --system-adapter-http-url http://127.0.0.1:8080/jsonrpc \
    --scrape-sut-metrics \
    --concurrency 4
```

**Stdio adapter with Docker:**

```bash
spicebench \
    --query-set tpch \
    --system-adapter-name spidapter \
    --system-adapter-stdio-cmd docker \
    --system-adapter-stdio-args "run -i --rm ghcr.io/spiceai/spidapter:latest" \
    --system-adapter-param profile=dev \
    --system-adapter-env API_TOKEN=$API_TOKEN
```

**With streaming metrics:**

```bash
spicebench \
    --query-set tpch \
    --system-adapter-name myplatform \
    --system-adapter-execution-mode direct-query \
    --system-adapter-http-url http://127.0.0.1:8080/jsonrpc \
    --otlp-endpoint http://localhost:4317 \
    --otlp-header "Authorization=Bearer $TOKEN"
```

---

## `data-generation`

Standalone binary for generating TPC-H datasets and writing Parquet batches to S3.

### Usage

```bash
data-generation run [OPTIONS]
```

### Options

| Flag                       | Type     | Default   | Description                           |
| -------------------------- | -------- | --------- | ------------------------------------- |
| `--scale-factor`           | `f64`    | required  | TPC-H scale factor (1, 10, 100, etc.) |
| `--bucket`                 | `String` | required  | S3 bucket for output                  |
| `--prefix`                 | `String` | required  | S3 key prefix                         |
| `--region`                 | `String` | —         | AWS region                            |
| `--num-steps`              | `usize`  | required  | Number of generation steps (batches)  |
| `--table-format`           | `String` | `parquet` | Table format metadata                 |
| `--executor-instance-type` | `String` | —         | Executor hardware class for metadata  |

### Example

```bash
cargo run -p data-generation -- run \
    --scale-factor 1 \
    --bucket my-benchmark-data \
    --region us-west-2 \
    --prefix raw \
    --num-steps 10 \
    --table-format parquet
```

---

## `etl`

Standalone ETL pipeline binary. Reads raw batches from S3, rehydrates records, and writes to a configurable sink.

### Usage

```bash
etl [OPTIONS]
```

### Options

| Flag                   | Type        | Default        | Description                                  |
| ---------------------- | ----------- | -------------- | -------------------------------------------- |
| `--scenario`           | `String`    | `tpch`         | Scenario name                                |
| `--version`            | `String`    | required       | Version of the generated data                |
| `--bucket`             | `String`    | required       | S3 bucket with source batches                |
| `--prefix`             | `String`    | required       | S3 key prefix for source data                |
| `--region`             | `String`    | —              | AWS region                                   |
| `--endpoint`           | `String`    | —              | Custom S3 endpoint                           |
| `--sink`               | `Enum`      | `s3-hive`      | Sink type: `s3-hive`, `adbc`, `null`         |
| `--target-prefix`      | `String`    | —              | Output prefix for Hive sink                  |
| `--partition-by`       | `String`    | `__created_at` | Partition columns (comma-separated)          |
| `--adbc-driver`        | `String`    | —              | ADBC driver name (for `adbc` sink)           |
| `--adbc-uri`           | `String`    | —              | ADBC connection URI                          |
| `--adbc-option`        | `KEY=VALUE` | —              | Repeatable. Additional ADBC database options |
| `--adbc-create-tables` | `bool`      | `false`        | Create tables before ETL starts              |
| `--adbc-schema`        | `String`    | —              | Target schema for ADBC tables                |

### Examples

See [Data Generation & ETL](data-generation-and-etl.md) for full examples.

---

## `checkpointer`

Runs ETL to specific steps and captures expected query results as Parquet checkpoint files.

### Usage

```bash
cargo run -p checkpointer -- [OPTIONS]
```

See [Data Generation & ETL — Checkpointing](data-generation-and-etl.md#checkpointing) for details.

---

## Makefile Targets

| Target        | Command                                                   | Description                         |
| ------------- | --------------------------------------------------------- | ----------------------------------- |
| `lint`        | `check + test + clippy`                                   | Full lint suite                     |
| `check`       | `cargo check --workspace`                                 | Type-check all crates               |
| `test`        | `cargo test -p spicebench`                                | Run spicebench tests                |
| `clippy`      | `cargo clippy -p spicebench --all-targets -- -D warnings` | Lint with warnings as errors        |
| `fmt`         | `cargo fmt --all`                                         | Format all code                     |
| `fmt-check`   | `cargo fmt --all -- --check`                              | Check formatting                    |
| `fix`         | `fmt + clippy-fix`                                        | Auto-fix formatting and lint issues |
| `build`       | `cargo build --release -p spicebench`                     | Release build                       |
| `build-dev`   | `cargo build -p spicebench`                               | Debug build                         |
| `install`     | Build release + copy to `~/.spice/bin/`                   | Install release binary              |
| `install-dev` | Build debug + copy to `~/.spice/bin/`                     | Install debug binary                |
