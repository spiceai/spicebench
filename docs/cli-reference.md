# CLI Reference

Complete command-line reference for SpiceBench.

All functionality is accessed through the single `spicebench` binary with subcommands:

```
spicebench <COMMAND>

Commands:
  run         Run the full benchmark lifecycle
  generate    Generate a dataset archive
  etl         Run a standalone ETL pipeline
  checkpoint  Capture checkpoint query results
```

---

## `spicebench run`

Run the full benchmark lifecycle: download and extract a pre-generated data archive, connect to a system adapter, run setup, execute the timed benchmark, and tear the target system down.

### Usage

```bash
spicebench run [OPTIONS] --scenario <SCENARIO>
```

### Core Options

| Flag                       | Type       | Default   | Description                                                            |
| -------------------------- | ---------- | --------- | ---------------------------------------------------------------------- |
| `--scenario`               | `Scenario` | required  | Benchmark scenario to run. Current value: `tpch`                       |
| `--concurrency`            | `usize`    | `2`       | Number of concurrent query clients during the timed benchmark          |
| `--validate-results`       | `bool`     | `false`   | Enable checkpoint-based query result validation when checkpoints exist |
| `--executor-instance-type` | `String`   | `unknown` | Hardware class identifier attached to emitted benchmark metrics        |

The `run` subcommand does not expose separate `--query-set`, `--scenario-query-file`, or `--query-overrides` flags. The scenario selects the built-in benchmark workload.

### System Adapter Options

| Flag                              | Type        | Default           | Description                                                                                                  |
| --------------------------------- | ----------- | ----------------- | ------------------------------------------------------------------------------------------------------------ |
| `--system-adapter-name`           | `String`    | `system_adapter`  | Logical name for the system adapter connection                                                               |
| `--system-adapter-execution-mode` | `Enum`      | `adapter-command` | Accepted values: `adapter-command`, `direct-query`. The current main binary does not branch on this flag yet |
| `--system-adapter-stdio-cmd`      | `String`    | -                 | Command to start a stdio adapter (mutually exclusive with `--system-adapter-http-url`)                       |
| `--system-adapter-stdio-args`     | `String`    | -                 | Space-delimited arguments passed to the stdio adapter command                                                |
| `--system-adapter-http-url`       | `String`    | -                 | URL of a running HTTP adapter (mutually exclusive with `--system-adapter-stdio-cmd`)                         |
| `--system-adapter-param`          | `KEY=VALUE` | -                 | Repeatable. Adapter-specific params passed in `setup` metadata                                               |
| `--system-adapter-env`            | `KEY=VALUE` | -                 | Repeatable. Environment variables for stdio adapters only                                                    |

Set exactly one of `--system-adapter-stdio-cmd` or `--system-adapter-http-url`.

### Data & ETL Options

| Flag                         | Type     | Default                   | Description                                                                                            |
| ---------------------------- | -------- | ------------------------- | ------------------------------------------------------------------------------------------------------ |
| `--etl-bucket`               | `String` | `spiceai-public-datasets` | S3 bucket containing source data batches                                                               |
| `--etl-prefix`               | `String` | `data-gen`                | S3 key prefix for source data                                                                          |
| `--scale-factor`             | `f64`    | `1.0`                     | Dataset scale factor. The ETL version path is derived automatically                                    |
| `--etl-sink`                 | `Enum`   | `adbc`                    | ETL sink type: `adbc` (ADBC bulk ingest)                                                               |
| `--etl-region`               | `String` | `us-east-1`               | AWS region for S3 operations                                                                           |
| `--etl-endpoint`             | `String` | -                         | Custom S3 endpoint (for MinIO, LocalStack, and similar)                                                |
| `--table-format`             | `Enum`   | `parquet`                 | Table format propagated through ETL dataset metadata and adapter setup                                 |
| `--scheduler-state-location` | `String` | -                         | Optional S3 URI for shared scheduler state passed through setup metadata                               |

### Metrics & Telemetry Options

| Flag                   | Type        | Default | Description                                                              |
| ---------------------- | ----------- | ------- | ------------------------------------------------------------------------ |
| `--scrape-sut-metrics` | `bool`      | `false` | Enable periodic SUT metrics scraping via adapter `metrics()`             |
| `--otlp-endpoint`      | `String`    | -       | OTLP endpoint for streaming metrics export                               |
| `--otlp-header`        | `KEY=VALUE` | -       | Repeatable. Headers for OTLP export requests. Requires `--otlp-endpoint` |

### Scenarios

| Scenario | Flag              | Description                                |
| -------- | ----------------- | ------------------------------------------ |
| TPC-H    | `--scenario tpch` | Built-in TPC-H scenario and query workload |

### Examples

**HTTP adapter:**

```bash
spicebench run \
    --scenario tpch \
    --system-adapter-name myplatform \
    --system-adapter-http-url http://127.0.0.1:8080/jsonrpc \
    --scrape-sut-metrics \
    --concurrency 4
```

**Stdio adapter with Docker:**

```bash
spicebench run \
    --scenario tpch \
    --system-adapter-name spidapter \
    --system-adapter-stdio-cmd docker \
    --system-adapter-stdio-args "run -i --rm ghcr.io/spiceai/spidapter:latest" \
    --system-adapter-param profile=dev \
    --system-adapter-env API_TOKEN=$API_TOKEN
```

**With streaming metrics:**

```bash
spicebench run \
    --scenario tpch \
    --system-adapter-name myplatform \
    --system-adapter-http-url http://127.0.0.1:8080/jsonrpc \
    --otlp-endpoint http://localhost:4317 \
    --otlp-header "Authorization=Bearer $TOKEN"
```

---

## `spicebench generate`

Generate versioned datasets and either upload the resulting archive to S3 or write it to a local archive file.

### Usage

```bash
spicebench generate [OPTIONS]
```

### Options

| Flag                | Type     | Default | Description                                                          |
| ------------------- | -------- | ------- | -------------------------------------------------------------------- |
| `--dataset`         | `String` | `tpch`  | Dataset type to generate                                             |
| `--scale-factor`    | `f64`    | `1.0`   | Dataset scale factor                                                 |
| `--num-steps`       | `u16`    | `25`    | Number of data generation steps                                      |
| `--scenario`        | `String` | `tpch`  | Scenario name used in the output path                                |
| `--output-archive`  | `String` | -       | Write the generated `.tar.zst` archive to a local path instead of S3 |
| `--bucket`          | `String` | -       | S3 bucket for output. Required unless `--output-archive` is set      |
| `--prefix`          | `String` | `""`    | S3 key prefix for generated files                                    |
| `--region`          | `String` | -       | AWS region                                                           |
| `--endpoint`        | `String` | -       | S3 endpoint URL (for MinIO, LocalStack, and similar)                 |
| `--update-ratio`    | `f64`    | `0.0`   | Ratio of update mutations per batch (0.0 to 1.0)                    |
| `--delete-ratio`    | `f64`    | `0.0`   | Ratio of delete mutations per batch (0.0 to 1.0)                    |

The generated version string is derived automatically from `--scale-factor`, so `--scale-factor 1` writes to a `1.0` version path.

### Examples

**Upload generated data to S3:**

```bash
spicebench generate \
    --scale-factor 1 \
    --bucket my-benchmark-data \
    --region us-west-2 \
    --prefix raw \
    --num-steps 10
```

**Write a local archive:**

```bash
spicebench generate \
    --scale-factor 1 \
    --num-steps 10 \
    --output-archive ./tpch-sf1.tar.zst
```

---

## `spicebench etl`

Standalone ETL pipeline. Reads a generated archive, rehydrates records, and writes to an ADBC target or a null sink.

### Usage

```bash
spicebench etl [OPTIONS]
```

### Options

| Flag                   | Type        | Default        | Description                                                            |
| ---------------------- | ----------- | -------------- | ---------------------------------------------------------------------- |
| `--scenario`           | `String`    | `tpch`         | Scenario name                                                          |
| `--scale-factor`       | `f64`       | `1.0`          | Dataset scale factor. The version path is derived automatically        |
| `--archive-file`       | `Path`      | -              | Local `.tar.zst` archive to extract instead of downloading from S3     |
| `--extract-dir`        | `Path`      | temp dir       | Directory to extract the archive into                                  |
| `--bucket`             | `String`    | -              | S3 bucket with source archive. Required unless `--archive-file` is set |
| `--prefix`             | `String`    | `""`           | S3 key prefix for source data                                          |
| `--region`             | `String`    | -              | AWS region                                                             |
| `--endpoint`           | `String`    | -              | Custom S3 endpoint                                                     |
| `--sink`               | `Enum`      | `adbc`         | Sink type: `adbc`, `null`                                              |
| `--adbc-driver`        | `String`    | -              | ADBC driver name for the `adbc` sink                                   |
| `--adbc-uri`           | `String`    | -              | ADBC connection URI for the `adbc` sink                                |
| `--adbc-catalog`       | `String`    | -              | Optional target catalog for ADBC bulk ingest                           |
| `--adbc-schema`        | `String`    | -              | Optional target schema for ADBC bulk ingest                            |
| `--adbc-create-tables` | `bool`      | `false`        | Create tables before ETL starts. Requires `--sink adbc`                |
| `--adbc-option`        | `KEY=VALUE` | -              | Repeatable. Additional ADBC database options                           |

### Examples

**Local archive to null sink:**

```bash
spicebench etl \
    --scenario tpch \
    --scale-factor 1 \
    --archive-file ./tpch-sf1.tar.zst \
    --sink null
```

**ADBC sink:**

```bash
spicebench etl \
    --scenario tpch \
    --scale-factor 1 \
    --bucket my-data \
    --prefix raw \
    --sink adbc \
    --adbc-driver databricks \
    --adbc-uri "databricks://token:${DATABRICKS_TOKEN}@${DATABRICKS_ENDPOINT}:443/${DATABRICKS_HTTP_PATH}" \
    --adbc-catalog main \
    --adbc-schema tpch \
    --adbc-create-tables
```

---

## `spicebench checkpoint`

Capture expected query results at ETL checkpoints. Requires the `duckdb` feature (`--features duckdb`) and writes checkpoint results by replaying ETL into a local DuckDB database.

### Usage

```bash
spicebench checkpoint [OPTIONS] --version <VERSION> --bucket <BUCKET> --duckdb-path <DUCKDB_PATH>
```

Building with the `duckdb` feature:

```bash
cargo build -p spicebench --features duckdb
```

### Options

| Flag                          | Type     | Default         | Description                                                  |
| ----------------------------- | -------- | --------------- | ------------------------------------------------------------ |
| `--scenario`                  | `String` | `tpch`          | Scenario to checkpoint                                       |
| `--version`                   | `String` | required        | Data generation version to read from S3                      |
| `--bucket`                    | `String` | required        | S3 bucket used for the source archive and checkpoint uploads |
| `--prefix`                    | `String` | `""`            | S3 key prefix                                                |
| `--region`                    | `String` | -               | AWS region                                                   |
| `--endpoint`                  | `String` | -               | Custom S3 endpoint                                           |
| `--duckdb-path`               | `Path`   | required        | Local DuckDB database file used during checkpointing         |
| `--checkpoint-interval-steps` | `u64`    | `100`           | Capture a checkpoint every N ETL steps                       |
| `--checkpoint-dir`            | `Path`   | `./checkpoints` | Local directory for checkpoint parquet files                 |

### Example

```bash
spicebench checkpoint \
    --scenario tpch \
    --version 1.0 \
    --bucket my-data \
    --prefix raw \
    --duckdb-path ./checkpoints.duckdb \
    --checkpoint-interval-steps 5 \
    --checkpoint-dir ./checkpoints
```

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
