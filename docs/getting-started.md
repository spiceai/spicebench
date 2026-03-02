# Getting Started

This guide covers installing, building, and running SpiceBench for the first time.

## Prerequisites

- **Rust** — nightly toolchain (see `rust-toolchain.toml`; currently Rust 1.91.0+, edition 2024)
- **S3 access** — read access to the source data bucket (default: `spiceai-public-datasets`)
- **System adapter** — a running or launchable adapter for your target platform (see [System Adapters](system-adapters.md))
- **ADBC driver** — the appropriate ADBC driver for your target (FlightSQL, Databricks, etc.)

### Optional

- **Docker** — for running adapters as containers
- **Grafana** — for visualizing benchmark metrics with the included dashboard

## Building

```bash
# Development build
make build-dev

# Release build
make build

# Install to ~/.spice/bin/
make install      # release
make install-dev  # debug
```

Or directly with Cargo:

```bash
cargo build -p spicebench              # debug
cargo build --release -p spicebench    # release
```

## Running Lint & Tests

```bash
make lint    # runs check + test + clippy
make test    # cargo test -p spicebench
make fmt     # format all code
```

## Quick Start

### 1. Build SpiceBench

```bash
make build-dev
```

### 2. Start or configure your system adapter

SpiceBench needs a system adapter to provision and communicate with the System Under Test. You can either:

- **Start a stdio adapter** — SpiceBench spawns it as a child process
- **Connect to an HTTP adapter** — SpiceBench connects to a running server

Example with the Databricks adapter (stdio):

```bash
# Build the adapter
cargo build --manifest-path system-adapters/databricks/Cargo.toml

# Install the ADBC driver
curl -LsSf https://dbc.columnar.tech/install.sh | sh
dbc install databricks
```

### 3. Run the benchmark

```bash
spicebench \
    --query-set tpch \
    --system-adapter-name databricks \
    --system-adapter-execution-mode direct-query \
    --system-adapter-stdio-cmd system-adapters/databricks/target/debug/databricks-system-adapter \
    --system-adapter-stdio-args "stdio" \
    --system-adapter-env DATABRICKS_ENDPOINT=$DATABRICKS_ENDPOINT \
    --system-adapter-env DATABRICKS_TOKEN=$DATABRICKS_TOKEN \
    --system-adapter-env DATABRICKS_HTTP_PATH=$DATABRICKS_HTTP_PATH \
    --system-adapter-env DATABRICKS_SQL_WAREHOUSE_ID=$DATABRICKS_SQL_WAREHOUSE_ID \
    --system-adapter-env DATABRICKS_TABLE_FORMAT=parquet \
    --system-adapter-env DATABRICKS_CATALOG=spiceai_sandbox \
    --system-adapter-env DATABRICKS_SCHEMA=tpch \
    --scrape-sut-metrics
```

### 4. View results

Results are emitted via OpenTelemetry to `telemetry.spiceai.io` and published on [SpiceBench.com](https://spicebench.com).

For local visualization, import `dashboards/spicebench-benchmarks.grafana.json` into Grafana.

## Docker

A minimal Docker image is provided:

```bash
docker build -t spicebench .
docker run --rm spicebench --help
```

The `Dockerfile` uses `debian:trixie-slim` and copies the pre-built binary to `/usr/local/bin/spicebench`.

## Data Generation (standalone)

To generate fresh TPC-H datasets:

```bash
cargo run -p data-generation -- run \
    --scale-factor 1 \
    --bucket my-bucket \
    --region us-west-2 \
    --prefix raw \
    --num-steps 10 \
    --table-format parquet
```

See [Data Generation & ETL](data-generation-and-etl.md) for full details.

## ETL Pipeline (standalone)

To run the ETL pipeline independently:

```bash
cargo run -p etl -- \
    --scenario tpch \
    --version 1 \
    --bucket spiceai-public-datasets \
    --prefix data-gen \
    --target-prefix rehydrated \
    --partition-by __created_at
```

See [Data Generation & ETL](data-generation-and-etl.md) for all sink options.

## Next Steps

- [CLI Reference](cli-reference.md) — all flags and options for `spicebench` and `data-generation`
- [System Adapters](system-adapters.md) — how to build an adapter for your platform
- [Configuration](configuration.md) — Spicepod YAML format and query sets
- [Metrics & Telemetry](metrics-and-telemetry.md) — all collected metrics and how to visualize them
