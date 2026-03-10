# Data Generation & ETL

SpiceBench uses a two-stage data pipeline: `data-generation` produces versioned raw archives, and the ETL pipeline reads those archives, rehydrates records, and ingests them into the System Under Test.

The main `spicebench` binary benchmarks ingestion and querying against a pre-generated archive. Archive download and extraction happen before the timed benchmark starts.

## Data Generation

The `data-generation` crate produces versioned datasets and writes the result either to S3 or to a local `.tar.zst` archive.

### S3 Layout

```text
s3://{bucket}/{prefix}/{scenario}/{version}/
├── archive.tar.zst                 # Archive uploaded by the generator
├── version.json                    # Version metadata
├── {table_name}/
│   ├── metadata.json               # Table metadata (schema, keys, batch info)
│   ├── batch_{id}/
│   │   ├── part_0.parquet          # Parquet batch data
│   │   ├── part_1.parquet
│   │   └── ...
│   └── ...
└── ...
```

### Version Metadata (`version.json`)

Abbreviated example:

```json
{
    "version": "1.0",
    "scenario": "tpch",
    "scale_factor": 1.0,
    "num_steps": 10,
    "dataset_type": "tpch",
    "mutations": {
        "update_ratio": 0.0,
        "delete_ratio": 0.0
    }
}
```

The full file also includes per-table metadata used by ETL.

### Table Metadata

Each table directory contains a `metadata.json` with:

- Schema
- Primary key columns
- Time column
- Batch IDs
- Batch part counts

### Supported Datasets

| Dataset         | Type              | Description                                |
| --------------- | ----------------- | ------------------------------------------ |
| TPC-H           | `tpch`            | 8 standard TPC-H benchmark tables          |
| Simple Sequence | `simple_sequence` | Simple integer sequence tables for testing |

### TPC-H Tables

| Table      | Primary Key                  |
| ---------- | ---------------------------- |
| `customer` | `c_custkey`                  |
| `lineitem` | `l_orderkey`, `l_linenumber` |
| `nation`   | `n_nationkey`                |
| `orders`   | `o_orderkey`                 |
| `part`     | `p_partkey`                  |
| `partsupp` | `ps_partkey`, `ps_suppkey`   |
| `region`   | `r_regionkey`                |
| `supplier` | `s_suppkey`                  |

### Operation Codes

The ETL pipeline understands three raw operation codes:

| Operation | Internal Column | Description                                  |
| --------- | --------------- | -------------------------------------------- |
| Create    | `__op = "c"`    | New row insertion                            |
| Update    | `__op = "u"`    | Modify existing row (tracked by primary key) |
| Delete    | `__op = "d"`    | Remove existing row (tracked by primary key) |

The current `data-generation run` CLI does not expose mutation-ratio flags and currently emits create-only batches. The generated `version.json` therefore records `update_ratio = 0.0` and `delete_ratio = 0.0` in the shipped path.

### Running Data Generation

```bash
cargo run -p data-generation -- run \
    --scale-factor 1 \
    --bucket my-benchmark-data \
    --region us-west-2 \
    --prefix raw \
    --num-steps 10
```

To write a local archive instead of uploading to S3, use `--output-archive ./tpch-sf1.tar.zst`.

---

## ETL Pipeline

The ETL pipeline reads a generated archive, processes raw batches, and writes to a configurable sink.

### Processing Steps

1. Read raw Parquet batches from the extracted archive
2. Rehydrate records and append the time column
3. Split rows by operation type (`__op`)
4. Append `__created_at` for freshness tracking
5. Strip internal columns (`__op`, `__key_*`)
6. Write the resulting batches to the configured sink

### Local Archive Mode

Instead of downloading from S3, standalone ETL can read a local archive directly:

```bash
cargo run -p etl -- \
    --scenario tpch \
    --scale-factor 1 \
    --archive-file ./tpch-sf1.tar.zst \
    --sink null
```

### Sinks

#### S3 Hive Sink (default)

Writes hive-partitioned Parquet to S3. Each batch becomes one or more Parquet files partitioned by `__created_at` or a custom partition key list.

```bash
cargo run -p etl -- \
    --scenario tpch \
    --scale-factor 1 \
    --bucket my-data \
    --prefix raw \
    --sink s3-hive \
    --target-prefix rehydrated \
    --partition-by __created_at
```

Output layout:

```text
s3://{bucket}/{target-prefix}/{scenario}/{run_id}/
└── {table_name}/
    └── __created_at={timestamp}/
        └── part_0.parquet
```

#### ADBC Sink

Writes directly to the SUT via ADBC bulk ingest.

```bash
cargo run -p etl -- \
    --scenario tpch \
    --scale-factor 1 \
    --bucket my-data \
    --prefix raw \
    --sink adbc \
    --adbc-driver flightsql \
    --adbc-uri "grpcs://my-platform.example.com:443" \
    --adbc-option username="" \
    --adbc-option password="$API_KEY" \
    --adbc-create-tables
```

When using FlightSQL, ETL automatically sets `adbc.flight.sql.client_option.with_max_msg_size` to `78643200` (75 MiB) unless you explicitly override that option with `--adbc-option`.

**Databricks example:**

```bash
cargo run -p etl -- \
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

#### Null Sink

Discards all writes. Useful for measuring source and ETL throughput without sink overhead.

```bash
cargo run -p etl -- \
    --scenario tpch \
    --scale-factor 1 \
    --bucket my-data \
    --prefix raw \
    --sink null
```

### Pipeline States

The ETL pipeline transitions through these states:

```text
NotStarted -> Initialized -> Running -> Paused -> Running -> ... -> Stopped
```

| State                | Description                                  |
| -------------------- | -------------------------------------------- |
| `NotStarted`         | Pipeline created but not initialized         |
| `Initialized`        | Storage connected and metadata loaded        |
| `Running`            | Actively processing batches                  |
| `Paused`             | Temporarily paused for checkpoint validation |
| `Stopped(Completed)` | All batches processed successfully           |
| `Stopped(Cancelled)` | Pipeline cancelled by user or system         |
| `Stopped(Error)`     | Pipeline stopped due to an error             |

### ETL within SpiceBench

In the current main benchmark path:

1. SpiceBench downloads and extracts the data archive before timed execution
2. SpiceBench calls adapter `setup` and prepares the ADBC query path
3. The timed benchmark starts, then ETL runs concurrently with query execution
4. At checkpoint boundaries, ETL can pause for result validation when `--validate-results` is enabled and checkpoints are available
5. After ETL completes, SpiceBench stops the benchmark and then calls adapter `teardown`

The ETL sink type is selected with `--etl-sink`:

- `hive`: S3 Hive Parquet output. The adapter receives S3 dataset locations in `setup`
- `adbc`: direct ADBC ingest. The adapter's `setup` response provides write-side ADBC config

---

## Checkpointing

The `checkpointer` binary captures expected query results at specific ETL steps so benchmark runs can validate correctness while ingestion is active.

### How It Works

1. Generate checkpoints by replaying ETL into DuckDB
2. Execute the scenario's query workload at configured checkpoint intervals
3. Write each checkpoint result set as Parquet
4. Upload checkpoint files and a manifest to S3
5. During benchmark runs, pause ETL at checkpoint boundaries and compare live results against the stored checkpoint data

### S3 Checkpoint Layout

```text
s3://{bucket}/{prefix}/
├── checkpoints.json
└── checkpoints/
    └── {scenario}/
        └── {checkpoint_idx}/
            ├── {query_idx_0}.parquet
            ├── {query_idx_1}.parquet
            └── ...
```

### Checkpoint Manifest

```json
{
    "scenarios": {
        "tpch": {
            "checkpoint_indexes": [5, 10],
            "query_indexes": [0, 1, 2, 3, 4],
            "checkpoint_interval_steps": 5
        }
    }
}
```

### Using Checkpoints in SpiceBench

Enable checkpoint validation with `--validate-results`:

```bash
spicebench \
    --scenario tpch \
    --system-adapter-name myplatform \
    --system-adapter-http-url http://127.0.0.1:8080/jsonrpc \
    --validate-results
```

During the benchmark, when ETL reaches a checkpoint step:

1. ETL pauses
2. SpiceBench runs the scenario workload against the current system state
3. Results are compared against stored checkpoint output
4. ETL resumes if validation succeeds
