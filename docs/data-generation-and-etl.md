# Data Generation & ETL

SpiceBench uses a two-stage data pipeline: **data generation** produces raw TPC-H batches in S3, and the **ETL pipeline** reads, rehydrates, and ingests them into the System Under Test.

## Data Generation

The `data-generation` crate produces TPC-H datasets as partitioned Parquet batches and writes them to S3.

### S3 Layout

```
s3://{bucket}/{prefix}/{scenario}/{version}/
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

Written by the generator, consumed by the ETL pipeline:

```json
{
    "version": "1",
    "scenario": "tpch",
    "scale_factor": 1.0,
    "num_steps": 10,
    "dataset_type": "tpch",
    "update_ratio": 0.1,
    "delete_ratio": 0.05
}
```

### Table Metadata

Each table directory contains a `metadata.json` with:

- **Schema** — Arrow schema (field names, types, nullability)
- **Key columns** — Primary key columns for mutation tracking
- **Time column** — Timestamp column for temporal ordering
- **Batch IDs** — List of generated batch IDs
- **Batch parts** — Part counts per batch

### Supported Datasets

| Dataset         | Type              | Description                                   |
| --------------- | ----------------- | --------------------------------------------- |
| TPC-H           | `tpch`            | 8 standard TPC-H tables with mutation support |
| Simple Sequence | `simple_sequence` | Simple integer sequence tables for testing    |

### TPC-H Tables

| Table      | Primary Key                  | Supports Mutations |
| ---------- | ---------------------------- | ------------------ |
| `customer` | `c_custkey`                  | Yes                |
| `lineitem` | `l_orderkey`, `l_linenumber` | Yes                |
| `nation`   | `n_nationkey`                | Yes                |
| `orders`   | `o_orderkey`                 | Yes                |
| `part`     | `p_partkey`                  | Yes                |
| `partsupp` | `ps_partkey`, `ps_suppkey`   | Yes                |
| `region`   | `r_regionkey`                | Yes                |
| `supplier` | `s_suppkey`                  | Yes                |

### Mutations

Data generation supports three operation types:

| Operation  | Internal Column | Description                                  |
| ---------- | --------------- | -------------------------------------------- |
| **Create** | `__op = "c"`    | New row insertion                            |
| **Update** | `__op = "u"`    | Modify existing row (tracked by primary key) |
| **Delete** | `__op = "d"`    | Remove existing row (tracked by primary key) |

Mutation ratios are configurable:
- `--update-ratio` — fraction of rows that are updates (default: 0.1)
- `--delete-ratio` — fraction of rows that are deletes (default: 0.05)

### Running Data Generation

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

## ETL Pipeline

The ETL pipeline reads raw batches from S3, processes them, and writes to a configurable sink.

### Processing Steps

1. **Read** — Fetch raw Parquet batches from S3
2. **Rehydrate** — Restore full records from columnar format, apply time column
3. **Split** — Separate rows by operation type (`__op`: create, update, delete)
4. **Timestamp** — Append `__created_at` column for freshness tracking
5. **Strip** — Remove internal columns (`__op`, `__key_*`)
6. **Write** — Send processed batches to the configured sink

### Sinks

#### S3 Hive Sink (default)

Writes hive-partitioned Parquet to S3. Each batch becomes a set of Parquet files partitioned by `__created_at` (or custom partition columns).

```bash
cargo run -p etl -- \
    --scenario tpch \
    --version 1 \
    --bucket my-data \
    --prefix raw \
    --sink s3-hive \
    --target-prefix rehydrated \
    --partition-by __created_at
```

Output layout:

```
s3://{bucket}/{target-prefix}/{scenario}/{run_id}/
└── {table_name}/
    └── __created_at={timestamp}/
        └── part_0.parquet
```

#### ADBC Sink

Writes directly to the SUT via ADBC bulk ingest. Supports FlightSQL, Databricks, and PostgreSQL drivers.

```bash
cargo run -p etl -- \
    --scenario tpch \
    --version 1 \
    --bucket my-data \
    --prefix raw \
    --sink adbc \
    --adbc-driver flightsql \
    --adbc-uri "grpcs://my-platform.example.com:443" \
    --adbc-option username="" \
    --adbc-option password="$API_KEY" \
    --adbc-create-tables
```

When using FlightSQL, the ETL pipeline automatically sets `adbc.flight.sql.client_option.with_max_msg_size` to `78643200` (75 MiB) unless explicitly overridden.

**Databricks example:**

```bash
cargo run -p etl -- \
    --scenario tpch \
    --version 1 \
    --bucket my-data \
    --prefix raw \
    --sink adbc \
    --adbc-driver databricks \
    --adbc-uri "databricks://token:${DATABRICKS_TOKEN}@${DATABRICKS_ENDPOINT}:443/${DATABRICKS_HTTP_PATH}" \
    --adbc-create-tables \
    --adbc-schema tpch
```

#### Null Sink

Discards all writes. Useful for measuring source + ETL pipeline throughput without sink overhead.

```bash
cargo run -p etl -- \
    --scenario tpch \
    --version 1 \
    --bucket my-data \
    --prefix raw \
    --sink null
```

### Pipeline States

The ETL pipeline transitions through these states:

```
NotStarted → Initialized → Running → Paused → Running → ... → Stopped
```

| State                | Description                                    |
| -------------------- | ---------------------------------------------- |
| `NotStarted`         | Pipeline created but not initialized           |
| `Initialized`        | Storage connected, metadata loaded             |
| `Running`            | Actively processing batches                    |
| `Paused`             | Temporarily paused (for checkpoint validation) |
| `Stopped(Completed)` | All batches processed successfully             |
| `Stopped(Cancelled)` | Pipeline cancelled by user or system           |
| `Stopped(Error)`     | Pipeline stopped due to an error               |

### ETL within SpiceBench

When SpiceBench runs in `direct-query` mode, it manages the ETL pipeline internally:

1. The pipeline initializes with source configuration from `--etl-*` flags
2. During the benchmark phase, ETL runs concurrently with query execution
3. At checkpoint boundaries (if configured), ETL pauses for result validation
4. After the load test, SpiceBench waits for ETL completion before teardown

The ETL sink type is selected via `--etl-sink`:

- `hive` — S3 Hive Parquet (default). The adapter's `create_tables` receives S3 `location` paths.
- `adbc` — Direct ADBC ingest. The adapter's `setup` response provides write-side ADBC config.

---

## Checkpointing

The `checkpointer` binary captures expected query results at specific ETL steps to enable correctness validation during benchmark runs.

### How It Works

1. **Generate checkpoints** — Run ETL to specific steps, execute queries, save results as Parquet
2. **Upload to S3** — Store checkpoint files and a manifest in S3
3. **Validate during benchmark** — SpiceBench downloads checkpoints, pauses ETL at checkpoint boundaries, runs queries, and compares results

### S3 Checkpoint Layout

```
s3://{bucket}/{prefix}/
├── checkpoints.json                          # Manifest
└── checkpoints/
    └── {scenario}/
        └── {checkpoint_idx}/
            ├── {query_idx_0}.parquet         # Expected results
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
    --query-set tpch \
    --system-adapter-name myplatform \
    --system-adapter-execution-mode direct-query \
    --system-adapter-http-url http://127.0.0.1:8080/jsonrpc \
    --validate-results
```

During the benchmark, when ETL reaches a checkpoint step:

1. ETL pipeline pauses
2. SpiceBench executes the query set
3. Results are compared against stored expected results
4. ETL resumes if validation passes
