## Goal-State/What/Result

SpiceBench gains first-class streaming/CDC benchmark support for three connector types — **PostgreSQL** (WAL CDC), **DynamoDB** (DynamoDB Streams), and **Mongo-Debezium** (CDC via Debezium + Kafka) — each queryable through Spice (Cayenne or DuckDB acceleration).

Following metrics will be available for each added scenario:
* Benchmark duration,
* End-to-end latency,
* Throughput (queries/sec),
* Query latency p99 (overall and per-query),
* Per-query pass/fail status,
* Spice resource usage (CPU, memory, disk I/O).

## Why/Purpose

To build confidence in latency SLAs and catch regressions across storage backends in streaming execution, we need a reproducible harness that:

- Loads TPC-H mutable/immutable data through each backend's native write path.
- Measures how quickly Spice propagates changes to the query layer and verifies data correctness.
- Runs periodically (similarly to testoperator) and on-demand via GitHub Actions.

While the CH-benchmark covers OLTP-style ingestion (small, high-frequency updates simulating transactional load) and does not verify query result correctness, SpiceBench complements it with ETL-style ingestion (bulk insert, update, and delete across full TPC-H datasets) and first-class correctness verification via checkpoint validation — ensuring propagated data matches expected snapshots, not just that queries return fast.

## By When

**Issue/Spec written and reviewed:** May 26, 2025
**Done-Done:** May 29, 2025

## Done-Done

- [x] [Principles Driven](https://github.com/spiceai/spiceai/blob/trunk/docs/PRINCIPLES.md)
- [x] The Algorithm
- [ ] PM/Design Review
- [ ] DX/UX Review
- [ ] Release Notes / PRFAQ
- [ ] Threat Model / Security Review
- [ ] Tests
- [ ] Telemetry / Metrics / Task History
- [ ] Performance / Benchmarks
- [ ] Documentation
- [ ] Cookbook Recipes/Tutorials

## The Algorithm

- [x] Every requirement questioned?
- [x] Delete (Scope) any part you can.
- [x] Simplify.
- [x] Break down into smaller iterations/milestones.
- [x] Opportunities for automation.

## Specification

### Supported `system-under-test` variants

| `--system-under-test`    | Federated Storage        | Acceleration |
|--------------------------|--------------------------|---|
| `postgres-cdc-cayenne`   | PostgreSQL WAL CDC       | Cayenne |
| `postgres-cdc-duckdb`    | PostgreSQL WAL CDC       | DuckDB |
| `mongo-debezium-cayenne` | Mongo + Debezium + Kafka | Cayenne |
| `mongo-debezium-duckdb`  | Mongo + Debezium + Kafka | DuckDB |
| `dynamodb-cdc-cayenne`   | DynamoDB Streams         | Cayenne |
| `dynamodb-cdc-duckdb`    | DynamoDB Streams         | DuckDB |

All six variants support both `events` (append-only) and `changes` (mutations) workloads.

## How/Implementation Plan

### SpiceBench changes

#### Ingestion abstraction

SpiceBench currently supports only one ingestion path: ADBC. Every backend that spicebench writes to must expose an ADBC driver. For backends that already have one (Postgres, FlightSQL/Cayenne, Databricks) this is natural. For DynamoDB and Debezium (MongoDB source) it is not — neither has an off-the-shelf ADBC driver.

The proposed approach: introduce a `BulkIngestionSink` with three methods matching the three ETL operations:

```rust
pub trait BulkIngestionSink: Send + Sync {
  fn insert(&self, table_name: &str, batch: RecordBatch)
            -> anyhow::Result<Option<i64>>;

  fn update(&self, table_name: &str, batch: RecordBatch, pk_columns: &[String])
            -> anyhow::Result<Option<i64>>;

  fn delete(&self, table_name: &str, batch: RecordBatch, pk_columns: &[String])
            -> anyhow::Result<Option<i64>>;
}
```

Three concrete implementations:

- **`AdbcBulkIngestionSink`** — wraps the existing `AdbcConnection`. ADBC-specific concerns like `SPICEBENCH_ADBC_UPDATE_STRATEGY` (`statement`, `staging_table`, `bulk_ingest_upsert`) remain entirely inside this implementation and are invisible to the trait. Used for Postgres, FlightSQL/Cayenne, and Databricks.

- **`DynamoDbBulkIngestionSink`** — calls the AWS SDK directly:
  - `insert` / `update`: `BatchWriteItem` with `PutRequest` (DynamoDB's `PutItem` always upserts by partition+sort key — no extra logic needed).
  - `delete`: `BatchWriteItem` with `DeleteRequest` keyed on partition+sort key columns.

- **`MongoDbBulkIngestionSink`** — calls the MongoDB driver directly, used as the write target for the Debezium CDC variant (Debezium monitors MongoDB Change Streams and streams events to Spice):
  - `insert`: `Collection::insert_many`.
  - `update`: `Collection::bulk_write` with `ReplaceOneModel { filter: {pk_cols}, replacement: doc, upsert: true }`.
  - `delete`: `Collection::bulk_write` with `DeleteOneModel { filter: {pk_cols} }`.

#### System adapter protocol: `setup_ingestion` / `setup_query` split

`ETLPipeline.initialize()` is documented as:

> *"Initializes the ETL pipeline by processing only the first batch (batch ID 0) for every table. This ensures the target has some initial data before calling `setup()` on the system adapter."*

The implementation does the opposite: `setup()` is called first, then `pipeline.initialize()`. The order is inverted because of a hard dependency — `pipeline.initialize()` requires the `AdbcSink`, which requires the `SetupResponse` (driver name, connection kwargs, `table_name_map`), which only comes back from `setup()`. The intent in the docstring is simply impossible to achieve with a single `setup()` call.

For schema-ful backends (Postgres CDC, Databricks, Cayenne) this doesn't matter — the read connector (spiced) can start before any rows exist because it derives schema from DDL or explicit spicepod configuration. But for **schema-less backends** (DynamoDB, MongoDB), spiced infers schema by sampling existing documents at startup. If `setup()` starts spiced before any rows exist, schema inference fails or produces an empty schema.

##### Proposed: `setup_ingestion` / `setup_query`

Split the single `setup()` RPC into two:

1. **`setup_ingestion(metadata, datasets)`** — adapter creates tables/collections and returns a `SinkConfig` describing how spicebench should write data. In some cases may start spiced first (e.g. Cayenne). Returns `{ sink: SinkConfig, table_name_map }`. 
2. spicebench constructs the appropriate `BulkIngestionSink` from `SinkConfig` and calls **`pipeline.initialize()`** — writes real batch 0 through the normal ingest path.
3. **`setup_query(run_id)`** — Initializes read connection - in case of spidapter with MongoDB or DynamoDB starts spiced. By this point real data exists, so schema inference works correctly. Returns `{ read_driver, read_db_kwargs, catalog_namespace, endpoints }`.

```
setup_ingestion(run_id, metadata, datasets)
        → { sink: SinkConfig, table_name_map }
                    ↓
    spicebench: construct BulkIngestionSink from SinkConfig
                    ↓
    spicebench: pipeline.initialize()   ← real batch 0 written here
                    ↓
setup_query(run_id)
        → { read_driver, read_db_kwargs, catalog_namespace, endpoints }
```

`SinkConfig` is a tagged enum serialized as JSON over the JSON-RPC wire:

```rust
#[serde(tag = "type")]
pub enum SinkConfig {
  Adbc {
    driver: AdbcDriver,
    db_kwargs: HashMap<String, serde_json::Value>,
  },
  DynamoDb {
    region: String,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
  },
  MongoDb {
    uri: String,
  },
}
```

### Spidapter changes

#### CLI flags

Four orthogonal dimensions, each controlled by its own flag:

| Flag / env                                        | Values                                                            | Controls |
|---------------------------------------------------|-------------------------------------------------------------------|---|
| `--spice-compute` / `SPIDAPTER_COMPUTE`           | `cloud` (default), `local`                                        | Where **spiced** runs (Spice Cloud Platform vs local process) |
| `--storage` / `SPIDAPTER_STORAGE`                 | `cayenne` (default), `postgres-cdc`, `mongo-debezium`, `dynamodb` | CDC connector / federated storage backend |
| `--storage-compute` / `SPIDAPTER_STORAGE_COMPUTE` | `existing` (default), `ec2`                                       | How the storage backend is provisioned |
| `--acceleration` / `SPICE_ACCELERATION`           | `cayenne` (default), `duckdb`                                     | Acceleration engine inside spiced |

Notes:
* `--storage-compute existing` connects to a pre-existing host via `PG_HOST` / `MONGO_URI` / `KAFKA_BROKERS`
* `--storage-compute ec2` provisions a fresh EC2 instance for the run and tears it down on completion. Only relevant for `--storage postgres-cdc` and `--storage mongo-debezium`; ignored for `cayenne` and `dynamodb` (which are cloud-native and need no host provisioning).

EC2 flags (required when `--storage-compute ec2`):

| Flag / env | Default | Purpose |
|---|---|---|
| `EC2_SUBNET_ID` | — | VPC subnet for the instance |
| `EC2_SECURITY_GROUP_ID` | — | Security group (must allow 5432 for Postgres / 27017 for MongoDB / 9092 for Kafka) |
| `EC2_AMI_ID` | — | Ubuntu 22.04 AMI |
| `EC2_INSTANCE_TYPE` | `m5.large` | Instance type |
| `EC2_ASSOCIATE_PUBLIC_IP` | `false` | Assign public IP (needed outside VPC) |
| `EC2_IAM_INSTANCE_PROFILE` | — | IAM profile for SSM access |

On setup, spidapter launches the instance, installs Postgres 15 (for `--storage postgres-cdc`) or MongoDB 7.0 + Kafka + Debezium (for `--storage mongo-debezium`) via cloud-init, waits for the service to become reachable, then tears down the instance on benchmark completion.

#### Table name mapping (DynamoDB)

DynamoDB tables are created with a run-unique prefix to allow concurrent benchmark runs without collisions. The mapping from logical dataset name (e.g. `lineitem`) to physical DynamoDB table name (e.g. `sb_a1b2c3_lineitem`) is returned in `SetupResponse.table_name_map` and consumed by the ETL sink.

### Security Review

- No customer data; all data is synthetically generated TPC-H.
- PostgreSQL and MongoDB credentials are generated per-run and never stored in logs or artifacts.
- DynamoDB tables use run-unique prefixes; concurrent runs cannot collide.
- EC2 instances are always terminated on teardown, including on benchmark failure via drop guards.
- Benchmark runners access via AWS SSM Session Manager rather than direct SSH.
