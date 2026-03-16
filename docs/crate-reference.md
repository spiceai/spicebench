# Crate Reference

API overview for all workspace crates in SpiceBench.

## Dependency Graph

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

---

## `spicebench` (binary)

**Path:** `src/`

The single CLI entry point with four subcommands: `run` (benchmark lifecycle), `generate` (data generation), `etl` (standalone ETL pipeline), and `checkpoint` (checkpoint capture). Parses arguments, dispatches to the appropriate command handler, and orchestrates the full workflow.

### `spicebench` Key Modules

| Module                    | Description                                                                                |
| ------------------------- | ------------------------------------------------------------------------------------------ |
| `args`                    | CLI argument definitions via `clap` derive macros, including `Cli`, `Command`, and per-subcommand arg structs |
| `args::generate`          | `GenerateArgs` for the `generate` subcommand                                               |
| `args::etl`              | `EtlArgs` and `EtlSinkType` for the `etl` subcommand                                      |
| `args::checkpoint`        | `CheckpointArgs` for the `checkpoint` subcommand                                           |
| `args::dataset`           | Query-related argument types (`QueryArgs`, `DatasetTestArgs`, `QuerySetArg`)               |
| `commands::run`           | Full benchmark lifecycle (setup → ETL + queries → teardown)                                |
| `commands::generate`      | Data generation and archive creation/upload                                                |
| `commands::etl_cmd`       | Standalone ETL pipeline execution                                                          |
| `commands::checkpoint`    | Checkpoint capture with DuckDB (behind `duckdb` feature)                                   |
| `commands::load`          | Main load test runner with metrics scraping, E2E latency checks, and checkpoint validation |
| `commands::adbc_executor` | ADBC direct query executor implementing `QueryExecutor`                                    |
| `metrics`                 | OTel metric instrument definitions (all `LazyLock` statics)                                |
| `scenario`                | Re-exports `test_framework::Scenario`                                                      |

### Defined Metrics

All metrics are defined as `LazyLock` statics in `src/metrics.rs`:

```rust
pub static ITERATIONS: LazyLock<Gauge<u64>>;          // "iterations"
pub static QUERY_STATUS: LazyLock<Gauge<u64>>;        // "query_status"
pub static MEDIAN_DURATION: LazyLock<Gauge<u64>>;     // "median_duration_ms"
pub static MIN_DURATION: LazyLock<Gauge<u64>>;        // "min_duration_ms"
pub static MAX_DURATION: LazyLock<Gauge<u64>>;        // "max_duration_ms"
pub static P99_DURATION: LazyLock<Gauge<u64>>;        // "p99_duration_ms"
pub static TEST_DURATION: LazyLock<Gauge<u64>>;       // "test_duration_ms"
pub static HEALTH_LATENCY: LazyLock<Histogram<f64>>;  // "health_latency_ms"
pub static PEAK_MEMORY_USAGE: LazyLock<Gauge<f64>>;   // "peak_memory_usage_mb"
pub static MEDIAN_MEMORY_USAGE: LazyLock<Gauge<f64>>; // "median_memory_usage_mb"
pub static INGESTION_ROWS_PER_SEC: LazyLock<Gauge<f64>>; // "ingestion_rows_per_sec"
pub static QUERIES_TOTAL: LazyLock<Counter<u64>>;     // "queries_total"
pub static QUERIES_PER_SEC: LazyLock<Gauge<f64>>;     // "queries_per_sec"
pub static ACTIVE_CONNECTIONS: LazyLock<Gauge<u64>>;  // "active_connections"
pub static EFFICIENCY_QUERIES_PER_CORE: LazyLock<Gauge<f64>>; // "efficiency_queries_per_core"
pub static E2E_LATENCY_MS: LazyLock<Histogram<f64>>;  // "e2e_latency_ms"
```

---

## `test-framework`

**Path:** `crates/test-framework/`

Core benchmark engine - orchestrates query execution pipelines, manages scenarios, and collects statistics.

### `test-framework` Public Types

| Type              | Description                                                                                          |
| ----------------- | ---------------------------------------------------------------------------------------------------- |
| `Scenario` (enum) | Benchmark scenarios (e.g., `TPCH`). Methods: `load_query_set()`, `end_condition()`                   |
| `TestType` (enum) | Test types: `Throughput`, `Load`, `Benchmark`, `DataConsistency`, `Search`, `TextToSql`, `Streaming` |

### `test-framework` Key Modules

| Module      | Description                                         |
| ----------- | --------------------------------------------------- |
| `execution` | Query execution pipeline and benchmark helpers      |
| `flight`    | Arrow Flight integration                            |
| `metrics`   | Internal metrics collection                         |
| `queries`   | Query set loading, parameterization, and management |
| `snapshot`  | Snapshot testing utilities                          |
| `spicetest` | SpiceTest runner (throughput test orchestrator)     |
| `telemetry` | Telemetry integration                               |

### `test-framework` Re-exports

- `anyhow`, `arrow`, `opentelemetry`, `opentelemetry_sdk`, `rustls`

---

## `system-adapter-protocol`

**Path:** `crates/system-adapter-protocol/`

JSON-RPC 2.0 protocol definitions for system adapter communication. Supports both client and server roles with stdio and HTTP transports.

### Core Types

| Type                                                        | Description                                              |
| ----------------------------------------------------------- | -------------------------------------------------------- |
| `AdbcDriver` (enum)                                         | Supported ADBC drivers (e.g., `Flightsql`, `Databricks`) |
| `EtlSinkType` (enum)                                        | `Hive`, `Adbc`                                           |
| `EtlType` (enum)                                            | `S3`, `Adbc`                                             |
| `DatasetConfig`                                             | Dataset schema, keys, location, ETL type, and params     |
| `SetupRequest` / `SetupResponse`                            | Setup method request/response                            |
| `QueryMethodResponse`                                       | ADBC driver + connection kwargs                          |
| `TeardownRequest` / `TeardownResponse`                      | Teardown method request/response                         |
| `MetricsRequest` / `MetricsResponse`                        | Metrics method request/response                          |
| `ResourceMetrics`                                           | CPU, memory, disk I/O from the SUT                       |
| `IngestionMetrics`                                          | Rows, bytes, throughput, connection count from the SUT   |
| `JsonRpcRequest<T>` / `JsonRpcResponse<T>` / `JsonRpcError` | JSON-RPC 2.0 wire types                                  |

### Client (`client` feature)

| Type            | Description                                                                                 |
| --------------- | ------------------------------------------------------------------------------------------- |
| `Client` (enum) | `Stdio` or `Http` transport. Methods: `rpc_methods()`, `setup()`, `teardown()`, `metrics()` |
| `ClientBuilder` | Builder for constructing clients                                                            |
| `ClientError`   | Client error type                                                                           |

### Server (`server` feature)

| Type                 | Description                                                             |
| -------------------- | ----------------------------------------------------------------------- |
| `Handler` (trait)    | `setup()`, `teardown()`, `metrics()`, `query_method()`, `rpc_methods()` |
| `Server<H: Handler>` | JSON-RPC server. Method: `run_stdio()`                                  |

### `system-adapter-protocol` Constants

| Module        | Constants                                                                                                                             |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `methods`     | `SETUP`, `TEARDOWN`, `METRICS`, `RPC_METHODS`                                                                                         |
| `error_codes` | `PARSE_ERROR` (-32700), `INVALID_REQUEST` (-32600), `METHOD_NOT_FOUND` (-32601), `INVALID_PARAMS` (-32602), `INTERNAL_ERROR` (-32603) |

---

## `data-generation`

**Path:** `crates/data-generation/`

Generates Arrow data (TPC-H or simple sequences) with mutation support, packages results into a `.tar.zst` archive, and uploads the archive to S3 or writes it to a local path.

### `data-generation` Public Types

| Type                       | Description                                                    |
| -------------------------- | -------------------------------------------------------------- |
| `DatasetConfig`            | Dataset type, scale factor, number of steps                    |
| `TargetConfig`             | S3 target bucket, prefix, region, endpoint, partition columns  |
| `DataGenerator`            | Main generator. Method: `run()`                                |
| `VersionConfig`            | Version configuration for reproducible generation              |
| `VersionMetadata`          | Metadata read from/written to S3 (`version.json`)              |
| `TableMetadata`            | Per-table metadata (schema, keys, batch info)                  |
| `DatasetTable`             | Table definition with schema and methods                       |
| `MutationConfig`           | Update/delete ratios                                           |
| `IndexedKeySet<K>`         | O(1) insert/remove/random-select key set for mutation tracking |
| `PrimaryKeyValue` (enum)   | `Single(i64)` or `Composite(Box<[i64]>)`                       |
| `Metrics` / `IngestResult` | Atomic write counters                                          |

### `data-generation` Traits

| Trait         | Description                                                                              |
| ------------- | ---------------------------------------------------------------------------------------- |
| `Dataset`     | `create()`, `storage()`, `batch_ids()`, `next_batch()`, `tables()`, `primary_key()`      |
| `DataStorage` | `list_batches()`, `read_batch()`, `write()`, `read_version_metadata()`, `table_params()` |

### `data-generation` Implementations

| Struct                  | Description                                |
| ----------------------- | ------------------------------------------ |
| `TpchDataset`           | 8 TPC-H tables with full mutation support  |
| `SimpleSequenceDataset` | Simple integer sequence tables for testing |
| `S3Storage`             | S3 backend with caching                    |

---

## `etl`

**Path:** `crates/etl/`

ETL pipeline - reads from S3, rehydrates, and writes to configurable sinks.

### `etl` Public Types

| Type                   | Description                                                                       |
| ---------------------- | --------------------------------------------------------------------------------- |
| `DatasetSource` (enum) | `SimpleSequence`, `Tpch` - factory for Dataset instances                          |
| `PipelineState` (enum) | `NotStarted`, `Initialized`, `Running`, `Paused`, `Stopped(StopReason)`           |
| `StopReason` (enum)    | `Completed`, `Cancelled`, `Error(String)`                                         |
| `ETLPipeline`          | Full pipeline lifecycle: `initialize()`, `start()`, `run()`, `wait()`, `cancel()` |
| `InsertOp` (enum)      | `Insert`, `Update { key_columns }`, `Delete { key_columns }`                      |

### `etl` Sinks

| Struct       | Description                                                      |
| ------------ | ---------------------------------------------------------------- |
| `AdbcSink`   | ADBC bulk ingest. Method: `create_tables_from_dataset_configs()` |
| `NullSink`   | Discards all writes (for benchmarking pipeline throughput)       |

### `etl` Sink Trait

```rust
trait Sink {
    fn write(table_name, batch_id, batch, op, partition_columns) -> Result
}
```

---

## `adbc_client`

**Path:** `crates/adbc_client/`

Generic ADBC connection wrapper with r2d2 connection pooling.

### `adbc_client` Public Types

| Type                    | Description                                                                             |
| ----------------------- | --------------------------------------------------------------------------------------- |
| `AdbcConnection`        | Connection wrapper. Methods: `create()`, `query()`, `execute_update()`, `bulk_ingest()` |
| `AdbcConnectionPool`    | `r2d2::Pool<AdbcConnectionManager>` type alias                                          |
| `AdbcConnectionManager` | Implements `r2d2::ManageConnection`                                                     |
| `Error` (enum)          | `LoadDriver`, `CreateDatabase`, `CreateConnection`, `ExecuteQuery`, `ReadBatch`         |

### `adbc_client` Public Functions

```rust
fn create_pool(driver: &str, kwargs: HashMap, size: Option<u32>) -> Result<AdbcConnectionPool>
```

### `adbc_client` Re-exports

- `IngestMode` from `adbc_core`

---

## `flight_client`

**Path:** `crates/flight_client/`

Apache Arrow Flight client for querying and publishing data. Cheap to clone, supports TLS, auth, and cookie middleware.

### `flight_client` Public Types

| Type                               | Description                                                                  |
| ---------------------------------- | ---------------------------------------------------------------------------- |
| `FlightClient`                     | Main client. Methods: `try_new()`, `query()`, `publish()`, `with_metadata()` |
| `Credentials` (enum)               | `UsernamePassword`, `Anonymous`, `Bearer`                                    |
| `Error` (enum)                     | `UnableToConnectToServer`, `Unauthorized`, `PermissionDenied`, etc.          |
| `CookieStore`                      | Automatic cookie management                                                  |
| `CookieLayer` / `CookieService<S>` | Tower Layer/Service for cookie middleware                                    |

### `flight_client` Constants

- `MAX_ENCODING_MESSAGE_SIZE`: 100 MB
- `MAX_DECODING_MESSAGE_SIZE`: 100 MB

---

## `telemetry`

**Path:** `crates/telemetry/`

OpenTelemetry-based metrics collection with Arrow Flight export.

### `telemetry` Public Types

| Type                              | Description                                   |
| --------------------------------- | --------------------------------------------- |
| `TelemetryExporterBuilder`        | Builder for Arrow Flight exporter             |
| `TelemetryExporter`               | Implements `ArrowExporter`                    |
| `HardwareInfo`                    | CPU, GPU, memory detection (supports cgroups) |
| `NoopMeterProvider` / `NoopMeter` | No-op implementations for testing             |
| `InitialReader`                   | Wraps `ManualReader` for initial metric reads |

### `telemetry` Public Functions

| Function                           | Description                   |
| ---------------------------------- | ----------------------------- |
| `track_query_count()`              | Track query count             |
| `track_bytes_processed()`          | Track bytes processed         |
| `track_rows_returned()`            | Track rows returned           |
| `track_query_duration()`           | Track query duration          |
| `track_query_execution_duration()` | Track execution-only duration |

### `telemetry` Features

- `anonymous_telemetry` - SHA256-hashed instance IDs for anonymous usage tracking

---

## `otel-arrow`

**Path:** `crates/otel-arrow/`

Converts OpenTelemetry metrics to Arrow RecordBatch format.

### `otel-arrow` Public Types

| Type                   | Description                                                   |
| ---------------------- | ------------------------------------------------------------- |
| `OtelToArrowConverter` | `convert(&ResourceMetrics) -> RecordBatch`                    |
| `OtelArrowExporter<E>` | Implements `PushMetricExporter`, delegates to `ArrowExporter` |
| `Error`                | Conversion error type                                         |

### `otel-arrow` Traits

```rust
trait ArrowExporter {
    async fn export(batch: RecordBatch) -> Result;
    async fn force_flush() -> Result;
    async fn shutdown() -> Result;
}
```

### `otel-arrow` Public Functions

```rust
fn schema() -> Arc<Schema>  // Flattened OTel metrics Arrow schema
```

---

## `yaml`

**Path:** `crates/yaml/`

Custom YAML serialization/deserialization library with ordered mappings and merge key support.

### `yaml` Public Types

| Type            | Description                                                         |
| --------------- | ------------------------------------------------------------------- |
| `Value` (enum)  | `Null`, `Bool`, `Number`, `String`, `Sequence`, `Mapping`           |
| `Number` (enum) | `PosInt(u64)`, `NegInt(i64)`, `Float(f64)` - implements `Eq + Hash` |
| `Mapping`       | `IndexMap<Value, Value>` - ordered key-value map                    |
| `Error`         | Parse/serialize errors with source location                         |

### `yaml` Public Functions

| Function                   | Description                |
| -------------------------- | -------------------------- |
| `from_str(s)`              | Parse single YAML document |
| `from_reader(reader)`      | Parse from `Read`          |
| `to_string(value)`         | Serialize to YAML string   |
| `to_writer(writer, value)` | Serialize to `Write`       |

---

## `util`

**Path:** `crates/util/`

Shared utilities.

### `util` Public Functions

| Function                            | Description                               |
| ----------------------------------- | ----------------------------------------- |
| `human_readable_bytes(bytes)`       | Format bytes as human-readable string     |
| `pretty_print_number(n)`            | Format number with comma separators       |
| `parse_enabled(s)`                  | Parse boolean-like strings                |
| `shutdown_signal()`                 | Wait for Ctrl+C                           |
| `force_shutdown_signal()`           | Force shutdown on second Ctrl+C           |
| `humantime_elapsed(duration)`       | Human-readable elapsed time               |
| `distribute_nulls(batch, fraction)` | Randomly null out values in a RecordBatch |

### `util` Re-exports

- `backoff::Error as RetryError`, `ExponentialBackoff`, `backoff::future::retry`

---

## `duration-parse`

**Path:** `crates/duration-parse/`

Human-readable duration parsing and formatting.

### `duration-parse` Public Functions

```rust
fn parse_duration(input: &str) -> Result<Duration, ParseError>
fn format_duration(duration: Duration) -> String
```

### `duration-parse` Supported Units

`ns`, `us`/`μs`, `ms`, `s`, `m`, `h`, `d`, `w`

Accepts compound durations: `"1h30m"`, `"2.5d"`, `"10s"`.

---

## `checkpointer`

**Path:** `crates/checkpointer/`

Captures expected query results at ETL checkpoints for validation.

### `checkpointer` Public Types

| Type                 | Description                                        |
| -------------------- | -------------------------------------------------- |
| `CheckpointManifest` | `scenarios: HashMap<String, ScenarioCheckpoint>`   |
| `ScenarioCheckpoint` | Checkpoint indexes, query indexes, interval steps  |
| `CheckpointStore`    | S3-backed store for upload/download of checkpoints |

### `checkpointer` Key Methods

- `CheckpointStore::new(bucket, prefix, region, endpoint)`
- `CheckpointStore::upload_checkpoints()`
- `CheckpointStore::download_manifest()`
- `CheckpointStore::download_checkpoint()`
