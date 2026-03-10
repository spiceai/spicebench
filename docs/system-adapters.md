# System Adapters

System adapters decouple SpiceBench from specific data platforms. Each adapter implements a JSON-RPC 2.0 interface that SpiceBench calls to prepare connection details for the System Under Test (SUT), optionally provision benchmark resources, and optionally clean them up afterward.

## Current Support

SpiceBench currently supports benchmark runs against:

- Databricks SQL
- Databricks Lakebase
- Spice Cloud

## Protocol Overview

| Method                                             | Purpose                                                               |
| -------------------------------------------------- | --------------------------------------------------------------------- |
| `setup(run_id, metadata, datasets, etl_sink_type)` | Return ADBC config and optionally provision/register benchmark assets |
| `teardown(run_id)`                                 | Optionally clean up resources created or referenced by `setup`        |
| `metrics(run_id)`                                  | Return resource and ingestion metrics                                 |
| `rpc.methods`                                      | Return the list of supported JSON-RPC methods                         |

## Transport Modes

### stdio (child process)

SpiceBench starts the adapter as a child process and communicates via stdin/stdout using line-delimited JSON-RPC.

```bash
spicebench \
    --scenario tpch \
    --system-adapter-name my-adapter \
    --system-adapter-stdio-cmd ./my-adapter \
    --system-adapter-stdio-args "stdio" \
    --system-adapter-env SECRET_KEY=$SECRET_KEY
```

- `--system-adapter-stdio-cmd` - command to start the adapter
- `--system-adapter-stdio-args` - arguments passed to the command
- `--system-adapter-env KEY=VALUE` - environment variables (repeatable, stdio only)

### HTTP (remote server)

SpiceBench connects to a running adapter server via HTTP POST.

```bash
spicebench \
    --scenario tpch \
    --system-adapter-name my-adapter \
    --system-adapter-http-url http://127.0.0.1:8080/jsonrpc
```

Set **exactly one** of `--system-adapter-stdio-cmd` or `--system-adapter-http-url`.

## Method Specifications

### `setup`

Returns ADBC connection details for the benchmark run and can optionally provision or register benchmark resources.

**Request:**

```json
{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "setup",
    "params": {
        "run_id": "550e8400-e29b-41d4-a716-446655440000",
        "metadata": {
            "scenario": "tpch",
            "table_format": "parquet",
            "executor_instance_type": "c6i.4xlarge",
            "system_under_test": "myplatform",
            "etl_bucket": "spiceai-public-datasets",
            "etl_prefix": "data-gen",
            "etl_version": "1.0"
        },
        "datasets": {
            "customer": {
                "schema": { ... },
                "primary_key_columns": ["c_custkey"],
                "time_column": "__created_at",
                "partition_columns": ["__created_at"]
            }
        },
        "etl_sink_type": "hive"
    }
}
```

**Response:**

```json
{
    "jsonrpc": "2.0",
    "id": 1,
    "result": {
        "driver": "flightsql",
        "db_kwargs": {
            "uri": "grpc+tls://my-platform.example.com:443",
            "username": "",
            "password": "my-api-key"
        },
        "catalog_namespace": "my_catalog.my_schema"
    }
}
```

The response tells SpiceBench which ADBC driver to use for query execution. For manually prepared systems, `setup` can simply validate inputs and return the existing driver + connection details without creating any new resources.

| Field               | Required | Description                                                |
| ------------------- | -------- | ---------------------------------------------------------- |
| `driver`            | Yes      | ADBC driver name (`flightsql`, `databricks`, `postgresql`) |
| `db_kwargs`         | Yes      | Driver-specific connection parameters                      |
| `catalog_namespace` | No       | Catalog/schema path where tables were created              |
| `read_driver`       | No       | Optional separate driver + kwargs for read-side queries    |

### `teardown`

Optionally cleans up resources created or referenced during `setup`.

**Request:**

```json
{
    "jsonrpc": "2.0",
    "id": 3,
    "method": "teardown",
    "params": {
        "run_id": "550e8400-e29b-41d4-a716-446655440000"
    }
}
```

**Response:**

```json
{
    "jsonrpc": "2.0",
    "id": 3,
    "result": { "ok": true }
}
```

### `metrics` (optional)

Returns current resource usage and ingestion progress from the SUT. SpiceBench scrapes this every 5 seconds when `--scrape-sut-metrics` is enabled.

**Request:**

```json
{
    "jsonrpc": "2.0",
    "id": 4,
    "method": "metrics",
    "params": {
        "run_id": "550e8400-e29b-41d4-a716-446655440000"
    }
}
```

**Response:**

```json
{
    "jsonrpc": "2.0",
    "id": 4,
    "result": {
        "resource": {
            "cpu_usage_percent": 45.2,
            "memory_usage_bytes": 8589934592,
            "disk_read_bytes": 1073741824,
            "disk_write_bytes": 2147483648,
            "disk_read_iops": 5000,
            "disk_write_iops": 3000
        },
        "ingestion": {
            "rows_ingested": 10000000,
            "bytes_ingested": 5368709120,
            "rows_per_sec": 50000.0,
            "active_connections": 8
        }
    }
}
```

All fields in `resource` and `ingestion` are **optional** - return `null` or omit fields that are unavailable from your SUT. The default `Handler::metrics()` implementation returns empty metrics, so existing adapters remain compatible without changes.

### `rpc.methods`

Returns the list of JSON-RPC methods supported by the adapter.

**Request:**

```json
{
    "jsonrpc": "2.0",
    "id": 5,
    "method": "rpc.methods",
    "params": {}
}
```

**Response:**

```json
{
    "jsonrpc": "2.0",
    "id": 5,
    "result": ["setup", "teardown", "metrics", "rpc.methods"]
}
```

An adapter can therefore implement `teardown` as a no-op when the SUT is managed externally or benchmark artifacts should be preserved for inspection.

## Adapter Lifecycle

In the current shipped benchmark path, SpiceBench calls adapter methods in this order:

```text
setup(run_id, metadata, datasets, etl_sink_type)
    │
    ▼
benchmark execution
    │  ├── concurrent query clients (via ADBC)
    │  ├── ETL pipeline (data ingestion)
    │  └── optional: metrics(run_id) every 5s
    │
    ▼
teardown(run_id)
```

SpiceBench always invokes `teardown`, even if the benchmark encounters errors, but the adapter may choose to perform full cleanup, partial cleanup, or a no-op depending on how the target system is managed.

## Adapter Development

SpiceBench supports adding new system adapters for benchmark runs. See [Supported Systems](#current-support) for first-class adapters. Additional adapter development is possible using the starter templates below.

### Starter Templates

Templates are available in `system-adapters/templates/` for five languages:

| Language | Path                                | Runtime          |
| -------- | ----------------------------------- | ---------------- |
| Python   | `system-adapters/templates/python/` | Python 3.10+     |
| Node.js  | `system-adapters/templates/nodejs/` | Node.js 18+      |
| Rust     | `system-adapters/templates/rust/`   | Rust (nightly)   |
| Go       | `system-adapters/templates/go/`     | Go 1.21+         |
| Java     | `system-adapters/templates/java/`   | Java 17+ / Maven |

All templates:

- Implement JSON-RPC 2.0 methods: `setup`, `teardown`, `metrics`, `rpc.methods`
- Support both **stdio** (line-delimited requests) and **HTTP** (POST endpoint) transports
- Include `metrics` stubs with commented examples for SUT monitoring
- Are intentionally minimal and designed for customization

### Implementation Checklist

1. **`setup`** - Parse `metadata`, `datasets`, and `etl_sink_type` from the request. Return an ADBC `driver` name and `db_kwargs` connection map. If your adapter manages lifecycle, this is the place to start services, create schemas, or register benchmark destination tables from `datasets` (using Arrow schema, `primary_key_columns`, `time_column`, `partition_columns`, and `location` for Hive sources).

2. **`teardown`** - If your adapter created temporary resources, drop tables, stop services, and release them here. If the SUT is pre-provisioned, a successful no-op teardown is fine. Track any state from `setup` using `run_id`.

3. **`metrics`** (optional) - Poll your SUT for CPU, memory, disk I/O, and ingestion progress. Return whatever is available; omit unavailable fields.

### Rust Adapter (using `system-adapter-protocol`)

For Rust adapters, the `system-adapter-protocol` crate provides a server framework:

```rust
use system_adapter_protocol::{Handler, Server};

struct MyAdapter { /* state */ }

#[async_trait::async_trait]
impl Handler for MyAdapter {
    async fn setup(&self, request: SetupRequest) -> Result<SetupResponse, JsonRpcError> {
        // Optionally prepare the SUT, then return ADBC config
    }

    async fn teardown(&self, request: TeardownRequest) -> Result<TeardownResponse, JsonRpcError> {
        // Optionally clean up
    }
}

#[tokio::main]
async fn main() {
    let adapter = MyAdapter { /* ... */ };
    let server = Server::new(adapter);
    server.run_stdio().await;
}
```

### Error Handling

Return JSON-RPC errors using standard error codes:

| Code   | Constant           | Meaning                      |
| ------ | ------------------ | ---------------------------- |
| -32700 | `PARSE_ERROR`      | Invalid JSON                 |
| -32600 | `INVALID_REQUEST`  | Not a valid JSON-RPC request |
| -32601 | `METHOD_NOT_FOUND` | Method not supported         |
| -32602 | `INVALID_PARAMS`   | Invalid method parameters    |
| -32603 | `INTERNAL_ERROR`   | Internal adapter error       |

```json
{
    "jsonrpc": "2.0",
    "id": 1,
    "error": {
        "code": -32603,
        "message": "Failed to provision SUT",
        "data": "Connection timeout after 30s"
    }
}
```

## Existing Adapters

### Databricks Adapter

Located at `system-adapters/databricks/`. Creates external Parquet tables on Databricks via the SQL Statements API.

**Build:**

```bash
cargo build --manifest-path system-adapters/databricks/Cargo.toml
```

**Configuration (environment variables):**

| Variable                      | Description                    |
| ----------------------------- | ------------------------------ |
| `DATABRICKS_ENDPOINT`         | Databricks workspace URL       |
| `DATABRICKS_TOKEN`            | Personal access token          |
| `DATABRICKS_HTTP_PATH`        | SQL warehouse HTTP path        |
| `DATABRICKS_SQL_WAREHOUSE_ID` | SQL warehouse ID               |
| `DATABRICKS_TABLE_FORMAT`     | Table format (e.g., `parquet`) |
| `DATABRICKS_CATALOG`          | Unity Catalog name             |
| `DATABRICKS_SCHEMA`           | Schema name for tables         |

**Run:**

```bash
spicebench \
    --scenario tpch \
    --system-adapter-name databricks \
    --system-adapter-stdio-cmd system-adapters/databricks/target/debug/databricks-system-adapter \
    --system-adapter-stdio-args "stdio" \
    --system-adapter-env DATABRICKS_ENDPOINT=$DATABRICKS_ENDPOINT \
    --system-adapter-env DATABRICKS_TOKEN=$DATABRICKS_TOKEN \
    --system-adapter-env DATABRICKS_HTTP_PATH=$DATABRICKS_HTTP_PATH \
    --system-adapter-env DATABRICKS_SQL_WAREHOUSE_ID=$DATABRICKS_SQL_WAREHOUSE_ID \
    --system-adapter-env DATABRICKS_TABLE_FORMAT=parquet \
    --system-adapter-env DATABRICKS_CATALOG=spiceai_sandbox \
    --system-adapter-env DATABRICKS_SCHEMA=tpch
```

For GitHub Actions runs, use a `system_under_test` value prefixed with `databricks-` (e.g., `databricks-sql` or `databricks-lakebase`); the workflow routes to the Databricks adapter and passes the variant through setup metadata.

### Claude Skill

A Claude skill for automated adapter authoring is available at `.claude/skills/system-adapter-builder/SKILL.md`. It provides guidance for building and validating adapters including JSON-RPC setup, template scaffolding, and testing.
