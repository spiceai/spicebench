---
name: system-adapter-builder
description: Build or update a SpiceBench system adapter with JSON-RPC over stdio and HTTP, including setup/create_tables/teardown/metrics support and template validation.
---

# SpiceBench System Adapter Builder

Use this skill when implementing a new system adapter or customizing one of the templates in `system-adapters/templates`.

## What this skill builds

A JSON-RPC 2.0 adapter that supports both transports:

- stdio transport (line-delimited JSON-RPC messages)
- HTTP transport (`POST /jsonrpc` by default)

Required methods:

- `setup(run_id, datasets)`
- `create_tables(run_id)`
- `teardown(run_id)`
- `metrics(run_id)`
- `rpc.methods`

## Inputs to collect first

- Target language (`python`, `nodejs`, `rust`, `go`, or `java`)
- SUT provisioning flow for `setup` and `teardown`
- How to resolve query endpoint and credentials for `setup`
- Where to source metrics (cloud APIs, DB telemetry, host exporters)
- Runtime/toolchain target (latest/LTS channel used by repository workflows)

## Build steps

1. Copy the nearest template from `system-adapters/templates/<language>`.
2. Keep request/response envelopes JSON-RPC 2.0 compliant (`jsonrpc`, `id`, `method`, `params`).
3. Implement `setup` with run-scoped resources keyed by `run_id`.
4. Implement `setup` to return:
   - `driver`: typically `flightsql` or `databricks`
   - `db_kwargs`: real endpoint + auth kwargs for the SUT
5. Implement `create_tables` so the adapter creates/registers benchmark destination tables.
6. Implement `teardown` with run-scoped cleanup keyed by `run_id`.
7. Implement `metrics` to return both objects:
   - `resource`: CPU, memory, disk bytes, disk IOPS
   - `ingestion`: rows, bytes, rows/s, active connections
8. Keep stdio and HTTP using the same dispatcher so behavior is identical.
9. Return JSON-RPC errors with standard codes:
   - `-32700` parse error
   - `-32600` invalid request
   - `-32601` method not found
   - `-32602` invalid params
   - `-32603` internal error

## Metrics mapping checklist

Map live SUT metrics into these fields:

- `resource.cpu_usage_percent`
- `resource.memory_usage_bytes`
- `resource.disk_read_bytes`
- `resource.disk_write_bytes`
- `resource.disk_read_iops`
- `resource.disk_write_iops`
- `ingestion.rows_ingested`
- `ingestion.bytes_ingested`
- `ingestion.rows_per_sec`
- `ingestion.active_connections`

If any metric is unavailable, return `0`/`0.0` and document why.

## Validation checklist

- Adapter responds to all required methods over stdio and HTTP.
- `rpc.methods` includes every exposed method.
- `create_tables` creates/registers benchmark tables for each dataset.
- `setup` returns a valid `driver` and complete `db_kwargs`.
- `metrics` returns both `resource` and `ingestion` objects.
- Language build/syntax checks pass:
  - Python: `python -m py_compile`
  - Node.js: `node --check`
  - Rust: `cargo build`
  - Go: `go build ./...`
  - Java: `mvn compile`

## Done criteria

The adapter is considered complete when:

- both transports work,
- required methods are implemented,
- metric fields are populated from real telemetry or documented stubs,
- and template validation workflow passes in CI.
