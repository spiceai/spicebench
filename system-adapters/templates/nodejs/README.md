# Node.js System Adapter Template

This template is a minimal JSON-RPC 2.0 system adapter for SpiceBench.

Runtime target: Node.js latest LTS.

Implemented methods:

- `setup`
- `teardown`
- `metrics`
- `rpc.methods`

The same JSON-RPC dispatcher is used by both transports:

- stdio (line-delimited JSON-RPC requests)
- HTTP (POST to `/jsonrpc` by default)

## Run

### Stdio mode

```bash
node adapter.js --transport stdio
```

### HTTP mode

```bash
node adapter.js --transport http --host 127.0.0.1 --port 8080 --path /jsonrpc
```

## Use with SpiceBench

### Stdio transport

```bash
spicebench run \
  --scenario tpch \
  --system-adapter-name node-template \
  --system-adapter-stdio-cmd node \
  --system-adapter-stdio-args "system-adapters/templates/nodejs/adapter.js --transport stdio"
```

### HTTP transport

```bash
spicebench run \
  --scenario tpch \
  --system-adapter-name node-template \
  --system-adapter-http-url http://127.0.0.1:8080/jsonrpc
```

## Notes

- `setup` returns a placeholder FlightSQL connection in `db_kwargs` and receives dataset definitions to create/register benchmark tables.
- Update `setup` to match your real target system and credentials.
- `metrics` returns zero values by default and includes commented examples for where to poll real SUT telemetry.
- The current `spicebench` benchmark path uses `setup`, `metrics`, and `teardown` plus the ADBC connection returned from `setup`.
