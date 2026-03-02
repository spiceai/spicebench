# Python System Adapter Template

This template is a minimal JSON-RPC 2.0 system adapter for SpiceBench.

Runtime target: Python `3.x` (latest stable).

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
python3 adapter.py --transport stdio
```

### HTTP mode

```bash
python3 adapter.py --transport http --host 127.0.0.1 --port 8080 --path /jsonrpc
```

## Use with SpiceBench

### Stdio transport

```bash
spicebench \
  --query-set tpch \
  --spicepod-path ./spicepod.yaml \
  --system-adapter-name python-template \
  --system-adapter-stdio-cmd python3 \
  --system-adapter-stdio-args "system-adapters/templates/python/adapter.py --transport stdio"
```

### HTTP transport

```bash
spicebench \
  --query-set tpch \
  --spicepod-path ./spicepod.yaml \
  --system-adapter-name python-template \
  --system-adapter-http-url http://127.0.0.1:8080/jsonrpc
```

## Notes

- `setup` returns a placeholder FlightSQL connection in `db_kwargs` and receives dataset definitions to create/register benchmark tables.
- Update `setup` to match your real target system and credentials.
- `metrics` returns zero values by default and includes commented examples for where to poll real SUT telemetry.
- If you use `--system-adapter-execution-mode adapter-command`, implement custom RPC methods such as `run.load` for your adapter runtime.
