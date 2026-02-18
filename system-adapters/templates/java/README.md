# Java System Adapter Template

This template is a minimal JSON-RPC 2.0 system adapter for SpiceBench.

Runtime target: Java `25` (latest LTS).

Implemented methods:

- `setup`
- `create_tables`
- `query_method`
- `teardown`
- `metrics`
- `rpc.methods`

The same JSON-RPC dispatcher is used by both transports:

- stdio (line-delimited JSON-RPC requests)
- HTTP (POST to `/jsonrpc` by default)

## Run

### Stdio mode

```bash
mvn -f system-adapters/templates/java/pom.xml -q compile exec:java -Dexec.mainClass=com.spicebench.template.AdapterServer -Dexec.args="--transport stdio"
```

### HTTP mode

```bash
mvn -f system-adapters/templates/java/pom.xml -q compile exec:java -Dexec.mainClass=com.spicebench.template.AdapterServer -Dexec.args="--transport http --host 127.0.0.1 --port 8080 --path /jsonrpc"
```

## Use with SpiceBench

### Stdio transport

```bash
spicebench \
  --query-set tpch \
  --spicepod-path ./spicepod.yaml \
  --system-adapter-name java-template \
  --system-adapter-stdio-cmd mvn \
  --system-adapter-stdio-args "-f system-adapters/templates/java/pom.xml -q compile exec:java -Dexec.mainClass=com.spicebench.template.AdapterServer -Dexec.args='--transport stdio'"
```

### HTTP transport

```bash
spicebench \
  --query-set tpch \
  --spicepod-path ./spicepod.yaml \
  --system-adapter-name java-template \
  --system-adapter-http-url http://127.0.0.1:8080/jsonrpc
```

## Notes

- `query_method` returns a placeholder FlightSQL connection in `db_kwargs`.
- Update `query_method` to match your real target system and credentials.
- `metrics` returns zero values by default and includes commented examples for where to poll real SUT telemetry.
- If you use `--system-adapter-execution-mode adapter-command`, implement custom RPC methods such as `run.load` for your adapter runtime.
