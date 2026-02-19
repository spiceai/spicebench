#!/usr/bin/env node

const http = require('http');
const readline = require('readline');

const JSONRPC_VERSION = '2.0';

function jsonrpcSuccess(id, result) {
  return {
    jsonrpc: JSONRPC_VERSION,
    id,
    result,
  };
}

function jsonrpcError(id, code, message, data) {
  const error = { code, message };
  if (data !== undefined) {
    error.data = data;
  }

  return {
    jsonrpc: JSONRPC_VERSION,
    id,
    error,
  };
}

function methodSetup(params) {
  void params.run_id;
  void (params.datasets || {});

  // Stub: Provision or initialize your SUT for this run and return
  // query driver details SpiceBench should use.
  // Example:
  // - create a test database or schema for this run_id
  // - configure ingestion routes for provided datasets
  // - block until SUT readiness checks are healthy
  // - resolve endpoint + credentials from your control plane

  const host = process.env.SUT_HOST || '127.0.0.1';
  const port = Number(process.env.SUT_PORT || '50051');
  const useTls = (process.env.SUT_TLS || 'false').toLowerCase() === 'true';

  return {
    driver: 'flightsql',
    db_kwargs: {
      uri: `grpc${useTls ? 's' : ''}://${host}:${port}`,
      username: process.env.SUT_USERNAME || '',
      password: process.env.SUT_PASSWORD || '',
      tls: useTls,
    },
  };
}

function methodCreateTables(params) {
  void params.run_id;

  // Stub: Create/register destination tables for benchmark datasets.
  // Example:
  // - create tables if they do not exist
  // - apply expected schema/partitioning

  return { ok: true };
}

function methodTeardown(params) {
  void params.run_id;

  // Stub: Deprovision resources created in setup.
  // Example:
  // - drop run-scoped schema/database
  // - stop ingestion workers/jobs

  return { ok: true };
}

function methodMetrics(params) {
  void params.run_id;

  // Stub: Poll SUT telemetry and map values into the expected metrics shape.
  // Example sources:
  // - CPU/memory/disk from infra metrics APIs
  // - rows/bytes ingested from ingestion status endpoint
  // - active connections from DB or service diagnostics

  return {
    resource: {
      cpu_usage_percent: 0.0,
      memory_usage_bytes: 0,
      disk_read_bytes: 0,
      disk_write_bytes: 0,
      disk_read_iops: 0,
      disk_write_iops: 0,
    },
    ingestion: {
      rows_ingested: 0,
      bytes_ingested: 0,
      rows_per_sec: 0.0,
      active_connections: 0,
    },
  };
}

function methodRpcMethods() {
  return {
    methods: ['setup', 'create_tables', 'teardown', 'metrics', 'rpc.methods'],
  };
}

function dispatch(request) {
  const id = Object.prototype.hasOwnProperty.call(request, 'id')
    ? request.id
    : null;

  if (request.jsonrpc !== JSONRPC_VERSION) {
    return jsonrpcError(id, -32600, "Invalid Request: jsonrpc must be '2.0'");
  }

  if (typeof request.method !== 'string') {
    return jsonrpcError(id, -32600, 'Invalid Request: method must be a string');
  }

  const params = request.params !== undefined ? request.params : {};
  if (params === null || typeof params !== 'object' || Array.isArray(params)) {
    return jsonrpcError(id, -32602, 'Invalid params: expected object');
  }

  try {
    switch (request.method) {
      case 'setup':
        return jsonrpcSuccess(id, methodSetup(params));
      case 'create_tables':
        return jsonrpcSuccess(id, methodCreateTables(params));
      case 'teardown':
        return jsonrpcSuccess(id, methodTeardown(params));
      case 'metrics':
        return jsonrpcSuccess(id, methodMetrics(params));
      case 'rpc.methods':
        return jsonrpcSuccess(id, methodRpcMethods());
      default:
        return jsonrpcError(id, -32601, 'Method not found');
    }
  } catch (err) {
    return jsonrpcError(id, -32603, `Internal error: ${String(err)}`);
  }
}

function processPayload(buffer) {
  try {
    const request = JSON.parse(buffer.toString('utf8'));
    const response = dispatch(request);
    return Buffer.from(JSON.stringify(response));
  } catch (err) {
    const response = jsonrpcError(null, -32700, 'Parse error', String(err));
    return Buffer.from(JSON.stringify(response));
  }
}

function runStdio() {
  const rl = readline.createInterface({
    input: process.stdin,
    crlfDelay: Infinity,
  });

  rl.on('line', (line) => {
    const trimmed = line.trim();
    if (!trimmed) {
      return;
    }

    const response = processPayload(Buffer.from(trimmed, 'utf8'));
    process.stdout.write(response.toString('utf8'));
    process.stdout.write('\n');
  });
}

function runHttp(host, port, rpcPath) {
  const server = http.createServer((req, res) => {
    if (req.method !== 'POST' || req.url !== rpcPath) {
      res.writeHead(404);
      res.end();
      return;
    }

    const chunks = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => {
      const payload = Buffer.concat(chunks);
      const response = processPayload(payload);
      res.writeHead(200, {
        'Content-Type': 'application/json',
        'Content-Length': response.length,
      });
      res.end(response);
    });
    req.on('error', (err) => {
      const response = Buffer.from(
        JSON.stringify(
          jsonrpcError(null, -32603, 'Internal error', String(err)),
        ),
      );
      res.writeHead(500, {
        'Content-Type': 'application/json',
        'Content-Length': response.length,
      });
      res.end(response);
    });
  });

  server.listen(port, host, () => {
    process.stderr.write(
      `JSON-RPC HTTP server listening on http://${host}:${port}${rpcPath}\n`,
    );
  });
}

function parseArgs(argv) {
  const args = {
    transport: 'stdio',
    host: '127.0.0.1',
    port: 8080,
    path: '/jsonrpc',
  };

  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    const next = argv[i + 1];

    if (token === '--transport' && next) {
      args.transport = next;
      i += 1;
      continue;
    }
    if (token === '--host' && next) {
      args.host = next;
      i += 1;
      continue;
    }
    if (token === '--port' && next) {
      args.port = Number(next);
      i += 1;
      continue;
    }
    if (token === '--path' && next) {
      args.path = next;
      i += 1;
      continue;
    }
  }

  return args;
}

function main() {
  const args = parseArgs(process.argv.slice(2));

  if (args.transport === 'stdio') {
    runStdio();
    return;
  }

  if (args.transport === 'http') {
    runHttp(args.host, args.port, args.path);
    return;
  }

  process.stderr.write('Invalid --transport value. Use stdio or http.\n');
  process.exit(1);
}

main();
