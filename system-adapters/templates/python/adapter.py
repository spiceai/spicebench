#!/usr/bin/env python3
import argparse
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

JSONRPC_VERSION = "2.0"


def jsonrpc_success(request_id: Any, result: Any) -> dict[str, Any]:
    return {
        "jsonrpc": JSONRPC_VERSION,
        "id": request_id,
        "result": result,
    }


def jsonrpc_error(request_id: Any, code: int, message: str, data: Any | None = None) -> dict[str, Any]:
    err = {
        "code": code,
        "message": message,
    }
    if data is not None:
        err["data"] = data

    return {
        "jsonrpc": JSONRPC_VERSION,
        "id": request_id,
        "error": err,
    }


def method_setup(params: dict[str, Any]) -> dict[str, Any]:
    _run_id = params.get("run_id")
    _datasets = params.get("datasets", {})

    # Stub: Provision or initialize your SUT for this run.
    # Example:
    # - create a test database / schema for _run_id
    # - configure ingestion pipelines for _datasets
    # - wait for SUT readiness checks to pass

    return {"ok": True}


def method_query_method(params: dict[str, Any]) -> dict[str, Any]:
    _run_id = params.get("run_id")

    # Stub: Resolve real connection details from your SUT control plane.
    # Example:
    # - fetch endpoint and auth token from orchestrator APIs
    # - map TLS and port settings from environment or secret store

    host = os.getenv("SUT_HOST", "127.0.0.1")
    port = int(os.getenv("SUT_PORT", "50051"))
    use_tls = os.getenv("SUT_TLS", "false").lower() == "true"

    return {
        "driver": "flightsql",
        "db_kwargs": {
            "uri": f"grpc{'s' if use_tls else ''}://{host}:{port}",
            "username": os.getenv("SUT_USERNAME", ""),
            "password": os.getenv("SUT_PASSWORD", ""),
            "tls": use_tls,
        },
    }


def method_teardown(params: dict[str, Any]) -> dict[str, Any]:
    _run_id = params.get("run_id")

    # Stub: Deprovision resources created in setup.
    # Example:
    # - delete per-run database/schema
    # - stop ingestion jobs and background workers

    return {"ok": True}


def method_metrics(params: dict[str, Any]) -> dict[str, Any]:
    _run_id = params.get("run_id")

    # Stub: Poll or scrape the live SUT and map values into this schema.
    # Example data sources:
    # - CPU / memory / disk stats from cloud provider or node exporter
    # - rows/bytes ingested from ingestion status endpoint
    # - active connections from database telemetry API

    return {
        "resource": {
            "cpu_usage_percent": 0.0,
            "memory_usage_bytes": 0,
            "disk_read_bytes": 0,
            "disk_write_bytes": 0,
            "disk_read_iops": 0,
            "disk_write_iops": 0,
        },
        "ingestion": {
            "rows_ingested": 0,
            "bytes_ingested": 0,
            "rows_per_sec": 0.0,
            "active_connections": 0,
        },
    }


def method_rpc_methods() -> dict[str, Any]:
    return {
        "methods": [
            "setup",
            "query_method",
            "teardown",
            "metrics",
            "rpc.methods",
        ]
    }


def dispatch(request: dict[str, Any]) -> dict[str, Any]:
    request_id = request.get("id", None)

    if request.get("jsonrpc") != JSONRPC_VERSION:
        return jsonrpc_error(request_id, -32600, "Invalid Request: jsonrpc must be '2.0'")

    method = request.get("method")
    if not isinstance(method, str):
        return jsonrpc_error(request_id, -32600, "Invalid Request: method must be a string")

    params = request.get("params", {})
    if not isinstance(params, dict):
        return jsonrpc_error(request_id, -32602, "Invalid params: expected object")

    try:
        if method == "setup":
            return jsonrpc_success(request_id, method_setup(params))
        if method == "query_method":
            return jsonrpc_success(request_id, method_query_method(params))
        if method == "teardown":
            return jsonrpc_success(request_id, method_teardown(params))
        if method == "metrics":
            return jsonrpc_success(request_id, method_metrics(params))
        if method == "rpc.methods":
            return jsonrpc_success(request_id, method_rpc_methods())

        return jsonrpc_error(request_id, -32601, "Method not found")
    except Exception as exc:
        return jsonrpc_error(request_id, -32603, f"Internal error: {exc}")


def process_payload(payload: bytes) -> bytes:
    try:
        request = json.loads(payload.decode("utf-8"))
    except Exception as exc:
        return json.dumps(
            jsonrpc_error(None, -32700, "Parse error", str(exc)),
            separators=(",", ":"),
        ).encode("utf-8")

    response = dispatch(request)
    return json.dumps(response, separators=(",", ":")).encode("utf-8")


def run_stdio() -> None:
    for line in sys.stdin:
        raw = line.strip()
        if not raw:
            continue
        response = process_payload(raw.encode("utf-8"))
        sys.stdout.write(response.decode("utf-8") + "\n")
        sys.stdout.flush()


class JsonRpcHttpHandler(BaseHTTPRequestHandler):
    rpc_path: str = "/jsonrpc"

    def do_POST(self) -> None:
        if self.path != self.rpc_path:
            self.send_response(404)
            self.end_headers()
            return

        try:
            content_length = int(self.headers.get("Content-Length", "0"))
            payload = self.rfile.read(content_length)
            response = process_payload(payload)

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)
        except Exception as exc:
            response = json.dumps(
                jsonrpc_error(None, -32603, "Internal error", str(exc)),
                separators=(",", ":"),
            ).encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

    def log_message(self, format: str, *_args: Any) -> None:
        _ = format
        return


def run_http(host: str, port: int, path: str) -> None:
    JsonRpcHttpHandler.rpc_path = path
    server = ThreadingHTTPServer((host, port), JsonRpcHttpHandler)
    print(f"JSON-RPC HTTP server listening on http://{host}:{port}{path}", file=sys.stderr)
    server.serve_forever()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="System adapter template (Python)")
    parser.add_argument("--transport", choices=["stdio", "http"], default="stdio")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--path", default="/jsonrpc")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.transport == "stdio":
        run_stdio()
        return

    run_http(args.host, args.port, args.path)


if __name__ == "__main__":
    main()
