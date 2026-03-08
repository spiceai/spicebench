use std::io::{self, BufRead, Write};

use anyhow::{Context, Result};
use clap::Parser;
use serde_json::{json, Value};
use tiny_http::{Method, Response, Server, StatusCode};

const JSONRPC_VERSION: &str = "2.0";

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "stdio", value_parser = ["stdio", "http"])]
    transport: String,

    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value_t = 8080)]
    port: u16,

    #[arg(long, default_value = "/jsonrpc")]
    path: String,
}

fn jsonrpc_success(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": JSONRPC_VERSION,
        "id": id,
        "result": result,
    })
}

fn jsonrpc_error(id: Value, code: i32, message: &str, data: Option<Value>) -> Value {
    let mut error = json!({
        "code": code,
        "message": message,
    });

    if let Some(data) = data {
        error["data"] = data;
    }

    json!({
        "jsonrpc": JSONRPC_VERSION,
        "id": id,
        "error": error,
    })
}

fn method_setup(_params: &Value) -> Value {
    // Stub: Provision or initialize your SUT for this run and return
    // query driver details SpiceBench should use.
    // params contains: run_id, metadata, datasets, etl_sink_type
    // Example:
    // - create run-scoped database/schema
    // - create/register destination tables from params.datasets
    // - wait for service readiness checks
    // - resolve connection details from your control plane
    let host = std::env::var("SUT_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("SUT_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(50051);
    let tls = std::env::var("SUT_TLS")
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    json!({
        "driver": "flightsql",
        "db_kwargs": {
            "uri": format!("grpc{}://{}:{}", if tls { "s" } else { "" }, host, port),
            "username": std::env::var("SUT_USERNAME").unwrap_or_default(),
            "password": std::env::var("SUT_PASSWORD").unwrap_or_default(),
            "tls": tls,
        },
    })
}

fn method_teardown(_params: &Value) -> Value {
    // Stub: Deprovision resources created in setup.
    // Example:
    // - drop run-scoped database/schema
    // - stop ingestion workers/jobs
    json!({"ok": true})
}

fn method_create_tables(_params: &Value) -> Value {
    // Stub: Create destination tables for all datasets in this run.
    // params contains: run_id, datasets
    // Example:
    // - iterate dataset names from params.datasets
    // - issue CREATE TABLE statements with matching schema
    json!({"ok": true})
}

fn method_metrics(_params: &Value) -> Value {
    // Stub: Poll live SUT telemetry and map values into this metrics response.
    // Example sources:
    // - CPU/memory/disk stats from infrastructure monitoring APIs
    // - rows/bytes ingested from ingestion status endpoint
    // - active connections from DB/service diagnostics endpoint
    json!({
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
    })
}

fn method_rpc_methods() -> Value {
    json!({
        "methods": ["setup", "create_tables", "teardown", "metrics", "rpc.methods"]
    })
}

fn dispatch(request: &Value) -> Value {
    let id = request.get("id").cloned().unwrap_or(Value::Null);

    if request
        .get("jsonrpc")
        .and_then(Value::as_str)
        .unwrap_or_default()
        != JSONRPC_VERSION
    {
        return jsonrpc_error(
            id,
            -32600,
            "Invalid Request: jsonrpc must be '2.0'",
            None,
        );
    }

    let method = match request.get("method").and_then(Value::as_str) {
        Some(value) => value,
        None => {
            return jsonrpc_error(id, -32600, "Invalid Request: method must be a string", None);
        }
    };

    let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
    if !params.is_object() {
        return jsonrpc_error(id, -32602, "Invalid params: expected object", None);
    }

    let result = match method {
        "setup" => Ok(method_setup(&params)),
        "create_tables" => Ok(method_create_tables(&params)),
        "teardown" => Ok(method_teardown(&params)),
        "metrics" => Ok(method_metrics(&params)),
        "rpc.methods" => Ok(method_rpc_methods()),
        _ => Err(jsonrpc_error(id.clone(), -32601, "Method not found", None)),
    };

    match result {
        Ok(result) => jsonrpc_success(id, result),
        Err(error) => error,
    }
}

fn process_payload(payload: &[u8]) -> Value {
    match serde_json::from_slice::<Value>(payload) {
        Ok(request) => dispatch(&request),
        Err(error) => jsonrpc_error(
            Value::Null,
            -32700,
            "Parse error",
            Some(json!(error.to_string())),
        ),
    }
}

fn run_stdio() -> Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    for line in stdin.lock().lines() {
        let line = line.context("failed to read stdin line")?;
        if line.trim().is_empty() {
            continue;
        }

        let response = process_payload(line.trim().as_bytes());
        serde_json::to_writer(&mut stdout, &response).context("failed to write JSON-RPC response")?;
        writeln!(&mut stdout).context("failed to write newline")?;
        stdout.flush().context("failed to flush stdout")?;
    }

    Ok(())
}

fn run_http(host: &str, port: u16, path: &str) -> Result<()> {
    let address = format!("{}:{}", host, port);
    let server = Server::http(&address)
        .map_err(|error| anyhow::anyhow!("failed to bind {address}: {error}"))?;
    eprintln!("JSON-RPC HTTP server listening on http://{}{}", address, path);

    for mut request in server.incoming_requests() {
        if request.method() != &Method::Post || request.url() != path {
            let _ = request.respond(Response::empty(StatusCode(404)));
            continue;
        }

        let mut body = Vec::new();
        if let Err(error) = request.as_reader().read_to_end(&mut body) {
            let response = jsonrpc_error(
                Value::Null,
                -32603,
                "Internal error",
                Some(json!(error.to_string())),
            );
            let payload = serde_json::to_string(&response)?;
            let _ = request.respond(Response::from_string(payload).with_status_code(StatusCode(500)));
            continue;
        }

        let response = process_payload(&body);
        let payload = serde_json::to_string(&response)?;
        let _ = request.respond(Response::from_string(payload));
    }

    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    match args.transport.as_str() {
        "stdio" => run_stdio(),
        "http" => run_http(&args.host, args.port, &args.path),
        _ => unreachable!(),
    }
}
