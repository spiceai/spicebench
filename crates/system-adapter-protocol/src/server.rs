/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Server implementations for system adapter JSON-RPC protocol.

use crate::{
    error_codes, methods, DatasetConfig, JsonRpcError, JsonRpcResponse, MetricsRequest,
    MetricsResponse, QueryMethodRequest, QueryMethodResponse, SetupRequest, SetupResponse,
    TeardownRequest, TeardownResponse,
};
use async_trait::async_trait;
use std::collections::HashMap;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use uuid::Uuid;

/// Error type for server operations
#[derive(Debug)]
pub enum ServerError {
    /// I/O error
    Io(std::io::Error),
    /// JSON serialization/deserialization error
    Json(serde_json::Error),
    /// Handler returned an error
    Handler(String),
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Json(e) => write!(f, "JSON error: {e}"),
            Self::Handler(msg) => write!(f, "Handler error: {msg}"),
        }
    }
}

impl std::error::Error for ServerError {}

impl From<std::io::Error> for ServerError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for ServerError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

/// Result type for server operations
pub type Result<T> = std::result::Result<T, ServerError>;

/// Handler trait for implementing system adapter logic
///
/// Implement this trait to define how your system adapter handles
/// setup, query_method, and teardown requests.
#[async_trait]
pub trait Handler: Send + Sync {
    /// Setup a benchmark run with ETL configuration
    async fn setup(
        &mut self,
        run_id: Uuid,
        datasets: HashMap<String, DatasetConfig>,
    ) -> std::result::Result<SetupResponse, String>;

    /// Get query method/driver information for a benchmark run
    async fn query_method(
        &mut self,
        run_id: Uuid,
    ) -> std::result::Result<QueryMethodResponse, String>;

    /// Teardown a benchmark run
    async fn teardown(
        &mut self,
        run_id: Uuid,
    ) -> std::result::Result<TeardownResponse, String>;

    /// Collect current metrics from the system under test
    ///
    /// Called periodically by spicebench when `--scrape-sut-metrics` is enabled.
    /// Returns a snapshot of resource utilization and ingestion progress.
    /// Default implementation returns empty metrics.
    async fn metrics(
        &mut self,
        run_id: Uuid,
    ) -> std::result::Result<MetricsResponse, String> {
        let _ = run_id;
        Ok(MetricsResponse::default())
    }

    /// List available RPC methods
    ///
    /// Override this if you want to add custom methods beyond the standard ones.
    fn rpc_methods(&self) -> Vec<String> {
        vec![
            methods::SETUP.to_string(),
            methods::QUERY_METHOD.to_string(),
            methods::TEARDOWN.to_string(),
            methods::METRICS.to_string(),
            methods::RPC_METHODS.to_string(),
        ]
    }
}

/// System adapter server
pub struct Server<H: Handler> {
    handler: H,
}

impl<H: Handler> Server<H> {
    /// Create a new server with the given handler
    pub fn new(handler: H) -> Self {
        Self { handler }
    }

    /// Run the server on stdio (reads from stdin, writes to stdout)
    pub async fn run_stdio(&mut self) -> Result<()> {
        let stdin = tokio::io::stdin();
        let mut stdout = tokio::io::stdout();
        let mut reader = BufReader::new(stdin);
        let mut line = String::new();

        loop {
            line.clear();
            let bytes_read = reader.read_line(&mut line).await?;

            if bytes_read == 0 {
                // EOF reached
                break;
            }

            let response = self.handle_request(line.trim()).await;
            let response_json = serde_json::to_string(&response)?;
            stdout.write_all(response_json.as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }

        Ok(())
    }

    /// Handle a single JSON-RPC request
    async fn handle_request(&mut self, request_str: &str) -> serde_json::Value {
        // Parse the request
        let request: serde_json::Value = match serde_json::from_str(request_str) {
            Ok(req) => req,
            Err(e) => {
                return serde_json::to_value(JsonRpcResponse::<()>::error(
                    serde_json::Value::Null,
                    JsonRpcError::new(error_codes::PARSE_ERROR, format!("Parse error: {e}")),
                ))
                .unwrap_or(serde_json::json!({}));
            }
        };

        let id = request.get("id").cloned().unwrap_or(serde_json::Value::Null);
        let method = match request.get("method").and_then(|v| v.as_str()) {
            Some(m) => m,
            None => {
                return serde_json::to_value(JsonRpcResponse::<()>::error(
                    id,
                    JsonRpcError::new(error_codes::INVALID_REQUEST, "Missing method"),
                ))
                .unwrap_or(serde_json::json!({}));
            }
        };

        // Dispatch to appropriate handler
        let result = match method {
            methods::SETUP => self.handle_setup(&request, id.clone()).await,
            methods::QUERY_METHOD => self.handle_query_method(&request, id.clone()).await,
            methods::TEARDOWN => self.handle_teardown(&request, id.clone()).await,
            methods::METRICS => self.handle_metrics(&request, id.clone()).await,
            methods::RPC_METHODS => self.handle_rpc_methods(id.clone()).await,
            _ => serde_json::to_value(JsonRpcResponse::<()>::error(
                id,
                JsonRpcError::new(error_codes::METHOD_NOT_FOUND, "Method not found"),
            ))
            .unwrap_or(serde_json::json!({})),
        };

        result
    }

    async fn handle_setup(&mut self, request: &serde_json::Value, id: serde_json::Value) -> serde_json::Value {
        let params = match request.get("params") {
            Some(p) => p,
            None => {
                return serde_json::to_value(JsonRpcResponse::<()>::error(
                    id,
                    JsonRpcError::new(error_codes::INVALID_PARAMS, "Missing params"),
                ))
                .unwrap_or(serde_json::json!({}));
            }
        };

        let setup_request: SetupRequest = match serde_json::from_value(params.clone()) {
            Ok(req) => req,
            Err(e) => {
                return serde_json::to_value(JsonRpcResponse::<()>::error(
                    id,
                    JsonRpcError::new(error_codes::INVALID_PARAMS, format!("Invalid params: {e}")),
                ))
                .unwrap_or(serde_json::json!({}));
            }
        };

        match self.handler.setup(setup_request.run_id, setup_request.datasets).await {
            Ok(response) => serde_json::to_value(JsonRpcResponse::success(id, response))
                .unwrap_or(serde_json::json!({})),
            Err(e) => serde_json::to_value(JsonRpcResponse::<()>::error(
                id,
                JsonRpcError::new(error_codes::INTERNAL_ERROR, e),
            ))
            .unwrap_or(serde_json::json!({})),
        }
    }

    async fn handle_query_method(&mut self, request: &serde_json::Value, id: serde_json::Value) -> serde_json::Value {
        let params = match request.get("params") {
            Some(p) => p,
            None => {
                return serde_json::to_value(JsonRpcResponse::<()>::error(
                    id,
                    JsonRpcError::new(error_codes::INVALID_PARAMS, "Missing params"),
                ))
                .unwrap_or(serde_json::json!({}));
            }
        };

        let query_request: QueryMethodRequest = match serde_json::from_value(params.clone()) {
            Ok(req) => req,
            Err(e) => {
                return serde_json::to_value(JsonRpcResponse::<()>::error(
                    id,
                    JsonRpcError::new(error_codes::INVALID_PARAMS, format!("Invalid params: {e}")),
                ))
                .unwrap_or(serde_json::json!({}));
            }
        };

        match self.handler.query_method(query_request.run_id).await {
            Ok(response) => serde_json::to_value(JsonRpcResponse::success(id, response))
                .unwrap_or(serde_json::json!({})),
            Err(e) => serde_json::to_value(JsonRpcResponse::<()>::error(
                id,
                JsonRpcError::new(error_codes::INTERNAL_ERROR, e),
            ))
            .unwrap_or(serde_json::json!({})),
        }
    }

    async fn handle_teardown(&mut self, request: &serde_json::Value, id: serde_json::Value) -> serde_json::Value {
        let params = match request.get("params") {
            Some(p) => p,
            None => {
                return serde_json::to_value(JsonRpcResponse::<()>::error(
                    id,
                    JsonRpcError::new(error_codes::INVALID_PARAMS, "Missing params"),
                ))
                .unwrap_or(serde_json::json!({}));
            }
        };

        let teardown_request: TeardownRequest = match serde_json::from_value(params.clone()) {
            Ok(req) => req,
            Err(e) => {
                return serde_json::to_value(JsonRpcResponse::<()>::error(
                    id,
                    JsonRpcError::new(error_codes::INVALID_PARAMS, format!("Invalid params: {e}")),
                ))
                .unwrap_or(serde_json::json!({}));
            }
        };

        match self.handler.teardown(teardown_request.run_id).await {
            Ok(response) => serde_json::to_value(JsonRpcResponse::success(id, response))
                .unwrap_or(serde_json::json!({})),
            Err(e) => serde_json::to_value(JsonRpcResponse::<()>::error(
                id,
                JsonRpcError::new(error_codes::INTERNAL_ERROR, e),
            ))
            .unwrap_or(serde_json::json!({})),
        }
    }

    async fn handle_metrics(&mut self, request: &serde_json::Value, id: serde_json::Value) -> serde_json::Value {
        let params = match request.get("params") {
            Some(p) => p,
            None => {
                return serde_json::to_value(JsonRpcResponse::<()>::error(
                    id,
                    JsonRpcError::new(error_codes::INVALID_PARAMS, "Missing params"),
                ))
                .unwrap_or(serde_json::json!({}));
            }
        };

        let metrics_request: MetricsRequest = match serde_json::from_value(params.clone()) {
            Ok(req) => req,
            Err(e) => {
                return serde_json::to_value(JsonRpcResponse::<()>::error(
                    id,
                    JsonRpcError::new(error_codes::INVALID_PARAMS, format!("Invalid params: {e}")),
                ))
                .unwrap_or(serde_json::json!({}));
            }
        };

        match self.handler.metrics(metrics_request.run_id).await {
            Ok(response) => serde_json::to_value(JsonRpcResponse::success(id, response))
                .unwrap_or(serde_json::json!({})),
            Err(e) => serde_json::to_value(JsonRpcResponse::<()>::error(
                id,
                JsonRpcError::new(error_codes::INTERNAL_ERROR, e),
            ))
            .unwrap_or(serde_json::json!({})),
        }
    }

    async fn handle_rpc_methods(&mut self, id: serde_json::Value) -> serde_json::Value {
        let methods = self.handler.rpc_methods();
        let result = serde_json::json!({ "methods": methods });
        serde_json::to_value(JsonRpcResponse::success(id, result))
            .unwrap_or(serde_json::json!({}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestHandler;

    #[async_trait]
    impl Handler for TestHandler {
        async fn setup(
            &mut self,
            _run_id: Uuid,
            _datasets: HashMap<String, DatasetConfig>,
        ) -> std::result::Result<SetupResponse, String> {
            Ok(SetupResponse { ok: true })
        }

        async fn query_method(
            &mut self,
            _run_id: Uuid,
        ) -> std::result::Result<QueryMethodResponse, String> {
            Ok(QueryMethodResponse {
                driver: crate::AdbcDriver::Flightsql,
                db_kwargs: HashMap::new(),
            })
        }

        async fn teardown(
            &mut self,
            _run_id: Uuid,
        ) -> std::result::Result<TeardownResponse, String> {
            Ok(TeardownResponse { ok: true })
        }

        async fn metrics(
            &mut self,
            _run_id: Uuid,
        ) -> std::result::Result<MetricsResponse, String> {
            Ok(MetricsResponse::default())
        }
    }

    #[tokio::test]
    async fn test_server_setup() {
        let mut server = Server::new(TestHandler);
        let request = r#"{"jsonrpc":"2.0","id":1,"method":"setup","params":{"run_id":"00000000-0000-0000-0000-000000000000","datasets":{}}}"#;
        let response = server.handle_request(request).await;

        assert!(response.get("result").is_some());
        assert_eq!(response["result"]["ok"], true);
    }

    #[tokio::test]
    async fn test_server_rpc_methods() {
        let mut server = Server::new(TestHandler);
        let request = r#"{"jsonrpc":"2.0","id":1,"method":"rpc.methods","params":{}}"#;
        let response = server.handle_request(request).await;

        let methods = response["result"]["methods"].as_array().unwrap();
        assert!(methods.len() >= 4);
    }
}
