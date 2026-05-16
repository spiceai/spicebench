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

//! Client implementations for system adapter JSON-RPC communication.

use crate::{JsonRpcError, JsonRpcRequest, JsonRpcResponse, methods};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashMap;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
};

/// Result type for client operations
pub type Result<T> = std::result::Result<T, ClientError>;

/// Errors that can occur during client operations
#[derive(Debug)]
pub enum ClientError {
    /// JSON-RPC error returned by the server
    JsonRpc(JsonRpcError),
    /// I/O error during communication
    Io(std::io::Error),
    /// JSON serialization/deserialization error
    Json(serde_json::Error),
    /// HTTP transport error
    #[cfg(feature = "client")]
    Http(reqwest::Error),
    /// Invalid response format
    InvalidResponse(String),
    /// Transport-specific error
    Transport(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::JsonRpc(e) => write!(f, "JSON-RPC error: {}", e.message),
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Json(e) => write!(f, "JSON error: {e}"),
            #[cfg(feature = "client")]
            Self::Http(e) => write!(f, "HTTP error: {e}"),
            Self::InvalidResponse(msg) => write!(f, "Invalid response: {msg}"),
            Self::Transport(msg) => write!(f, "Transport error: {msg}"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<std::io::Error> for ClientError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for ClientError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

#[cfg(feature = "client")]
impl From<reqwest::Error> for ClientError {
    fn from(e: reqwest::Error) -> Self {
        Self::Http(e)
    }
}

/// System adapter client for JSON-RPC communication
pub enum Client {
    /// Stdio transport - communicate via stdin/stdout with a child process
    Stdio {
        _child: Box<Child>,
        stdin: ChildStdin,
        stdout: BufReader<ChildStdout>,
    },
    /// HTTP transport - communicate via HTTP POST requests
    #[cfg(feature = "client")]
    Http {
        client: reqwest::Client,
        endpoint: String,
    },
}

impl Client {
    /// Create a client using stdio transport by spawning a command
    pub fn stdio(
        command: impl AsRef<str>,
        args: Vec<String>,
        env: HashMap<String, String>,
    ) -> Result<Self> {
        let command_str = command.as_ref();
        let mut cmd = Command::new(command_str);

        cmd.args(args);
        for (key, value) in env {
            cmd.env(key, value);
        }

        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit());

        // Place the child in its own process group so it doesn't receive
        // SIGINT when the user presses ctrl+c, allowing orderly teardown.
        #[cfg(unix)]
        cmd.process_group(0);

        let mut child = cmd.spawn().map_err(|e| {
            ClientError::Transport(format!(
                "Failed to start stdio command '{command_str}': {e}"
            ))
        })?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| ClientError::Transport("Stdio child missing stdin".to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ClientError::Transport("Stdio child missing stdout".to_string()))?;

        Ok(Self::Stdio {
            _child: Box::new(child),
            stdin,
            stdout: BufReader::new(stdout),
        })
    }

    /// Create a client using HTTP transport
    #[cfg(feature = "client")]
    pub fn http(endpoint: impl Into<String>) -> Self {
        Self::Http {
            client: reqwest::Client::new(),
            endpoint: endpoint.into(),
        }
    }

    /// Get the transport name
    pub fn transport_name(&self) -> &'static str {
        match self {
            Self::Stdio { .. } => "stdio",
            #[cfg(feature = "client")]
            Self::Http { .. } => "http",
        }
    }

    /// Query available RPC methods from the server
    pub async fn rpc_methods(&mut self) -> Result<Vec<String>> {
        let request = JsonRpcRequest::new(1, methods::RPC_METHODS, serde_json::json!({}));
        let response: JsonRpcResponse<serde_json::Value> = self.call_typed(request).await?;

        let methods = response
            .result
            .as_ref()
            .and_then(|v| v.get("methods"))
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                ClientError::InvalidResponse("Response missing result.methods array".to_string())
            })?
            .iter()
            .filter_map(|v| v.as_str().map(ToString::to_string))
            .collect();

        Ok(methods)
    }

    /// Setup a benchmark run
    pub async fn setup(
        &mut self,
        run_id: uuid::Uuid,
        metadata: std::collections::HashMap<String, serde_json::Value>,
        datasets: std::collections::HashMap<String, crate::DatasetConfig>,
        etl_sink_type: Option<crate::EtlSinkType>,
        seed_data: std::collections::HashMap<String, String>,
    ) -> Result<crate::SetupResponse> {
        let request = crate::SetupRequest {
            run_id,
            metadata,
            datasets,
            etl_sink_type,
            seed_data,
        };
        let rpc_request = JsonRpcRequest::new(1, crate::methods::SETUP, request);
        let response = self.call_typed(rpc_request).await?;
        response
            .result
            .ok_or_else(|| ClientError::InvalidResponse("Missing result".to_string()))
    }

    /// Teardown a benchmark run
    pub async fn teardown(&mut self, run_id: uuid::Uuid) -> Result<crate::TeardownResponse> {
        let request = crate::TeardownRequest { run_id };
        let rpc_request = JsonRpcRequest::new(1, crate::methods::TEARDOWN, request);
        let response = self.call_typed(rpc_request).await?;
        response
            .result
            .ok_or_else(|| ClientError::InvalidResponse("Missing result".to_string()))
    }

    /// Collect current metrics from the system under test
    pub async fn metrics(
        &mut self,
        run_id: uuid::Uuid,
        final_scrape: bool,
    ) -> Result<crate::MetricsResponse> {
        let request = crate::MetricsRequest {
            run_id,
            final_scrape,
        };
        let rpc_request = JsonRpcRequest::new(1, crate::methods::METRICS, request);
        let response = self.call_typed(rpc_request).await?;
        response
            .result
            .ok_or_else(|| ClientError::InvalidResponse("Missing result".to_string()))
    }

    /// Create a staging table for MERGE-based updates.
    ///
    /// If the remote adapter does not support this method (returns
    /// `METHOD_NOT_FOUND`), the call is treated as a successful no-op so that
    /// newer spicebench versions work against older adapters.
    pub async fn create_staging_table(
        &mut self,
        run_id: uuid::Uuid,
        source_dataset: &str,
        staging_table_name: &str,
    ) -> Result<crate::CreateStagingTableResponse> {
        let request = crate::CreateStagingTableRequest {
            run_id,
            source_dataset: source_dataset.to_string(),
            staging_table_name: staging_table_name.to_string(),
        };
        let rpc_request = JsonRpcRequest::new(1, crate::methods::CREATE_STAGING_TABLE, request);
        match self.call_typed(rpc_request).await {
            Ok(response) => response
                .result
                .ok_or_else(|| ClientError::InvalidResponse("Missing result".to_string())),
            Err(ClientError::JsonRpc(ref e)) if e.code == crate::error_codes::METHOD_NOT_FOUND => {
                tracing::warn!(
                    source_dataset,
                    staging_table_name,
                    "System adapter does not support create_staging_table; \
                     falling back to implicit table creation via bulk ingest"
                );
                Ok(crate::CreateStagingTableResponse { ok: true })
            }
            Err(e) => Err(e),
        }
    }

    /// Make a typed JSON-RPC call with request and response types
    async fn call_typed<Req: Serialize, Resp: DeserializeOwned>(
        &mut self,
        request: JsonRpcRequest<Req>,
    ) -> Result<JsonRpcResponse<Resp>> {
        let request_value = serde_json::to_value(request)?;
        let response_value = self.call_raw(request_value).await?;
        let response: JsonRpcResponse<Resp> = serde_json::from_value(response_value)?;
        Ok(response)
    }

    /// Make a raw JSON-RPC call with serde_json::Value
    async fn call_raw(&mut self, request: serde_json::Value) -> Result<serde_json::Value> {
        match self {
            Self::Stdio {
                _child: _,
                stdin,
                stdout,
            } => {
                let payload = serde_json::to_string(&request)?;
                stdin.write_all(payload.as_bytes()).await?;
                stdin.write_all(b"\n").await?;
                stdin.flush().await?;

                let mut line = String::new();
                let read = stdout.read_line(&mut line).await?;
                if read == 0 {
                    return Err(ClientError::Transport(
                        "Stdio process closed stdout before responding".to_string(),
                    ));
                }

                let response: serde_json::Value = serde_json::from_str(line.trim_end())?;
                if let Some(error) = response.get("error") {
                    let error: JsonRpcError = serde_json::from_value(error.clone())?;
                    return Err(ClientError::JsonRpc(error));
                }
                Ok(response)
            }
            #[cfg(feature = "client")]
            Self::Http { client, endpoint } => {
                let response = client
                    .post(endpoint.as_str())
                    .json(&request)
                    .send()
                    .await
                    .map_err(|e| {
                        ClientError::Transport(format!("Failed to POST to {endpoint}: {e}"))
                    })?;

                let status = response.status();
                let value: serde_json::Value = response.json().await.map_err(|e| {
                    ClientError::Transport(format!(
                        "Failed to parse response body (status {status}): {e}"
                    ))
                })?;

                if let Some(error) = value.get("error") {
                    let error: JsonRpcError = serde_json::from_value(error.clone())?;
                    return Err(ClientError::JsonRpc(error));
                }
                Ok(value)
            }
        }
    }
}

/// Builder for creating a `Client` with various configuration options
pub struct ClientBuilder {
    transport: TransportConfig,
}

enum TransportConfig {
    Stdio {
        command: String,
        args: Vec<String>,
        env: HashMap<String, String>,
    },
    #[cfg(feature = "client")]
    Http { endpoint: String },
}

impl ClientBuilder {
    /// Create a builder for stdio transport
    pub fn stdio(command: impl Into<String>) -> Self {
        Self {
            transport: TransportConfig::Stdio {
                command: command.into(),
                args: Vec::new(),
                env: HashMap::new(),
            },
        }
    }

    /// Create a builder for HTTP transport
    #[cfg(feature = "client")]
    pub fn http(endpoint: impl Into<String>) -> Self {
        Self {
            transport: TransportConfig::Http {
                endpoint: endpoint.into(),
            },
        }
    }

    /// Add command-line arguments (stdio only)
    pub fn with_args(mut self, args: Vec<String>) -> Self {
        if let TransportConfig::Stdio {
            args: ref mut a, ..
        } = self.transport
        {
            *a = args;
        }
        self
    }

    /// Add environment variables (stdio only)
    pub fn with_env(mut self, env: HashMap<String, String>) -> Self {
        if let TransportConfig::Stdio { env: ref mut e, .. } = self.transport {
            *e = env;
        }
        self
    }

    /// Build the client
    pub fn build(self) -> Result<Client> {
        match self.transport {
            TransportConfig::Stdio { command, args, env } => Client::stdio(command, args, env),
            #[cfg(feature = "client")]
            TransportConfig::Http { endpoint } => Ok(Client::http(endpoint)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_error_display() {
        let error = ClientError::Transport("test error".to_string());
        assert_eq!(format!("{error}"), "Transport error: test error");
    }

    #[test]
    fn test_builder_stdio() {
        let builder = ClientBuilder::stdio("python");
        assert!(matches!(builder.transport, TransportConfig::Stdio { .. }));
    }

    #[cfg(feature = "client")]
    #[test]
    fn test_builder_http() {
        let builder = ClientBuilder::http("http://localhost:8080");
        assert!(matches!(builder.transport, TransportConfig::Http { .. }));
    }
}
