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

//! JSON-RPC protocol definitions for system adapter communication.
//!
//! This crate defines the request/response types for the system adapter
//! JSON-RPC protocol, which allows spicebench to communicate with external
//! benchmark execution environments.
//!
//! # Features
//!
//! - **Protocol types**: Request/response types for setup, query_method, and teardown
//! - **Client**: Ready-to-use client with Stdio and HTTP transports (requires `client` feature)
//! - **JSON-RPC**: Standard JSON-RPC 2.0 envelope types
//!
//! # Example
//!
//! ```no_run
//! # #[cfg(feature = "client")]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use system_adapter_protocol::{Client, JsonRpcRequest, SetupRequest, SetupResponse};
//! use std::collections::HashMap;
//! use uuid::Uuid;
//!
//! // Create an HTTP client
//! let mut client = Client::http("http://localhost:8080");
//!
//! // Make a setup request
//! let request = JsonRpcRequest::new(
//!     1,
//!     "setup",
//!     SetupRequest {
//!         run_id: Uuid::new_v4(),
//!         datasets: HashMap::new(),
//!     }
//! );
//!
//! let response: system_adapter_protocol::JsonRpcResponse<SetupResponse> =
//!     client.call_typed(request).await?;
//! # Ok(())
//! # }
//! ```

use arrow_schema::SchemaRef;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[cfg(feature = "client")]
pub mod client;

#[cfg(feature = "client")]
pub use client::{Client, ClientBuilder, ClientError};

/// ETL type for data ingestion configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EtlType {
    S3,
}

/// ADBC driver types supported by the system adapter
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AdbcDriver {
    #[serde(rename = "flightsql")]
    Flightsql,
    #[serde(rename = "databricks")]
    Databricks,
}

/// Configuration for a single dataset's ETL source
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetConfig {
    /// Type of ETL to configure
    pub etl_type: EtlType,
    /// Arrow schema for the dataset
    pub schema: SchemaRef,
    /// ETL-specific configuration parameters
    pub params: HashMap<String, serde_json::Value>,
}

/// Request to setup a benchmark run with ETL configuration
///
/// JSON-RPC method: `setup`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetupRequest {
    /// Unique identifier for this benchmark run
    pub run_id: Uuid,
    /// Map of dataset name to its ETL configuration
    pub datasets: HashMap<String, DatasetConfig>,
}

/// Response from setup request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetupResponse {
    /// Indicates if setup was successful
    pub ok: bool,
}

/// Request to get query method/driver information
///
/// JSON-RPC method: `query_method`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMethodRequest {
    /// Unique identifier for the benchmark run
    pub run_id: Uuid,
}

/// Response containing database connection information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMethodResponse {
    /// ADBC driver to use for database connections
    pub driver: AdbcDriver,
    /// Driver-specific connection parameters
    pub db_kwargs: HashMap<String, serde_json::Value>,
}

/// Request to teardown a benchmark run
///
/// JSON-RPC method: `teardown`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeardownRequest {
    /// Unique identifier for the benchmark run to clean up
    pub run_id: Uuid,
}

/// Response from teardown request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeardownResponse {
    /// Indicates if teardown was successful
    pub ok: bool,
}

/// Standard JSON-RPC 2.0 request envelope
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest<T> {
    pub jsonrpc: String,
    pub id: serde_json::Value,
    pub method: String,
    pub params: T,
}

impl<T> JsonRpcRequest<T> {
    /// Create a new JSON-RPC 2.0 request
    pub fn new(id: impl Into<serde_json::Value>, method: impl Into<String>, params: T) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id: id.into(),
            method: method.into(),
            params,
        }
    }
}

/// Standard JSON-RPC 2.0 response envelope
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse<T> {
    pub jsonrpc: String,
    pub id: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

impl<T> JsonRpcResponse<T> {
    /// Create a successful response
    pub fn success(id: impl Into<serde_json::Value>, result: T) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id: id.into(),
            result: Some(result),
            error: None,
        }
    }

    /// Create an error response
    pub fn error(id: impl Into<serde_json::Value>, error: JsonRpcError) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id: id.into(),
            result: None,
            error: Some(error),
        }
    }
}

/// JSON-RPC 2.0 error object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl JsonRpcError {
    /// Create a new error
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    /// Create an error with additional data
    pub fn with_data(
        code: i32,
        message: impl Into<String>,
        data: impl Into<serde_json::Value>,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            data: Some(data.into()),
        }
    }
}

/// Standard JSON-RPC error codes
pub mod error_codes {
    pub const PARSE_ERROR: i32 = -32700;
    pub const INVALID_REQUEST: i32 = -32600;
    pub const METHOD_NOT_FOUND: i32 = -32601;
    pub const INVALID_PARAMS: i32 = -32602;
    pub const INTERNAL_ERROR: i32 = -32603;
}

/// Method names for the system adapter protocol
pub mod methods {
    pub const SETUP: &str = "setup";
    pub const QUERY_METHOD: &str = "query_method";
    pub const TEARDOWN: &str = "teardown";
    pub const RPC_METHODS: &str = "rpc.methods";
}
