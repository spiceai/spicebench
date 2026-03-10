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
//! - **Protocol types**: Request/response types for setup, teardown, and metrics
//! - **Client**: Ready-to-use client with Stdio and HTTP transports (requires `client` feature)
//! - **Server**: Easy server implementation via Handler trait (requires `server` feature)
//! - **JSON-RPC**: Standard JSON-RPC 2.0 envelope types
//!
//! # Client Example
//!
//! ```no_run
//! # #[cfg(feature = "client")]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use system_adapter_protocol::Client;
//! use std::collections::HashMap;
//! use uuid::Uuid;
//!
//! // Create an HTTP client
//! let mut client = Client::http("http://localhost:8080");
//!
//! // Setup a benchmark run
//! let run_id = Uuid::new_v4();
//! let setup_response = client
//!     .setup(run_id, HashMap::new(), HashMap::new(), None)
//!     .await?;
//!
//! println!("Driver: {:?}", setup_response.driver);
//!
//! // Teardown the run
//! let teardown_response = client.teardown(run_id).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Server Example
//!
//! ```no_run
//! # #[cfg(feature = "server")]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use system_adapter_protocol::{
//!     AdbcDriver, DatasetConfig, EtlSinkType, Handler, Server, SetupResponse, TeardownResponse,
//! };
//! use async_trait::async_trait;
//! use std::collections::HashMap;
//! use uuid::Uuid;
//!
//! struct MyHandler;
//!
//! #[async_trait]
//! impl Handler for MyHandler {
//!     async fn setup(
//!         &mut self,
//!         run_id: Uuid,
//!         metadata: HashMap<String, serde_json::Value>,
//!         datasets: HashMap<String, DatasetConfig>,
//!         etl_sink_type: Option<EtlSinkType>,
//!     ) -> Result<SetupResponse, String> {
//!         let _ = (metadata, datasets, etl_sink_type);
//!         Ok(SetupResponse {
//!             driver: AdbcDriver::Flightsql,
//!             db_kwargs: HashMap::new(),
//!             catalog_namespace: None,
//!         })
//!     }
//!
//!     async fn teardown(&mut self, run_id: Uuid) -> Result<TeardownResponse, String> {
//!         Ok(TeardownResponse { ok: true })
//!     }
//! }
//!
//! // Run the server on stdio
//! let mut server = Server::new(MyHandler);
//! server.run_stdio().await?;
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

#[cfg(feature = "server")]
pub mod server;

#[cfg(feature = "server")]
pub use server::{Handler, Server, ServerError};

/// ADBC driver types supported by the system adapter
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AdbcDriver {
    #[serde(rename = "flightsql")]
    Flightsql,
    #[serde(rename = "databricks")]
    Databricks,
    #[serde(rename = "postgresql")]
    Postgresql,
}

/// ETL sink type used by spicebench for this run.
///
/// This is provided to adapters in [`SetupRequest`] so they can optionally
/// adjust setup behavior based on how data is loaded.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EtlSinkType {
    Hive,
    Adbc,
}

impl std::fmt::Display for AdbcDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Flightsql => write!(f, "flightsql"),
            Self::Databricks => write!(f, "databricks"),
            Self::Postgresql => write!(f, "postgresql"),
        }
    }
}

/// Configuration for a single dataset to be prepared for benchmarking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetConfig {
    /// Arrow schema for the dataset
    pub schema: SchemaRef,
    /// Primary key column names for the dataset
    #[serde(default)]
    pub primary_key_columns: Vec<String>,
    /// Dataset S3 location (e.g. "s3://my-bucket/path/to/data/")
    pub location: Option<String>,
    /// Optional column name to use as the ingestion time for metrics tracking
    pub time_column: Option<String>,
    /// Optional list of columns to use for partitioning the dataset in storage
    pub partition_columns: Vec<String>,
}

/// Request to setup a benchmark run.
///
/// JSON-RPC method: `setup`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetupRequest {
    /// Unique identifier for this benchmark run
    pub run_id: Uuid,
    /// Arbitrary run metadata propagated from spicebench to adapters
    #[serde(default)]
    pub metadata: HashMap<String, serde_json::Value>,
    /// Map of dataset name to dataset definition
    pub datasets: HashMap<String, DatasetConfig>,
    /// Optional ETL sink type selected by spicebench.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etl_sink_type: Option<EtlSinkType>,
}

/// Response from setup request containing ADBC connection information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SetupResponse {
    /// ADBC driver to use for database connections
    pub driver: AdbcDriver,
    /// Driver-specific connection parameters
    pub db_kwargs: HashMap<String, serde_json::Value>,
    /// Optional catalog/namespace path where benchmark tables were created
    /// (e.g. "catalog.schema").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog_namespace: Option<String>,
    /// Optional read driver to use for reading data from the benchmark tables.
    pub read_driver: Option<(AdbcDriver, HashMap<String, serde_json::Value>)>,
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TeardownResponse {
    /// Indicates if teardown was successful
    pub ok: bool,
}

/// Request to collect current metrics from the system under test
///
/// JSON-RPC method: `metrics`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsRequest {
    /// Unique identifier for the benchmark run
    pub run_id: Uuid,
}

/// Resource utilization snapshot from the system under test
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ResourceMetrics {
    /// Cumulative CPU seconds used
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_usage_percent: Option<f64>,
    /// Resident memory usage in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_usage_bytes: Option<u64>,
    /// Cumulative disk bytes read
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_read_bytes: Option<u64>,
    /// Cumulative disk bytes written
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_write_bytes: Option<u64>,
    /// Cumulative disk read operations
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_read_iops: Option<u64>,
    /// Cumulative disk write operations
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_write_iops: Option<u64>,
    /// Number of active compute nodes / clusters backing the SUT
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_compute_nodes: Option<u64>,
}

/// Ingestion progress snapshot from the system under test
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct IngestionMetrics {
    /// Total rows ingested so far
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_ingested: Option<u64>,
    /// Total bytes ingested so far
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_ingested: Option<u64>,
    /// Current ingestion throughput in rows/sec
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_per_sec: Option<f64>,
    /// Number of active connections / clients
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_connections: Option<u64>,
}

/// Response containing current SUT metrics
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct MetricsResponse {
    /// Resource utilization metrics (CPU, memory, disk, IOPS)
    pub resource: ResourceMetrics,
    /// Ingestion progress metrics
    pub ingestion: IngestionMetrics,
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
    pub const TEARDOWN: &str = "teardown";
    pub const METRICS: &str = "metrics";
    pub const RPC_METHODS: &str = "rpc.methods";
}
