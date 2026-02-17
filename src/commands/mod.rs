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

use std::time::Duration;

use crate::args::{CommonArgs, DatasetTestArgs, SystemAdapterExecutionMode};
use datafusion::prelude::SessionContext;
use datafusion_table_providers::flight::sql::{FlightSqlDriver, QUERY};
use datafusion_table_providers::flight::FlightTableFactory;
use std::collections::HashMap;
use std::sync::Arc;
use test_framework::{
    anyhow,
    anyhow::Context,
    app::{App, AppBuilder},
    opentelemetry_sdk::Resource,
    queries::QuerySet,
    spiced::StartRequest,
    spicepod::Spicepod,
    spicepod_utils::from_app,
    spicetest::datasets::NotStarted,
    telemetry::{OtlpExporterConfig, Telemetry},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Mutex,
};

pub(crate) mod load;

/// Create telemetry with resource attributes known upfront.
///
/// This ensures the `SdkMeterProvider` is created with the correct resource,
/// so metrics recorded after this call will have the proper resource attributes.
#[must_use]
pub(crate) fn create_telemetry_with_resource(common: &CommonArgs, resource: Resource) -> Telemetry {
    if let Some(endpoint) = &common.otlp_endpoint {
        return Telemetry::with_otlp_resource(
            OtlpExporterConfig {
                endpoint: endpoint.clone().into(),
                headers: common.otlp_header.clone(),
                timeout: Duration::from_secs(10),
            },
            resource,
        );
    }

    Telemetry::new_with_resource(&resource, "SPICEAI_BENCHMARK_METRICS_KEY")
}

/// Build a test configuration with validation data if applicable
///
/// This is a common helper for bench, throughput, and load tests that:
/// 1. Loads the query set from args
/// 2. Applies query overrides if specified
/// 3. Adds validation data for scenario queries when validation is enabled
/// 4. Adds reference schema for validation against known good tables
///
/// # Returns
/// Tuple of (`QuerySet`, `NotStarted` builder)
pub(crate) async fn build_test_with_validation(
    args: &DatasetTestArgs,
    test_builder: NotStarted,
) -> anyhow::Result<(QuerySet, NotStarted)> {
    let query_set = args.load_query_set()?;
    let query_overrides = args
        .query_overrides
        .clone()
        .map(test_framework::queries::QueryOverrides::from);
    let queries = query_set.get_queries(query_overrides, None, None).await?;

    let mut test_builder = test_builder
        .with_query_set(queries)
        .with_query_set_type(query_set.clone())
        .with_query_overrides(query_overrides);

    // Add validation data if this is a scenario query set with validation enabled
    if args.validate
        && let Some(validation_data) =
            query_set.get_validation_data(args.scenario_query_file.as_deref())?
    {
        test_builder = test_builder.with_validation_data(validation_data);
    }

    // Add reference schema if provided for validation against known good tables
    if let Some(ref_schema) = &args.reference_schema {
        test_builder = test_builder.with_reference_schema(Some(ref_schema.clone()));
    }

    Ok((query_set, test_builder))
}

pub(crate) async fn get_app_and_start_request(
    args: &CommonArgs,
) -> anyhow::Result<(App, StartRequest)> {
    // When metrics are disabled, no Telemetry is created, so METER_PROVIDER_ONCE
    // remains unset and all metric operations are no-ops.

    let mut spicepod = Spicepod::load_exact(args.spicepod_path.clone()).await?;

    let mut app_builder = AppBuilder::new(spicepod.name.clone()).with_spicepod(spicepod.clone());

    if let Some(dependencies_root) = &args.spicepod_dependencies {
        for dependency in &spicepod.dependencies {
            let dependent_spicepod = Spicepod::load(&dependencies_root.join(dependency)).await?;
            app_builder = app_builder.with_spicepod_dependency(dependent_spicepod);
        }
    }
    // After we've loaded dependencies, remove.
    spicepod.dependencies = vec![];
    let app = app_builder.build();

    let mut start_request = StartRequest::new(args.spiced_path_buf(), from_app(app.clone()))?;

    if let Some(ref data_dir) = args.data_dir {
        start_request = start_request.with_data_dir(data_dir.clone());
    }

    Ok((app, start_request))
}

pub(crate) async fn maybe_dispatch_run_to_system_adapter(
    _raw_cli_args: &[String],
    common_args: &CommonArgs,
) -> anyhow::Result<bool> {
    if common_args.system_adapter_execution_mode == SystemAdapterExecutionMode::AdapterCommand
        && !has_system_adapter_transport(common_args)
    {
        anyhow::bail!(
            "Adapter-command mode requires one system adapter transport: --system-adapter-stdio-cmd or --system-adapter-http-url"
        );
    }
    Ok(false)
}

fn has_system_adapter_transport(args: &CommonArgs) -> bool {
    args.system_adapter_stdio_cmd.is_some() || args.system_adapter_http_url.is_some()
}


const SYSTEM_ADAPTER_ASYNC_QUERY_METHOD: &str = "query.async";

#[derive(Clone)]
struct SystemAdapterAsyncQueryExecutor {
    adapter: std::sync::Arc<Mutex<SystemAdapterClient>>,
    base_params: serde_json::Map<String, serde_json::Value>,
}

#[async_trait::async_trait]
impl test_framework::execution::QueryExecutor for SystemAdapterAsyncQueryExecutor {
    async fn execute(
        &self,
        query: &test_framework::queries::Query,
    ) -> anyhow::Result<test_framework::execution::ExecutionResult> {
        let start = std::time::Instant::now();

        let mut params = self.base_params.clone();
        params.insert(
            "sql".to_string(),
            serde_json::Value::String(query.to_sql_with_inlined_params().to_string()),
        );

        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": SYSTEM_ADAPTER_ASYNC_QUERY_METHOD,
            "params": serde_json::Value::Object(params),
        });

        let response = self.adapter.lock().await.call(request).await?;
        let result = response
            .get("result")
            .context("System adapter async query response missing result payload")?;

        if let Some(success) = result.get("success").and_then(|v| v.as_bool())
            && !success
        {
            let message = result
                .get("error")
                .and_then(|v| v.as_str())
                .or_else(|| result.get("stderr").and_then(|v| v.as_str()))
                .unwrap_or("unknown system adapter query error");
            anyhow::bail!("System adapter async query failed: {message}");
        }

        let row_count = result
            .get("row_count")
            .or_else(|| result.get("stats").and_then(|s| s.get("row_count")))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        Ok(test_framework::execution::ExecutionResult {
            duration: start.elapsed(),
            row_count: row_count as usize,
            batches: None,
        })
    }

    fn name(&self) -> &'static str {
        "system-adapter-async"
    }

    fn supports_validation(&self) -> bool {
        false
    }

    fn clone_box(&self) -> Box<dyn test_framework::execution::QueryExecutor> {
        Box::new(self.clone())
    }
}

#[derive(Clone)]
struct AdbcDirectQueryExecutor {
    flight_sql_uri: String,
}

#[async_trait::async_trait]
impl test_framework::execution::QueryExecutor for AdbcDirectQueryExecutor {
    async fn execute(
        &self,
        query: &test_framework::queries::Query,
    ) -> anyhow::Result<test_framework::execution::ExecutionResult> {
        let start = std::time::Instant::now();

        let ctx = SessionContext::new();
        let flight_sql = FlightTableFactory::new(Arc::new(FlightSqlDriver::new()));
        let table = flight_sql
            .open_table(
                self.flight_sql_uri.clone(),
                HashMap::from([(
                    QUERY.into(),
                    query.to_sql_with_inlined_params().to_string(),
                )]),
            )
            .await?;

        ctx.register_table("spicebench_direct_query", Arc::new(table))?;
        let batches = ctx
            .sql("SELECT * FROM spicebench_direct_query")
            .await?
            .collect()
            .await?;
        let row_count = batches.iter().map(arrow::record_batch::RecordBatch::num_rows).sum();

        Ok(test_framework::execution::ExecutionResult {
            duration: start.elapsed(),
            row_count,
            batches: None,
        })
    }

    fn name(&self) -> &'static str {
        "adbc-direct"
    }

    fn supports_validation(&self) -> bool {
        false
    }

    fn clone_box(&self) -> Box<dyn test_framework::execution::QueryExecutor> {
        Box::new(self.clone())
    }
}

enum SystemAdapterClient {
    Stdio {
        _child: Box<Child>,
        stdin: ChildStdin,
        stdout: BufReader<ChildStdout>,
    },
    Http {
        client: reqwest::Client,
        endpoint: String,
    },
}

impl SystemAdapterClient {
    async fn connect(args: &CommonArgs) -> anyhow::Result<Option<Self>> {
        let has_stdio = args.system_adapter_stdio_cmd.is_some();
        let has_http = args.system_adapter_http_url.is_some();

        if has_stdio && has_http {
            anyhow::bail!(
                "Set only one system adapter transport: --system-adapter-stdio-cmd or --system-adapter-http-url"
            );
        }

        if !has_stdio && !has_http {
            if args.system_adapter_stdio_args.is_some()
                || !args.system_adapter_param.is_empty()
                || !args.system_adapter_env.is_empty()
            {
                anyhow::bail!(
                    "System adapter params were provided without a transport. Set either --system-adapter-stdio-cmd or --system-adapter-http-url."
                );
            }
            return Ok(None);
        }

        if has_http && !args.system_adapter_env.is_empty() {
            anyhow::bail!(
                "--system-adapter-env is only valid with --system-adapter-stdio-cmd transport."
            );
        }

        if let Some(command) = &args.system_adapter_stdio_cmd {
            let mut cmd = Command::new(command);

            if let Some(raw_args) = &args.system_adapter_stdio_args {
                cmd.args(raw_args.split_whitespace());
            }

            for (key, value) in &args.system_adapter_env {
                cmd.env(key, value);
            }

            cmd.stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::inherit());

            let mut child = cmd.spawn().map_err(|e| {
                anyhow::anyhow!("Failed to start system adapter stdio command '{command}': {e}")
            })?;

            let stdin = child
                .stdin
                .take()
                .context("System adapter stdio child missing stdin")?;
            let stdout = child
                .stdout
                .take()
                .context("System adapter stdio child missing stdout")?;

            return Ok(Some(Self::Stdio {
                _child: Box::new(child),
                stdin,
                stdout: BufReader::new(stdout),
            }));
        }

        Ok(Some(Self::Http {
            client: reqwest::Client::new(),
            endpoint: args
                .system_adapter_http_url
                .clone()
                .context("system adapter HTTP URL not provided")?,
        }))
    }

    fn transport_name(&self) -> &'static str {
        match self {
            Self::Stdio { .. } => "stdio",
            Self::Http { .. } => "http",
        }
    }

    async fn rpc_methods(&mut self) -> anyhow::Result<Vec<String>> {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "rpc.methods"
        });
        let response = self.call(request).await?;

        let methods = response
            .get("result")
            .and_then(|v| v.get("methods"))
            .and_then(|v| v.as_array())
            .context("System adapter response missing result.methods")?
            .iter()
            .filter_map(|v| v.as_str().map(ToString::to_string))
            .collect();
        Ok(methods)
    }

    async fn call(&mut self, request: serde_json::Value) -> anyhow::Result<serde_json::Value> {
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
                    anyhow::bail!("System adapter stdio process closed stdout before responding");
                }

                let response: serde_json::Value = serde_json::from_str(line.trim_end())?;
                if let Some(error) = response.get("error") {
                    anyhow::bail!("System adapter returned JSON-RPC error: {error}");
                }
                Ok(response)
            }
            Self::Http { client, endpoint } => {
                let response = client
                    .post(endpoint.as_str())
                    .json(&request)
                    .send()
                    .await
                    .with_context(|| {
                        format!("Failed to POST JSON-RPC request to system adapter at {endpoint}")
                    })?;

                let status = response.status();
                let value: serde_json::Value = response.json().await.with_context(|| {
                    format!("Failed to parse JSON-RPC response body from system adapter ({status})")
                })?;

                if let Some(error) = value.get("error") {
                    anyhow::bail!("System adapter returned JSON-RPC error: {error}");
                }
                Ok(value)
            }
        }
    }
}

/// Create the appropriate query executor based on command-line arguments
///
/// This helper function centralizes the executor creation logic to avoid duplication
/// across different test commands (bench, throughput, load, query).
pub(crate) async fn create_query_executor(
    args: &DatasetTestArgs,
    spiced_instance: &test_framework::spiced::SpicedInstance,
) -> anyhow::Result<Box<dyn test_framework::execution::QueryExecutor>> {
    match args.common.system_adapter_execution_mode {
        SystemAdapterExecutionMode::DirectQuery => {
            let flight_sql_uri = if args.common.is_external_instance() {
                args.common.spiced_path.clone()
            } else {
                let http_base = spiced_instance.http_base_url();
                if let Some(last_colon) = http_base.rfind(':') {
                    format!("{}:50051", &http_base[..last_colon])
                } else {
                    "http://127.0.0.1:50051".to_string()
                }
            };

            Ok(Box::new(AdbcDirectQueryExecutor { flight_sql_uri }))
        }
        SystemAdapterExecutionMode::AdapterCommand => {
            let mut adapter = SystemAdapterClient::connect(&args.common)
                .await?
                .context("System adapter transport was configured but could not be initialized")?;

            let methods = adapter.rpc_methods().await?;
            if !methods
                .iter()
                .any(|method| method == SYSTEM_ADAPTER_ASYNC_QUERY_METHOD)
            {
                anyhow::bail!(
                    "System adapter '{}' does not support required async query method '{}'. Available methods: {:?}",
                    args.common.system_adapter_name,
                    SYSTEM_ADAPTER_ASYNC_QUERY_METHOD,
                    methods
                );
            }

            let mut base_params = serde_json::Map::new();
            for (key, value) in &args.common.system_adapter_param {
                base_params.insert(key.clone(), serde_json::Value::String(value.clone()));
            }

            Ok(Box::new(SystemAdapterAsyncQueryExecutor {
                adapter: std::sync::Arc::new(Mutex::new(adapter)),
                base_params,
            }))
        }
    }
}

#[macro_export]
macro_rules! wait_test_and_memory {
    ($test:expr, $memory_token:expr, $memory_readings:expr) => {
        match $test.wait().await {
            Ok(test) => test,
            Err(e) => {
                observe_memory($memory_token, $memory_readings).await?;
                return Err(e);
            }
        }
    };
}

fn resolve_spiced_metrics_method(methods: &[String]) -> Option<&'static str> {
    const CANDIDATES: &[&str] = &[
        "spiced.metrics",
        "metrics.spiced",
        "metrics.scrape",
        "run.metrics",
    ];

    CANDIDATES
        .iter()
        .copied()
        .find(|candidate| methods.iter().any(|m| m == candidate))
}

fn metric_value(result: &serde_json::Value, metric_name: &str) -> Option<f64> {
    let value = result
        .get(metric_name)
        .or_else(|| result.get("metrics").and_then(|m| m.get(metric_name)));

    match value {
        Some(serde_json::Value::Number(n)) => n.as_f64(),
        Some(serde_json::Value::String(s)) => s.parse::<f64>().ok(),
        _ => None,
    }
}

/// Process and display spiced runtime metrics fetched via system adapter JSON-RPC.
pub(crate) async fn process_spiced_metrics(
    common_args: &CommonArgs,
    emit_to_telemetry: bool,
    attributes: &[test_framework::opentelemetry::KeyValue],
) {
    if !common_args.scrape_spiced_metrics {
        return;
    }

    if !has_system_adapter_transport(common_args) {
        println!(
            "Warning: --scrape-spiced-metrics requires a system adapter transport; skipping runtime metrics collection"
        );
        return;
    }

    let Ok(Some(mut adapter)) = SystemAdapterClient::connect(common_args).await else {
        println!("Warning: Failed to initialize system adapter for runtime metrics collection");
        return;
    };

    let methods = match adapter.rpc_methods().await {
        Ok(methods) => methods,
        Err(e) => {
            println!("Warning: Failed to query system adapter methods for runtime metrics: {e}");
            return;
        }
    };

    let Some(method) = resolve_spiced_metrics_method(&methods) else {
        println!(
            "Warning: System adapter '{}' via {} does not expose a supported spiced metrics method",
            common_args.system_adapter_name,
            adapter.transport_name(),
        );
        return;
    };

    let mut params = serde_json::Map::new();
    for (key, value) in &common_args.system_adapter_param {
        params.insert(key.clone(), serde_json::Value::String(value.clone()));
    }

    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": method,
        "params": serde_json::Value::Object(params),
    });

    let result = match adapter.call(request).await {
        Ok(response) => {
            let Some(result) = response.get("result") else {
                println!("Warning: System adapter metrics response missing result payload");
                return;
            };
            result.clone()
        }
        Err(e) => {
            println!("Warning: Failed to fetch spiced runtime metrics from system adapter: {e}");
            return;
        }
    };

    println!("\n{}", vec!["="; 30].join(""));
    println!("Spiced Runtime Metrics:");
    println!("{}", vec!["="; 30].join(""));

    if let Some(query_count) = metric_value(&result, "query_executions_total") {
        println!("Total Queries Executed: {query_count}");
        if emit_to_telemetry {
            crate::metrics::SPICED_QUERY_COUNT.record(query_count, attributes);
        }
    }

    if let Some(cache_hits) = metric_value(&result, "results_cache_hits_total")
        && let Some(cache_requests) = metric_value(&result, "results_cache_requests_total")
        && cache_requests > 0.0
    {
        let hit_rate = cache_hits / cache_requests;
        println!("Cache Hit Rate: {:.2}%", hit_rate * 100.0);
        if emit_to_telemetry {
            crate::metrics::SPICED_CACHE_HIT_RATE.record(hit_rate, attributes);
        }
    }

    if let Some(active_connections) = metric_value(&result, "query_active_count") {
        println!("Peak Active Connections: {active_connections}");
        if emit_to_telemetry {
            crate::metrics::SPICED_ACTIVE_CONNECTIONS.record(active_connections, attributes);
        }
    }

    println!("{}", vec!["="; 30].join(""));
}
