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

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use crate::args::{CommonArgs, DatasetTestArgs, SystemAdapterExecutionMode};
use test_framework::{
    anyhow,
    anyhow::Context,
    app::{App, AppBuilder},
    opentelemetry_sdk::Resource,
    queries::QuerySet,
    spiced::{SpicedInstance, StartRequest},
    spicepod::Spicepod,
    spicepod_utils::from_app,
    spicetest::datasets::NotStarted,
    telemetry::{OtlpExporterConfig, Telemetry},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
};

pub(crate) mod load;
pub(crate) type RowCounts = BTreeMap<Arc<str>, usize>;

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

pub(crate) async fn run_or_connect_spiced(
    args: &CommonArgs,
) -> anyhow::Result<(App, SpicedInstance)> {
    let (app, mut instance) = if args.is_external_instance() {
        println!(
            "Connecting to external spiced instance at: {}",
            args.spiced_path
        );
        let spicepod = Spicepod::load_exact(args.spicepod_path.clone()).await?;
        let app = AppBuilder::new(spicepod.name.clone())
            .with_spicepod(spicepod)
            .build();
        let instance = SpicedInstance::external(&args.spiced_path);
        (app, instance)
    } else {
        let (app, start_request) = get_app_and_start_request(args).await?;
        let instance = SpicedInstance::start(start_request).await?;
        (app, instance)
    };
    instance
        .wait_for_ready(std::time::Duration::from_secs(args.ready_wait))
        .await?;

    Ok((app, instance))
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

    // If scrape_spiced_metrics is enabled, add --metrics flag to spiced
    if args.scrape_spiced_metrics {
        start_request = start_request
            .with_additional_args(vec!["--metrics".to_string(), "0.0.0.0:9090".to_string()]);
    }

    Ok((app, start_request))
}

pub(crate) async fn maybe_dispatch_run_to_system_adapter(
    raw_cli_args: &[String],
    common_args: &CommonArgs,
) -> anyhow::Result<bool> {
    if !has_system_adapter_transport(common_args) {
        return Ok(false);
    }

    let mut adapter = SystemAdapterClient::connect(common_args)
        .await?
        .context("System adapter transport was configured but could not be initialized")?;

    let methods = adapter.rpc_methods().await?;

    if common_args.system_adapter_execution_mode == SystemAdapterExecutionMode::DirectQuery {
        println!(
            "Connected to system adapter '{}' via {} in direct-query mode (spicebench executes query/load path directly)",
            common_args.system_adapter_name,
            adapter.transport_name(),
        );
        return Ok(false);
    }

    let Some(method) = resolve_system_adapter_method(raw_cli_args) else {
        anyhow::bail!(
            "No JSON-RPC adapter method mapping for current command invocation: {:?}",
            raw_cli_args
        );
    };

    if !methods.iter().any(|available| available == method) {
        anyhow::bail!(
            "System adapter '{}' via {} does not support required method '{method}'",
            common_args.system_adapter_name,
            adapter.transport_name(),
        );
    }

    let adapter_args = adapter_cli_args_for_run(raw_cli_args);
    let mut params = serde_json::Map::new();
    params.insert("args".to_string(), serde_json::to_value(adapter_args)?);

    for (key, value) in &common_args.system_adapter_param {
        params.insert(key.clone(), serde_json::Value::String(value.clone()));
    }

    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": method,
        "params": serde_json::Value::Object(params),
    });

    let response = adapter.call(request).await?;
    handle_adapter_execution_response(&response)?;

    Ok(true)
}

fn resolve_system_adapter_method(raw_cli_args: &[String]) -> Option<&'static str> {
    match raw_cli_args.first().map(String::as_str) {
        Some("run") => Some("run.load"),
        _ => None,
    }
}

fn has_system_adapter_transport(args: &CommonArgs) -> bool {
    args.system_adapter_stdio_cmd.is_some() || args.system_adapter_http_url.is_some()
}

fn adapter_cli_args_for_run(raw_cli_args: &[String]) -> Vec<String> {
    let mut filtered = Vec::new();
    let mut skip_next = false;

    for (index, arg) in raw_cli_args.iter().enumerate() {
        if index == 0 && arg == "run" {
            continue;
        }

        if skip_next {
            skip_next = false;
            continue;
        }

        let takes_value = [
            "--system-adapter-name",
            "--system-adapter-execution-mode",
            "--system-adapter-stdio-cmd",
            "--system-adapter-stdio-args",
            "--system-adapter-http-url",
            "--system-adapter-param",
            "--system-adapter-env",
        ];

        if takes_value.contains(&arg.as_str()) {
            skip_next = true;
            continue;
        }

        if takes_value
            .iter()
            .any(|flag| arg.starts_with(&format!("{flag}=")))
        {
            continue;
        }

        filtered.push(arg.clone());
    }

    filtered
}

fn handle_adapter_execution_response(response: &serde_json::Value) -> anyhow::Result<()> {
    let result = response
        .get("result")
        .context("System adapter response missing JSON-RPC result payload")?;

    if let Some(stdout) = result.get("stdout").and_then(|v| v.as_str())
        && !stdout.is_empty()
    {
        print!("{stdout}");
    }

    if let Some(stderr) = result.get("stderr").and_then(|v| v.as_str())
        && !stderr.is_empty()
    {
        eprint!("{stderr}");
    }

    let success = result
        .get("success")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let exit_code = result
        .get("exit_code")
        .and_then(|v| v.as_i64())
        .unwrap_or(1);

    if !success || exit_code != 0 {
        anyhow::bail!("System adapter command failed (success={success}, exit_code={exit_code})");
    }

    Ok(())
}

enum SystemAdapterClient {
    Stdio {
        child: Child,
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
                child,
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
                child: _,
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

pub(crate) async fn env_export(args: &CommonArgs) -> anyhow::Result<()> {
    let (_, mut start_request) = get_app_and_start_request(args).await?;

    start_request.prepare()?;
    let tempdir_path = start_request.get_tempdir_path();

    println!(
        "Exported spicepod environment to: {}",
        tempdir_path.to_string_lossy()
    );

    // Wait for input before exiting
    println!("Press Enter to exit...");
    std::io::stdin().read_line(&mut String::new())?;

    Ok(())
}

/// Create the appropriate query executor based on command-line arguments
///
/// This helper function centralizes the executor creation logic to avoid duplication
/// across different test commands (bench, throughput, load, query).
pub(crate) async fn create_query_executor(
    args: &DatasetTestArgs,
    spiced_instance: &test_framework::spiced::SpicedInstance,
) -> anyhow::Result<Box<dyn test_framework::execution::QueryExecutor>> {
    let executor: Box<dyn test_framework::execution::QueryExecutor> = if args.distributed {
        let http_client = spiced_instance.http_client()?;
        let base_url = spiced_instance.http_base_url().to_string();
        Box::new(test_framework::execution::DistributedExecutor::new(
            http_client,
            base_url,
        ))
    } else if args.http_clients {
        let http_client = spiced_instance.http_client()?;
        let base_url = spiced_instance.http_base_url().to_string();
        Box::new(test_framework::execution::HttpExecutor::new(
            http_client,
            base_url,
        ))
    } else {
        let spice_client = spiced_instance
            .spice_client(None, args.disable_caching)
            .await?;
        Box::new(test_framework::execution::FlightExecutor::new(
            std::sync::Arc::new(spice_client),
        ))
    };

    Ok(executor)
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

/// Process and display metrics from the spiced metrics scraper
///
/// # Arguments
/// * `scraper` - Optional metrics scraper to stop and process
/// * `emit_to_telemetry` - Whether to emit metrics to OpenTelemetry
/// * `attributes` - Optional attributes to attach to emitted metrics (e.g., test name)
///
/// # Returns
/// The collected `SpicedMetrics` if scraper was present, None otherwise
pub(crate) async fn process_spiced_metrics(
    scraper: Option<crate::spiced_metrics::MetricsScraper>,
    emit_to_telemetry: bool,
    attributes: &[test_framework::opentelemetry::KeyValue],
) -> Option<crate::spiced_metrics::SpicedMetrics> {
    let scraper = scraper?;

    match scraper.stop().await {
        Ok(metrics) => {
            println!("\n{}", vec!["="; 30].join(""));
            println!("Spiced Runtime Metrics:");
            println!("{}", vec!["="; 30].join(""));

            // Display and optionally emit key metrics
            // Note: Prometheus exporter appends _total to counter metrics
            if let Some(query_count) = metrics.get_counter_value("query_executions_total") {
                println!("Total Queries Executed: {query_count}");

                if emit_to_telemetry {
                    crate::metrics::SPICED_QUERY_COUNT.record(query_count, attributes);
                }
            }

            if let Some(cache_hits) = metrics.get_counter_value("results_cache_hits_total")
                && let Some(cache_requests) =
                    metrics.get_counter_value("results_cache_requests_total")
                && cache_requests > 0.0
            {
                let hit_rate = cache_hits / cache_requests;
                println!("Cache Hit Rate: {:.2}%", hit_rate * 100.0);

                if emit_to_telemetry {
                    crate::metrics::SPICED_CACHE_HIT_RATE.record(hit_rate, attributes);
                }
            }

            if let Some(active_conns) = metrics.get_gauge_max("query_active_count") {
                println!("Peak Active Connections: {active_conns}");

                if emit_to_telemetry {
                    crate::metrics::SPICED_ACTIVE_CONNECTIONS.record(active_conns, attributes);
                }
            }

            println!("{}", vec!["="; 30].join(""));
            Some(metrics)
        }
        Err(e) => {
            println!("Warning: Failed to collect spiced metrics: {e}");
            None
        }
    }
}
