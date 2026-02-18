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

use crate::args::{CommonArgs, DatasetTestArgs};
use system_adapter_protocol::{Client as SystemAdapterClient, ClientBuilder, JsonRpcRequest};
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
    common_args: &CommonArgs,
) -> anyhow::Result<Option<SystemAdapterClient>> {
    if !has_system_adapter_transport(common_args) {
        return Ok(None);
    }

    connect_system_adapter(common_args)
        .await
        .context("System adapter transport was configured but could not be initialized")
}

fn has_system_adapter_transport(args: &CommonArgs) -> bool {
    args.system_adapter_stdio_cmd.is_some() || args.system_adapter_http_url.is_some()
}

/// Connect to a system adapter based on command-line arguments
async fn connect_system_adapter(args: &CommonArgs) -> anyhow::Result<Option<SystemAdapterClient>> {
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
        let args_vec = args
            .system_adapter_stdio_args
            .as_ref()
            .map(|s| s.split_whitespace().map(String::from).collect())
            .unwrap_or_default();

        let client = ClientBuilder::stdio(command)
            .with_args(args_vec)
            .with_env(args.system_adapter_env.clone().into_iter().collect())
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to create stdio client: {e}"))?;

        return Ok(Some(client));
    }

    if let Some(endpoint) = &args.system_adapter_http_url {
        let client = ClientBuilder::http(endpoint)
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to create HTTP client: {e}"))?;
        return Ok(Some(client));
    }

    Ok(None)
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

fn resolve_sut_metrics_method(methods: &[String]) -> Option<&'static str> {
    const CANDIDATES: &[&str] = &[
        "sut.metrics",
        "metrics.sut",
        "system.metrics",
        "metrics.system",
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

/// Process and display SUT metrics fetched via system adapter JSON-RPC.
pub(crate) async fn process_sut_metrics(
    common_args: &CommonArgs,
    emit_to_telemetry: bool,
    attributes: &[test_framework::opentelemetry::KeyValue],
) {
    if !common_args.scrape_sut_metrics {
        return;
    }

    if !has_system_adapter_transport(common_args) {
        println!(
            "Warning: --scrape-sut-metrics requires a system adapter transport; skipping SUT metrics collection"
        );
        return;
    }

    let Ok(Some(mut adapter)) = connect_system_adapter(common_args).await else {
        println!("Warning: Failed to initialize system adapter for SUT metrics collection");
        return;
    };

    let methods = match adapter.rpc_methods().await {
        Ok(methods) => methods,
        Err(e) => {
            println!("Warning: Failed to query system adapter methods for SUT metrics: {e}");
            return;
        }
    };

    let Some(method) = resolve_sut_metrics_method(&methods) else {
        println!(
            "Warning: System adapter '{}' via {} does not expose a supported SUT metrics method",
            common_args.system_adapter_name,
            adapter.transport_name(),
        );
        return;
    };

    let mut params = serde_json::Map::new();
    for (key, value) in &common_args.system_adapter_param {
        params.insert(key.clone(), serde_json::Value::String(value.clone()));
    }

    let request = JsonRpcRequest::new(3, method, serde_json::Value::Object(params));

    let result = match adapter.call_typed::<_, serde_json::Value>(request).await {
        Ok(response) => {
            let Some(result) = response.result else {
                println!("Warning: System adapter metrics response missing result payload");
                return;
            };
            result
        }
        Err(e) => {
            println!("Warning: Failed to fetch SUT metrics from system adapter: {e}");
            return;
        }
    };

    println!("\n{}", vec!["="; 30].join(""));
    println!("SUT Metrics:");
    println!("{}", vec!["="; 30].join(""));

    if let Some(query_count) = metric_value(&result, "query_executions_total") {
        println!("Total Queries Executed: {query_count}");
        if emit_to_telemetry {
            crate::metrics::SUT_QUERY_COUNT.record(query_count, attributes);
        }
    }

    if let Some(cache_hits) = metric_value(&result, "results_cache_hits_total")
        && let Some(cache_requests) = metric_value(&result, "results_cache_requests_total")
        && cache_requests > 0.0
    {
        let hit_rate = cache_hits / cache_requests;
        println!("Cache Hit Rate: {:.2}%", hit_rate * 100.0);
        if emit_to_telemetry {
            crate::metrics::SUT_CACHE_HIT_RATE.record(hit_rate, attributes);
        }
    }

    if let Some(active_connections) = metric_value(&result, "query_active_count") {
        println!("Peak Active Connections: {active_connections}");
        if emit_to_telemetry {
            crate::metrics::SUT_ACTIVE_CONNECTIONS.record(active_connections, attributes);
        }
    }

    println!("{}", vec!["="; 30].join(""));
}
