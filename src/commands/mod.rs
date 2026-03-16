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

use crate::{args::RunArgs, scenario::Scenario};
use system_adapter_protocol::{Client as SystemAdapterClient, ClientBuilder};
use test_framework::{
    anyhow,
    opentelemetry_sdk::Resource,
    queries::QuerySet,
    spicetest::datasets::NotStarted,
    telemetry::{OtlpExporterConfig, Telemetry},
};

pub(crate) mod adbc_executor;
pub(crate) mod checkpoint;
pub(crate) mod etl_cmd;
pub(crate) mod generate;
pub(crate) mod load;
pub(crate) mod run;

/// Create telemetry with resource attributes known upfront.
///
/// This ensures the `SdkMeterProvider` is created with the correct resource,
/// so metrics recorded after this call will have the proper resource attributes.
#[must_use]
pub(crate) fn create_telemetry_with_resource(common: &RunArgs, resource: Resource) -> Telemetry {
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

pub(crate) fn rewrite_queries_with_catalog_namespace(
    queries: Vec<test_framework::queries::Query>,
    query_catalog_namespace: Option<&str>,
) -> anyhow::Result<Vec<test_framework::queries::Query>> {
    if let Some(catalog_namespace) = query_catalog_namespace
        && !catalog_namespace.trim().is_empty()
    {
        return queries
            .into_iter()
            .map(|query| query.rewrite_with_reference_schema(catalog_namespace))
            .collect::<anyhow::Result<Vec<_>>>();
    }

    Ok(queries)
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
    scenario: &Scenario,
    test_builder: NotStarted,
    query_catalog_namespace: Option<&str>,
) -> anyhow::Result<(QuerySet, NotStarted)> {
    let query_set = scenario.load_query_set()?;
    let queries = rewrite_queries_with_catalog_namespace(
        query_set.get_queries(None, None, None).await?,
        query_catalog_namespace,
    )?;

    let test_builder = test_builder
        .with_query_set(queries)
        .with_query_set_type(query_set.clone());

    Ok((query_set, test_builder))
}

/// Connect to a system adapter based on command-line arguments
///
/// All validation is handled by clap:
/// - `conflicts_with` ensures stdio and http aren't both set
/// - `requires` ensures params/args/env need a transport
/// - `group` allows either stdio or http transport
pub async fn connect_system_adapter(args: &RunArgs) -> anyhow::Result<SystemAdapterClient> {
    if let Some(command) = &args.system_adapter_stdio_cmd {
        let args_vec = args
            .system_adapter_stdio_args
            .as_ref()
            .map(|s| s.split_whitespace().map(String::from).collect())
            .unwrap_or_default();

        return ClientBuilder::stdio(command)
            .with_args(args_vec)
            .with_env(args.system_adapter_env.clone().into_iter().collect())
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to create stdio client: {e}"));
    }

    if let Some(endpoint) = &args.system_adapter_http_url {
        return ClientBuilder::http(endpoint)
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to create HTTP client: {e}"));
    }

    Err(anyhow::anyhow!("No system adapter transport configured"))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_queries_with_catalog_namespace() {
        let queries = vec![test_framework::queries::Query::new(
            "q1".into(),
            "SELECT * FROM customer".into(),
            false,
        )];

        let rewritten = rewrite_queries_with_catalog_namespace(queries, Some("catalog.schema"))
            .expect("rewrite should succeed");

        assert_eq!(rewritten.len(), 1);
        assert_eq!(
            rewritten[0].sql.as_ref(),
            "SELECT * FROM catalog.schema.customer"
        );
    }
}
