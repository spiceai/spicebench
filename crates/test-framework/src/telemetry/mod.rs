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

pub mod streaming;

use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration;

use anyhow::Result;

use opentelemetry::metrics::{Meter, MeterProvider};

use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::reader::MetricReader;
use opentelemetry_sdk::metrics::PeriodicReader;
use opentelemetry_sdk::{
    Resource,
    metrics::{SdkMeterProvider, data::ResourceMetrics},
};

use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use secrecy::SecretString;
use telemetry::exporter::TelemetryExporterBuilder;
pub use telemetry::meter::METER_PROVIDER_ONCE;
use telemetry::noop::NoopMeterProvider;
use telemetry::reader::InitialReader;

const ENDPOINT_CONST: &str = "https://telemetry.spiceai.io";

pub static ENDPOINT: LazyLock<Arc<str>> = LazyLock::new(|| {
    std::env::var("SPICEAI_TELEMETRY_ENDPOINT")
        .unwrap_or_else(|_| ENDPOINT_CONST.into())
        .into()
});

/// The global meter for benchmark telemetry.
///
/// Initialized explicitly by [`Telemetry::new_with_resource()`] or
/// [`Telemetry::with_otlp_resource()`] after the provider is set.
/// Using `OnceLock` prevents the ordering issue where early access
/// with `LazyLock` would permanently lock the meter to a noop provider.
///
/// When metrics are disabled and `METER` is never initialized, all
/// metric operations fall through to noop via the `meter()` helper.
pub static METER: OnceLock<Meter> = OnceLock::new();

/// Shared noop meter used when `METER` has not been initialized.
/// This avoids allocating a new `NoopMeterProvider` on every `meter()` call.
static NOOP_METER: LazyLock<Meter> =
    LazyLock::new(|| NoopMeterProvider::new().meter("benchmarks_telemetry"));

/// Returns the initialized meter, or a shared noop meter if not yet initialized.
#[must_use]
pub fn meter() -> Meter {
    METER.get().cloned().unwrap_or_else(|| NOOP_METER.clone())
}

#[derive(Debug, Clone)]
pub struct OtlpExporterConfig {
    pub endpoint: Arc<str>,
    pub headers: Vec<(String, String)>,
    pub timeout: Duration,
}

enum TelemetryBackend {
    Arrow { api_key: Option<SecretString> },
    Otlp(OtlpExporterConfig),
}

pub struct Telemetry {
    reader: InitialReader,
    resource: Resource,
    setup: bool,
    backend: TelemetryBackend,
}

impl Telemetry {
    /// Create telemetry with empty resource.
    /// Use `set_resource()` later to set the actual resource before calling `emit()`.
    #[must_use]
    pub fn new(api_key_name: &str) -> Self {
        let resource = Resource::builder_empty().build();
        Self::new_with_resource(&resource, api_key_name)
    }

    /// Create telemetry with a resource provided upfront.
    ///
    /// Use this when the resource attributes are already available at creation time.
    /// For most cases, prefer `new()` + `set_resource()` to ensure telemetry is initialized
    /// before any metrics calls.
    #[must_use]
    pub fn new_with_resource(resource: &Resource, api_key_name: &str) -> Self {
        let reader = InitialReader::default();

        let provider = SdkMeterProvider::builder()
            .with_resource(resource.clone())
            .with_reader(reader.clone())
            .build();

        let provider: Arc<dyn MeterProvider + Send + Sync> = Arc::new(provider);
        let setup = METER_PROVIDER_ONCE.set(Arc::clone(&provider)).is_ok();
        if !setup {
            println!("Telemetry disabled");
        }

        // Initialize METER after the provider is set to avoid binding to a noop meter.
        let _ = METER.set(provider.meter("benchmarks_telemetry"));

        let api_key = std::env::var(api_key_name)
            .ok()
            .as_deref()
            .map(|key| SecretString::new(key.into()));

        Self {
            reader,
            resource: resource.clone(),
            setup,
            backend: TelemetryBackend::Arrow { api_key },
        }
    }

    #[must_use]
    pub fn with_otlp(config: OtlpExporterConfig) -> Self {
        let resource = Resource::builder_empty().build();
        Self::with_otlp_resource(config, resource)
    }

    /// Create telemetry with OTLP backend and a resource provided upfront.
    ///
    /// Use this when the resource attributes are already available at creation time.
    #[must_use]
    pub fn with_otlp_resource(config: OtlpExporterConfig, resource: Resource) -> Self {
        let reader = InitialReader::default();

        let provider = SdkMeterProvider::builder()
            .with_resource(resource.clone())
            .with_reader(reader.clone())
            .build();

        let provider: Arc<dyn MeterProvider + Send + Sync> = Arc::new(provider);
        let setup = METER_PROVIDER_ONCE.set(Arc::clone(&provider)).is_ok();
        if !setup {
            println!("Telemetry disabled");
        }

        // Initialize METER after the provider is set to avoid binding to a noop meter.
        let _ = METER.set(provider.meter("benchmarks_telemetry"));

        Self {
            reader,
            resource,
            setup,
            backend: TelemetryBackend::Otlp(config),
        }
    }

    /// Set the resource to be used when emitting metrics.
    ///
    /// Call this after collecting all the resource attributes (e.g., `spiced_version`, `commit_sha`)
    /// but before calling `emit()`.
    pub fn set_resource(&mut self, resource: Resource) {
        self.resource = resource;
    }

    pub async fn emit(&self) -> Result<()> {
        if !self.setup {
            return Ok(());
        }

        match &self.backend {
            TelemetryBackend::Arrow { api_key } => {
                if let Some(api_key) = api_key {
                    println!("Emitting to exporter at {}", *ENDPOINT);
                    let telemetry_exporter = otel_arrow::OtelArrowExporter::new(
                        TelemetryExporterBuilder::new()
                            .with_credentials(flight_client::Credentials::Bearer {
                                token: api_key.clone().into(),
                                prefix: false,
                            })
                            .with_service_name("benchmarks_telemetry".into())
                            .with_endpoint(Arc::clone(&ENDPOINT))
                            .build()
                            .await?,
                    );

                    let mut rm = ResourceMetrics::default();

                    self.reader.collect(&mut rm)?;

                    // Note: In OpenTelemetry SDK 0.31+, ResourceMetrics.resource is set by the
                    // pipeline during collection and cannot be overridden.

                    telemetry_exporter.export(&rm).await.unwrap_or_else(|err| {
                        println!("Failed to export initial telemetry: {err:?}");
                    });
                } else {
                    println!("No API key provided, telemetry is disabled");
                }
            }
            TelemetryBackend::Otlp(config) => {
                let mut rm = ResourceMetrics::default();
                // Note: Resource is set by the pipeline during collection.
                self.reader.collect(&mut rm)?;

                let exporter = MetricExporter::builder()
                    .with_tonic()
                    .with_timeout(config.timeout)
                    .with_endpoint(config.endpoint.as_ref())
                    .build()?;
                exporter
                    .export(&rm)
                    .await
                    .unwrap_or_else(|err| println!("Failed to export OTLP telemetry: {err:?}"));
            }
        }

        Ok(())
    }
}

/// A dedicated periodic metrics pipeline for SUT resource metrics.
///
/// Always exports to the Arrow backend (when `SPICEAI_BENCHMARK_METRICS_KEY` is set).
/// Additionally exports to an OTLP endpoint when configured.
/// Instruments created on the returned [`Meter`] are exported every 5 seconds.
pub struct SutMetricsPipeline {
    provider: SdkMeterProvider,
    meter: Meter,
}

impl SutMetricsPipeline {
    /// Create the pipeline.
    ///
    /// - `api_key_name`: env var name for the Arrow backend API key (e.g. `"SPICEAI_BENCHMARK_METRICS_KEY"`).
    /// - `otlp_endpoint`: optional OTLP gRPC endpoint to also export to.
    /// - `resource`: OTel resource attributes for exported metrics.
    pub async fn new(
        api_key_name: &str,
        otlp_endpoint: Option<&str>,
        resource: Resource,
    ) -> Result<Self> {
        let mut builder = SdkMeterProvider::builder().with_resource(resource);

        // Arrow periodic reader (always, when API key is present)
        let api_key = std::env::var(api_key_name).ok();
        if let Some(key) = api_key {
            let arrow_exporter = otel_arrow::OtelArrowExporter::new(
                TelemetryExporterBuilder::new()
                    .with_credentials(flight_client::Credentials::Bearer {
                        token: SecretString::new(key.into()).into(),
                        prefix: false,
                    })
                    .with_service_name("benchmarks_telemetry".into())
                    .with_endpoint(Arc::clone(&ENDPOINT))
                    .build()
                    .await?,
            );
            let reader = PeriodicReader::builder(arrow_exporter)
                .with_interval(Duration::from_secs(5))
                .build();
            builder = builder.with_reader(reader);
            println!("SUT metrics: Arrow periodic exporter enabled (endpoint: {})", *ENDPOINT);
        }

        // OTLP periodic reader (when --otlp-endpoint is configured)
        if let Some(endpoint) = otlp_endpoint {
            match MetricExporter::builder()
                .with_tonic()
                .with_timeout(Duration::from_secs(10))
                .with_endpoint(endpoint)
                .build()
            {
                Ok(otlp_exporter) => {
                    let reader = PeriodicReader::builder(otlp_exporter)
                        .with_interval(Duration::from_secs(5))
                        .build();
                    builder = builder.with_reader(reader);
                    println!("SUT metrics: OTLP periodic exporter enabled (endpoint: {endpoint})");
                }
                Err(e) => {
                    eprintln!("Failed to create OTLP exporter for SUT metrics: {e}");
                }
            }
        }

        let provider = builder.build();
        let meter = provider.meter("spicebench-sut");

        Ok(Self { provider, meter })
    }

    /// Get the meter for creating SUT instruments.
    #[must_use]
    pub fn meter(&self) -> Meter {
        self.meter.clone()
    }

    /// Flush and shut down the pipeline.
    pub fn shutdown(&self) {
        if let Err(e) = self.provider.force_flush() {
            eprintln!("Failed to flush SUT metrics pipeline: {e}");
        }
        if let Err(e) = self.provider.shutdown() {
            eprintln!("Failed to shutdown SUT metrics pipeline: {e}");
        }
    }
}
