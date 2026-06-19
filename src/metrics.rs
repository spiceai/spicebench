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
#![allow(dead_code)]

use std::sync::LazyLock;

use test_framework::opentelemetry::metrics::{Counter, Gauge, Histogram};
use test_framework::telemetry::meter;

pub static ITERATIONS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("iterations")
        .with_description("Number of query iterations.")
        .with_unit("iterations")
        .build()
});

pub static QUERY_STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("query_status")
        .with_description("Query pass status.")
        .with_unit("status")
        .build()
});

pub static HEALTH_LATENCY: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    meter()
        .f64_histogram("health_latency_ms")
        .with_description("Latency of /health and /v1/ready probes.")
        .with_unit("ms")
        .build()
});

pub static MEDIAN_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("median_duration_ms")
        .with_description("Median duration of the query.")
        .with_unit("ms")
        .build()
});

pub static MIN_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("min_duration_ms")
        .with_description("Minimum duration of the query.")
        .with_unit("ms")
        .build()
});

pub static MAX_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("max_duration_ms")
        .with_description("Maximum duration of the query.")
        .with_unit("ms")
        .build()
});

pub static P99_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("p99_duration_ms")
        .with_description("99th percentile duration of the query.")
        .with_unit("ms")
        .build()
});

pub static TEST_DURATION: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("test_duration_ms")
        .with_description("The entire duration of the test.")
        .with_unit("ms")
        .build()
});

pub static PEAK_MEMORY_USAGE: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("peak_memory_usage_mb")
        .with_description("The maximum observed memory usage during the test.")
        .with_unit("mb")
        .build()
});

pub static MEDIAN_MEMORY_USAGE: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("median_memory_usage_mb")
        .with_description("The median observed memory usage during the test.")
        .with_unit("mb")
        .build()
});

// --- Query throughput ---

pub static QUERIES_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter()
        .u64_counter("queries_total")
        .with_description("Total number of queries executed during the benchmark run.")
        .with_unit("queries")
        .build()
});

pub static QUERIES_PER_SEC: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("queries_per_sec")
        .with_description("Query throughput in queries per second.")
        .with_unit("queries/s")
        .build()
});

// --- Compute ---
//
// `active_connections` (offered query-client concurrency) is recorded on the
// periodic SUT metrics pipeline in `commands::load`, not here, so it forms a
// time series instead of a single end-of-run point.

pub static NUM_COMPUTE_NODES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    meter()
        .u64_gauge("num_compute_nodes")
        .with_description("Number of active compute nodes / clusters backing the SUT.")
        .with_unit("nodes")
        .build()
});

// --- Efficiency ---

pub static EFFICIENCY_QUERIES_PER_CORE: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("efficiency_queries_per_core")
        .with_description("Query throughput normalized by CPU cores (queries/s per core).")
        .with_unit("queries/s/core")
        .build()
});

// --- E2E Latency ---

pub static E2E_LATENCY_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    meter()
        .f64_histogram("e2e_latency_ms")
        .with_description("End-to-end latency from event creation to the event being queryable.")
        .with_unit("ms")
        .build()
});

/// Wall-clock seconds to make the bootstrapped base dataset queryable in the SUT
/// (SUT start + initial snapshot of existing data, until row counts match).
pub static BOOTSTRAP_LOAD_SECONDS: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("bench_bootstrap_load_seconds")
        .with_description("Time to load the existing base dataset into the SUT during bootstrap (activate + initial snapshot).")
        .with_unit("s")
        .build()
});

/// Initial-load throughput during bootstrap: base rows / bootstrap load seconds.
pub static BOOTSTRAP_LOAD_ROWS_PER_SEC: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    meter()
        .f64_gauge("bench_bootstrap_load_rows_per_sec")
        .with_description(
            "Initial-load throughput during bootstrap (base rows / bootstrap load seconds).",
        )
        .build()
});
