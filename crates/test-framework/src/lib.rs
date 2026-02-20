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

#![allow(clippy::missing_errors_doc)]

pub mod execution;
pub mod flight;
pub mod git;
pub mod metrics;
pub mod queries;
pub mod snapshot;
pub mod spiced;
pub mod spicetest;
pub mod telemetry;

use std::fmt::Display;
use std::time::Duration;

use queries::QuerySet;
use spicetest::datasets::EndCondition;

pub use anyhow;
pub use arrow;
pub use opentelemetry;
pub use opentelemetry_sdk;
pub use rustls;

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum Scenario {
    #[allow(clippy::upper_case_acronyms)]
    TPCH,
}

impl Display for Scenario {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Scenario::TPCH => write!(f, "tpch"),
        }
    }
}

impl Scenario {
    /// Load the query set corresponding to this scenario.
    pub fn load_query_set(&self) -> anyhow::Result<QuerySet> {
        match self {
            Scenario::TPCH => Ok(QuerySet::Tpch),
        }
    }

    pub fn end_condition(&self) -> EndCondition {
        match self {
            Scenario::TPCH => EndCondition::Duration(Duration::from_secs(30)),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TestType {
    Throughput,
    Load,
    Benchmark,
    DataConsistency,
    Search,
    TextToSql,
    Streaming,
}

impl TestType {
    #[must_use]
    pub fn workflow(&self) -> &str {
        match self {
            TestType::Throughput => "spicebench_run_throughput.yml",
            TestType::Load => "spicebench_run_load.yml",
            TestType::Benchmark => "spicebench_run_bench.yml",
            TestType::DataConsistency => "spicebench_run_data_consistency.yml",
            TestType::Search => "spicebench_run_search.yml",
            TestType::TextToSql => "spicebench_run_texttosql.yml",
            TestType::Streaming => "spicebench_run_streaming_dynamodb.yml",
        }
    }
}

impl Display for TestType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestType::Throughput => write!(f, "throughput"),
            TestType::Load => write!(f, "load"),
            TestType::Benchmark => write!(f, "benchmark"),
            TestType::DataConsistency => write!(f, "data_consistency"),
            TestType::Search => write!(f, "search"),
            TestType::TextToSql => write!(f, "text_to_sql"),
            TestType::Streaming => write!(f, "streaming"),
        }
    }
}
