/*
Copyright 2026 The Spice.ai OSS Authors

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

use std::fmt::Display;

use test_framework::queries::QuerySet;

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
    pub fn load_query_set(&self) -> test_framework::anyhow::Result<QuerySet> {
        match self {
            Scenario::TPCH => Ok(QuerySet::Tpch),
        }
    }

    pub fn end_condition(&self) -> test_framework::spicetest::datasets::EndCondition {
        match self {
            Scenario::TPCH => test_framework::spicetest::datasets::EndCondition::Unlimited,
        }
    }
}
