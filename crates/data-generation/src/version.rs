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

//! Version metadata for data generation.
//!
//! Each generation run produces a `version.json` file stored at
//! `{prefix}/{scenario}/{version}/version.json` that captures all
//! configuration and table metadata for that version.

use std::collections::HashMap;

use arrow::datatypes::SchemaRef;
use serde::{Deserialize, Serialize};

/// Converts an Arrow [`SchemaRef`] to a JSON-compatible representation
/// using Arrow's built-in IPC JSON serialization (the "Schema" portion
/// of the Arrow JSON integration format).
pub fn arrow_schema_to_json(schema: &SchemaRef) -> serde_json::Value {
    let json_schema: Vec<serde_json::Value> = schema
        .fields()
        .iter()
        .map(|field| {
            let mut obj = serde_json::Map::new();
            obj.insert(
                "name".to_string(),
                serde_json::Value::String(field.name().clone()),
            );
            obj.insert(
                "type".to_string(),
                serde_json::Value::String(format!("{:?}", field.data_type())),
            );
            obj.insert(
                "nullable".to_string(),
                serde_json::Value::Bool(field.is_nullable()),
            );
            serde_json::Value::Object(obj)
        })
        .collect();
    serde_json::Value::Array(json_schema)
}

/// Top-level metadata persisted as `version.json` in the version directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionMetadata {
    /// The version identifier.
    pub version: u64,
    /// The scenario name (e.g. "tpch").
    pub scenario: String,
    /// The scale factor used for data generation.
    pub scale_factor: f64,
    /// Number of generation steps (partitions).
    pub num_steps: u16,
    /// The dataset type (e.g. "tpch", "simple_sequence").
    pub dataset_type: String,
    /// Mutation configuration used during generation.
    pub mutations: MutationsMetadata,
    /// Per-table metadata, keyed by table name.
    pub tables: HashMap<String, TableMetadata>,
}

/// Mutation configuration stored in version metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationsMetadata {
    /// Ratio of rows that are updates (0.0–1.0).
    pub update_ratio: f64,
    /// Ratio of rows that are deletes (0.0–1.0).
    pub delete_ratio: f64,
}

/// Per-table metadata stored inside `version.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    /// The table name.
    pub name: String,
    /// The Arrow schema serialized as JSON (industry-standard field descriptions).
    pub schema: serde_json::Value,
    /// The time column name appended during rehydration.
    pub time_column: String,
    /// Primary key column names (may be empty for append-only tables).
    pub key_columns: Vec<String>,
    /// The batch IDs that were successfully written for this table.
    pub batch_ids: Vec<u64>,
}
