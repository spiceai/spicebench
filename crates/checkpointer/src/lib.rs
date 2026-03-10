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

//! Checkpoint object store — upload / download checkpoint artefacts to S3.
//!
//! ## Layout
//!
//! Checkpoints are stored under the version directory:
//!
//! ```text
//! s3://{bucket}/{prefix}/{scenario}/{version}/checkpoints/{checkpoint_idx}/{query_idx}.parquet
//! s3://{bucket}/{prefix}/{scenario}/{version}/checkpoints.json          ← manifest
//! ```
//!
//! The `prefix` passed to [`CheckpointStore`] is the fully-qualified version
//! prefix (`{prefix}/{scenario}/{version}`), so checkpoint paths are relative
//! to that.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use serde::{Deserialize, Serialize};

/// Top-level manifest persisted as `{prefix}/checkpoints.json`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CheckpointManifest {
    /// Map from scenario name to its metadata.
    pub scenarios: HashMap<String, ScenarioCheckpoint>,
}

/// Per-scenario metadata stored inside the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioCheckpoint {
    /// The checkpoint snapshot indexes that are stored for this scenario.
    ///
    /// Replaces the old `num_checkpoints` count — callers should iterate
    /// over this vec directly rather than assuming a contiguous `0..N` range.
    #[serde(default)]
    pub checkpoint_indexes: Vec<usize>,
    /// The query indexes that have results stored in each checkpoint.
    ///
    /// Replaces the old `num_queries` count — callers should iterate
    /// over this vec directly rather than assuming a contiguous `0..N` range.
    #[serde(default)]
    pub query_indexes: Vec<usize>,
    /// Number of ETL steps between each checkpoint.
    ///
    /// This is the step count that was passed to [`ETLPipeline::run`] during
    /// checkpoint generation. Consumers can use this value to replay the
    /// pipeline with the same cadence.
    #[serde(default)]
    pub checkpoint_interval_steps: usize,
}

/// S3‑backed store for uploading and downloading checkpoint artefacts.
pub struct CheckpointStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl CheckpointStore {
    /// Build a new `CheckpointStore` from raw S3 connection parameters.
    ///
    /// `prefix` may be empty; it is the key‑prefix shared by all scenarios
    /// (e.g. `"run-42"`).
    pub fn new(
        bucket: &str,
        prefix: &str,
        region: Option<&str>,
        endpoint: Option<&str>,
    ) -> anyhow::Result<Self> {
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);

        if let Some(region) = region {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = endpoint
            && !endpoint.is_empty()
        {
            builder = builder.with_endpoint(endpoint);
            if endpoint.starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
        }

        let store = Arc::new(builder.build()?);
        Ok(Self {
            store,
            prefix: prefix.to_owned(),
        })
    }

    fn object_path(&self, suffix: &str) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(suffix.to_owned())
        } else {
            ObjectPath::from(format!("{}/{suffix}", self.prefix))
        }
    }

    fn manifest_path(&self) -> ObjectPath {
        self.object_path("checkpoints.json")
    }

    fn checkpoint_parquet_path(&self, checkpoint_idx: usize, query_idx: usize) -> ObjectPath {
        self.object_path(&format!("checkpoints/{checkpoint_idx}/{query_idx}.parquet"))
    }

    /// Upload all checkpoint parquet files from `local_checkpoint_dir` to S3,
    /// then update (merge into) the manifest at `{prefix}/checkpoints.json`.
    ///
    /// The local directory is expected to have the layout produced by the
    /// checkpointer binary:
    ///
    /// ```text
    /// {local_checkpoint_dir}/
    ///   0/
    ///     0.parquet
    ///     1.parquet
    ///   1/
    ///     0.parquet
    ///     ...
    /// ```
    pub async fn upload_checkpoints(
        &self,
        scenario: &str,
        local_checkpoint_dir: &Path,
        checkpoint_interval_steps: usize,
    ) -> anyhow::Result<()> {
        if !local_checkpoint_dir.is_dir() {
            anyhow::bail!(
                "Checkpoint directory does not exist: {}",
                local_checkpoint_dir.display()
            );
        }

        let mut checkpoint_indexes: Vec<usize> = Vec::new();
        let mut query_indexes_set: std::collections::BTreeSet<usize> =
            std::collections::BTreeSet::new();

        // Iterate over checkpoint index directories (0, 1, 2, …).
        let mut checkpoint_dirs: Vec<_> = std::fs::read_dir(local_checkpoint_dir)?
            .filter_map(Result::ok)
            .filter(|e| e.path().is_dir())
            .collect();
        checkpoint_dirs.sort_by_key(|e| e.file_name());

        for checkpoint_entry in &checkpoint_dirs {
            let checkpoint_idx: usize = checkpoint_entry
                .file_name()
                .to_string_lossy()
                .parse()
                .unwrap_or(0);

            let mut query_files: Vec<_> = std::fs::read_dir(checkpoint_entry.path())?
                .filter_map(Result::ok)
                .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
                .collect();
            query_files.sort_by_key(|e| e.file_name());

            for qf in &query_files {
                let q_idx: usize = qf
                    .path()
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);

                let bytes = std::fs::read(qf.path())?;
                let dest = self.checkpoint_parquet_path(checkpoint_idx, q_idx);

                tracing::info!(
                    scenario,
                    checkpoint = checkpoint_idx,
                    query = q_idx,
                    dest = %dest,
                    "Uploading checkpoint parquet"
                );
                self.store.put(&dest, PutPayload::from(bytes)).await?;

                query_indexes_set.insert(q_idx);
            }

            checkpoint_indexes.push(checkpoint_idx);
        }

        // Merge into manifest.
        let mut manifest = self.download_manifest().await.unwrap_or_default();
        let query_indexes: Vec<usize> = query_indexes_set.into_iter().collect();
        let num_checkpoints = checkpoint_indexes.len();
        let num_queries = query_indexes.len();
        manifest.scenarios.insert(
            scenario.to_owned(),
            ScenarioCheckpoint {
                checkpoint_indexes,
                query_indexes,
                checkpoint_interval_steps,
            },
        );
        self.put_manifest(&manifest).await?;

        tracing::info!(
            scenario,
            num_checkpoints,
            num_queries,
            "Checkpoint upload complete"
        );
        Ok(())
    }

    /// Upload (overwrite) the manifest JSON.
    async fn put_manifest(&self, manifest: &CheckpointManifest) -> anyhow::Result<()> {
        let json = serde_json::to_vec_pretty(manifest)?;
        self.store
            .put(&self.manifest_path(), PutPayload::from(json))
            .await?;
        Ok(())
    }

    /// Download and deserialise the manifest from S3.
    ///
    /// Returns `Ok(manifest)` or an error if the manifest does not exist or
    /// cannot be parsed.
    pub async fn download_manifest(&self) -> anyhow::Result<CheckpointManifest> {
        let data = self.store.get(&self.manifest_path()).await?.bytes().await?;
        let manifest: CheckpointManifest = serde_json::from_slice(&data)?;
        Ok(manifest)
    }

    /// Download all checkpoint parquet files for `scenario` into
    /// `local_dir/{checkpoint_idx}/{query_idx}.parquet`.
    pub async fn download_checkpoints(
        &self,
        scenario: &str,
        info: &ScenarioCheckpoint,
        local_dir: &Path,
    ) -> anyhow::Result<()> {
        for &checkpoint_idx in &info.checkpoint_indexes {
            let checkpoint_dir = local_dir.join(checkpoint_idx.to_string());
            std::fs::create_dir_all(&checkpoint_dir)?;

            for &q_idx in &info.query_indexes {
                let remote = self.checkpoint_parquet_path(checkpoint_idx, q_idx);
                let data = self.store.get(&remote).await?.bytes().await?;
                let local_path = checkpoint_dir.join(format!("{q_idx}.parquet"));
                std::fs::write(&local_path, &data)?;

                tracing::info!(
                    scenario,
                    checkpoint = checkpoint_idx,
                    query = q_idx,
                    path = %local_path.display(),
                    "Downloaded checkpoint parquet"
                );
            }
        }

        Ok(())
    }
}
