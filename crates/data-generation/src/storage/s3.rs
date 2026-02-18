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

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;

use crate::config::TargetConfig;

/// Unified S3 storage backend that implements both [`Source`] and [`Target`].
///
/// The trait implementations live in their respective modules
/// (`source/s3.rs` and `target/s3.rs`) to keep concerns separated.
#[derive(Clone)]
pub struct S3Storage {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) bucket: String,
    pub(crate) prefix: String,
    pub(crate) region: Option<String>,
}

impl S3Storage {
    pub fn new(config: &TargetConfig) -> anyhow::Result<Self> {
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(&config.bucket);

        if let Some(region) = &config.region {
            tracing::info!("S3 storage with region: {region}");
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = &config.endpoint
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
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
            region: config.region.clone(),
        })
    }

    /// Returns the S3 URI for a given table name (e.g. `s3://bucket/prefix/customer/`).
    pub fn table_s3_path(&self, table_name: &str) -> String {
        if self.prefix.is_empty() {
            format!("s3://{}/{table_name}/", self.bucket)
        } else {
            format!("s3://{}/{}/{table_name}/", self.bucket, self.prefix)
        }
    }

    /// Returns the [`ObjectPath`] for a batch file within a table directory.
    pub(crate) fn batch_object_path(&self, table_name: &str, batch_id: u64) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(format!("{table_name}/batch-{batch_id:06}.parquet"))
        } else {
            ObjectPath::from(format!(
                "{}/{table_name}/batch-{batch_id:06}.parquet",
                self.prefix
            ))
        }
    }

    /// Returns the [`ObjectPath`] prefix for listing objects in a table directory.
    pub(crate) fn table_object_prefix(&self, table_name: &str) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(format!("{table_name}/"))
        } else {
            ObjectPath::from(format!("{}/{table_name}/", self.prefix))
        }
    }
}
