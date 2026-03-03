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

use std::{fmt::Display, future::Future, path::PathBuf, process::Child, time::Duration};

use anyhow::{Result, anyhow};
use flight_client::{Credentials, FlightClient};
use secrecy::SecretString;
use sysinfo::Pid;
use tempfile::TempDir;

const HTTP_BASE_URL: &str = "http://localhost:8090";
const FLIGHT_URL: &str = "http://localhost:50051";
const READY_ENDPOINT: &str = "/v1/ready";

async fn wait_until_true<F, Fut>(max_wait: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = std::time::Instant::now();
    while start.elapsed() < max_wait {
        if f().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

pub struct Process {
    _pid: Pid,
}

impl Process {
    #[must_use]
    pub fn new(pid: Pid) -> Self {
        Self { _pid: pid }
    }
}

#[derive(Debug, Clone)]
pub struct SpicedVersion(String);
impl SpicedVersion {
    #[must_use]
    pub fn new(version: String) -> Self {
        Self(version)
    }
}

impl Display for SpicedVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub enum SpicedInstance {
    Owned {
        child: Child,
        tempdir: TempDir,
        version: SpicedVersion,
    },
}

impl SpicedInstance {
    #[must_use]
    pub fn version(&self) -> &str {
        let Self::Owned { version, .. } = self;
        version.0.as_str()
    }

    pub fn get_tempdir_path(&self) -> Result<PathBuf> {
        let Self::Owned { tempdir, .. } = self;

        Ok(tempdir.path().to_path_buf())
    }

    /// Get a flight client for the spiced instance
    ///
    /// # Errors
    ///
    /// - If the flight client fails to be created
    pub async fn flight_client(
        &self,
        api_key: Option<String>,
        disable_caching: bool,
    ) -> Result<FlightClient> {
        let credentials = if let Some(key) = api_key {
            Credentials::Bearer {
                token: std::sync::Arc::new(SecretString::new(key.into())),
                prefix: true,
            }
        } else {
            Credentials::Anonymous
        };

        let mut metadata = tonic::metadata::MetadataMap::new();
        if disable_caching {
            metadata.insert("cache-control", "no-cache".parse()?);
        }

        let flight_client = FlightClient::try_new(
            std::sync::Arc::from(FLIGHT_URL),
            credentials,
            Some(metadata),
            None,
        )
        .await
        .map_err(|e| anyhow!("{e}"))?;

        Ok(flight_client)
    }

    /// Get an http client for the spiced instance
    ///
    /// # Errors
    ///
    /// - If the http client fails to be created
    pub fn http_client(&self) -> Result<reqwest::Client> {
        Ok(reqwest::Client::builder()
            .user_agent("spice-test-framework/1.0")
            .build()?)
    }

    /// Get the HTTP base URL for this instance
    #[must_use]
    pub fn http_base_url(&self) -> &str {
        HTTP_BASE_URL
    }

    /// Wait for the spiced instance to be ready
    ///
    /// # Errors
    ///
    /// - If the spiced instance fails to be ready within the timeout
    pub async fn wait_for_ready(&mut self, timeout: Duration) -> Result<()> {
        // Wait for the spiced instance to be ready by polling the `/v1/ready` endpoint
        let client = self.http_client()?;
        let http_base = self.http_base_url().to_string();
        let ready_url = format!("{http_base}{READY_ENDPOINT}");
        if !wait_until_true(timeout, || async {
            let response = client.get(&ready_url).send().await;
            match response {
                Ok(response) => response.status().is_success(),
                Err(_) => false,
            }
        })
        .await
        {
            anyhow::bail!("Spiced instance not ready within {timeout:?}");
        }

        // Give Flight server a moment to finish starting up after HTTP is ready
        // Flight starts asynchronously and may not be available immediately
        tokio::time::sleep(Duration::from_millis(500)).await;

        Ok(())
    }

    pub async fn is_ready(&self) -> bool {
        let Ok(client) = self.http_client() else {
            return false;
        };
        let ready_url = format!("{}{READY_ENDPOINT}", self.http_base_url());
        let response = client.get(&ready_url).send().await;
        match response {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// Stop the spiced instance
    ///
    /// # Errors
    ///
    /// - If the spiced instance fails to exit
    pub fn stop(&mut self) -> Result<()> {
        let Self::Owned { child, .. } = self;

        #[cfg(not(target_os = "windows"))]
        {
            // Send a SIGTERM to the spiced instance and wait for it to exit
            let Ok(pid_i32) = child.id().try_into() else {
                anyhow::bail!("Failed to convert pid to i32");
            };
            nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(pid_i32),
                nix::sys::signal::Signal::SIGTERM,
            )?;
            child.wait()?;
        }

        #[cfg(target_os = "windows")]
        {
            // On Windows, we can use the built-in process termination
            child.kill()?;
            child.wait()?;
        }

        Ok(())
    }

    /// Returns an instance of a `Process` for the spiced instance
    /// This allows tracking the spiced process, without owning the spiced instance
    pub fn process(&self) -> Result<Process> {
        let Self::Owned { child, .. } = self;

        Ok(Process::new(Pid::from_u32(child.id())))
    }
}

impl Drop for SpicedInstance {
    fn drop(&mut self) {
        let Self::Owned { child, .. } = self;

        match child.kill() {
            Ok(()) => (),
            Err(e) => eprintln!("Failed to kill spiced instance: {e}"),
        }
    }
}
