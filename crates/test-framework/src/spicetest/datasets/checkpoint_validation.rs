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

//! Checkpoint-based results validation for the spicetest runner.
//!
//! When the ETL pipeline pauses at a checkpoint boundary, the load runner
//! sends a [`ValidationCommand::Enable`] to the test runner, providing the
//! expected result batches for each query. Worker 0 then validates every
//! query execution against those expected results until a
//! [`ValidationCommand::Disable`] command is received, at which point the
//! validation data and accumulated results are dropped.
//!
//! The current status can be observed at any time through a
//! `tokio::sync::watch` channel that publishes [`ValidationStatus`] snapshots.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;

use crate::queries::validation::QueryValidationFailReason;

/// Commands sent from the load runner to the spicetest runner to control
/// checkpoint-based results validation.
#[derive(Debug, Clone)]
pub enum ValidationCommand {
    /// Enable results validation using the provided checkpoint data.
    ///
    /// The map keys are query names (matching [`Query::name`]) and values are
    /// the expected result batches for that query at the current checkpoint.
    Enable {
        /// The checkpoint index this validation window corresponds to.
        checkpoint_idx: usize,
        /// Expected results keyed by query name.
        expected_results: HashMap<Arc<str>, Vec<RecordBatch>>,
    },
    /// Disable results validation. The runner drops the expected batches and
    /// clears any accumulated validation results.
    Disable,
}

/// Per-query validation outcome recorded during a validation window.
#[derive(Debug, Clone)]
pub struct QueryValidationOutcome {
    /// The query name.
    pub query_name: Arc<str>,
    /// Number of times this query was validated successfully.
    pub pass_count: usize,
    /// Number of times this query failed validation.
    pub fail_count: usize,
    /// The most recent failure reason, if any.
    pub last_failure: Option<QueryValidationFailReason>,
}

/// A snapshot of the current checkpoint validation state, published via a
/// `watch` channel so the load runner can inspect it at any time.
#[derive(Debug, Clone, Default)]
pub enum ValidationStatus {
    /// Checkpoint validation is not currently active.
    #[default]
    Inactive,
    /// Checkpoint validation is active for the given checkpoint index.
    Active {
        /// The checkpoint index being validated.
        checkpoint_idx: usize,
        /// Per-query outcomes accumulated so far in this validation window.
        outcomes: Vec<QueryValidationOutcome>,
        /// Number of complete query-set iterations that have finished
        /// since validation was enabled for this checkpoint.
        completed_iterations: usize,
    },
}

impl ValidationStatus {
    /// Returns `true` if all validated queries have passed (no failures).
    #[must_use]
    pub fn all_passed(&self) -> bool {
        match self {
            ValidationStatus::Inactive => true,
            ValidationStatus::Active { outcomes, .. } => outcomes.iter().all(|o| o.fail_count == 0),
        }
    }

    /// Returns the total number of validation failures across all queries.
    #[must_use]
    pub fn total_failures(&self) -> usize {
        match self {
            ValidationStatus::Inactive => 0,
            ValidationStatus::Active { outcomes, .. } => {
                outcomes.iter().map(|o| o.fail_count).sum()
            }
        }
    }

    /// Returns the number of completed query-set iterations since
    /// validation was enabled, or `0` if inactive.
    #[must_use]
    pub fn completed_iterations(&self) -> usize {
        match self {
            ValidationStatus::Inactive => 0,
            ValidationStatus::Active {
                completed_iterations,
                ..
            } => *completed_iterations,
        }
    }
}

/// Handles for the load runner to interact with checkpoint validation.
///
/// Created by [`create_validation_channels`] and threaded through the
/// `NotStarted` → `Running` → worker pipeline.
pub struct ValidationController {
    /// Send commands to the test runner (worker 0).
    pub command_tx: tokio::sync::watch::Sender<Option<ValidationCommand>>,
    /// Observe the current validation status.
    pub status_rx: tokio::sync::watch::Receiver<ValidationStatus>,
}

/// Handles held by the test runner (worker 0) to receive commands and
/// publish status updates.
pub struct ValidationWorkerHandles {
    /// Receive commands from the load runner.
    pub command_rx: tokio::sync::watch::Receiver<Option<ValidationCommand>>,
    /// Publish status updates.
    pub status_tx: Arc<tokio::sync::watch::Sender<ValidationStatus>>,
}

/// Create the paired channels for checkpoint validation.
///
/// Returns `(controller, worker_handles)`.
///
/// - The **controller** is held by the load runner to send enable/disable
///   commands and read the current validation status.
/// - The **worker handles** are given to worker 0 so it can react to
///   commands and publish its validation status.
pub fn create_validation_channels() -> (ValidationController, ValidationWorkerHandles) {
    let (command_tx, command_rx) = tokio::sync::watch::channel(None);
    let (status_tx, status_rx) = tokio::sync::watch::channel(ValidationStatus::Inactive);

    (
        ValidationController {
            command_tx,
            status_rx,
        },
        ValidationWorkerHandles {
            command_rx,
            status_tx: Arc::new(status_tx),
        },
    )
}
