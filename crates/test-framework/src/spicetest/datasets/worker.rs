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

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    panic,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use anyhow::Result;
use arrow::array::RecordBatch;
use dashmap::DashMap;
use futures::TryStreamExt;
use indicatif::ProgressBar;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::execution::QueryExecutor;
use crate::telemetry::streaming::QueryMetricEvent;

use crate::{
    metrics::QueryStatus,
    queries::{Query, validation, validation::QueryValidationResult},
    snapshot::record_explain_plan,
};

use super::EndCondition;
use super::checkpoint_validation::{
    QueryValidationOutcome, ValidationCommand, ValidationStatus, ValidationWorkerHandles,
};

/// Mutable state for checkpoint-based results validation.
///
/// Held only by worker 0 when validation handles are configured. Tracks the
/// current checkpoint data and accumulated per-query outcomes.
struct CheckpointValidationState {
    /// Whether checkpoint validation is currently enabled.
    active: bool,
    /// The checkpoint index for the current validation window.
    checkpoint_idx: usize,
    /// Expected results keyed by query name.
    expected_results: HashMap<Arc<str>, Vec<RecordBatch>>,
    /// Per-query validation outcomes accumulated during this window.
    outcomes: HashMap<Arc<str>, QueryValidationOutcome>,
    /// Number of complete query-set iterations since validation was enabled.
    completed_iterations: usize,
    /// Sender for publishing validation status updates.
    status_tx: Arc<tokio::sync::watch::Sender<ValidationStatus>>,
    /// Receiver for validation commands from the load runner.
    command_rx: tokio::sync::watch::Receiver<Option<ValidationCommand>>,
}

impl CheckpointValidationState {
    fn new(handles: ValidationWorkerHandles) -> Self {
        Self {
            active: false,
            checkpoint_idx: 0,
            expected_results: HashMap::new(),
            outcomes: HashMap::new(),
            completed_iterations: 0,
            status_tx: handles.status_tx,
            command_rx: handles.command_rx,
        }
    }

    /// Check for new commands, returning true if the validation state changed.
    fn poll_commands(&mut self) -> bool {
        // Check if there's a new command without blocking
        if !self.command_rx.has_changed().unwrap_or(false) {
            return false;
        }

        let command = self.command_rx.borrow_and_update().clone();
        match command {
            Some(ValidationCommand::Enable {
                checkpoint_idx,
                expected_results,
            }) => {
                self.active = true;
                self.checkpoint_idx = checkpoint_idx;
                self.expected_results = expected_results;
                self.outcomes.clear();
                self.completed_iterations = 0;
                eprintln!(
                    "Checkpoint validation enabled for checkpoint {}",
                    checkpoint_idx
                );
                self.publish_status();
                true
            }
            Some(ValidationCommand::Disable) => {
                self.active = false;
                self.expected_results.clear();
                self.outcomes.clear();
                eprintln!("Checkpoint validation disabled");
                let _ = self.status_tx.send(ValidationStatus::Inactive);
                true
            }
            None => false,
        }
    }

    /// Validate query results against the checkpoint expected data.
    ///
    /// Returns `None` if no expected data exists for the given query,
    /// or `Some(result)` with the validation outcome.
    fn validate(
        &mut self,
        query: &Query,
        actual_batches: &[RecordBatch],
    ) -> Option<QueryValidationResult> {
        if !self.active {
            return None;
        }

        let expected_batches = self.expected_results.get(&query.name)?;

        let result = validation::validate_with_expected_batches(
            &query.name,
            actual_batches,
            expected_batches,
        );

        match &result {
            Ok(QueryValidationResult::Pass) => {
                self.record_outcome(&query.name, true, None);
            }
            Ok(QueryValidationResult::Fail(reason)) => {
                eprintln!(
                    "Checkpoint validation FAIL - checkpoint {} - query '{}': {:?}",
                    self.checkpoint_idx, query.name, reason
                );
                self.record_outcome(&query.name, false, Some(reason.clone()));
            }
            Err(e) => {
                eprintln!(
                    "Checkpoint validation error - checkpoint {} - query '{}': {}",
                    self.checkpoint_idx, query.name, e
                );
                // Treat errors as failures
                self.record_outcome(
                    &query.name,
                    false,
                    Some(crate::queries::validation::QueryValidationFailReason::NoExpectedAnswer),
                );
            }
        }

        self.publish_status();
        result.ok()
    }

    fn record_outcome(
        &mut self,
        query_name: &Arc<str>,
        passed: bool,
        failure: Option<crate::queries::validation::QueryValidationFailReason>,
    ) {
        let outcome = self
            .outcomes
            .entry(Arc::clone(query_name))
            .or_insert_with(|| QueryValidationOutcome {
                query_name: Arc::clone(query_name),
                pass_count: 0,
                fail_count: 0,
                last_failure: None,
            });

        if passed {
            outcome.pass_count += 1;
        } else {
            outcome.fail_count += 1;
            outcome.last_failure = failure;
        }
    }

    /// Notify that a complete query-set iteration has finished.
    ///
    /// This should be called once per iteration of the main query loop so
    /// the load runner can poll `completed_iterations` to decide when
    /// enough validation passes have been recorded.
    fn record_iteration_completed(&mut self) {
        if self.active {
            self.completed_iterations += 1;
            self.publish_status();
        }
    }

    fn publish_status(&self) {
        let status = ValidationStatus::Active {
            checkpoint_idx: self.checkpoint_idx,
            outcomes: self.outcomes.values().cloned().collect(),
            completed_iterations: self.completed_iterations,
        };
        let _ = self.status_tx.send(status);
    }
}

pub(crate) struct SpiceTestQueryWorker {
    id: usize,
    query_set: Vec<Query>,
    end_condition: EndCondition,
    explain_plan_snapshot: bool,
    results_snapshot_predicate: Option<fn(&str) -> bool>,
    name: String,
    pub progress_bar: Option<ProgressBar>,
    validate: bool,
    scale_factor: f64,
    executor: Box<dyn QueryExecutor>,
    /// Optional custom validation data for scenario queries
    validation_data: Option<HashMap<Arc<str>, Vec<RecordBatch>>>,
    /// Optional reference schema for validating against known good tables
    reference_schema: Option<String>,
    /// Queries to skip row count validation for (e.g., queries that legitimately return 0 rows)
    skip_row_count_validation: HashSet<String>,
    shutdown_token: CancellationToken,
    /// Optional sender for streaming query metrics to OTLP
    streaming_metrics_sender: Option<mpsc::Sender<QueryMetricEvent>>,
    /// Duration threshold - queries exceeding this are marked as failed in streaming metrics
    query_duration_threshold: Option<Duration>,
    /// Optional handles for checkpoint-based results validation.
    /// Only worker 0 receives these handles; other workers leave this as `None`.
    validation_handles: Option<ValidationWorkerHandles>,
}

pub struct SpiceTestQueryWorkerResult {
    pub query_durations: BTreeMap<Arc<str>, Vec<Duration>>,
    pub query_iteration_durations: BTreeMap<Arc<str>, (SystemTime, SystemTime)>,
    pub query_statuses: BTreeMap<Arc<str>, QueryStatus>,
    pub connection_failed: bool,
    pub row_counts: BTreeMap<Arc<str>, Vec<usize>>,
}

struct QueryRunResult {
    connection_failed: bool,
    query_failure: Option<String>,
    shutdown: bool,
}

impl SpiceTestQueryWorkerResult {
    pub fn new(
        query_durations: &Arc<DashMap<Arc<str>, Vec<Duration>>>,
        query_iteration_durations: BTreeMap<Arc<str>, (SystemTime, SystemTime)>,
        query_statuses: BTreeMap<Arc<str>, QueryStatus>,
        connection_failed: bool,
        row_counts: BTreeMap<Arc<str>, Vec<usize>>,
    ) -> Self {
        let query_durations = query_durations
            .iter()
            .map(|mapref| (Arc::clone(mapref.key()), mapref.value().clone()))
            .collect();

        Self {
            query_durations,
            query_iteration_durations,
            query_statuses,
            connection_failed,
            row_counts,
        }
    }
}

impl SpiceTestQueryWorker {
    pub fn new(
        id: usize,
        query_set: Vec<Query>,
        end_condition: EndCondition,
        name: String,
        executor: Box<dyn QueryExecutor>,
    ) -> Self {
        Self {
            id,
            query_set,
            end_condition,
            executor,
            explain_plan_snapshot: false,
            results_snapshot_predicate: None,
            name,
            progress_bar: None,
            validate: false,
            scale_factor: 1.0,
            validation_data: None,
            reference_schema: None,
            skip_row_count_validation: default_row_count_validation_skip_queries(),
            shutdown_token: CancellationToken::new(),
            streaming_metrics_sender: None,
            query_duration_threshold: None,
            validation_handles: None,
        }
    }

    pub fn with_scale_factor(mut self, scale_factor: f64) -> Self {
        self.scale_factor = scale_factor;
        self
    }

    pub fn with_shutdown_token(mut self, shutdown_token: CancellationToken) -> Self {
        self.shutdown_token = shutdown_token;
        self
    }

    pub fn with_validate(mut self, validate: bool) -> Self {
        self.validate = validate;
        self
    }

    pub fn with_streaming_metrics(mut self, sender: mpsc::Sender<QueryMetricEvent>) -> Self {
        self.streaming_metrics_sender = Some(sender);
        self
    }

    pub fn with_query_duration_threshold(mut self, threshold: Duration) -> Self {
        self.query_duration_threshold = Some(threshold);
        self
    }

    /// Set the checkpoint validation handles for this worker.
    ///
    /// Only worker 0 should receive these handles. When set, the worker will
    /// react to [`ValidationCommand`]s, validating query results against
    /// checkpoint data and publishing [`ValidationStatus`] updates.
    pub fn with_validation_handles(mut self, handles: ValidationWorkerHandles) -> Self {
        self.validation_handles = Some(handles);
        self
    }

    pub fn with_explain_plan_snapshot(mut self, explain_plan_snapshot: bool) -> Self {
        self.explain_plan_snapshot = explain_plan_snapshot;
        self
    }

    pub fn with_results_snapshot(
        mut self,
        results_snapshot_predicate: Option<fn(&str) -> bool>,
    ) -> Self {
        self.results_snapshot_predicate = results_snapshot_predicate;
        self
    }

    pub fn with_progress_bar(mut self, progress_bar: ProgressBar) -> Self {
        self.progress_bar = Some(progress_bar);
        self
    }

    pub fn with_validation_data(
        mut self,
        validation_data: HashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Self {
        self.validation_data = Some(validation_data);
        self
    }

    pub fn with_reference_schema(mut self, reference_schema: Option<String>) -> Self {
        self.reference_schema = reference_schema;
        self
    }

    pub fn with_skip_row_count_validation(
        mut self,
        queries: impl IntoIterator<Item = String>,
    ) -> Self {
        self.skip_row_count_validation = queries.into_iter().collect();
        self
    }

    /// Send a query metric event to the streaming exporter if configured.
    /// If a duration threshold is set and the query exceeds it, it will be marked as a timeout failure.
    fn send_streaming_metric(&self, query_name: &str, duration: Duration, success: bool) {
        let Some(sender) = &self.streaming_metrics_sender else {
            return;
        };

        // Check if duration exceeds threshold - if so, mark as timeout failure
        let exceeded_threshold =
            success && self.query_duration_threshold.is_some_and(|t| duration > t);

        let event = if exceeded_threshold {
            QueryMetricEvent::with_failure(query_name.to_string(), duration, self.id, "timeout")
        } else if success {
            QueryMetricEvent::new(query_name.to_string(), duration, true, self.id)
        } else {
            QueryMetricEvent::with_failure(query_name.to_string(), duration, self.id, "error")
        };

        // Non-blocking send - if channel is full, we drop the metric
        let _ = sender.try_send(event);
    }

    /// Validate query results against expected data
    /// Uses TPCH validation for TPCH queries, custom validation data for scenario queries
    fn validate_query_results(
        &self,
        query: &Query,
        actual_batches: &[RecordBatch],
    ) -> Result<QueryValidationResult> {
        // Check if we have custom validation data for this query
        if let Some(validation_data) = &self.validation_data
            && let Some(expected_batches) = validation_data.get(&query.name)
        {
            return validation::validate_with_expected_batches(
                &query.name,
                actual_batches,
                expected_batches,
            );
        }

        // Fall back to TPCH validation (which handles TPCH, parameterized TPCH, etc.)
        validation::validate_tpch_query(query, actual_batches)
    }

    pub fn start(mut self) -> JoinHandle<Result<SpiceTestQueryWorkerResult>> {
        tokio::spawn(async move {
            // Load test queries may be generated with multiple parameter sets, resulting in a large
            // set of queries. To respect duration limits, we group queries by name and run one
            // group at a time, cycling through each group's parameter variations.
            // If queries are unique, it will result in a single query set and will be the same as usual
            let query_sets = build_unique_query_sets(&self.query_set)?;

            let query_durations: Arc<DashMap<Arc<str>, Vec<Duration>>> = Arc::new(DashMap::new());

            // Keeps track of the start and end time of each query iteration
            let mut query_iteration_durations: BTreeMap<Arc<str>, (SystemTime, SystemTime)> =
                BTreeMap::new();

            let mut query_statuses: BTreeMap<Arc<str>, QueryStatus> = BTreeMap::new();
            let mut row_counts: BTreeMap<Arc<str>, Vec<usize>> = BTreeMap::new();
            let mut query_set_count = 0;
            let start = Instant::now();

            // Initialize checkpoint validation state if handles were provided (worker 0 only).
            let mut checkpoint_validation = self
                .validation_handles
                .take()
                .map(CheckpointValidationState::new);

            match self.end_condition {
                EndCondition::Duration(_) | EndCondition::Unlimited => {
                    // For Duration-based or Unlimited end condition, keep running queries in sequence
                    while !self.shutdown_token.is_cancelled()
                        && !self.end_condition.is_met(&start, query_set_count)
                    {
                        // Poll for checkpoint validation commands (non-blocking).
                        if let Some(ref mut cv) = checkpoint_validation {
                            cv.poll_commands();
                        }

                        if self.progress_bar.is_none() && self.id == 0 {
                            println!(
                                "Worker {} - Query set count: {} - Elapsed time: {:?}",
                                self.id,
                                query_set_count,
                                start.elapsed()
                            );
                        }

                        // Select the query set to use for this iteration
                        let queries_to_run = {
                            let set_index = query_set_count % query_sets.len();
                            &query_sets[set_index]
                        };

                        if !self
                            .run_query_set(
                                Arc::clone(&query_durations),
                                &mut query_statuses,
                                &mut row_counts,
                                queries_to_run,
                                &mut checkpoint_validation,
                            )
                            .await?
                        {
                            if self.shutdown_token.is_cancelled() {
                                println!("Worker {} exiting due to shutdown", self.id);
                                break;
                            }
                            return Ok(SpiceTestQueryWorkerResult::new(
                                &query_durations,
                                query_iteration_durations,
                                query_statuses,
                                true,
                                row_counts,
                            ));
                        }
                        query_set_count += 1;

                        // Notify checkpoint validation that a full iteration completed.
                        if let Some(ref mut cv) = checkpoint_validation {
                            cv.record_iteration_completed();
                        }
                    }
                }
                EndCondition::QuerySetCompleted(target_count) => {
                    // For QuerySetCompleted, run each query target_count times before moving to next
                    let start = SystemTime::now();
                    for query in &self.query_set {
                        if self.shutdown_token.is_cancelled() {
                            break;
                        }
                        if self.validate && query.name.contains("simple") {
                            continue; // skip validation for simple TPCH queries, because they are not part of the spec
                        }

                        let mut current_query_count = 0;
                        let query_start = SystemTime::now();
                        let mut query_status = QueryStatus::Passed;

                        let snapshot_results = self
                            .results_snapshot_predicate
                            .is_some_and(|predicate| predicate(&query.name))
                            && self.id == 0; // only one worker should snapshot results

                        // Additional round of query run before recording results.
                        // To discard the abnormal results caused by: establishing initial connection / spark cluster startup time

                        let QueryRunResult {
                            connection_failed,
                            shutdown,
                            ..
                        } = self
                            .run_single_query(
                                query,
                                Arc::new(DashMap::new()),
                                &mut BTreeMap::new(),
                                snapshot_results,
                                false,
                                &mut checkpoint_validation,
                            )
                            .await?;
                        if shutdown {
                            break;
                        }
                        if connection_failed {
                            return Ok(SpiceTestQueryWorkerResult::new(
                                &query_durations,
                                query_iteration_durations,
                                query_statuses,
                                true,
                                row_counts,
                            ));
                        }

                        if self.explain_plan_snapshot
                            && self.id == 0
                            && let Some(client) = self.executor.as_flight_client()
                        {
                            println!("Worker {} - Query '{}' - Explain plan", self.id, query.name);
                            if let Err(e) = record_explain_plan(
                                client,
                                self.name.as_str(),
                                query,
                                self.scale_factor,
                            )
                            .await
                            {
                                println!(
                                    "Worker {} - Query '{}' explain plan failed: {}",
                                    self.id, query.name, e
                                );

                                query_status = QueryStatus::Failed(Some(
                                    "Explain plan snapshot assertion failed".into(),
                                ));
                            }
                        }

                        while current_query_count < target_count {
                            if self.shutdown_token.is_cancelled() {
                                break;
                            }
                            if self.progress_bar.is_none()
                                && self.id == 0
                                && (current_query_count % 10 == 0 || target_count <= 5)
                            {
                                println!(
                                    "Worker {} - Query '{}' - {}/{} - Elapsed time: {:?}",
                                    self.id,
                                    query.name,
                                    current_query_count + 1,
                                    target_count,
                                    start.elapsed().unwrap_or_default()
                                );
                            }

                            let QueryRunResult {
                                connection_failed,
                                query_failure,
                                shutdown,
                            } = self
                                .run_single_query(
                                    query,
                                    Arc::clone(&query_durations),
                                    &mut row_counts,
                                    false, // don't attempt to snapshot results more than once
                                    self.validate,
                                    &mut checkpoint_validation,
                                )
                                .await?;

                            if shutdown {
                                break;
                            }

                            if connection_failed {
                                return Ok(SpiceTestQueryWorkerResult::new(
                                    &query_durations,
                                    query_iteration_durations,
                                    query_statuses,
                                    true,
                                    row_counts,
                                ));
                            }

                            if let Some(query_failure) = query_failure {
                                query_status = QueryStatus::Failed(Some(query_failure.into()));
                            }

                            current_query_count += 1;
                        }
                        let end = SystemTime::now();
                        query_iteration_durations
                            .insert(Arc::clone(&query.name), (query_start, end));
                        query_statuses.insert(Arc::clone(&query.name), query_status);
                    }
                }
            }

            println!("Worker {} exited", self.id);

            Ok(SpiceTestQueryWorkerResult::new(
                &query_durations,
                query_iteration_durations,
                query_statuses,
                false,
                row_counts,
            ))
        })
    }

    // run queries as a duration-based test
    async fn run_query_set(
        &self,
        query_durations: Arc<DashMap<Arc<str>, Vec<Duration>>>,
        query_statuses: &mut BTreeMap<Arc<str>, QueryStatus>,
        row_counts: &mut BTreeMap<Arc<str>, Vec<usize>>,
        queries: &[Query],
        checkpoint_validation: &mut Option<CheckpointValidationState>,
    ) -> Result<bool> {
        for query in queries {
            if self.shutdown_token.is_cancelled() {
                return Ok(false);
            }
            let QueryRunResult {
                connection_failed,
                query_failure,
                shutdown,
            } = self
                .run_single_query(
                    query,
                    Arc::clone(&query_durations),
                    row_counts,
                    false,
                    false,
                    checkpoint_validation,
                )
                .await?;
            if shutdown || connection_failed {
                return Ok(false);
            }

            let worker_status = if let Some(query_failure) = query_failure {
                QueryStatus::Failed(Some(query_failure.into()))
            } else {
                QueryStatus::Passed
            };

            query_statuses
                .entry(Arc::clone(&query.name))
                .and_modify(|existing_status| {
                    // If the worker reports failure, update the status to Failed
                    if matches!(worker_status, QueryStatus::Failed(_)) {
                        *existing_status = worker_status.clone();
                    }
                })
                .or_insert(worker_status);
        }
        Ok(true)
    }

    // run queries as a set-completion based test
    async fn run_single_query(
        &self,
        query: &Query,
        query_durations: Arc<DashMap<Arc<str>, Vec<Duration>>>,
        row_counts: &mut BTreeMap<Arc<str>, Vec<usize>>,
        results_snapshot: bool,
        validate: bool,
        checkpoint_validation: &mut Option<CheckpointValidationState>,
    ) -> Result<QueryRunResult> {
        match self
            .execute_query(
                query,
                Arc::clone(&query_durations),
                row_counts,
                results_snapshot,
                validate,
                checkpoint_validation,
            )
            .await
        {
            Ok(()) => Ok(QueryRunResult {
                connection_failed: false,
                query_failure: None,
                shutdown: false,
            }),
            Err(e) => {
                // If shutdown was requested, exit quietly without logging errors
                if self.shutdown_token.is_cancelled() {
                    return Ok(QueryRunResult {
                        connection_failed: false,
                        query_failure: None,
                        shutdown: true,
                    });
                }

                // Check if this is a connection error using typed error checking
                // This is more reliable than string matching
                let is_connection_error =
                    e.downcast_ref::<flight_client::Error>()
                        .is_some_and(|flight_err| {
                            matches!(
                                flight_err,
                                flight_client::Error::UnableToConnectToServer { .. }
                                    | flight_client::Error::UnableToPerformHandshake { .. }
                            )
                        });

                if is_connection_error {
                    eprintln!(
                        "FAIL - EARLY EXIT - Worker {} - Query '{}' failed: {}",
                        self.id, query.name, e
                    );
                    Ok(QueryRunResult {
                        connection_failed: true,
                        query_failure: None,
                        shutdown: false,
                    })
                } else {
                    eprintln!(
                        "{} FAIL - Worker {} - Query '{}' failed: {}",
                        chrono::Utc::now(),
                        self.id,
                        query.name,
                        e
                    );

                    query_durations.entry(Arc::clone(&query.name)).or_default();
                    Ok(QueryRunResult {
                        connection_failed: false,
                        query_failure: Some(format!("{e}")),
                        shutdown: false,
                    })
                }
            }
        }
    }

    async fn execute_query(
        &self,
        query: &Query,
        query_durations: Arc<DashMap<Arc<str>, Vec<Duration>>>,
        row_counts: &mut BTreeMap<Arc<str>, Vec<usize>>,
        results_snapshot: bool,
        validate: bool,
        checkpoint_validation: &mut Option<CheckpointValidationState>,
    ) -> Result<()> {
        // Race query execution against shutdown signal so in-flight queries
        // are aborted immediately when shutdown is requested.
        let result = tokio::select! {
            biased;
            () = self.shutdown_token.cancelled() => {
                return Err(anyhow::anyhow!("Shutdown requested"));
            }
            result = self.executor.execute(query) => result?,
        };

        // --- Checkpoint validation (worker 0 only) ---
        // This does NOT fail the query; it only records pass/fail outcomes.
        if let Some(cv) = checkpoint_validation
            && cv.active
            && self.executor.supports_validation()
            && let Some(batches) = &result.batches
        {
            cv.validate(query, batches);
        }

        // Handle validation if supported and requested
        if validate
            && self.executor.supports_validation()
            && let Some(batches) = &result.batches
        {
            // Execute reference query if reference_schema is provided
            if let Some(ref_schema) = &self.reference_schema
                && let Some(flight_client) = self.executor.as_flight_client()
            {
                let reference_query = query.rewrite_with_reference_schema(ref_schema)?;
                println!(
                    "Worker {} - Query '{}' - Executing reference query against {}.* tables",
                    self.id, query.name, ref_schema
                );

                let mut ref_result_stream = flight_client
                    .query(reference_query.to_sql_with_inlined_params().as_ref())
                    .await?;

                let mut ref_batches = vec![];
                while let Some(batch) = ref_result_stream.try_next().await? {
                    ref_batches.push(batch);
                }

                // Validate against reference query results
                let validation_result =
                    validation::validate_with_expected_batches(&query.name, batches, &ref_batches)?;

                if let QueryValidationResult::Fail(validation_reason) = validation_result {
                    eprintln!(
                        "\n{} FAIL - Worker {} - Query '{}' reference validation failed",
                        chrono::Utc::now(),
                        self.id,
                        query.name
                    );
                    eprintln!("Query SQL: {}", query.sql);
                    eprintln!("Validation failure reason: {validation_reason:?}");
                    eprintln!("\nExpected results (from reference schema):");
                    match arrow::util::pretty::pretty_format_batches(&ref_batches) {
                        Ok(pretty) => eprintln!("{pretty}"),
                        Err(e) => eprintln!("Failed to format expected batches: {e}"),
                    }
                    eprintln!("\nActual results:");
                    match arrow::util::pretty::pretty_format_batches(batches) {
                        Ok(pretty) => eprintln!("{pretty}"),
                        Err(e) => eprintln!("Failed to format actual batches: {e}"),
                    }
                    eprintln!();
                    return Err(anyhow::anyhow!(
                        "Query reference validation failed: {validation_reason:?}"
                    ));
                }
            }

            // Also validate using existing validation logic (TPCH or custom validation data)
            let validation_result = self.validate_query_results(query, batches)?;

            if let QueryValidationResult::Fail(validation_reason) = validation_result {
                eprintln!(
                    "\n{} FAIL - Worker {} - Query '{}' validation failed",
                    chrono::Utc::now(),
                    self.id,
                    query.name
                );
                eprintln!("Query SQL: {}", query.sql);
                eprintln!("Validation failure reason: {validation_reason:?}");

                // Print expected results based on validation source
                if let Some(validation_data) = &self.validation_data
                    && let Some(expected_batches) = validation_data.get(&query.name)
                {
                    eprintln!("\nExpected results (from custom validation data):");
                    match arrow::util::pretty::pretty_format_batches(expected_batches) {
                        Ok(pretty) => eprintln!("{pretty}"),
                        Err(e) => eprintln!("Failed to format expected batches: {e}"),
                    }
                } else {
                    eprintln!(
                        "\nExpected results: See TPCH specification for query {}",
                        query.name
                    );
                }

                eprintln!("\nActual results:");
                match arrow::util::pretty::pretty_format_batches(batches) {
                    Ok(pretty) => eprintln!("{pretty}"),
                    Err(e) => eprintln!("Failed to format actual batches: {e}"),
                }
                eprintln!();

                return Err(anyhow::anyhow!(
                    "Query validation failed: {validation_reason:?}"
                ));
            }
        }

        // Handle result snapshots if requested
        if results_snapshot && let Some(batches) = &result.batches {
            let query_name = Arc::clone(&query.name);
            let name = self.name.clone();
            let snapshot_name = if (self.scale_factor - 1.0).abs() < f64::EPSILON {
                format!("{name}_{query_name}")
            } else {
                format!("{name}_{query_name}_sf{}", self.scale_factor)
            };

            // Limit to first 10 rows for snapshot
            let mut limited_records = vec![];
            for batch in batches {
                if limited_records.len() >= 10 {
                    break;
                }
                let required_rows = 10 - limited_records.len();
                let end = if batch.num_rows() > required_rows {
                    required_rows
                } else {
                    batch.num_rows()
                };
                for i in 0..end {
                    limited_records.push(batch.slice(i, 1));
                }
            }

            let records_pretty = arrow::util::pretty::pretty_format_batches(&limited_records)?;
            let result = panic::catch_unwind(|| {
                insta::with_settings!({
                     description => format!("Query: {query_name}"),
                                         omit_expression => true,
                    snapshot_path => "../../snapshot/snapshots/results"
                }, {
                    insta::assert_snapshot!(snapshot_name, records_pretty);
                });
            });
            if result.is_err() {
                let error_str = format!("Query `{name}` `{query_name}` snapshot assertion failed",);
                eprintln!("{error_str}");
                return Err(anyhow::anyhow!(error_str));
            }
        }

        // Send streaming metric
        self.send_streaming_metric(&query.name, result.duration, true);

        // Record metrics
        query_durations
            .entry(Arc::clone(&query.name))
            .or_default()
            .push(result.duration);

        row_counts
            .entry(Arc::clone(&query.name))
            .or_default()
            .push(result.row_count);

        if let Some(pb) = self.progress_bar.as_ref() {
            pb.inc(1);
        }

        Ok(())
    }
}

fn default_row_count_validation_skip_queries() -> HashSet<String> {
    [
        "tpcds_q8",
        "tpcds_q29",
        "tpcds_q37",
        "tpcds_q41",
        "tpcds_q44",
        "tpcds_q54",
        "tpcds_q58",
    ]
    .iter()
    .map(std::string::ToString::to_string)
    .collect()
}

/// Build unique query sets by grouping queries by parameter index.
/// Creates one query set per parameter variation, where each set contains
/// one query of each type with the same parameter index.
fn build_unique_query_sets(queries: &[Query]) -> Result<Vec<Vec<Query>>> {
    use std::collections::HashMap;

    // Group queries by name first
    let mut groups: HashMap<Arc<str>, Vec<&Query>> = HashMap::new();
    for query in queries {
        groups
            .entry(Arc::clone(&query.name))
            .or_default()
            .push(query);
    }

    // Validate that all groups have the same size
    let mut expected_size = None;
    for (name, query_group) in &groups {
        let group_size = query_group.len();
        match expected_size {
            None => expected_size = Some(group_size),
            Some(expected) if expected != group_size => {
                return Err(anyhow::anyhow!(
                    "Uneven parameter groups detected: query '{name}' has {group_size} parameters, expected {expected}"
                ));
            }
            _ => {}
        }
    }

    let num_variations = expected_size.unwrap_or(0);

    // Create query sets by parameter index
    let mut result = Vec::with_capacity(num_variations);

    for param_index in 0..num_variations {
        let mut query_set = Vec::with_capacity(groups.len());

        for query_group in groups.values() {
            if let Some(query) = query_group.get(param_index) {
                query_set.push((*query).clone());
            }
        }

        result.push(query_set);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use crate::queries::parameterized::ParameterValue;

    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_build_unique_query_sets_single_group() {
        let queries = vec![
            Query {
                name: Arc::from("query1"),
                sql: Arc::from("SELECT * FROM table WHERE id = ?"),
                overridden: false,
                parameters: Some(vec![ParameterValue::String("1".into())]),
            },
            Query {
                name: Arc::from("query1"),
                sql: Arc::from("SELECT * FROM table WHERE id = ?"),
                overridden: false,
                parameters: Some(vec![ParameterValue::String("2".into())]),
            },
        ];

        let result = build_unique_query_sets(&queries).expect("Should succeed");

        assert_eq!(
            result.len(),
            2,
            "Should have two query sets (one per parameter)"
        );
        assert_eq!(result[0].len(), 1, "Each set should have one query");
        assert_eq!(result[1].len(), 1, "Each set should have one query");
    }

    #[test]
    fn test_build_unique_query_sets_multiple_groups() {
        let queries = vec![
            Query {
                name: Arc::from("query1"),
                sql: Arc::from("SELECT * FROM table1"),
                overridden: false,
                parameters: None,
            },
            Query {
                name: Arc::from("query2"),
                sql: Arc::from("SELECT * FROM table2"),
                overridden: false,
                parameters: None,
            },
            Query {
                name: Arc::from("query1"),
                sql: Arc::from("SELECT * FROM table1 WHERE id = ?"),
                overridden: false,
                parameters: Some(vec![ParameterValue::String("1".into())]),
            },
            Query {
                name: Arc::from("query2"),
                sql: Arc::from("SELECT * FROM table2 WHERE id = ?"),
                overridden: false,
                parameters: Some(vec![ParameterValue::String("2".into())]),
            },
        ];

        let result = build_unique_query_sets(&queries).expect("Should succeed");

        assert_eq!(
            result.len(),
            2,
            "Should have two query sets (one per parameter)"
        );
        for group in &result {
            assert_eq!(
                group.len(),
                2,
                "Each set should have two queries (one per query type)"
            );
        }

        // Verify each set contains one query of each type
        let set1_names: Vec<&str> = result[0].iter().map(|q| q.name.as_ref()).collect();
        let set2_names: Vec<&str> = result[1].iter().map(|q| q.name.as_ref()).collect();
        assert!(set1_names.contains(&"query1") && set1_names.contains(&"query2"));
        assert!(set2_names.contains(&"query1") && set2_names.contains(&"query2"));
    }

    #[test]
    fn test_build_unique_query_sets_unique_names() {
        let queries = vec![
            Query {
                name: Arc::from("query1"),
                sql: Arc::from("SELECT * FROM table1"),
                overridden: false,
                parameters: None,
            },
            Query {
                name: Arc::from("query2"),
                sql: Arc::from("SELECT * FROM table2"),
                overridden: false,
                parameters: None,
            },
        ];

        let result = build_unique_query_sets(&queries).expect("Should succeed");

        assert_eq!(result.len(), 1, "Should have one query set");
        assert_eq!(result[0].len(), 2, "Set should have both queries");

        // Verify we have both query names in the single set
        let names: Vec<&str> = result[0].iter().map(|q| q.name.as_ref()).collect();
        assert!(names.contains(&"query1") && names.contains(&"query2"));
    }
}
