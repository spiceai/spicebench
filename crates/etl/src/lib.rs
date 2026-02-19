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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use data_generation::config::DatasetConfig as GenerationDatasetConfig;
use data_generation::dataset::simple_sequence::SimpleSequenceDataset;
use data_generation::dataset::tpch::TpchDataset;
use data_generation::dataset::{Dataset, MutationConfig};
use data_generation::storage::{BatchOperation, DataStorage};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc as StdArc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use system_adapter_protocol::DatasetConfig as ProtocolDatasetConfig;
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::sink::{InsertOp, Sink};

pub mod sink;

/// Column name appended by the ETL pipeline to every batch.
const CREATED_AT_COLUMN: &str = "__created_at";

/// Internal columns that must be stripped before writing to the sink.
const INTERNAL_COLUMNS: &[&str] = &["__op", "__op_index"];

/// Returns a new schema with the `__created_at` timestamp column appended.
fn schema_with_created_at(schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<_> = schema.fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(
        CREATED_AT_COLUMN,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        true,
    )));
    Arc::new(Schema::new(fields))
}

/// Appends a `__created_at` column (current wall-clock time, microsecond UTC)
/// to the given batch.
fn append_created_at(batch: &RecordBatch) -> anyhow::Result<RecordBatch> {
    let now_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_micros() as i64;

    let timestamps =
        TimestampMicrosecondArray::from(vec![Some(now_us); batch.num_rows()]).with_timezone("UTC");

    let new_schema = schema_with_created_at(&batch.schema());
    let mut columns: Vec<_> = batch.columns().to_vec();
    columns.push(Arc::new(timestamps));

    Ok(RecordBatch::try_new(new_schema, columns)?)
}

/// Removes internal bookkeeping columns (`__op`, `__op_index`) from a
/// [`RecordBatch`] so they are not persisted to the sink.
fn strip_internal_columns(batch: &RecordBatch) -> anyhow::Result<RecordBatch> {
    let schema = batch.schema();
    let indices_to_keep: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !INTERNAL_COLUMNS.contains(&f.name().as_str()))
        .map(|(i, _)| i)
        .collect();

    if indices_to_keep.len() == schema.fields().len() {
        return Ok(batch.clone());
    }

    let new_fields: Vec<_> = indices_to_keep
        .iter()
        .map(|&i| schema.field(i).clone())
        .collect();
    let new_columns: Vec<_> = indices_to_keep
        .iter()
        .map(|&i| batch.column(i).clone())
        .collect();

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(new_fields)),
        new_columns,
    )?)
}

/// Specifies which dataset implementation to use for the ETL pipeline.
#[derive(Debug, Clone)]
pub enum DatasetSource {
    /// A simple auto-incrementing integer sequence dataset.
    SimpleSequence,
    /// The TPC-H benchmark dataset generated via DuckDB.
    Tpch,
}

impl DatasetSource {
    /// Create an [`Arc<dyn Dataset>`] for this source variant using the given
    /// configuration.
    ///
    /// Delegates to the [`Dataset::create`] factory method on the concrete type.
    pub fn create(
        &self,
        config: &GenerationDatasetConfig,
        mutations: &MutationConfig,
    ) -> anyhow::Result<Arc<dyn Dataset>> {
        match self {
            DatasetSource::SimpleSequence => SimpleSequenceDataset::create(config, mutations),
            DatasetSource::Tpch => TpchDataset::create(config, mutations),
        }
    }
}

/// The current state of an [`ETLPipeline`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineState {
    /// The pipeline has been created with a dataset, source, and target but has
    /// not yet started processing.
    NotStarted,
    /// The pipeline has been initialized: the first batch for every table has
    /// been ETL'd into the target so the system adapter can discover initial
    /// data.
    Initialized,
    /// The pipeline is actively rehydrating batches (in order of batch ID) from
    /// the configured [`Source`] into the configured [`Target`].
    Running,
    /// The pipeline has processed the requested number of steps and is waiting
    /// for [`continue_pipeline`](ETLPipeline::continue_pipeline) to be called.
    Paused,
    /// The pipeline has completed, was cancelled, or encountered an error in its
    /// background task.
    Stopped(StopReason),
}

/// Why the pipeline entered the [`PipelineState::Stopped`] state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReason {
    /// All batches for every table were processed successfully.
    Completed,
    /// The pipeline was cancelled via its [`CancellationToken`].
    Cancelled,
    /// The background task encountered an unrecoverable error.
    Error(String),
}

/// Shared mutable state for work remaining in the pipeline.
///
/// This is stored behind an `Arc<StdMutex<...>>` so the spawned background
/// task can hand back unconsumed work when it pauses or finishes.
struct PipelineWorkState {
    /// Remaining steps grouped by batch ID (ascending). Each entry maps a
    /// batch ID to the list of tables that still need to be processed for
    /// that batch.
    steps: BTreeMap<u64, Vec<String>>,
    /// Tables whose data has been fully consumed (source returned `None`).
    finished_tables: HashSet<String>,
}

/// An ETL pipeline that reads batches from [`DataStorage`], rehydrates them
/// using a [`Dataset`], and writes them to a [`Sink`].
///
/// # Lifecycle
///
/// 1. **[`NotStarted`](PipelineState::NotStarted)** — created via [`ETLPipeline::new`]
///    with a dataset, source, and target. Call [`setup_request_datasets`](ETLPipeline::setup_request_datasets)
///    to obtain the dataset configurations that a system adapter needs.
/// 2. **[`Initialized`](PipelineState::Initialized)** — the first batch (batch 0)
///    has been ETL'd into the target via [`initialize`](ETLPipeline::initialize).
///    The system adapter can now discover initial data.
/// 3. **[`Running`](PipelineState::Running)** — the pipeline is actively processing
///    remaining batches (batch 1+).
/// 4. **[`Paused`](PipelineState::Paused)** — the pipeline processed the requested
///    number of steps and is waiting to be resumed via
///    [`continue_pipeline`](ETLPipeline::continue_pipeline).
/// 5. **[`Stopped`](PipelineState::Stopped)** — the pipeline finished, was cancelled,
///    or hit an error.
pub struct ETLPipeline {
    dataset_source: DatasetSource,
    dataset: Arc<dyn Dataset>,
    data_storage: Arc<dyn DataStorage>,
    data_sink: Arc<dyn Sink>,
    state_rx: watch::Receiver<PipelineState>,
    state_tx: Arc<watch::Sender<PipelineState>>,
    cancel_token: CancellationToken,
    handle: Option<JoinHandle<()>>,
    /// How many steps to process per `run` / `continue_pipeline` invocation.
    /// `None` means unlimited (process everything).
    batch_budget: Option<usize>,
    /// Shared work state handed between the pipeline and its background task.
    work_state: Arc<StdMutex<PipelineWorkState>>,
}

impl ETLPipeline {
    /// Creates a new ETL pipeline in the [`PipelineState::NotStarted`] state.
    ///
    /// The `dataset_source` selects which [`Dataset`] implementation to use,
    /// and `config` is forwarded to the [`Dataset::create`] factory method to
    /// build the dataset instance.
    pub fn new(
        dataset_source: DatasetSource,
        config: &GenerationDatasetConfig,
        data_storage: Arc<dyn DataStorage>,
        data_sink: Arc<dyn Sink>,
        mutations: &MutationConfig,
    ) -> anyhow::Result<Self> {
        let dataset = dataset_source.create(config, mutations)?;
        let (state_tx, state_rx) = watch::channel(PipelineState::NotStarted);
        Ok(Self {
            dataset_source,
            dataset,
            data_storage,
            data_sink,
            state_rx,
            state_tx: Arc::new(state_tx),
            cancel_token: CancellationToken::new(),
            handle: None,
            batch_budget: None,
            work_state: Arc::new(StdMutex::new(PipelineWorkState {
                steps: BTreeMap::new(),
                finished_tables: HashSet::new(),
            })),
        })
    }

    /// Returns the current state of the pipeline.
    pub fn state(&self) -> PipelineState {
        self.state_rx.borrow().clone()
    }

    /// Returns a [`watch::Receiver`] that can be used to observe state changes.
    pub fn state_watch(&self) -> watch::Receiver<PipelineState> {
        self.state_rx.clone()
    }

    /// Returns the [`DatasetSource`] variant this pipeline was created with.
    pub fn dataset_source(&self) -> &DatasetSource {
        &self.dataset_source
    }

    /// Returns the underlying [`Dataset`] trait object.
    pub fn dataset(&self) -> &Arc<dyn Dataset> {
        &self.dataset
    }

    /// Returns the [`CancellationToken`] for this pipeline.
    ///
    /// Cancelling this token will cause the background task to stop after the
    /// current batch finishes and transition the pipeline to
    /// [`PipelineState::Stopped(StopReason::Cancelled)`].
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// Cancels the pipeline if it is running.
    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }

    /// Returns the dataset configurations required to set up the system adapter.
    ///
    /// Each entry maps a table name to its
    /// [`DatasetConfig`](system_adapter_protocol::DatasetConfig), which includes
    /// the rehydrated Arrow schema. This can be used to build a
    /// [`SetupRequest`](system_adapter_protocol::SetupRequest) for the system
    /// adapter.
    pub fn setup_request_datasets(&self) -> HashMap<String, ProtocolDatasetConfig> {
        self.dataset
            .tables()
            .into_iter()
            .map(|(name, table)| {
                let config = ProtocolDatasetConfig {
                    schema: schema_with_created_at(&table.schema),
                };
                (name, config)
            })
            .collect()
    }

    /// Initializes the ETL pipeline by processing only the first batch (batch
    /// ID 0) for every table.
    ///
    /// This ensures the target has some initial data before calling
    /// `setup()` on the system adapter. After successful initialization the
    /// pipeline transitions to [`PipelineState::Initialized`].
    ///
    /// Returns an error if the pipeline is not in the [`NotStarted`] state or
    /// if any batch fails to process.
    pub async fn initialize(&mut self) -> anyhow::Result<()> {
        if *self.state_rx.borrow() != PipelineState::NotStarted {
            anyhow::bail!(
                "Cannot initialize pipeline: current state is {:?}",
                *self.state_rx.borrow()
            );
        }

        let tables = self.dataset.tables();
        let first_batch_id = 0u64;

        let mut join_set: JoinSet<Result<String, String>> = JoinSet::new();
        for table_name in tables.keys() {
            let source = Arc::clone(&self.data_storage);
            let target = Arc::clone(&self.data_sink);
            let table_name = table_name.clone();

            join_set.spawn(async move {
                let read_result = source
                    .read_batch(&table_name, first_batch_id)
                    .await
                    .map_err(|e| format!("read {table_name} batch {first_batch_id}: {e}"))?
                    .ok_or_else(|| {
                        format!("No data for table {table_name} at batch {first_batch_id}")
                    })?;

                let op = sink_op_from_batch_op(&read_result.operation);

                for batch in read_result.batches {
                    let stripped = strip_internal_columns(&batch).map_err(|e| {
                        format!("strip internal columns from {table_name} batch {first_batch_id}: {e}")
                    })?;
                    let rehydrated = append_created_at(&stripped).map_err(|e| {
                        format!("append __created_at to {table_name} batch {first_batch_id}: {e}")
                    })?;

                    target
                        .write(&table_name, first_batch_id, rehydrated, op.clone())
                        .await
                        .map_err(|e| format!("write {table_name} batch {first_batch_id}: {e}"))?;
                }

                debug!(
                    table = %table_name,
                    batch_id = first_batch_id,
                    "Initial batch processed"
                );
                Ok(table_name)
            });
        }

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(_table_name)) => {}
                Ok(Err(err_msg)) => {
                    let _ = self
                        .state_tx
                        .send(PipelineState::Stopped(StopReason::Error(err_msg.clone())));
                    anyhow::bail!("ETL initialization failed: {err_msg}");
                }
                Err(e) => {
                    let msg = format!("Task panicked during initialization: {e}");
                    let _ = self
                        .state_tx
                        .send(PipelineState::Stopped(StopReason::Error(msg.clone())));
                    anyhow::bail!("{msg}");
                }
            }
        }

        info!("ETL pipeline initialized with first batch for all tables");
        let _ = self.state_tx.send(PipelineState::Initialized);
        Ok(())
    }

    /// Starts the ETL pipeline, transitioning from [`PipelineState::Initialized`]
    /// to [`PipelineState::Running`].
    ///
    /// Spawns a background tokio task that iterates over every table and
    /// processes batch IDs in ascending order, skipping batch 0 which was
    /// already processed during [`initialize`](ETLPipeline::initialize). For
    /// each batch the task:
    ///
    /// 1. Reads the batch from the [`Source`].
    /// 2. Appends the `__created_at` timestamp column.
    /// 3. Writes the enriched batch to the [`Sink`].
    ///
    /// The task transitions to [`PipelineState::Stopped`] when all batches are
    /// processed, the [`CancellationToken`] is triggered, or an error occurs.
    ///
    /// Returns an error if the pipeline is not in the [`Initialized`] state.
    pub fn start(&mut self) -> anyhow::Result<()> {
        let current_state = self.state_rx.borrow().clone();
        if current_state != PipelineState::Initialized {
            anyhow::bail!(
                "Cannot start pipeline: current state is {:?} (must be Initialized)",
                current_state
            );
        }

        self.batch_budget = None;
        self.build_work_plan();
        self.spawn_run_task(None);
        Ok(())
    }

    /// Starts the ETL pipeline and processes at most `step_count` steps (batch
    /// ID groups) before transitioning to [`PipelineState::Paused`].
    ///
    /// Each step processes all active tables for a single batch ID
    /// concurrently. After `step_count` steps the pipeline pauses and can be
    /// resumed by calling [`continue_pipeline`](ETLPipeline::continue_pipeline),
    /// which will process another `step_count` steps.
    ///
    /// If there are fewer remaining steps than `step_count`, all remaining
    /// steps are processed and the pipeline transitions directly to
    /// [`PipelineState::Stopped(StopReason::Completed)`].
    ///
    /// Returns an error if the pipeline is not in the [`Initialized`] state.
    pub fn run(&mut self, step_count: usize) -> anyhow::Result<()> {
        let current_state = self.state_rx.borrow().clone();
        if current_state != PipelineState::Initialized {
            anyhow::bail!(
                "Cannot run pipeline: current state is {:?} (must be Initialized)",
                current_state
            );
        }

        self.batch_budget = Some(step_count);
        self.build_work_plan();
        self.spawn_run_task(Some(step_count));
        Ok(())
    }

    /// Resumes a paused pipeline for another batch of steps.
    ///
    /// The pipeline processes up to the same `step_count` that was originally
    /// passed to [`run`](ETLPipeline::run). If all remaining steps are
    /// consumed, the pipeline transitions to
    /// [`PipelineState::Stopped(StopReason::Completed)`] instead of
    /// [`PipelineState::Paused`].
    ///
    /// Returns an error if the pipeline is not in the [`Paused`] state.
    pub fn continue_pipeline(&mut self) -> anyhow::Result<()> {
        let current_state = self.state_rx.borrow().clone();
        if current_state != PipelineState::Paused {
            anyhow::bail!(
                "Cannot continue pipeline: current state is {:?} (must be Paused)",
                current_state
            );
        }

        // Wait for the previous background task to finish (it should already
        // be done since it transitioned to Paused).
        if let Some(handle) = self.handle.take() {
            // The task should already be finished, but drop the handle cleanly.
            handle.abort();
        }

        self.spawn_run_task(self.batch_budget);
        Ok(())
    }

    /// Build the initial work plan from the dataset and store it in
    /// `self.work_state`.
    fn build_work_plan(&self) {
        let dataset = &self.dataset;
        let tables = dataset.tables();
        let mut steps: BTreeMap<u64, Vec<String>> = BTreeMap::new();

        for name in tables.keys() {
            for id in dataset.batch_ids(name) {
                // Skip batch 0 — it was already processed during initialize().
                if id == 0 {
                    continue;
                }
                steps.entry(id).or_default().push(name.clone());
            }
        }

        let mut state = self.work_state.lock().expect("work_state lock poisoned");
        state.steps = steps;
        state.finished_tables.clear();
    }

    /// Spawn the background task that processes steps from the shared work
    /// state. If `step_limit` is `Some(n)`, at most `n` steps are processed
    /// before the pipeline transitions to [`PipelineState::Paused`].
    fn spawn_run_task(&mut self, step_limit: Option<usize>) {
        let _ = self.state_tx.send(PipelineState::Running);

        let source = Arc::clone(&self.data_storage);
        let target = Arc::clone(&self.data_sink);
        let cancel = self.cancel_token.clone();
        let state_tx = Arc::clone(&self.state_tx);
        let work_state = Arc::clone(&self.work_state);

        let handle = tokio::spawn(async move {
            let outcome = run_pipeline(source, target, work_state, cancel, step_limit).await;
            let _ = state_tx.send(outcome);
        });

        self.handle = Some(handle);
    }

    /// Waits for the pipeline background task to finish and returns the final
    /// [`PipelineState`].
    ///
    /// If the pipeline has not been started, this returns immediately with the
    /// current state.
    pub async fn wait(&mut self) -> PipelineState {
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
        self.state()
    }
}

/// Core loop executed inside the spawned task.
///
/// Processes steps from the shared work state, removing each step as it is
/// consumed. If `step_limit` is `Some(n)`, at most `n` steps are processed
/// before the function returns [`PipelineState::Paused`]. Unconsumed steps
/// remain in the shared work state for a subsequent call.
async fn run_pipeline(
    data_storage: Arc<dyn DataStorage>,
    data_sink: Arc<dyn Sink>,
    work_state: Arc<StdMutex<PipelineWorkState>>,
    cancel: CancellationToken,
    step_limit: Option<usize>,
) -> PipelineState {
    // Take a snapshot of total counts for logging.
    let (total_steps, total_batches) = {
        let state = work_state.lock().expect("work_state lock poisoned");
        let total_steps = state.steps.len();
        let total_batches: usize = state.steps.values().map(|v| v.len()).sum();
        (total_steps, total_batches)
    };

    let limit_label = step_limit
        .map(|n| format!("{n}"))
        .unwrap_or_else(|| "unlimited".to_string());
    info!(
        total_steps,
        total_batches,
        step_limit = %limit_label,
        "ETL pipeline run started"
    );

    // Shared progress counters for periodic logging.
    let steps_completed = StdArc::new(AtomicU64::new(0));
    let batches_processed = StdArc::new(AtomicU64::new(0));
    let tables_finished_counter = StdArc::new(AtomicU64::new(0));
    let pipeline_start = Instant::now();

    // Spawn periodic progress logger (every 5 seconds).
    let progress_logger = {
        let steps_completed = StdArc::clone(&steps_completed);
        let batches_processed = StdArc::clone(&batches_processed);
        let tables_finished_counter = StdArc::clone(&tables_finished_counter);
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let elapsed = pipeline_start.elapsed();
                        let secs = elapsed.as_secs_f64();
                        if secs < 0.001 {
                            continue;
                        }
                        let steps_done = steps_completed.load(Ordering::Relaxed);
                        let batches_done = batches_processed.load(Ordering::Relaxed);
                        let tables_done = tables_finished_counter.load(Ordering::Relaxed);
                        info!(
                            elapsed_secs = format!("{secs:.1}"),
                            steps = format!("{steps_done}/{total_steps}"),
                            batches = format!("{batches_done}/{total_batches}"),
                            tables_finished = tables_done,
                            batches_per_sec = format!("{:.1}", batches_done as f64 / secs),
                            "ETL progress"
                        );
                    }
                    () = cancel.cancelled() => break,
                }
            }
        })
    };

    let mut steps_processed: usize = 0;

    loop {
        // Check step budget.
        if let Some(limit) = step_limit {
            if steps_processed >= limit {
                info!(steps_processed, "Step limit reached, pausing pipeline");
                progress_logger.abort();
                return PipelineState::Paused;
            }
        }

        if cancel.is_cancelled() {
            warn!("ETL pipeline cancelled after {steps_processed} steps");
            progress_logger.abort();
            return PipelineState::Stopped(StopReason::Cancelled);
        }

        // Pop the next step from the shared work state.
        let next_step = {
            let mut state = work_state.lock().expect("work_state lock poisoned");
            if let Some(entry) = state.steps.first_entry() {
                let batch_id = *entry.key();
                let tables = entry.remove();
                // Filter out already-finished tables.
                let active: Vec<String> = tables
                    .into_iter()
                    .filter(|t| !state.finished_tables.contains(t))
                    .collect();
                Some((batch_id, active))
            } else {
                None
            }
        };

        let (batch_id, active_tables) = match next_step {
            Some((_bid, tables)) if tables.is_empty() => {
                // All tables in this step are already finished, skip it.
                continue;
            }
            Some((bid, tables)) => (bid, tables),
            None => {
                // No more work — pipeline is done.
                break;
            }
        };

        // Process all tables for this batch_id concurrently.
        let mut join_set: JoinSet<Result<(String, bool), String>> = JoinSet::new();
        for table_name in active_tables {
            let data_storage = Arc::clone(&data_storage);
            let data_sink = Arc::clone(&data_sink);

            join_set.spawn(async move {
                // 1. Read from source
                let read_result = match data_storage.read_batch(&table_name, batch_id).await {
                    Ok(Some(r)) => r,
                    Ok(None) => {
                        debug!(
                            table = %table_name,
                            batch_id,
                            "No more batches for table, marking as finished"
                        );
                        return Ok((table_name, true)); // mark as finished
                    }
                    Err(e) => {
                        error!(
                            table = %table_name,
                            batch_id,
                            error = %e,
                            "Failed to read batch from source"
                        );
                        return Err(format!("read {table_name} batch {batch_id}: {e}"));
                    }
                };

                let op = sink_op_from_batch_op(&read_result.operation);

                // 2. Strip internal columns, append __created_at, and write to target
                for batch in read_result.batches {
                    let stripped = match strip_internal_columns(&batch) {
                        Ok(b) => b,
                        Err(e) => {
                            error!(
                                table = %table_name,
                                batch_id,
                                error = %e,
                                "Failed to strip internal columns"
                            );
                            return Err(format!(
                                "strip internal columns from {table_name} batch {batch_id}: {e}"
                            ));
                        }
                    };
                    let rehydrated = match append_created_at(&stripped) {
                        Ok(b) => b,
                        Err(e) => {
                            error!(
                                table = %table_name,
                                batch_id,
                                error = %e,
                                "Failed to append __created_at column"
                            );
                            return Err(format!(
                                "append __created_at to {table_name} batch {batch_id}: {e}"
                            ));
                        }
                    };

                    // 3. Write to sink
                    if let Err(e) = data_sink
                        .write(&table_name, batch_id, rehydrated, op.clone())
                        .await
                    {
                        error!(
                            table = %table_name,
                            batch_id,
                            error = %e,
                            "Failed to write batch to target"
                        );
                        return Err(format!("write {table_name} batch {batch_id}: {e}"));
                    }
                }

                debug!(
                    table = %table_name,
                    batch_id,
                    "Table batch processed"
                );
                Ok((table_name, false)) // not finished
            });
        }

        // Collect results from all concurrent table tasks in this step.
        let mut step_batch_count: u64 = 0;
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok((table_name, is_finished))) => {
                    step_batch_count += 1;
                    if is_finished {
                        let mut state = work_state.lock().expect("work_state lock poisoned");
                        state.finished_tables.insert(table_name);
                        tables_finished_counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Ok(Err(err_msg)) => {
                    progress_logger.abort();
                    return PipelineState::Stopped(StopReason::Error(err_msg));
                }
                Err(e) => {
                    progress_logger.abort();
                    return PipelineState::Stopped(StopReason::Error(format!(
                        "Task panicked: {e}"
                    )));
                }
            }
        }

        steps_processed += 1;
        steps_completed.fetch_add(1, Ordering::Relaxed);
        batches_processed.fetch_add(step_batch_count, Ordering::Relaxed);

        debug!(batch_id, steps_processed, "Step completed");
    }

    progress_logger.abort();
    info!(
        elapsed = ?pipeline_start.elapsed(),
        steps_processed,
        "ETL pipeline completed successfully"
    );
    PipelineState::Stopped(StopReason::Completed)
}

fn sink_op_from_batch_op(op: &BatchOperation) -> InsertOp {
    match op {
        BatchOperation::Insert => InsertOp::Insert,
        BatchOperation::Update { key_columns } => InsertOp::Update {
            key_columns: key_columns.clone(),
        },
        BatchOperation::Delete { key_columns } => InsertOp::Delete {
            key_columns: key_columns.clone(),
        },
    }
}
