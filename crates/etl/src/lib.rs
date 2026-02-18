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

use data_generation::config::DatasetConfig as GenerationDatasetConfig;
use data_generation::dataset::simple_sequence::SimpleSequenceDataset;
use data_generation::dataset::tpch::TpchDataset;
use data_generation::dataset::Dataset;
use data_generation::source::Source;
use data_generation::target::Target;
use system_adapter_protocol::{DatasetConfig as ProtocolDatasetConfig, EtlType};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

type DynSource = Arc<dyn Source>;
type DynTarget = Arc<dyn Target>;

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
    ) -> anyhow::Result<Arc<dyn Dataset>> {
        match self {
            DatasetSource::SimpleSequence => SimpleSequenceDataset::create(config),
            DatasetSource::Tpch => TpchDataset::create(config),
        }
    }
}

/// The current state of an [`ETLPipeline`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineState {
    /// The pipeline has been created with a dataset, source, and target but has
    /// not yet started processing.
    NotStarted,
    /// The pipeline is actively rehydrating batches (in order of batch ID) from
    /// the configured [`Source`] into the configured [`Target`].
    Running,
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

/// An ETL pipeline that reads batches from a [`Source`], rehydrates them using a
/// [`Dataset`], and writes them to a [`Target`].
///
/// # Lifecycle
///
/// 1. **[`NotStarted`](PipelineState::NotStarted)** — created via [`ETLPipeline::new`]
///    with a dataset, source, and target. Call [`setup_request_datasets`](ETLPipeline::setup_request_datasets)
///    to obtain the dataset configurations that a system adapter needs.
/// 2. **[`Running`](PipelineState::Running)** — the pipeline is actively processing
///    batches.
/// 3. **[`Stopped`](PipelineState::Stopped)** — the pipeline finished, was cancelled,
///    or hit an error.
pub struct ETLPipeline {
    dataset_source: DatasetSource,
    dataset: Arc<dyn Dataset>,
    source: DynSource,
    target: DynTarget,
    state_rx: watch::Receiver<PipelineState>,
    state_tx: Arc<watch::Sender<PipelineState>>,
    cancel_token: CancellationToken,
    handle: Option<JoinHandle<()>>,
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
        source: DynSource,
        target: DynTarget,
    ) -> anyhow::Result<Self> {
        let dataset = dataset_source.create(config)?;
        let (state_tx, state_rx) = watch::channel(PipelineState::NotStarted);
        Ok(Self {
            dataset_source,
            dataset,
            source,
            target,
            state_rx,
            state_tx: Arc::new(state_tx),
            cancel_token: CancellationToken::new(),
            handle: None,
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
    /// the rehydrated Arrow schema and the ETL type. This can be used to build a
    /// [`SetupRequest`](system_adapter_protocol::SetupRequest) for the system
    /// adapter.
    pub fn setup_request_datasets(&self) -> HashMap<String, ProtocolDatasetConfig> {
        self.dataset
            .tables()
            .into_iter()
            .map(|(name, table)| {
                let config = ProtocolDatasetConfig {
                    etl_type: EtlType::S3,
                    schema: table.rehydrated_schema(),
                    params: HashMap::new(),
                };
                (name, config)
            })
            .collect()
    }

    /// Starts the ETL pipeline, transitioning from [`PipelineState::NotStarted`]
    /// to [`PipelineState::Running`].
    ///
    /// Spawns a background tokio task that iterates over every table and
    /// processes batch IDs in ascending order. For each batch the task:
    ///
    /// 1. Reads the batch from the [`Source`].
    /// 2. Rehydrates it through the [`Dataset`] (appending time columns, etc.).
    /// 3. Writes the rehydrated batch to the [`Target`].
    ///
    /// The task transitions to [`PipelineState::Stopped`] when all batches are
    /// processed, the [`CancellationToken`] is triggered, or an error occurs.
    ///
    /// Returns an error if the pipeline is not in the [`NotStarted`] state.
    pub fn start(&mut self) -> anyhow::Result<()> {
        if *self.state_rx.borrow() != PipelineState::NotStarted {
            anyhow::bail!(
                "Cannot start pipeline: current state is {:?}",
                *self.state_rx.borrow()
            );
        }

        let _ = self.state_tx.send(PipelineState::Running);

        let dataset = Arc::clone(&self.dataset);
        let source = Arc::clone(&self.source);
        let target = Arc::clone(&self.target);
        let cancel = self.cancel_token.clone();
        let state_tx = Arc::clone(&self.state_tx);

        // Build the ordered work plan: Vec<(table_name, batch_id)> sorted by
        // batch_id so all tables advance together.
        let tables = dataset.tables();
        let mut work: Vec<(String, u64)> = Vec::new();
        for (name, _) in &tables {
            for id in dataset.batch_ids(name) {
                work.push((name.clone(), id));
            }
        }
        work.sort_by_key(|(_, id)| *id);

        let handle = tokio::spawn(async move {
            let reason = run_pipeline(dataset, source, target, work, cancel).await;
            let _ = state_tx.send(PipelineState::Stopped(reason));
        });

        self.handle = Some(handle);
        Ok(())
    }

    /// Waits for the pipeline background task to finish and returns the final
    /// [`PipelineState`].
    ///
    /// If the pipeline has not been started, this returns immediately with the
    /// current state.
    pub async fn wait(mut self) -> PipelineState {
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
        self.state()
    }
}

/// Core loop executed inside the spawned task.
///
/// Processes `work` items sequentially, checking for cancellation between each
/// batch.
async fn run_pipeline(
    dataset: Arc<dyn Dataset>,
    source: DynSource,
    target: DynTarget,
    work: Vec<(String, u64)>,
    cancel: CancellationToken,
) -> StopReason {
    let total = work.len();
    info!(total_batches = total, "ETL pipeline started");

    for (idx, (table_name, batch_id)) in work.into_iter().enumerate() {
        if cancel.is_cancelled() {
            warn!("ETL pipeline cancelled at batch {idx}/{total}");
            return StopReason::Cancelled;
        }

        // 1. Read from source
        let read_result = match source.read_batch(&table_name, batch_id).await {
            Ok(r) => r,
            Err(e) => {
                error!(
                    table = %table_name,
                    batch_id,
                    error = %e,
                    "Failed to read batch from source"
                );
                return StopReason::Error(format!(
                    "read {table_name} batch {batch_id}: {e}"
                ));
            }
        };

        // 2. Rehydrate each record batch and write to target
        for batch in read_result.batches {
            let rehydrated = match dataset.rehydrate(&table_name, &batch) {
                Ok(b) => b,
                Err(e) => {
                    error!(
                        table = %table_name,
                        batch_id,
                        error = %e,
                        "Failed to rehydrate batch"
                    );
                    return StopReason::Error(format!(
                        "rehydrate {table_name} batch {batch_id}: {e}"
                    ));
                }
            };

            // 3. Write to target
            if let Err(e) = target.write(&table_name, batch_id, rehydrated).await {
                error!(
                    table = %table_name,
                    batch_id,
                    error = %e,
                    "Failed to write batch to target"
                );
                return StopReason::Error(format!(
                    "write {table_name} batch {batch_id}: {e}"
                ));
            }
        }

        info!(
            table = %table_name,
            batch_id,
            progress = format!("{}/{}", idx + 1, total),
            "Batch processed"
        );
    }

    info!("ETL pipeline completed successfully");
    StopReason::Completed
}

