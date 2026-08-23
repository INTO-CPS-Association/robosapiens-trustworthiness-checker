//! Stream-driven runtime adapter for the synchronous dataflow monitor.
//!
//! # What the runtime is
//!
//! [`crate::dataflow::DataflowMonitor`] is the interpreter proper. It is a compiled, stateful
//! synchronous machine: one call to [`DataflowMonitor::evaluate`] consumes one complete logical
//! input row and immediately produces one complete output row. It does not own an input source,
//! create output streams, buffer results, or drive itself asynchronously.
//!
//! [`DataflowRuntime`] supplies that application-facing machinery. It owns:
//!
//! | component | role |
//! |:----------|:-----|
//! | [`InputStream<Value>`] | Asynchronously supplies transport batches containing one or more logical ticks. |
//! | [`DataflowMonitor`] | Compiles and evaluates the specification, retaining all language state between ticks. |
//! | `DirectDataflowEngine` | Direct adapter that retains complete monitor rows in row-major batches. |
//! | [`OutputWriter`] | Receives validated packed row batches for one stable monitor layout. |
//! | [`ExecutionPolicy`] | Selects when accumulated output values cross the sink boundary. |
//!
//! The runtime is therefore an adapter around the monitor, not a second interpreter. Input
//! batching, channel buffering, and asynchronous sink delivery may change transport granularity and
//! backpressure, but they do not add logical ticks or alter monitor semantics. See
//! [`crate::dataflow`] for compilation, scheduling, temporal state, dynamic expressions, and
//! type specialization.
//!
//! ## Construction
//!
//! [`DataflowRuntimeBuilder<S>`] accepts any model type for which `DataflowMonitor: TryFrom<S>`.
//! Passing a [`crate::DsrvSpecification`] selects untyped dataflow compilation; passing a
//! [`crate::CheckedDsrvSpecification`] selects the checked path and enables type-directed scalar
//! specialization. The builder requires an input stream and an already-open [`OutputWriter`].
//! `build` stores the compilation result in the runtime, so a compilation failure is returned when
//! [`Runtime::run`] begins.
//!
//! The builder's executor setting is intentionally unused: this runtime does not spawn a separate
//! interpreter worker. `run` cooperatively polls the dataflow engine and output-writer futures in
//! the caller's local executor. [`DataflowRuntimeBuilder::execution_policy`] chooses buffering
//! behavior. [`DataflowRuntimeBuilder::controlled_input`] wraps an input stream with an
//! [`crate::io::InputController`] and selects synchronous flushing so control acknowledgements align
//! with processed logical ticks.
//!
//! # End-to-end flow
//!
//! When `run` starts, the direct engine sends packed row batches using the monitor's
//! output-variable order. No per-variable channels or compatibility adapters are created.
//!
//! The engine performs this loop:
//!
//! 1. Await the next [`crate::core::InputBatch`].
//! 2. Visit its logical ticks in order.
//! 3. Write the tick's values into a reusable monitor input row, leaving omitted inputs as
//!    [`Value::NoVal`].
//! 4. Call `DataflowMonitor::evaluate` exactly once.
//! 5. Reset the supplied input slots to `NoVal`.
//! 6. Append each successful output row contiguously in monitor order.
//! 7. Flush according to the selected execution policy.
//!
//! The input-row and output-row allocations are reused across ticks. Variable layouts are cached as
//! input-slot indices, so repeated update shapes do not repeat name lookup. Undeclared input names
//! are runtime errors. Width-one updates and simultaneous ticks retain their logical boundaries;
//! packed segments keep their row boundaries until this evaluator boundary.
//!
//! # Output buffering and asynchronous delivery
//!
//! The direct adapter appends each complete output row to one contiguous value buffer and
//! sends it as a packed `OutputBatch`. Every successful monitor evaluation contributes
//! exactly one complete row in the monitor's output order. A failed evaluation contributes
//! no row, and buffered rows not yet flushed are discarded on that error.
//!
//! ## Flush policies
//!
//! | policy | when buffers flush | intended effect |
//! |:-------|:-------------------|:----------------|
//! | [`ExecutionPolicy::Buffered`] (default) | After 256 logical ticks, plus a final non-empty partial batch at input EOF. | Amortize output-writer overhead across many values. |
//! | [`ExecutionPolicy::Synchronous`] | After every logical tick. | Send that tick's output batch to the writer before polling the next input tick. |
//!
//! Synchronous policy sends one complete row to the writer at a time. A successful asynchronous
//! send does not necessarily mean the external sink has persisted or consumed the value.
//!
//! ## Backpressure
//!
//! A writer applies backpressure through its sink while accepting complete row batches. A
//! sufficiently slow output sink therefore suspends the engine and eventually stops further input
//! polling. The monitor itself remains a synchronous row evaluator; backpressure belongs entirely
//! to this adapter.
//!
//! ## Completion, shutdown, and errors
//!
//! The direct engine sends full packed row batches, performs a final writer flush, and
//! explicitly closes the writer. `OutputError::Closed` means intentional early completion;
//! other writer, input, and monitor errors are returned. Rows accumulated before a monitor
//! or input error are not emitted.
//!
//! Values accumulated since the previous completed flush may be lost on an error. EOF does not
//! synthesize extra ticks, so delayed monitor values are not drained after the final input row.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::Arc;

use crate::core::{
    ExecutionPolicy, InputBatch, InputStream, OutputBatch, OutputError, OutputWriter, Runtime,
    Specification, Value,
};
#[cfg(feature = "jit")]
use crate::dataflow::JitConfig;
use crate::dataflow::{ContextTransferPolicy, DataflowCompilationError, DataflowMonitor};
use crate::io::reconfigurable_input::{
    ReconfigurableInput, ReconfigurableInputItem, ReconfigurableInputStream,
};
use crate::io::{InputPipeline, MonitorConfig, OutputBackendBuilder};
use crate::runtime::builder::RuntimeBuilder;
use anyhow::Context as _;
use async_trait::async_trait;
use futures::StreamExt;
use futures::future::LocalBoxFuture;
use smol::LocalExecutor;
use tracing::info;

const DATAFLOW_RUNTIME_BATCH_SIZE: usize = 256;

/// Acknowledgement published after a root reconfiguration has reached its new
/// input/output cutover boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReconfigurationAck {
    pub revision: crate::dataflow::RevisionId,
    pub interface_epoch: crate::dataflow::InterfaceEpoch,
    pub applied: bool,
}

/// Releases producers waiting at the global reconfiguration command barrier.
pub type ReconfigurationAckSink = async_unsync::bounded::Sender<ReconfigurationAck>;

type ReconfigurationCompiler = Rc<dyn Fn(&str) -> anyhow::Result<CompiledReconfiguration>>;

struct CompiledReconfiguration {
    monitor: DataflowMonitor,
    input_vars: BTreeSet<crate::VarName>,
    output_vars: BTreeSet<crate::VarName>,
    auxiliary_vars: BTreeSet<crate::VarName>,
}

struct ReconfigurationRuntimeState {
    input: ReconfigurableInput<Value>,
    active_input: crate::io::config::ResolvedInput,
    active_input_config: Option<MonitorConfig>,
    output_builder: OutputBackendBuilder<Value>,
    active_output: crate::io::output::ResolvedOutput,
    compiler: ReconfigurationCompiler,
    transfer_policy: ContextTransferPolicy,
    revision: crate::dataflow::RevisionId,
    interface_epoch: crate::dataflow::InterfaceEpoch,
    acknowledgements: Option<ReconfigurationAckSink>,
}

enum DataflowInput {
    Standard(InputStream<Value>),
    Reconfigurable(ReconfigurableInputStream<Value>),
}

/// Owns and asynchronously drives one compiled dataflow monitor.
pub struct DataflowRuntime {
    input_stream: DataflowInput,
    output_writer: Option<OutputWriter<Value>>,
    monitor: Result<DataflowMonitor, anyhow::Error>,
    execution_policy: ExecutionPolicy,
    reconfiguration: Option<ReconfigurationRuntimeState>,
    startup_error: Option<anyhow::Error>,
}

/// Configures the model, input stream, output writer, and flush policy for a
/// [`DataflowRuntime`].
pub struct DataflowRuntimeBuilder<S>
where
    S: 'static,
    DataflowMonitor: TryFrom<S, Error = DataflowCompilationError>,
{
    model: Option<S>,
    input: Option<InputStream<Value>>,
    output_writer: Option<OutputWriter<Value>>,
    execution_policy: ExecutionPolicy,
    quickening: bool,
    #[cfg(feature = "jit")]
    jit_config: Option<JitConfig>,
}

impl<S> DataflowRuntimeBuilder<S>
where
    S: 'static,
    DataflowMonitor: TryFrom<S, Error = DataflowCompilationError>,
{
    /// Select when completed monitor rows are flushed to the output writer.
    pub fn execution_policy(self, execution_policy: ExecutionPolicy) -> Self {
        Self {
            execution_policy,
            ..self
        }
    }

    /// Enable or disable scheduler-plan quickening. Quickening is enabled by default.
    pub fn quickening(self, enabled: bool) -> Self {
        Self {
            quickening: enabled,
            ..self
        }
    }

    /// Send monitor rows directly to an already-open output writer.
    pub fn output_writer(self, output_writer: OutputWriter<Value>) -> Self {
        Self {
            output_writer: Some(output_writer),
            ..self
        }
    }

    /// Enable the optional native tier on the checked dataflow monitor constructed by this
    /// runtime. The runtime adapter, buffering policy, and output path are otherwise unchanged.
    #[cfg(feature = "jit")]
    pub fn jit(self, config: JitConfig) -> Self {
        Self {
            jit_config: Some(config),
            ..self
        }
    }

    /// Wrap an input stream with tick control and select synchronous output flushing.
    ///
    /// The returned controller acknowledges progress at the runtime's logical-tick
    /// boundary; output sinks may still consume values asynchronously.
    pub fn controlled_input(self, input: InputStream<Value>) -> (Self, crate::io::InputController) {
        let (input, controller) = crate::io::controlled(input);
        (
            self.execution_policy(ExecutionPolicy::Synchronous)
                .input(input),
            controller,
        )
    }
}

/// Configures a dataflow runtime whose input and output sessions are replaced at
/// validated root-command boundaries.
pub struct ReconfigurableDataflowRuntimeBuilder<S>
where
    S: 'static,
    DataflowMonitor: TryFrom<S, Error = DataflowCompilationError>,
{
    executor: Option<Rc<LocalExecutor<'static>>>,
    model: Option<S>,
    input_pipeline: Option<InputPipeline<Value>>,
    output_builder: Option<OutputBackendBuilder<Value>>,
    reconf_topic: Option<String>,
    parse_spec: Option<fn(&str) -> anyhow::Result<S>>,
    execution_policy: ExecutionPolicy,
    quickening: bool,
    transfer_policy: ContextTransferPolicy,
    acknowledgements: Option<ReconfigurationAckSink>,
    setup_error: Option<String>,
    #[cfg(feature = "jit")]
    jit_config: Option<JitConfig>,
}

impl<S> ReconfigurableDataflowRuntimeBuilder<S>
where
    S: Specification + 'static,
    DataflowMonitor: TryFrom<S, Error = DataflowCompilationError>,
{
    pub fn parse_spec(mut self, parse_spec: fn(&str) -> anyhow::Result<S>) -> Self {
        self.parse_spec = Some(parse_spec);
        self
    }

    pub fn input_pipeline(mut self, input_pipeline: InputPipeline<Value>) -> Self {
        self.input_pipeline = Some(input_pipeline);
        self
    }

    pub fn output_builder(mut self, output_builder: OutputBackendBuilder<Value>) -> Self {
        self.output_builder = Some(output_builder);
        self
    }

    pub fn reconf_topic(mut self, reconf_topic: impl Into<String>) -> Self {
        self.reconf_topic = Some(reconf_topic.into());
        self
    }

    pub(crate) fn setup_error(mut self, error: impl Into<String>) -> Self {
        self.setup_error = Some(error.into());
        self
    }

    pub fn context_transfer(mut self, policy: ContextTransferPolicy) -> Self {
        self.transfer_policy = policy;
        self
    }

    pub fn execution_policy(mut self, policy: ExecutionPolicy) -> Self {
        self.execution_policy = policy;
        self
    }

    /// Enable or disable scheduler-plan quickening. Quickening is enabled by default.
    pub fn quickening(mut self, enabled: bool) -> Self {
        self.quickening = enabled;
        self
    }

    pub fn acknowledgements(mut self, sink: ReconfigurationAckSink) -> Self {
        self.acknowledgements = Some(sink);
        self
    }

    #[cfg(feature = "jit")]
    pub fn jit(mut self, config: JitConfig) -> Self {
        self.jit_config = Some(config);
        self
    }
}

impl<S> RuntimeBuilder<S, Value> for ReconfigurableDataflowRuntimeBuilder<S>
where
    S: Specification + 'static,
    DataflowMonitor: TryFrom<S, Error = DataflowCompilationError>,
{
    type Runtime = DataflowRuntime;

    fn new() -> Self {
        Self {
            executor: None,
            model: None,
            input_pipeline: None,
            output_builder: None,
            reconf_topic: None,
            parse_spec: None,
            execution_policy: ExecutionPolicy::Synchronous,
            quickening: true,
            transfer_policy: ContextTransferPolicy::Compatible,
            acknowledgements: None,
            setup_error: None,
            #[cfg(feature = "jit")]
            jit_config: None,
        }
    }

    fn executor(mut self, executor: Rc<LocalExecutor<'static>>) -> Self {
        self.executor = Some(executor);
        self
    }

    fn model(mut self, model: S) -> Self {
        self.model = Some(model);
        self
    }

    fn input(self, _input: InputStream<Value>) -> Self {
        self.setup_error(
            "reconfigurable dataflow runtime requires an InputPipeline, not a direct InputStream",
        )
    }

    fn output_writer(self, _writer: OutputWriter<Value>) -> Self {
        self.setup_error(
            "reconfigurable dataflow runtime requires an OutputBackendBuilder, not a direct OutputWriter",
        )
    }

    fn build(self) -> LocalBoxFuture<'static, Self::Runtime> {
        Box::pin(async move {
            let policy = self.execution_policy;
            if let Some(error) = self.setup_error {
                return failed_dataflow_runtime(policy, anyhow::anyhow!(error));
            }
            let Some(executor) = self.executor else {
                return failed_dataflow_runtime(
                    policy,
                    anyhow::anyhow!("reconfigurable dataflow runtime executor is not configured"),
                );
            };
            let Some(model) = self.model else {
                return failed_dataflow_runtime(
                    policy,
                    anyhow::anyhow!("reconfigurable dataflow runtime model is not configured"),
                );
            };
            let Some(parse_spec) = self.parse_spec else {
                return failed_dataflow_runtime(
                    policy,
                    anyhow::anyhow!("reconfigurable dataflow runtime parser is not configured"),
                );
            };
            let Some(input_pipeline) = self.input_pipeline else {
                return failed_dataflow_runtime(
                    policy,
                    anyhow::anyhow!("reconfigurable dataflow runtime requires an InputPipeline"),
                );
            };
            let Some(output_builder) = self.output_builder else {
                return failed_dataflow_runtime(
                    policy,
                    anyhow::anyhow!(
                        "reconfigurable dataflow runtime requires an OutputBackendBuilder"
                    ),
                );
            };

            let input = match ReconfigurableInput::new(input_pipeline, self.reconf_topic) {
                Ok(input) => input,
                Err(error) => return failed_dataflow_runtime(policy, error),
            };
            let input_vars = model.input_vars();
            let output_vars = model.output_vars();
            let auxiliary_vars = model.aux_vars();
            let resolved_input = match input.pipeline().resolve(&input_vars, None) {
                Ok(resolved) => resolved,
                Err(error) => return failed_dataflow_runtime(policy, error),
            };
            let output_builder = output_builder.executor(executor.clone());
            let resolved_output = match output_builder.resolve(&output_vars, &auxiliary_vars, None)
            {
                Ok(resolved) => resolved,
                Err(error) => return failed_dataflow_runtime(policy, error),
            };

            let mut compiled = match compile_model(model) {
                Ok(compiled) => compiled,
                Err(error) => return failed_dataflow_runtime(policy, error.into()),
            };
            compiled.monitor.set_quickening(self.quickening);
            #[cfg(feature = "jit")]
            apply_jit_config(&mut compiled, self.jit_config);
            compiled
                .monitor
                .set_reconfiguration_transfer_policy(self.transfer_policy);

            let input_stream = match input.open_resolved(resolved_input.clone()).await {
                Ok(stream) => stream,
                Err(error) => return failed_dataflow_runtime(policy, error),
            };
            let output_writer = match output_builder.open(resolved_output.clone()).await {
                Ok(writer) => writer,
                Err(error) => {
                    return failed_dataflow_runtime(
                        policy,
                        anyhow::anyhow!(
                            "reconfigurable output pipeline could not be opened: {error}"
                        ),
                    );
                }
            };

            #[cfg(feature = "jit")]
            let jit_config = self.jit_config;
            let transfer_policy = self.transfer_policy;
            let quickening = self.quickening;
            let compiler: ReconfigurationCompiler = Rc::new(move |source| {
                let model = parse_spec(source)?;
                let mut compiled = compile_model(model).map_err(anyhow::Error::from)?;
                compiled.monitor.set_quickening(quickening);
                #[cfg(feature = "jit")]
                apply_jit_config(&mut compiled, jit_config);
                compiled
                    .monitor
                    .set_reconfiguration_transfer_policy(transfer_policy);
                Ok(compiled)
            });

            DataflowRuntime {
                input_stream: DataflowInput::Reconfigurable(input_stream),
                output_writer: Some(output_writer),
                monitor: Ok(compiled.monitor),
                execution_policy: policy,
                reconfiguration: Some(ReconfigurationRuntimeState {
                    input,
                    active_input: resolved_input,
                    active_input_config: None,
                    output_builder,
                    active_output: resolved_output,
                    compiler,
                    transfer_policy: self.transfer_policy,
                    revision: crate::dataflow::RevisionId::INITIAL,
                    interface_epoch: crate::dataflow::InterfaceEpoch::INITIAL,
                    acknowledgements: self.acknowledgements,
                }),
                startup_error: None,
            }
        })
    }
}

impl<S> RuntimeBuilder<S, Value> for DataflowRuntimeBuilder<S>
where
    S: 'static,
    DataflowMonitor: TryFrom<S, Error = DataflowCompilationError>,
{
    type Runtime = DataflowRuntime;

    fn new() -> Self {
        Self {
            model: None,
            input: None,
            output_writer: None,
            execution_policy: ExecutionPolicy::Buffered,
            quickening: true,
            #[cfg(feature = "jit")]
            jit_config: None,
        }
    }

    fn executor(self, _executor: Rc<LocalExecutor<'static>>) -> Self {
        self
    }

    fn model(self, model: S) -> Self {
        Self {
            model: Some(model),
            ..self
        }
    }

    fn input(self, input: InputStream<Value>) -> Self {
        Self {
            input: Some(input),
            ..self
        }
    }

    fn output_writer(self, output_writer: OutputWriter<Value>) -> Self {
        DataflowRuntimeBuilder::<S>::output_writer(self, output_writer)
    }

    fn build(self) -> LocalBoxFuture<'static, Self::Runtime> {
        Box::pin(async move {
            let execution_policy = self.execution_policy;
            let mut startup_error = None;
            let mut monitor = match self.model {
                Some(model) => DataflowMonitor::try_from(model).map_err(anyhow::Error::from),
                None => Err(anyhow::anyhow!("dataflow runtime model is not configured")),
            };
            if let Ok(monitor) = &mut monitor {
                monitor.set_quickening(self.quickening);
            }
            #[cfg(feature = "jit")]
            let monitor = {
                let mut monitor = monitor;
                if let (Some(config), Ok(monitor)) = (self.jit_config, &mut monitor) {
                    monitor.enable_jit(config);
                }
                monitor
            };
            let input_stream = match self.input {
                Some(input) => DataflowInput::Standard(input),
                None => {
                    startup_error = Some(anyhow::anyhow!(
                        "dataflow runtime input stream is not configured"
                    ));
                    DataflowInput::Standard(Box::pin(futures::stream::empty()))
                }
            };
            let output_writer = self.output_writer;
            if output_writer.is_none() {
                startup_error = Some(anyhow::anyhow!(
                    "dataflow runtime output writer is not configured"
                ));
            }
            DataflowRuntime {
                input_stream,
                output_writer,
                monitor,
                execution_policy,
                reconfiguration: None,
                startup_error,
            }
        })
    }
}

#[async_trait(?Send)]
impl Runtime for DataflowRuntime {
    async fn run_boxed(self: Box<Self>) -> anyhow::Result<()> {
        let DataflowRuntime {
            input_stream,
            mut output_writer,
            monitor,
            execution_policy,
            reconfiguration,
            startup_error,
        } = *self;

        if let Some(error) = startup_error {
            return match output_writer.as_mut() {
                Some(writer) => finish_direct_output(writer, Some(error)).await,
                None => Err(error),
            };
        }
        let Some(mut output_writer) = output_writer else {
            return Err(anyhow::anyhow!(
                "dataflow runtime output writer is not configured"
            ));
        };
        let monitor = match monitor {
            Ok(monitor) => monitor,
            Err(error) => return finish_direct_output(&mut output_writer, Some(error)).await,
        };

        match reconfiguration {
            Some(state) => {
                let DataflowInput::Reconfigurable(input) = input_stream else {
                    return Err(anyhow::anyhow!(
                        "reconfigurable dataflow runtime did not receive its control input"
                    ));
                };
                run_reconfigurable_dataflow(input, monitor, output_writer, execution_policy, state)
                    .await
            }
            None => {
                let DataflowInput::Standard(input) = input_stream else {
                    return Err(anyhow::anyhow!(
                        "ordinary dataflow runtime received a reconfigurable input"
                    ));
                };
                run_direct_dataflow_engine(input, monitor, output_writer, execution_policy).await
            }
        }
    }
}

async fn run_direct_dataflow_engine(
    mut input_stream: InputStream<Value>,
    monitor: DataflowMonitor,
    output_writer: OutputWriter<Value>,
    execution_policy: ExecutionPolicy,
) -> anyhow::Result<()> {
    let mut engine = DirectDataflowEngine::new(monitor, output_writer);
    let mut error = None;

    'input: while let Some(batch) = input_stream.next().await {
        let batch = match batch {
            Ok(batch) => batch,
            Err(input_error) => {
                error = Some(input_error);
                break;
            }
        };

        if let Some((layout, values)) = batch.packed_rows_segment() {
            if let Err(evaluation_error) = engine.select_packed_layout(layout) {
                error = Some(evaluation_error);
                break;
            }
            for row in values.chunks(layout.len()) {
                if let Err(evaluation_error) = engine.evaluate_packed_row(row) {
                    error = Some(evaluation_error);
                    break 'input;
                }
                if execution_policy == ExecutionPolicy::Synchronous
                    || engine.pending_rows == DATAFLOW_RUNTIME_BATCH_SIZE
                {
                    if let Err(output_error) = flush_reconfigurable_output(&mut engine).await {
                        error = Some(output_error);
                        break 'input;
                    }
                }
            }
            continue;
        }

        for tick in batch.ticks() {
            if let Err(evaluation_error) = engine.evaluate_tick(&tick) {
                error = Some(evaluation_error);
                break 'input;
            }
            if execution_policy == ExecutionPolicy::Synchronous
                || engine.pending_rows == DATAFLOW_RUNTIME_BATCH_SIZE
            {
                if let Err(output_error) = flush_reconfigurable_output(&mut engine).await {
                    error = Some(output_error);
                    break 'input;
                }
            }
        }
    }

    // Do not send rows accumulated before a monitor or input error. In
    // particular, a failed evaluation must never turn its incomplete row into
    // an output batch. The writer still receives its cleanup flush and close.
    if error.is_none() && engine.pending_rows != 0 {
        if let Err(output_error) = flush_reconfigurable_output(&mut engine).await {
            error = Some(output_error);
        }
    }

    finish_direct_output(&mut engine.output_writer, error).await
}
async fn finish_direct_output(
    output_writer: &mut OutputWriter<Value>,
    primary: Option<anyhow::Error>,
) -> anyhow::Result<()> {
    let cleanup = crate::runtime::output::finish_writer(output_writer).await;
    match primary {
        Some(primary) => match cleanup {
            Ok(()) => Err(primary),
            Err(cleanup) => Err(combine_errors(primary, cleanup)),
        },
        None => cleanup,
    }
}

fn combine_errors(primary: anyhow::Error, additional: anyhow::Error) -> anyhow::Error {
    let primary_message = primary.to_string();
    let additional_message = additional.to_string();
    if primary_message == additional_message {
        primary
    } else {
        anyhow::anyhow!("{primary_message}; additionally: {additional_message}")
    }
}

fn failed_dataflow_runtime(
    execution_policy: ExecutionPolicy,
    error: anyhow::Error,
) -> DataflowRuntime {
    DataflowRuntime {
        input_stream: DataflowInput::Standard(Box::pin(futures::stream::empty())),
        output_writer: None,
        monitor: Err(anyhow::anyhow!("dataflow runtime startup failed")),
        execution_policy,
        reconfiguration: None,
        startup_error: Some(error),
    }
}

fn compile_model<S>(model: S) -> Result<CompiledReconfiguration, DataflowCompilationError>
where
    S: Specification + 'static,
    DataflowMonitor: TryFrom<S, Error = DataflowCompilationError>,
{
    let input_vars = model.input_vars();
    let output_vars = model.output_vars();
    let auxiliary_vars = model.aux_vars();
    let monitor = DataflowMonitor::try_from(model)?;
    Ok(CompiledReconfiguration {
        monitor,
        input_vars,
        output_vars,
        auxiliary_vars,
    })
}

#[cfg(feature = "jit")]
fn apply_jit_config(result: &mut CompiledReconfiguration, config: Option<JitConfig>) {
    if let Some(config) = config {
        result.monitor.enable_jit(config);
    }
}

#[derive(Debug)]
struct ReconfigurationRequest {
    spec: String,
    input_config: MonitorConfig,
}

fn request_from_monitor_config(config: MonitorConfig) -> ReconfigurationRequest {
    ReconfigurationRequest {
        spec: config.spec.clone(),
        input_config: config,
    }
}

async fn acknowledge_reconfiguration(
    state: &ReconfigurationRuntimeState,
    applied: bool,
) -> anyhow::Result<()> {
    let acknowledgement = ReconfigurationAck {
        revision: state.revision,
        interface_epoch: state.interface_epoch,
        applied,
    };
    info!(
        revision = %acknowledgement.revision,
        interface_epoch = %acknowledgement.interface_epoch,
        applied,
        "acknowledging dataflow reconfiguration"
    );
    if let Some(sink) = &state.acknowledgements {
        sink.send(acknowledgement).await.map_err(|_| {
            anyhow::anyhow!(
                "reconfiguration acknowledgement channel is closed, so the producer barrier cannot be honoured"
            )
        })?;
    }
    Ok(())
}

fn writer_is_closed(writer: &OutputWriter<Value>) -> bool {
    writer.error().is_some_and(OutputError::is_closed)
}

async fn flush_reconfigurable_output(engine: &mut DirectDataflowEngine) -> anyhow::Result<()> {
    match engine.flush().await {
        Ok(()) if writer_is_closed(&engine.output_writer) => Err(anyhow::anyhow!(
            "dataflow output writer closed while flushing"
        )),
        Ok(()) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Drain all rows already committed by a generation before releasing its monitor
/// and writer. A writer close is the external cutover barrier for the unified
/// output pipeline.
async fn drain_previous_output(
    mut engine: DirectDataflowEngine,
) -> anyhow::Result<DataflowMonitor> {
    let mut primary = None;
    if engine.pending_rows != 0 {
        if let Err(error) = flush_reconfigurable_output(&mut engine).await {
            primary = Some(error);
        }
    }

    let DirectDataflowEngine {
        monitor,
        mut output_writer,
        ..
    } = engine;
    let cleanup = crate::runtime::output::finish_writer(&mut output_writer).await;
    match (primary, cleanup) {
        (None, Ok(())) => Ok(monitor),
        (Some(primary), Ok(())) => Err(primary),
        (None, Err(cleanup)) => Err(cleanup),
        (Some(primary), Err(cleanup)) => Err(combine_errors(primary, cleanup)),
    }
}

/// Preserve committed rows on an evaluation/input failure, then close the
/// generation's writer. The original failure remains the primary error.
async fn terminate_after_output_drain(
    mut engine: DirectDataflowEngine,
    primary: anyhow::Error,
) -> anyhow::Result<()> {
    let mut error = primary;
    if engine.pending_rows != 0 {
        if let Err(flush_error) = flush_reconfigurable_output(&mut engine).await {
            error = combine_errors(error, flush_error);
        }
    }
    let cleanup = crate::runtime::output::finish_writer(&mut engine.output_writer).await;
    if let Err(cleanup) = cleanup {
        error = combine_errors(error, cleanup);
    }
    Err(error)
}

async fn run_reconfigurable_dataflow(
    mut input: ReconfigurableInputStream<Value>,
    monitor: DataflowMonitor,
    output_writer: OutputWriter<Value>,
    execution_policy: ExecutionPolicy,
    mut state: ReconfigurationRuntimeState,
) -> anyhow::Result<()> {
    let mut engine = DirectDataflowEngine::new(monitor, output_writer);

    loop {
        let Some(item) = input.next().await else {
            drain_previous_output(engine).await.map(|_| ())?;
            return Ok(());
        };
        let item = match item {
            Ok(item) => item,
            Err(error) => return terminate_after_output_drain(engine, error).await,
        };

        match item {
            ReconfigurableInputItem::Data(batch) => {
                if let Err(error) =
                    evaluate_reconfigurable_batch(&mut engine, &batch, execution_policy).await
                {
                    return terminate_after_output_drain(engine, error).await;
                }
                state.revision = engine.monitor.revision();
            }
            ReconfigurableInputItem::Reconfigure(config) => {
                let (next_engine, next_input) =
                    replace_root(engine, &mut input, &mut state, config).await?;
                engine = next_engine;
                input = next_input;
            }
        }
    }
}

async fn replace_root(
    engine: DirectDataflowEngine,
    input: &mut ReconfigurableInputStream<Value>,
    state: &mut ReconfigurationRuntimeState,
    config: MonitorConfig,
) -> anyhow::Result<(DirectDataflowEngine, ReconfigurableInputStream<Value>)> {
    let active_monitor = drain_previous_output(engine).await?;
    config.validate_structure()?;
    let request = request_from_monitor_config(config);

    crate::dataflow::validate_replacement(
        &crate::dataflow::ReplacementTarget::Root,
        &crate::dataflow::DefinitionSource::text(request.spec.clone()),
        &crate::dataflow::ActivationFrontier::EmptyEvaluation {
            revision: active_monitor.revision(),
            interface_epoch: active_monitor.interface_epoch(),
        },
        state.revision,
    )?;

    let mut compiled = (state.compiler)(&request.spec)?;
    let candidate_input = state
        .input
        .pipeline()
        .resolve(&compiled.input_vars, Some(&request.input_config))?;
    let candidate_output = state.output_builder.resolve(
        &compiled.output_vars,
        &compiled.auxiliary_vars,
        Some(&request.input_config),
    )?;

    let active_inputs = active_monitor
        .input_vars()
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let active_outputs = active_monitor
        .output_vars()
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let semantic_changed = compiled.monitor.definition_key() != active_monitor.definition_key();
    let input_interface_changed =
        compiled.input_vars != active_inputs || candidate_input != state.active_input;
    let output_interface_changed =
        compiled.output_vars != active_outputs || candidate_output != state.active_output;
    let interface_changed = input_interface_changed || output_interface_changed;

    let next_revision = if semantic_changed {
        state
            .revision
            .checked_next()
            .ok_or_else(|| anyhow::anyhow!("dataflow semantic revision overflow"))?
    } else {
        state.revision
    };
    let next_interface_epoch = if interface_changed {
        state
            .interface_epoch
            .checked_next()
            .ok_or_else(|| anyhow::anyhow!("dataflow interface epoch overflow"))?
    } else {
        state.interface_epoch
    };

    let mut next_monitor = if semantic_changed {
        if state.transfer_policy != ContextTransferPolicy::None {
            let context = active_monitor.export_context()?;
            compiled
                .monitor
                .import_context(&context, state.transfer_policy)?;
        }
        compiled.monitor
    } else {
        active_monitor
    };
    next_monitor.install_revision(next_revision, next_interface_epoch);

    let old_input = std::mem::replace(
        input,
        Box::pin(futures::stream::empty()) as ReconfigurableInputStream<Value>,
    );
    drop(old_input);
    let next_input = state
        .input
        .open_resolved(candidate_input.clone())
        .await
        .context("reconfigurable replacement input could not be opened")?;
    let next_writer = match state.output_builder.open(candidate_output.clone()).await {
        Ok(writer) => writer,
        Err(error) => {
            return Err(anyhow::anyhow!(
                "reconfigurable replacement output could not be opened: {error}"
            ));
        }
    };

    state.active_input = candidate_input;
    state.active_input_config = Some(request.input_config);
    state.active_output = candidate_output;
    state.revision = next_revision;
    state.interface_epoch = next_interface_epoch;

    let mut next_engine = DirectDataflowEngine::new(next_monitor, next_writer);
    if let Err(error) =
        acknowledge_reconfiguration(state, semantic_changed || interface_changed).await
    {
        let acknowledgement_error =
            match finish_direct_output(&mut next_engine.output_writer, Some(error)).await {
                Ok(()) => anyhow::anyhow!("reconfiguration acknowledgement failed"),
                Err(error) => error,
            };
        return Err(acknowledgement_error);
    }
    Ok((next_engine, next_input))
}

async fn evaluate_reconfigurable_batch(
    engine: &mut DirectDataflowEngine,
    batch: &InputBatch<Value>,
    execution_policy: ExecutionPolicy,
) -> anyhow::Result<()> {
    if let Some((layout, values)) = batch.packed_rows_segment() {
        engine.select_packed_layout(layout)?;
        for row in values.chunks(layout.len()) {
            engine.evaluate_packed_row(row)?;
            if execution_policy == ExecutionPolicy::Synchronous
                || engine.pending_rows == DATAFLOW_RUNTIME_BATCH_SIZE
            {
                flush_reconfigurable_output(engine).await?;
            }
        }
        return Ok(());
    }

    for tick in batch.ticks() {
        engine.evaluate_tick(&tick)?;
        if execution_policy == ExecutionPolicy::Synchronous
            || engine.pending_rows == DATAFLOW_RUNTIME_BATCH_SIZE
        {
            flush_reconfigurable_output(engine).await?;
        }
    }
    Ok(())
}

struct DirectDataflowEngine {
    monitor: DataflowMonitor,
    output_writer: OutputWriter<Value>,
    output_layout: Arc<[crate::VarName]>,
    output_values: Vec<Value>,
    output_value_capacity: usize,
    pending_rows: usize,
    input_row: Vec<Value>,
    output_row: Vec<Value>,
    input_ids: BTreeMap<crate::VarName, usize>,
    cached_layout_vars: Vec<crate::VarName>,
    cached_layout_slots: Vec<usize>,
}

impl DirectDataflowEngine {
    fn new(monitor: DataflowMonitor, output_writer: OutputWriter<Value>) -> Self {
        let output_layout: Arc<[crate::VarName]> = monitor.output_vars().to_vec().into();
        let output_value_capacity = DATAFLOW_RUNTIME_BATCH_SIZE.saturating_mul(output_layout.len());
        let input_row = vec![Value::NoVal; monitor.input_vars().len()];
        let output_row = vec![Value::NoVal; output_layout.len()];
        let input_ids: BTreeMap<crate::VarName, usize> = monitor
            .input_vars()
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, var)| (var, index))
            .collect();
        Self {
            monitor,
            output_writer,
            output_layout,
            output_values: Vec::with_capacity(output_value_capacity),
            output_value_capacity,
            pending_rows: 0,
            input_row,
            output_row,
            cached_layout_vars: Vec::with_capacity(input_ids.len()),
            cached_layout_slots: Vec::with_capacity(input_ids.len()),
            input_ids,
        }
    }

    fn evaluate_tick(&mut self, tick: &crate::core::InputTick<'_, Value>) -> anyhow::Result<()> {
        let layout_matches = tick.len() == self.cached_layout_vars.len()
            && tick
                .iter()
                .zip(&self.cached_layout_vars)
                .all(|(event, var)| event.variable == var);
        if !layout_matches {
            self.cached_layout_vars.clear();
            self.cached_layout_slots.clear();
            for event in tick.iter() {
                let Some(&slot) = self.input_ids.get(event.variable) else {
                    return Err(anyhow::anyhow!(
                        "input stream emitted undeclared dataflow variable `{}`",
                        event.variable
                    ));
                };
                self.cached_layout_vars.push(event.variable.clone());
                self.cached_layout_slots.push(slot);
            }
        }

        for (event, &slot) in tick.iter().zip(&self.cached_layout_slots) {
            self.input_row[slot] = event.value.clone();
        }

        let result = self.monitor.evaluate(&self.input_row, &mut self.output_row);
        for &slot in &self.cached_layout_slots {
            self.input_row[slot] = Value::NoVal;
        }
        result?;
        self.push_output_row();
        Ok(())
    }

    fn select_packed_layout(&mut self, layout: &[crate::VarName]) -> anyhow::Result<()> {
        if layout == self.cached_layout_vars {
            return Ok(());
        }
        self.cached_layout_vars.clear();
        self.cached_layout_slots.clear();
        for var in layout {
            let Some(&slot) = self.input_ids.get(var) else {
                return Err(anyhow::anyhow!(
                    "input stream emitted undeclared dataflow variable `{var}`"
                ));
            };
            self.cached_layout_vars.push(var.clone());
            self.cached_layout_slots.push(slot);
        }
        Ok(())
    }

    #[inline]
    fn evaluate_packed_row(&mut self, values: &[Value]) -> anyhow::Result<()> {
        debug_assert_eq!(values.len(), self.cached_layout_slots.len());
        for (value, &slot) in values.iter().zip(&self.cached_layout_slots) {
            self.input_row[slot] = value.clone();
        }

        let result = self.monitor.evaluate(&self.input_row, &mut self.output_row);
        for &slot in &self.cached_layout_slots {
            self.input_row[slot] = Value::NoVal;
        }
        result?;
        self.push_output_row();
        Ok(())
    }

    fn push_output_row(&mut self) {
        self.output_values.extend(self.output_row.iter().cloned());
        self.pending_rows += 1;
    }

    async fn flush(&mut self) -> Result<(), OutputError> {
        if self.pending_rows == 0 {
            return Ok(());
        }
        if self.output_layout.is_empty() {
            self.output_values.clear();
            self.pending_rows = 0;
            return Ok(());
        }

        let values = std::mem::take(&mut self.output_values);
        let rows = match OutputBatch::packed_rows(Arc::clone(&self.output_layout), values) {
            Ok(rows) => rows,
            Err(error) => {
                self.output_values = Vec::with_capacity(self.output_value_capacity);
                self.pending_rows = 0;
                return Err(error);
            }
        };
        match self.output_writer.send(rows).await {
            Ok(()) => {
                self.output_values = Vec::with_capacity(self.output_value_capacity);
                self.pending_rows = 0;
                Ok(())
            }
            Err(error) => {
                self.output_values = Vec::with_capacity(self.output_value_capacity);
                self.pending_rows = 0;
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::collections::BTreeMap;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::{Context, Poll};

    use async_trait::async_trait;
    use async_unsync::bounded;
    use futures::Sink;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;

    use crate::VarName;
    use crate::core::{OutputBackend, OutputBatch};

    use crate::io::output::ManualOutputBackend;
    use crate::io::testing::{input_source_with_control, limited_null_output, manual_output};
    use crate::io::{InputPipeline, OutputBackendBuilder, OutputBackendConfig, map};
    use crate::stream_utils::Fanout;
    use crate::{CheckedDsrvSpecification, DsrvSpecification, TypeCheckOptions, Value, async_test};

    use super::*;

    fn recording_writer(batches: Rc<RefCell<Vec<OutputBatch<Value>>>>) -> OutputWriter<Value> {
        let batches_for_sink = Rc::clone(&batches);
        let sink = futures::sink::unfold((), move |_, batch: OutputBatch<Value>| {
            let batches = Rc::clone(&batches_for_sink);
            async move {
                batches.borrow_mut().push(batch);
                Ok::<_, OutputError>(())
            }
        });
        OutputWriter::from_sink(sink)
    }

    fn failing_writer(error: OutputError) -> OutputWriter<Value> {
        let sink = futures::sink::unfold((), move |_, _batch: OutputBatch<Value>| {
            let error = error.clone();
            async move { Err::<(), _>(error) }
        });
        OutputWriter::from_sink(sink)
    }

    struct CleanupFailingSink {
        flush_error: Option<OutputError>,
        close_error: Option<OutputError>,
        closes: Rc<Cell<usize>>,
    }

    impl Sink<OutputBatch<Value>> for CleanupFailingSink {
        type Error = OutputError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, _batch: OutputBatch<Value>) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let this = self.get_mut();
            Poll::Ready(this.flush_error.take().map_or(Ok(()), Err))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let this = self.get_mut();
            this.closes.set(this.closes.get() + 1);
            Poll::Ready(this.close_error.take().map_or(Ok(()), Err))
        }
    }

    struct RecordingBackend {
        opened: Rc<RefCell<usize>>,
        writer: RefCell<Option<OutputWriter<Value>>>,
    }

    #[async_trait(?Send)]
    impl crate::OutputBackend for RecordingBackend {
        type Val = Value;

        async fn open(
            &self,
            interface: crate::OutputInterface,
        ) -> Result<OutputWriter<Value>, OutputError> {
            assert_eq!(interface.routes().len(), 1);
            *self.opened.borrow_mut() += 1;
            self.writer
                .borrow_mut()
                .take()
                .ok_or_else(|| OutputError::backend("test backend opened twice"))
        }
    }

    struct FailingBackend;

    #[async_trait(?Send)]
    impl crate::OutputBackend for FailingBackend {
        type Val = Value;

        async fn open(
            &self,
            _interface: crate::OutputInterface,
        ) -> Result<OutputWriter<Value>, OutputError> {
            Err(OutputError::backend("backend open failed"))
        }
    }

    async fn run_dataflow_runtime(
        executor: Rc<LocalExecutor<'static>>,
        spec_src: &'static str,
        inputs: BTreeMap<VarName, Vec<Value>>,
        limit: usize,
    ) {
        let spec = spec_src.parse::<DsrvSpecification>().unwrap();
        let output_writer = limited_null_output(spec.output_vars().clone(), limit).await;
        let runtime = DataflowRuntimeBuilder::<DsrvSpecification>::new()
            .executor(executor.clone())
            .model(spec)
            .input(map::input_stream(inputs))
            .output_writer(output_writer)
            .build()
            .await;

        runtime.run().await.expect("dataflow runtime should run");
    }

    #[apply(async_test)]
    async fn dataflow_runtime_evaluates_simple_arithmetic(executor: Rc<LocalExecutor<'static>>) {
        run_dataflow_runtime(
            executor,
            "in x\nin y\nout z\nz = x + y",
            BTreeMap::from([
                (
                    VarName::new("x"),
                    vec![Value::Int(1), Value::Int(2), Value::Int(3)],
                ),
                (
                    VarName::new("y"),
                    vec![Value::Int(10), Value::Int(20), Value::Int(30)],
                ),
            ]),
            3,
        )
        .await;
    }

    #[apply(async_test)]
    async fn reconfigurable_dataflow_replacement_inherits_quickening_setting(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let model = "in x: Int\nout z: Int\nz = x + 1"
            .parse::<DsrvSpecification>()
            .unwrap();
        let (_x_sender, x_fanout) = Fanout::<Value>::new();
        let (_control_sender, control_fanout) = Fanout::<Value>::new();
        let input_source = input_source_with_control(
            BTreeMap::from([(VarName::new("x"), x_fanout)]),
            control_fanout,
        )
        .with_reconfiguration_route("control")
        .unwrap();
        let runtime = ReconfigurableDataflowRuntimeBuilder::<DsrvSpecification>::new()
            .parse_spec(|source| source.parse().map_err(anyhow::Error::from))
            .executor(executor)
            .model(model)
            .input_pipeline(InputPipeline::new(input_source))
            .output_builder(OutputBackendBuilder::new(OutputBackendConfig::null()))
            .reconf_topic("control")
            .quickening(false)
            .build()
            .await;

        assert!(!runtime.monitor.as_ref().unwrap().quickening_enabled());
        let replacement = (runtime.reconfiguration.as_ref().unwrap().compiler)(
            "in x: Int\nout z: Int\nz = x + 2",
        )
        .unwrap();
        assert!(!replacement.monitor.quickening_enabled());
    }

    #[apply(async_test)]
    async fn dataflow_synchronous_controller_acknowledges_processed_ticks(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let spec_src = "in x\nout z\nz = x + 1";
        let spec = spec_src.parse::<DsrvSpecification>().unwrap();
        let (output_writer, mut outputs) = manual_output(spec.output_vars().clone()).await;
        let input = map::input_stream(BTreeMap::from([(
            VarName::new("x"),
            vec![Value::Int(1), Value::Int(2)],
        )]));
        let (builder, controller) = DataflowRuntimeBuilder::<DsrvSpecification>::new()
            .executor(executor)
            .model(spec)
            .controlled_input(input);
        let runtime = builder.output_writer(output_writer).build().await;

        let control = async move {
            controller.advance().await.unwrap();
            let first = outputs.next().await.unwrap();
            assert_eq!(first.get(&VarName::new("z")), Some(&Value::Int(2)));
            controller.advance().await.unwrap();
            let second = outputs.next().await.unwrap();
            assert_eq!(second.get(&VarName::new("z")), Some(&Value::Int(3)));
        };
        let (result, ()) = futures::join!(runtime.run(), control);
        result.unwrap();
    }
    #[apply(async_test)]
    async fn reconfigurable_dataflow_strict_transfer_rejects_incompatible_nested_dynamic_body(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let old_spec = "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
        let new_spec = "in a: Int\nin x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
        let model = old_spec.parse::<DsrvSpecification>().unwrap();

        let (a_sender, a_fanout) = Fanout::<Value>::new();
        let (x_sender, x_fanout) = Fanout::<Value>::new();
        let (source_sender, source_fanout) = Fanout::<Value>::new();
        let (control_sender, control_fanout) = Fanout::<Value>::new();
        let input_source = input_source_with_control(
            BTreeMap::from([
                (VarName::new("a"), a_fanout),
                (VarName::new("x"), x_fanout),
                (VarName::new("source"), source_fanout),
            ]),
            control_fanout,
        )
        .with_reconfiguration_route("control")
        .unwrap();
        let input_pipeline = InputPipeline::new(input_source);

        let (output_backend, mut outputs) = ManualOutputBackend::<Value>::channel(4);
        let output_builder =
            OutputBackendBuilder::new(OutputBackendConfig::manual(output_backend.sender().clone()));
        drop(output_backend);
        let (ack_sender, mut acknowledgements) =
            bounded::channel::<ReconfigurationAck>(1).into_split();

        let runtime = ReconfigurableDataflowRuntimeBuilder::<DsrvSpecification>::new()
            .parse_spec(|source| source.parse().map_err(anyhow::Error::from))
            .executor(executor.clone())
            .model(model)
            .input_pipeline(input_pipeline)
            .output_builder(output_builder)
            .reconf_topic("control")
            .context_transfer(ContextTransferPolicy::Strict)
            .acknowledgements(ack_sender)
            .build()
            .await;
        let task = executor.spawn(runtime.run());

        x_sender.send(Value::Int(1)).await;
        source_sender.send(Value::Str("x[1]".into())).await;
        assert_eq!(
            outputs.recv().await,
            Some(BTreeMap::from([(VarName::new("z"), Value::Deferred)]))
        );

        control_sender
            .send(Value::Str(
                serde_json::json!({"spec": new_spec}).to_string().into(),
            ))
            .await;
        let acknowledgement = acknowledgements
            .recv()
            .await
            .expect("root reconfiguration acknowledgement should arrive");
        assert!(acknowledgement.applied);

        x_sender.send(Value::Int(2)).await;
        source_sender.send(Value::Str("x + 1".into())).await;
        a_sender.send(Value::NoVal).await;
        drop(a_sender);
        drop(x_sender);
        drop(source_sender);
        drop(control_sender);

        let error =
            tc_testutils::streams::with_timeout(task, 5, "strict nested dynamic transfer runtime")
                .await
                .expect("strict nested dynamic transfer runtime should terminate")
                .expect_err("incompatible nested dynamic transfer should fail the runtime");
        assert!(matches!(
            error.downcast_ref::<crate::dataflow::DataflowEvaluationError>(),
            Some(crate::dataflow::DataflowEvaluationError::IncompatibleRegionTransfer(_))
        ));
        assert_eq!(outputs.recv().await, None);
    }

    #[apply(async_test)]
    async fn dataflow_runtime_evaluates_recursive_accumulator(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        run_dataflow_runtime(
            executor,
            "in x\nout z\nz = default(z[1], 0) + x",
            BTreeMap::from([(
                VarName::new("x"),
                vec![
                    Value::Int(1),
                    Value::Int(2),
                    Value::Int(3),
                    Value::Int(4),
                    Value::Int(5),
                ],
            )]),
            5,
        )
        .await;
    }

    #[apply(async_test)]
    async fn typed_dataflow_runtime_evaluates_simple_arithmetic(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let spec_src = "in x: Int\nin y: Int\nout z: Int\nz = x + y";
        let spec = spec_src.parse::<CheckedDsrvSpecification>().unwrap();
        let output_writer = limited_null_output(spec.output_vars().clone(), 3).await;
        let runtime =
            DataflowRuntimeBuilder::<crate::lang::dsrv::ast::CheckedDsrvSpecification>::new()
                .executor(executor.clone())
                .model(spec)
                .input(map::input_stream(BTreeMap::from([
                    (VarName::new("x"), vec![1.into(), 2.into(), 3.into()]),
                    (VarName::new("y"), vec![10.into(), 20.into(), 30.into()]),
                ])))
                .output_writer(output_writer)
                .build()
                .await;

        runtime
            .run()
            .await
            .expect("typed dataflow runtime should run");
    }

    #[cfg(feature = "jit")]
    #[apply(async_test)]
    async fn checked_dataflow_runtime_can_enable_delayed_jit(executor: Rc<LocalExecutor<'static>>) {
        let spec = "in x: Int\nout z: Int\nz = x + 1"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let (output_writer, outputs) = manual_output(spec.output_vars().clone()).await;
        let runtime = DataflowRuntimeBuilder::<CheckedDsrvSpecification>::new()
            .jit(JitConfig::after_events(1))
            .executor(executor.clone())
            .model(spec)
            .input(map::input_stream(BTreeMap::from([(
                VarName::new("x"),
                vec![1.into(), 2.into(), 3.into()],
            )])))
            .output_writer(output_writer)
            .build()
            .await;

        executor.spawn(runtime.run()).detach();
        let outputs = tc_testutils::streams::with_timeout(
            outputs.collect::<Vec<_>>(),
            5,
            "JIT dataflow output collection",
        )
        .await
        .expect("JIT dataflow output collection should finish");

        assert_eq!(
            outputs,
            vec![
                BTreeMap::from([(VarName::new("z"), Value::Int(2))]),
                BTreeMap::from([(VarName::new("z"), Value::Int(3))]),
                BTreeMap::from([(VarName::new("z"), Value::Int(4))]),
            ]
        );
    }

    #[apply(async_test)]
    async fn gradual_typed_dataflow_runtime_uses_value_fallback(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let spec_src = "in x: Any\nin y: Any\nout z: Any\nz = x + y";
        let spec =
            CheckedDsrvSpecification::parse_with(spec_src, TypeCheckOptions::GRADUAL).unwrap();
        let output_writer = limited_null_output(spec.output_vars().clone(), 3).await;
        let runtime =
            DataflowRuntimeBuilder::<crate::lang::dsrv::ast::CheckedDsrvSpecification>::new()
                .executor(executor.clone())
                .model(spec)
                .input(map::input_stream(BTreeMap::from([
                    (VarName::new("x"), vec![1.into(), 2.into(), 3.into()]),
                    (VarName::new("y"), vec![10.into(), 20.into(), 30.into()]),
                ])))
                .output_writer(output_writer)
                .build()
                .await;

        runtime
            .run()
            .await
            .expect("gradual typed dataflow runtime should run");
    }

    #[apply(async_test)]
    async fn gradual_typed_dataflow_runtime_casts_untyped_input(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let spec_src = "in x\nout z\nz = x + 1";
        let spec =
            CheckedDsrvSpecification::parse_with(spec_src, TypeCheckOptions::GRADUAL).unwrap();
        let (output_writer, outputs) = manual_output(spec.output_vars().clone()).await;
        let runtime =
            DataflowRuntimeBuilder::<crate::lang::dsrv::ast::CheckedDsrvSpecification>::new()
                .executor(executor.clone())
                .model(spec)
                .input(map::input_stream(BTreeMap::from([(
                    VarName::new("x"),
                    vec![41.into(), 1.into()],
                )])))
                .output_writer(output_writer)
                .build()
                .await;

        executor.spawn(runtime.run()).detach();
        let outputs = tc_testutils::streams::with_timeout(
            outputs.collect::<Vec<_>>(),
            5,
            "gradual typed dataflow cast output collection",
        )
        .await
        .expect("dataflow cast output collection should finish");

        assert_eq!(
            outputs,
            vec![
                BTreeMap::from([(VarName::new("z"), Value::Int(42))]),
                BTreeMap::from([(VarName::new("z"), Value::Int(2))]),
            ]
        );
    }

    #[apply(async_test)]
    async fn dataflow_runtime_drains_outputs_after_input_finishes(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let spec_src = "in x\nout z\nz = x + 10";
        let spec = spec_src.parse::<DsrvSpecification>().unwrap();
        let (output_writer, outputs) = manual_output(spec.output_vars().clone()).await;
        let runtime = DataflowRuntimeBuilder::<DsrvSpecification>::new()
            .executor(executor.clone())
            .model(spec)
            .input(map::input_stream(BTreeMap::from([(
                VarName::new("x"),
                vec![Value::Int(1), Value::Int(2)],
            )])))
            .output_writer(output_writer)
            .build()
            .await;

        executor.spawn(runtime.run()).detach();
        let outputs = tc_testutils::streams::with_timeout(
            outputs.collect::<Vec<_>>(),
            5,
            "dataflow manual output collection",
        )
        .await
        .expect("dataflow output collection should finish");

        assert_eq!(
            outputs,
            vec![
                BTreeMap::from([(VarName::new("z"), Value::Int(11))]),
                BTreeMap::from([(VarName::new("z"), Value::Int(12))]),
            ]
        );
    }

    #[apply(async_test)]
    async fn dataflow_runtime_flushes_full_and_partial_internal_batches(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let spec_src = "in x\nout z\nz = x + 1";
        let spec = spec_src.parse::<DsrvSpecification>().unwrap();
        let (output_writer, outputs) = manual_output(spec.output_vars().clone()).await;
        let runtime = DataflowRuntimeBuilder::<DsrvSpecification>::new()
            .executor(executor.clone())
            .model(spec)
            .input(map::input_stream(BTreeMap::from([(
                VarName::new("x"),
                (0..300).map(Value::Int).collect(),
            )])))
            .output_writer(output_writer)
            .build()
            .await;

        executor.spawn(runtime.run()).detach();
        let outputs = tc_testutils::streams::with_timeout(
            outputs.collect::<Vec<_>>(),
            5,
            "privately batched dataflow output collection",
        )
        .await
        .expect("privately batched output collection should finish");

        assert_eq!(outputs.len(), 300);
        assert_eq!(outputs[0][&VarName::new("z")], Value::Int(1));
        assert_eq!(outputs[299][&VarName::new("z")], Value::Int(300));
    }

    #[apply(async_test)]
    async fn dataflow_runtime_preserves_logical_ticks_inside_transport_batches(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let spec_src = "in x\nin y\nout z\nz = 42";
        let spec = spec_src.parse::<DsrvSpecification>().unwrap();
        let (output_writer, outputs) = manual_output(spec.output_vars().clone()).await;
        let simultaneous = crate::InputBatch::tick(vec![
            crate::InputUpdate::new("x".into(), Value::Int(1)),
            crate::InputUpdate::new("y".into(), Value::Int(10)),
        ])
        .unwrap();
        let independent = crate::InputBatch::from_ticks(vec![
            vec![crate::InputUpdate::new("x".into(), Value::Int(2))],
            vec![crate::InputUpdate::new("y".into(), Value::Int(20))],
        ])
        .unwrap();
        let input = Box::pin(futures::stream::iter([Ok(simultaneous), Ok(independent)]));
        let runtime = DataflowRuntimeBuilder::<DsrvSpecification>::new()
            .executor(executor.clone())
            .model(spec)
            .input(input)
            .output_writer(output_writer)
            .build()
            .await;

        executor.spawn(runtime.run()).detach();
        let outputs = tc_testutils::streams::with_timeout(
            outputs.collect::<Vec<_>>(),
            5,
            "dataflow logical tick output collection",
        )
        .await
        .expect("dataflow logical tick output collection should finish");

        assert_eq!(outputs.len(), 3);
        assert!(
            outputs
                .iter()
                .all(|row| row[&VarName::new("z")] == Value::Int(42))
        );
    }

    #[apply(async_test)]
    async fn direct_dataflow_emits_row_major_batches_and_preserves_no_val(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let batches = Rc::new(RefCell::new(Vec::new()));
        let spec = "in x\nin y\nout a\nout z\na = x\nz = y"
            .parse::<DsrvSpecification>()
            .unwrap();
        let runtime = DataflowRuntimeBuilder::<DsrvSpecification>::new()
            .executor(executor)
            .model(spec)
            .input(map::input_stream(BTreeMap::from([
                (
                    VarName::new("x"),
                    (0..300).map(Value::Int).collect::<Vec<_>>(),
                ),
                (
                    VarName::new("y"),
                    (0..299).map(Value::Int).collect::<Vec<_>>(),
                ),
            ])))
            .output_writer(recording_writer(Rc::clone(&batches)))
            .build()
            .await;

        runtime.run().await.unwrap();

        let batches = batches.borrow();
        assert_eq!(batches.len(), 2);
        let first = &batches[0];
        let partial = &batches[1];
        assert_eq!(first.tick_count(), 256);
        assert_eq!(partial.tick_count(), 44);
        let first_tick = first.ticks().next().expect("first output row exists");
        assert_eq!(
            first_tick
                .updates()
                .map(|update| (update.variable.name(), update.value.clone()))
                .collect::<Vec<_>>(),
            vec![("a".into(), Value::Int(0)), ("z".into(), Value::Int(0))]
        );
        let partial_first = partial.ticks().next().expect("partial output row exists");
        assert_eq!(
            partial_first
                .updates()
                .map(|update| update.value.clone())
                .collect::<Vec<_>>(),
            vec![Value::Int(256), Value::Int(256)]
        );
        let partial_last = partial.ticks().last().expect("last partial row exists");
        assert_eq!(
            partial_last
                .updates()
                .map(|update| update.value.clone())
                .collect::<Vec<_>>(),
            vec![Value::Int(299), Value::NoVal]
        );
    }

    #[apply(async_test)]
    async fn direct_dataflow_opens_backend_during_build(executor: Rc<LocalExecutor<'static>>) {
        let batches = Rc::new(RefCell::new(Vec::new()));
        let opened = Rc::new(RefCell::new(0));
        let backend = Rc::new(RecordingBackend {
            opened: Rc::clone(&opened),
            writer: RefCell::new(Some(recording_writer(Rc::clone(&batches)))),
        });
        let interface =
            crate::OutputInterface::new([crate::OutputRoute::output(VarName::new("z"))]).unwrap();
        let spec = "in x\nout z\nz = x".parse::<DsrvSpecification>().unwrap();
        let runtime = DataflowRuntimeBuilder::<DsrvSpecification>::new()
            .executor(executor)
            .model(spec)
            .input(map::input_stream(BTreeMap::from([(
                VarName::new("x"),
                vec![Value::Int(1)],
            )])))
            .output_writer(backend.open(interface).await.unwrap())
            .build()
            .await;

        assert_eq!(*opened.borrow(), 1);
        runtime.run().await.unwrap();
        assert_eq!(batches.borrow().len(), 1);
    }

    #[apply(async_test)]
    async fn direct_dataflow_retains_backend_open_errors_until_run(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let spec = "in x\nout z\nz = x".parse::<DsrvSpecification>().unwrap();
        let error = match Rc::new(FailingBackend)
            .open(crate::OutputInterface::empty())
            .await
        {
            Ok(_) => panic!("failing backend unexpectedly opened"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("backend open failed"));
        let _ = (executor, spec);
    }

    #[apply(async_test)]
    async fn direct_dataflow_synchronous_policy_emits_one_row_batches(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let batches = Rc::new(RefCell::new(Vec::new()));
        let spec = "in x\nout z\nz = x".parse::<DsrvSpecification>().unwrap();
        let runtime = DataflowRuntimeBuilder::<DsrvSpecification>::new()
            .execution_policy(ExecutionPolicy::Synchronous)
            .executor(executor)
            .model(spec)
            .input(map::input_stream(BTreeMap::from([(
                VarName::new("x"),
                vec![Value::Int(1), Value::Int(2)],
            )])))
            .output_writer(recording_writer(Rc::clone(&batches)))
            .build()
            .await;

        runtime.run().await.unwrap();

        let batches = batches.borrow();
        assert_eq!(batches.len(), 2);
        assert!(batches.iter().all(|batch| batch.tick_count() == 1));
    }

    #[apply(async_test)]
    async fn direct_dataflow_does_not_flush_rows_after_monitor_error(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let batches = Rc::new(RefCell::new(Vec::new()));
        let spec = "in source\nout z\nz = dynamic(source: Int)"
            .parse::<DsrvSpecification>()
            .unwrap();
        let input = Box::pin(futures::stream::iter([Ok(crate::InputBatch::from(
            crate::InputUpdate::new(
                VarName::new("source"),
                Value::Str("not a valid expression".into()),
            ),
        ))]));
        let runtime = DataflowRuntimeBuilder::<DsrvSpecification>::new()
            .executor(executor)
            .model(spec)
            .input(input)
            .output_writer(recording_writer(Rc::clone(&batches)))
            .build()
            .await;

        let error = runtime.run().await.unwrap_err();

        assert!(error.to_string().contains("dynamic"));
        assert!(batches.borrow().is_empty());
    }

    #[apply(async_test)]
    async fn direct_dataflow_preserves_evaluation_error_when_cleanup_fails(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let closes = Rc::new(Cell::new(0));
        let spec = "in source\nout z\nz = dynamic(source: Int)"
            .parse::<DsrvSpecification>()
            .unwrap();
        let input = Box::pin(futures::stream::iter([Ok(crate::InputBatch::from(
            crate::InputUpdate::new(
                VarName::new("source"),
                Value::Str("not a valid expression".into()),
            ),
        ))]));
        let output_writer = OutputWriter::from_sink(CleanupFailingSink {
            flush_error: Some(OutputError::backend("dataflow flush failed")),
            close_error: Some(OutputError::backend("dataflow close failed")),
            closes: Rc::clone(&closes),
        });
        let runtime = DataflowRuntimeBuilder::<DsrvSpecification>::new()
            .executor(executor)
            .model(spec)
            .input(input)
            .output_writer(output_writer)
            .build()
            .await;

        let error = runtime.run().await.unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("dynamic"));
        assert!(message.contains("dataflow flush failed"));
        assert!(message.contains("dataflow close failed"));
        assert_eq!(closes.get(), 1);
    }

    #[apply(async_test)]
    async fn direct_dataflow_treats_closed_sink_as_intentional_early_completion(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let spec = "in x\nout z\nz = x".parse::<DsrvSpecification>().unwrap();
        let runtime = DataflowRuntimeBuilder::<DsrvSpecification>::new()
            .executor(executor)
            .model(spec)
            .input(map::input_stream(BTreeMap::from([(
                VarName::new("x"),
                vec![Value::Int(1)],
            )])))
            .output_writer(failing_writer(OutputError::Closed))
            .build()
            .await;

        runtime.run().await.unwrap();
    }

    #[apply(async_test)]
    async fn direct_dataflow_returns_non_closed_sink_errors(executor: Rc<LocalExecutor<'static>>) {
        let spec = "in x\nout z\nz = x".parse::<DsrvSpecification>().unwrap();
        let runtime = DataflowRuntimeBuilder::<DsrvSpecification>::new()
            .executor(executor)
            .model(spec)
            .input(map::input_stream(BTreeMap::from([(
                VarName::new("x"),
                vec![Value::Int(1)],
            )])))
            .output_writer(failing_writer(OutputError::backend("sink failed")))
            .build()
            .await;

        let error = runtime.run().await.unwrap_err();

        assert!(error.to_string().contains("sink failed"));
    }
}
