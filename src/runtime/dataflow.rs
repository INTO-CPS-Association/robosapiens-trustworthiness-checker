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
use crate::dataflow::{
    ContextTransferPolicy, DataflowCompilationError, DataflowMonitor, DataflowProgram,
    InterfaceRevision, MonitorReconfigurationPlan, MonitorRevision, ReconfigurationReport,
};

use crate::io::InputPipelineReconfigurationPlan;
use crate::io::output::{OutputPipelineReconfigurationPlan, OutputPipelineSession};
use crate::io::reconfigurable_input::{
    InputPipelineSession, ReconfigurableInput, ReconfigurableInputItem,
};
use crate::io::{InputPipeline, OutputBackendBuilder, ReconfigurationRequest};
use crate::runtime::builder::RuntimeBuilder;

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
    pub monitor_changed: bool,
    pub interface_changed: bool,
    pub monitor_revision: MonitorRevision,
    pub interface_revision: InterfaceRevision,
}

/// Releases producers waiting at the global reconfiguration command barrier.
pub type ReconfigurationAckSink = async_unsync::bounded::Sender<ReconfigurationAck>;

type ReconfigurationCompiler = Rc<dyn Fn(&str) -> anyhow::Result<CompiledDefinition>>;

struct CompiledDefinition {
    program: DataflowProgram,
    input_vars: BTreeSet<crate::VarName>,
    output_vars: BTreeSet<crate::VarName>,
    auxiliary_vars: BTreeSet<crate::VarName>,
}

#[derive(Clone, Copy)]
struct ExecutionConfiguration {
    quickening: bool,
    #[cfg(feature = "jit")]
    jit: Option<JitConfig>,
}

impl ExecutionConfiguration {
    fn configure(self, monitor: &mut DataflowMonitor) {
        monitor.set_quickening(self.quickening);
        #[cfg(feature = "jit")]
        if let Some(config) = self.jit {
            monitor.enable_jit(config);
        }
    }
}

struct RuntimeReconfigurationPlan {
    monitor: MonitorReconfigurationPlan,
    input: InputPipelineReconfigurationPlan,
    output: OutputPipelineReconfigurationPlan,
}

struct RuntimeReconfigurationContext {
    input: ReconfigurableInput<Value>,
    output_builder: OutputBackendBuilder<Value>,
    compiler: ReconfigurationCompiler,
    execution_configuration: ExecutionConfiguration,
    transfer_policy: ContextTransferPolicy,
    acknowledgements: Option<ReconfigurationAckSink>,
}

struct ActiveRuntime {
    engine: DirectDataflowEngine<OutputPipelineSession<Value>>,
    input: InputPipelineSession<Value>,
}

fn plan_runtime_reconfiguration(
    active: &ActiveRuntime,
    context: &RuntimeReconfigurationContext,
    request: ReconfigurationRequest,
) -> anyhow::Result<RuntimeReconfigurationPlan> {
    request.validate_structure()?;
    let compiled = (context.compiler)(&request.specification)?;
    let input_resolution = context
        .input
        .pipeline()
        .resolve(&compiled.input_vars, Some(&request.input))?;
    let input = context
        .input
        .pipeline()
        .plan_reconfiguration(active.input.active(), input_resolution)?;
    let output_resolution = context.output_builder.resolve(
        &compiled.output_vars,
        &compiled.auxiliary_vars,
        Some(&request.output),
    )?;
    let output = context
        .output_builder
        .pipeline()
        .plan_reconfiguration(active.engine.output.resolved(), output_resolution)?;
    let monitor = active
        .engine
        .monitor
        .plan_reconfiguration(compiled.program, context.transfer_policy);

    Ok(RuntimeReconfigurationPlan {
        monitor,
        input,
        output,
    })
}
enum DataflowInput {
    Standard(InputStream<Value>),
    Reconfigurable(InputPipelineSession<Value>),
}

/// Owns and asynchronously drives one compiled dataflow monitor.
pub struct DataflowRuntime {
    input_stream: DataflowInput,
    output_writer: Option<OutputWriter<Value>>,
    output_session: Option<OutputPipelineSession<Value>>,
    monitor: Result<DataflowMonitor, anyhow::Error>,
    execution_policy: ExecutionPolicy,
    reconfiguration: Option<RuntimeReconfigurationContext>,
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

/// Configures a dataflow runtime whose monitor and live I/O sessions are reconfigured at
/// validated root-command boundaries.
pub struct ReconfigurableDataflowRuntimeBuilder<S>
where
    S: 'static,
    DataflowProgram: TryFrom<S, Error = DataflowCompilationError>,
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
    DataflowProgram: TryFrom<S, Error = DataflowCompilationError>,
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
    DataflowProgram: TryFrom<S, Error = DataflowCompilationError>,
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
            transfer_policy: ContextTransferPolicy::MatchingStreamState,
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

            let input =
                match ReconfigurableInput::new(input_pipeline, self.reconf_topic, executor.clone())
                {
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

            let compiled = match compile_model(model) {
                Ok(compiled) => compiled,
                Err(error) => return failed_dataflow_runtime(policy, error.into()),
            };
            let execution_configuration = ExecutionConfiguration {
                quickening: self.quickening,
                #[cfg(feature = "jit")]
                jit: self.jit_config,
            };
            let mut monitor = DataflowMonitor::from_program(compiled.program);
            execution_configuration.configure(&mut monitor);
            monitor.set_reconfiguration_transfer_policy(self.transfer_policy);

            let input_session = match input.open_session(resolved_input.clone()).await {
                Ok(session) => session,
                Err(error) => return failed_dataflow_runtime(policy, error),
            };
            let output_session = match output_builder.open_session(resolved_output.clone()).await {
                Ok(session) => session,
                Err(error) => {
                    return failed_dataflow_runtime(
                        policy,
                        anyhow::anyhow!(
                            "reconfigurable output pipeline could not be opened: {error}"
                        ),
                    );
                }
            };

            let compiler: ReconfigurationCompiler = Rc::new(move |source| {
                let model = parse_spec(source)?;
                compile_model(model).map_err(anyhow::Error::from)
            });

            DataflowRuntime {
                input_stream: DataflowInput::Reconfigurable(input_session),
                output_writer: None,
                output_session: Some(output_session),
                monitor: Ok(monitor),
                execution_policy: policy,
                reconfiguration: Some(RuntimeReconfigurationContext {
                    input,
                    output_builder,
                    compiler,
                    execution_configuration,
                    transfer_policy: self.transfer_policy,
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
                output_session: None,
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
            mut output_session,
            monitor,
            execution_policy,
            reconfiguration,
            startup_error,
        } = *self;

        if let Some(error) = startup_error {
            if let Some(writer) = output_writer.as_mut() {
                return finish_direct_output(writer, Some(error)).await;
            }
            if let Some(session) = output_session.as_mut() {
                return finish_reconfigurable_output_session(session, Some(error)).await;
            }
            return Err(error);
        }

        match reconfiguration {
            Some(state) => {
                let Some(mut output_session) = output_session else {
                    return Err(anyhow::anyhow!(
                        "reconfigurable dataflow runtime did not receive its output session"
                    ));
                };
                let DataflowInput::Reconfigurable(input) = input_stream else {
                    return finish_reconfigurable_output_session(
                        &mut output_session,
                        Some(anyhow::anyhow!(
                            "reconfigurable dataflow runtime did not receive its control input"
                        )),
                    )
                    .await;
                };
                let monitor = match monitor {
                    Ok(monitor) => monitor,
                    Err(error) => {
                        return finish_reconfigurable_output_session(
                            &mut output_session,
                            Some(error),
                        )
                        .await;
                    }
                };
                run_reconfigurable_dataflow(input, monitor, output_session, execution_policy, state)
                    .await
            }
            None => {
                let Some(mut output_writer) = output_writer else {
                    return Err(anyhow::anyhow!(
                        "dataflow runtime output writer is not configured"
                    ));
                };
                let monitor = match monitor {
                    Ok(monitor) => monitor,
                    Err(error) => {
                        return finish_direct_output(&mut output_writer, Some(error)).await;
                    }
                };
                let DataflowInput::Standard(input) = input_stream else {
                    return finish_direct_output(
                        &mut output_writer,
                        Some(anyhow::anyhow!(
                            "ordinary dataflow runtime received a reconfigurable input"
                        )),
                    )
                    .await;
                };
                run_direct_dataflow_engine(input, monitor, output_writer, execution_policy).await
            }
        }
    }
}

trait DataflowOutput {
    async fn send_output(&mut self, batch: OutputBatch<Value>) -> Result<(), OutputError>;
    async fn flush_output(&mut self) -> Result<(), OutputError>;
    async fn close_output(&mut self) -> Result<(), OutputError>;
    fn output_error(&self) -> Option<&OutputError>;
}

impl DataflowOutput for OutputWriter<Value> {
    async fn send_output(&mut self, batch: OutputBatch<Value>) -> Result<(), OutputError> {
        self.send(batch).await
    }

    async fn flush_output(&mut self) -> Result<(), OutputError> {
        self.flush().await
    }

    async fn close_output(&mut self) -> Result<(), OutputError> {
        self.close().await
    }

    fn output_error(&self) -> Option<&OutputError> {
        self.error()
    }
}

impl DataflowOutput for OutputPipelineSession<Value> {
    async fn send_output(&mut self, batch: OutputBatch<Value>) -> Result<(), OutputError> {
        self.send(batch).await
    }

    async fn flush_output(&mut self) -> Result<(), OutputError> {
        self.flush().await
    }

    async fn close_output(&mut self) -> Result<(), OutputError> {
        self.close().await
    }

    fn output_error(&self) -> Option<&OutputError> {
        self.writer().error()
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

    finish_direct_output(&mut engine.output, error).await
}

async fn finish_dataflow_output<O: DataflowOutput>(output: &mut O) -> anyhow::Result<()> {
    let flush_result = output.flush_output().await;
    let close_result = output.close_output().await;
    let error = match close_result {
        Err(error) if !error.is_closed() => Some(error),
        _ => match flush_result {
            Err(error) if !error.is_closed() => Some(error),
            _ => None,
        },
    };
    error.map_or(Ok(()), |error| Err(error.into()))
}

async fn finish_direct_output<O: DataflowOutput>(
    output: &mut O,
    primary: Option<anyhow::Error>,
) -> anyhow::Result<()> {
    let cleanup = finish_dataflow_output(output).await;
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
        output_session: None,
        monitor: Err(anyhow::anyhow!("dataflow runtime startup failed")),
        execution_policy,
        reconfiguration: None,
        startup_error: Some(error),
    }
}

fn compile_model<S>(model: S) -> Result<CompiledDefinition, DataflowCompilationError>
where
    S: Specification + 'static,
    DataflowProgram: TryFrom<S, Error = DataflowCompilationError>,
{
    let input_vars = model.input_vars();
    let output_vars = model.output_vars();
    let auxiliary_vars = model.aux_vars();
    let program = DataflowProgram::try_from(model)?;
    Ok(CompiledDefinition {
        program,
        input_vars,
        output_vars,
        auxiliary_vars,
    })
}

async fn acknowledge_reconfiguration(
    context: &RuntimeReconfigurationContext,
    report: &ReconfigurationReport,
) -> anyhow::Result<()> {
    let acknowledgement = ReconfigurationAck {
        monitor_changed: report.monitor_changed,
        interface_changed: report.interface_changed,
        monitor_revision: report.monitor_revision,
        interface_revision: report.interface_revision,
    };
    info!(
        monitor_revision = %acknowledgement.monitor_revision,
        interface_revision = %acknowledgement.interface_revision,
        monitor_changed = acknowledgement.monitor_changed,
        interface_changed = acknowledgement.interface_changed,
        "acknowledging dataflow reconfiguration"
    );
    if let Some(sink) = &context.acknowledgements {
        sink.send(acknowledgement).await.map_err(|_| {
            anyhow::anyhow!(
                "reconfiguration acknowledgement channel is closed, so the producer barrier cannot be honoured"
            )
        })?;
    }
    Ok(())
}

fn writer_is_closed<O: DataflowOutput>(output: &O) -> bool {
    output.output_error().is_some_and(OutputError::is_closed)
}

async fn flush_reconfigurable_output<O: DataflowOutput>(
    engine: &mut DirectDataflowEngine<O>,
) -> anyhow::Result<()> {
    match engine.flush().await {
        Ok(()) if writer_is_closed(&engine.output) => Err(anyhow::anyhow!(
            "dataflow output writer closed while flushing"
        )),
        Ok(()) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

async fn flush_reconfiguration_barrier(
    engine: &mut DirectDataflowEngine<OutputPipelineSession<Value>>,
) -> Result<(), OutputError> {
    if engine.pending_rows != 0 {
        engine.flush().await?;
    }
    if writer_is_closed(&engine.output) {
        return Err(OutputError::Closed);
    }
    Ok(())
}

async fn finish_reconfigurable_output_session(
    output: &mut OutputPipelineSession<Value>,
    primary: Option<anyhow::Error>,
) -> anyhow::Result<()> {
    finish_direct_output(output, primary).await
}

async fn finish_reconfigurable_engine(
    mut engine: DirectDataflowEngine<OutputPipelineSession<Value>>,
    primary: Option<anyhow::Error>,
) -> anyhow::Result<()> {
    let mut error = primary;
    if engine.pending_rows != 0 {
        if let Err(flush_error) = engine.flush().await {
            error = Some(match error {
                Some(primary) => combine_errors(primary, flush_error.into()),
                None => flush_error.into(),
            });
        }
    }
    if let Err(cleanup) = finish_dataflow_output(&mut engine.output).await {
        error = Some(match error {
            Some(primary) => combine_errors(primary, cleanup),
            None => cleanup,
        });
    }
    error.map_or(Ok(()), Err)
}

async fn reconfiguration_failure(
    mut engine: DirectDataflowEngine<OutputPipelineSession<Value>>,
    primary: anyhow::Error,
) -> anyhow::Error {
    let mut error = primary;
    if engine.pending_rows != 0 {
        if let Err(flush_error) = engine.flush().await {
            error = combine_errors(error, flush_error.into());
        }
    }
    if let Err(cleanup) = finish_dataflow_output(&mut engine.output).await {
        error = combine_errors(error, cleanup);
    }
    error
}

async fn run_reconfigurable_dataflow(
    input: InputPipelineSession<Value>,
    monitor: DataflowMonitor,
    output_session: OutputPipelineSession<Value>,
    execution_policy: ExecutionPolicy,
    context: RuntimeReconfigurationContext,
) -> anyhow::Result<()> {
    let mut active = ActiveRuntime {
        engine: DirectDataflowEngine::new(monitor, output_session),
        input,
    };

    loop {
        let Some(item) = active.input.next().await else {
            return finish_reconfigurable_engine(active.engine, None).await;
        };
        let item = match item {
            Ok(item) => item,
            Err(error) => {
                return finish_reconfigurable_engine(active.engine, Some(error)).await;
            }
        };

        match item {
            ReconfigurableInputItem::Data(batch) => {
                if let Err(error) =
                    evaluate_reconfigurable_batch(&mut active.engine, &batch, execution_policy)
                        .await
                {
                    return finish_reconfigurable_engine(active.engine, Some(error)).await;
                }
            }
            ReconfigurableInputItem::Reconfigure(request) => {
                let plan = match plan_runtime_reconfiguration(&active, &context, request) {
                    Ok(plan) => plan,
                    Err(error) => {
                        return Err(reconfiguration_failure(active.engine, error).await);
                    }
                };
                active =
                    apply_runtime_reconfiguration(active, &context, plan, execution_policy).await?;
            }
        }
    }
}

async fn apply_runtime_reconfiguration(
    active: ActiveRuntime,
    context: &RuntimeReconfigurationContext,
    plan: RuntimeReconfigurationPlan,
    execution_policy: ExecutionPolicy,
) -> anyhow::Result<ActiveRuntime> {
    let ActiveRuntime {
        mut engine,
        mut input,
    } = active;
    let RuntimeReconfigurationPlan {
        monitor,
        input: input_plan,
        output: output_plan,
    } = plan;
    let io_interface_changed = input_plan.is_changed() || output_plan.is_changed();

    let mut removed = match input.remove_sources(&input_plan) {
        Ok(removed) => removed,
        Err(error) => {
            drop(input);
            return Err(reconfiguration_failure(engine, error).await);
        }
    };
    while let Some(item) = removed.next().await {
        let batch = match item {
            Ok(ReconfigurableInputItem::Data(batch)) => batch,
            Ok(ReconfigurableInputItem::Reconfigure(_)) => {
                drop(input);
                return Err(reconfiguration_failure(
                    engine,
                    anyhow::anyhow!(
                        "a second reconfiguration command arrived while removing input sources"
                    ),
                )
                .await);
            }
            Err(error) => {
                drop(input);
                return Err(reconfiguration_failure(engine, error).await);
            }
        };
        if let Err(error) =
            evaluate_reconfigurable_batch(&mut engine, &batch, execution_policy).await
        {
            drop(input);
            return Err(reconfiguration_failure(engine, error).await);
        }
    }

    if let Err(error) = flush_reconfiguration_barrier(&mut engine).await {
        drop(input);
        return Err(reconfiguration_failure(engine, error.into()).await);
    }

    if let Err(error) = input.add_sources_and_commit(input_plan).await {
        drop(input);
        return Err(reconfiguration_failure(engine, error).await);
    }
    if let Err(error) = engine.output.apply_reconfiguration(output_plan).await {
        drop(input);
        return Err(reconfiguration_failure(engine, error.into()).await);
    }

    let report = match engine.monitor.apply_reconfiguration_plan(
        monitor,
        io_interface_changed,
        |candidate| context.execution_configuration.configure(candidate),
    ) {
        Ok(report) => report,
        Err(error) => {
            drop(input);
            return Err(reconfiguration_failure(engine, error.into()).await);
        }
    };

    engine.rebuild_monitor_layout();
    if let Err(error) = acknowledge_reconfiguration(context, &report).await {
        drop(input);
        return Err(reconfiguration_failure(engine, error).await);
    }

    Ok(ActiveRuntime { engine, input })
}
async fn evaluate_reconfigurable_batch(
    engine: &mut DirectDataflowEngine<OutputPipelineSession<Value>>,
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

struct DirectDataflowEngine<O> {
    monitor: DataflowMonitor,
    output: O,
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

impl<O: DataflowOutput> DirectDataflowEngine<O> {
    fn new(monitor: DataflowMonitor, output: O) -> Self {
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
            output,
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

    fn rebuild_monitor_layout(&mut self) {
        let output_layout: Arc<[crate::VarName]> = self.monitor.output_vars().to_vec().into();
        let output_value_capacity = DATAFLOW_RUNTIME_BATCH_SIZE.saturating_mul(output_layout.len());
        let input_ids: BTreeMap<crate::VarName, usize> = self
            .monitor
            .input_vars()
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, var)| (var, index))
            .collect();

        self.output_layout = output_layout;
        self.output_value_capacity = output_value_capacity;
        self.output_values.clear();
        if self.output_values.capacity() < output_value_capacity {
            self.output_values
                .reserve(output_value_capacity - self.output_values.capacity());
        }
        self.pending_rows = 0;
        self.input_row = vec![Value::NoVal; self.monitor.input_vars().len()];
        self.output_row = vec![Value::NoVal; self.output_layout.len()];
        self.input_ids = input_ids;
        self.cached_layout_vars = Vec::with_capacity(self.input_ids.len());
        self.cached_layout_slots = Vec::with_capacity(self.input_ids.len());
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
        match self.output.send_output(rows).await {
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

    /// Counts destination opens and closes so a replacement can be observed
    /// without inspecting runtime internals.
    #[derive(Default)]
    struct SessionCounts {
        opens: usize,
        closes: usize,
    }

    struct CountingBackend {
        counts: Rc<RefCell<SessionCounts>>,
    }

    #[async_trait(?Send)]
    impl crate::OutputBackend for CountingBackend {
        type Val = Value;

        async fn open(
            &self,
            _interface: crate::OutputInterface,
        ) -> Result<OutputWriter<Value>, OutputError> {
            self.counts.borrow_mut().opens += 1;
            let counts = Rc::clone(&self.counts);
            Ok(OutputWriter::from_sink(CountingSink { counts }))
        }
    }

    struct CountingSink {
        counts: Rc<RefCell<SessionCounts>>,
    }

    impl Sink<OutputBatch<Value>> for CountingSink {
        type Error = OutputError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, _batch: OutputBatch<Value>) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.counts.borrow_mut().closes += 1;
            Poll::Ready(Ok(()))
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
    async fn reconfigurable_dataflow_ack_tracks_independent_revisions(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let spec_src = "in x: Int\nout z: Int\nz = x";
        let model = spec_src.parse::<DsrvSpecification>().unwrap();
        let (x_sender, x_fanout) = Fanout::<Value>::new();
        let (control_sender, control_fanout) = Fanout::<Value>::new();
        let input_source = input_source_with_control(
            BTreeMap::from([(VarName::new("x"), x_fanout)]),
            control_fanout,
        )
        .with_reconfiguration_route("control")
        .unwrap();
        let (ack_sender, mut acknowledgements) =
            bounded::channel::<ReconfigurationAck>(2).into_split();

        let runtime = ReconfigurableDataflowRuntimeBuilder::<DsrvSpecification>::new()
            .parse_spec(|source| source.parse().map_err(anyhow::Error::from))
            .executor(executor.clone())
            .model(model)
            .input_pipeline(InputPipeline::new(input_source))
            .output_builder(OutputBackendBuilder::new(OutputBackendConfig::null()))
            .reconf_topic("control")
            .acknowledgements(ack_sender)
            .build()
            .await;
        let task = executor.spawn(runtime.run());

        control_sender
            .send(Value::Str(
                serde_json::json!({"specification": spec_src})
                    .to_string()
                    .into(),
            ))
            .await;
        let unchanged = acknowledgements
            .recv()
            .await
            .expect("unchanged root reconfiguration acknowledgement should arrive");
        assert!(!unchanged.monitor_changed);
        assert!(!unchanged.interface_changed);
        assert_eq!(unchanged.monitor_revision, MonitorRevision(1));
        assert_eq!(unchanged.interface_revision, InterfaceRevision::INITIAL);

        control_sender
            .send(Value::Str(
                serde_json::json!({
                    "specification": spec_src,
                    "output": {"outputs": {"z": "/changed"}}
                })
                .to_string()
                .into(),
            ))
            .await;
        let interface_only = acknowledgements
            .recv()
            .await
            .expect("interface-only reconfiguration acknowledgement should arrive");
        assert!(!interface_only.monitor_changed);
        assert!(interface_only.interface_changed);
        assert_eq!(interface_only.monitor_revision, MonitorRevision(2));
        assert_eq!(interface_only.interface_revision, InterfaceRevision(1));

        drop(x_sender);
        drop(control_sender);
        tc_testutils::streams::with_timeout(task, 5, "revision acknowledgement runtime")
            .await
            .expect("revision acknowledgement runtime should terminate")
            .expect("revision acknowledgement runtime should succeed");
    }

    #[apply(async_test)]
    async fn accepted_noop_reconfiguration_keeps_input_and_output_live(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        // An exact no-op keeps both pipeline sessions open; only the monitor
        // revision advances for the accepted request.
        let spec_src = "in x: Int\nout z: Int\nz = x";
        let model = spec_src.parse::<DsrvSpecification>().unwrap();
        let (x_sender, x_fanout) = Fanout::<Value>::new();
        let (control_sender, control_fanout) = Fanout::<Value>::new();
        let input_source = input_source_with_control(
            BTreeMap::from([(VarName::new("x"), x_fanout)]),
            control_fanout,
        )
        .with_reconfiguration_route("control")
        .unwrap();
        let counts = Rc::new(RefCell::new(SessionCounts::default()));
        let (ack_sender, mut acknowledgements) =
            bounded::channel::<ReconfigurationAck>(1).into_split();

        let runtime = ReconfigurableDataflowRuntimeBuilder::<DsrvSpecification>::new()
            .parse_spec(|source| source.parse().map_err(anyhow::Error::from))
            .executor(executor.clone())
            .model(model)
            .input_pipeline(InputPipeline::new(input_source))
            .output_builder(OutputBackendBuilder::new(OutputBackendConfig::custom(
                CountingBackend {
                    counts: Rc::clone(&counts),
                },
            )))
            .reconf_topic("control")
            .acknowledgements(ack_sender)
            .build()
            .await;
        let task = executor.spawn(runtime.run());

        x_sender.send(Value::Int(1)).await;
        control_sender
            .send(Value::Str(
                serde_json::json!({"specification": spec_src})
                    .to_string()
                    .into(),
            ))
            .await;
        let ack = acknowledgements
            .recv()
            .await
            .expect("an exact request is still accepted and acknowledged");

        // Acknowledgement happens after the no-op plan is committed without
        // opening or closing another output owner.
        let counts = counts.borrow();
        assert_eq!(counts.opens, 1, "the output owner should stay open");
        assert_eq!(counts.closes, 0, "the output owner should stay unclosed");
        drop(counts);

        assert!(!ack.monitor_changed);
        assert!(!ack.interface_changed);
        assert_eq!(ack.monitor_revision, MonitorRevision(1));
        assert_eq!(ack.interface_revision, InterfaceRevision::INITIAL);

        // The original input session remains the one feeding the monitor.
        x_sender.send(Value::Int(2)).await;
        drop(x_sender);
        drop(control_sender);
        tc_testutils::streams::with_timeout(task, 5, "no-op reconfiguration runtime")
            .await
            .expect("no-op reconfiguration runtime should terminate")
            .expect("no-op reconfiguration runtime should succeed");
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
        let state = runtime.reconfiguration.as_ref().unwrap();
        let replacement = (state.compiler)("in x: Int\nout z: Int\nz = x + 2").unwrap();
        let mut replacement = DataflowMonitor::from_program(replacement.program);
        state.execution_configuration.configure(&mut replacement);
        assert!(!replacement.quickening_enabled());
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
