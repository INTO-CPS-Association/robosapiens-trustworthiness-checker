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

use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use crate::core::{
    ExecutionPolicy, InputStream, OutputBatch, OutputError, OutputWriter, Runtime, Value,
};
use crate::dataflow::{DataflowCompilationError, DataflowMonitor};
use crate::runtime::builder::RuntimeBuilder;
use async_trait::async_trait;
use futures::StreamExt;
use futures::future::LocalBoxFuture;
use smol::LocalExecutor;

const DATAFLOW_RUNTIME_BATCH_SIZE: usize = 256;

/// Owns and asynchronously drives one compiled dataflow monitor.
pub struct DataflowRuntime {
    input_stream: InputStream<Value>,
    output_writer: OutputWriter<Value>,
    monitor: Result<DataflowMonitor, DataflowCompilationError>,
    execution_policy: ExecutionPolicy,
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

    /// Send monitor rows directly to an already-open output writer.
    pub fn output_writer(self, output_writer: OutputWriter<Value>) -> Self {
        Self {
            output_writer: Some(output_writer),
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
            let model = self.model.expect("Model not supplied");
            let monitor = DataflowMonitor::try_from(model);
            let input_stream = self.input.expect("Input stream not supplied");
            let output_writer = self.output_writer.expect("Output writer not supplied");
            DataflowRuntime {
                input_stream,
                output_writer,
                monitor,
                execution_policy: self.execution_policy,
            }
        })
    }
}

#[async_trait(?Send)]
impl Runtime for DataflowRuntime {
    async fn run_boxed(self: Box<Self>) -> anyhow::Result<()> {
        let monitor = self.monitor?;
        run_direct_dataflow_engine(
            self.input_stream,
            monitor,
            self.output_writer,
            self.execution_policy,
        )
        .await
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
    let mut closed = false;

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
                    match engine.flush().await {
                        Ok(()) => {}
                        Err(sink_error) if sink_error.is_closed() => {
                            closed = true;
                            break 'input;
                        }
                        Err(sink_error) => {
                            error = Some(sink_error.into());
                            break 'input;
                        }
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
                match engine.flush().await {
                    Ok(()) => {}
                    Err(sink_error) if sink_error.is_closed() => {
                        closed = true;
                        break 'input;
                    }
                    Err(sink_error) => {
                        error = Some(sink_error.into());
                        break 'input;
                    }
                }
            }
        }
    }

    // Do not send rows accumulated before a monitor or input error. In
    // particular, a failed evaluation must never turn its incomplete row into
    // an output batch. The writer itself still gets its final flush and close.
    if error.is_none() && !closed && engine.pending_rows != 0 {
        match engine.flush().await {
            Ok(()) => {}
            Err(sink_error) if sink_error.is_closed() => {}
            Err(sink_error) => error = Some(sink_error.into()),
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
        let rows = OutputBatch::packed_rows(Arc::clone(&self.output_layout), values)?;
        self.output_writer.send(rows).await?;
        self.output_values = Vec::with_capacity(self.output_value_capacity);
        self.pending_rows = 0;
        Ok(())
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
    use futures::Sink;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;

    use crate::VarName;
    use crate::core::{OutputBackend, OutputBatch};

    use crate::io::map;
    use crate::io::testing::{limited_null_output, manual_output};
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
