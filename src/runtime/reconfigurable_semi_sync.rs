use crate::core::{DeferrableStreamData, OutputWriter, Runtime, Specification, input};
use crate::io::reconfigurable_input::{
    ReconfigurableInput, ReconfigurableInputItem, ReconfigurableInputStream,
};
use crate::io::{
    InputConfiguration, InputPipeline, OutputBackendBuilder, OutputConfiguration,
    ReconfigurationRequest,
};
use crate::lang::core::{DependencyGraphExpr, DependencyGraphSpec};
use crate::runtime::{
    RuntimeBuilder,
    semi_sync::{
        ExprEvalutor, SemiSyncContext, SemiSyncOutput, SemiSyncRuntime, SemiSyncRuntimeBuilder,
    },
};
use crate::semantics::{AsyncConfig, MonitoringSemantics, StreamContext};
use crate::{InputStream, Value, VarName};

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use futures::{FutureExt, StreamExt, future::LocalBoxFuture};
use smol::LocalExecutor;
use std::{collections::BTreeMap, fmt::Debug, rc::Rc};
use tracing::{debug, info};

/// Builder for the reconfigurable semi-sync runtime. Input ownership and
/// control routing are kept in `ReconfigurableInput`; model replacement and
/// history transfer remain in this module.
#[derive(Clone)]
pub struct ReconfSemiSyncRuntimeBuilder<AC, MS>
where
    AC: AsyncConfig<Ctx = SemiSyncContext<AC>>,
    AC::Expr: DependencyGraphExpr + PartialEq + Debug,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
{
    executor: Option<Rc<LocalExecutor<'static>>>,
    model: Option<AC::Spec>,
    input_pipeline: Option<InputPipeline<AC::Val>>,
    resolved_input: Option<crate::io::config::ResolvedInput>,
    output_builder: Option<OutputBackendBuilder<AC::Val>>,
    resolved_output: Option<crate::io::output::ResolvedOutput>,
    reconf_topic: Option<String>,
    input_config: Option<InputConfiguration>,
    use_context_transfer: bool,
    starting_history: Option<BTreeMap<VarName, Vec<AC::Val>>>,
    parse_spec: Option<fn(&str) -> anyhow::Result<AC::Spec>>,
    setup_error: Option<String>,
    _marker: (std::marker::PhantomData<MS>, std::marker::PhantomData<AC>),
}

impl<AC, MS> RuntimeBuilder<AC::Spec, AC::Val> for ReconfSemiSyncRuntimeBuilder<AC, MS>
where
    AC: AsyncConfig<Val = Value, Ctx = SemiSyncContext<AC>>,
    AC::Expr: DependencyGraphExpr + PartialEq + Debug,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
{
    type Runtime = ReconfSemiSyncRuntime<AC, MS>;

    fn new() -> Self {
        Self {
            executor: None,
            model: None,
            input_pipeline: None,
            resolved_input: None,
            output_builder: None,
            resolved_output: None,
            reconf_topic: None,
            input_config: None,
            use_context_transfer: true,
            starting_history: None,
            parse_spec: None,
            setup_error: None,
            _marker: (std::marker::PhantomData, std::marker::PhantomData),
        }
    }

    fn executor(mut self, executor: Rc<LocalExecutor<'static>>) -> Self {
        self.executor = Some(executor);
        self
    }

    fn model(mut self, model: AC::Spec) -> Self {
        self.model = Some(model);
        self
    }

    fn input(self, _input: InputStream<AC::Val>) -> Self {
        self.with_setup_error("direct input streams are not supported by the reconfigurable runtime; configure an InputPipeline")
    }

    fn output_writer(self, _writer: OutputWriter<AC::Val>) -> Self {
        self.with_setup_error("direct output writers are not supported by the reconfigurable runtime; configure an output pipeline")
    }

    fn build(self) -> LocalBoxFuture<'static, Self::Runtime> {
        Box::pin(async move {
            let mut builder = self;
            let (mut input, resolved_input, mut setup_error) = match builder.finalize_input() {
                Ok((input, resolved_input)) => (Some(input), Some(resolved_input), None),
                Err(error) => (None, None, Some(error.to_string())),
            };
            builder.resolved_input = resolved_input;

            // Resolution is deliberately resource-free. Resolve the complete
            // initial monitor before opening either input or output; the
            // same ordering is used by replacement builders below.
            if setup_error.is_none()
                && builder.setup_error.is_none()
                && builder.resolved_output.is_none()
            {
                match (builder.output_builder.as_ref(), builder.model.as_ref()) {
                    (Some(output_builder), Some(model)) => {
                        match output_builder.resolve(model.output_vars(), model.aux_vars(), None) {
                            Ok(resolved) => builder.resolved_output = Some(resolved),
                            Err(error) => setup_error = Some(error.to_string()),
                        }
                    }
                    (None, _) => {
                        setup_error = Some("reconfigurable output builder is not configured".into())
                    }
                    (_, None) => {
                        setup_error = Some("reconfigurable runtime model is not configured".into())
                    }
                }
            }
            if setup_error.is_none() {
                setup_error = builder.setup_error.clone();
            }

            let mut input_stream = None;
            let mut output_writer = None;
            if setup_error.is_none() {
                let Some(executor) = builder.executor.clone() else {
                    setup_error = Some("reconfigurable runtime executor is not configured".into());
                    return ReconfSemiSyncRuntime {
                        builder,
                        input,
                        input_stream,
                        output: output_writer,
                        setup_error,
                        _marker: std::marker::PhantomData,
                    };
                };
                let input_ref = input
                    .as_ref()
                    .expect("resolved reconfigurable input must be present");
                let resolved_input = builder
                    .resolved_input
                    .clone()
                    .expect("resolved reconfigurable input plan must be present");
                let output_builder = builder
                    .output_builder
                    .clone()
                    .expect("resolved reconfigurable output builder must be present")
                    .executor(executor);
                let resolved_output = builder
                    .resolved_output
                    .clone()
                    .expect("resolved reconfigurable output plan must be present");

                // Input is opened during build so callers can safely send the
                // first manual tick immediately after spawning the runtime.
                // Both opens still start only after all resolution is complete.
                let (input_result, output_result) = futures::join!(
                    input_ref.open_stream(resolved_input),
                    output_builder.open(resolved_output),
                );
                let input_result = input_result.map_err(|error| {
                    let message =
                        format!("reconfigurable input stream could not be opened: {error:#}");
                    error.context(message)
                });
                let output_result = output_result.map_err(|error| {
                    let message =
                        format!("reconfigurable output pipeline could not be opened: {error}");
                    anyhow::Error::new(error).context(message)
                });
                match (input_result, output_result) {
                    (Ok(opened_input), Ok(opened_output)) => {
                        input_stream = Some(opened_input);
                        output_writer = Some(opened_output);
                    }
                    (Err(input_error), Ok(mut opened_output)) => {
                        let cleanup = opened_output.close().await;
                        drop(opened_output);
                        drop(input.take());
                        setup_error = Some(match cleanup {
                            Ok(()) => input_error,
                            Err(cleanup_error) => anyhow!(
                                "{input_error}; additionally: reconfigurable output cleanup failed: {cleanup_error}"
                            ),
                        }
                        .to_string());
                    }
                    (Ok(opened_input), Err(output_error)) => {
                        drop(opened_input);
                        drop(input.take());
                        setup_error = Some(output_error.to_string());
                    }
                    (Err(input_error), Err(output_error)) => {
                        drop(input.take());
                        setup_error = Some(
                            anyhow!(
                                "{output_error}; additionally: reconfigurable input opening failed: {input_error}"
                            )
                            .to_string(),
                        );
                    }
                }
            }

            ReconfSemiSyncRuntime {
                builder,
                input,
                input_stream,
                output: output_writer,
                setup_error,
                _marker: std::marker::PhantomData,
            }
        })
    }
}

impl<AC, MS> ReconfSemiSyncRuntimeBuilder<AC, MS>
where
    AC: AsyncConfig<Val = Value, Ctx = SemiSyncContext<AC>>,
    AC::Expr: DependencyGraphExpr + PartialEq + Debug,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
{
    pub fn parse_spec(mut self, parse_spec: fn(&str) -> anyhow::Result<AC::Spec>) -> Self {
        self.parse_spec = Some(parse_spec);
        self
    }

    pub fn input_pipeline(mut self, pipeline: InputPipeline<AC::Val>) -> Self {
        self.input_pipeline = Some(pipeline);
        self
    }

    pub fn output_builder(mut self, output_builder: OutputBackendBuilder<AC::Val>) -> Self {
        self.output_builder = Some(output_builder);
        self.resolved_output = None;
        self
    }

    pub fn reconf_topic(mut self, reconf_topic: String) -> Self {
        self.reconf_topic = Some(reconf_topic);
        self
    }

    pub fn input_config(mut self, input_config: InputConfiguration) -> Self {
        self.input_config = Some(input_config);
        self
    }

    pub fn use_context_transfer(mut self, use_context_transfer: bool) -> Self {
        self.use_context_transfer = use_context_transfer;
        self
    }

    fn with_setup_error(mut self, message: impl Into<String>) -> Self {
        self.setup_error = Some(message.into());
        self
    }

    fn finalize_input(
        &self,
    ) -> anyhow::Result<(
        ReconfigurableInput<AC::Val>,
        crate::io::config::ResolvedInput,
    )> {
        if let Some(error) = self.setup_error.as_ref() {
            return Err(anyhow!(error.clone()));
        }
        let pipeline = self
            .input_pipeline
            .clone()
            .ok_or_else(|| anyhow!("reconfigurable input pipeline is not configured"))?;
        let executor = self
            .executor
            .clone()
            .ok_or_else(|| anyhow!("reconfigurable runtime executor is not configured"))?;
        let input = ReconfigurableInput::new(pipeline, self.reconf_topic.clone(), executor)
            .context("reconfigurable input could not be configured")?;
        let resolved_input = match self.resolved_input.clone() {
            Some(resolved) => resolved,
            None => input
                .pipeline()
                .resolve(&self.model_ref()?.input_vars(), self.input_config.as_ref())?,
        };
        Ok((input, resolved_input))
    }

    fn model_ref(&self) -> anyhow::Result<&AC::Spec> {
        self.model
            .as_ref()
            .ok_or_else(|| anyhow!("reconfigurable runtime model is not configured"))
    }
}

pub struct ReconfSemiSyncRuntime<AC, MS>
where
    AC: AsyncConfig<Ctx = SemiSyncContext<AC>>,
    AC::Expr: DependencyGraphExpr + PartialEq + Debug,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
{
    builder: ReconfSemiSyncRuntimeBuilder<AC, MS>,
    input: Option<ReconfigurableInput<AC::Val>>,
    input_stream: Option<ReconfigurableInputStream<AC::Val>>,
    output: Option<OutputWriter<AC::Val>>,
    setup_error: Option<String>,
    _marker: std::marker::PhantomData<MS>,
}

impl<AC, MS> ReconfSemiSyncRuntime<AC, MS>
where
    AC: AsyncConfig<Val = Value, Ctx = SemiSyncContext<AC>>,
    AC::Expr: DependencyGraphExpr + PartialEq + Debug,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
{
    async fn setup_inner_monitor(
        monitor: SemiSyncRuntime<AC, MS>,
    ) -> anyhow::Result<(
        SemiSyncOutput<AC::Val>,
        SemiSyncContext<AC>,
        Vec<ExprEvalutor<AC, MS>>,
    )> {
        monitor.setup_runtime_without_input().await
    }

    /// Explicitly retain context by variable identity and align every retained
    /// history to the longest retained history with `NoVal` on the left.
    fn transfer_context(
        &self,
        context: &SemiSyncContext<AC>,
        next_model: &AC::Spec,
    ) -> BTreeMap<VarName, Vec<AC::Val>> {
        if !self.builder.use_context_transfer {
            return BTreeMap::new();
        }
        let mut retained = context.get_retained_history();
        let variables = next_model.var_names();
        retained.retain(|variable, _| variables.contains(variable));
        for variable in variables {
            retained.entry(variable).or_default();
        }
        let longest = retained.values().map(Vec::len).max().unwrap_or(0);
        retained
            .into_iter()
            .map(|(variable, history)| {
                let padding = longest.saturating_sub(history.len());
                let mut aligned = vec![AC::Val::no_val_value(); padding];
                aligned.extend(history);
                (variable, aligned)
            })
            .collect()
    }

    fn log_model_changes(&self, old_model: &AC::Spec, new_model: &AC::Spec) {
        let old_inputs = old_model.input_vars();
        let new_inputs = new_model.input_vars();
        let added_inputs = new_inputs.difference(&old_inputs).collect::<Vec<_>>();
        let removed_inputs = old_inputs.difference(&new_inputs).collect::<Vec<_>>();
        if !added_inputs.is_empty() || !removed_inputs.is_empty() {
            info!(
                ?added_inputs,
                ?removed_inputs,
                "Reconfiguration input set changed"
            );
        }

        let old_outputs = old_model.output_vars();
        let new_outputs = new_model.output_vars();
        let added_outputs = new_outputs.difference(&old_outputs).collect::<Vec<_>>();
        let removed_outputs = old_outputs.difference(&new_outputs).collect::<Vec<_>>();
        if !added_outputs.is_empty() || !removed_outputs.is_empty() {
            info!(
                ?added_outputs,
                ?removed_outputs,
                "Reconfiguration output set changed"
            );
        }

        let changed_expressions = old_model
            .var_names()
            .intersection(&new_model.var_names())
            .filter(|variable| old_model.var_expr(variable) != new_model.var_expr(variable))
            .cloned()
            .collect::<Vec<_>>();
        if !changed_expressions.is_empty() {
            info!(?changed_expressions, "Reconfiguration expressions changed");
        }
    }

    fn resolve_output(
        &self,
        builder: &OutputBackendBuilder<AC::Val>,
        model: &AC::Spec,
        request: Option<&OutputConfiguration>,
    ) -> anyhow::Result<crate::io::output::ResolvedOutput> {
        builder.resolve(model.output_vars(), model.aux_vars(), request)
    }

    /// Parse, validate, resolve, transfer, and prepare the replacement monitor.
    /// This method is the semantic center of monitor reconfiguration.
    async fn handle_reconfig_input(
        &mut self,
        input: &ReconfigurableInput<AC::Val>,
        request: ReconfigurationRequest,
        context: &mut SemiSyncContext<AC>,
    ) -> anyhow::Result<Option<ReconfSemiSyncRuntimeBuilder<AC, MS>>> {
        request.validate_structure()?;
        let parse_spec = self
            .builder
            .parse_spec
            .ok_or_else(|| anyhow!("reconfiguration parser is not configured"))?;
        let next_model = parse_spec(&request.specification)
            .map_err(|error| anyhow!("failed to parse reconfiguration command: {error}"))?;
        let old_model = self.builder.model_ref()?.clone();
        self.log_model_changes(&old_model, &next_model);

        let mut next_builder = self.builder.clone().model(next_model.clone());
        let resolved_input = input
            .pipeline()
            .resolve(&next_model.input_vars(), Some(&request.input))?;
        next_builder.resolved_input = Some(resolved_input);
        next_builder.input_config = Some(request.input.clone());

        if let Some(output_builder) = next_builder.output_builder.as_ref() {
            next_builder.resolved_output =
                Some(self.resolve_output(output_builder, &next_model, Some(&request.output))?);
        }

        next_builder.starting_history = Some(self.transfer_context(context, &next_model));
        debug!("Prepared replacement reconfigurable semi-sync builder");
        Ok(Some(next_builder))
    }

    async fn process_input_updates(
        &mut self,
        input: &ReconfigurableInput<AC::Val>,
        input_stream: &mut ReconfigurableInputStream<AC::Val>,
        context: &mut SemiSyncContext<AC>,
        expr_evals: &mut Vec<ExprEvalutor<AC, MS>>,
    ) -> anyhow::Result<Option<ReconfSemiSyncRuntimeBuilder<AC, MS>>> {
        let cancellation = context.cancellation_token();
        loop {
            let item = match futures::future::select(input_stream.next(), cancellation.cancelled())
                .await
            {
                futures::future::Either::Left((item, _cancelled)) => item,
                futures::future::Either::Right((_cancelled, _input)) => {
                    context.cancel();
                    return Ok(None);
                }
            };
            let Some(item) = item else {
                return Ok(None);
            };

            match item? {
                ReconfigurableInputItem::Data(batch) => {
                    for tick in batch.into_ticks() {
                        let tick_cancelled = match futures::future::select(
                            Box::pin(SemiSyncRuntime::<AC, MS>::advance_tick(
                                tick, context, expr_evals,
                            )),
                            cancellation.cancelled(),
                        )
                        .await
                        {
                            futures::future::Either::Left((result, _cancelled)) => {
                                result?;
                                false
                            }
                            futures::future::Either::Right((_cancelled, _tick_future)) => true,
                        };
                        if tick_cancelled {
                            context.cancel();
                            return Ok(None);
                        }
                    }
                }
                ReconfigurableInputItem::Reconfigure(request) => {
                    return self.handle_reconfig_input(input, request, context).await;
                }
            }
        }
    }

    /// Own all resources of the active monitor locally. The replacement builder is
    /// returned only after input, context, evaluators, output, and processing
    /// futures have left this scope.
    async fn run_active_monitor(
        &mut self,
    ) -> anyhow::Result<Option<ReconfSemiSyncRuntimeBuilder<AC, MS>>> {
        if let Some(error) = self.setup_error.take() {
            return Err(anyhow!(error));
        }
        let executor = self
            .builder
            .executor
            .clone()
            .ok_or_else(|| anyhow!("reconfigurable runtime executor is not configured"))?;
        let model = self.builder.model_ref()?.clone();
        let input = self
            .input
            .take()
            .ok_or_else(|| anyhow!("reconfigurable input is not configured"))?;
        let mut input_stream = self
            .input_stream
            .take()
            .ok_or_else(|| anyhow!("reconfigurable input stream is not configured"))?;
        let writer = self
            .output
            .take()
            .ok_or_else(|| anyhow!("reconfigurable output pipeline is not configured"))?;

        // Input and output plans were resolved before either resource opened in
        // the builder. The input stream is already subscribed here so a tick
        // sent immediately after build cannot be lost.

        let monitor = SemiSyncRuntimeBuilder::new()
            .executor(executor)
            .model(model)
            .input(input::empty_input_stream())
            .starting_history(self.builder.starting_history.clone().unwrap_or_default())
            .output_writer(writer)
            .build()
            .await;
        let (output, mut context, mut expr_evals) = Self::setup_inner_monitor(monitor).await?;
        let active_cancellation = context.cancellation_token();
        let mut output_future = Box::pin(output.run().fuse());
        let mut output_completed = false;
        let pending_builder = {
            let mut process = Box::pin(
                self.process_input_updates(
                    &input,
                    &mut input_stream,
                    &mut context,
                    &mut expr_evals,
                )
                .fuse(),
            );
            loop {
                if output_completed {
                    // Output completion is terminal for the active monitor. The
                    // processing future is cancellation-aware, so a pending
                    // input cannot keep the runtime alive indefinitely.
                    active_cancellation.cancel();
                    break process.await;
                }
                futures::select! {
                    input = process.as_mut() => break input,
                    output = output_future.as_mut() => {
                        output_completed = true;
                        // Cancel before awaiting processing. This covers both
                        // normal output EOF and an intentional downstream
                        // close, while preserving a ready replacement request.
                        active_cancellation.cancel();
                        if let Err(error) = output.context("reconfigurable output failed") {
                            break Err(error);
                        }
                    },
                }
            }
        };

        // Cancellation stops the active monitor's producers, but the
        // output future remains owned here until its flush/close barrier has
        // completed. This drains coalescers and buffers before any old input
        // resources are dropped or the replacement is built.
        context.cancel();
        if !output_completed {
            output_future
                .await
                .context("reconfigurable output failed")?;
        }
        drop(input_stream);
        drop(input);
        drop(context);
        drop(expr_evals);
        pending_builder
    }
}

#[async_trait(?Send)]
impl<AC, MS> Runtime for ReconfSemiSyncRuntime<AC, MS>
where
    AC: AsyncConfig<Val = Value, Ctx = SemiSyncContext<AC>>,
    AC::Expr: DependencyGraphExpr + PartialEq + Debug,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
{
    async fn run_boxed(mut self: Box<Self>) -> anyhow::Result<()> {
        loop {
            let pending_update = self.run_active_monitor().await?;
            let Some(builder) = pending_update else {
                return Ok(());
            };
            self = Box::new(builder.build().await);
            info!("Starting reconfigured runtime");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        collections::BTreeMap,
        pin::Pin,
        rc::Rc,
        task::{Context, Poll},
        time::Duration,
    };

    use async_trait::async_trait;
    use async_unsync::bounded;
    use futures::{Sink, future::Either};

    use super::*;
    use crate::core::{OutputBackend, OutputBatch, OutputError, OutputInterface, OutputWriter};
    #[cfg(not(feature = "ros"))]
    use crate::io::{CodecId, Route};
    use crate::io::{InputPipeline, InputSource, OutputBackendBuilder, OutputBackendConfig};
    use crate::runtime::RuntimeBuilder;
    use crate::runtime::builder::SemiSyncValueConfig;
    use crate::semantics::UntimedDsrvSemantics;
    use crate::stream_utils::{Fanout, FanoutSender};
    use crate::{DsrvSpecification, Value, VarName};

    type TestRuntime = ReconfSemiSyncRuntime<SemiSyncValueConfig, UntimedDsrvSemantics>;

    const PENDING_MODEL: &str = "in x: Int\nout z: Int\nz = x";
    const FINITE_FIRST_OUTPUT_MODEL: &str = "in x: Int\nout z: Int\nz = 1";

    fn parse_spec(source: &str) -> anyhow::Result<DsrvSpecification> {
        source.parse().map_err(anyhow::Error::from)
    }

    fn manual_input() -> (
        InputSource,
        FanoutSender<Value>,
        Rc<Fanout<Value>>,
        FanoutSender<Value>,
        Rc<Fanout<Value>>,
    ) {
        let (data_sender, data_fanout) = Fanout::new();
        let (control_sender, control_fanout) = Fanout::new();
        let source = InputSource::manual_with_control(
            BTreeMap::from([(VarName::new("x"), Rc::clone(&data_fanout))]),
            Some(Rc::clone(&control_fanout)),
        );
        (
            source,
            data_sender,
            data_fanout,
            control_sender,
            control_fanout,
        )
    }

    async fn build_runtime(
        executor: Rc<smol::LocalExecutor<'static>>,
        model: &str,
        input: InputSource,
        output: OutputBackendBuilder<Value>,
    ) -> TestRuntime {
        ReconfSemiSyncRuntimeBuilder::<SemiSyncValueConfig, UntimedDsrvSemantics>::new()
            .parse_spec(parse_spec)
            .executor(executor)
            .model(model.parse().expect("test model should parse"))
            .input_pipeline(InputPipeline::new(input))
            .output_builder(output)
            .reconf_topic("reconf".to_owned())
            .build()
            .await
    }

    async fn run_with_timeout(runtime: TestRuntime) -> anyhow::Result<()> {
        let run = Box::pin(runtime.run());
        let timeout = Box::pin(smol::Timer::after(Duration::from_secs(1)));
        match futures::future::select(run, timeout).await {
            Either::Left((result, _timeout)) => result,
            Either::Right((_timeout, _run)) => {
                panic!("reconfigurable runtime did not finish before the timeout")
            }
        }
    }

    #[derive(Clone)]
    struct CountingBackend {
        opens: Rc<Cell<usize>>,
        closes: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
        fail_on_attempt: Option<usize>,
        close_error: Option<OutputError>,
        send_error: Option<OutputError>,
    }

    impl CountingBackend {
        fn new(fail_on_attempt: Option<usize>, close_error: Option<OutputError>) -> Self {
            Self {
                opens: Rc::new(Cell::new(0)),
                closes: Rc::new(Cell::new(0)),
                drops: Rc::new(Cell::new(0)),
                fail_on_attempt,
                close_error,
                send_error: None,
            }
        }

        fn with_send_error(mut self, send_error: OutputError) -> Self {
            self.send_error = Some(send_error);
            self
        }

        fn builder(&self) -> OutputBackendBuilder<Value> {
            OutputBackendBuilder::new(OutputBackendConfig::custom(self.clone()))
        }
    }

    struct CountingSink {
        closes: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
        close_error: Option<OutputError>,
        send_error: Option<OutputError>,
        ready: bool,
        closed: bool,
    }

    impl Drop for CountingSink {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    impl Sink<OutputBatch<Value>> for CountingSink {
        type Error = OutputError;

        fn poll_ready(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if self.closed {
                return Poll::Ready(Err(OutputError::Closed));
            }
            self.ready = true;
            Poll::Ready(Ok(()))
        }

        fn start_send(
            mut self: Pin<&mut Self>,
            _batch: OutputBatch<Value>,
        ) -> Result<(), Self::Error> {
            if self.closed {
                return Err(OutputError::Closed);
            }
            if !self.ready {
                return Err(OutputError::backend("counting sink was not ready"));
            }
            self.ready = false;
            if let Some(error) = self.send_error.take() {
                return Err(error);
            }
            Ok(())
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if self.closed {
                return Poll::Ready(Err(OutputError::Closed));
            }
            self.ready = true;
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if self.closed {
                return Poll::Ready(Err(OutputError::Closed));
            }
            self.closes.set(self.closes.get() + 1);
            self.closed = true;
            Poll::Ready(self.close_error.take().map_or(Ok(()), Err))
        }
    }

    #[async_trait(?Send)]
    impl OutputBackend for CountingBackend {
        type Val = Value;

        async fn open(
            &self,
            _interface: OutputInterface,
        ) -> Result<OutputWriter<Value>, OutputError> {
            let attempt = self.opens.get();
            self.opens.set(attempt + 1);
            if self.fail_on_attempt == Some(attempt) {
                return Err(OutputError::backend("counting output open failed"));
            }
            Ok(OutputWriter::from_sink(CountingSink {
                closes: Rc::clone(&self.closes),
                drops: Rc::clone(&self.drops),
                close_error: self.close_error.clone(),
                send_error: self.send_error.clone(),
                ready: false,
                closed: false,
            }))
        }
    }

    #[test]
    fn output_closed_cancels_pending_input() {
        smol::block_on(async {
            let executor = Rc::new(smol::LocalExecutor::new());
            let (input, data_sender, _data_fanout, _control_sender, _control_fanout) =
                manual_input();
            let output = CountingBackend::new(None, None)
                .with_send_error(OutputError::Closed)
                .builder();
            let runtime = build_runtime(executor, FINITE_FIRST_OUTPUT_MODEL, input, output).await;
            let send_first_tick = async move {
                data_sender.send(Value::Int(1)).await;
            };
            let run = Box::pin(runtime.run());
            let send_first_tick = Box::pin(send_first_tick);
            let joined = Box::pin(futures::future::join(run, send_first_tick));
            let timeout = Box::pin(smol::Timer::after(Duration::from_secs(1)));
            let result = match futures::future::select(joined, timeout).await {
                Either::Left(((result, ()), _timeout)) => result,
                Either::Right((_timeout, _joined)) => {
                    panic!("closed output did not cancel pending input before the timeout")
                }
            };
            assert!(
                result.is_ok(),
                "closed output should end the active monitor: {result:?}"
            );
        });
    }

    #[test]
    fn healthy_monitor_forwards_immediate_input() {
        smol::block_on(async {
            let executor = Rc::new(smol::LocalExecutor::new());
            let (input, data_sender, _data_fanout, _control_sender, _control_fanout) =
                manual_input();
            let (output_sender, mut output_receiver) =
                bounded::channel::<BTreeMap<VarName, Value>>(1).into_split();
            let output = OutputBackendBuilder::new(OutputBackendConfig::Manual(output_sender));
            let runtime = build_runtime(executor, PENDING_MODEL, input, output).await;

            // Queue the tick before the runtime is polled. A healthy build must
            // already own its input subscription, otherwise the fan-out drops
            // this value and the monitor waits forever for its first tick.
            data_sender.send(Value::Int(7)).await;
            let first = Box::pin(futures::future::select(
                Box::pin(output_receiver.recv()),
                Box::pin(runtime.run()),
            ));
            let output = match futures::future::select(
                first,
                Box::pin(smol::Timer::after(Duration::from_secs(1))),
            )
            .await
            {
                Either::Left((Either::Left((Some(output), _run)), _timeout)) => output,
                Either::Left((Either::Left((None, _run)), _timeout)) => {
                    panic!("healthy output channel closed")
                }
                Either::Left((Either::Right((result, _output)), _timeout)) => {
                    panic!("healthy monitor ended before output: {result:?}")
                }
                Either::Right((_timeout, _first)) => {
                    panic!("healthy monitor did not forward immediate input")
                }
            };
            assert_eq!(output.get(&VarName::new("z")), Some(&Value::Int(7)));
        });
    }

    #[test]
    fn initial_output_failure_drops_open_input() {
        smol::block_on(async {
            let executor = Rc::new(smol::LocalExecutor::new());
            let (input, data_sender, data_fanout, _control_sender, _control_fanout) =
                manual_input();
            let backend = CountingBackend::new(Some(0), None);
            let runtime = build_runtime(executor, PENDING_MODEL, input, backend.builder()).await;

            let error = run_with_timeout(runtime)
                .await
                .expect_err("initial output open should fail");
            assert!(
                error.to_string().contains("counting output open failed"),
                "{error}"
            );
            assert_eq!(backend.opens.get(), 1);
            assert_eq!(backend.closes.get(), 0);
            assert_eq!(backend.drops.get(), 0);
            assert!(data_fanout.sub_events() > 0, "input source was not opened");

            let seen = data_fanout.prune_events();
            data_sender.send(Value::Int(1)).await;
            assert!(
                data_fanout.prune_events() > seen,
                "opened input source was not dropped after output failure"
            );
        });
    }

    #[cfg(not(feature = "ros"))]
    #[test]
    fn initial_input_failure_closes_output_and_preserves_cleanup_error() {
        smol::block_on(async {
            let executor = Rc::new(smol::LocalExecutor::new());
            let input = InputSource::ros(
                BTreeMap::from([(
                    VarName::new("x"),
                    Route::new("x".to_owned().into_boxed_str(), Some(CodecId::new("json")))
                        .unwrap(),
                )]),
                Rc::clone(&executor),
            );
            let backend = CountingBackend::new(
                None,
                Some(OutputError::backend("counting output close failed")),
            );
            let runtime = build_runtime(executor, PENDING_MODEL, input, backend.builder()).await;

            let error = run_with_timeout(runtime)
                .await
                .expect_err("ROS-disabled input open should fail");
            let message = error.to_string();
            assert!(message.contains("ROS support not enabled"), "{message}");
            assert!(
                message.contains("counting output close failed"),
                "{message}"
            );
            assert_eq!(backend.opens.get(), 1);
            assert_eq!(backend.closes.get(), 1);
            assert_eq!(backend.drops.get(), 1);
        });
    }

    #[test]
    fn replacement_output_failure_drops_new_input_and_closes_old_output() {
        smol::block_on(async {
            let executor = Rc::new(smol::LocalExecutor::new());
            let (input, data_sender, data_fanout, control_sender, _control_fanout) = manual_input();
            let backend = CountingBackend::new(Some(1), None);
            let runtime = build_runtime(executor, PENDING_MODEL, input, backend.builder()).await;
            let request = async move {
                let payload = serde_json::json!({ "specification": PENDING_MODEL }).to_string();
                control_sender.send(Value::Str(payload.into())).await;
            };
            let run = Box::pin(runtime.run());
            let request = Box::pin(request);
            let joined = Box::pin(futures::future::join(run, request));
            let timeout = Box::pin(smol::Timer::after(Duration::from_secs(1)));
            let result = match futures::future::select(joined, timeout).await {
                Either::Left(((result, ()), _timeout)) => result,
                Either::Right((_timeout, _joined)) => {
                    panic!("replacement failure did not finish before the timeout")
                }
            };

            let error = result.expect_err("replacement output open should fail");
            assert!(
                error.to_string().contains("counting output open failed"),
                "{error}"
            );
            assert_eq!(backend.opens.get(), 2);
            assert_eq!(backend.closes.get(), 1);
            assert_eq!(backend.drops.get(), 1);
            assert!(
                data_fanout.sub_events() >= 2,
                "replacement input source was not opened"
            );

            let seen = data_fanout.prune_events();
            data_sender.send(Value::Int(1)).await;
            assert!(
                data_fanout.prune_events() > seen,
                "replacement input source was not dropped after output failure"
            );
        });
    }
}
