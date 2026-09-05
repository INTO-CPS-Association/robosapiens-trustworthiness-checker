use crate::core::{DeferrableStreamData, OutputWriter, Runtime, Specification};
use crate::io::reconfigurable_input::{
    InputPipelineSession, ReconfigurableInput, ReconfigurableInputItem,
};
use crate::io::{
    InputConfiguration, InputPipeline, OutputConfiguration, OutputPipeline, ReconfigurationRequest,
};
use crate::lang::core::{DependencyGraphExpr, DependencyGraphSpec};
use crate::runtime::{
    RuntimeBuilder,
    semi_sync::{ExprEvalutor, SemiSyncContext, SemiSyncRuntime},
};
use crate::semantics::{AsyncConfig, MonitoringSemantics, StreamContext};
use crate::{Value, VarName};

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
    output_pipeline: Option<OutputPipeline<AC::Val>>,
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
            output_pipeline: None,
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

    fn input(self, _input: crate::io::OpenedInput<AC::Val>) -> Self {
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
                match (builder.output_pipeline.as_ref(), builder.model.as_ref()) {
                    (Some(output_pipeline), Some(model)) => {
                        match output_pipeline.resolve(model.output_vars(), model.aux_vars(), None) {
                            Ok(resolved) => builder.resolved_output = Some(resolved),
                            Err(error) => setup_error = Some(error.to_string()),
                        }
                    }
                    (None, _) => {
                        setup_error =
                            Some("reconfigurable output pipeline is not configured".into())
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
                let output_pipeline = builder
                    .output_pipeline
                    .clone()
                    .expect("resolved reconfigurable output pipeline must be present")
                    .with_executor(executor);
                let shutdown_timeout = output_pipeline.shutdown_timeout();
                let resolved_output = builder
                    .resolved_output
                    .clone()
                    .expect("resolved reconfigurable output plan must be present");

                // Input is opened during build so callers can safely send the
                // first channel tick immediately after spawning the runtime.
                // Both opens still start only after all resolution is complete.
                let (input_result, output_result) = futures::join!(
                    input_ref.open_session(resolved_input),
                    output_pipeline.open_session(resolved_output),
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
                        let deadline = opened_output.writer().shutdown_deadline();
                        let cleanup = crate::runtime::output::finish_writer_with_deadline(
                            opened_output.writer_mut(),
                            deadline,
                        )
                        .await;
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
                        let deadline = shutdown_timeout.map_or_else(
                            crate::io::ShutdownDeadline::none,
                            crate::io::ShutdownDeadline::after,
                        );
                        let mut drain = opened_input.into_drain_with_deadline(deadline);
                        let mut error = output_error;
                        while let Some(item) = drain.next().await {
                            if let Err(cleanup) = item {
                                error = error.context(format!(
                                    "reconfigurable input cleanup also failed: {cleanup:#}"
                                ));
                            }
                        }
                        drop(input.take());
                        setup_error = Some(error.to_string());
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

    pub fn output_pipeline(mut self, output_pipeline: OutputPipeline<AC::Val>) -> Self {
        self.output_pipeline = Some(output_pipeline);
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
        let pipeline = pipeline.with_executor(executor.clone());
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
    input_stream: Option<InputPipelineSession<AC::Val>>,
    output: Option<crate::io::output::OutputPipelineSession<AC::Val>>,
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
        builder: &OutputPipeline<AC::Val>,
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

        if let Some(output_pipeline) = next_builder.output_pipeline.as_ref() {
            next_builder.resolved_output =
                Some(self.resolve_output(output_pipeline, &next_model, Some(&request.output))?);
        }

        debug!("Prepared replacement reconfigurable semi-sync builder");
        Ok(Some(next_builder))
    }

    async fn process_input_updates(
        &mut self,
        input: &ReconfigurableInput<AC::Val>,
        input_stream: &mut InputPipelineSession<AC::Val>,
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
                ReconfigurableInputItem::Boundary(_) => {
                    anyhow::bail!("unexpected input lifecycle boundary")
                }
                ReconfigurableInputItem::Reconfigure(request) => {
                    return self.handle_reconfig_input(input, request).await;
                }
            }
        }
    }

    /// Replace model evaluation at an ordered boundary while retaining the
    /// input and output owners. Terminal paths drain both sides once.
    async fn run_active_monitor(
        &mut self,
    ) -> anyhow::Result<Option<ReconfSemiSyncRuntimeBuilder<AC, MS>>> {
        if let Some(error) = self.setup_error.take() {
            return Err(anyhow!(error));
        }
        let model = self.builder.model_ref()?.clone();
        let input = self
            .input
            .take()
            .ok_or_else(|| anyhow!("reconfigurable input is not configured"))?;
        let mut input_stream = self.input_stream.take();
        let mut output = self
            .output
            .take()
            .ok_or_else(|| anyhow!("reconfigurable output pipeline is not configured"))?;
        let output_resolution = output.resolved().clone();
        let output_id = output.session_id();
        let output_revision = output.revision();
        let shutdown_timeout = output.writer().shutdown_timeout();
        let make_deadline = || {
            shutdown_timeout.map_or_else(
                crate::io::ShutdownDeadline::none,
                crate::io::ShutdownDeadline::after,
            )
        };
        let mut terminal_deadline = None;
        let setup = SemiSyncRuntime::<AC, MS>::setup_evaluation(
            model,
            self.builder.starting_history.clone().unwrap_or_default(),
        )
        .await;
        let mut pending_builder = match setup {
            Err(error) => Err(error),
            Ok((streams, mut context, mut expr_evals)) => {
                let cancellation = context.cancellation_token();
                let mut output_future = Box::pin(
                    crate::runtime::output::consume_row_streams(streams, output.writer_mut())
                        .fuse(),
                );
                let mut output_completed = false;
                let mut pending = {
                    let process = Box::pin(self.process_input_updates(
                        &input,
                        input_stream.as_mut().expect("active input session"),
                        &mut context,
                        &mut expr_evals,
                    ));
                    match futures::future::select(process, output_future.as_mut()).await {
                        futures::future::Either::Left((result, _)) => result,
                        futures::future::Either::Right((result, process)) => {
                            output_completed = true;
                            cancellation.cancel();
                            drop(process);
                            result
                                .context("reconfigurable output failed")
                                .map(|()| None)
                        }
                    }
                };
                if !matches!(pending, Ok(Some(_))) {
                    terminal_deadline = Some(make_deadline());
                }

                // Both plans are pure: reject unsupported bindings before
                // pausing any source or changing any destination.
                let plans = match &pending {
                    Ok(Some(next)) => (|| {
                        let active = input_stream.as_ref().expect("active input session");
                        let input_plan = input.pipeline().plan_reconfiguration(
                            active.active(),
                            next.resolved_input.clone().expect("resolved input"),
                            active.session_id(),
                            active.revision(),
                        )?;
                        let output_plan = next
                            .output_pipeline
                            .as_ref()
                            .expect("output pipeline")
                            .plan_reconfiguration(
                            &output_resolution,
                            next.resolved_output.clone().expect("resolved output"),
                            output_id,
                            output_revision,
                        )?;
                        Ok::<_, anyhow::Error>((input_plan, output_plan, active.revision()))
                    })()
                    .map(Some),
                    _ => Ok(None),
                };
                let mut output_plan = None;
                let mut input_revision = None;
                match plans {
                    Err(error) => pending = Err(error),
                    Ok(Some((plan, next_output, revision))) => {
                        let input_failure_deadline = Rc::new(std::cell::Cell::new(None));
                        let mut rebind =
                            Box::pin(input_stream.take().expect("active input session").rebind(
                                plan,
                                shutdown_timeout,
                                input_failure_deadline.clone(),
                                async |batch| {
                                    for tick in batch.into_ticks() {
                                        SemiSyncRuntime::<AC, MS>::advance_tick(
                                            tick,
                                            &mut context,
                                            &mut expr_evals,
                                        )
                                        .await?;
                                    }
                                    Ok(())
                                },
                            ));
                        let rebound =
                            match futures::future::select(rebind.as_mut(), output_future.as_mut())
                                .await
                            {
                                futures::future::Either::Left((result, _)) => result,
                                futures::future::Either::Right((result, _)) => {
                                    output_completed = true;
                                    cancellation.cancel();
                                    let deadline =
                                        input_failure_deadline.get().unwrap_or_else(|| {
                                            let deadline = *terminal_deadline
                                                .get_or_insert_with(make_deadline);
                                            input_failure_deadline.set(Some(deadline));
                                            deadline
                                        });
                                    terminal_deadline = Some(deadline);
                                    pending = result
                                        .context("reconfigurable output failed")
                                        .map(|()| None);
                                    // The callback observes cancellation; retain the
                                    // consumed owner until its transition finishes or
                                    // the shared terminal deadline expires.
                                    match deadline.timeout(rebind.as_mut()).await {
                                        Ok(result) => result,
                                        Err(timeout) => Err(anyhow::Error::new(timeout)),
                                    }
                                }
                            };
                        drop(rebind);
                        match rebound {
                            Ok(session) => {
                                input_stream = Some(session);
                                input_revision = Some(revision);
                                output_plan = Some(next_output);
                                if let Ok(Some(next)) = &mut pending {
                                    next.starting_history =
                                        Some(self.transfer_context(&context, next.model_ref()?));
                                }
                            }
                            Err(error) => {
                                if terminal_deadline.is_none() {
                                    terminal_deadline = input_failure_deadline.get();
                                }
                                terminal_deadline.get_or_insert_with(make_deadline);
                                pending = Err(error.context("input rebind failed"));
                            }
                        }
                    }
                    Ok(None) => {}
                }
                context.cancel();
                if !output_completed {
                    if matches!(pending, Ok(Some(_))) {
                        if let Err(error) = output_future.as_mut().await {
                            terminal_deadline.get_or_insert_with(make_deadline);
                            pending = Err(error.context("reconfigurable output failed"));
                        }
                    } else {
                        let deadline = *terminal_deadline.get_or_insert_with(make_deadline);
                        match deadline.timeout(output_future.as_mut()).await {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => {
                                pending = Err(error.context("reconfigurable output failed"));
                            }
                            Err(timeout) => pending = Err(anyhow::Error::new(timeout)),
                        }
                    }
                }
                drop(output_future);
                if matches!(pending, Ok(Some(_))) {
                    let transition = async {
                        output
                            .apply_reconfiguration(output_plan.expect("prepared output plan"))
                            .await?;
                        input_stream
                            .as_mut()
                            .expect("rebound input session")
                            .commit_revision(input_revision.expect("prepared input revision"))?;
                        output.commit_revision(output_revision)?;
                        Ok::<_, anyhow::Error>(())
                    }
                    .await;
                    if let Err(error) = transition {
                        terminal_deadline.get_or_insert_with(make_deadline);
                        pending = Err(error.context("session reconfiguration failed"));
                    }
                }
                pending
            }
        };
        if matches!(pending_builder, Ok(Some(_))) {
            self.input = Some(input);
            self.input_stream = input_stream;
            self.output = Some(output);
            return pending_builder;
        }

        let deadline = terminal_deadline.unwrap_or_else(make_deadline);
        if let Some(session) = input_stream {
            let mut drain = session.into_drain_with_deadline(deadline);
            while let Some(item) = drain.next().await {
                if let Err(error) = item {
                    pending_builder = Err(match pending_builder {
                        Ok(_) => error,
                        Err(primary) => anyhow!("{primary:#}; additionally: {error:#}"),
                    });
                }
            }
        }
        if let Err(error) =
            crate::runtime::output::finish_writer_with_deadline(output.writer_mut(), deadline).await
        {
            pending_builder = Err(match pending_builder {
                Ok(_) => error,
                Err(primary) => anyhow!("{primary:#}; additionally: {error:#}"),
            });
        }
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
            self.builder = builder;
            info!("Starting reconfigured runtime");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        collections::BTreeMap,
        future::Future,
        pin::Pin,
        rc::Rc,
        task::{Context, Poll},
        time::Duration,
    };

    use async_trait::async_trait;
    use async_unsync::bounded;
    use futures::{Sink, future::Either};
    use macro_rules_attribute::apply;

    use super::*;
    use crate::async_test;
    use crate::core::{OutputBatch, OutputError, OutputInterface, OutputWriter};
    use crate::io::output::TestOutputOpener;
    #[cfg(not(feature = "ros"))]
    use crate::io::{FormatId, Route};
    use crate::io::{InputPipeline, InputSource, OutputBackendConfig, OutputPipeline};
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

    fn channel_input() -> (
        InputSource,
        FanoutSender<Value>,
        Rc<Fanout<Value>>,
        FanoutSender<Value>,
        Rc<Fanout<Value>>,
    ) {
        let (data_sender, data_fanout) = Fanout::new();
        let (control_sender, control_fanout) = Fanout::new();
        let source = InputSource::channel_with_control(
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
        output: OutputPipeline<Value>,
    ) -> TestRuntime {
        ReconfSemiSyncRuntimeBuilder::<SemiSyncValueConfig, UntimedDsrvSemantics>::new()
            .parse_spec(parse_spec)
            .executor(executor)
            .model(model.parse().expect("test model should parse"))
            .input_pipeline(InputPipeline::new(input))
            .output_pipeline(output)
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
        pending_close: bool,
        close_delay: Option<Duration>,
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
                pending_close: false,
                close_delay: None,
            }
        }

        fn with_send_error(mut self, send_error: OutputError) -> Self {
            self.send_error = Some(send_error);
            self
        }

        fn with_pending_close(mut self) -> Self {
            self.pending_close = true;
            self
        }

        fn with_close_delay(mut self, delay: Duration) -> Self {
            self.close_delay = Some(delay);
            self
        }

        fn builder(&self) -> OutputPipeline<Value> {
            OutputPipeline::from_backend(OutputBackendConfig::test(self.clone()))
        }
    }

    struct CountingSink {
        closes: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
        close_error: Option<OutputError>,
        send_error: Option<OutputError>,
        ready: bool,
        closed: bool,
        pending_close: bool,
        close_delay: Option<Duration>,
        close_timer: Option<Pin<Box<smol::Timer>>>,
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
                return Poll::Ready(Err(OutputError::closed()));
            }
            self.ready = true;
            Poll::Ready(Ok(()))
        }

        fn start_send(
            mut self: Pin<&mut Self>,
            _batch: OutputBatch<Value>,
        ) -> Result<(), Self::Error> {
            if self.closed {
                return Err(OutputError::closed());
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
                return Poll::Ready(Err(OutputError::closed()));
            }
            self.ready = true;
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if self.pending_close {
                return Poll::Pending;
            }
            if let Some(delay) = self.close_delay {
                let timer = self
                    .close_timer
                    .get_or_insert_with(|| Box::pin(smol::Timer::after(delay)));
                if timer.as_mut().poll(context).is_pending() {
                    return Poll::Pending;
                }
            }
            if self.closed {
                return Poll::Ready(Err(OutputError::closed()));
            }
            self.closes.set(self.closes.get() + 1);
            self.closed = true;
            Poll::Ready(self.close_error.take().map_or(Ok(()), Err))
        }
    }

    #[async_trait(?Send)]
    impl TestOutputOpener<Value> for CountingBackend {
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
                pending_close: self.pending_close,
                close_delay: self.close_delay,
                close_timer: None,
            }))
        }
    }

    #[apply(async_test)]
    async fn terminal_output_close_uses_configured_shutdown_deadline(
        executor: Rc<smol::LocalExecutor<'static>>,
    ) {
        let (input, data_sender, _data_fanout, control_sender, _control_fanout) = channel_input();
        let output = CountingBackend::new(None, None)
            .with_pending_close()
            .builder()
            .with_shutdown_timeout(Some(Duration::from_millis(10)));
        let runtime = build_runtime(executor, PENDING_MODEL, input, output).await;
        data_sender.send(Value::Int(1)).await;
        drop(data_sender);
        drop(control_sender);

        let error = run_with_timeout(runtime)
            .await
            .expect_err("pending output close should reach its shutdown deadline");
        assert!(
            error.to_string().contains("shutdown deadline expired"),
            "{error:#}"
        );
    }

    #[apply(async_test)]
    async fn input_cleanup_and_output_close_share_one_shutdown_deadline(
        executor: Rc<smol::LocalExecutor<'static>>,
    ) {
        let (input, _data_sender, _data_fanout, control_sender, _control_fanout) = channel_input();
        let output = CountingBackend::new(None, None)
            .with_close_delay(Duration::from_millis(100))
            .builder()
            .with_shutdown_timeout(Some(Duration::from_millis(150)));
        let mut runtime = build_runtime(executor, PENDING_MODEL, input, output).await;
        let cleanup_completed = Rc::new(Cell::new(false));
        runtime
            .input_stream
            .as_mut()
            .expect("input session")
            .add_delayed_cleanup_for_test(
                Duration::from_millis(100),
                Rc::clone(&cleanup_completed),
            );
        control_sender.send(Value::Str("invalid JSON".into())).await;

        let error = run_with_timeout(runtime)
            .await
            .expect_err("output close must expire in the remainder of the input cleanup budget");
        assert!(cleanup_completed.get(), "input cleanup did not complete");
        assert!(
            error.to_string().contains("shutdown deadline expired"),
            "{error:#}"
        );
    }

    #[apply(async_test)]
    async fn output_closed_cancels_pending_input(executor: Rc<smol::LocalExecutor<'static>>) {
        let (input, data_sender, _data_fanout, _control_sender, _control_fanout) = channel_input();
        let output = CountingBackend::new(None, None)
            .with_send_error(OutputError::closed())
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
    }

    #[apply(async_test)]
    async fn healthy_monitor_forwards_immediate_input(executor: Rc<smol::LocalExecutor<'static>>) {
        let (input, data_sender, _data_fanout, _control_sender, _control_fanout) = channel_input();
        let (output_sender, mut output_receiver) =
            bounded::channel::<BTreeMap<VarName, Value>>(1).into_split();
        let output = OutputPipeline::from_backend(OutputBackendConfig::channel(output_sender));
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
    }

    #[apply(async_test)]
    async fn initial_output_failure_drops_open_input(executor: Rc<smol::LocalExecutor<'static>>) {
        let (input, data_sender, data_fanout, _control_sender, _control_fanout) = channel_input();
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
    }

    #[cfg(not(feature = "ros"))]
    #[apply(async_test)]
    async fn initial_input_failure_closes_output_and_preserves_cleanup_error(
        executor: Rc<smol::LocalExecutor<'static>>,
    ) {
        let input = InputSource::ros(
            BTreeMap::from([(
                VarName::new("x"),
                Route::new("x".to_owned().into_boxed_str(), Some(FormatId::new("json"))).unwrap(),
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
    }

    #[apply(async_test)]
    async fn reconfiguration_retains_unchanged_input_and_output_owners(
        executor: Rc<smol::LocalExecutor<'static>>,
    ) {
        let (input, data_sender, data_fanout, control_sender, _control_fanout) = channel_input();
        // A second open would fail: unchanged destinations must retain their owner.
        let backend = CountingBackend::new(Some(1), None);
        let mut runtime = build_runtime(executor, PENDING_MODEL, input, backend.builder()).await;
        let subscriptions = data_fanout.sub_events();
        let send = async {
            let payload = serde_json::json!({ "specification": PENDING_MODEL }).to_string();
            control_sender.send(Value::Str(payload.into())).await;
        };
        let transition = futures::future::join(runtime.run_active_monitor(), send);
        let next = match futures::future::select(
            Box::pin(transition),
            Box::pin(smol::Timer::after(Duration::from_secs(1))),
        )
        .await
        {
            Either::Left(((result, ()), _)) => result.unwrap().expect("prepared model"),
            Either::Right(_) => panic!("reconfiguration did not finish"),
        };
        assert_eq!(backend.opens.get(), 1);
        assert_eq!(backend.closes.get(), 0);
        assert_eq!(backend.drops.get(), 0);
        assert_eq!(data_fanout.sub_events(), subscriptions);
        assert_eq!(runtime.input_stream.as_ref().unwrap().revision().get(), 1);
        assert_eq!(runtime.output.as_ref().unwrap().revision().get(), 1);
        runtime.builder = next;
        let fail = async {
            control_sender.send(Value::Str("invalid JSON".into())).await;
        };
        let (result, ()) = futures::join!(run_with_timeout(runtime), fail);
        assert!(result.is_err());
        assert_eq!(backend.opens.get(), 1);
        assert_eq!(backend.closes.get(), 1);
        assert_eq!(backend.drops.get(), 1);
        let seen = data_fanout.prune_events();
        data_sender.send(Value::Int(1)).await;
        assert!(
            data_fanout.prune_events() > seen,
            "input owner was not released"
        );
    }
}
