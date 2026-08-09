use crate::core::{DeferrableStreamData, OutputHandler, Runtime, Specification, input};
use crate::io::reconfigurable_input::{
    ReconfigurableInput, ReconfigurableInputItem, ReconfigurableInputStream,
};
use crate::io::{InputPipeline, MonitorConfig, OutputHandlerBuilder, OutputHandlerSpec, Route};
use crate::lang::core::{DependencyGraphExpr, DependencyGraphSpec};
use crate::runtime::{
    RuntimeBuilder,
    semi_sync::{ExprEvalutor, SemiSyncContext, SemiSyncRuntime, SemiSyncRuntimeBuilder},
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
    output_builder: Option<OutputHandlerBuilder<AC::Val>>,
    reconf_topic: Option<String>,
    input_config: Option<MonitorConfig>,
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
            output_builder: None,
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

    fn output(self, _output: Box<dyn OutputHandler<Val = AC::Val>>) -> Self {
        self.with_setup_error("direct output handlers are not supported by the reconfigurable runtime; configure an OutputHandlerBuilder")
    }

    fn build(self) -> LocalBoxFuture<'static, Self::Runtime> {
        Box::pin(async move {
            let finalized_input = self.finalize_input().await;
            let (input, input_stream, setup_error) = match finalized_input {
                Ok((input, input_stream)) => (Some(input), Some(input_stream), None),
                Err(error) => (None, None, Some(error.to_string())),
            };
            ReconfSemiSyncRuntime {
                builder: self,
                input,
                setup_error,
                input_stream,
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

    pub fn output_builder(mut self, output_builder: OutputHandlerBuilder<AC::Val>) -> Self {
        self.output_builder = Some(output_builder);
        self
    }

    pub fn reconf_topic(mut self, reconf_topic: String) -> Self {
        self.reconf_topic = Some(reconf_topic);
        self
    }

    pub fn input_config(mut self, input_config: MonitorConfig) -> Self {
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

    async fn finalize_input(
        &self,
    ) -> anyhow::Result<(
        ReconfigurableInput<AC::Val>,
        ReconfigurableInputStream<AC::Val>,
    )> {
        if let Some(error) = self.setup_error.as_ref() {
            return Err(anyhow!(error.clone()));
        }
        let pipeline = self
            .input_pipeline
            .clone()
            .ok_or_else(|| anyhow!("reconfigurable input pipeline is not configured"))?;
        let input = ReconfigurableInput::new(pipeline, self.reconf_topic.clone())
            .context("reconfigurable input could not be configured")?;
        let input_stream = input
            .open(self.model_ref()?.input_vars(), self.input_config.as_ref())
            .await
            .context("reconfigurable input stream could not be opened")?;
        Ok((input, input_stream))
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
    setup_error: Option<String>,
    input_stream: Option<ReconfigurableInputStream<AC::Val>>,
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
        Box<dyn OutputHandler<Val = AC::Val>>,
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

    fn update_output_builder(
        &self,
        builder: &mut OutputHandlerBuilder<AC::Val>,
        model: &AC::Spec,
        configured: Option<&BTreeMap<VarName, Route>>,
    ) -> anyhow::Result<()> {
        let output_vars = model.output_vars();
        match &mut builder.spec {
            OutputHandlerSpec::Stdout | OutputHandlerSpec::Manual(_) => {}
            OutputHandlerSpec::Mqtt(topics) | OutputHandlerSpec::Redis(topics) => {
                let previous = topics.take().unwrap_or_default();
                let mut next = BTreeMap::new();
                for variable in output_vars {
                    let route = configured
                        .and_then(|routes| routes.get(&variable))
                        .map(|route| route.route.to_string())
                        .or_else(|| previous.get(&variable).cloned())
                        .unwrap_or_else(|| variable.to_string());
                    next.insert(variable, route);
                }
                *topics = Some(next);
            }
            OutputHandlerSpec::Ros(topics, codecs) => {
                let previous_topics = std::mem::take(topics);
                let previous_codecs = std::mem::take(codecs);
                let mut next_topics = BTreeMap::new();
                let mut next_codecs = BTreeMap::new();
                for variable in output_vars {
                    let route = configured.and_then(|routes| routes.get(&variable));
                    let topic = route
                        .map(|route| route.route.to_string())
                        .or_else(|| previous_topics.get(&variable).cloned())
                        .unwrap_or_else(|| variable.to_string());
                    let codec = route
                        .and_then(|route| route.codec.as_ref())
                        .map(|codec| codec.0.to_string())
                        .or_else(|| previous_codecs.get(&variable).cloned())
                        .ok_or_else(|| anyhow!("output route for `{variable}` requires a codec"))?;
                    next_topics.insert(variable.clone(), topic);
                    next_codecs.insert(variable, codec);
                }
                *topics = next_topics;
                *codecs = next_codecs;
            }
        }
        Ok(())
    }

    /// Parse, validate, resolve, transfer, and prepare the next generation.
    /// This method is the semantic center of monitor reconfiguration.
    async fn handle_reconfig_input(
        &mut self,
        input: &ReconfigurableInput<AC::Val>,
        request: MonitorConfig,
        context: &mut SemiSyncContext<AC>,
    ) -> anyhow::Result<Option<ReconfSemiSyncRuntimeBuilder<AC, MS>>> {
        request.validate_structure()?;
        let parse_spec = self
            .builder
            .parse_spec
            .ok_or_else(|| anyhow!("reconfiguration parser is not configured"))?;
        let next_model = parse_spec(&request.spec)
            .map_err(|error| anyhow!("failed to parse reconfiguration command: {error}"))?;
        let old_model = self.builder.model_ref()?.clone();
        self.log_model_changes(&old_model, &next_model);

        let mut next_builder = self.builder.clone().model(next_model.clone());
        input
            .pipeline()
            .resolve(&next_model.input_vars(), Some(&request))?;
        next_builder.input_config = Some(request.clone());

        if let Some(output_builder) = next_builder.output_builder.as_mut() {
            self.update_output_builder(output_builder, &next_model, request.outputs.as_ref())?;
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
        while let Some(item) = input_stream.next().await {
            match item? {
                ReconfigurableInputItem::Data(batch) => {
                    for tick in batch.into_ticks() {
                        SemiSyncRuntime::<AC, MS>::advance_tick(tick, context, expr_evals).await?;
                    }
                }
                ReconfigurableInputItem::Reconfigure(request) => {
                    return self.handle_reconfig_input(input, request, context).await;
                }
            }
        }
        Ok(None)
    }

    /// Own all resources of one generation locally. The replacement builder is
    /// returned only after input, context, evaluators, output, and processing
    /// futures have left this scope.
    async fn run_current_generation(
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
        let input_stream = self
            .input_stream
            .take()
            .ok_or_else(|| anyhow!("reconfigurable input stream is not configured"))?;
        let output_builder = self
            .builder
            .output_builder
            .clone()
            .ok_or_else(|| anyhow!("reconfigurable output builder is not configured"))?
            .output_var_names(model.output_vars());
        let output = output_builder
            .build()
            .await
            .context("reconfigurable output handler could not be built")?;
        let monitor = SemiSyncRuntimeBuilder::new()
            .executor(executor)
            .model(model)
            .input(input::empty_input_stream())
            .output(output)
            .starting_history(self.builder.starting_history.clone().unwrap_or_default())
            .build()
            .await;
        let (mut output_handler, mut context, mut expr_evals) =
            Self::setup_inner_monitor(monitor).await?;
        let mut input_stream = input_stream;
        let mut output_future = Box::pin(output_handler.run().fuse());
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
                    break process.await;
                }
                futures::select! {
                    input = process.as_mut() => break input,
                    output = output_future.as_mut() => {
                        output_completed = true;
                        if let Err(error) = output.context("reconfigurable output handler failed") {
                            break Err(error);
                        }
                    },
                }
            }
        };

        context.cancel();
        drop(input_stream);
        drop(input);
        drop(context);
        drop(expr_evals);
        if !output_completed {
            output_future
                .await
                .context("reconfigurable output handler failed")?;
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
            let pending_update = self.run_current_generation().await?;
            let Some(builder) = pending_update else {
                return Ok(());
            };
            self = Box::new(builder.build().await);
            info!("Starting reconfigured runtime");
        }
    }
}
