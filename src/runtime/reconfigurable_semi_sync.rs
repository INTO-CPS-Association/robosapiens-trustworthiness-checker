use crate::{
    InputStream, InputTickStream, Value, VarName,
    core::{DeferrableStreamData, OutputHandler, Runtime, Specification},
    io::{
        InputStreamFactory, MsgTypeMapping, OutputHandlerBuilder, OutputHandlerSpec, TopicMapping,
    },
    lang::core::{DependencyGraphExpr, DependencyGraphSpec, parser::SpecParser},
    runtime::{
        RuntimeBuilder,
        semi_sync::{ExprEvalutor, SemiSyncContext, SemiSyncRuntime, SemiSyncRuntimeBuilder},
    },
    semantics::{AsyncConfig, MonitoringSemantics, StreamContext},
};
use anyhow::{Context, anyhow};
use async_stream::try_stream;
use async_trait::async_trait;
use futures::{FutureExt, StreamExt, future::LocalBoxFuture, stream::LocalBoxStream};
use serde::Deserialize;
use smol::LocalExecutor;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    rc::Rc,
};
use tracing::{debug, error, info, warn};

// TODO: Because InputStreamFactory is hardcoded to Value, this also needs to be in some places

#[derive(Deserialize, Clone, Debug)]
struct ReconfInput {
    spec: String,
    // TODO: Certain input transports are type-strong (e.g. ROS). These must know the types of Inputs
    // and Outputs. When type checking becomes more stable for the language, we
    // should use that instead of a variable here, and simply enforce that the spec is typed.
    type_info: MsgTypeMapping,
    topic_mapping: TopicMapping,
}

#[derive(Clone)]
pub struct ReconfSemiSyncRuntimeBuilder<AC, MS, P>
where
    AC: AsyncConfig<Ctx = SemiSyncContext<AC>>,
    AC::Expr: DependencyGraphExpr + PartialEq + Debug,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
    P: SpecParser<AC::Spec>,
{
    executor: Option<Rc<LocalExecutor<'static>>>,
    model: Option<AC::Spec>,
    input_factory: Option<InputStreamFactory>,
    output_builder: Option<OutputHandlerBuilder>,
    reconf_topic: Option<String>,
    use_context_transfer: bool,
    starting_history: Option<BTreeMap<VarName, Vec<AC::Val>>>,
    known_topic_mapping: TopicMapping,
    known_type_info: MsgTypeMapping,
    _marker: (
        std::marker::PhantomData<MS>,
        std::marker::PhantomData<AC>,
        std::marker::PhantomData<P>,
    ),
}

impl<AC, MS, P> RuntimeBuilder<AC::Spec, AC::Val> for ReconfSemiSyncRuntimeBuilder<AC, MS, P>
where
    AC: AsyncConfig<Val = Value, Ctx = SemiSyncContext<AC>>,
    AC::Expr: DependencyGraphExpr + PartialEq + Debug,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
    P: SpecParser<AC::Spec>,
{
    type Runtime = ReconfSemiSyncRuntime<AC, MS, P>;

    fn new() -> Self {
        Self {
            executor: None,
            model: None,
            input_factory: None,
            output_builder: None,
            reconf_topic: None,
            use_context_transfer: true,
            starting_history: None,
            known_topic_mapping: BTreeMap::new(),
            known_type_info: BTreeMap::new(),
            _marker: (
                std::marker::PhantomData,
                std::marker::PhantomData,
                std::marker::PhantomData,
            ),
        }
    }

    fn executor(mut self, executor: Rc<LocalExecutor<'static>>) -> Self {
        self.executor = Some(executor);
        self
    }

    fn model(mut self, model: AC::Spec) -> Self {
        self.model = Some(model.clone());
        if let Some(output_builder) = self.output_builder {
            self.output_builder = Some(output_builder.output_var_names(model.output_vars()));
        }
        self
    }

    fn input(self, _input: InputStream<AC::Val>) -> Self {
        panic!(
            "Direct InputStream is not supported in ReconfSemiSyncRuntimeBuilder. Use InputStreamFactory instead."
        );
    }

    fn output(self, _output: Box<dyn OutputHandler<Val = AC::Val>>) -> Self {
        panic!(
            "Direct OutputHandler is not supported in ReconfSemiSyncRuntimeBuilder. Use OutputHandlerBuilder instead."
        );
    }

    fn build(mut self) -> LocalBoxFuture<'static, Self::Runtime> {
        Box::pin(async move {
            let executor = self.executor.clone().unwrap();
            let output_builder = self.output_builder.clone().unwrap();
            let model = self.model.clone().unwrap();
            let use_context_transfer = self.use_context_transfer;
            let starting_history = self.starting_history.clone().unwrap_or_default();
            self.inject_reconf_stream();

            if let Some(input_factory) = &self.input_factory {
                if let Some((topic_mapping, msg_type_mapping)) = input_factory.ros_mappings() {
                    self.known_topic_mapping.extend(topic_mapping.clone());
                    self.known_type_info.extend(msg_type_mapping.clone());
                }
            }
            if let Some(output_builder) = &self.output_builder {
                if let OutputHandlerSpec::Ros(topic_mapping, msg_type_mapping) =
                    &output_builder.spec
                {
                    self.known_topic_mapping.extend(topic_mapping.clone());
                    self.known_type_info.extend(msg_type_mapping.clone());
                }
            }

            let input_vars = self
                .model
                .as_ref()
                .expect("Input factory must have a model")
                .input_vars();
            let input_factory = self
                .input_factory
                .as_ref()
                .expect("Input factory must be configured before building");
            info!(
                ?input_factory,
                ?input_vars,
                "Opening reconfigurable input stream"
            );
            let input_stream = match input_factory.ensure_reconfigurable() {
                Ok(()) => input_factory
                    .open(input_vars)
                    .await
                    .context("Reconfigurable input stream could not be opened"),
                Err(error) => Err(error),
            };

            info!(
                ?self.output_builder,
                "Building ReconfSemiSyncRuntime output handler"
            );
            let output = output_builder.build().await;
            let semi_sync_monitor = SemiSyncRuntimeBuilder::new()
                .executor(executor.clone())
                .model(model)
                .input(Box::pin(futures::stream::empty()))
                .output(output)
                .starting_history(starting_history)
                .build()
                .await;

            ReconfSemiSyncRuntime {
                semi_sync_monitor: Some(semi_sync_monitor),
                input_stream: Some(input_stream),
                self_builder: self,
                use_context_transfer,
                _marker: (std::marker::PhantomData, std::marker::PhantomData),
            }
        })
    }
}

impl<AC, MS, P> ReconfSemiSyncRuntimeBuilder<AC, MS, P>
where
    AC: AsyncConfig<Val = Value, Ctx = SemiSyncContext<AC>>,
    AC::Expr: DependencyGraphExpr + PartialEq + Debug,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
    P: SpecParser<AC::Spec>,
{
    pub fn input_factory(self, input_factory: InputStreamFactory) -> Self {
        Self {
            input_factory: Some(input_factory),
            ..self
        }
    }

    pub fn output_builder(self, output_builder: OutputHandlerBuilder) -> Self {
        Self {
            output_builder: Some(output_builder),
            ..self
        }
    }

    pub fn reconf_topic(mut self, reconf_topic: String) -> Self {
        self.reconf_topic = Some(reconf_topic);
        self
    }

    pub fn use_context_transfer(mut self, use_context_transfer: bool) -> Self {
        self.use_context_transfer = use_context_transfer;
        self
    }

    fn inject_reconf_stream(&mut self) {
        let input_factory = self
            .input_factory
            .as_mut()
            .expect("Input factory must be set before injecting the reconfiguration stream");

        // Early‑return if self.reconf_topic is already an input var
        let model = self.model.clone().expect("Input factory must have a model");
        let reconf_topic: VarName = self
            .reconf_topic
            .clone()
            .expect("Reconf topic must be set")
            .into();
        if model.input_vars().contains(&reconf_topic) {
            info!(
                ?reconf_topic,
                "Reconfiguration variable already present in the input model, skipping injection"
            );
            return;
        }

        input_factory.add_reconfiguration_input(reconf_topic.clone(), model.input_vars());
        self.model
            .as_mut()
            .expect("Input factory must have a model")
            .add_input_var(reconf_topic);
        debug!(
            ?self.input_factory,
            "Updated InputStreamFactory with reconfiguration variable"
        );
    }
}

pub struct ReconfSemiSyncRuntime<AC, MS, P>
where
    AC: AsyncConfig<Ctx = SemiSyncContext<AC>>,
    AC::Expr: DependencyGraphExpr + PartialEq + Debug,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
    P: SpecParser<AC::Spec>,
{
    semi_sync_monitor: Option<SemiSyncRuntime<AC, MS>>,
    /// Opening happens while building so transports are subscribed before the runtime is spawned.
    /// The result is retained so setup failures can still be returned from `run_boxed`.
    input_stream: Option<anyhow::Result<InputStream<AC::Val>>>,
    self_builder: ReconfSemiSyncRuntimeBuilder<AC, MS, P>,
    use_context_transfer: bool,
    _marker: (std::marker::PhantomData<MS>, std::marker::PhantomData<P>),
}

impl<AC, MS, P> ReconfSemiSyncRuntime<AC, MS, P>
where
    AC: AsyncConfig<Val = Value, Ctx = SemiSyncContext<AC>>,
    AC::Expr: DependencyGraphExpr + PartialEq + Debug,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
    P: SpecParser<AC::Spec>,
{
    fn setup_input_stream(&mut self) -> anyhow::Result<InputTickStream<AC::Val>> {
        let input = self
            .input_stream
            .take()
            .expect("Input stream already taken")?;
        Ok(crate::into_tick_stream(input))
    }

    async fn setup_inner_monitor(
        monitor: SemiSyncRuntime<AC, MS>,
    ) -> anyhow::Result<(
        Box<dyn OutputHandler<Val = AC::Val>>,
        SemiSyncContext<AC>,
        Vec<ExprEvalutor<AC, MS>>,
    )> {
        monitor.setup_runtime_without_input().await
    }

    // Merge the existing mappings/type data with the existing one so that the types for
    // new variables are either given by the previous mapping (so, initially, from the
    // mapping file passed on the commandline) or the new one if new values are provided.
    // This means that new mapping data replaces old data, but old topic names and msg types
    // can still be used if new ones are not sent.
    fn merge_topic_msg_type_mappings(
        vars: &BTreeSet<VarName>,
        incoming_topic_mapping: &TopicMapping,
        existing_topic_mapping: &TopicMapping,
        incoming_msg_type_mapping: &MsgTypeMapping,
        existing_msg_type_mapping: &MsgTypeMapping,
    ) -> anyhow::Result<(TopicMapping, MsgTypeMapping)> {
        let merged_msg_type_mapping: MsgTypeMapping = vars
            .iter()
            .filter_map(|v| {
                incoming_msg_type_mapping
                    .get(v)
                    .cloned()
                    .or_else(|| existing_msg_type_mapping.get(v).cloned())
                    .map(|typ| (v.clone(), typ))
            })
            .collect();

        let missing: Vec<_> = vars
            .iter()
            .filter_map(|v| merged_msg_type_mapping.get(v).is_none().then_some(v.name()))
            .collect();
        if !missing.is_empty() {
            return Err(anyhow!("Missing type_info for vars: {:?}.", missing));
        }

        let merged_topics: TopicMapping = vars
            .iter()
            .map(|v| {
                let topic = incoming_topic_mapping
                    .get(v)
                    .cloned()
                    .or_else(|| existing_topic_mapping.get(v).cloned())
                    .unwrap_or_else(|| format!("/{}", v.name()));
                (v.clone(), topic)
            })
            .collect();

        Ok((merged_topics, merged_msg_type_mapping))
    }

    fn merge_topic_mappings(
        vars: BTreeSet<VarName>,
        incoming_topic_mapping: &TopicMapping,
        existing_topics: Option<&TopicMapping>,
    ) -> TopicMapping {
        let mut merged = existing_topics.cloned().unwrap_or_default();
        merged.extend(incoming_topic_mapping.clone());

        for v in vars {
            merged.entry(v.clone()).or_insert_with(|| v.to_string());
        }

        merged
    }

    fn reconf_input_from_map(
        map: &BTreeMap<ecow::EcoString, Value>,
    ) -> anyhow::Result<ReconfInput> {
        let spec = match map.get("spec") {
            Some(Value::Str(s)) => s.to_string(),
            Some(v) => {
                return Err(anyhow!(
                    "Invalid reconfiguration map: 'spec' must be a string, got {:?}",
                    v
                ));
            }
            None => {
                return Err(anyhow!(
                    "Invalid reconfiguration map: missing required field 'spec'"
                ));
            }
        };

        let to_string_mapping = |field_name: &str| -> anyhow::Result<BTreeMap<VarName, String>> {
            let Value::Map(inner) = map.get(field_name).ok_or_else(|| {
                anyhow!(
                    "Invalid reconfiguration map: missing required field '{}'",
                    field_name
                )
            })?
            else {
                return Err(anyhow!(
                    "Invalid reconfiguration map: '{}' must be an object",
                    field_name
                ));
            };

            inner
                .iter()
                .map(|(k, v)| match v {
                    Value::Str(s) => Ok((VarName::new(k.as_str()), s.to_string())),
                    other => Err(anyhow!(
                        "Invalid reconfiguration map: '{}' value for key '{}' must be a string, got {:?}",
                        field_name,
                        k,
                        other
                    )),
                })
                .collect()
        };

        let type_info = to_string_mapping("type_info")?;
        let topic_mapping = to_string_mapping("topic_mapping")?;

        Ok(ReconfInput {
            spec,
            type_info,
            topic_mapping,
        })
    }

    async fn handle_reconfig_input<'a>(
        &mut self,
        val: Value,
        context: &'a mut SemiSyncContext<AC>,
    ) -> anyhow::Result<Option<Self>> {
        let deserialized = match val {
            Value::Str(config) => {
                info!("Received reconfiguration command: {:?}", config);
                // TODO: This error message prints horribly... But I did not want to add another
                // crate like schemar just for this
                serde_json5::from_str::<ReconfInput>(&config).map_err(|err| {
                    anyhow!("Failed to deserialize reconfiguration command: {:?}", err)
                })?
            }
            Value::Map(map) => {
                info!("Received reconfiguration command as map payload");
                Self::reconf_input_from_map(&map)?
            }
            v => {
                return Err(anyhow!(
                    "Received invalid reconfiguration value type: {:?}",
                    v
                ));
            }
        };

        self.self_builder
            .known_topic_mapping
            .extend(deserialized.topic_mapping.clone());
        self.self_builder
            .known_type_info
            .extend(deserialized.type_info.clone());

        let parsed = P::parse(&mut deserialized.spec.as_str());
        info!("Parsed as: {:?}", parsed);
        let parsed =
            parsed.map_err(|err| anyhow!("Failed to parse reconfiguration command: {:?}", err))?;
        let old_model = self
            .self_builder
            .model
            .clone()
            .expect("Model must exist for reconfiguration");
        let reconf_topic: VarName = self
            .self_builder
            .reconf_topic
            .clone()
            .expect("Reconf topic must be set")
            .into();
        let old_input_set = old_model
            .input_vars()
            .into_iter()
            .filter(|v| *v != reconf_topic) // Exclude reconf topic from comparison
            .map(|v| v.name())
            .collect::<BTreeSet<_>>();
        let old_out_exprs = old_model
            .output_vars()
            .into_iter()
            .map(|v| {
                let expr = old_model.var_expr(&v).ok_or_else(|| {
                    anyhow!(
                        "Output variable {:?} must have an expression in the old model",
                        v
                    )
                })?;
                Ok((v.name(), expr))
            })
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        let new_input_set = parsed
            .input_vars()
            .into_iter()
            .map(|v| v.name())
            .collect::<BTreeSet<_>>();
        let out_exprs = parsed
            .output_vars()
            .into_iter()
            .map(|v| {
                let expr = parsed.var_expr(&v).ok_or_else(|| {
                    anyhow!(
                        "Output variable {:?} must have an expression in the new model",
                        v
                    )
                })?;
                Ok((v.name(), expr))
            })
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        if new_input_set == old_input_set && out_exprs == old_out_exprs {
            info!(
                "Reconfiguration does not change input vars or output expressions, skipping rebuild"
            );
            return Ok(None);
        }

        let added_inputs: Vec<_> = new_input_set.difference(&old_input_set).cloned().collect();
        let removed_inputs: Vec<_> = old_input_set.difference(&new_input_set).cloned().collect();
        if !added_inputs.is_empty() || !removed_inputs.is_empty() {
            info!(
                "Reconfiguration input vars changed. Added: {:?}, removed: {:?}",
                added_inputs, removed_inputs
            );
        }

        let old_out_keys: BTreeSet<_> = old_out_exprs.keys().cloned().collect();
        let new_out_keys: BTreeSet<_> = out_exprs.keys().cloned().collect();
        let added_outputs: Vec<_> = new_out_keys.difference(&old_out_keys).cloned().collect();
        let removed_outputs: Vec<_> = old_out_keys.difference(&new_out_keys).cloned().collect();
        let changed_outputs: Vec<_> = out_exprs
            .iter()
            .filter_map(|(name, expr)| {
                old_out_exprs
                    .get(name)
                    .filter(|old_expr| *old_expr != expr)
                    .map(|old_expr| (name.clone(), old_expr, expr))
            })
            .collect();
        if !added_outputs.is_empty() || !removed_outputs.is_empty() || !changed_outputs.is_empty() {
            info!(
                "Reconfiguration output expressions changed. Added: {:?}, removed: {:?}, changed: {:?}",
                added_outputs, removed_outputs, changed_outputs
            );
        }

        self.self_builder = self.self_builder.clone().model(parsed.clone());

        self.self_builder
            .input_factory
            .as_mut()
            .expect("Input factory must exist")
            .reconfigure(
                parsed.input_vars(),
                &self.self_builder.known_topic_mapping,
                &self.self_builder.known_type_info,
            )?;

        // Update OutputSpec
        let output_spec = match self.self_builder.output_builder.clone().unwrap().spec {
            OutputHandlerSpec::Stdout => OutputHandlerSpec::Stdout,
            // Requires type_info:
            OutputHandlerSpec::Ros(topic_mapping, msg_type_mapping) => {
                let vars = parsed.output_vars();
                let (new_topics, merged_type_info) = Self::merge_topic_msg_type_mappings(
                    &vars,
                    &self.self_builder.known_topic_mapping,
                    &topic_mapping,
                    &self.self_builder.known_type_info,
                    &msg_type_mapping,
                )?;
                OutputHandlerSpec::Ros(new_topics, merged_type_info)
            }
            OutputHandlerSpec::Mqtt(topics) => {
                OutputHandlerSpec::Mqtt(Some(Self::merge_topic_mappings(
                    parsed.output_vars(),
                    &self.self_builder.known_topic_mapping,
                    topics.as_ref(),
                )))
            }
            OutputHandlerSpec::Redis(topics) => {
                OutputHandlerSpec::Redis(Some(Self::merge_topic_mappings(
                    parsed.output_vars(),
                    &self.self_builder.known_topic_mapping,
                    topics.as_ref(),
                )))
            }
            OutputHandlerSpec::Manual(tx) => OutputHandlerSpec::Manual(tx),
        };
        if let Some(ref mut output_builder) = self.self_builder.output_builder {
            output_builder.spec = output_spec;
            self.self_builder.output_builder = Some(output_builder.clone());
        }

        if self.use_context_transfer {
            // Make history for new runtime of equal length, containing all variables, and padded with NoVal if needed.
            // NOTE: Using NoVal here, because this indicates the start of a new trace where no
            // values have previously been received on the stream.
            // Also, Deferred messes up signal semantics outputs. E.g., z = x + y and
            let mut retained_history = context.get_retained_history();
            let vars = self
                .self_builder
                .model
                .clone()
                .expect("Model must exist")
                .var_names();
            // Retain only variables that are still present in the new model
            retained_history.retain(|var_name, _| vars.contains(var_name));
            // Add empty history for new variables (should not be needed but better safe than sorry)
            vars.iter().for_each(|var| {
                retained_history.entry(var.clone()).or_default();
            });
            let longest_history = retained_history
                .values()
                .map(|h| h.len())
                .max()
                .unwrap_or(0);
            // Pad histories to be of equal length
            let starting_history = retained_history
                .into_iter()
                .map(|(var_name, hist)| {
                    let padding_needed = longest_history.saturating_sub(hist.len());
                    let mut padded_hist = vec![AC::Val::no_val_value(); padding_needed];
                    padded_hist.extend(hist);
                    (var_name, padded_hist)
                })
                .collect();

            self.self_builder.starting_history = Some(starting_history);
        }
        warn!(?self.self_builder.model, ?self.self_builder.input_factory, ?self.self_builder.starting_history, "Reconfiguring ReconfSemiSyncMonitor");
        context.cancel();
        // For now, reassign existing input stream with empty to shut down. In future when we can reconfig them, we should do that instead.
        self.input_stream = None;
        let new_self = Box::new(self.self_builder.clone()).build().await;
        Ok(Some(new_self))
    }

    fn process_input_updates<'a>(
        &'a mut self,
        input_ticks: &'a mut InputTickStream<AC::Val>,
        reconf_topic: &'a VarName,
        context: &'a mut SemiSyncContext<AC>,
        expr_evals: &'a mut Vec<ExprEvalutor<AC, MS>>,
    ) -> LocalBoxStream<'a, anyhow::Result<Option<Self>>> {
        Box::pin(try_stream! {
            loop {
                info!("ReconfSemiSyncRuntime: Waiting for inputs",);
                let Some(tick) = input_ticks.next().await else {
                    return;
                };
                let mut events = tick?;
                // If we have reconf then do only that, as reconfiguration is orthogonal to receiving
                // regular inputs
                if let Some(index) = events.iter().position(|event| &event.var == reconf_topic) {
                    match events.remove(index).value {
                        Value::NoVal => {
                            info!("Ignoring NoVal for reconfiguration command");
                        }
                        val => {
                            let new_self = self.handle_reconfig_input(val, context).await?;
                            if let Some(new_self) = new_self {
                                yield Some(new_self);
                            }
                            else {
                                // In case we received reconfig input but it did not lead to new
                                // RT, e.g., due to duplicate specs
                                events.clear();
                            }
                        }
                    }
                }

                if !events.is_empty() {
                    SemiSyncRuntime::<AC, MS>::advance_tick(
                        events,
                        context,
                        expr_evals,
                    ).await?;
                }
                yield None;
            }
        })
    }
}

#[cfg(test)]
mod input_tick_tests {
    use std::collections::BTreeMap;

    use async_unsync::bounded;
    use futures::future::LocalBoxFuture;
    use macro_rules_attribute::apply;

    use super::{ReconfSemiSyncRuntime, ReconfSemiSyncRuntimeBuilder};
    use crate::{
        OutputStream, Runtime, Specification, Value, VarName,
        core::OutputHandler,
        io::{InputStreamFactory, OutputHandlerBuilder, OutputHandlerSpec},
        lang::dsrv::{lalr_parser::LALRParser, parser::dsrv_specification},
        runtime::{
            RuntimeBuilder, builder::SemiSyncValueConfig, semi_sync::SemiSyncRuntimeBuilder,
        },
        semantics::UntimedDsrvSemantics,
    };

    type TestSemantics = UntimedDsrvSemantics<LALRParser>;
    type TestRuntime = ReconfSemiSyncRuntime<SemiSyncValueConfig, TestSemantics, LALRParser>;

    struct FailingOutputHandler;

    impl OutputHandler for FailingOutputHandler {
        type Val = Value;

        fn provide_streams(&mut self, _streams: BTreeMap<VarName, OutputStream<Value>>) {}

        fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
            Box::pin(async { anyhow::bail!("forced output failure") })
        }
    }

    #[apply(crate::async_test)]
    async fn reconfigurable_input_setup_errors_are_returned(
        executor: std::rc::Rc<smol::LocalExecutor<'static>>,
    ) {
        let spec = dsrv_specification(&mut "in x\nout z\nz = x").unwrap();
        let input_factory = InputStreamFactory::mqtt(Some(BTreeMap::new()), None);
        let (output_sender, _output_receiver) =
            bounded::channel::<BTreeMap<VarName, Value>>(1).into_split();
        let output_builder = OutputHandlerBuilder::new(OutputHandlerSpec::Manual(output_sender))
            .executor(executor.clone())
            .output_var_names(spec.output_vars());

        let runtime =
            ReconfSemiSyncRuntimeBuilder::<SemiSyncValueConfig, TestSemantics, LALRParser>::new()
                .executor(executor)
                .model(spec)
                .input_factory(input_factory)
                .output_builder(output_builder)
                .reconf_topic("reconfigure".into())
                .build()
                .await;

        let error = runtime
            .run()
            .await
            .expect_err("invalid input setup should be returned from run");
        let message = format!("{error:#}");
        assert!(
            message.contains("Topic mapping is missing topics"),
            "unexpected setup error: {message}"
        );
    }

    #[apply(crate::async_test)]
    async fn reconfigurable_output_errors_are_returned(
        executor: std::rc::Rc<smol::LocalExecutor<'static>>,
    ) {
        let spec = dsrv_specification(&mut "in x\nout z\nz = x").unwrap();
        let monitor = SemiSyncRuntimeBuilder::<SemiSyncValueConfig, TestSemantics>::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(Box::pin(futures::stream::empty()))
            .output(Box::new(FailingOutputHandler))
            .build()
            .await;
        let self_builder = ReconfSemiSyncRuntimeBuilder::new()
            .executor(executor)
            .model(spec)
            .reconf_topic("reconfigure".into());
        let runtime: TestRuntime = ReconfSemiSyncRuntime {
            semi_sync_monitor: Some(monitor),
            input_stream: Some(Ok(Box::pin(futures::stream::pending()))),
            self_builder,
            use_context_transfer: true,
            _marker: (std::marker::PhantomData, std::marker::PhantomData),
        };

        let run_result = tc_testutils::streams::with_timeout(
            runtime.run(),
            1,
            "reconfigurable output failure propagation",
        )
        .await
        .expect("runtime should not hang after an output failure");
        let error = run_result.expect_err("output failure should be returned from run");
        assert!(
            format!("{error:#}").contains("forced output failure"),
            "unexpected output error: {error:#}"
        );
    }
}

#[async_trait(?Send)]
impl<AC, MS, P> Runtime for ReconfSemiSyncRuntime<AC, MS, P>
where
    AC: AsyncConfig<Val = Value, Ctx = SemiSyncContext<AC>>,
    AC::Expr: DependencyGraphExpr + PartialEq + Debug,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
    P: SpecParser<AC::Spec>,
{
    async fn run_boxed(mut self: Box<Self>) -> anyhow::Result<()> {
        // TODO: Refactor inner loop to function

        // Outer loop that starts a new, potentially reconfigured, reconf monitor
        loop {
            let reconf_topic: VarName = self
                .self_builder
                .reconf_topic
                .clone()
                .expect("Reconf topic must be set")
                .into();

            // Includes reconf stream:
            let mut input_ticks = self.setup_input_stream()?;

            let monitor = self
                .semi_sync_monitor
                .take()
                .expect("SemiSyncRuntime must exist");
            let (mut output_handler, mut context, mut expr_evals) =
                Self::setup_inner_monitor(monitor).await?;

            let mut process_stream = self.process_input_updates(
                &mut input_ticks,
                &reconf_topic,
                &mut context,
                &mut expr_evals,
            );
            let mut pending_update = None;
            let output_fut = output_handler.run().fuse();
            futures::pin_mut!(output_fut);
            let mut output_finished = false;

            enum GenerationEvent<T> {
                Input(Option<anyhow::Result<Option<T>>>),
                Output(anyhow::Result<()>),
            }

            // Inner loop that runs the current reconf monitor as long as inputs are available and
            // checks for reconfiguration commands. If a reconfiguration command is received, it
            // breaks to start the new monitor with the new config.
            loop {
                let event = if output_finished {
                    GenerationEvent::Input(process_stream.next().await)
                } else {
                    futures::select! {
                        process_res = process_stream.next().fuse() => GenerationEvent::Input(process_res),
                        output_res = output_fut.as_mut() => GenerationEvent::Output(output_res),
                    }
                };

                match event {
                    GenerationEvent::Output(result) => {
                        result.context("Reconfigurable output handler failed")?;
                        output_finished = true;
                    }
                    GenerationEvent::Input(Some(Ok(Some(new_self)))) => {
                        debug!(
                            "ReconfSemiSyncRuntime: Received new configuration, preparing to switch runtimes"
                        );
                        pending_update = Some(new_self);
                        break;
                    }
                    GenerationEvent::Input(Some(Ok(None))) => continue,
                    GenerationEvent::Input(Some(Err(err))) => {
                        error!(
                            "Error processing inputs in ReconfSemiSyncRuntime: {:?}",
                            err
                        );
                        return Err(err);
                    }
                    GenerationEvent::Input(None) => break,
                }
            }
            drop(process_stream);
            drop(context);
            drop(expr_evals);

            if !output_finished {
                output_fut
                    .await
                    .context("Reconfigurable output handler failed")?;
            }

            if let Some(new_self) = pending_update {
                self = Box::new(new_self);
                info!("ReconfSemiSyncRuntime: Starting reconfigured runtime");
            } else {
                return Ok(());
            }
        }
    }
}
