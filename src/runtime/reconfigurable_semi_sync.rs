use crate::{
    OutputStream, Value, VarName,
    core::{DeferrableStreamData, InputProvider, OutputHandler, Runtime, Specification},
    io::{
        InputProviderBuilder, MsgTypeMapping, TopicMapping,
        builders::{
            InputProviderSpec, OutputHandlerBuilder, output_handler_builder::OutputHandlerSpec,
        },
        map::MapInputProvider,
        testing::ManualInputProvider,
    },
    lang::core::{DependencyGraphExpr, DependencyGraphSpec, parser::SpecParser},
    runtime::{
        RuntimeBuilder,
        semi_sync::{ExprEvalutor, SemiSyncContext, SemiSyncRuntime, SemiSyncRuntimeBuilder},
    },
    semantics::{AsyncConfig, MonitoringSemantics},
};
use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use futures::{
    FutureExt, StreamExt,
    future::{LocalBoxFuture, join_all},
    stream::LocalBoxStream,
};
use serde::Deserialize;
use smol::LocalExecutor;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    rc::Rc,
};
use tracing::{debug, error, info, warn};
use unsync::spsc::Sender as SpscSender;

// TODO: Because InputProviderBuilder is hardcoded to Value, this also needs to be in some places

#[derive(Deserialize, Clone, Debug)]
struct ReconfInput {
    spec: String,
    // TODO: Certain InputProviders are type-strong (e.g. Ros). These must know the types of Inputs
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
    input_builder: Option<InputProviderBuilder>,
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
            input_builder: None,
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
        if let Some(input_builder) = self.input_builder {
            self.input_builder = Some(input_builder.model(model.clone()));
        }
        if let Some(output_builder) = self.output_builder {
            self.output_builder = Some(output_builder.output_var_names(model.output_vars()));
        }
        self
    }

    fn input(self, _input: Box<dyn InputProvider<Val = AC::Val>>) -> Self {
        panic!(
            "Direct InputProvider is not supported in ReconfSemiSyncRuntimeBuilder. Use InputProviderBuilder instead."
        );
    }

    fn input_builder(mut self, input_builder: InputProviderBuilder) -> Self {
        self.input_builder = Some(match self.model.clone() {
            Some(model) => input_builder.model(model),
            None => input_builder,
        });
        self
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
            let mut inner_input = Box::new(ManualInputProvider::<AC>::new(model.input_vars()));
            let sender_channels = model
                .input_vars()
                .into_iter()
                .map(|v| {
                    (
                        v.clone(),
                        inner_input
                            .sender_channel(&v)
                            .expect("Should never happen unless bug in ManualInputProvider"),
                    )
                })
                .collect();
            self.inject_reconf_stream();

            if let Some(input_builder) = &self.input_builder {
                if let InputProviderSpec::Ros(topic_mapping, msg_type_mapping) = &input_builder.spec
                {
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

            info!(
                ?self.input_builder,
                "Building ReconfSemiSyncRuntime input provider"
            );
            let input_provider = self.input_builder.clone().unwrap().build().await;
            info!(
                ?self.output_builder,
                "Building ReconfSemiSyncRuntime output handler"
            );
            let output = output_builder.build().await;
            let semi_sync_monitor = SemiSyncRuntimeBuilder::new()
                .executor(executor.clone())
                .model(model)
                .input(inner_input)
                .output(output)
                .starting_history(starting_history)
                .build()
                .await;

            ReconfSemiSyncRuntime {
                executor,
                semi_sync_monitor: Some(semi_sync_monitor),
                input_provider,
                self_builder: self,
                sender_channels,
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
    pub fn input_builder(self, input_builder: InputProviderBuilder) -> Self {
        Self {
            input_builder: Some(input_builder),
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
        let input_builder = self
            .input_builder
            .clone()
            .expect("Input builder must be set before injecting reconfiguration stream");

        // Early‑return if self.reconf_topic is already an input var
        let model = self.model.clone().expect("Input builder must have model");
        let reconf_topic: VarName = self
            .reconf_topic
            .clone()
            .expect("Reconf topic must be set")
            .into();
        if model.input_vars().contains(&reconf_topic) {
            info!(
                ?reconf_topic,
                "Reconfiguration variable already present in InputProviderModel, skipping injection"
            );
            return;
        }

        // Compute new spec with reconf_topic injected when applicable
        let new_spec = match &input_builder.spec {
            InputProviderSpec::File(_) => {
                warn!(
                    "Limited support for reconfiguration of file inputs. \
                 Treating var '{:?}' as reconfiguration variable",
                    reconf_topic
                );
                input_builder.spec.clone()
            }
            InputProviderSpec::Manual(fan_rx) => {
                // No need to change enything here, as ManualInputProvider simply forwards whatever
                // it receives on the channel
                InputProviderSpec::Manual(fan_rx.clone())
            }
            InputProviderSpec::Mqtt(topics) | InputProviderSpec::Redis(topics) => {
                info!(
                    ?reconf_topic,
                    "Injecting reconf variable into InputProvider topics"
                );

                let mut var_topics: TopicMapping = topics.clone().unwrap_or_else(|| {
                    model
                        .input_vars()
                        .into_iter()
                        .map(|v| {
                            let topic: String = (&v).into();
                            (v, topic)
                        })
                        .collect()
                });
                var_topics.insert(reconf_topic.clone(), reconf_topic.name());

                match input_builder.spec {
                    InputProviderSpec::Mqtt(_) => InputProviderSpec::Mqtt(Some(var_topics)),
                    InputProviderSpec::Redis(_) => InputProviderSpec::Redis(Some(var_topics)),
                    _ => unreachable!(),
                }
            }
            InputProviderSpec::Ros(topic_mapping, msg_type_mapping) => {
                let mut topic_mapping = topic_mapping.clone();
                let mut msg_type_mapping = msg_type_mapping.clone();
                info!(
                    ?reconf_topic,
                    "Injecting reconf variable into InputProvider topics"
                );
                let reconf_topic_name = format!("/{}", reconf_topic.name());
                topic_mapping.insert(reconf_topic.clone(), reconf_topic_name.into());
                msg_type_mapping.insert(reconf_topic.clone(), "String".into());
                InputProviderSpec::Ros(topic_mapping, msg_type_mapping)
            }
        };

        // Update model and builder
        self.model
            .as_mut()
            .expect("Input builder must have model")
            .add_input_var(reconf_topic.into());
        // Including reconf topic
        let model = self.model.clone().expect("Model must exist");
        self.input_builder = Some(
            self.input_builder
                .clone()
                .unwrap()
                .spec(new_spec)
                .model(model),
        );
        debug!(
            ?self.input_builder,
            "Updated InputProviderBuilder with reconfiguration variable"
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
    executor: Rc<LocalExecutor<'static>>,
    semi_sync_monitor: Option<SemiSyncRuntime<AC, MS>>,
    input_provider: Box<dyn InputProvider<Val = AC::Val>>,
    self_builder: ReconfSemiSyncRuntimeBuilder<AC, MS, P>,
    sender_channels: BTreeMap<VarName, SpscSender<AC::Val>>,
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
    // Returns a configured input_provider and the input streams mapped by variable name
    async fn setup_input_provider(&mut self) -> BTreeMap<VarName, OutputStream<AC::Val>> {
        let input_streams = self
            .self_builder
            .model
            .clone()
            .expect("Model must exist")
            .input_vars()
            .into_iter()
            .map(|var| {
                let stream = self.input_provider.var_stream(&var);
                (
                    var.clone(),
                    stream.expect(&format!(
                        "Input stream unavailable for input variable: {}",
                        var
                    )),
                )
            })
            .collect::<BTreeMap<VarName, OutputStream<AC::Val>>>();
        input_streams
    }

    async fn inner_monitor_tasks(
        monitor: SemiSyncRuntime<AC, MS>,
    ) -> anyhow::Result<(
        LocalBoxFuture<'static, anyhow::Result<()>>,
        LocalBoxFuture<'static, anyhow::Result<()>>,
        SemiSyncContext<AC>,
        Vec<ExprEvalutor<AC, MS>>,
    )> {
        let (mut input_provider, mut output_handler, context, expr_evals) =
            monitor.setup_runtime().await?;
        let output_fut = async move { output_handler.run().await }.boxed_local();
        let input_fut =
            async move { SemiSyncRuntime::<AC, MS>::input_task(&mut *input_provider).await }
                .boxed_local();

        Ok((output_fut, input_fut, context, expr_evals))
    }

    async fn await_inputs(
        streams: &mut BTreeMap<VarName, OutputStream<AC::Val>>,
    ) -> BTreeMap<VarName, Option<AC::Val>> {
        // Collect (name, future) pairs to preserve key association
        let (names, futs): (Vec<_>, Vec<_>) = streams
            .iter_mut()
            .map(|(name, stream)| (name.clone(), stream.next()))
            .unzip();

        // Await all futures concurrently, results come back in the same order as names
        let results = join_all(futs).await;

        let mut ret = BTreeMap::new();
        for (name, res) in names.into_iter().zip(results) {
            if let Some(val) = res {
                ret.insert(name, Some(val));
            } else {
                debug!("ReconfSemiSyncRuntime: Input stream for {} has ended", name);
                ret.insert(name, None);
            }
        }
        ret
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

        // Update InputProvider spec
        let input_spec = match self.self_builder.input_builder.clone().unwrap().spec {
            InputProviderSpec::Mqtt(topics) => {
                InputProviderSpec::Mqtt(Some(Self::merge_topic_mappings(
                    parsed.input_vars(),
                    &self.self_builder.known_topic_mapping,
                    topics.as_ref(),
                )))
            }
            InputProviderSpec::Ros(topic_mapping, msg_type_mapping) => {
                let vars = parsed.input_vars();
                let (new_topic_mapping, new_type_mapping) = Self::merge_topic_msg_type_mappings(
                    &vars,
                    &self.self_builder.known_topic_mapping,
                    &topic_mapping,
                    &self.self_builder.known_type_info,
                    &msg_type_mapping,
                )?;
                InputProviderSpec::Ros(new_topic_mapping, new_type_mapping)
            }
            InputProviderSpec::File(_path) => {
                // TODO: Maybe this could be implemented if FileInputProvider had a way of telling us which line it
                // currently read on
                // (or simply by having a counter inside our RT and forwarding inputs up until that
                // point)
                unimplemented!(
                    "Reconfiguration of file inputs is not supported as it requires re-reading the input file, which causes the inputs to start over."
                )
            }
            InputProviderSpec::Redis(topics) => {
                InputProviderSpec::Redis(Some(Self::merge_topic_mappings(
                    parsed.input_vars(),
                    &self.self_builder.known_topic_mapping,
                    topics.as_ref(),
                )))
            }
            InputProviderSpec::Manual(tx) => InputProviderSpec::Manual(tx),
        };
        if let Some(ref mut input_builder) = self.self_builder.input_builder {
            input_builder.spec = input_spec;
            self.self_builder.input_builder = Some(input_builder.clone());
        }

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
        warn!(?self.self_builder.model, ?self.self_builder.input_builder, ?self.self_builder.starting_history, "Reconfiguring ReconfSemiSyncMonitor");
        // For now, reassign existing InputProvider with empty to shut down. In future when we can reconfig them, we should do that instead.
        self.input_provider = Box::new(MapInputProvider::new(BTreeMap::new()));
        let new_self = Box::new(self.self_builder.clone()).build().await;
        Ok(Some(new_self))
    }

    async fn handle_regular_input_update(
        &mut self,
        input_streams: &mut BTreeMap<VarName, OutputStream<AC::Val>>,
        var: VarName,
        val: Option<Value>,
    ) -> anyhow::Result<()> {
        let Some(val) = val else {
            info!("Input stream for variable {:?} ended", var);
            input_streams.remove(&var);
            return Ok(());
        };

        let chan = self
            .sender_channels
            .get_mut(&var)
            .ok_or_else(|| anyhow!("No sender channel found for variable: {:?}", var))?;

        info!(
            "Forwarding to inner RT input for variable: {:?} with value: {:?}",
            var, val
        );

        chan.send(val).await.map_err(|send_err| {
            anyhow!(
                "Failed to send for variable: {:?} with result: {:?}",
                var,
                send_err
            )
        })
    }

    fn process_input_updates<'a>(
        &'a mut self,
        input_streams: &'a mut BTreeMap<VarName, OutputStream<AC::Val>>,
        reconf_topic: &'a VarName,
        context: &'a mut SemiSyncContext<AC>,
        expr_evals: &'a mut Vec<ExprEvalutor<AC, MS>>,
    ) -> LocalBoxStream<'a, anyhow::Result<Option<Self>>> {
        Box::pin(try_stream! {
            loop {
                info!("ReconfSemiSyncRuntime: Waiting for inputs",);
                let mut values = Self::await_inputs(input_streams).await;

                // If we have reconf then do only that, as reconfiguration is orthogonal to receiving
                // regular inputs
                if let Some(reconf_val) = values.remove(reconf_topic) {
                    match reconf_val {
                        Some(Value::NoVal) => {
                            info!("Ignoring NoVal for reconfiguration command");
                        }
                        Some(val) => {
                            let new_self = self.handle_reconfig_input(val, context).await?;
                            if let Some(new_self) = new_self {
                                yield Some(new_self);
                            }
                            else {
                                // In case we received reconfig input but it did not lead to new
                                // RT, e.g., due to duplicate specs
                                values.clear();
                            }
                        }
                        None => {
                            info!("Input stream for variable {:?} ended", reconf_topic);
                            input_streams.remove(reconf_topic);
                        }
                    }
                }

                // Done synchronously because we would have to take/give back sender_channels otherwise,
                // which is a bit annoying
                let mut forwarded_regular_input = false;
                for (var, val) in values {
                    self.handle_regular_input_update(input_streams, var, val)
                    .await?;
                    forwarded_regular_input = true;
                }

                if forwarded_regular_input {
                    SemiSyncRuntime::<AC, MS>::step(context, expr_evals).await?;
                }
                yield None;
            }
        })
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
            let mut input_streams = self.setup_input_provider().await;
            let mut input_provider_stream = self.input_provider.control_stream().await;

            let monitor = self
                .semi_sync_monitor
                .take()
                .expect("SemiSyncRuntime must exist");
            let (inner_input_task, output_task, mut context, mut expr_evals) =
                Self::inner_monitor_tasks(monitor).await?;
            // TODO: Don't spawn these
            self.executor.spawn(inner_input_task).detach();
            self.executor.spawn(output_task).detach();

            let mut process_stream = self.process_input_updates(
                &mut input_streams,
                &reconf_topic,
                &mut context,
                &mut expr_evals,
            );
            let mut pending_update = None;

            // Inner loop that runs the current reconf monitor as long as inputs are available and
            // checks for reconfiguration commands. If a reconfiguration command is received, it
            // breaks to start the new monitor with the new config.
            while let Some(ip_res) = input_provider_stream.next().await {
                ip_res.map_err(|err| {
                    error!(
                        "ReconfSemiSyncRuntime: Input provider stream returned error: {:?}",
                        err
                    );
                    err
                })?;

                let Some(process_res) = process_stream.next().await else {
                    info!("ReconfSemiSyncRuntime: Input streams ended. Shutting down.");
                    return Ok(());
                };
                match process_res {
                    Ok(Some(new_self)) => {
                        debug!(
                            "ReconfSemiSyncRuntime: Received new configuration, preparing to switch runtimes"
                        );
                        pending_update = Some(new_self);
                        break;
                    }
                    Ok(None) => continue, // No reconfiguration, continue processing inputs
                    Err(err) => {
                        error!(
                            "Error processing inputs in ReconfSemiSyncRuntime: {:?}",
                            err
                        );
                        return Err(err);
                    }
                };
            }
            drop(process_stream);

            if let Some(new_self) = pending_update {
                self = Box::new(new_self);
                info!("ReconfSemiSyncRuntime: Starting reconfigured runtime");
            } else {
                return Ok(());
            }
        }
    }
}
