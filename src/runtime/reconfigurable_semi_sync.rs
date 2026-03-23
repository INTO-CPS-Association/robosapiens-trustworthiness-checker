use crate::{
    OutputStream, SExpr, Value, VarName,
    core::{
        AbstractMonitorBuilder, DeferrableStreamData, InputProvider, OutputHandler, Runnable,
        Specification,
    },
    io::{
        InputProviderBuilder,
        builders::{
            InputProviderSpec, OutputHandlerBuilder, output_handler_builder::OutputHandlerSpec,
        },
        testing::ManualInputProvider,
    },
    lang::core::parser::SpecParser,
    runtime::semi_sync::{ExprEvalutor, SemiSyncContext, SemiSyncMonitor, SemiSyncMonitorBuilder},
    semantics::{AsyncConfig, MonitoringSemantics},
};
use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use futures::{
    FutureExt, StreamExt,
    future::LocalBoxFuture,
    stream::{FuturesUnordered, LocalBoxStream},
};
use serde::Deserialize;
use serde_json::Value as JValue;
use smol::LocalExecutor;
use std::{collections::BTreeMap, rc::Rc};
use tracing::{debug, error, info, warn};
use unsync::spsc::Sender as SpscSender;

// TODO: Because InputProviderBuilder is hardcoded to Value, this also needs to be in some places

#[derive(Deserialize, Clone, Debug)]
struct ReconfInput {
    spec: String,
    // TODO: Certain InputProviders are type-strong (e.g. Ros). These must know the types of Inputs
    // and Outputs. When type checking becomes more stable for the language, we
    // should use that instead of a variable here, and simply enforce that the spec is typed.
    #[allow(dead_code)]
    type_info: BTreeMap<String, String>,
}

#[derive(Clone)]
pub struct ReconfSemiSyncMonitorBuilder<AC, MS, P>
where
    AC: AsyncConfig<Expr = SExpr, Ctx = SemiSyncContext<AC>>,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
    P: SpecParser<AC::Spec>,
{
    executor: Option<Rc<LocalExecutor<'static>>>,
    model: Option<AC::Spec>,
    input_builder: Option<InputProviderBuilder>,
    output_builder: Option<OutputHandlerBuilder>,
    reconf_topic: Option<String>,
    _marker: (
        std::marker::PhantomData<MS>,
        std::marker::PhantomData<AC>,
        std::marker::PhantomData<P>,
    ),
}

impl<AC, MS, P> AbstractMonitorBuilder<AC::Spec, AC::Val>
    for ReconfSemiSyncMonitorBuilder<AC, MS, P>
where
    AC: AsyncConfig<Expr = SExpr, Val = Value, Ctx = SemiSyncContext<AC>>,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
    P: SpecParser<AC::Spec>,
{
    type Mon = ReconfSemiSyncMonitor<AC, MS, P>;

    fn new() -> Self {
        Self {
            executor: None,
            model: None,
            input_builder: None,
            output_builder: None,
            reconf_topic: None,
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
            "Direct InputProvider is not supported in ReconfSemiSyncMonitorBuilder. Use InputProviderBuilder instead."
        );
    }

    fn output(self, _output: Box<dyn OutputHandler<Val = AC::Val>>) -> Self {
        panic!(
            "Direct OutputHandler is not supported in ReconfSemiSyncMonitorBuilder. Use OutputHandlerBuilder instead."
        );
    }

    fn build(self) -> ReconfSemiSyncMonitor<AC, MS, P> {
        panic!("One does not simply build a ReconfSemiSync - use async_build instead!");
    }

    fn async_build(mut self: Box<Self>) -> LocalBoxFuture<'static, Self::Mon> {
        Box::pin(async move {
            let executor = self.executor.clone().unwrap();
            let output_builder = self.output_builder.clone().unwrap();
            let model = self.model.clone().unwrap();
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
            info!(
                ?self.input_builder,
                "Building ReconfSemiSyncMonitor input provider"
            );
            let input_provider = self.input_builder.clone().unwrap().async_build().await;
            info!(
                ?self.output_builder,
                "Building ReconfSemiSyncMonitor output handler"
            );
            let output = output_builder.async_build().await;
            let semi_sync_monitor = SemiSyncMonitorBuilder::new()
                .executor(executor.clone())
                .model(model)
                .input(inner_input)
                .output(output)
                .build();

            ReconfSemiSyncMonitor {
                executor,
                semi_sync_monitor: Some(semi_sync_monitor),
                input_provider,
                self_builder: *self,
                sender_channels,
                _marker: (std::marker::PhantomData, std::marker::PhantomData),
            }
        })
    }
}

impl<AC, MS, P> ReconfSemiSyncMonitorBuilder<AC, MS, P>
where
    AC: AsyncConfig<Expr = SExpr, Val = Value, Ctx = SemiSyncContext<AC>>,
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
            InputProviderSpec::Manual | InputProviderSpec::File(_) => {
                warn!(
                    "Limited support for reconfiguration of file inputs, ros inputs and manual. \
                 Treating var '{:?}' as reconfiguration variable",
                    reconf_topic
                );
                input_builder.spec.clone()
            }
            InputProviderSpec::MQTT(topics) | InputProviderSpec::Redis(topics) => {
                info!(
                    ?reconf_topic,
                    "Injecting reconf variable into InputProvider"
                );

                let mut var_topics: Vec<String> = topics.clone().unwrap_or_else(|| {
                    model
                        .input_vars()
                        .into_iter()
                        .map(|v| v.to_string())
                        .collect()
                });
                var_topics.push(reconf_topic.name());

                match input_builder.spec {
                    InputProviderSpec::MQTT(_) => InputProviderSpec::MQTT(Some(var_topics)),
                    InputProviderSpec::Redis(_) => InputProviderSpec::Redis(Some(var_topics)),
                    _ => unreachable!(),
                }
            }
            InputProviderSpec::Ros(json_info) => {
                info!(
                    ?reconf_topic,
                    "Injecting reconf variable into InputProvider"
                );
                // Read as JSON:
                let mut json = serde_json5::from_str::<JValue>(&json_info)
                    .expect("ROS input topics must be valid JSON");
                if let JValue::Object(obj) = &mut json {
                    obj.extend([(
                        reconf_topic.name(),
                        serde_json::json!({
                            "topic": format!("/{}", reconf_topic.name()),
                            "msg_type": "String",
                        }),
                    )]);
                }
                InputProviderSpec::Ros(json.to_string())
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

pub struct ReconfSemiSyncMonitor<AC, MS, P>
where
    AC: AsyncConfig<Expr = SExpr, Ctx = SemiSyncContext<AC>>,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
    P: SpecParser<AC::Spec>,
{
    executor: Rc<LocalExecutor<'static>>,
    semi_sync_monitor: Option<SemiSyncMonitor<AC, MS>>,
    input_provider: Box<dyn InputProvider<Val = AC::Val>>,
    self_builder: ReconfSemiSyncMonitorBuilder<AC, MS, P>,
    sender_channels: BTreeMap<VarName, SpscSender<AC::Val>>,
    _marker: (std::marker::PhantomData<MS>, std::marker::PhantomData<P>),
}

impl<AC, MS, P> ReconfSemiSyncMonitor<AC, MS, P>
where
    AC: AsyncConfig<Expr = SExpr, Val = Value, Ctx = SemiSyncContext<AC>>,
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
        monitor: SemiSyncMonitor<AC, MS>,
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
            async move { SemiSyncMonitor::<AC, MS>::input_task(&mut *input_provider).await }
                .boxed_local();

        Ok((output_fut, input_fut, context, expr_evals))
    }

    async fn await_inputs(
        streams: &mut BTreeMap<VarName, OutputStream<AC::Val>>,
    ) -> BTreeMap<VarName, Option<AC::Val>> {
        // Create input tasks
        let mut futs: FuturesUnordered<_> = streams
            .iter_mut()
            .map(|(name, stream)| {
                stream
                    .next()
                    .map(move |val| (name.clone(), val))
                    .boxed_local()
            })
            .collect();

        // Futs returns None when empty - does not indicate tasks result
        let mut ret = BTreeMap::new();
        while let Some((name, res)) = futs.next().await {
            if let Some(val) = res {
                ret.insert(name, Some(val));
            } else {
                // Not an error, most likely because the channel is done
                debug!("ReconfSemiSyncMonitor: Input stream for {} has ended", name);
                ret.insert(name, None);
            }
        }
        ret
    }

    async fn handle_reconfig_input(&mut self, val: Value) -> anyhow::Result<()> {
        match val {
            Value::Str(config) => {
                info!("Received reconfiguration command: {:?}", config);
                // TODO: This error message prints horribly... But I did not want to add another
                // crate like schemar just for this
                let deserialized =
                    serde_json5::from_str::<ReconfInput>(&config).map_err(|err| {
                        anyhow!("Failed to deserialize reconfiguration command: {:?}", err)
                    })?;
                let parsed = P::parse(&mut deserialized.spec.as_str());
                info!("Parsed as: {:?}", parsed);
                let parsed = parsed
                    .map_err(|err| anyhow!("Failed to parse reconfiguration command: {:?}", err))?;

                // TODO: Does not work with FileInputProvider as it reads the file from
                // fresh...
                self.self_builder = self.self_builder.clone().model(parsed.clone());

                // Update InputProvider spec
                let input_spec = match self.self_builder.input_builder.clone().unwrap().spec {
                    // TODO: does not respect _topics for any kind of InputProvider...
                    InputProviderSpec::MQTT(_topics) => InputProviderSpec::MQTT(Some(
                        parsed
                            .input_vars()
                            .into_iter()
                            .map(|v| v.to_string())
                            .collect(),
                    )),
                    InputProviderSpec::Ros(_json) => {
                        let types = deserialized.type_info.clone();
                        let vars = parsed.input_vars();
                        let missing: Vec<_> = vars
                            .iter()
                            .filter_map(|v| types.get(&v.name()).is_none().then_some(v.name()))
                            .collect();
                        if !missing.is_empty() {
                            return Err(anyhow!(
                                "Missing msg_types for vars: {:?}. Required for ROS2 InputProvider",
                                missing
                            ));
                        }
                        let combined = JValue::Object(
                            vars.into_iter()
                                .map(|v| {
                                    let name = v.name();
                                    let msg_type = types.get(&name).unwrap().clone(); // Safe now
                                    (
                                        name,
                                        serde_json::json!({
                                            "topic": format!("/{}", v),
                                            "msg_type": msg_type,
                                        }),
                                    )
                                })
                                .collect(),
                        );
                        InputProviderSpec::Ros(combined.to_string())
                    }
                    _ => self.self_builder.input_builder.clone().unwrap().spec,
                };
                if let Some(ref mut input_builder) = self.self_builder.input_builder {
                    input_builder.spec = input_spec;
                    self.self_builder.input_builder = Some(input_builder.clone());
                }

                // Update OutputSpec
                // TODO: Most matches do not respect _topics...
                let output_spec = match self.self_builder.output_builder.clone().unwrap().spec {
                    OutputHandlerSpec::Stdout => OutputHandlerSpec::Stdout,
                    // Requires type_info:
                    OutputHandlerSpec::Ros(_) => {
                        let types = deserialized.type_info.clone();
                        let vars = parsed.output_vars();
                        let missing: Vec<_> = vars
                            .iter()
                            .filter_map(|v| types.get(&v.name()).is_none().then_some(v.name()))
                            .collect();
                        if !missing.is_empty() {
                            return Err(anyhow!(
                                "Missing msg_types for vars: {:?}. Required for ROS2 InputProvider",
                                missing
                            ));
                        }
                        let combined = JValue::Object(
                            vars.into_iter()
                                .map(|v| {
                                    let name = v.name();
                                    let msg_type = types.get(&name).unwrap().clone(); // Safe now
                                    (
                                        name,
                                        serde_json::json!({
                                            "topic": format!("/{}", v),
                                            "msg_type": msg_type,
                                        }),
                                    )
                                })
                                .collect(),
                        );

                        OutputHandlerSpec::Ros(combined.to_string())
                    }
                    // Auto assign topics on rebuild instead - a problem if manual topics were configured
                    OutputHandlerSpec::MQTT(_) => OutputHandlerSpec::MQTT(None),
                    // Auto assign topics on rebuild instead - a problem if manual topics were configured
                    OutputHandlerSpec::Redis(_) => OutputHandlerSpec::Redis(None),
                    OutputHandlerSpec::Manual => unimplemented!("Not needed yet"),
                };
                if let Some(ref mut output_builder) = self.self_builder.output_builder {
                    output_builder.spec = output_spec;
                    self.self_builder.output_builder = Some(output_builder.clone());
                }

                warn!(?self.self_builder.model, ?self.self_builder.input_builder, "Reconfiguring ReconfSemiSyncMonitor");
                let new_self = Box::new(self.self_builder.clone()).async_build().await;
                new_self.run().await
            }
            v => Err(anyhow!(
                "Received invalid reconfiguration value type: {:?}",
                v
            )),
        }
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
    ) -> LocalBoxStream<'a, anyhow::Result<()>> {
        Box::pin(try_stream! {
            loop {
                info!("ReconfSemiSyncMonitor: Waiting for inputs",);
                let mut values = Self::await_inputs(input_streams).await;

                // If we have reconf then do only that, as reconfiguration is orthogonal to receiving
                // regular inputs
                if let Some(reconf_val) = values.remove(reconf_topic) {
                    match reconf_val {
                        Some(Value::NoVal) => {
                            info!("Ignoring NoVal for reconfiguration command");
                        }
                        Some(val) => {
                            self.handle_reconfig_input(val).await?;
                            return;
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
                    SemiSyncMonitor::<AC, MS>::step(context, expr_evals).await?;
                }
                yield ();
            }
        })
    }
}

#[async_trait(?Send)]
impl<AC, MS, P> Runnable for ReconfSemiSyncMonitor<AC, MS, P>
where
    AC: AsyncConfig<Expr = SExpr, Val = Value, Ctx = SemiSyncContext<AC>>,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
    P: SpecParser<AC::Spec>,
{
    async fn run_boxed(mut self: Box<Self>) -> anyhow::Result<()> {
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
            .expect("SemiSyncMonitor must exist");
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

        while let Some(ip_res) = input_provider_stream.next().await {
            ip_res.map_err(|err| {
                error!(
                    "ReconfSemiSyncMonitor: Input provider stream returned error: {:?}",
                    err
                );
                err
            })?;

            let Some(process_res) = process_stream.next().await else {
                info!("Process stream ended, shutting down ReconfSemiSyncMonitor");
                return Ok(());
            };

            process_res.map_err(|err| {
                error!("Error in ReconfSemiSyncMonitor main loop: {:?}", err);
                err
            })?;
        }

        info!("Input provider control stream ended, shutting down ReconfSemiSyncMonitor");
        Ok(())
    }
}
