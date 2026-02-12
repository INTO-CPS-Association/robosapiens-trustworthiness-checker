use crate::{
    OutputStream, Value, VarName,
    core::{
        AbstractMonitorBuilder, DeferrableStreamData, InputProvider, Monitor, OutputHandler,
        Runnable, Specification,
    },
    io::{
        InputProviderBuilder,
        builders::{InputProviderSpec, OutputHandlerBuilder},
        testing::ManualInputProvider,
    },
    lang::core::parser::SpecParser,
    runtime::semi_sync::{SemiSyncContext, SemiSyncMonitor, SemiSyncMonitorBuilder},
    semantics::{AsyncConfig, MonitoringSemantics},
};
use anyhow::anyhow;
use async_trait::async_trait;
use futures::{StreamExt, future::LocalBoxFuture};
use smol::LocalExecutor;
use std::{collections::BTreeMap, rc::Rc};
use tracing::{debug, error, info, warn};
use unsync::spsc::Sender as SpscSender;

const RECONF_TOPIC_NAME: &str = "reconf";

// TODO: Because InputProviderBuilder is hardcoded to Value, this also needs to be in some places

#[derive(Clone)]
pub struct ReconfSemiSyncMonitorBuilder<AC, S, MS, P>
where
    AC: AsyncConfig<Ctx = SemiSyncContext<AC>>,
    AC::Val: DeferrableStreamData,
    S: Specification<Expr = AC::Expr>,
    MS: MonitoringSemantics<AC>,
    P: SpecParser<S>,
{
    executor: Option<Rc<LocalExecutor<'static>>>,
    model: Option<S>,
    input_builder: Option<InputProviderBuilder>,
    output_builder: Option<OutputHandlerBuilder>,
    _marker: (
        std::marker::PhantomData<MS>,
        std::marker::PhantomData<AC>,
        std::marker::PhantomData<P>,
    ),
}

impl<AC, S, MS, P> AbstractMonitorBuilder<S, AC::Val> for ReconfSemiSyncMonitorBuilder<AC, S, MS, P>
where
    AC: AsyncConfig<Val = Value, Ctx = SemiSyncContext<AC>>,
    AC::Val: DeferrableStreamData,
    S: Specification<Expr = AC::Expr>,
    MS: MonitoringSemantics<AC>,
    P: SpecParser<S>,
{
    type Mon = ReconfSemiSyncMonitor<AC, S, MS, P>;

    fn new() -> Self {
        Self {
            executor: None,
            model: None,
            input_builder: None,
            output_builder: None,
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

    fn model(mut self, model: S) -> Self {
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

    fn build(self) -> ReconfSemiSyncMonitor<AC, S, MS, P> {
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
                semi_sync_monitor,
                input_provider,
                self_builder: *self,
                sender_channels,
                _marker: (std::marker::PhantomData, std::marker::PhantomData),
            }
        })
    }

    fn mqtt_reconfig_provider(self, _provider: crate::io::mqtt::MQTTLocalityReceiver) -> Self {
        todo!()
    }
}

impl<AC, S, MS, P> ReconfSemiSyncMonitorBuilder<AC, S, MS, P>
where
    AC: AsyncConfig<Val = Value, Ctx = SemiSyncContext<AC>>,
    AC::Val: DeferrableStreamData,
    S: Specification<Expr = AC::Expr>,
    MS: MonitoringSemantics<AC>,
    P: SpecParser<S>,
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

    fn inject_reconf_stream(&mut self) {
        let input_builder = self
            .input_builder
            .clone()
            .expect("Input builder must be set before injecting reconfiguration stream");

        // Early‑return if RECONF_TOPIC_NAME is already an input var
        let model = self.model.clone().expect("Input builder must have model");
        if model.input_vars().contains(&RECONF_TOPIC_NAME.into()) {
            info!(
                ?RECONF_TOPIC_NAME,
                "Reconfiguration variable already present in InputProviderModel, skipping injection"
            );
            return;
        }

        // Compute new spec with RECONF_TOPIC_NAME injected when applicable
        let new_spec = match &input_builder.spec {
            InputProviderSpec::Manual | InputProviderSpec::File(_) | InputProviderSpec::Ros(_) => {
                warn!(
                    "Limited support for reconfiguration of file inputs, ros inputs and manual. \
                 Treating var '{:?}' as reconfiguration variable",
                    RECONF_TOPIC_NAME
                );
                input_builder.spec.clone()
            }
            InputProviderSpec::MQTT(topics) | InputProviderSpec::Redis(topics) => {
                info!(
                    ?RECONF_TOPIC_NAME,
                    "Injecting reconf variable into InputProvider"
                );

                let mut var_topics: Vec<String> = topics.clone().unwrap_or_else(|| {
                    model
                        .input_vars()
                        .into_iter()
                        .map(|v| v.to_string())
                        .collect()
                });
                var_topics.push(RECONF_TOPIC_NAME.into());

                match input_builder.spec {
                    InputProviderSpec::MQTT(_) => InputProviderSpec::MQTT(Some(var_topics)),
                    InputProviderSpec::Redis(_) => InputProviderSpec::Redis(Some(var_topics)),
                    _ => unreachable!(),
                }
            }
        };

        // Update model and builder
        self.model
            .as_mut()
            .expect("Input builder must have model")
            .add_input_var(RECONF_TOPIC_NAME.into());
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

pub struct ReconfSemiSyncMonitor<AC, S, MS, P>
where
    AC: AsyncConfig<Ctx = SemiSyncContext<AC>>,
    AC::Val: DeferrableStreamData,
    S: Specification<Expr = AC::Expr>,
    MS: MonitoringSemantics<AC>,
    P: SpecParser<S>,
{
    executor: Rc<LocalExecutor<'static>>,
    semi_sync_monitor: SemiSyncMonitor<AC, S, MS>,
    input_provider: Box<dyn InputProvider<Val = AC::Val>>,
    self_builder: ReconfSemiSyncMonitorBuilder<AC, S, MS, P>,
    sender_channels: BTreeMap<VarName, SpscSender<AC::Val>>,
    _marker: (std::marker::PhantomData<MS>, std::marker::PhantomData<P>),
}

impl<AC, S, MS, P> ReconfSemiSyncMonitor<AC, S, MS, P>
where
    AC: AsyncConfig<Val = Value, Ctx = SemiSyncContext<AC>>,
    AC::Val: DeferrableStreamData,
    S: Specification<Expr = AC::Expr>,
    MS: MonitoringSemantics<AC>,
    P: SpecParser<S>,
{
    // Returns a configured input_provider and the input streams mapped by variable name
    async fn setup_input_provider(&mut self) -> BTreeMap<VarName, OutputStream<AC::Val>> {
        // Note: Must use model and not InputProvider directly due to bug with vars() in
        // InputProvider.
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
}

impl<AC, S, MS, P> Monitor<S, AC::Val> for ReconfSemiSyncMonitor<AC, S, MS, P>
where
    AC: AsyncConfig<Val = Value, Ctx = SemiSyncContext<AC>>,
    AC::Val: DeferrableStreamData,
    S: Specification<Expr = AC::Expr>,
    MS: MonitoringSemantics<AC>,
    P: SpecParser<S>,
{
    fn spec(&self) -> &S {
        unimplemented!("Don't think this is used anywhere...")
    }
}

#[async_trait(?Send)]
impl<AC, S, MS, P> Runnable for ReconfSemiSyncMonitor<AC, S, MS, P>
where
    AC: AsyncConfig<Val = Value, Ctx = SemiSyncContext<AC>>,
    AC::Val: DeferrableStreamData,
    S: Specification<Expr = AC::Expr>,
    MS: MonitoringSemantics<AC>,
    P: SpecParser<S>,
{
    // TODO: We are currently using a loop to poll each stream one at a time, which is quite inefficient.
    // The reason is that I have a weird bug when trying to merge the streams. The bug is that the
    // streams seem to hang forever after having yielded a single value.
    // See below.
    //
    // Merges input_streams into a SellectAll collection that can be awaited to get
    // VarNames/Values whenever they are ready
    // This does not work:
    // let mut merged_streams = stream::select_all(
    //     input_streams
    //         .into_iter()
    //         .map(|(key, stream)| {
    //             let key_clone = key.clone();
    //             stream.map(move |value| (key_clone.clone(), value))
    //         })
    //         .collect::<Vec<_>>(),
    // );
    //
    // This magically works:
    // let mut merged_streams = input_streams
    //     .remove(&("x".into()))
    //     .expect("x stream missing")
    //     .map(|value| ("x".to_string(), value));
    //
    // I have spent so long trying to find the bug but nothing seems to do the trick. Neither
    // pinning, fusing or boxing seems to catch this.
    async fn run_boxed(mut self: Box<Self>) -> anyhow::Result<()> {
        // It needs to be like a PuppetMaster around the InputProvider.
        // Whenever an Input is received it decides whether to forward it to the Inner runtime or
        // to reconfigure

        // Includes reconf stream:
        let mut input_streams = self.setup_input_provider().await;

        // TODO: Fix this...
        // Need to spawn input_provider in a separate task because of weird rule that they are
        // supposed to run forever...
        // This is especially important here as we want to turn it off sometimes.
        let mut input_provider_stream = self.input_provider.control_stream().await;
        let input_provider_future = Box::pin(async move {
            while let Some(res) = input_provider_stream.next().await {
                if res.is_err() {
                    error!(
                        "ReconfSemiSyncMonitor: Input provider stream returned error: {:?}",
                        res
                    );
                    return res;
                }
            }
            Ok(())
        });
        self.executor.spawn(input_provider_future).detach();
        // let inner_run = self.semi_sync_monitor.run().map(|res| {
        //     info!("Inner semi_sync_monitor.run() ended");
        //     res
        // });
        // TODO: Must not be a spawn, but it is not trivial since it requires us to define a
        // work_task function that is not using self, but still enables killing.
        self.executor.spawn(self.semi_sync_monitor.run()).detach();
        let mut res = Ok(());

        // For now just print values...
        loop {
            for (var, val) in input_streams.iter_mut() {
                info!(
                    "ReconfSemiSyncMonitor waiting for input on variable: {:?}...",
                    var
                );
                if let Some(val) = val.next().await {
                    if var == &RECONF_TOPIC_NAME.into() {
                        match val {
                            Value::Str(eco_string) => {
                                info!("Received reconfiguration command: {:?}", eco_string);
                                let parsed = P::parse(&mut eco_string.as_str());
                                info!("Parsed as: {:?}", parsed);
                                if let Ok(parsed) = parsed {
                                    // TODO: Does not work with FileInputProvider as it reads the file from
                                    // fresh...
                                    self.self_builder = self.self_builder.model(parsed.clone());

                                    // Update InputProvder spec
                                    let spec = match self
                                        .self_builder
                                        .input_builder
                                        .clone()
                                        .unwrap()
                                        .spec
                                    {
                                        // TODO: does not respect _topics...
                                        InputProviderSpec::MQTT(_topics) => {
                                            InputProviderSpec::MQTT(Some(
                                                parsed
                                                    .input_vars()
                                                    .into_iter()
                                                    .map(|v| v.to_string())
                                                    .collect(),
                                            ))
                                        }
                                        _ => self.self_builder.input_builder.clone().unwrap().spec,
                                    };
                                    if let Some(ref mut input_builder) =
                                        self.self_builder.input_builder
                                    {
                                        input_builder.spec = spec;
                                        self.self_builder.input_builder =
                                            Some(input_builder.clone());
                                    }
                                    warn!(?self.self_builder.model, ?self.self_builder.input_builder, "Reconfiguring ReconfSemiSyncMonitor - restarting inner runtime");
                                    let new_self =
                                        Box::new(self.self_builder.clone()).async_build().await;
                                    return new_self.run().await;
                                } else {
                                    let msg = format!(
                                        "Failed to parse reconfiguration command: {:?}",
                                        parsed.err()
                                    );
                                    error!("{}", &msg);
                                    res = Err(anyhow!(msg));
                                    break;
                                }
                            }
                            Value::NoVal => {
                                info!("Ignoring NoVal for reconfiguration command");
                            }
                            v => {
                                error!("Received invalid reconfiguration value type: {:?}", v);
                                res = Err(anyhow!(
                                    "Received invalid reconfiguration value type: {:?}",
                                    v
                                ));
                                break;
                            }
                        }
                    } else {
                        if let Some(chan) = self.sender_channels.get_mut(&var) {
                            info!(
                                "Forwarding to inner RT input for variable: {:?} with value: {:?}",
                                var, val
                            );
                            let send_res = chan.send(val).await;
                            if send_res.is_err() {
                                error!(
                                    "Failed to send for variable: {:?} with result: {:?}",
                                    var, send_res
                                );
                                res = Err(anyhow!(
                                    "Failed to send for variable: {:?} with result: {:?}",
                                    var,
                                    send_res
                                ));
                                break;
                            }
                            info!(
                                "Successfully forwarded to inner RT input for variable: {:?}",
                                var
                            );
                        } else {
                            error!("No sender channel found for variable: {:?}", var);
                            res = Err(anyhow!("No sender channel found for variable: {:?}", var));
                            break;
                        }
                    }
                } else {
                    // TODO: Remove it from input_streams but handle multiple borrows
                    info!("Input stream for variable {:?} ended", var);
                }
            }
            if res.is_err() {
                // Print error:
                error!("Error in ReconfSemiSyncMonitor main loop: {:?}", res);
                break;
            }
        }

        // while let Some((var, val)) = merged_streams.next().await {}
        // info!("ReconfSemiSyncMonitor.run_boxed() ending - waiting for inner to end");
        // let inner_res = inner_run.await;
        // if inner_res.is_err() {
        //     error!(
        //         "Inner semi_sync_monitor.run() ended with error: {:?}",
        //         inner_res
        //     );
        //     if res.is_ok() {
        //         res = inner_res;
        //     }
        // } else {
        //     info!("Inner semi_sync_monitor.run() ended successfully");
        // }
        res
    }
}
