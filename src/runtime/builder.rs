use std::fmt::Debug;
use std::rc::Rc;

use futures::future::LocalBoxFuture;
use smol::LocalExecutor;
use tracing::{debug, warn};

use crate::{
    LOLASpecification, Monitor, Value, VarName,
    cli::{adapters::DistributionModeBuilder, args::ParserMode},
    core::{AbstractMonitorBuilder, OutputHandler, Runnable, Runtime, Semantics, StreamData},
    io::{InputProviderBuilder, builders::OutputHandlerBuilder, mqtt::MQTTLocalityReceiver},
    lang::dynamic_lola::{
        lalr_parser::LALRExprParser,
        parser::CombExprParser,
        type_checker::{TypedLOLASpecification, type_check},
    },
    runtime::reconfigurable_async::ReconfAsyncMonitorBuilder,
    semantics::{
        DistributedSemantics, TypedUntimedLolaSemantics, UntimedLolaSemantics,
        distributed::{contexts::DistributedContext, localisation::LocalitySpec},
    },
};

use super::{
    asynchronous::{AsyncMonitorBuilder, Context},
    distributed::{DistAsyncMonitorBuilder, SchedulerCommunication},
};

use static_assertions::assert_obj_safe;

pub trait AnonymousMonitorBuilder<M, V: StreamData>: 'static {
    fn executor(
        self: Box<Self>,
        ex: Rc<LocalExecutor<'static>>,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>>;

    fn model(self: Box<Self>, model: M) -> Box<dyn AnonymousMonitorBuilder<M, V>>;

    // fn input(self, input: Box<dyn InputProvider<Val = V>>) -> Self;
    fn input(
        self: Box<Self>,
        input: Box<dyn crate::InputProvider<Val = V>>,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>>;

    fn input_builder(
        self: Box<Self>,
        input_builder: InputProviderBuilder,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>>;

    fn output(
        self: Box<Self>,
        output: Box<dyn OutputHandler<Val = V>>,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>>;

    fn mqtt_reconfig_provider(
        self: Box<Self>,
        provider: MQTTLocalityReceiver,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>>;

    fn build(self: Box<Self>) -> Box<dyn Runnable>;

    fn async_build(self: Box<Self>) -> LocalBoxFuture<'static, Box<dyn Runnable>>;
}

assert_obj_safe!(AnonymousMonitorBuilder<(), ()>);

impl<
    M,
    V: StreamData,
    Mon: Runnable + 'static,
    MonBuilder: AbstractMonitorBuilder<M, V, Mon = Mon> + 'static,
> AnonymousMonitorBuilder<M, V> for MonBuilder
{
    fn executor(
        self: Box<Self>,
        ex: Rc<LocalExecutor<'static>>,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>> {
        Box::new(MonBuilder::executor(*self, ex))
    }

    fn model(self: Box<Self>, model: M) -> Box<dyn AnonymousMonitorBuilder<M, V>> {
        Box::new(MonBuilder::model(*self, model))
    }

    fn input(
        self: Box<Self>,
        input: Box<dyn crate::InputProvider<Val = V>>,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>> {
        Box::new(MonBuilder::input(*self, input))
    }

    fn input_builder(
        self: Box<Self>,
        _input_builder: InputProviderBuilder,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>> {
        panic!("This builder type does not support input_builder method")
    }

    fn output(
        self: Box<Self>,
        output: Box<dyn OutputHandler<Val = V>>,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>> {
        Box::new(MonBuilder::output(*self, output))
    }

    fn mqtt_reconfig_provider(
        self: Box<Self>,
        provider: MQTTLocalityReceiver,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>> {
        Box::new(MonBuilder::mqtt_reconfig_provider(*self, provider))
    }

    fn build(self: Box<Self>) -> Box<dyn Runnable> {
        Box::new(MonBuilder::build(*self))
    }

    fn async_build(self: Box<Self>) -> LocalBoxFuture<'static, Box<dyn Runnable>> {
        Box::pin(async move { Box::new(MonBuilder::async_build(self).await) as Box<dyn Runnable> })
    }
}

struct TypeCheckingBuilder<Builder>(Builder);

impl<
    V: StreamData,
    Mon: Monitor<TypedLOLASpecification, V> + 'static,
    MonBuilder: AbstractMonitorBuilder<TypedLOLASpecification, V, Mon = Mon> + 'static,
> AbstractMonitorBuilder<LOLASpecification, V> for TypeCheckingBuilder<MonBuilder>
{
    type Mon = Mon;

    fn new() -> Self {
        Self(MonBuilder::new())
    }

    fn executor(self, ex: Rc<LocalExecutor<'static>>) -> Self {
        Self(self.0.executor(ex))
    }

    fn model(self, model: LOLASpecification) -> Self {
        let model = type_check(model).expect("Model failed to type check");
        Self(self.0.model(model))
    }

    fn input(self, input: Box<dyn crate::InputProvider<Val = V>>) -> Self {
        Self(self.0.input(input))
    }

    fn output(self, output: Box<dyn OutputHandler<Val = V>>) -> Self {
        Self(self.0.output(output))
    }

    fn mqtt_reconfig_provider(self, provider: crate::io::mqtt::MQTTLocalityReceiver) -> Self {
        Self(self.0.mqtt_reconfig_provider(provider))
    }

    fn build(self) -> Self::Mon {
        let builder = self.0.build();
        // Perform type checking here
        builder
    }

    fn async_build(self: Box<Self>) -> LocalBoxFuture<'static, Self::Mon> {
        Box::pin(async move { (*self).build() })
    }
}

pub enum DistributionMode {
    CentralMonitor,
    LocalMonitor(Box<dyn LocalitySpec>), // Local topics
    LocalMonitorWithReceiverAndLocality(
        Box<dyn LocalitySpec>,
        crate::io::mqtt::MQTTLocalityReceiver,
    ), // Local topics with receiver for reconfiguration
    /// Receiver for reconfiguration but no current local monitor; this is for dynamic
    /// reconfiguration each timestep
    ReconfigurableLocalMonitor(crate::io::mqtt::MQTTLocalityReceiver),
    DistributedCentralised(
        /// Location names
        Vec<String>,
    ),
    DistributedRandom(
        /// Location names
        Vec<String>,
    ),
    DistributedOptimizedStatic(
        /// Location names
        Vec<String>,
        /// Variables which represent the constraints which determine the static distribution
        Vec<VarName>,
    ),
    DistributedOptimizedDynamic(
        /// Location names
        Vec<String>,
        /// Variables which represent the constraints which determine the static distribution
        Vec<VarName>,
    ),
}

impl Debug for DistributionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DistributionMode::CentralMonitor => write!(f, "CentralMonitor"),
            DistributionMode::LocalMonitor(_) => write!(f, "LocalMonitor"),
            DistributionMode::LocalMonitorWithReceiverAndLocality(_, _) => {
                write!(f, "LocalMonitorWithReceiverAndLocality")
            }
            DistributionMode::ReconfigurableLocalMonitor(_) => {
                write!(f, "LocalMonitorWithReceiver")
            }
            DistributionMode::DistributedCentralised(locations) => {
                write!(f, "DistributedCentralised({:?})", locations)
            }
            DistributionMode::DistributedRandom(locations) => {
                write!(f, "DistributedRandom({:?})", locations)
            }
            DistributionMode::DistributedOptimizedStatic(locations, dist_constraints) => {
                write!(
                    f,
                    "DistributedOptimizedStatic({:?}, {:?})",
                    locations, dist_constraints
                )
            }
            DistributionMode::DistributedOptimizedDynamic(locations, dist_constraints) => {
                write!(
                    f,
                    "DistributedOptimizedDynamic({:?}, {:?})",
                    locations, dist_constraints
                )
            }
        }
    }
}

pub struct GenericMonitorBuilder<M, V: StreamData> {
    pub executor: Option<Rc<LocalExecutor<'static>>>,
    pub model: Option<M>,
    pub input: Option<Box<dyn crate::InputProvider<Val = V>>>,
    pub input_provider_builder: Option<InputProviderBuilder>,
    pub output: Option<Box<dyn OutputHandler<Val = V>>>,
    pub output_handler_builder: Option<OutputHandlerBuilder>,
    pub runtime: Runtime,
    pub semantics: Semantics,
    pub distribution_mode: DistributionMode,
    pub distribution_mode_builder: Option<DistributionModeBuilder>,
    pub mqtt_reconfig_provider: Option<MQTTLocalityReceiver>,
    pub scheduler_mode: SchedulerCommunication,
    pub parser: ParserMode,
}

impl<M, V: StreamData> GenericMonitorBuilder<M, V> {
    pub fn runtime(self, runtime: Runtime) -> Self {
        Self { runtime, ..self }
    }

    pub fn maybe_runtime(self, runtime: Option<Runtime>) -> Self {
        match runtime {
            Some(runtime) => self.runtime(runtime),
            None => self,
        }
    }

    pub fn semantics(self, semantics: Semantics) -> Self {
        Self { semantics, ..self }
    }

    pub fn maybe_semantics(self, semantics: Option<Semantics>) -> Self {
        match semantics {
            Some(semantics) => self.semantics(semantics),
            None => self,
        }
    }

    pub fn distribution_mode(self, dist_mode: DistributionMode) -> Self {
        Self {
            distribution_mode: dist_mode,
            ..self
        }
    }

    pub fn distribution_mode_builder(
        self,
        distribution_mode_builder: DistributionModeBuilder,
    ) -> Self {
        Self {
            distribution_mode_builder: Some(distribution_mode_builder),
            ..self
        }
    }

    pub fn input_provider_builder(self, builder: InputProviderBuilder) -> Self {
        Self {
            input_provider_builder: Some(builder),
            ..self
        }
    }

    pub fn output_handler_builder(self, builder: OutputHandlerBuilder) -> Self {
        Self {
            output_handler_builder: Some(builder),
            ..self
        }
    }

    pub fn maybe_distribution_mode(self, dist_mode: Option<DistributionMode>) -> Self {
        match dist_mode {
            Some(dist_mode) => self.distribution_mode(dist_mode),
            None => self,
        }
    }

    pub fn scheduler_mode(self, scheduler_mode: impl Into<SchedulerCommunication>) -> Self {
        Self {
            scheduler_mode: scheduler_mode.into(),
            ..self
        }
    }

    pub fn parser(self, parser: ParserMode) -> Self {
        Self { parser, ..self }
    }
}

impl AbstractMonitorBuilder<LOLASpecification, Value>
    for GenericMonitorBuilder<LOLASpecification, Value>
{
    type Mon = Box<dyn Runnable>;

    fn new() -> Self {
        Self {
            executor: None,
            model: None,
            input: None,
            input_provider_builder: None,
            output: None,
            output_handler_builder: None,
            distribution_mode: DistributionMode::CentralMonitor,
            distribution_mode_builder: None,
            runtime: Runtime::Async,
            semantics: Semantics::Untimed,
            mqtt_reconfig_provider: None,
            scheduler_mode: SchedulerCommunication::Null,
            parser: ParserMode::Lalr,
        }
    }

    fn executor(self, ex: Rc<LocalExecutor<'static>>) -> Self {
        Self {
            executor: Some(ex),
            ..self
        }
    }

    fn model(self, model: LOLASpecification) -> Self {
        Self {
            model: Some(model),
            ..self
        }
    }

    fn input(self, input: Box<dyn crate::InputProvider<Val = Value>>) -> Self {
        Self {
            input: Some(input),
            ..self
        }
    }

    fn output(self, output: Box<dyn OutputHandler<Val = Value>>) -> Self {
        Self {
            output: Some(output),
            ..self
        }
    }

    fn mqtt_reconfig_provider(self, provider: MQTTLocalityReceiver) -> Self {
        // Generic builder doesn't directly use the MQTT reconfiguration provider
        Self {
            mqtt_reconfig_provider: Some(provider),
            ..self
        }
    }

    fn build(self) -> Self::Mon {
        if self.distribution_mode_builder.is_some()
            || self.input_provider_builder.is_some()
            || self.output_handler_builder.is_some()
        {
            panic!("Call async_build instead");
        }

        let builder: Box<dyn AnonymousMonitorBuilder<LOLASpecification, Value>> =
            Self::create_common_builder(
                self.runtime,
                self.semantics,
                self.parser,
                self.executor,
                self.model,
                self.mqtt_reconfig_provider,
                self.distribution_mode,
                self.scheduler_mode,
                self.input_provider_builder.clone(),
                self.output_handler_builder.clone(),
            );

        let builder = if let Some(output) = self.output {
            builder.output(output)
        } else {
            builder
        };
        let builder = if let Some(input) = self.input {
            builder.input(input)
        } else {
            builder
        };

        builder.build()
    }

    fn async_build(self: Box<Self>) -> LocalBoxFuture<'static, Self::Mon> {
        Box::pin(async move { (*self).async_build().await })
    }
}

impl GenericMonitorBuilder<LOLASpecification, Value> {
    // Creates the common parts of the builder
    fn create_common_builder(
        runtime: Runtime,
        semantics: Semantics,
        parser: ParserMode,
        executor: Option<Rc<LocalExecutor<'static>>>,
        model: Option<LOLASpecification>,
        mqtt_reconfig_provider: Option<MQTTLocalityReceiver>,
        distribution_mode: DistributionMode,
        scheduler_mode: SchedulerCommunication,
        input_provider_builder: Option<InputProviderBuilder>,
        output_handler_builder: Option<OutputHandlerBuilder>,
    ) -> Box<dyn AnonymousMonitorBuilder<LOLASpecification, Value>> {
        debug!(
            "Creating common builder with distribution mode: {:?}",
            distribution_mode
        );
        let builder: Box<dyn AnonymousMonitorBuilder<LOLASpecification, Value>> = match (
            runtime, semantics, parser,
        ) {
            (Runtime::Async, Semantics::Untimed, ParserMode::Lalr) => {
                Box::new(AsyncMonitorBuilder::<
                    LOLASpecification,
                    Context<Value>,
                    Value,
                    _,
                    UntimedLolaSemantics<LALRExprParser>,
                >::new())
            }
            (Runtime::Async, Semantics::Untimed, ParserMode::Combinator) => {
                Box::new(AsyncMonitorBuilder::<
                    LOLASpecification,
                    Context<Value>,
                    Value,
                    _,
                    UntimedLolaSemantics<CombExprParser>,
                >::new())
            }
            (Runtime::Async, Semantics::TypedUntimed, _) => {
                Box::new(TypeCheckingBuilder(AsyncMonitorBuilder::<
                    TypedLOLASpecification,
                    Context<Value>,
                    Value,
                    _,
                    TypedUntimedLolaSemantics,
                >::new()))
            }
            (Runtime::ReconfigurableAsync, Semantics::Untimed, ParserMode::Lalr) => {
                let mut builder = ReconfAsyncMonitorBuilder::<
                    LOLASpecification,
                    // Reconfigurable async runtime does not work with DistributedContext
                    // or DistributedSemantics as it has no way of proving the graph stream for the
                    // network topology
                    Context<Value>,
                    Value,
                    _,
                    UntimedLolaSemantics<LALRExprParser>,
                >::new();

                debug!(
                    "Checking runtime distribution mode: {:?}",
                    distribution_mode
                );
                if let DistributionMode::LocalMonitorWithReceiverAndLocality(_, receiver) =
                    &distribution_mode
                {
                    debug!("Building runtime with LocalMonitorWithReceiverAndLocality");
                    // If we have a LocalMonitorWithReceiver, pass the receiver to the builder
                    builder = builder.reconf_provider(receiver.clone());
                } else if let DistributionMode::ReconfigurableLocalMonitor(receiver) =
                    &distribution_mode
                {
                    debug!("Building runtime with ReconfigurableLocalMonitor");
                    // If we have a ReconfigurableLocalMonitor, pass the receiver to the builder
                    builder = builder.reconf_provider(receiver.clone());
                } else {
                    debug!(
                        "No matching distribution mode found for MQTT receiver, mode was: {:?}",
                        distribution_mode
                    );
                }

                // For reconfigurable runtime, pass builders instead of built providers
                if let Some(input_provider_builder) = input_provider_builder {
                    builder = builder.input_builder(input_provider_builder);
                }
                if let Some(output_handler_builder) = output_handler_builder {
                    builder = builder.output_builder(output_handler_builder);
                }

                Box::new(builder)
            }
            (Runtime::ReconfigurableAsync, Semantics::Untimed, ParserMode::Combinator) => {
                let mut builder = ReconfAsyncMonitorBuilder::<
                    LOLASpecification,
                    // Reconfigurable async runtime does not work with DistributedContext
                    // as it has no way of proving the graph stream for the network topology
                    Context<Value>,
                    Value,
                    _,
                    UntimedLolaSemantics<CombExprParser>,
                >::new();

                // If we have a LocalMonitorWithReceiver, pass the receiver to the builder
                debug!(
                    "Checking Combinator runtime distribution mode: {:?}",
                    distribution_mode
                );
                if let DistributionMode::LocalMonitorWithReceiverAndLocality(_, receiver) =
                    &distribution_mode
                {
                    debug!("Combinator: Building runtime with LocalMonitorWithReceiverAndLocality");
                    builder = builder.reconf_provider(receiver.clone());
                } else if let DistributionMode::ReconfigurableLocalMonitor(receiver) =
                    &distribution_mode
                {
                    debug!("Combinator: Building runtime with ReconfigurableLocalMonitor");
                    builder = builder.reconf_provider(receiver.clone());
                } else {
                    debug!(
                        "Combinator: No matching distribution mode for MQTT receiver, mode was: {:?}",
                        distribution_mode
                    );
                }

                builder = builder.maybe_mqtt_reconfig_provider(mqtt_reconfig_provider);

                // For reconfigurable runtime, pass builders instead of built providers
                if let Some(input_provider_builder) = input_provider_builder {
                    builder = builder.input_builder(input_provider_builder);
                }
                if let Some(output_handler_builder) = output_handler_builder {
                    builder = builder.output_builder(output_handler_builder);
                }

                Box::new(builder)
            }
            (Runtime::Distributed, Semantics::Untimed, _) => {
                debug!(
                    "Setting up distributed runtime with distribution_mode = {:?}",
                    distribution_mode
                );
                if matches!(parser, ParserMode::Combinator) {
                    // Because we would need to duplicate all this DistAsyncMonitorBuilder code or
                    // implement an AnonymousDistAsyncMonitorBuilder...
                    warn!(
                        "Combinator parser not supported for DUPs with Distributed Runtime. Defaulting to LALR parser."
                    );
                }

                let builder = DistAsyncMonitorBuilder::<
                    LOLASpecification,
                    DistributedContext<Value>,
                    Value,
                    _,
                    DistributedSemantics<LALRExprParser>,
                >::new();

                let builder = builder.maybe_mqtt_reconfig_provider(mqtt_reconfig_provider);

                let builder = builder.scheduler_mode(scheduler_mode);
                let builder = match distribution_mode {
                    DistributionMode::CentralMonitor => builder,
                    DistributionMode::LocalMonitor(_)
                    | DistributionMode::LocalMonitorWithReceiverAndLocality(_, _)
                    | DistributionMode::ReconfigurableLocalMonitor(_) => {
                        todo!("Local monitor not implemented here yet")
                    }
                    DistributionMode::DistributedCentralised(locations) => {
                        let locations = locations
                            .into_iter()
                            .map(|loc| (loc.clone().into(), loc))
                            .collect();
                        builder.mqtt_centralised_dist_graph(locations)
                    }
                    DistributionMode::DistributedRandom(locations) => {
                        let locations = locations
                            .into_iter()
                            .map(|loc| (loc.clone().into(), loc))
                            .collect();
                        builder.mqtt_random_dist_graph(locations)
                    }
                    DistributionMode::DistributedOptimizedStatic(locations, dist_constraints) => {
                        let locations = locations
                            .into_iter()
                            .map(|loc| (loc.clone().into(), loc))
                            .collect();
                        builder.mqtt_optimized_static_dist_graph(locations, dist_constraints)
                    }
                    DistributionMode::DistributedOptimizedDynamic(locations, dist_constraints) => {
                        let locations = locations
                            .into_iter()
                            .map(|loc| (loc.clone().into(), loc))
                            .collect();
                        builder.mqtt_optimized_dynamic_dist_graph(locations, dist_constraints)
                    }
                };

                Box::new(builder)
            }
            _ => {
                panic!("Unsupported runtime and semantics combination");
            }
        };

        let builder = match executor {
            Some(ex) => builder.executor(ex),
            None => builder,
        };
        let builder = match model {
            Some(model) => builder.model(model),
            None => builder,
        };
        builder
    }

    pub async fn async_build(self) -> Box<dyn Runnable> {
        let distribution_mode = match self.distribution_mode_builder {
            // TODO: add error handling to this method
            Some(distribution_mode_builder) => {
                debug!("Building with distribution_mode_builder");
                distribution_mode_builder
                    .build()
                    .await
                    .expect("Failed to build distribution mode")
            }
            None => {
                debug!(
                    "Directly using distribution mode: {:?}",
                    self.distribution_mode
                );
                self.distribution_mode
            }
        };

        let builder: Box<dyn AnonymousMonitorBuilder<LOLASpecification, Value>> =
            Self::create_common_builder(
                self.runtime,
                self.semantics,
                self.parser,
                self.executor,
                self.model,
                self.mqtt_reconfig_provider,
                distribution_mode,
                self.scheduler_mode,
                self.input_provider_builder.clone(),
                self.output_handler_builder.clone(),
            );

        // Construct inputs and outputs:
        // Skip this for ReconfigurableAsync runtime since we handle builders directly in the match above
        let builder = if self.runtime == Runtime::ReconfigurableAsync {
            builder
        } else {
            // Normal handling for non-reconfigurable runtimes
            let builder = if let Some(input_provider_builder) = self.input_provider_builder {
                let input = input_provider_builder.async_build().await;
                builder.input(input)
            } else if let Some(input) = self.input {
                builder.input(input)
            } else {
                builder
            };

            if let Some(output_handler_builder) = self.output_handler_builder {
                let output = output_handler_builder.async_build().await;
                builder.output(output)
            } else if let Some(output) = self.output {
                builder.output(output)
            } else {
                builder
            }
        };

        builder.async_build().await
    }

    pub fn partial_clone(self) -> Self {
        Self {
            executor: self.executor.clone(),
            model: self.model.clone(),
            input: None, // Not clonable. TODO: We should make all builders clonable
            input_provider_builder: self.input_provider_builder.clone(),
            output: None, // Not clonable. TODO: We should make all builders clonable
            output_handler_builder: self.output_handler_builder.clone(),
            distribution_mode: DistributionMode::CentralMonitor, // Not clonable. TODO: We should make all builders clonable
            distribution_mode_builder: self.distribution_mode_builder.clone(),
            runtime: self.runtime.clone(),
            semantics: self.semantics.clone(),
            mqtt_reconfig_provider: None,
            scheduler_mode: self.scheduler_mode.clone(),
            parser: self.parser.clone(),
        }
    }
}
