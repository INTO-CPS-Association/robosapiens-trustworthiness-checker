use std::fmt::Debug;
use std::rc::Rc;

use futures::future::LocalBoxFuture;
use smol::LocalExecutor;
use tracing::{debug, warn};

use crate::{
    DsrvSpecification, Monitor, SExpr, Value, VarName,
    cli::{adapters::DistributionModeBuilder, args::ParserMode},
    core::{AbstractMonitorBuilder, OutputHandler, Runnable, Runtime, Semantics, StreamData},
    define_config,
    distributed::distribution_graphs::LabelledDistributionGraph,
    io::{InputProviderBuilder, builders::OutputHandlerBuilder},
    lang::dsrv::{
        lalr_parser::LALRParser,
        parser::CombExprParser,
        type_checker::{SExprTE, TypedDsrvSpecification, type_check},
    },
    runtime::{
        reconfigurable_semi_sync::ReconfSemiSyncMonitorBuilder,
        semi_sync::{SemiSyncContext, SemiSyncMonitorBuilder},
    },
    semantics::{
        AsyncConfig, DistributedSemantics, TypedUntimedDsrvSemantics, UntimedDsrvSemantics,
        distributed::{contexts::DistributedContext, localisation::LocalitySpec},
    },
};

use super::{
    asynchronous::{AsyncMonitorBuilder, Context},
    distributed::{DistAsyncMonitorBuilder, SchedulerCommunication},
};

use static_assertions::assert_obj_safe;

// Various AsyncConfigs to use
#[rustfmt::skip]
define_config!(ValueConfig, Val = Value, Expr = SExpr, Ctx = Context, Spec = DsrvSpecification);
#[rustfmt::skip]
define_config!(TypedValueConfig, Val = Value, Expr = SExprTE, Ctx = Context, Spec = TypedDsrvSpecification);
#[rustfmt::skip]
define_config!(DistValueConfig, Val = Value, Expr = SExpr, Ctx = DistributedContext, Spec = DsrvSpecification);
#[rustfmt::skip]
define_config!(SemiSyncValueConfig, Val = Value, Expr = SExpr, Ctx = SemiSyncContext, Spec = DsrvSpecification);

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
    Mon: Monitor<TypedDsrvSpecification, V> + 'static,
    MonBuilder: AbstractMonitorBuilder<TypedDsrvSpecification, V, Mon = Mon> + 'static,
> AbstractMonitorBuilder<DsrvSpecification, V> for TypeCheckingBuilder<MonBuilder>
{
    type Mon = Mon;

    fn new() -> Self {
        Self(MonBuilder::new())
    }

    fn executor(self, ex: Rc<LocalExecutor<'static>>) -> Self {
        Self(self.0.executor(ex))
    }

    fn model(self, model: DsrvSpecification) -> Self {
        let model = type_check(model).expect("Model failed to type check");
        Self(self.0.model(model))
    }

    fn input(self, input: Box<dyn crate::InputProvider<Val = V>>) -> Self {
        Self(self.0.input(input))
    }

    fn output(self, output: Box<dyn OutputHandler<Val = V>>) -> Self {
        Self(self.0.output(output))
    }

    fn build(self) -> Self::Mon {
        self.0.build()
    }

    fn async_build(self: Box<Self>) -> LocalBoxFuture<'static, Self::Mon> {
        Box::pin(async move { (*self).build() })
    }
}

pub enum DistributionMode {
    CentralMonitor,
    LocalMonitor(Box<dyn LocalitySpec>), // Local topics
    // Receiver for reconfiguration but no current local monitor; this is for dynamic
    // reconfiguration each timestep
    // TODO: reintroduce this as a shorthand for using the ReconfigurableSemiSyncRuntime
    // ReconfigurableLocalMonitor(crate::io::mqtt::MQTTLocalityReceiver),
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
    DistributedRosCentralised(
        /// Location names
        Vec<String>,
        /// Topic used by ROS distribution graph provider
        String,
    ),
    DistributedRosRandom(
        /// Location names
        Vec<String>,
        /// Topic used by ROS distribution graph provider
        String,
    ),
    DistributedRosOptimizedStatic(
        /// Location names
        Vec<String>,
        /// Variables which represent the constraints which determine the static distribution
        Vec<VarName>,
        /// Topic used by ROS distribution graph provider
        String,
    ),
    DistributedRosOptimizedDynamic(
        /// Location names
        Vec<String>,
        /// Variables which represent the constraints which determine the static distribution
        Vec<VarName>,
        /// Topic used by ROS distribution graph provider
        String,
    ),
    DistributedPredefinedStatic(
        /// Predefined labelled distribution graph with static assignments
        LabelledDistributionGraph,
    ),
    DistributedPredefinedOptimized(
        /// Predefined labelled distribution graph used for topology
        LabelledDistributionGraph,
        /// Variables which represent the constraints which determine dynamic assignments
        Vec<VarName>,
    ),
}

impl Debug for DistributionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DistributionMode::CentralMonitor => write!(f, "CentralMonitor"),
            DistributionMode::LocalMonitor(_) => write!(f, "LocalMonitor"),
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
            DistributionMode::DistributedRosCentralised(locations, topic) => {
                write!(f, "DistributedRosCentralised({:?}, {:?})", locations, topic)
            }
            DistributionMode::DistributedRosRandom(locations, topic) => {
                write!(f, "DistributedRosRandom({:?}, {:?})", locations, topic)
            }
            DistributionMode::DistributedRosOptimizedStatic(locations, dist_constraints, topic) => {
                write!(
                    f,
                    "DistributedRosOptimizedStatic({:?}, {:?}, {:?})",
                    locations, dist_constraints, topic
                )
            }
            DistributionMode::DistributedRosOptimizedDynamic(
                locations,
                dist_constraints,
                topic,
            ) => {
                write!(
                    f,
                    "DistributedRosOptimizedDynamic({:?}, {:?}, {:?})",
                    locations, dist_constraints, topic
                )
            }
            DistributionMode::DistributedPredefinedStatic(graph) => {
                write!(f, "DistributedPredefinedStatic({:?})", graph)
            }
            DistributionMode::DistributedPredefinedOptimized(graph, dist_constraints) => {
                write!(
                    f,
                    "DistributedPredefinedOptimized({:?}, {:?})",
                    graph, dist_constraints
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
    pub scheduler_mode: SchedulerCommunication,
    pub parser: ParserMode,
    pub reconf_topic: String,
}

impl<M, V: StreamData> GenericMonitorBuilder<M, V> {
    pub fn runtime(self, runtime: Runtime) -> Self {
        Self { runtime, ..self }
    }

    pub fn semantics(self, semantics: Semantics) -> Self {
        Self { semantics, ..self }
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

    pub fn reconf_topic(self, reconf_topic: String) -> Self {
        Self {
            reconf_topic,
            ..self
        }
    }
}

impl AbstractMonitorBuilder<DsrvSpecification, Value>
    for GenericMonitorBuilder<DsrvSpecification, Value>
{
    type Mon = Box<dyn Runnable>;

    // TODO: Refactor. This needs to either reuse defaults used within the CLI parser, or not allow
    // constructing without args.
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
            scheduler_mode: SchedulerCommunication::Null,
            parser: ParserMode::Lalr,
            reconf_topic: "reconf".to_string(),
        }
    }

    fn executor(self, ex: Rc<LocalExecutor<'static>>) -> Self {
        Self {
            executor: Some(ex),
            ..self
        }
    }

    fn model(self, model: DsrvSpecification) -> Self {
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

    fn build(self) -> Self::Mon {
        if self.distribution_mode_builder.is_some()
            || self.input_provider_builder.is_some()
            || self.output_handler_builder.is_some()
        {
            panic!("Call async_build instead");
        }

        let builder: Box<dyn AnonymousMonitorBuilder<DsrvSpecification, Value>> =
            Self::create_common_builder(
                self.runtime,
                self.semantics,
                self.parser,
                self.executor,
                self.model,
                self.distribution_mode,
                self.scheduler_mode,
                self.input_provider_builder.clone(),
                self.output_handler_builder.clone(),
                self.reconf_topic.clone(),
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

impl GenericMonitorBuilder<DsrvSpecification, Value> {
    // Creates the common parts of the builder
    fn create_common_builder(
        runtime: Runtime,
        semantics: Semantics,
        parser: ParserMode,
        executor: Option<Rc<LocalExecutor<'static>>>,
        model: Option<DsrvSpecification>,
        distribution_mode: DistributionMode,
        scheduler_mode: SchedulerCommunication,
        input_provider_builder: Option<InputProviderBuilder>,
        output_handler_builder: Option<OutputHandlerBuilder>,
        reconf_topic: String,
    ) -> Box<dyn AnonymousMonitorBuilder<DsrvSpecification, Value>> {
        debug!(
            "Creating common builder with distribution mode: {:?}",
            distribution_mode
        );
        let builder: Box<dyn AnonymousMonitorBuilder<DsrvSpecification, Value>> = match (
            runtime, semantics, parser,
        ) {
            (Runtime::Async, Semantics::Untimed, ParserMode::Lalr) => {
                Box::new(AsyncMonitorBuilder::<
                    ValueConfig,
                    UntimedDsrvSemantics<LALRParser>,
                >::new())
            }
            (Runtime::Async, Semantics::Untimed, ParserMode::Combinator) => {
                Box::new(AsyncMonitorBuilder::<
                    ValueConfig,
                    UntimedDsrvSemantics<CombExprParser>,
                >::new())
            }
            (Runtime::SemiSync, Semantics::Untimed, ParserMode::Lalr) => {
                Box::new(SemiSyncMonitorBuilder::<
                    SemiSyncValueConfig,
                    UntimedDsrvSemantics<LALRParser>,
                >::new())
            }
            (Runtime::ReconfSemiSync, Semantics::Untimed, ParserMode::Lalr) => {
                let mut builder = ReconfSemiSyncMonitorBuilder::<
                    SemiSyncValueConfig,
                    UntimedDsrvSemantics<LALRParser>,
                    LALRParser,
                >::new();
                builder = builder.reconf_topic(reconf_topic);
                builder =
                    builder.input_builder(input_provider_builder.expect(
                        "Input provider builder required for ReconfigurableSemiSync runtime",
                    ));
                builder =
                    builder.output_builder(output_handler_builder.expect(
                        "Output handler builder required for ReconfigurableSemiSync runtime",
                    ));
                Box::new(builder)
            }
            (Runtime::Async, Semantics::TypedUntimed, ParserMode::Lalr) => {
                Box::new(TypeCheckingBuilder(AsyncMonitorBuilder::<
                    TypedValueConfig,
                    TypedUntimedDsrvSemantics<LALRParser>,
                >::new()))
            }
            (Runtime::Async, Semantics::TypedUntimed, ParserMode::Combinator) => {
                Box::new(TypeCheckingBuilder(AsyncMonitorBuilder::<
                    TypedValueConfig,
                    TypedUntimedDsrvSemantics<CombExprParser>,
                >::new()))
            }
            (Runtime::Distributed, Semantics::Untimed, _) => {
                debug!(
                    "Setting up distributed runtime with distribution_mode = {:?}",
                    distribution_mode
                );
                if matches!(parser, ParserMode::Lalr) {
                    warn!(
                        "LALR parser not supported for DUPs with Distributed Runtime. Defaulting to Combinator parser."
                    );
                }

                let builder = DistAsyncMonitorBuilder::<
                    DistValueConfig,
                    DistributedSemantics<CombExprParser>,
                >::new();

                let builder = builder.scheduler_mode(scheduler_mode);
                let builder = match distribution_mode {
                    DistributionMode::CentralMonitor => builder,
                    DistributionMode::LocalMonitor(_) => {
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
                    DistributionMode::DistributedRosCentralised(locations, topic) => {
                        let locations = locations
                            .into_iter()
                            .map(|loc| (loc.clone().into(), loc))
                            .collect();
                        builder.ros_centralised_dist_graph(locations, topic)
                    }
                    DistributionMode::DistributedRosRandom(locations, topic) => {
                        let locations = locations
                            .into_iter()
                            .map(|loc| (loc.clone().into(), loc))
                            .collect();
                        builder.ros_random_dist_graph(locations, topic)
                    }
                    DistributionMode::DistributedRosOptimizedStatic(
                        locations,
                        dist_constraints,
                        topic,
                    ) => {
                        let locations = locations
                            .into_iter()
                            .map(|loc| (loc.clone().into(), loc))
                            .collect();
                        builder.ros_optimized_static_dist_graph(locations, dist_constraints, topic)
                    }
                    DistributionMode::DistributedRosOptimizedDynamic(
                        locations,
                        dist_constraints,
                        topic,
                    ) => {
                        let locations = locations
                            .into_iter()
                            .map(|loc| (loc.clone().into(), loc))
                            .collect();
                        builder.ros_optimized_dynamic_dist_graph(locations, dist_constraints, topic)
                    }
                    DistributionMode::DistributedPredefinedStatic(graph) => {
                        builder.static_dist_graph(graph)
                    }
                    DistributionMode::DistributedPredefinedOptimized(graph, dist_constraints) => {
                        builder.predefined_optimized_dist_graph(graph, dist_constraints)
                    }
                };

                Box::new(builder)
            }
            (runtime, semantics, parser) => {
                panic!(
                    "Unsupported runtime: {:?}, semantics: {:?} and parser: {:?} combination",
                    runtime, semantics, parser
                );
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

        let builder: Box<dyn AnonymousMonitorBuilder<DsrvSpecification, Value>> =
            Self::create_common_builder(
                self.runtime,
                self.semantics,
                self.parser,
                self.executor,
                self.model,
                distribution_mode,
                self.scheduler_mode,
                self.input_provider_builder.clone(),
                self.output_handler_builder.clone(),
                self.reconf_topic.clone(),
            );

        // Construct inputs and outputs:
        // Skip this for ReconfigurableSemiSync runtime since we handle builders directly in the match above
        let builder = if self.runtime == Runtime::ReconfSemiSync {
            builder
        } else {
            // Normal handling for non-reconfigurable runtimes
            let builder = if let Some(input_provider_builder) = self.input_provider_builder {
                let input = input_provider_builder
                    .runtime(self.runtime)
                    .async_build()
                    .await;
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
}
