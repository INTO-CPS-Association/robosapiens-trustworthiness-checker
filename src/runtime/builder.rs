use std::fmt::Debug;
use std::rc::Rc;

use smol::LocalExecutor;
use tracing::debug;

use crate::{
    LOLASpecification, Monitor, Value, VarName,
    cli::adapters::DistributionModeBuilder,
    core::{AbstractMonitorBuilder, OutputHandler, Runnable, Runtime, Semantics, StreamData},
    dep_manage::interface::DependencyManager,
    io::{InputProviderBuilder, builders::OutputHandlerBuilder},
    lang::dynamic_lola::type_checker::{TypedLOLASpecification, type_check},
    runtime::reconfigurable_async::ReconfAsyncMonitorBuilder,
    semantics::distributed::{contexts::DistributedContext, localisation::LocalitySpec},
};

use super::{
    asynchronous::{AsyncMonitorBuilder, Context},
    constraints::runtime::ConstraintBasedMonitorBuilder,
    distributed::{DistAsyncMonitorBuilder, SchedulerCommunication},
};

use static_assertions::assert_obj_safe;

trait AnonymousMonitorBuilder<M, V: StreamData>: 'static {
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

    fn output(
        self: Box<Self>,
        output: Box<dyn OutputHandler<Val = V>>,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>>;

    fn dependencies(
        self: Box<Self>,
        dependencies: DependencyManager,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>>;

    fn build(self: Box<Self>) -> Box<dyn Runnable>;
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

    fn output(
        self: Box<Self>,
        output: Box<dyn OutputHandler<Val = V>>,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>> {
        Box::new(MonBuilder::output(*self, output))
    }

    fn dependencies(
        self: Box<Self>,
        dependencies: DependencyManager,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>> {
        Box::new(MonBuilder::dependencies(*self, dependencies))
    }

    fn build(self: Box<Self>) -> Box<dyn Runnable> {
        Box::new(MonBuilder::build(*self))
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

    fn dependencies(self, dependencies: DependencyManager) -> Self {
        Self(self.0.dependencies(dependencies))
    }

    fn build(self) -> Self::Mon {
        let builder = self.0.build();
        // Perform type checking here
        builder
    }
}

pub enum DistributionMode {
    CentralMonitor,
    LocalMonitor(Box<dyn LocalitySpec>), // Local topics
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
    pub dependencies: Option<DependencyManager>,
    pub runtime: Runtime,
    pub semantics: Semantics,
    pub distribution_mode: DistributionMode,
    pub distribution_mode_builder: Option<DistributionModeBuilder>,
    pub scheduler_mode: SchedulerCommunication,
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
            dependencies: None,
            distribution_mode: DistributionMode::CentralMonitor,
            distribution_mode_builder: None,
            runtime: Runtime::Async,
            semantics: Semantics::Untimed,
            scheduler_mode: SchedulerCommunication::Null,
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

    fn dependencies(self, dependencies: DependencyManager) -> Self {
        Self {
            dependencies: Some(dependencies),
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
            match (self.runtime, self.semantics) {
                (Runtime::Async, Semantics::Untimed) => Box::new(AsyncMonitorBuilder::<
                    LOLASpecification,
                    Context<Value>,
                    Value,
                    _,
                    crate::semantics::UntimedLolaSemantics,
                >::new()),
                (Runtime::Async, Semantics::TypedUntimed) => {
                    Box::new(TypeCheckingBuilder(AsyncMonitorBuilder::<
                        TypedLOLASpecification,
                        Context<Value>,
                        Value,
                        _,
                        crate::semantics::TypedUntimedLolaSemantics,
                    >::new()))
                }
                (Runtime::Constraints, Semantics::Untimed) => {
                    Box::new(ConstraintBasedMonitorBuilder::new())
                }
                (Runtime::Distributed, Semantics::Untimed) => {
                    debug!(
                        "Setting up distributed runtime with distribution_mode = {:?}",
                        self.distribution_mode
                    );
                    let builder = DistAsyncMonitorBuilder::<
                        LOLASpecification,
                        DistributedContext<Value>,
                        Value,
                        _,
                        crate::semantics::DistributedSemantics,
                    >::new();

                    let builder = builder.scheduler_mode(self.scheduler_mode);

                    let builder = match self.distribution_mode {
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
                        DistributionMode::DistributedOptimizedStatic(
                            locations,
                            dist_constraints,
                        ) => {
                            let locations = locations
                                .into_iter()
                                .map(|loc| (loc.clone().into(), loc))
                                .collect();
                            builder.mqtt_optimized_static_dist_graph(locations, dist_constraints)
                        }
                        DistributionMode::DistributedOptimizedDynamic(
                            locations,
                            dist_constraints,
                        ) => {
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

        let builder = match self.executor {
            Some(ex) => builder.executor(ex),
            None => builder,
        };
        let builder = match self.dependencies {
            Some(dependencies) => builder.dependencies(dependencies),
            None => builder,
        };
        let builder = match self.model {
            Some(model) => builder.model(model),
            None => builder,
        };
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
}

impl GenericMonitorBuilder<LOLASpecification, Value> {
    pub async fn async_build(self) -> Box<dyn Runnable> {
        let dist_mode = match self.distribution_mode_builder {
            // TODO: add error handling to this method
            Some(distribution_mode_builder) => Some(
                distribution_mode_builder
                    .build()
                    .await
                    .expect("Failed to build distribution mode"),
            ),
            None => None,
        };

        let builder: Box<dyn AnonymousMonitorBuilder<LOLASpecification, Value>> =
            match (self.runtime, self.semantics) {
                (Runtime::Async, Semantics::Untimed) => Box::new(AsyncMonitorBuilder::<
                    LOLASpecification,
                    Context<Value>,
                    Value,
                    _,
                    crate::semantics::UntimedLolaSemantics,
                >::new()),
                (Runtime::Async, Semantics::TypedUntimed) => {
                    Box::new(TypeCheckingBuilder(AsyncMonitorBuilder::<
                        TypedLOLASpecification,
                        Context<Value>,
                        Value,
                        _,
                        crate::semantics::TypedUntimedLolaSemantics,
                    >::new()))
                }
                (Runtime::Constraints, Semantics::Untimed) => {
                    Box::new(ConstraintBasedMonitorBuilder::new())
                }
                (Runtime::ReconfigurableAsync, Semantics::Untimed) => {
                    Box::new(ReconfAsyncMonitorBuilder::<
                        LOLASpecification,
                        DistributedContext<Value>,
                        Value,
                        _,
                        crate::semantics::DistributedSemantics,
                    >::new())
                }
                (Runtime::Distributed, Semantics::Untimed) => {
                    debug!(
                        "Setting up distributed runtime with distribution_mode = {:?}",
                        self.distribution_mode
                    );
                    let builder = DistAsyncMonitorBuilder::<
                        LOLASpecification,
                        DistributedContext<Value>,
                        Value,
                        _,
                        crate::semantics::DistributedSemantics,
                    >::new();

                    let builder = builder.scheduler_mode(self.scheduler_mode);

                    let builder = match dist_mode.unwrap_or(self.distribution_mode) {
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
                        DistributionMode::DistributedOptimizedStatic(
                            locations,
                            dist_constraints,
                        ) => {
                            let locations = locations
                                .into_iter()
                                .map(|loc| (loc.clone().into(), loc))
                                .collect();
                            builder.mqtt_optimized_static_dist_graph(locations, dist_constraints)
                        }
                        DistributionMode::DistributedOptimizedDynamic(
                            locations,
                            dist_constraints,
                        ) => {
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

        let builder = match self.executor {
            Some(ex) => builder.executor(ex),
            None => builder,
        };
        let builder = match self.dependencies {
            Some(dependencies) => builder.dependencies(dependencies),
            None => builder,
        };
        let builder = match self.model {
            Some(model) => builder.model(model),
            None => builder,
        };

        // Construct inputs:
        let builder = if let Some(input_provider_builder) = self.input_provider_builder {
            let input = input_provider_builder.async_build().await;
            builder.input(input)
        } else if let Some(input) = self.input {
            builder.input(input)
        } else {
            builder
        };

        // Construct outputs:
        let builder = if let Some(output_handler_builder) = self.output_handler_builder {
            let output = output_handler_builder.async_build().await;
            builder.output(output)
        } else if let Some(output) = self.output {
            builder.output(output)
        } else {
            builder
        };

        builder.build()
    }

    pub fn partial_clone(self) -> Self {
        Self {
            executor: self.executor.clone(),
            model: self.model.clone(),
            input: None, // Not clonable. TODO: We should make all builders clonable
            input_provider_builder: self.input_provider_builder.clone(),
            output: None, // Not clonable. TODO: We should make all builders clonable
            output_handler_builder: self.output_handler_builder.clone(),
            dependencies: self.dependencies.clone(),
            distribution_mode: DistributionMode::CentralMonitor, // Not clonable. TODO: We should make all builders clonable
            distribution_mode_builder: self.distribution_mode_builder.clone(),
            runtime: self.runtime.clone(),
            semantics: self.semantics.clone(),
            scheduler_mode: self.scheduler_mode.clone(),
        }
    }
}
