use std::rc::Rc;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Debug, Display},
};

use futures::future::LocalBoxFuture;
use mstlo::{Algorithm, SynchronizationStrategy, Variables};
use smol::LocalExecutor;
use tracing::{debug, warn};

use crate::ExecutionPolicy;
use crate::io::{MsgTypeMapping, TopicMapping};
use crate::{
    DsrvSpecification, InputStream, Runtime, Specification, Value, VarName,
    cli::{
        adapters::DistributionModeBuilder,
        args::{MstloAlgorithm, MstloSynchronizationStrategy, ParserMode},
    },
    core::{OutputHandler, RuntimeSpec, Semantics, StreamData, StreamType},
    define_config,
    distributed::distribution_graphs::LabelledDistributionGraph,
    io::{InputStreamFactory, OutputHandlerBuilder},
    lang::core::parser::SpecParser,
    lang::dsrv::{
        ast::{CheckedDsrvSpecification, CheckedExpr, Expr},
        lalr_parser::LALRParser,
        parser::CombExprParser,
        type_checker::{type_check, type_check_gradual},
    },
    lang::mstlo::MstloSpecification,
    runtime::{
        dataflow::DataflowRuntimeBuilder,
        mstlo::MstloRuntimeBuilder,
        reconfigurable_semi_sync::ReconfSemiSyncRuntimeBuilder,
        semi_sync::{SemiSyncContext, SemiSyncRuntimeBuilder},
    },
    semantics::{
        AsyncConfig, CheckedUntimedDsrvSemantics, DistributedSemantics, UntimedDsrvSemantics,
        distributed::{contexts::DistributedContext, localisation::LocalitySpec},
    },
};

use super::{
    asynchronous::{AsyncRuntimeBuilder, Context},
    distributed::{DistAsyncRuntimeBuilder, SchedulerCommunication},
};

use static_assertions::assert_obj_safe;

// Various AsyncConfigs to use
#[rustfmt::skip]
define_config!(ValueConfig, Val = Value, Expr = Expr, Ctx = Context, Spec = DsrvSpecification);
#[rustfmt::skip]
define_config!(CheckedValueConfig, Val = Value, Expr = CheckedExpr, Ctx = Context, Spec = CheckedDsrvSpecification);
#[rustfmt::skip]
define_config!(DistValueConfig, Val = Value, Expr = Expr, Ctx = DistributedContext, Spec = DsrvSpecification);
#[rustfmt::skip]
define_config!(SemiSyncValueConfig, Val = Value, Expr = Expr, Ctx = SemiSyncContext, Spec = DsrvSpecification);
#[rustfmt::skip]
define_config!(CheckedSemiSyncValueConfig, Val = Value, Expr = CheckedExpr, Ctx = SemiSyncContext, Spec = CheckedDsrvSpecification);

#[derive(Clone, Debug)]
pub enum LangSpecification {
    Dsrv(DsrvSpecification),
    Mstlo(MstloSpecification),
}

impl From<DsrvSpecification> for LangSpecification {
    fn from(spec: DsrvSpecification) -> Self {
        Self::Dsrv(spec)
    }
}

impl From<MstloSpecification> for LangSpecification {
    fn from(formula: MstloSpecification) -> Self {
        Self::Mstlo(formula)
    }
}

impl Display for LangSpecification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LangSpecification::Dsrv(spec) => Display::fmt(spec, f),
            LangSpecification::Mstlo(spec) => Display::fmt(spec, f),
        }
    }
}

impl Specification for LangSpecification {
    type Expr = ();

    fn input_vars(&self) -> BTreeSet<VarName> {
        match self {
            LangSpecification::Dsrv(spec) => spec.input_vars().clone(),
            LangSpecification::Mstlo(spec) => spec.input_vars(),
        }
    }

    fn output_vars(&self) -> BTreeSet<VarName> {
        match self {
            LangSpecification::Dsrv(spec) => spec.output_vars().clone(),
            LangSpecification::Mstlo(spec) => spec.output_vars(),
        }
    }

    fn aux_vars(&self) -> BTreeSet<VarName> {
        match self {
            LangSpecification::Dsrv(spec) => spec.aux_vars().clone(),
            LangSpecification::Mstlo(formula) => formula.aux_vars(),
        }
    }

    fn var_expr(&self, _var: &VarName) -> Option<Self::Expr> {
        None
    }

    fn type_annotations(&self) -> BTreeMap<VarName, StreamType> {
        match self {
            LangSpecification::Dsrv(spec) => spec.type_annotations().clone(),
            LangSpecification::Mstlo(spec) => spec.type_annotations(),
        }
    }
}

/* A trait for builders, which construct a particular runtime
 *
 */
pub trait RuntimeBuilder<M, V: StreamData> {
    type Runtime: Runtime;

    fn new() -> Self;

    fn executor(self, ex: Rc<LocalExecutor<'static>>) -> Self;

    fn maybe_executor(self, ex: Option<Rc<LocalExecutor<'static>>>) -> Self
    where
        Self: Sized,
    {
        if let Some(ex) = ex {
            self.executor(ex)
        } else {
            self
        }
    }

    fn model(self, model: M) -> Self;

    fn maybe_model(self, model: Option<M>) -> Self
    where
        Self: Sized,
    {
        if let Some(model) = model {
            self.model(model)
        } else {
            self
        }
    }

    fn input(self, input: InputStream<V>) -> Self;

    fn output(self, output: Box<dyn OutputHandler<Val = V>>) -> Self;

    fn maybe_output(self, output: Option<Box<dyn OutputHandler<Val = V>>>) -> Self
    where
        Self: Sized,
    {
        if let Some(output) = output {
            self.output(output)
        } else {
            self
        }
    }

    fn build(self) -> LocalBoxFuture<'static, Self::Runtime>;
}

/* Builders which construct a given runtime in an object-safe manner.
 *
 * Due to object safety, the return types do not reveal what type of runtime is being built.
 * Builders should not implement this directly, but should instead implement the non-object--safe
 * trait RuntimeBuilder.
 */
pub trait RuntimeBuilderDyn<M, V: StreamData>: 'static {
    fn executor(
        self: Box<Self>,
        ex: Rc<LocalExecutor<'static>>,
    ) -> Box<dyn RuntimeBuilderDyn<M, V>>;

    fn maybe_executor(
        self: Box<Self>,
        ex: Option<Rc<LocalExecutor<'static>>>,
    ) -> Box<dyn RuntimeBuilderDyn<M, V>>;

    fn model(self: Box<Self>, model: M) -> Box<dyn RuntimeBuilderDyn<M, V>>;

    fn maybe_model(self: Box<Self>, model: Option<M>) -> Box<dyn RuntimeBuilderDyn<M, V>>;

    fn input(self: Box<Self>, input: crate::InputStream<V>) -> Box<dyn RuntimeBuilderDyn<M, V>>;

    fn output(
        self: Box<Self>,
        output: Box<dyn OutputHandler<Val = V>>,
    ) -> Box<dyn RuntimeBuilderDyn<M, V>>;

    fn maybe_output(
        self: Box<Self>,
        output: Option<Box<dyn OutputHandler<Val = V>>>,
    ) -> Box<dyn RuntimeBuilderDyn<M, V>>;

    fn build(self: Box<Self>) -> LocalBoxFuture<'static, Box<dyn Runtime>>;
}

assert_obj_safe!(RuntimeBuilderDyn<(), ()>);

impl<
    M,
    V: StreamData,
    Mon: Runtime + 'static,
    MonBuilder: RuntimeBuilder<M, V, Runtime = Mon> + 'static,
> RuntimeBuilderDyn<M, V> for MonBuilder
{
    fn executor(
        self: Box<Self>,
        ex: Rc<LocalExecutor<'static>>,
    ) -> Box<dyn RuntimeBuilderDyn<M, V>> {
        Box::new(MonBuilder::executor(*self, ex))
    }

    fn maybe_executor(
        self: Box<Self>,
        ex: Option<Rc<LocalExecutor<'static>>>,
    ) -> Box<dyn RuntimeBuilderDyn<M, V>> {
        Box::new(MonBuilder::maybe_executor(*self, ex))
    }

    fn model(self: Box<Self>, model: M) -> Box<dyn RuntimeBuilderDyn<M, V>> {
        Box::new(MonBuilder::model(*self, model))
    }

    fn maybe_model(self: Box<Self>, model: Option<M>) -> Box<dyn RuntimeBuilderDyn<M, V>> {
        Box::new(MonBuilder::maybe_model(*self, model))
    }

    fn input(self: Box<Self>, input: crate::InputStream<V>) -> Box<dyn RuntimeBuilderDyn<M, V>> {
        Box::new(MonBuilder::input(*self, input))
    }

    fn output(
        self: Box<Self>,
        output: Box<dyn OutputHandler<Val = V>>,
    ) -> Box<dyn RuntimeBuilderDyn<M, V>> {
        Box::new(MonBuilder::output(*self, output))
    }

    fn maybe_output(
        self: Box<Self>,
        output: Option<Box<dyn OutputHandler<Val = V>>>,
    ) -> Box<dyn RuntimeBuilderDyn<M, V>> {
        Box::new(MonBuilder::maybe_output(*self, output))
    }

    fn build(self: Box<Self>) -> LocalBoxFuture<'static, Box<dyn Runtime>> {
        Box::pin(async move {
            let mon = <MonBuilder as RuntimeBuilder<M, V>>::build(*self).await;
            Box::new(mon) as Box<dyn Runtime>
        })
    }
}

struct TypeCheckingBuilder<Builder>(Builder);
struct GradualTypeCheckingBuilder<Builder>(Builder);

#[derive(Clone)]
struct TypeCheckingSpecParser<P>(std::marker::PhantomData<P>);

#[derive(Clone)]
struct GradualTypeCheckingSpecParser<P>(std::marker::PhantomData<P>);

impl<P> SpecParser<CheckedDsrvSpecification> for TypeCheckingSpecParser<P>
where
    P: SpecParser<DsrvSpecification>,
{
    fn parse(input: &mut &str) -> anyhow::Result<CheckedDsrvSpecification> {
        let spec = P::parse(input)?;
        type_check(spec).map_err(|errors| {
            anyhow::anyhow!("Reconfigured spec failed type checking: {:?}", errors)
        })
    }
}

impl<P> SpecParser<CheckedDsrvSpecification> for GradualTypeCheckingSpecParser<P>
where
    P: SpecParser<DsrvSpecification>,
{
    fn parse(input: &mut &str) -> anyhow::Result<CheckedDsrvSpecification> {
        let spec = P::parse(input)?;
        type_check_gradual(spec).map_err(|errors| {
            anyhow::anyhow!(
                "Reconfigured spec failed gradual type checking: {:?}",
                errors
            )
        })
    }
}

impl<
    V: StreamData,
    Mon: Runtime + 'static,
    MonBuilder: RuntimeBuilder<CheckedDsrvSpecification, V, Runtime = Mon> + 'static,
> RuntimeBuilder<DsrvSpecification, V> for TypeCheckingBuilder<MonBuilder>
{
    type Runtime = Mon;

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

    fn input(self, input: crate::InputStream<V>) -> Self {
        Self(self.0.input(input))
    }

    fn output(self, output: Box<dyn OutputHandler<Val = V>>) -> Self {
        Self(self.0.output(output))
    }

    fn build(self) -> LocalBoxFuture<'static, Self::Runtime> {
        Box::pin(async move { self.0.build().await })
    }
}

impl<
    V: StreamData,
    Mon: Runtime + 'static,
    MonBuilder: RuntimeBuilder<CheckedDsrvSpecification, V, Runtime = Mon> + 'static,
> RuntimeBuilder<DsrvSpecification, V> for GradualTypeCheckingBuilder<MonBuilder>
{
    type Runtime = Mon;

    fn new() -> Self {
        Self(MonBuilder::new())
    }

    fn executor(self, ex: Rc<LocalExecutor<'static>>) -> Self {
        Self(self.0.executor(ex))
    }

    fn model(self, model: DsrvSpecification) -> Self {
        let model = type_check_gradual(model).expect("Model failed to gradual type check");
        Self(self.0.model(model))
    }

    fn input(self, input: crate::InputStream<V>) -> Self {
        Self(self.0.input(input))
    }

    fn output(self, output: Box<dyn OutputHandler<Val = V>>) -> Self {
        Self(self.0.output(output))
    }

    fn build(self) -> LocalBoxFuture<'static, Self::Runtime> {
        Box::pin(async move { self.0.build().await })
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
    DistributedOptimizedStaticSat(
        /// Location names
        Vec<String>,
        /// Variables which represent the constraints which determine the static distribution
        Vec<VarName>,
    ),
    DistributedOptimizedDynamicSat(
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
    DistributedRosOptimizedStaticSat(
        /// Location names
        Vec<String>,
        /// Variables which represent the constraints which determine the static distribution
        Vec<VarName>,
        /// Topic used by ROS distribution graph provider
        String,
    ),
    DistributedRosOptimizedDynamicSat(
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
    DistributedPredefinedOptimizedSat(
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
            DistributionMode::DistributedOptimizedStaticSat(locations, dist_constraints) => {
                write!(
                    f,
                    "DistributedOptimizedStaticSat({:?}, {:?})",
                    locations, dist_constraints
                )
            }
            DistributionMode::DistributedOptimizedDynamicSat(locations, dist_constraints) => {
                write!(
                    f,
                    "DistributedOptimizedDynamicSat({:?}, {:?})",
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
            DistributionMode::DistributedRosOptimizedStaticSat(
                locations,
                dist_constraints,
                topic,
            ) => {
                write!(
                    f,
                    "DistributedRosOptimizedStaticSat({:?}, {:?}, {:?})",
                    locations, dist_constraints, topic
                )
            }
            DistributionMode::DistributedRosOptimizedDynamicSat(
                locations,
                dist_constraints,
                topic,
            ) => {
                write!(
                    f,
                    "DistributedRosOptimizedDynamicSat({:?}, {:?}, {:?})",
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
            DistributionMode::DistributedPredefinedOptimizedSat(graph, dist_constraints) => {
                write!(
                    f,
                    "DistributedPredefinedOptimizedSat({:?}, {:?})",
                    graph, dist_constraints
                )
            }
        }
    }
}

pub struct GeneralRuntimeBuilder<M, V: StreamData> {
    pub executor: Option<Rc<LocalExecutor<'static>>>,
    pub model: Option<M>,
    input: Option<InputStream<V>>,
    input_factory: Option<InputStreamFactory>,
    pub output: Option<Box<dyn OutputHandler<Val = V>>>,
    pub output_handler_builder: Option<OutputHandlerBuilder>,
    pub runtime: RuntimeSpec,
    pub semantics: Semantics,
    pub distribution_mode: DistributionMode,
    pub distribution_mode_builder: Option<DistributionModeBuilder>,
    pub scheduler_mode: SchedulerCommunication,
    pub parser: ParserMode,
    pub reconf_topic: String,
    pub use_context_transfer: bool,
    pub var_msg_types: Option<BTreeMap<VarName, String>>,
    pub topic_mapping: Option<TopicMapping>,
    pub mstlo_algorithm: Algorithm,
    pub mstlo_synchronization_strategy: SynchronizationStrategy,
    pub mstlo_variables: Variables,
}

impl<M, V: StreamData> GeneralRuntimeBuilder<M, V> {
    pub fn runtime(self, runtime: RuntimeSpec) -> Self {
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

    pub fn var_msg_types(self, var_msg_types: BTreeMap<VarName, String>) -> Self {
        Self {
            var_msg_types: Some(var_msg_types),
            ..self
        }
    }

    pub fn maybe_var_msg_types(self, var_msg_types: Option<BTreeMap<VarName, String>>) -> Self {
        match var_msg_types {
            Some(var_msg_types) => self.var_msg_types(var_msg_types),
            None => self,
        }
    }

    pub fn topic_mapping(self, topic_mapping: TopicMapping) -> Self {
        Self {
            topic_mapping: Some(topic_mapping),
            ..self
        }
    }

    pub fn maybe_topic_mapping(self, topic_mapping: Option<TopicMapping>) -> Self {
        match topic_mapping {
            Some(topic_mapping) => self.topic_mapping(topic_mapping),
            None => self,
        }
    }

    pub fn reconf_topic(self, reconf_topic: String) -> Self {
        Self {
            reconf_topic,
            ..self
        }
    }

    pub fn use_context_transfer(self, use_context_transfer: bool) -> Self {
        Self {
            use_context_transfer,
            ..self
        }
    }

    pub fn mstlo_algorithm(self, algorithm: MstloAlgorithm) -> Self {
        Self {
            mstlo_algorithm: algorithm.into(),
            ..self
        }
    }

    pub fn mstlo_synchronization_strategy(
        self,
        synchronization_strategy: MstloSynchronizationStrategy,
    ) -> Self {
        Self {
            mstlo_synchronization_strategy: synchronization_strategy.into(),
            ..self
        }
    }

    pub fn mstlo_variables(self, variables: Variables) -> Self {
        Self {
            mstlo_variables: variables,
            ..self
        }
    }
}

impl GeneralRuntimeBuilder<DsrvSpecification, Value> {
    pub fn input_factory(self, builder: InputStreamFactory) -> anyhow::Result<Self> {
        builder.ensure_reconfigurable()?;
        Ok(Self {
            input_factory: Some(builder),
            ..self
        })
    }
}

impl GeneralRuntimeBuilder<LangSpecification, Value> {
    pub fn input_factory(self, builder: InputStreamFactory) -> anyhow::Result<Self> {
        builder.ensure_reconfigurable()?;
        Ok(Self {
            input_factory: Some(builder),
            ..self
        })
    }
}

impl From<MstloAlgorithm> for Algorithm {
    fn from(algorithm: MstloAlgorithm) -> Self {
        match algorithm {
            MstloAlgorithm::Naive => Algorithm::Naive,
            MstloAlgorithm::Incremental => Algorithm::Incremental,
        }
    }
}

impl From<MstloSynchronizationStrategy> for SynchronizationStrategy {
    fn from(strategy: MstloSynchronizationStrategy) -> Self {
        match strategy {
            MstloSynchronizationStrategy::None => SynchronizationStrategy::None,
            MstloSynchronizationStrategy::ZeroOrderHold => SynchronizationStrategy::ZeroOrderHold,
            MstloSynchronizationStrategy::Linear => SynchronizationStrategy::Linear,
        }
    }
}

impl RuntimeBuilder<LangSpecification, Value> for GeneralRuntimeBuilder<LangSpecification, Value> {
    type Runtime = Box<dyn Runtime>;

    fn new() -> Self {
        Self {
            executor: None,
            model: None,
            input: None,
            input_factory: None,
            output: None,
            output_handler_builder: None,
            distribution_mode: DistributionMode::CentralMonitor,
            distribution_mode_builder: None,
            runtime: RuntimeSpec::Async,
            semantics: Semantics::GradualTypedUntimed,
            var_msg_types: None,
            topic_mapping: None,
            scheduler_mode: SchedulerCommunication::Null,
            parser: ParserMode::Lalr,
            reconf_topic: "reconf".to_string(),
            use_context_transfer: true,
            mstlo_algorithm: Algorithm::default(),
            mstlo_synchronization_strategy: SynchronizationStrategy::default(),
            mstlo_variables: Variables::new(),
        }
    }

    fn executor(self, ex: Rc<LocalExecutor<'static>>) -> Self {
        Self {
            executor: Some(ex),
            ..self
        }
    }

    fn model(self, model: LangSpecification) -> Self {
        Self {
            model: Some(model),
            ..self
        }
    }

    fn input(self, input: crate::InputStream<Value>) -> Self {
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

    fn build(self) -> LocalBoxFuture<'static, Self::Runtime> {
        Box::pin(
            async move { GeneralRuntimeBuilder::<LangSpecification, Value>::build(self).await },
        )
    }
}

impl GeneralRuntimeBuilder<LangSpecification, Value> {
    pub async fn build(self) -> Box<dyn Runtime> {
        match self.model.expect("Model/spec must be set") {
            LangSpecification::Dsrv(spec) => {
                GeneralRuntimeBuilder::<DsrvSpecification, Value> {
                    executor: self.executor,
                    model: Some(spec),
                    input: self.input,
                    input_factory: self.input_factory,
                    output: self.output,
                    output_handler_builder: self.output_handler_builder,
                    runtime: self.runtime,
                    semantics: self.semantics,
                    distribution_mode: self.distribution_mode,
                    distribution_mode_builder: self.distribution_mode_builder,
                    scheduler_mode: self.scheduler_mode,
                    parser: self.parser,
                    reconf_topic: self.reconf_topic,
                    use_context_transfer: self.use_context_transfer,
                    var_msg_types: self.var_msg_types,
                    topic_mapping: self.topic_mapping,
                    mstlo_algorithm: self.mstlo_algorithm,
                    mstlo_synchronization_strategy: self.mstlo_synchronization_strategy,
                    mstlo_variables: self.mstlo_variables,
                }
                .build()
                .await
            }
            LangSpecification::Mstlo(spec) => {
                GeneralRuntimeBuilder::<MstloSpecification, Value> {
                    executor: self.executor,
                    model: Some(spec),
                    input: self.input,
                    input_factory: self.input_factory,
                    output: self.output,
                    output_handler_builder: self.output_handler_builder,
                    runtime: match self.runtime {
                        RuntimeSpec::Mstlo(policy) => RuntimeSpec::Mstlo(policy),
                        RuntimeSpec::Async => RuntimeSpec::Mstlo(ExecutionPolicy::Buffered),
                        runtime => panic!("MSTLO specification cannot use {runtime:?}"),
                    },
                    semantics: self.semantics,
                    distribution_mode: DistributionMode::CentralMonitor,
                    distribution_mode_builder: None,
                    scheduler_mode: self.scheduler_mode,
                    parser: self.parser,
                    reconf_topic: self.reconf_topic,
                    use_context_transfer: self.use_context_transfer,
                    var_msg_types: self.var_msg_types,
                    topic_mapping: self.topic_mapping,
                    mstlo_algorithm: self.mstlo_algorithm,
                    mstlo_synchronization_strategy: self.mstlo_synchronization_strategy,
                    mstlo_variables: self.mstlo_variables,
                }
                .build()
                .await
            }
        }
    }
}

impl RuntimeBuilder<DsrvSpecification, Value> for GeneralRuntimeBuilder<DsrvSpecification, Value> {
    type Runtime = Box<dyn Runtime>;

    // TODO: Refactor. This needs to either reuse defaults used within the CLI parser, or not allow
    // constructing without args.
    fn new() -> Self {
        Self {
            executor: None,
            model: None,
            input: None,
            input_factory: None,
            output: None,
            output_handler_builder: None,
            distribution_mode: DistributionMode::CentralMonitor,
            distribution_mode_builder: None,
            runtime: RuntimeSpec::Async,
            semantics: Semantics::GradualTypedUntimed,
            var_msg_types: None,
            topic_mapping: None,
            scheduler_mode: SchedulerCommunication::Null,
            parser: ParserMode::Lalr,
            reconf_topic: "reconf".to_string(),
            use_context_transfer: true,
            mstlo_algorithm: Algorithm::default(),
            mstlo_synchronization_strategy: SynchronizationStrategy::default(),
            mstlo_variables: Variables::new(),
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

    fn input(self, input: crate::InputStream<Value>) -> Self {
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

    fn build(self) -> LocalBoxFuture<'static, Self::Runtime> {
        Box::pin(
            async move { GeneralRuntimeBuilder::<DsrvSpecification, Value>::build(self).await },
        )
    }
}

impl RuntimeBuilder<MstloSpecification, Value>
    for GeneralRuntimeBuilder<MstloSpecification, Value>
{
    type Runtime = Box<dyn Runtime>;

    fn new() -> Self {
        Self {
            executor: None,
            model: None,
            input: None,
            input_factory: None,
            output: None,
            output_handler_builder: None,
            distribution_mode: DistributionMode::CentralMonitor,
            distribution_mode_builder: None,
            runtime: RuntimeSpec::Mstlo(ExecutionPolicy::Buffered),
            semantics: Semantics::GradualTypedUntimed,
            var_msg_types: None,
            topic_mapping: None,
            scheduler_mode: SchedulerCommunication::Null,
            parser: ParserMode::Lalr,
            reconf_topic: "reconf".to_string(),
            use_context_transfer: true,
            mstlo_algorithm: Algorithm::default(),
            mstlo_synchronization_strategy: SynchronizationStrategy::default(),
            mstlo_variables: Variables::new(),
        }
    }

    fn executor(self, ex: Rc<LocalExecutor<'static>>) -> Self {
        Self {
            executor: Some(ex),
            ..self
        }
    }

    fn model(self, model: MstloSpecification) -> Self {
        Self {
            model: Some(model),
            ..self
        }
    }

    fn input(self, input: crate::InputStream<Value>) -> Self {
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

    fn build(self) -> LocalBoxFuture<'static, Self::Runtime> {
        Box::pin(
            async move { GeneralRuntimeBuilder::<MstloSpecification, Value>::build(self).await },
        )
    }
}

impl GeneralRuntimeBuilder<MstloSpecification, Value> {
    fn mstlo_semantics(semantics: Semantics) -> mstlo::Semantics {
        match semantics {
            Semantics::DelayedQuantitative | Semantics::GradualTypedUntimed => {
                mstlo::Semantics::DelayedQuantitative
            }
            Semantics::DelayedQualitative => mstlo::Semantics::DelayedQualitative,
            Semantics::EagerQualitative => mstlo::Semantics::EagerQualitative,
            Semantics::RobustnessInterval => mstlo::Semantics::RobustnessInterval,
            Semantics::Untimed | Semantics::TypedUntimed => mstlo::Semantics::default(),
        }
    }

    pub async fn build(self) -> Box<dyn Runtime> {
        assert!(
            self.input_factory.is_none(),
            "InputStreamFactory is only supported by ReconfigurableSemiSync DSRV runtimes"
        );
        let RuntimeSpec::Mstlo(execution_policy) = self.runtime else {
            panic!("MSTLO builder requires RuntimeSpec::Mstlo")
        };
        let mut builder = MstloRuntimeBuilder::new()
            .maybe_executor(self.executor)
            .maybe_model(self.model)
            .execution_policy(execution_policy)
            .algorithm(self.mstlo_algorithm)
            .semantics(Self::mstlo_semantics(self.semantics))
            .synchronization_strategy(self.mstlo_synchronization_strategy)
            .variables(self.mstlo_variables);

        builder = match self.input {
            Some(input) => builder.input(input),
            None => builder,
        };

        builder = if let Some(output_handler_builder) = self.output_handler_builder {
            let output = output_handler_builder.build().await;
            builder.output(output)
        } else if let Some(output) = self.output {
            builder.output(output)
        } else {
            builder
        };

        builder.build().await
    }
}

impl GeneralRuntimeBuilder<DsrvSpecification, Value> {
    // Creates the common parts of the builder
    fn create_common_builder(
        runtime: RuntimeSpec,
        semantics: Semantics,
        parser: ParserMode,
        executor: Option<Rc<LocalExecutor<'static>>>,
        model: Option<DsrvSpecification>,
        distribution_mode: DistributionMode,
        scheduler_mode: SchedulerCommunication,
        input_factory: Option<InputStreamFactory>,
        output_handler_builder: Option<OutputHandlerBuilder>,
        reconf_topic: String,
        use_context_transfer: bool,
        topic_mapping: Option<TopicMapping>,
        var_msg_types: Option<MsgTypeMapping>,
    ) -> Box<dyn RuntimeBuilderDyn<DsrvSpecification, Value>> {
        debug!(
            "Creating common builder with distribution mode: {:?}",
            distribution_mode
        );
        let builder: Box<dyn RuntimeBuilderDyn<DsrvSpecification, Value>> = match (
            runtime, semantics, parser,
        ) {
            (RuntimeSpec::Async, Semantics::Untimed, ParserMode::Lalr) => {
                Box::new(AsyncRuntimeBuilder::<
                    ValueConfig,
                    UntimedDsrvSemantics<LALRParser>,
                >::new())
            }
            (RuntimeSpec::Async, Semantics::Untimed, ParserMode::Combinator) => {
                Box::new(AsyncRuntimeBuilder::<
                    ValueConfig,
                    UntimedDsrvSemantics<CombExprParser>,
                >::new())
            }
            (RuntimeSpec::Dataflow(policy), Semantics::Untimed, ParserMode::Lalr) => Box::new(
                DataflowRuntimeBuilder::<DsrvSpecification>::new().execution_policy(policy),
            ),
            (RuntimeSpec::Dataflow(policy), Semantics::TypedUntimed, ParserMode::Lalr) => {
                Box::new(TypeCheckingBuilder(
                    DataflowRuntimeBuilder::<CheckedDsrvSpecification>::new()
                        .execution_policy(policy),
                ))
            }
            (RuntimeSpec::Dataflow(policy), Semantics::GradualTypedUntimed, ParserMode::Lalr) => {
                Box::new(GradualTypeCheckingBuilder(
                    DataflowRuntimeBuilder::<CheckedDsrvSpecification>::new()
                        .execution_policy(policy),
                ))
            }
            (RuntimeSpec::SemiSync, Semantics::Untimed, ParserMode::Lalr) => {
                Box::new(SemiSyncRuntimeBuilder::<
                    SemiSyncValueConfig,
                    UntimedDsrvSemantics<LALRParser>,
                >::new())
            }
            (RuntimeSpec::SemiSync, Semantics::TypedUntimed, ParserMode::Lalr) => {
                Box::new(TypeCheckingBuilder(SemiSyncRuntimeBuilder::<
                    CheckedSemiSyncValueConfig,
                    CheckedUntimedDsrvSemantics<LALRParser>,
                >::new()))
            }
            (RuntimeSpec::SemiSync, Semantics::TypedUntimed, ParserMode::Combinator) => {
                Box::new(TypeCheckingBuilder(SemiSyncRuntimeBuilder::<
                    CheckedSemiSyncValueConfig,
                    CheckedUntimedDsrvSemantics<CombExprParser>,
                >::new()))
            }
            (RuntimeSpec::SemiSync, Semantics::GradualTypedUntimed, ParserMode::Lalr) => {
                Box::new(GradualTypeCheckingBuilder(SemiSyncRuntimeBuilder::<
                    CheckedSemiSyncValueConfig,
                    CheckedUntimedDsrvSemantics<LALRParser>,
                >::new()))
            }
            (RuntimeSpec::SemiSync, Semantics::GradualTypedUntimed, ParserMode::Combinator) => {
                Box::new(GradualTypeCheckingBuilder(SemiSyncRuntimeBuilder::<
                    CheckedSemiSyncValueConfig,
                    CheckedUntimedDsrvSemantics<CombExprParser>,
                >::new()))
            }
            (RuntimeSpec::ReconfSemiSync, Semantics::Untimed, ParserMode::Lalr) => {
                let mut builder = ReconfSemiSyncRuntimeBuilder::<
                    SemiSyncValueConfig,
                    UntimedDsrvSemantics<LALRParser>,
                    LALRParser,
                >::new();
                builder = builder.reconf_topic(reconf_topic);
                builder = builder.use_context_transfer(use_context_transfer);
                builder = builder
                    .input_factory(input_factory.expect(
                        "Input stream builder required for ReconfigurableSemiSync runtime",
                    ));
                builder =
                    builder.output_builder(output_handler_builder.expect(
                        "Output handler builder required for ReconfigurableSemiSync runtime",
                    ));
                Box::new(builder)
            }
            (RuntimeSpec::ReconfSemiSync, Semantics::TypedUntimed, ParserMode::Lalr) => {
                let mut builder = ReconfSemiSyncRuntimeBuilder::<
                    CheckedSemiSyncValueConfig,
                    CheckedUntimedDsrvSemantics<LALRParser>,
                    TypeCheckingSpecParser<LALRParser>,
                >::new();
                builder = builder.reconf_topic(reconf_topic);
                builder = builder
                    .input_factory(input_factory.expect(
                        "Input stream builder required for ReconfigurableSemiSync runtime",
                    ));
                builder =
                    builder.output_builder(output_handler_builder.expect(
                        "Output handler builder required for ReconfigurableSemiSync runtime",
                    ));
                Box::new(TypeCheckingBuilder(builder))
            }
            (RuntimeSpec::ReconfSemiSync, Semantics::GradualTypedUntimed, ParserMode::Lalr) => {
                let mut builder = ReconfSemiSyncRuntimeBuilder::<
                    CheckedSemiSyncValueConfig,
                    CheckedUntimedDsrvSemantics<LALRParser>,
                    GradualTypeCheckingSpecParser<LALRParser>,
                >::new();
                builder = builder.reconf_topic(reconf_topic);
                builder = builder
                    .input_factory(input_factory.expect(
                        "Input stream builder required for ReconfigurableSemiSync runtime",
                    ));
                builder =
                    builder.output_builder(output_handler_builder.expect(
                        "Output handler builder required for ReconfigurableSemiSync runtime",
                    ));
                Box::new(GradualTypeCheckingBuilder(builder))
            }
            (RuntimeSpec::Async, Semantics::TypedUntimed, ParserMode::Lalr) => {
                Box::new(TypeCheckingBuilder(AsyncRuntimeBuilder::<
                    CheckedValueConfig,
                    CheckedUntimedDsrvSemantics<LALRParser>,
                >::new()))
            }
            (RuntimeSpec::Async, Semantics::TypedUntimed, ParserMode::Combinator) => {
                Box::new(TypeCheckingBuilder(AsyncRuntimeBuilder::<
                    CheckedValueConfig,
                    CheckedUntimedDsrvSemantics<CombExprParser>,
                >::new()))
            }
            (RuntimeSpec::Async, Semantics::GradualTypedUntimed, ParserMode::Lalr) => {
                Box::new(GradualTypeCheckingBuilder(AsyncRuntimeBuilder::<
                    CheckedValueConfig,
                    CheckedUntimedDsrvSemantics<LALRParser>,
                >::new()))
            }
            (RuntimeSpec::Async, Semantics::GradualTypedUntimed, ParserMode::Combinator) => {
                Box::new(GradualTypeCheckingBuilder(AsyncRuntimeBuilder::<
                    CheckedValueConfig,
                    CheckedUntimedDsrvSemantics<CombExprParser>,
                >::new()))
            }
            (RuntimeSpec::Distributed, Semantics::Untimed, _) => {
                debug!(
                    "Setting up distributed runtime with distribution_mode = {:?}",
                    distribution_mode
                );
                if matches!(parser, ParserMode::Lalr) {
                    warn!(
                        "LALR parser not supported for DUPs with Distributed Runtime. Defaulting to Combinator parser."
                    );
                }

                let builder = DistAsyncRuntimeBuilder::<
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
                    DistributionMode::DistributedOptimizedStaticSat(
                        locations,
                        dist_constraints,
                    ) => {
                        let locations = locations
                            .into_iter()
                            .map(|loc| (loc.clone().into(), loc))
                            .collect();
                        builder.mqtt_optimized_static_dist_graph_sat(locations, dist_constraints)
                    }
                    DistributionMode::DistributedOptimizedDynamicSat(
                        locations,
                        dist_constraints,
                    ) => {
                        let locations = locations
                            .into_iter()
                            .map(|loc| (loc.clone().into(), loc))
                            .collect();
                        builder.mqtt_optimized_dynamic_dist_graph_sat(locations, dist_constraints)
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
                    DistributionMode::DistributedRosOptimizedStaticSat(
                        locations,
                        dist_constraints,
                        topic,
                    ) => {
                        let locations = locations
                            .into_iter()
                            .map(|loc| (loc.clone().into(), loc))
                            .collect();
                        builder.ros_optimized_static_dist_graph_sat(
                            locations,
                            dist_constraints,
                            topic,
                        )
                    }
                    DistributionMode::DistributedRosOptimizedDynamicSat(
                        locations,
                        dist_constraints,
                        topic,
                    ) => {
                        let locations = locations
                            .into_iter()
                            .map(|loc| (loc.clone().into(), loc))
                            .collect();
                        builder.ros_optimized_dynamic_dist_graph_sat(
                            locations,
                            dist_constraints,
                            topic,
                        )
                    }
                    DistributionMode::DistributedPredefinedStatic(graph) => {
                        builder.static_dist_graph(graph)
                    }
                    DistributionMode::DistributedPredefinedOptimized(graph, dist_constraints) => {
                        builder.predefined_optimized_dist_graph(graph, dist_constraints)
                    }
                    DistributionMode::DistributedPredefinedOptimizedSat(
                        graph,
                        dist_constraints,
                    ) => builder.predefined_optimized_dist_graph_sat(graph, dist_constraints),
                };

                let builder = builder.maybe_var_msg_types(var_msg_types.clone());
                let builder = builder.maybe_topic_mapping(topic_mapping.clone());

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

    pub async fn build(self) -> Box<dyn Runtime> {
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
        let (input_factory, input) = if self.runtime == RuntimeSpec::ReconfSemiSync {
            assert!(
                self.input.is_none(),
                "ReconfigurableSemiSync runtime requires an InputStreamFactory"
            );
            (
                Some(
                    self.input_factory
                        .expect("ReconfigurableSemiSync runtime requires an InputStreamFactory"),
                ),
                None,
            )
        } else {
            assert!(
                self.input_factory.is_none(),
                "InputStreamFactory is only supported by ReconfigurableSemiSync runtime"
            );
            (None, self.input)
        };
        let builder: Box<dyn RuntimeBuilderDyn<DsrvSpecification, Value>> =
            Self::create_common_builder(
                self.runtime,
                self.semantics,
                self.parser,
                self.executor,
                self.model,
                distribution_mode,
                self.scheduler_mode,
                input_factory,
                self.output_handler_builder.clone(),
                self.reconf_topic.clone(),
                self.use_context_transfer,
                self.topic_mapping.clone(),
                self.var_msg_types.clone(),
            );
        // Construct inputs and outputs:
        // Skip this for ReconfigurableSemiSync runtime since we handle builders directly in the match above
        let builder = if self.runtime == RuntimeSpec::ReconfSemiSync {
            builder
        } else {
            let builder = match input {
                Some(input) => builder.input(input),
                None => builder,
            };

            if let Some(output_handler_builder) = self.output_handler_builder {
                let output = output_handler_builder.build().await;
                builder.output(output)
            } else if let Some(output) = self.output {
                builder.output(output)
            } else {
                builder
            }
        };

        builder.build().await
    }
}
