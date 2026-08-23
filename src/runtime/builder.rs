use anyhow::Context as _;
use std::rc::Rc;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Debug, Display},
};

use futures::future::LocalBoxFuture;
use mstlo::{Algorithm, SynchronizationStrategy, Variables};
use smol::LocalExecutor;
use tracing::debug;

use crate::ExecutionPolicy;
use crate::io::{MsgTypeMapping, TopicMapping};
use crate::{
    DsrvSpecification, InputStream, Runtime, Specification, Value, VarName,
    cli::{
        adapters::DistributionModeBuilder,
        args::{MstloAlgorithm, MstloSynchronizationStrategy},
    },
    core::{
        JsonStreamValue, OutputWriter, RosStreamValue, RuntimeSpec, Semantics, StreamData,
        StreamType,
    },
    distributed::distribution_graphs::LabelledDistributionGraph,
    io::{InputPipeline, OutputBackendBuilder},
    lang::dsrv::{
        DsrvPipelineError, TypeCheckOptions,
        ast::{CheckedDsrvSpecification, CheckedExpr, Expr},
    },
    lang::mstlo::MstloSpecification,
    runtime::{
        dataflow::DataflowRuntimeBuilder,
        mstlo::{MstloRuntimeBuilder, MstloStreamValue},
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

// Creates a struct name with the given name, and implements AsyncConfig for it with the specified
// associated types.
// E.g.: define_config!(ValueConfig, Val = Value, Expr = Expr, Ctx = Context, Spec = DsrvSpecification);
// Creates the struct ValueConfig with AsyncConfig implementation where Val = Value, Expr = Expr,
// Ctx = Context<ValueConfig>, and Spec = DsrvSpecification.
macro_rules! define_config {
    ($name:ident, Val=$val:ty, Expr=$expr:ty, Ctx=$ctx:ident, Spec=$spec:ty) => {
        #[derive(Clone)]
        pub struct $name;

        impl AsyncConfig for $name {
            type Val = $val;
            type Expr = $expr;
            type Ctx = $ctx<Self>;
            type Spec = $spec;
        }
    };
}

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

    fn output_writer(self, writer: OutputWriter<V>) -> Self;

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

    fn output_writer(self: Box<Self>, writer: OutputWriter<V>) -> Box<dyn RuntimeBuilderDyn<M, V>>;

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

    fn output_writer(self: Box<Self>, writer: OutputWriter<V>) -> Box<dyn RuntimeBuilderDyn<M, V>> {
        Box::new(MonBuilder::output_writer(*self, writer))
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

fn parse_unchecked_spec(input: &str) -> anyhow::Result<DsrvSpecification> {
    input.parse().map_err(anyhow::Error::from)
}

fn parse_checked_spec(input: &str) -> anyhow::Result<CheckedDsrvSpecification> {
    CheckedDsrvSpecification::parse_with(input, TypeCheckOptions::STRICT).map_err(|error| {
        match error {
            DsrvPipelineError::Parse(error) => {
                anyhow::Error::new(error).context("Failed to parse reconfigured specification")
            }
            DsrvPipelineError::TypeCheck(errors) => {
                anyhow::anyhow!("Reconfigured spec failed type checking: {errors:?}")
            }
        }
    })
}

fn parse_gradually_checked_spec(input: &str) -> anyhow::Result<CheckedDsrvSpecification> {
    CheckedDsrvSpecification::parse_with(input, TypeCheckOptions::GRADUAL).map_err(|error| {
        match error {
            DsrvPipelineError::Parse(error) => {
                anyhow::Error::new(error).context("Failed to parse reconfigured specification")
            }
            DsrvPipelineError::TypeCheck(errors) => {
                anyhow::anyhow!("Reconfigured spec failed gradual type checking: {errors:?}")
            }
        }
    })
}

fn configure_reconfigurable_builder<AC, MS>(
    builder: ReconfSemiSyncRuntimeBuilder<AC, MS>,
    input_pipeline: InputPipeline<Value>,
    output_builder: OutputBackendBuilder<Value>,
    reconf_topic: Option<String>,
    use_context_transfer: bool,
) -> ReconfSemiSyncRuntimeBuilder<AC, MS>
where
    AC: AsyncConfig<Val = Value, Ctx = SemiSyncContext<AC>>,
    AC::Expr: crate::lang::core::DependencyGraphExpr + PartialEq + Debug,
    AC::Spec: crate::lang::core::DependencyGraphSpec,
    MS: crate::semantics::MonitoringSemantics<AC>,
{
    let builder = builder
        .input_pipeline(input_pipeline)
        .output_builder(output_builder)
        .use_context_transfer(use_context_transfer);
    match reconf_topic {
        Some(topic) => builder.reconf_topic(topic),
        None => builder,
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
        let model = model
            .type_check(TypeCheckOptions::STRICT)
            .expect("Model failed to type check");
        Self(self.0.model(model))
    }

    fn input(self, input: crate::InputStream<V>) -> Self {
        Self(self.0.input(input))
    }

    fn output_writer(self, writer: OutputWriter<V>) -> Self {
        Self(self.0.output_writer(writer))
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
        let model = model
            .type_check(TypeCheckOptions::GRADUAL)
            .expect("Model failed to gradual type check");
        Self(self.0.model(model))
    }

    fn input(self, input: crate::InputStream<V>) -> Self {
        Self(self.0.input(input))
    }

    fn output_writer(self, writer: OutputWriter<V>) -> Self {
        Self(self.0.output_writer(writer))
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

fn distributed_constraint_scheduler_only(
    runtime: RuntimeSpec,
    distribution_mode: &DistributionMode,
) -> bool {
    if runtime != RuntimeSpec::Distributed {
        return false;
    }

    match distribution_mode {
        DistributionMode::DistributedOptimizedStatic(_, constraints)
        | DistributionMode::DistributedOptimizedDynamic(_, constraints)
        | DistributionMode::DistributedOptimizedStaticSat(_, constraints)
        | DistributionMode::DistributedOptimizedDynamicSat(_, constraints)
        | DistributionMode::DistributedPredefinedOptimized(_, constraints)
        | DistributionMode::DistributedPredefinedOptimizedSat(_, constraints) => {
            !constraints.is_empty()
        }
        DistributionMode::DistributedRosOptimizedStatic(_, constraints, _)
        | DistributionMode::DistributedRosOptimizedDynamic(_, constraints, _)
        | DistributionMode::DistributedRosOptimizedStaticSat(_, constraints, _)
        | DistributionMode::DistributedRosOptimizedDynamicSat(_, constraints, _) => {
            !constraints.is_empty()
        }
        _ => false,
    }
}

async fn reject_simultaneous_output_sources<V>(mut writer: OutputWriter<V>) -> anyhow::Error {
    let configuration_error =
        "output_writer and output_pipeline_builder cannot be configured together";
    match writer.close().await {
        Ok(()) => anyhow::anyhow!(configuration_error),
        Err(close_error) => anyhow::anyhow!(
            "{configuration_error}; additionally failed to close the supplied output writer: {close_error}"
        ),
    }
}

async fn close_scheduler_only_output_writer<V>(mut writer: OutputWriter<V>) -> anyhow::Result<()> {
    writer
        .close()
        .await
        .map_err(|error| anyhow::anyhow!("scheduler-only output writer close failed: {error}"))
}

pub struct GeneralRuntimeBuilder<M, V: StreamData> {
    pub executor: Option<Rc<LocalExecutor<'static>>>,
    pub model: Option<M>,
    input: Option<InputStream<V>>,
    input_pipeline: Option<InputPipeline<V>>,
    pub output_writer: Option<OutputWriter<V>>,
    pub output_pipeline_builder: Option<OutputBackendBuilder<V>>,
    pub runtime: RuntimeSpec,
    pub semantics: Semantics,
    pub distribution_mode: DistributionMode,
    pub distribution_mode_builder: Option<DistributionModeBuilder>,
    pub scheduler_mode: SchedulerCommunication,
    pub reconf_topic: Option<String>,
    pub use_context_transfer: bool,
    pub var_msg_types: Option<BTreeMap<VarName, String>>,
    pub topic_mapping: Option<TopicMapping>,
    pub mstlo_algorithm: Algorithm,
    pub mstlo_synchronization_strategy: SynchronizationStrategy,
    pub mstlo_variables: Variables,
}

impl<M, V: StreamData> GeneralRuntimeBuilder<M, V> {
    pub fn new() -> Self {
        Self::with_defaults(RuntimeSpec::Async)
    }

    fn with_defaults(runtime: RuntimeSpec) -> Self {
        Self {
            executor: None,
            model: None,
            input: None,
            input_pipeline: None,
            output_writer: None,
            output_pipeline_builder: None,
            runtime,
            semantics: Semantics::GradualTypedUntimed,
            distribution_mode: DistributionMode::CentralMonitor,
            distribution_mode_builder: None,
            scheduler_mode: SchedulerCommunication::Null,
            reconf_topic: None,
            use_context_transfer: true,
            var_msg_types: None,
            topic_mapping: None,
            mstlo_algorithm: Algorithm::default(),
            mstlo_synchronization_strategy: SynchronizationStrategy::default(),
            mstlo_variables: Variables::new(),
        }
    }

    pub fn executor(self, executor: Rc<LocalExecutor<'static>>) -> Self {
        Self {
            executor: Some(executor),
            ..self
        }
    }

    pub fn maybe_executor(self, executor: Option<Rc<LocalExecutor<'static>>>) -> Self {
        match executor {
            Some(executor) => self.executor(executor),
            None => self,
        }
    }

    pub fn model(self, model: M) -> Self {
        Self {
            model: Some(model),
            ..self
        }
    }

    pub fn maybe_model(self, model: Option<M>) -> Self {
        match model {
            Some(model) => self.model(model),
            None => self,
        }
    }

    pub fn input(self, input: InputStream<V>) -> Self {
        Self {
            input: Some(input),
            ..self
        }
    }

    pub fn output_writer(self, writer: OutputWriter<V>) -> Self {
        Self {
            output_writer: Some(writer),
            ..self
        }
    }

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

    pub fn output_pipeline_builder(self, builder: OutputBackendBuilder<V>) -> Self {
        Self {
            output_pipeline_builder: Some(builder),
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
            reconf_topic: Some(reconf_topic),
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
    pub fn input_pipeline(self, pipeline: InputPipeline<Value>) -> anyhow::Result<Self> {
        Ok(Self {
            input_pipeline: Some(pipeline),
            ..self
        })
    }
}

impl GeneralRuntimeBuilder<LangSpecification, Value> {
    pub fn input_pipeline(self, pipeline: InputPipeline<Value>) -> anyhow::Result<Self> {
        Ok(Self {
            input_pipeline: Some(pipeline),
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

impl GeneralRuntimeBuilder<LangSpecification, Value> {
    pub async fn build(self) -> anyhow::Result<Box<dyn Runtime>> {
        if self.output_writer.is_some() && self.output_pipeline_builder.is_some() {
            let writer = self
                .output_writer
                .expect("output writer exists after simultaneous-output check");
            return Err(reject_simultaneous_output_sources(writer).await);
        }
        let model = self
            .model
            .ok_or_else(|| anyhow::anyhow!("Model/spec must be set"))?;
        match model {
            LangSpecification::Dsrv(spec) => GeneralRuntimeBuilder::<DsrvSpecification, Value> {
                executor: self.executor,
                model: Some(spec),
                input: self.input,
                input_pipeline: self.input_pipeline,
                output_writer: self.output_writer,
                output_pipeline_builder: self.output_pipeline_builder,
                runtime: self.runtime,
                semantics: self.semantics,
                distribution_mode: self.distribution_mode,
                distribution_mode_builder: self.distribution_mode_builder,
                scheduler_mode: self.scheduler_mode,
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
            .context("DSRV runtime could not be built"),
            LangSpecification::Mstlo(spec) => {
                let runtime = match self.runtime {
                    RuntimeSpec::Mstlo(policy) => RuntimeSpec::Mstlo(policy),
                    RuntimeSpec::Async => RuntimeSpec::Mstlo(ExecutionPolicy::Buffered),
                    runtime => {
                        anyhow::bail!("MSTLO specification cannot use {runtime:?}");
                    }
                };
                GeneralRuntimeBuilder::<MstloSpecification, Value> {
                    executor: self.executor,
                    model: Some(spec),
                    input: self.input,
                    input_pipeline: self.input_pipeline,
                    output_writer: self.output_writer,
                    output_pipeline_builder: self.output_pipeline_builder,
                    runtime,
                    semantics: self.semantics,
                    distribution_mode: DistributionMode::CentralMonitor,
                    distribution_mode_builder: None,
                    scheduler_mode: self.scheduler_mode,
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
                .context("MSTLO runtime could not be built")
            }
        }
    }
}

impl<V> GeneralRuntimeBuilder<MstloSpecification, V>
where
    V: MstloStreamValue + JsonStreamValue + RosStreamValue,
{
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

    pub async fn build(self) -> anyhow::Result<Box<dyn Runtime>> {
        if self.output_writer.is_some() && self.output_pipeline_builder.is_some() {
            let writer = self
                .output_writer
                .expect("output writer exists after simultaneous-output check");
            return Err(reject_simultaneous_output_sources(writer).await);
        }
        if self.input_pipeline.is_some() {
            anyhow::bail!(
                "InputPipeline is only supported by ReconfigurableSemiSync DSRV runtimes"
            );
        }
        let execution_policy = match self.runtime {
            RuntimeSpec::Mstlo(execution_policy) => execution_policy,
            RuntimeSpec::Async => ExecutionPolicy::Buffered,
            runtime => anyhow::bail!("MSTLO builder cannot use {runtime:?}"),
        };
        let model = self
            .model
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("MSTLO model was not set"))?;
        let executor = self
            .executor
            .clone()
            .ok_or_else(|| anyhow::anyhow!("MSTLO executor was not set"))?;
        let input = self
            .input
            .ok_or_else(|| anyhow::anyhow!("MSTLO input stream was not set"))?;
        let output_names = model.output_vars();
        let auxiliary_names = model.aux_vars();
        let mut builder = MstloRuntimeBuilder::<V>::new()
            .executor(executor.clone())
            .model(
                self.model
                    .ok_or_else(|| anyhow::anyhow!("MSTLO model was not set"))?,
            )
            .execution_policy(execution_policy)
            .algorithm(self.mstlo_algorithm)
            .semantics(Self::mstlo_semantics(self.semantics))
            .synchronization_strategy(self.mstlo_synchronization_strategy)
            .variables(self.mstlo_variables)
            .input(input);

        builder = if let Some(output_backend_builder) = self.output_pipeline_builder {
            let writer = output_backend_builder
                .executor(executor)
                .build(&output_names, &auxiliary_names, None)
                .await
                .context("MSTLO output pipeline could not be opened")?;
            builder.output_writer(writer)
        } else if let Some(writer) = self.output_writer {
            builder.output_writer(writer)
        } else {
            anyhow::bail!("MSTLO output writer was not set");
        };

        Ok(builder.build().await)
    }
}

impl GeneralRuntimeBuilder<DsrvSpecification, Value> {
    // Creates the common parts of the builder
    fn create_common_builder(
        runtime: RuntimeSpec,
        semantics: Semantics,
        executor: Option<Rc<LocalExecutor<'static>>>,
        model: Option<DsrvSpecification>,
        distribution_mode: DistributionMode,
        scheduler_mode: SchedulerCommunication,
        input_pipeline: Option<InputPipeline>,
        output_pipeline_builder: Option<OutputBackendBuilder>,
        reconf_topic: Option<String>,
        use_context_transfer: bool,
        topic_mapping: Option<TopicMapping>,
        var_msg_types: Option<MsgTypeMapping>,
    ) -> anyhow::Result<Box<dyn RuntimeBuilderDyn<DsrvSpecification, Value>>> {
        debug!(
            "Creating common builder with distribution mode: {:?}",
            distribution_mode
        );
        let builder: Box<dyn RuntimeBuilderDyn<DsrvSpecification, Value>> =
            match (runtime, semantics) {
                (RuntimeSpec::Async, Semantics::Untimed) => {
                    Box::new(AsyncRuntimeBuilder::<ValueConfig, UntimedDsrvSemantics>::new())
                }
                (RuntimeSpec::Dataflow(policy), Semantics::Untimed) => Box::new(
                    DataflowRuntimeBuilder::<DsrvSpecification>::new().execution_policy(policy),
                ),
                (RuntimeSpec::Dataflow(policy), Semantics::TypedUntimed) => {
                    Box::new(TypeCheckingBuilder(
                        DataflowRuntimeBuilder::<CheckedDsrvSpecification>::new()
                            .execution_policy(policy),
                    ))
                }
                (RuntimeSpec::Dataflow(policy), Semantics::GradualTypedUntimed) => {
                    Box::new(GradualTypeCheckingBuilder(
                        DataflowRuntimeBuilder::<CheckedDsrvSpecification>::new()
                            .execution_policy(policy),
                    ))
                }
                (RuntimeSpec::SemiSync, Semantics::Untimed) => Box::new(SemiSyncRuntimeBuilder::<
                    SemiSyncValueConfig,
                    UntimedDsrvSemantics,
                >::new()),
                (RuntimeSpec::SemiSync, Semantics::TypedUntimed) => {
                    Box::new(TypeCheckingBuilder(SemiSyncRuntimeBuilder::<
                        CheckedSemiSyncValueConfig,
                        CheckedUntimedDsrvSemantics,
                    >::new()))
                }
                (RuntimeSpec::SemiSync, Semantics::GradualTypedUntimed) => {
                    Box::new(GradualTypeCheckingBuilder(SemiSyncRuntimeBuilder::<
                        CheckedSemiSyncValueConfig,
                        CheckedUntimedDsrvSemantics,
                    >::new()))
                }
                (RuntimeSpec::ReconfSemiSync, Semantics::Untimed) => {
                    let builder = configure_reconfigurable_builder(
                    ReconfSemiSyncRuntimeBuilder::<SemiSyncValueConfig, UntimedDsrvSemantics>::new(
                    )
                    .parse_spec(parse_unchecked_spec),
                    input_pipeline.ok_or_else(|| {
                        anyhow::anyhow!(
                            "Input pipeline required for ReconfigurableSemiSync runtime"
                        )
                    })?,
                    output_pipeline_builder.ok_or_else(|| {
                        anyhow::anyhow!(
                            "Output pipeline builder required for ReconfigurableSemiSync runtime"
                        )
                    })?,
                    reconf_topic,
                    use_context_transfer,
                );
                    Box::new(builder)
                }
                (RuntimeSpec::ReconfSemiSync, Semantics::TypedUntimed) => {
                    let builder = configure_reconfigurable_builder(
                    ReconfSemiSyncRuntimeBuilder::<
                        CheckedSemiSyncValueConfig,
                        CheckedUntimedDsrvSemantics,
                    >::new()
                    .parse_spec(parse_checked_spec),
                    input_pipeline.ok_or_else(|| {
                        anyhow::anyhow!(
                            "Input pipeline required for ReconfigurableSemiSync runtime"
                        )
                    })?,
                    output_pipeline_builder.ok_or_else(|| {
                        anyhow::anyhow!(
                            "Output pipeline builder required for ReconfigurableSemiSync runtime"
                        )
                    })?,
                    reconf_topic,
                    use_context_transfer,
                );
                    Box::new(TypeCheckingBuilder(builder))
                }
                (RuntimeSpec::ReconfSemiSync, Semantics::GradualTypedUntimed) => {
                    let builder = configure_reconfigurable_builder(
                    ReconfSemiSyncRuntimeBuilder::<
                        CheckedSemiSyncValueConfig,
                        CheckedUntimedDsrvSemantics,
                    >::new()
                    .parse_spec(parse_gradually_checked_spec),
                    input_pipeline.ok_or_else(|| {
                        anyhow::anyhow!(
                            "Input pipeline required for ReconfigurableSemiSync runtime"
                        )
                    })?,
                    output_pipeline_builder.ok_or_else(|| {
                        anyhow::anyhow!(
                            "Output pipeline builder required for ReconfigurableSemiSync runtime"
                        )
                    })?,
                    reconf_topic,
                    use_context_transfer,
                );
                    Box::new(GradualTypeCheckingBuilder(builder))
                }
                (RuntimeSpec::Async, Semantics::TypedUntimed) => {
                    Box::new(TypeCheckingBuilder(AsyncRuntimeBuilder::<
                        CheckedValueConfig,
                        CheckedUntimedDsrvSemantics,
                    >::new()))
                }
                (RuntimeSpec::Async, Semantics::GradualTypedUntimed) => {
                    Box::new(GradualTypeCheckingBuilder(AsyncRuntimeBuilder::<
                        CheckedValueConfig,
                        CheckedUntimedDsrvSemantics,
                    >::new()))
                }
                (RuntimeSpec::Distributed, Semantics::Untimed) => {
                    debug!(
                        "Setting up distributed runtime with distribution_mode = {:?}",
                        distribution_mode
                    );

                    let builder =
                        DistAsyncRuntimeBuilder::<DistValueConfig, DistributedSemantics>::new();

                    let builder = builder.scheduler_mode(scheduler_mode);
                    let builder = match distribution_mode {
                        DistributionMode::CentralMonitor => builder,
                        DistributionMode::LocalMonitor(_) => {
                            anyhow::bail!("Local monitor is not implemented here yet")
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
                        DistributionMode::DistributedOptimizedStaticSat(
                            locations,
                            dist_constraints,
                        ) => {
                            let locations = locations
                                .into_iter()
                                .map(|loc| (loc.clone().into(), loc))
                                .collect();
                            builder
                                .mqtt_optimized_static_dist_graph_sat(locations, dist_constraints)
                        }
                        DistributionMode::DistributedOptimizedDynamicSat(
                            locations,
                            dist_constraints,
                        ) => {
                            let locations = locations
                                .into_iter()
                                .map(|loc| (loc.clone().into(), loc))
                                .collect();
                            builder
                                .mqtt_optimized_dynamic_dist_graph_sat(locations, dist_constraints)
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
                            builder.ros_optimized_static_dist_graph(
                                locations,
                                dist_constraints,
                                topic,
                            )
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
                            builder.ros_optimized_dynamic_dist_graph(
                                locations,
                                dist_constraints,
                                topic,
                            )
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
                        DistributionMode::DistributedPredefinedOptimized(
                            graph,
                            dist_constraints,
                        ) => builder.predefined_optimized_dist_graph(graph, dist_constraints),
                        DistributionMode::DistributedPredefinedOptimizedSat(
                            graph,
                            dist_constraints,
                        ) => builder.predefined_optimized_dist_graph_sat(graph, dist_constraints),
                    };

                    let builder = builder.maybe_var_msg_types(var_msg_types.clone());
                    let builder = builder.maybe_topic_mapping(topic_mapping.clone());

                    Box::new(builder)
                }
                (runtime, semantics) => {
                    anyhow::bail!(
                        "Unsupported runtime: {:?} and semantics: {:?} combination",
                        runtime,
                        semantics
                    )
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
        Ok(builder)
    }

    pub async fn build(self) -> anyhow::Result<Box<dyn Runtime>> {
        if self.output_writer.is_some() && self.output_pipeline_builder.is_some() {
            let writer = self
                .output_writer
                .expect("output writer exists after simultaneous-output check");
            return Err(reject_simultaneous_output_sources(writer).await);
        }

        let distribution_mode = match self.distribution_mode_builder {
            Some(distribution_mode_builder) => {
                debug!("Building with distribution_mode_builder");
                distribution_mode_builder
                    .build()
                    .await
                    .context("Failed to build distribution mode")?
            }
            None => {
                debug!(
                    "Directly using distribution mode: {:?}",
                    self.distribution_mode
                );
                self.distribution_mode
            }
        };
        let output_names = self
            .model
            .as_ref()
            .map(Specification::output_vars)
            .unwrap_or_default();
        let auxiliary_names = self
            .model
            .as_ref()
            .map(Specification::aux_vars)
            .unwrap_or_default();
        let mut configured_output_writer = self.output_writer;
        let configured_output_pipeline_builder = self.output_pipeline_builder;
        let output_executor = self.executor.clone();
        let output_pipeline_builder = configured_output_pipeline_builder.map(|builder| {
            if let Some(executor) = output_executor.as_ref() {
                builder.executor(executor.clone())
            } else {
                builder
            }
        });
        let scheduler_only =
            distributed_constraint_scheduler_only(self.runtime, &distribution_mode);
        let (input_pipeline, input) = if self.runtime == RuntimeSpec::ReconfSemiSync {
            if self.input.is_some() {
                anyhow::bail!("ReconfigurableSemiSync runtime requires an InputPipeline");
            }
            (
                Some(self.input_pipeline.ok_or_else(|| {
                    anyhow::anyhow!("ReconfigurableSemiSync runtime requires an InputPipeline")
                })?),
                None,
            )
        } else {
            if self.input_pipeline.is_some() {
                anyhow::bail!("InputPipeline is only supported by ReconfigurableSemiSync runtime");
            }
            (None, self.input)
        };
        let builder: Box<dyn RuntimeBuilderDyn<DsrvSpecification, Value>> =
            Self::create_common_builder(
                self.runtime,
                self.semantics,
                self.executor,
                self.model,
                distribution_mode,
                self.scheduler_mode,
                input_pipeline,
                if scheduler_only {
                    None
                } else {
                    output_pipeline_builder.clone()
                },
                self.reconf_topic.clone(),
                self.use_context_transfer,
                self.topic_mapping.clone(),
                self.var_msg_types.clone(),
            )?;
        // Construct the complete output pipeline before the runtime starts.
        // Runtimes receive one writer and retain their native logical tick shape.
        let builder = if self.runtime == RuntimeSpec::ReconfSemiSync {
            builder
        } else {
            match input {
                Some(input) => builder.input(input),
                None => builder,
            }
        };

        if scheduler_only {
            // The direct constraint scheduler has no local monitor and therefore cannot emit
            // model output. Do not open a configured pipeline in this branch. A writer supplied
            // by an embedding caller is already open, so close it explicitly and surface a close
            // failure rather than dropping it.
            if let Some(writer) = configured_output_writer.take() {
                close_scheduler_only_output_writer(writer).await?;
            }
            return Ok(builder.build().await);
        }

        let builder = if self.runtime == RuntimeSpec::ReconfSemiSync {
            builder
        } else if let Some(output_backend_builder) = output_pipeline_builder {
            let writer = output_backend_builder
                .build(&output_names, &auxiliary_names, None)
                .await
                .context("DSRV output pipeline could not be opened")?;
            builder.output_writer(writer)
        } else if let Some(writer) = configured_output_writer {
            builder.output_writer(writer)
        } else {
            builder
        };

        Ok(builder.build().await)
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, collections::BTreeMap};

    use async_trait::async_trait;
    use petgraph::graph::DiGraph;

    use super::*;
    use crate::core::{
        OutputBackend, OutputError, OutputInterface, OutputWriter, empty_input_stream,
    };
    use crate::distributed::distribution_graphs::{
        DistributionGraph, GenericLabelledDistributionGraph, LabelledDistributionGraph, NodeName,
    };
    use crate::io::output::{AsyncFnSink, OutputBackendConfig};
    use crate::lang::mstlo::parse_named_properties;

    struct FailingOutputBackend;

    #[async_trait(?Send)]
    impl OutputBackend for FailingOutputBackend {
        type Val = Value;

        async fn open(
            &self,
            _interface: OutputInterface,
        ) -> Result<OutputWriter<Self::Val>, OutputError> {
            Err(OutputError::backend("intentional output open failure"))
        }
    }

    fn failing_output_builder() -> OutputBackendBuilder<Value> {
        OutputBackendBuilder::new(OutputBackendConfig::custom(FailingOutputBackend))
    }

    #[derive(Clone)]
    struct CountingOutputBackend {
        opens: Rc<Cell<usize>>,
        closes: Rc<Cell<usize>>,
    }

    #[async_trait(?Send)]
    impl OutputBackend for CountingOutputBackend {
        type Val = Value;

        async fn open(
            &self,
            _interface: OutputInterface,
        ) -> Result<OutputWriter<Self::Val>, OutputError> {
            self.opens.set(self.opens.get() + 1);
            let closes = self.closes.clone();
            let sink = AsyncFnSink::with_close(
                |_batch: crate::OutputBatch<Value>| async { Ok::<(), OutputError>(()) },
                move || {
                    closes.set(closes.get() + 1);
                    async { Ok::<(), OutputError>(()) }
                },
            );
            Ok(OutputWriter::from_sink(sink))
        }
    }

    fn labelled_test_graph() -> LabelledDistributionGraph {
        let mut graph = DiGraph::new();
        let a = graph.add_node(NodeName::new("A"));
        let b = graph.add_node(NodeName::new("B"));
        graph.add_edge(a, b, 1);
        graph.add_edge(b, a, 1);
        GenericLabelledDistributionGraph {
            dist_graph: Rc::new(DistributionGraph {
                central_monitor: a,
                graph,
            }),
            var_names: Vec::new(),
            node_labels: BTreeMap::new(),
        }
    }

    fn distributed_constraint_builder(
        output_writer: Option<OutputWriter<Value>>,
        output_pipeline_builder: Option<OutputBackendBuilder<Value>>,
    ) -> GeneralRuntimeBuilder<DsrvSpecification, Value> {
        let spec = "in x\nout c\nc = monitored_at(x, A)"
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");
        let mut builder = GeneralRuntimeBuilder::<DsrvSpecification, Value>::new()
            .executor(Rc::new(LocalExecutor::new()))
            .model(spec)
            .input(empty_input_stream())
            .distribution_mode(DistributionMode::DistributedPredefinedOptimized(
                labelled_test_graph(),
                vec![VarName::new("c")],
            ))
            .runtime(RuntimeSpec::Distributed)
            .semantics(Semantics::Untimed)
            .var_msg_types(BTreeMap::new());
        if let Some(writer) = output_writer {
            builder = builder.output_writer(writer);
        }
        if let Some(output_pipeline_builder) = output_pipeline_builder {
            builder = builder.output_pipeline_builder(output_pipeline_builder);
        }
        builder
    }

    #[test]
    fn dsrv_output_open_failure_is_returned_from_build() {
        let executor = Rc::new(LocalExecutor::new());
        let spec = "out z\nz = 1".parse::<DsrvSpecification>().unwrap();
        let builder = GeneralRuntimeBuilder::<DsrvSpecification, Value>::new()
            .executor(executor)
            .model(spec)
            .input(empty_input_stream())
            .output_pipeline_builder(failing_output_builder())
            .runtime(RuntimeSpec::SemiSync)
            .semantics(Semantics::Untimed);

        let result = smol::block_on(builder.build());
        let error = match result {
            Ok(_) => panic!("a failing output backend must make build fail"),
            Err(error) => error,
        };
        let message = format!("{error:#}");
        assert!(message.contains("DSRV output pipeline could not be opened"));
        assert!(
            message.contains("intentional output open failure"),
            "unexpected DSRV build error: {message}"
        );
    }

    #[test]
    fn mstlo_output_open_failure_is_returned_from_build() {
        let executor = Rc::new(LocalExecutor::new());
        let spec = parse_named_properties("out: x > 0").unwrap();
        let builder = GeneralRuntimeBuilder::<MstloSpecification, Value>::new()
            .executor(executor)
            .model(spec)
            .input(empty_input_stream())
            .output_pipeline_builder(failing_output_builder())
            .runtime(RuntimeSpec::Mstlo(ExecutionPolicy::Buffered));

        let result = smol::block_on(builder.build());
        let error = match result {
            Ok(_) => panic!("a failing output backend must make build fail"),
            Err(error) => error,
        };
        let message = format!("{error:#}");
        assert!(message.contains("MSTLO output pipeline could not be opened"));
        assert!(
            message.contains("intentional output open failure"),
            "unexpected MSTLO build error: {message}"
        );
    }

    fn counted_writer(closes: Rc<Cell<usize>>) -> OutputWriter<Value> {
        let sink = AsyncFnSink::with_close(
            |_batch: crate::OutputBatch<Value>| async { Ok::<(), OutputError>(()) },
            move || {
                closes.set(closes.get() + 1);
                async { Ok::<(), OutputError>(()) }
            },
        );
        OutputWriter::from_sink(sink)
    }

    #[test]
    fn scheduler_only_distribution_does_not_open_local_output_pipeline() {
        let opens = Rc::new(Cell::new(0));
        let closes = Rc::new(Cell::new(0));
        let output_builder =
            OutputBackendBuilder::new(OutputBackendConfig::custom(CountingOutputBackend {
                opens: opens.clone(),
                closes: closes.clone(),
            }));

        let runtime =
            smol::block_on(distributed_constraint_builder(None, Some(output_builder)).build())
                .expect("scheduler-only distributed runtime should build without local output");

        assert_eq!(
            opens.get(),
            0,
            "scheduler-only mode must not open local output"
        );
        assert_eq!(closes.get(), 0, "no output writer was opened");
        drop(runtime);
    }

    #[test]
    fn scheduler_only_distribution_closes_an_already_open_writer() {
        let closes = Rc::new(Cell::new(0));
        let runtime = smol::block_on(
            distributed_constraint_builder(Some(counted_writer(closes.clone())), None).build(),
        )
        .expect("scheduler-only distributed runtime should build");

        assert_eq!(
            closes.get(),
            1,
            "unused supplied writer must be closed once"
        );
        drop(runtime);
    }

    #[test]
    fn simultaneous_output_sources_are_rejected_without_opening_the_pipeline() {
        let opens = Rc::new(Cell::new(0));
        let closes = Rc::new(Cell::new(0));
        let output_builder =
            OutputBackendBuilder::new(OutputBackendConfig::custom(CountingOutputBackend {
                opens: opens.clone(),
                closes: closes.clone(),
            }));
        let result = smol::block_on(
            distributed_constraint_builder(
                Some(counted_writer(closes.clone())),
                Some(output_builder),
            )
            .build(),
        );

        let error = match result {
            Ok(_) => panic!("two output sources must be rejected"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("output_writer and output_pipeline_builder")
        );
        assert_eq!(opens.get(), 0);
        assert_eq!(
            closes.get(),
            1,
            "the supplied writer must be closed on rejection"
        );
    }
}
