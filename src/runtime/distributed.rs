use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use crate::io::TopicMapping;

use async_trait::async_trait;
use futures::{StreamExt, future::LocalBoxFuture, join};
use smol::LocalExecutor;
use tracing::debug;

use crate::{
    InputProvider, Specification, UntypedDsrvSpecification, Value, VarName,
    core::{OutputHandler, Runtime},
    distributed::{
        distribution_graphs::{LabelledDistributionGraph, NodeName},
        scheduling::{
            ReplanningCondition, Scheduler,
            communication::{NullSchedulerCommunicator, SchedulerCommunicator},
            planners::{
                constrained::{DynamicOptimizedSchedulerPlanner, StaticOptimizedSchedulerPlanner},
                constrained_sat::{
                    DynamicOptimizedSchedulerPlannerSat, StaticOptimizedSchedulerPlannerSat,
                },
                core::{
                    CentralisedSchedulerPlanner, SchedulerPlanner, StaticFixedSchedulerPlanner,
                },
                random::RandomSchedulerPlanner,
            },
        },
        solvers::{
            brute_solver::BruteForceDistConstraintSolver,
            sat_solver::SatMonitoredAtDistConstraintSolver,
        },
    },
    io::{
        mqtt::dist_graph_provider::{self, DistGraphProvider, StaticDistGraphProvider},
        testing::NullOutputHandler,
    },
    runtime::RuntimeBuilder,
    semantics::{
        AbstractContextBuilder, AsyncConfig, MonitoringSemantics, StreamContext,
        distributed::{
            contexts::{DistributedContext, DistributedContextBuilder},
            localisation::Localisable,
        },
    },
};

#[cfg(feature = "ros")]
use crate::io::ros::dist_graph_provider as ros_dist_graph_provider;

use super::asynchronous::{AbstractAsyncRuntimeBuilder, AsyncRuntime, AsyncRuntimeBuilder};

#[cfg(feature = "ros")]
use crate::io::ros::ros_scheduler_communicator::RosSchedulerCommunicator;

#[derive(Debug, Clone)]
pub enum DistGraphMode {
    Static(LabelledDistributionGraph),
    MqttCentralised(
        /// Locations
        BTreeMap<NodeName, String>,
    ),
    MqttRandom(
        /// Locations
        BTreeMap<NodeName, String>,
    ),
    MqttStaticOptimized(
        /// Locations
        BTreeMap<NodeName, String>,
        /// Output variables containing distribution constraints
        Vec<VarName>,
    ),
    MqttDynamicOptimized(
        /// Locations
        BTreeMap<NodeName, String>,
        /// Output variables containing distribution constraints
        Vec<VarName>,
    ),
    MqttStaticOptimizedSat(
        /// Locations
        BTreeMap<NodeName, String>,
        /// Output variables containing distribution constraints
        Vec<VarName>,
    ),
    MqttDynamicOptimizedSat(
        /// Locations
        BTreeMap<NodeName, String>,
        /// Output variables containing distribution constraints
        Vec<VarName>,
    ),
    RosCentralised(
        /// Locations (logical node -> RVData source_robot_id)
        BTreeMap<NodeName, String>,
        /// ROS topic used by distribution graph provider
        String,
    ),
    RosRandom(
        /// Locations (logical node -> RVData source_robot_id)
        BTreeMap<NodeName, String>,
        /// ROS topic used by distribution graph provider
        String,
    ),
    RosStaticOptimized(
        /// Locations (logical node -> RVData source_robot_id)
        BTreeMap<NodeName, String>,
        /// Output variables containing distribution constraints
        Vec<VarName>,
        /// ROS topic used by distribution graph provider
        String,
    ),
    RosDynamicOptimized(
        /// Locations (logical node -> RVData source_robot_id)
        BTreeMap<NodeName, String>,
        /// Output variables containing distribution constraints
        Vec<VarName>,
        /// ROS topic used by distribution graph provider
        String,
    ),
    RosStaticOptimizedSat(
        /// Locations (logical node -> RVData source_robot_id)
        BTreeMap<NodeName, String>,
        /// Output variables containing distribution constraints
        Vec<VarName>,
        /// ROS topic used by distribution graph provider
        String,
    ),
    RosDynamicOptimizedSat(
        /// Locations (logical node -> RVData source_robot_id)
        BTreeMap<NodeName, String>,
        /// Output variables containing distribution constraints
        Vec<VarName>,
        /// ROS topic used by distribution graph provider
        String,
    ),
    PredefinedDynamicOptimized(
        /// Predefined labelled distribution graph used as topology seed
        LabelledDistributionGraph,
        /// Output variables containing distribution constraints
        Vec<VarName>,
    ),
    PredefinedDynamicOptimizedSat(
        /// Predefined labelled distribution graph used as topology seed
        LabelledDistributionGraph,
        /// Output variables containing distribution constraints
        Vec<VarName>,
    ),
}

impl<
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = UntypedDsrvSpecification>,
    S: MonitoringSemantics<AC>,
> AbstractAsyncRuntimeBuilder<AC> for DistAsyncRuntimeBuilder<AC, S>
where
    AC::Spec: Localisable,
{
    fn context_builder(mut self, context_builder: DistributedContextBuilder<AC>) -> Self {
        self.context_builder = Some(context_builder);
        self
    }
}

pub struct DistAsyncRuntimeBuilder<AC: AsyncConfig, S: MonitoringSemantics<AC>> {
    pub async_monitor_builder: AsyncRuntimeBuilder<AC, S>,
    var_msg_types: Option<BTreeMap<VarName, String>>,
    topic_mapping: Option<TopicMapping>,
    input: Option<Box<dyn InputProvider<Val = AC::Val>>>,
    pub context_builder: Option<<<AC as AsyncConfig>::Ctx as StreamContext>::Builder>,
    dist_graph_mode: Option<DistGraphMode>,
    scheduler_mode: Option<SchedulerCommunication>,
}

impl<AC: AsyncConfig, S: MonitoringSemantics<AC>> DistAsyncRuntimeBuilder<AC, S> {
    pub fn static_dist_graph(mut self, graph: LabelledDistributionGraph) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::Static(graph));
        self
    }

    pub fn mqtt_centralised_dist_graph(mut self, locations: BTreeMap<NodeName, String>) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::MqttCentralised(locations));
        self
    }

    pub fn mqtt_random_dist_graph(mut self, locations: BTreeMap<NodeName, String>) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::MqttRandom(locations));
        self
    }

    pub fn mqtt_optimized_static_dist_graph(
        mut self,
        locations: BTreeMap<NodeName, String>,
        dist_constraints: Vec<VarName>,
    ) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::MqttStaticOptimized(
            locations,
            dist_constraints,
        ));
        self
    }

    pub fn mqtt_optimized_dynamic_dist_graph(
        mut self,
        locations: BTreeMap<NodeName, String>,
        dist_constraints: Vec<VarName>,
    ) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::MqttDynamicOptimized(
            locations,
            dist_constraints,
        ));
        self
    }

    pub fn mqtt_optimized_static_dist_graph_sat(
        mut self,
        locations: BTreeMap<NodeName, String>,
        dist_constraints: Vec<VarName>,
    ) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::MqttStaticOptimizedSat(
            locations,
            dist_constraints,
        ));
        self
    }

    pub fn mqtt_optimized_dynamic_dist_graph_sat(
        mut self,
        locations: BTreeMap<NodeName, String>,
        dist_constraints: Vec<VarName>,
    ) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::MqttDynamicOptimizedSat(
            locations,
            dist_constraints,
        ));
        self
    }

    pub fn ros_centralised_dist_graph(
        mut self,
        locations: BTreeMap<NodeName, String>,
        dist_graph_topic: String,
    ) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::RosCentralised(locations, dist_graph_topic));
        self
    }

    pub fn ros_random_dist_graph(
        mut self,
        locations: BTreeMap<NodeName, String>,
        dist_graph_topic: String,
    ) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::RosRandom(locations, dist_graph_topic));
        self
    }

    pub fn ros_optimized_static_dist_graph(
        mut self,
        locations: BTreeMap<NodeName, String>,
        dist_constraints: Vec<VarName>,
        dist_graph_topic: String,
    ) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::RosStaticOptimized(
            locations,
            dist_constraints,
            dist_graph_topic,
        ));
        self
    }

    pub fn ros_optimized_dynamic_dist_graph(
        mut self,
        locations: BTreeMap<NodeName, String>,
        dist_constraints: Vec<VarName>,
        dist_graph_topic: String,
    ) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::RosDynamicOptimized(
            locations,
            dist_constraints,
            dist_graph_topic,
        ));
        self
    }

    pub fn ros_optimized_static_dist_graph_sat(
        mut self,
        locations: BTreeMap<NodeName, String>,
        dist_constraints: Vec<VarName>,
        dist_graph_topic: String,
    ) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::RosStaticOptimizedSat(
            locations,
            dist_constraints,
            dist_graph_topic,
        ));
        self
    }

    pub fn ros_optimized_dynamic_dist_graph_sat(
        mut self,
        locations: BTreeMap<NodeName, String>,
        dist_constraints: Vec<VarName>,
        dist_graph_topic: String,
    ) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::RosDynamicOptimizedSat(
            locations,
            dist_constraints,
            dist_graph_topic,
        ));
        self
    }

    pub fn partial_clone(&self) -> Self {
        Self {
            async_monitor_builder: self.async_monitor_builder.partial_clone(),
            context_builder: self.context_builder.as_ref().map(|b| b.partial_clone()),
            dist_graph_mode: self.dist_graph_mode.as_ref().map(|b| b.clone()),
            scheduler_mode: self.scheduler_mode.as_ref().map(|b| b.clone()),
            var_msg_types: self.var_msg_types.as_ref().cloned(),
            topic_mapping: self.topic_mapping.as_ref().cloned(),
            input: None,
        }
    }

    pub fn predefined_optimized_dist_graph(
        mut self,
        graph: LabelledDistributionGraph,
        dist_constraints: Vec<VarName>,
    ) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::PredefinedDynamicOptimized(
            graph,
            dist_constraints,
        ));
        self
    }

    pub fn predefined_optimized_dist_graph_sat(
        mut self,
        graph: LabelledDistributionGraph,
        dist_constraints: Vec<VarName>,
    ) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::PredefinedDynamicOptimizedSat(
            graph,
            dist_constraints,
        ));
        self
    }
}
impl<AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>>, S: MonitoringSemantics<AC>>
    DistAsyncRuntimeBuilder<AC, S>
where
    AC::Spec: Localisable,
{
    pub fn scheduler_mode(mut self, scheduler_mode: SchedulerCommunication) -> Self {
        self.scheduler_mode = Some(scheduler_mode);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SchedulerCommunication {
    Null,
    Ros {
        ros_node_name: String,
        reconf_topic: String,
    },
}

impl<S, AC> DistAsyncRuntimeBuilder<AC, S>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = UntypedDsrvSpecification>,
    AC::Spec: Localisable,
{
    fn extract_replay_history(&self) -> Option<crate::io::replay_history::ReplayHistory> {
        self.input
            .as_ref()
            .and_then(|input| input.replay_history_handle())
            .or_else(|| {
                self.input
                    .as_ref()
                    .and_then(|input| input.replay_history())
                    .map(crate::io::replay_history::ReplayHistory::store_all_with_snapshot)
            })
    }

    fn make_sat_solver(
        &self,
        dist_constraints: Vec<VarName>,
        output_vars: Vec<VarName>,
        spec: &AC::Spec,
    ) -> SatMonitoredAtDistConstraintSolver<S, AC> {
        SatMonitoredAtDistConstraintSolver::new(
            dist_constraints,
            output_vars,
            spec.clone(),
            self.extract_replay_history(),
        )
    }

    pub fn var_msg_types(mut self, var_msg_types: BTreeMap<VarName, String>) -> Self {
        self.var_msg_types = Some(var_msg_types);
        self
    }

    pub fn maybe_var_msg_types(mut self, var_msg_types: Option<BTreeMap<VarName, String>>) -> Self {
        if var_msg_types.is_some() {
            self.var_msg_types = var_msg_types;
        }
        self
    }

    pub fn topic_mapping(mut self, topic_mapping: TopicMapping) -> Self {
        self.topic_mapping = Some(topic_mapping);
        self
    }

    pub fn maybe_topic_mapping(mut self, topic_mapping: Option<TopicMapping>) -> Self {
        if self.topic_mapping.is_some() {
            self.topic_mapping = topic_mapping;
        }
        self
    }
}

impl<S, AC> RuntimeBuilder<AC::Spec, AC::Val> for DistAsyncRuntimeBuilder<AC, S>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = UntypedDsrvSpecification>,
    AC::Spec: Localisable,
{
    type Runtime = DistributedRuntime<AC, S>;

    fn new() -> Self {
        DistAsyncRuntimeBuilder {
            async_monitor_builder: AsyncRuntimeBuilder::new(),
            var_msg_types: None,
            topic_mapping: None,
            context_builder: None,
            dist_graph_mode: None,
            input: None,
            scheduler_mode: None,
        }
    }

    fn executor(mut self, ex: Rc<LocalExecutor<'static>>) -> Self {
        self.async_monitor_builder = self.async_monitor_builder.executor(ex);
        self
    }

    fn model(mut self, model: AC::Spec) -> Self {
        self.async_monitor_builder = self.async_monitor_builder.model(model);
        self
    }

    fn input(mut self, input: Box<dyn crate::InputProvider<Val = AC::Val>>) -> Self {
        self.input = Some(input);
        self
    }

    fn output(mut self, output: Box<dyn OutputHandler<Val = AC::Val>>) -> Self {
        debug!("Setting output handler");
        self.async_monitor_builder = self.async_monitor_builder.output(output);
        self
    }

    fn build(self) -> LocalBoxFuture<'static, Self::Runtime> {
        Box::pin(async move {
            let dist_graph_mode = self
                .dist_graph_mode
                .as_ref()
                .expect("Dist graph mode not set")
                .clone();
            let spec = self
                .async_monitor_builder
                .model
                .as_ref()
                .expect("Specification expected to be present")
                .clone();
            let var_msg_types = self
                .var_msg_types
                .as_ref()
                .cloned()
                .expect("Variable message types not set");
            let topic_mapping = self.topic_mapping.as_ref().cloned().unwrap_or_else(|| {
                var_msg_types
                    .keys()
                    .cloned()
                    .map(|var| {
                        let topic = format!("/{}", var.name());
                        (var, topic)
                    })
                    .collect()
            });
            let executor = self
                .async_monitor_builder
                .executor
                .as_ref()
                .expect("Executor")
                .clone();
            // TODO: TW - potential bug here. It only considers output vars from the model
            // TODO: Use set here to avoid collecting into vec
            let var_names = self
                .async_monitor_builder
                .model
                .as_ref()
                .expect("Var names not set")
                .output_vars()
                .into_iter()
                .collect();

            // TODO: Use set here to avoid collecting into vec
            let output_vars: Vec<_> = self
                .async_monitor_builder
                .model
                .as_ref()
                .unwrap()
                .output_vars()
                .into_iter()
                .collect();
            let (planner, locations, dist_graph_provider, dist_constraints, replanning_condition): (
            Box<dyn SchedulerPlanner>,
            Vec<NodeName>,
            Box<dyn DistGraphProvider>,
            Vec<VarName>, // Distribution constraints
            ReplanningCondition,
        ) = match dist_graph_mode {
            DistGraphMode::Static(graph) => {
                let graph = Rc::new(graph);
                let dist_graph_provider =
                    Box::new(StaticDistGraphProvider::new(graph.dist_graph.clone()));
                let locations = graph.dist_graph.graph.node_weights().cloned().collect();
                let planner = Box::new(StaticFixedSchedulerPlanner { fixed_graph: graph });
                (
                    planner,
                    locations,
                    dist_graph_provider,
                    vec![],
                    ReplanningCondition::Never,
                )
            }
            DistGraphMode::MqttCentralised(locations) => {
                debug!("Creating MQTT dist graph provider");
                let location_names = locations.keys().cloned().collect();
                let dist_graph_provider = Box::new(
                    dist_graph_provider::MqttDistGraphProvider::new(
                        executor.clone(),
                        "central".to_string().into(),
                        locations,
                    )
                    .expect("Failed to create MQTT dist graph provider"),
                );
                let planner: Box<dyn SchedulerPlanner> = Box::new(CentralisedSchedulerPlanner {
                    var_names,
                    central_node: dist_graph_provider.central_node.clone(),
                });
                (
                    planner,
                    location_names,
                    dist_graph_provider,
                    vec![],
                    ReplanningCondition::Always,
                )
            }
            DistGraphMode::MqttRandom(locations) => {
                debug!("Creating random dist graph stream");
                let location_names = locations.keys().cloned().collect();
                let dist_graph_provider = Box::new(
                    dist_graph_provider::MqttDistGraphProvider::new(
                        executor.clone(),
                        "central".to_string().into(),
                        locations,
                    )
                    .expect("Failed to create MQTT dist graph provider"),
                );
                let planner: Box<dyn SchedulerPlanner> =
                    Box::new(RandomSchedulerPlanner { var_names });

                (
                    planner,
                    location_names,
                    dist_graph_provider,
                    vec![],
                    ReplanningCondition::Always,
                )
            }
            DistGraphMode::MqttStaticOptimized(locations, dist_constraints) => {
                debug!("Creating static optimized dist graph provider");
                let location_names = locations.keys().cloned().collect();
                let dist_graph_provider = Box::new(
                    dist_graph_provider::MqttDistGraphProvider::new(
                        executor.clone(),
                        "central".to_string().into(),
                        locations,
                    )
                    .expect("Failed to create MQTT dist graph provider"),
                );

                let replay_history = self.extract_replay_history();

                let solver = BruteForceDistConstraintSolver {
                    executor: executor.clone(),
                    monitor_builder: self.partial_clone(),
                    context_builder: self.context_builder.as_ref().map(|b| b.partial_clone()),
                    dist_constraints: dist_constraints.clone(),
                    input_vars: self
                        .async_monitor_builder
                        .model
                        .as_ref()
                        .unwrap()
                        .input_vars()
                        .into_iter()
                        .collect(),
                    output_vars: output_vars.clone(),
                    replay_history,
                };
                let planner: Box<dyn SchedulerPlanner> =
                    Box::new(StaticOptimizedSchedulerPlanner::new(solver));

                (
                    planner,
                    location_names,
                    dist_graph_provider,
                    dist_constraints,
                    ReplanningCondition::Never,
                )
            }
            DistGraphMode::MqttStaticOptimizedSat(locations, dist_constraints) => {
                debug!("Creating static optimized SAT dist graph provider");
                let location_names = locations.keys().cloned().collect();
                let dist_graph_provider = Box::new(
                    dist_graph_provider::MqttDistGraphProvider::new(
                        executor.clone(),
                        "central".to_string().into(),
                        locations,
                    )
                    .expect("Failed to create MQTT dist graph provider"),
                );

                let solver: SatMonitoredAtDistConstraintSolver<S, AC> =
                    self.make_sat_solver(dist_constraints.clone(), output_vars.clone(), &spec);
                let planner: Box<dyn SchedulerPlanner> =
                    Box::new(StaticOptimizedSchedulerPlannerSat::new(solver));

                (
                    planner,
                    location_names,
                    dist_graph_provider,
                    dist_constraints,
                    ReplanningCondition::Never,
                )
            }
            DistGraphMode::MqttDynamicOptimized(locations, dist_constraints) => {
                debug!("Creating dynamic optimized dist graph provider");
                let location_names = locations.keys().cloned().collect();
                let dist_graph_provider = Box::new(
                    dist_graph_provider::MqttDistGraphProvider::new(
                        executor.clone(),
                        "central".to_string().into(),
                        locations,
                    )
                    .expect("Failed to create MQTT dist graph provider"),
                );

                let replay_history = self.extract_replay_history();

                let solver = BruteForceDistConstraintSolver {
                    executor: executor.clone(),
                    monitor_builder: self.partial_clone(),
                    context_builder: self.context_builder.as_ref().map(|b| b.partial_clone()),
                    dist_constraints: dist_constraints.clone(),
                    input_vars: self
                        .async_monitor_builder
                        .model
                        .as_ref()
                        .unwrap()
                        .input_vars()
                        .into_iter()
                        .collect(),
                    output_vars: output_vars.clone(),
                    replay_history,
                };
                let planner: Box<dyn SchedulerPlanner> =
                    Box::new(DynamicOptimizedSchedulerPlanner::new(solver));

                (
                    planner,
                    location_names,
                    dist_graph_provider,
                    dist_constraints,
                    ReplanningCondition::ConstraintsFail,
                )
            }
            DistGraphMode::MqttDynamicOptimizedSat(locations, dist_constraints) => {
                debug!("Creating dynamic optimized SAT dist graph provider");
                let location_names = locations.keys().cloned().collect();
                let dist_graph_provider = Box::new(
                    dist_graph_provider::MqttDistGraphProvider::new(
                        executor.clone(),
                        "central".to_string().into(),
                        locations,
                    )
                    .expect("Failed to create MQTT dist graph provider"),
                );

                let solver: SatMonitoredAtDistConstraintSolver<S, AC> =
                    self.make_sat_solver(dist_constraints.clone(), output_vars.clone(), &spec);
                let planner: Box<dyn SchedulerPlanner> =
                    Box::new(DynamicOptimizedSchedulerPlannerSat::new(solver));

                (
                    planner,
                    location_names,
                    dist_graph_provider,
                    dist_constraints,
                    ReplanningCondition::ConstraintsFail,
                )
            }
            DistGraphMode::RosCentralised(_locations, _dist_graph_topic) => {
                debug!(
                    "Creating ROS dist graph provider with topic: {}",
                    _dist_graph_topic
                );
                #[cfg(feature = "ros")]
                {
                    let location_names: Vec<NodeName> = _locations.keys().cloned().collect();
                    let provider = ros_dist_graph_provider::RosDistGraphProvider::new(
                        executor.clone(),
                        "central".to_string().into(),
                        _locations,
                        _dist_graph_topic,
                    )
                    .expect("Failed to create ROS dist graph provider");
                    let central_node = provider.central_node.clone();
                    let planner: Box<dyn SchedulerPlanner> =
                        Box::new(CentralisedSchedulerPlanner {
                            var_names,
                            central_node,
                        });
                    (
                        planner,
                        location_names,
                        Box::new(provider) as Box<dyn DistGraphProvider>,
                        vec![],
                        ReplanningCondition::Always,
                    )
                }
                #[cfg(not(feature = "ros"))]
                {
                    panic!("ROS dist graph mode requires building with feature 'ros'");
                }
            }
            DistGraphMode::RosRandom(_locations, _dist_graph_topic) => {
                debug!(
                    "Creating ROS random dist graph stream with topic: {}",
                    _dist_graph_topic
                );
                #[cfg(feature = "ros")]
                {
                    let location_names: Vec<NodeName> = _locations.keys().cloned().collect();
                    let provider = ros_dist_graph_provider::RosDistGraphProvider::new(
                        executor.clone(),
                        "central".to_string().into(),
                        _locations,
                        _dist_graph_topic,
                    )
                    .expect("Failed to create ROS dist graph provider");

                    let planner: Box<dyn SchedulerPlanner> =
                        Box::new(RandomSchedulerPlanner { var_names });

                    (
                        planner,
                        location_names,
                        Box::new(provider) as Box<dyn DistGraphProvider>,
                        vec![],
                        ReplanningCondition::Always,
                    )
                }
                #[cfg(not(feature = "ros"))]
                {
                    panic!("ROS dist graph mode requires building with feature 'ros'");
                }
            }
            DistGraphMode::RosStaticOptimized(_locations, _dist_constraints, _dist_graph_topic) => {
                debug!(
                    "Creating ROS static optimized dist graph provider with topic: {}",
                    _dist_graph_topic
                );
                #[cfg(feature = "ros")]
                {
                    let location_names: Vec<NodeName> = _locations.keys().cloned().collect();
                    let provider = ros_dist_graph_provider::RosDistGraphProvider::new(
                        executor.clone(),
                        "central".to_string().into(),
                        _locations,
                        _dist_graph_topic,
                    )
                    .expect("Failed to create ROS dist graph provider");

                    let replay_history = self.extract_replay_history();

                    let solver = BruteForceDistConstraintSolver {
                        executor: executor.clone(),
                        monitor_builder: self.partial_clone(),
                        context_builder: self.context_builder.as_ref().map(|b| b.partial_clone()),
                        dist_constraints: _dist_constraints.clone(),
                        input_vars: self
                            .async_monitor_builder
                            .model
                            .as_ref()
                            .unwrap()
                            .input_vars()
                            .into_iter()
                            .collect(),
                        output_vars: output_vars.clone(),
                        replay_history,
                    };
                    let planner: Box<dyn SchedulerPlanner> =
                        Box::new(StaticOptimizedSchedulerPlanner::new(solver));

                    (
                        planner,
                        location_names,
                        Box::new(provider) as Box<dyn DistGraphProvider>,
                        _dist_constraints,
                        ReplanningCondition::Never,
                    )
                }
                #[cfg(not(feature = "ros"))]
                {
                    panic!("ROS dist graph mode requires building with feature 'ros'");
                }
            }
            DistGraphMode::RosStaticOptimizedSat(
                _locations,
                _dist_constraints,
                _dist_graph_topic,
            ) => {
                debug!(
                    "Creating ROS static optimized SAT dist graph provider with topic: {}",
                    _dist_graph_topic
                );
                #[cfg(feature = "ros")]
                {
                    let location_names: Vec<NodeName> = _locations.keys().cloned().collect();
                    let provider = ros_dist_graph_provider::RosDistGraphProvider::new(
                        executor.clone(),
                        "central".to_string().into(),
                        _locations,
                        _dist_graph_topic,
                    )
                    .expect("Failed to create ROS dist graph provider");

                    let solver: SatMonitoredAtDistConstraintSolver<S, AC> =
                        self.make_sat_solver(_dist_constraints.clone(), output_vars.clone(), &spec);
                    let planner: Box<dyn SchedulerPlanner> =
                        Box::new(StaticOptimizedSchedulerPlannerSat::new(solver));

                    (
                        planner,
                        location_names,
                        Box::new(provider) as Box<dyn DistGraphProvider>,
                        _dist_constraints,
                        ReplanningCondition::Never,
                    )
                }
                #[cfg(not(feature = "ros"))]
                {
                    panic!("ROS dist graph mode requires building with feature 'ros'");
                }
            }
            DistGraphMode::RosDynamicOptimized(
                _locations,
                _dist_constraints,
                _dist_graph_topic,
            ) => {
                debug!(
                    "Creating ROS dynamic optimized dist graph provider with topic: {}",
                    _dist_graph_topic
                );
                #[cfg(feature = "ros")]
                {
                    let location_names: Vec<NodeName> = _locations.keys().cloned().collect();
                    let provider = ros_dist_graph_provider::RosDistGraphProvider::new(
                        executor.clone(),
                        "central".to_string().into(),
                        _locations,
                        _dist_graph_topic,
                    )
                    .expect("Failed to create ROS dist graph provider");

                    let replay_history = self.extract_replay_history();

                    let solver = BruteForceDistConstraintSolver {
                        executor: executor.clone(),
                        monitor_builder: self.partial_clone(),
                        context_builder: self.context_builder.as_ref().map(|b| b.partial_clone()),
                        dist_constraints: _dist_constraints.clone(),
                        input_vars: self
                            .async_monitor_builder
                            .model
                            .as_ref()
                            .unwrap()
                            .input_vars()
                            .into_iter()
                            .collect(),
                        output_vars: output_vars.clone(),
                        replay_history,
                    };
                    let planner: Box<dyn SchedulerPlanner> =
                        Box::new(DynamicOptimizedSchedulerPlanner::new(solver));

                    (
                        planner,
                        location_names,
                        Box::new(provider) as Box<dyn DistGraphProvider>,
                        _dist_constraints,
                        ReplanningCondition::ConstraintsFail,
                    )
                }
                #[cfg(not(feature = "ros"))]
                {
                    panic!("ROS dist graph mode requires building with feature 'ros'");
                }
            }
            DistGraphMode::RosDynamicOptimizedSat(
                _locations,
                _dist_constraints,
                _dist_graph_topic,
            ) => {
                debug!(
                    "Creating ROS dynamic optimized SAT dist graph provider with topic: {}",
                    _dist_graph_topic
                );
                #[cfg(feature = "ros")]
                {
                    let location_names: Vec<NodeName> = _locations.keys().cloned().collect();
                    let provider = ros_dist_graph_provider::RosDistGraphProvider::new(
                        executor.clone(),
                        "central".to_string().into(),
                        _locations,
                        _dist_graph_topic,
                    )
                    .expect("Failed to create ROS dist graph provider");

                    let solver: SatMonitoredAtDistConstraintSolver<S, AC> =
                        self.make_sat_solver(_dist_constraints.clone(), output_vars.clone(), &spec);
                    let planner: Box<dyn SchedulerPlanner> =
                        Box::new(DynamicOptimizedSchedulerPlannerSat::new(solver));

                    (
                        planner,
                        location_names,
                        Box::new(provider) as Box<dyn DistGraphProvider>,
                        _dist_constraints,
                        ReplanningCondition::ConstraintsFail,
                    )
                }
                #[cfg(not(feature = "ros"))]
                {
                    panic!("ROS dist graph mode requires building with feature 'ros'");
                }
            }
            DistGraphMode::PredefinedDynamicOptimized(graph, dist_constraints) => {
                debug!("Creating predefined dynamic optimized dist graph provider");
                let graph = Rc::new(graph);
                let location_names = graph.dist_graph.graph.node_weights().cloned().collect();
                let dist_graph_provider =
                    Box::new(StaticDistGraphProvider::new(graph.dist_graph.clone()));

                let replay_history = self
                    .input
                    .as_ref()
                    .and_then(|input| input.replay_history_handle())
                    .or_else(|| {
                        self.input
                            .as_ref()
                            .and_then(|input| input.replay_history())
                            .map(crate::io::replay_history::ReplayHistory::store_all_with_snapshot)
                    });

                let solver = BruteForceDistConstraintSolver {
                    executor: executor.clone(),
                    monitor_builder: self.partial_clone(),
                    context_builder: self.context_builder.as_ref().map(|b| b.partial_clone()),
                    dist_constraints: dist_constraints.clone(),
                    input_vars: self
                        .async_monitor_builder
                        .model
                        .as_ref()
                        .unwrap()
                        .input_vars()
                        .into_iter()
                        .collect(),
                    output_vars: output_vars.clone(),
                    replay_history,
                };
                let planner: Box<dyn SchedulerPlanner> =
                    Box::new(DynamicOptimizedSchedulerPlanner::new(solver));

                (
                    planner,
                    location_names,
                    dist_graph_provider,
                    dist_constraints,
                    ReplanningCondition::ConstraintsFail,
                )
            }
            DistGraphMode::PredefinedDynamicOptimizedSat(graph, dist_constraints) => {
                debug!("Creating predefined dynamic optimized SAT dist graph provider");
                let graph = Rc::new(graph);
                let location_names = graph.dist_graph.graph.node_weights().cloned().collect();
                let dist_graph_provider =
                    Box::new(StaticDistGraphProvider::new(graph.dist_graph.clone()));

                let solver: SatMonitoredAtDistConstraintSolver<S, AC> =
                    self.make_sat_solver(dist_constraints.clone(), output_vars.clone(), &spec);
                let planner: Box<dyn SchedulerPlanner> =
                    Box::new(DynamicOptimizedSchedulerPlannerSat::new(solver));

                (
                    planner,
                    location_names,
                    dist_graph_provider,
                    dist_constraints,
                    ReplanningCondition::ConstraintsFail,
                )
            }
        };
            let scheduler_mode = self.scheduler_mode.unwrap_or(SchedulerCommunication::Null);
            let scheduler_communicator = match scheduler_mode {
                SchedulerCommunication::Null => {
                    Box::new(NullSchedulerCommunicator) as Box<dyn SchedulerCommunicator<AC::Spec>>
                }
                SchedulerCommunication::Ros {
                    #[allow(unused_variables)]
                    ros_node_name,
                    #[allow(unused_variables)]
                    reconf_topic,
                } => {
                    #[cfg(feature = "ros")]
                    {
                        Box::new(
                            RosSchedulerCommunicator::new(
                                executor.clone(),
                                locations.clone(),
                                ros_node_name,
                                reconf_topic,
                            )
                            .expect("Failed to create ROS scheduler communicator"),
                        ) as Box<dyn SchedulerCommunicator<AC::Spec>>
                    }
                    #[cfg(not(feature = "ros"))]
                    {
                        panic!(
                            "Scheduler communication mode 'ros' requires building with feature 'ros'"
                        );
                    }
                }
            };
            let scheduler = Rc::new(RefCell::new(Some(Scheduler::new(
                spec.clone(),
                var_msg_types,
                topic_mapping,
                planner,
                scheduler_communicator,
                dist_graph_provider,
                replanning_condition,
                false,
            ))));
            let dist_graph_stream = scheduler.borrow_mut().as_mut().unwrap().take_graph_stream();
            let scheduler_clone = scheduler.clone();
            let dist_constraints_for_callback = dist_constraints.clone();
            let context_builder = self
                .context_builder
                .unwrap_or(DistributedContextBuilder::new().graph_stream(dist_graph_stream))
                .node_names(locations.clone())
                .add_callback(Box::new(move |ctx| {
                    let mut scheduler_borrow = scheduler_clone.borrow_mut();
                    let scheduler_ref = (&mut *scheduler_borrow).as_mut().unwrap();
                    scheduler_ref.provide_dist_constraints_streams(
                        dist_constraints_for_callback
                            .iter()
                            .map(|x| {
                                let stream = ctx.var(x).unwrap();
                                // TODO: This should be an Option<bool> stream where None means NoVal.
                                // Currently, NoVal triggers false which means we replan on NoVal.
                                Box::pin(stream.map(|v| match v {
                                    Value::Bool(b) => b,
                                    _ => false,
                                })) as crate::OutputStream<bool>
                            })
                            .collect(),
                    )
                }));

            let localised_spec = if dist_constraints.is_empty() {
                spec
            } else {
                spec.localise(&dist_constraints)
            };

            let mut async_builder = self
                .async_monitor_builder
                .maybe_input(self.input)
                .context_builder(context_builder)
                .model(localised_spec);

            if !dist_constraints.is_empty() {
                let null_output = Box::new(NullOutputHandler::new(
                    executor.clone(),
                    dist_constraints.clone().into_iter().collect(),
                ));
                async_builder = async_builder.output(null_output);
            }

            let async_monitor = async_builder.build().await;

            DistributedRuntime {
                async_monitor,
                scheduler: scheduler.take().unwrap(),
            }
        })
    }
}

/// A Monitor instance implementing the Async Runtime.
///
/// This runtime uses async actors to keep track of dependencies between
/// channels and to distribute data between them, pass data around via async
/// streams, and automatically perform garbage collection of the data contained
/// in the streams.
///
///  - The Expr type parameter is the type of the expressions in the model.
///  - The Val type parameter is the type of the values used in the channels.
///  - The S type parameter is the monitoring semantics used to evaluate the
///    expressions as streams.
///  - The AC::Spec type parameter is the model/specification being monitored.
pub struct DistributedRuntime<AC, S>
where
    AC: AsyncConfig<Ctx = DistributedContext<AC>>,
    AC::Spec: Localisable,
    S: MonitoringSemantics<AC>,
{
    pub(crate) async_monitor: AsyncRuntime<AC, S>,
    // TODO: should we be responsible for building the stream of graphs
    pub(crate) scheduler: Scheduler<AC::Spec>,
}

#[async_trait(?Send)]
#[async_trait(?Send)]
impl<S, AC> Runtime for DistributedRuntime<AC, S>
where
    AC::Spec: Localisable,
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Ctx = DistributedContext<AC>, Spec = UntypedDsrvSpecification>,
    S: MonitoringSemantics<AC>,
{
    async fn run_boxed(self: Box<Self>) -> anyhow::Result<()> {
        let (res1, res2) = join!(self.scheduler.run(), self.async_monitor.run());
        res1.and(res2)
    }

    async fn run(self: Self) -> anyhow::Result<()> {
        let (res1, res2) = join!(self.scheduler.run(), self.async_monitor.run());
        res1.and(res2)
    }
}
