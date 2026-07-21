use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use crate::io::TopicMapping;

use async_trait::async_trait;
use futures::{Future, FutureExt, StreamExt, future::LocalBoxFuture, pin_mut, select};
use smol::LocalExecutor;
use tracing::debug;
use unsync::spsc;

use crate::{
    DsrvSpecification, InputStream, OutputStream, Value, VarName,
    core::{OutputHandler, Runtime},
    distributed::{
        distribution_graphs::{LabelledDistributionGraph, NodeName},
        scheduling::{
            ReplanningCondition, Scheduler,
            communication::{NullSchedulerCommunicator, SchedulerCommunicator},
            dist_constraint_evaluator::{
                ConstraintInputBatch, ConstraintInputIndex, dist_constraint_event_stream,
                dist_constraint_input_vars,
            },
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
            planning_context::{PlanningContext, spawn_planning_context_recorder},
        },
        solvers::{
            brute_solver::BruteForceDistConstraintSolver,
            sat_solver::SatMonitoredAtDistConstraintSolver,
        },
    },
    io::mqtt::dist_graph_provider::{DistGraphProvider, StaticDistGraphProvider},
    runtime::RuntimeBuilder,
    semantics::{
        AbstractContextBuilder, AsyncConfig, MonitoringSemantics, StreamContext,
        distributed::{
            contexts::{DistributedContext, DistributedContextBuilder},
            localisation::Localisable,
        },
    },
    stream_utils::channel_to_output_stream,
};

#[cfg(feature = "mqtt")]
use crate::io::mqtt::dist_graph_provider as mqtt_dist_graph_provider;

#[cfg(not(feature = "mqtt"))]
mod mqtt_dist_graph_provider {
    use std::{collections::BTreeMap, rc::Rc};

    use crate::{
        OutputStream,
        distributed::distribution_graphs::{DistributionGraph, NodeName},
        io::mqtt::dist_graph_provider::DistGraphProvider,
    };

    pub struct MqttDistGraphProvider {
        pub central_node: NodeName,
    }

    impl MqttDistGraphProvider {
        pub fn new(
            _executor: Rc<smol::LocalExecutor<'static>>,
            central_node: NodeName,
            _locations: BTreeMap<NodeName, String>,
        ) -> Result<Self, &'static str> {
            let _ = central_node;
            Err("MQTT support not enabled")
        }
    }

    impl DistGraphProvider for MqttDistGraphProvider {
        fn dist_graph_stream(&mut self) -> OutputStream<Rc<DistributionGraph>> {
            Box::pin(futures::stream::pending())
        }
    }
}

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
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = DsrvSpecification>,
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
    input: Option<InputStream<AC::Val>>,
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

struct DirectSchedulerInputRuntime {
    input_ticks: crate::InputTickStream<Value>,
    constraint_input_index: ConstraintInputIndex,
    constraint_sender: spsc::Sender<ConstraintInputBatch>,
    planning_context: Option<PlanningContext>,
}

impl DirectSchedulerInputRuntime {
    async fn run(mut self) -> anyhow::Result<()> {
        while let Some(tick) = self.input_ticks.next().await {
            let tick = tick?;
            let mut compact_batch = Vec::new();
            let mut planning_batch = Vec::new();
            for crate::InputEvent { var, value, .. } in tick {
                if matches!(value, Value::NoVal | Value::Deferred) {
                    continue;
                }

                if let Some(index) = self.constraint_input_index.index_of(&var) {
                    compact_batch.push((index, value.clone()));
                }
                planning_batch.push((var, value));
            }

            if let Some(planning_context) = &self.planning_context {
                planning_context.record_batch(planning_batch);
            }

            if !compact_batch.is_empty() {
                self.constraint_sender
                    .send(compact_batch)
                    .await
                    .map_err(|_| {
                        anyhow::anyhow!("failed to send scheduler constraint input batch")
                    })?;
            }
        }
        Ok(())
    }
}

impl<S, AC> DistAsyncRuntimeBuilder<AC, S>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = DsrvSpecification>,
    AC::Spec: Localisable,
{
    fn make_sat_solver(
        &self,
        dist_constraints: Vec<VarName>,
        output_vars: Vec<VarName>,
        spec: &AC::Spec,
    ) -> SatMonitoredAtDistConstraintSolver<S, AC> {
        SatMonitoredAtDistConstraintSolver::new(dist_constraints, output_vars, spec.clone(), None)
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
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = DsrvSpecification>,
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

    fn input(mut self, input: crate::InputStream<AC::Val>) -> Self {
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
                .iter()
                .cloned()
                .collect();

            // TODO: Use set here to avoid collecting into vec
            let output_vars: Vec<_> = self
                .async_monitor_builder
                .model
                .as_ref()
                .unwrap()
                .output_vars()
                .iter()
                .cloned()
                .collect();
            let planning_context = match &dist_graph_mode {
                DistGraphMode::MqttStaticOptimized(_, _)
                | DistGraphMode::MqttDynamicOptimized(_, _)
                | DistGraphMode::RosStaticOptimized(_, _, _)
                | DistGraphMode::RosDynamicOptimized(_, _, _)
                | DistGraphMode::PredefinedDynamicOptimized(_, _) => {
                    Some(PlanningContext::new(true))
                }
                DistGraphMode::MqttStaticOptimizedSat(_, _)
                | DistGraphMode::MqttDynamicOptimizedSat(_, _)
                | DistGraphMode::RosStaticOptimizedSat(_, _, _)
                | DistGraphMode::RosDynamicOptimizedSat(_, _, _)
                | DistGraphMode::PredefinedDynamicOptimizedSat(_, _) => {
                    Some(PlanningContext::new(false))
                }
                _ => None,
            };
            let (
                planner,
                locations,
                dist_graph_provider,
                dist_constraints,
                replanning_condition,
                cached_localised_spec,
            ): (
                Box<dyn SchedulerPlanner>,
                Vec<NodeName>,
                Box<dyn DistGraphProvider>,
                Vec<VarName>, // Distribution constraints
                ReplanningCondition,
                Option<AC::Spec>,
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
                        None,
                    )
                }
                DistGraphMode::MqttCentralised(locations) => {
                    debug!("Creating MQTT dist graph provider");
                    let location_names = locations.keys().cloned().collect();
                    let dist_graph_provider = Box::new(
                        mqtt_dist_graph_provider::MqttDistGraphProvider::new(
                            executor.clone(),
                            "central".to_string().into(),
                            locations,
                        )
                        .expect("Failed to create MQTT dist graph provider"),
                    );
                    let planner: Box<dyn SchedulerPlanner> =
                        Box::new(CentralisedSchedulerPlanner {
                            var_names,
                            central_node: dist_graph_provider.central_node.clone(),
                        });
                    (
                        planner,
                        location_names,
                        dist_graph_provider,
                        vec![],
                        ReplanningCondition::Always,
                        None,
                    )
                }
                DistGraphMode::MqttRandom(locations) => {
                    debug!("Creating random dist graph stream");
                    let location_names = locations.keys().cloned().collect();
                    let dist_graph_provider = Box::new(
                        mqtt_dist_graph_provider::MqttDistGraphProvider::new(
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
                        None,
                    )
                }
                DistGraphMode::MqttStaticOptimized(locations, dist_constraints) => {
                    debug!("Creating static optimized dist graph provider");
                    let location_names = locations.keys().cloned().collect();
                    let dist_graph_provider = Box::new(
                        mqtt_dist_graph_provider::MqttDistGraphProvider::new(
                            executor.clone(),
                            "central".to_string().into(),
                            locations,
                        )
                        .expect("Failed to create MQTT dist graph provider"),
                    );

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
                            .iter()
                            .cloned()
                            .collect(),
                        output_vars: output_vars.clone(),
                        planning_context: planning_context.clone(),
                    };
                    let planner: Box<dyn SchedulerPlanner> =
                        Box::new(StaticOptimizedSchedulerPlanner::new(solver));

                    (
                        planner,
                        location_names,
                        dist_graph_provider,
                        dist_constraints,
                        ReplanningCondition::Never,
                        None,
                    )
                }
                DistGraphMode::MqttStaticOptimizedSat(locations, dist_constraints) => {
                    debug!("Creating static optimized SAT dist graph provider");
                    let location_names = locations.keys().cloned().collect();
                    let dist_graph_provider = Box::new(
                        mqtt_dist_graph_provider::MqttDistGraphProvider::new(
                            executor.clone(),
                            "central".to_string().into(),
                            locations,
                        )
                        .expect("Failed to create MQTT dist graph provider"),
                    );

                    let solver: SatMonitoredAtDistConstraintSolver<S, AC> =
                        self.make_sat_solver(dist_constraints.clone(), output_vars.clone(), &spec);
                    let localised_dist_spec = solver.localised_dist_spec.clone();
                    let planner: Box<dyn SchedulerPlanner> =
                        Box::new(StaticOptimizedSchedulerPlannerSat::new(solver));

                    (
                        planner,
                        location_names,
                        dist_graph_provider,
                        dist_constraints,
                        ReplanningCondition::Never,
                        Some(localised_dist_spec),
                    )
                }
                DistGraphMode::MqttDynamicOptimized(locations, dist_constraints) => {
                    debug!("Creating dynamic optimized dist graph provider");
                    let location_names = locations.keys().cloned().collect();
                    let dist_graph_provider = Box::new(
                        mqtt_dist_graph_provider::MqttDistGraphProvider::new(
                            executor.clone(),
                            "central".to_string().into(),
                            locations,
                        )
                        .expect("Failed to create MQTT dist graph provider"),
                    );

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
                            .iter()
                            .cloned()
                            .collect(),
                        output_vars: output_vars.clone(),
                        planning_context: planning_context.clone(),
                    };
                    let planner: Box<dyn SchedulerPlanner> =
                        Box::new(DynamicOptimizedSchedulerPlanner::new(solver));

                    (
                        planner,
                        location_names,
                        dist_graph_provider,
                        dist_constraints,
                        ReplanningCondition::ConstraintsFail,
                        None,
                    )
                }
                DistGraphMode::MqttDynamicOptimizedSat(locations, dist_constraints) => {
                    debug!("Creating dynamic optimized SAT dist graph provider");
                    let location_names = locations.keys().cloned().collect();
                    let dist_graph_provider = Box::new(
                        mqtt_dist_graph_provider::MqttDistGraphProvider::new(
                            executor.clone(),
                            "central".to_string().into(),
                            locations,
                        )
                        .expect("Failed to create MQTT dist graph provider"),
                    );

                    let solver: SatMonitoredAtDistConstraintSolver<S, AC> =
                        self.make_sat_solver(dist_constraints.clone(), output_vars.clone(), &spec);
                    let localised_dist_spec = solver.localised_dist_spec.clone();
                    let planner: Box<dyn SchedulerPlanner> =
                        Box::new(DynamicOptimizedSchedulerPlannerSat::new(solver));

                    (
                        planner,
                        location_names,
                        dist_graph_provider,
                        dist_constraints,
                        ReplanningCondition::ConstraintsFail,
                        Some(localised_dist_spec),
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
                            None,
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
                            None,
                        )
                    }
                    #[cfg(not(feature = "ros"))]
                    {
                        panic!("ROS dist graph mode requires building with feature 'ros'");
                    }
                }
                DistGraphMode::RosStaticOptimized(
                    _locations,
                    _dist_constraints,
                    _dist_graph_topic,
                ) => {
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

                        let solver = BruteForceDistConstraintSolver {
                            executor: executor.clone(),
                            monitor_builder: self.partial_clone(),
                            context_builder: self
                                .context_builder
                                .as_ref()
                                .map(|b| b.partial_clone()),
                            dist_constraints: _dist_constraints.clone(),
                            input_vars: self
                                .async_monitor_builder
                                .model
                                .as_ref()
                                .unwrap()
                                .input_vars()
                                .iter()
                                .cloned()
                                .collect(),
                            output_vars: output_vars.clone(),
                            planning_context: planning_context.clone(),
                        };
                        let planner: Box<dyn SchedulerPlanner> =
                            Box::new(StaticOptimizedSchedulerPlanner::new(solver));

                        (
                            planner,
                            location_names,
                            Box::new(provider) as Box<dyn DistGraphProvider>,
                            _dist_constraints,
                            ReplanningCondition::Never,
                            None,
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

                        let solver: SatMonitoredAtDistConstraintSolver<S, AC> = self
                            .make_sat_solver(_dist_constraints.clone(), output_vars.clone(), &spec);
                        let localised_dist_spec = solver.localised_dist_spec.clone();
                        let planner: Box<dyn SchedulerPlanner> =
                            Box::new(StaticOptimizedSchedulerPlannerSat::new(solver));

                        (
                            planner,
                            location_names,
                            Box::new(provider) as Box<dyn DistGraphProvider>,
                            _dist_constraints,
                            ReplanningCondition::Never,
                            Some(localised_dist_spec),
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

                        let solver = BruteForceDistConstraintSolver {
                            executor: executor.clone(),
                            monitor_builder: self.partial_clone(),
                            context_builder: self
                                .context_builder
                                .as_ref()
                                .map(|b| b.partial_clone()),
                            dist_constraints: _dist_constraints.clone(),
                            input_vars: self
                                .async_monitor_builder
                                .model
                                .as_ref()
                                .unwrap()
                                .input_vars()
                                .iter()
                                .cloned()
                                .collect(),
                            output_vars: output_vars.clone(),
                            planning_context: planning_context.clone(),
                        };
                        let planner: Box<dyn SchedulerPlanner> =
                            Box::new(DynamicOptimizedSchedulerPlanner::new(solver));

                        (
                            planner,
                            location_names,
                            Box::new(provider) as Box<dyn DistGraphProvider>,
                            _dist_constraints,
                            ReplanningCondition::ConstraintsFail,
                            None,
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

                        let solver: SatMonitoredAtDistConstraintSolver<S, AC> = self
                            .make_sat_solver(_dist_constraints.clone(), output_vars.clone(), &spec);
                        let localised_dist_spec = solver.localised_dist_spec.clone();
                        let planner: Box<dyn SchedulerPlanner> =
                            Box::new(DynamicOptimizedSchedulerPlannerSat::new(solver));

                        (
                            planner,
                            location_names,
                            Box::new(provider) as Box<dyn DistGraphProvider>,
                            _dist_constraints,
                            ReplanningCondition::ConstraintsFail,
                            Some(localised_dist_spec),
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
                            .iter()
                            .cloned()
                            .collect(),
                        output_vars: output_vars.clone(),
                        planning_context: planning_context.clone(),
                    };
                    let planner: Box<dyn SchedulerPlanner> =
                        Box::new(DynamicOptimizedSchedulerPlanner::new(solver));

                    (
                        planner,
                        location_names,
                        dist_graph_provider,
                        dist_constraints,
                        ReplanningCondition::ConstraintsFail,
                        None,
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
                    let localised_dist_spec = solver.localised_dist_spec.clone();
                    let planner: Box<dyn SchedulerPlanner> =
                        Box::new(DynamicOptimizedSchedulerPlannerSat::new(solver));

                    (
                        planner,
                        location_names,
                        dist_graph_provider,
                        dist_constraints,
                        ReplanningCondition::ConstraintsFail,
                        Some(localised_dist_spec),
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
                planning_context.clone(),
                dist_constraints.is_empty(),
                false,
            ))));
            let placement_labelling_stream = scheduler
                .borrow_mut()
                .as_mut()
                .unwrap()
                .take_placement_labelling_stream();
            if !dist_constraints.is_empty() {
                let constraint_inputs = dist_constraint_input_vars(&spec, &dist_constraints);
                let constraint_input_index =
                    ConstraintInputIndex::new(constraint_inputs.iter().cloned());
                let input = self.input.expect("Input stream not set");
                let input_ticks = crate::into_tick_stream(input);
                let constraint_channel_size =
                    constraint_input_index.len().saturating_mul(4).max(64);
                let (constraint_sender, constraint_receiver) =
                    spsc::channel(constraint_channel_size);
                let constraint_events = channel_to_output_stream(constraint_receiver);

                let stream = dist_constraint_event_stream(
                    spec.clone(),
                    dist_constraints.clone(),
                    placement_labelling_stream,
                    constraint_input_index.clone(),
                    constraint_events,
                );
                scheduler
                    .borrow_mut()
                    .as_mut()
                    .unwrap()
                    .provide_dist_constraints_streams(vec![stream]);

                return DistributedRuntime {
                    async_monitor: None,
                    direct_input_runtime: Some(DirectSchedulerInputRuntime {
                        input_ticks,
                        constraint_input_index: constraint_input_index.clone(),
                        constraint_sender,
                        planning_context,
                    }),
                    scheduler: scheduler.take().unwrap(),
                };
            }

            let dist_graph_stream = scheduler.borrow_mut().as_mut().unwrap().take_graph_stream();
            let scheduler_clone = scheduler.clone();
            let input_vars_for_planning_context =
                spec.input_vars().iter().cloned().collect::<Vec<_>>();
            let planning_context_for_callback = planning_context.clone();
            let executor_for_planning_context = executor.clone();
            let context_builder = self
                .context_builder
                .unwrap_or(DistributedContextBuilder::new().graph_stream(dist_graph_stream))
                .node_names(locations.clone())
                .add_callback(Box::new(move |ctx| {
                    if let Some(planning_context) = planning_context_for_callback {
                        let streams = input_vars_for_planning_context
                            .iter()
                            .filter_map(|var| {
                                ctx.var(var).map(|stream| {
                                    let var = var.clone();
                                    Box::pin(stream.map(move |value| vec![(var.clone(), value)]))
                                        as OutputStream<Vec<(VarName, Value)>>
                                })
                            })
                            .collect::<Vec<_>>();
                        let batches = Box::pin(futures::stream::select_all(streams))
                            as OutputStream<Vec<(VarName, Value)>>;
                        spawn_planning_context_recorder(
                            executor_for_planning_context.clone(),
                            planning_context,
                            batches,
                        )
                        .detach();
                    }

                    scheduler_clone
                        .borrow_mut()
                        .as_mut()
                        .unwrap()
                        .provide_dist_constraints_streams(Vec::new());
                }));

            let monitor_spec = if dist_constraints.is_empty() {
                if let Some(localised_spec) = cached_localised_spec {
                    localised_spec
                } else {
                    spec.clone()
                }
            } else {
                unreachable!("nonempty distribution constraints use the direct scheduler runtime")
            };

            let async_builder = self.async_monitor_builder;
            let async_builder = match self.input {
                Some(input) => async_builder.input(input),
                None => async_builder,
            };
            let async_builder = async_builder
                .context_builder(context_builder)
                .model(monitor_spec);

            let async_monitor = async_builder.build().await;

            DistributedRuntime {
                async_monitor: Some(async_monitor),
                direct_input_runtime: None,
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
    pub(crate) async_monitor: Option<AsyncRuntime<AC, S>>,
    direct_input_runtime: Option<DirectSchedulerInputRuntime>,
    // TODO: should we be responsible for building the stream of graphs
    pub(crate) scheduler: Scheduler<AC::Spec>,
}

async fn run_with_stay_alive_scheduler<S, W>(scheduler: S, worker: W) -> anyhow::Result<()>
where
    S: Future<Output = anyhow::Result<()>>,
    W: Future<Output = anyhow::Result<()>>,
{
    let scheduler = scheduler.fuse();
    let worker = worker.fuse();
    pin_mut!(scheduler, worker);

    select! {
        result = scheduler => result,
        result = worker => {
            result?;
            // A clean worker shutdown must not close distributed output topics.
            // The scheduler owns that stay-alive policy and normally remains pending.
            scheduler.await
        }
    }
}

#[async_trait(?Send)]
impl<S, AC> Runtime for DistributedRuntime<AC, S>
where
    AC::Spec: Localisable,
    AC: AsyncConfig<Ctx = DistributedContext<AC>, Spec = DsrvSpecification>,
    S: MonitoringSemantics<AC>,
{
    async fn run_boxed(self: Box<Self>) -> anyhow::Result<()> {
        self.run().await
    }

    async fn run(self: Self) -> anyhow::Result<()> {
        match (self.async_monitor, self.direct_input_runtime) {
            (Some(async_monitor), None) => {
                run_with_stay_alive_scheduler(self.scheduler.run(), async_monitor.run()).await
            }
            (None, Some(direct_input_runtime)) => {
                run_with_stay_alive_scheduler(self.scheduler.run(), direct_input_runtime.run())
                    .await
            }
            _ => panic!("Distributed runtime must have exactly one input driver"),
        }
    }
}

#[cfg(test)]
mod input_tests {
    use super::*;

    #[test]
    fn distributed_task_orchestration_propagates_worker_errors() {
        smol::block_on(async {
            let scheduler = futures::future::pending::<anyhow::Result<()>>();
            let worker = futures::future::ready(Err(anyhow::anyhow!("input failed")));

            let error = run_with_stay_alive_scheduler(scheduler, worker)
                .await
                .unwrap_err();
            assert_eq!(error.to_string(), "input failed");
        });
    }

    #[test]
    fn distributed_task_orchestration_keeps_running_after_clean_worker_shutdown() {
        let result = run_with_stay_alive_scheduler(
            futures::future::pending::<anyhow::Result<()>>(),
            futures::future::ready(Ok(())),
        )
        .now_or_never();
        assert!(result.is_none());
    }

    #[test]
    fn scheduler_input_preserves_event_ticks_and_packed_step_boundaries() {
        smol::block_on(async {
            let events = Box::pin(futures::stream::iter([Ok(crate::InputBatch::events(
                vec![
                    crate::InputEvent::new(VarName::new("x"), Value::Int(1)),
                    crate::InputEvent::new(VarName::new("x"), Value::Int(2)),
                ],
            ))]));
            let event_ticks = crate::into_tick_stream(events)
                .map(Result::unwrap)
                .collect::<Vec<_>>()
                .await;
            assert_eq!(event_ticks.len(), 2);
            assert_eq!(event_ticks[0][0].value, Value::Int(1));
            assert_eq!(event_ticks[1][0].value, Value::Int(2));

            let steps = Box::pin(futures::stream::iter([crate::InputBatch::packed_steps(
                std::num::NonZeroUsize::new(2).unwrap(),
                vec![
                    crate::InputEvent::new(VarName::new("x"), Value::Int(1)),
                    crate::InputEvent::new(VarName::new("y"), Value::Int(10)),
                    crate::InputEvent::new(VarName::new("x"), Value::Int(2)),
                    crate::InputEvent::new(VarName::new("y"), Value::Int(20)),
                ],
            )]));
            let step_ticks = crate::into_tick_stream(steps)
                .map(Result::unwrap)
                .collect::<Vec<_>>()
                .await;
            assert_eq!(step_ticks.len(), 2);
            assert_eq!(step_ticks[0][0].value, Value::Int(1));
            assert_eq!(step_ticks[1][0].value, Value::Int(2));
        });
    }

    #[test]
    fn direct_scheduler_input_propagates_errors() {
        smol::block_on(async {
            let (constraint_sender, _constraint_receiver) = spsc::channel(1);
            let runtime = DirectSchedulerInputRuntime {
                input_ticks: Box::pin(futures::stream::iter([Err(anyhow::anyhow!(
                    "input failed"
                ))])),
                constraint_input_index: ConstraintInputIndex::new(std::iter::empty()),
                constraint_sender,
                planning_context: None,
            };

            let error = runtime.run().await.unwrap_err();
            assert_eq!(error.to_string(), "input failed");
        });
    }

    #[test]
    fn direct_scheduler_does_not_advance_constraints_for_planning_only_ticks() {
        smol::block_on(async {
            let planning_context = PlanningContext::new(false);
            let (constraint_sender, constraint_receiver) = spsc::channel(1);
            let runtime = DirectSchedulerInputRuntime {
                input_ticks: Box::pin(futures::stream::iter([Ok(vec![crate::InputEvent::new(
                    VarName::new("planning_only"),
                    Value::Int(1),
                )])])),
                constraint_input_index: ConstraintInputIndex::new(std::iter::empty()),
                constraint_sender,
                planning_context: Some(planning_context.clone()),
            };
            let mut constraint_ticks = channel_to_output_stream(constraint_receiver);

            runtime.run().await.unwrap();
            assert_eq!(constraint_ticks.next().await, None);
            assert_eq!(
                planning_context.snapshot().latest_bindings,
                BTreeMap::from([(VarName::new("planning_only"), Value::Int(1))])
            );
        });
    }
}
