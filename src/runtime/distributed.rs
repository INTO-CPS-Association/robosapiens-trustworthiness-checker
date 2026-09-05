use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use crate::io::TopicMapping;
use ::core::cfg_select;

use async_trait::async_trait;
use futures::{Future, FutureExt, StreamExt, future::LocalBoxFuture, pin_mut, select};
use smol::LocalExecutor;
use tracing::debug;
use unsync::spsc;

use crate::{
    DsrvSpecification, LocalStream, Value, VarName,
    core::{OutputWriter, Runtime, input},
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
    utils::cancellation_token::CancellationToken,
};

use crate::io::mqtt::dist_graph_provider as mqtt_dist_graph_provider;

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
    input: Option<crate::io::OpenedInput<AC::Val>>,
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
    input: crate::io::OpenedInput<Value>,
    constraint_input_index: ConstraintInputIndex,
    constraint_sender: spsc::Sender<ConstraintInputBatch>,
    planning_context: Option<PlanningContext>,
    cancellation_token: CancellationToken,
}

impl DirectSchedulerInputRuntime {
    async fn run(mut self) -> anyhow::Result<()> {
        let cancellation_token = self.cancellation_token.clone();
        let mut result = Ok(());
        {
            let mut input_ticks = input::borrowed_tick_stream(&mut self.input);
            'input: loop {
                let tick = select! {
                    tick = input_ticks.next().fuse() => match tick {
                        Some(Ok(tick)) => tick,
                        Some(Err(error)) => {
                            result = Err(error.into());
                            break 'input;
                        }
                        None => break,
                    },
                    _ = cancellation_token.cancelled().fuse() => break,
                };
                let mut compact_batch = Vec::new();
                let mut planning_batch = Vec::new();
                for crate::InputUpdate {
                    variable, value, ..
                } in tick
                {
                    if matches!(value, Value::NoVal | Value::Deferred) {
                        continue;
                    }

                    if let Some(index) = self.constraint_input_index.index_of(&variable) {
                        compact_batch.push((index, value.clone()));
                    }
                    planning_batch.push((variable, value));
                }

                if let Some(planning_context) = &self.planning_context {
                    planning_context.record_batch(planning_batch);
                }

                if !compact_batch.is_empty() {
                    // Scheduler shutdown can close the receiver just before or just after it requests
                    // worker cancellation. Prefer cancellation, and classify a closed send observed
                    // after cancellation as normal shutdown rather than a worker error.
                    futures::select_biased! {
                        _ = cancellation_token.cancelled().fuse() => break 'input,
                        send_result = self.constraint_sender.send(compact_batch).fuse() => {
                            match send_result {
                                Ok(()) => {}
                                Err(_) => {
                                    if cancellation_token.is_cancelled().await {
                                        break 'input;
                                    }
                                    result = Err(anyhow::anyhow!(
                                        "failed to send scheduler constraint input batch"
                                    ));
                                    break 'input;
                                }
                            }
                        },
                    }
                }
            }
        }
        let mut drain = self.input.into_drain();
        while let Some(item) = drain.next().await {
            if let Err(cleanup) = item {
                let cleanup = anyhow::Error::new(cleanup);
                result = Err(match result {
                    Ok(()) => cleanup,
                    Err(primary) => anyhow::anyhow!("{}; additionally: {}", primary, cleanup),
                });
            }
        }
        result
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

    fn input(mut self, input: crate::io::OpenedInput<AC::Val>) -> Self {
        self.input = Some(input);
        self
    }

    fn output_writer(mut self, writer: OutputWriter<AC::Val>) -> Self {
        debug!("Setting output writer");
        self.async_monitor_builder = self.async_monitor_builder.output_writer(writer);
        self
    }

    fn build(mut self) -> LocalBoxFuture<'static, Self::Runtime> {
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
                    cfg_select! {
                        feature = "ros" => {
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
                        },
                        _ => {
                        panic!("ROS dist graph mode requires building with feature 'ros'");
                        },
                    }
                }
                DistGraphMode::RosRandom(_locations, _dist_graph_topic) => {
                    debug!(
                        "Creating ROS random dist graph stream with topic: {}",
                        _dist_graph_topic
                    );
                    cfg_select! {
                        feature = "ros" => {
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
                        },
                        _ => {
                        panic!("ROS dist graph mode requires building with feature 'ros'");
                        },
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
                    cfg_select! {
                        feature = "ros" => {
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
                        },
                        _ => {
                        panic!("ROS dist graph mode requires building with feature 'ros'");
                        },
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
                    cfg_select! {
                        feature = "ros" => {
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
                        },
                        _ => {
                        panic!("ROS dist graph mode requires building with feature 'ros'");
                        },
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
                    cfg_select! {
                        feature = "ros" => {
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
                        },
                        _ => {
                        panic!("ROS dist graph mode requires building with feature 'ros'");
                        },
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
                    cfg_select! {
                        feature = "ros" => {
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
                        },
                        _ => {
                        panic!("ROS dist graph mode requires building with feature 'ros'");
                        },
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
                    cfg_select! {
                        feature = "ros" => {
                        Box::new(
                            RosSchedulerCommunicator::new(
                                executor.clone(),
                                locations.clone(),
                                ros_node_name,
                                reconf_topic,
                            )
                            .expect("Failed to create ROS scheduler communicator"),
                        ) as Box<dyn SchedulerCommunicator<AC::Spec>>
                        },
                        _ => {
                        panic!(
                            "Scheduler communication mode 'ros' requires building with feature 'ros'"
                        );
                        },
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
            // Optimized distributed constraint modes are scheduler/input-driver only. They
            // intentionally do not construct a local monitor, so an output writer supplied to
            // this builder cannot emit anything. Close it deliberately instead of silently
            // dropping an already-open writer. GeneralRuntimeBuilder avoids opening a configured
            // output pipeline before reaching this branch.
            let scheduler_only_output_writer = if !dist_constraints.is_empty() {
                self.async_monitor_builder.output_writer.take()
            } else {
                None
            };
            if let Some(mut writer) = scheduler_only_output_writer {
                if let Err(error) = writer.close().await {
                    debug!(
                        %error,
                        "scheduler-only distributed runtime failed to close its unused output writer"
                    );
                }
            }
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

                let cancellation_token = CancellationToken::new();
                return DistributedRuntime {
                    async_monitor: None,
                    direct_input_runtime: Some(DirectSchedulerInputRuntime {
                        input,
                        constraint_input_index: constraint_input_index.clone(),
                        constraint_sender,
                        planning_context,
                        cancellation_token: cancellation_token.clone(),
                    }),
                    scheduler: scheduler.take().unwrap(),
                    worker_cancellation: cancellation_token,
                };
            }

            let dist_graph_stream = scheduler.borrow_mut().as_mut().unwrap().take_graph_stream();
            let scheduler_clone = scheduler.clone();
            let input_vars_for_planning_context =
                spec.input_vars().iter().cloned().collect::<Vec<_>>();
            let planning_context_for_callback = planning_context.clone();
            let executor_for_planning_context = executor.clone();
            let worker_cancellation = Rc::new(RefCell::new(None));
            let worker_cancellation_for_callback = worker_cancellation.clone();
            let context_builder = self
                .context_builder
                .unwrap_or(DistributedContextBuilder::new().graph_stream(dist_graph_stream))
                .node_names(locations.clone())
                .add_callback(Box::new(move |ctx| {
                    worker_cancellation_for_callback
                        .borrow_mut()
                        .replace(ctx.cancellation_token());
                    if let Some(planning_context) = planning_context_for_callback {
                        let streams = input_vars_for_planning_context
                            .iter()
                            .filter_map(|var| {
                                ctx.var(var).map(|stream| {
                                    let var = var.clone();
                                    Box::pin(stream.map(move |value| vec![(var.clone(), value)]))
                                        as LocalStream<Vec<(VarName, Value)>>
                                })
                            })
                            .collect::<Vec<_>>();
                        let batches = Box::pin(futures::stream::select_all(streams))
                            as LocalStream<Vec<(VarName, Value)>>;
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
            let worker_cancellation = worker_cancellation
                .borrow_mut()
                .take()
                .expect("distributed async runtime context did not expose cancellation");

            DistributedRuntime {
                async_monitor: Some(async_monitor),
                direct_input_runtime: None,
                scheduler: scheduler.take().unwrap(),
                worker_cancellation,
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
    worker_cancellation: CancellationToken,
    // TODO: should we be responsible for building the stream of graphs
    pub(crate) scheduler: Scheduler<AC::Spec>,
}

async fn run_with_stay_alive_scheduler<S, W>(
    scheduler: S,
    worker: W,
    worker_cancellation: CancellationToken,
) -> anyhow::Result<()>
where
    S: Future<Output = anyhow::Result<()>>,
    W: Future<Output = anyhow::Result<()>>,
{
    let scheduler = scheduler.fuse();
    let worker = worker.fuse();
    pin_mut!(scheduler, worker);

    futures::select_biased! {
        scheduler_result = scheduler => {
            // The scheduler is the stay-alive owner. Request cooperative cancellation from the
            // worker, then await its normal shutdown so output-owned resources are finalized.
            worker_cancellation.cancel();
            let worker_result = worker.await;
            combine_runtime_results(scheduler_result, worker_result)
        }
        worker_result = worker => match worker_result {
            Ok(()) => {
                // A clean worker shutdown must not close distributed output topics.
                // The scheduler owns that stay-alive policy and normally remains pending.
                scheduler.await
            }
            Err(worker_error) => {
                // Preserve a scheduler error when it completed at the same time, but do not
                // await a scheduler that intentionally owns the stay-alive lifetime.
                match scheduler.now_or_never() {
                    Some(scheduler_result) => {
                        combine_runtime_results(scheduler_result, Err(worker_error))
                    }
                    None => Err(worker_error),
                }
            }
        }
    }
}

fn combine_runtime_results(
    primary: anyhow::Result<()>,
    additional: anyhow::Result<()>,
) -> anyhow::Result<()> {
    match (primary, additional) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(primary), Err(additional)) => {
            Err(anyhow::anyhow!("{primary}; additionally: {additional}"))
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
        let DistributedRuntime {
            async_monitor,
            direct_input_runtime,
            worker_cancellation,
            scheduler,
        } = self;
        match (async_monitor, direct_input_runtime) {
            (Some(async_monitor), None) => {
                run_with_stay_alive_scheduler(
                    scheduler.run(),
                    async_monitor.run(),
                    worker_cancellation,
                )
                .await
            }
            (None, Some(direct_input_runtime)) => {
                run_with_stay_alive_scheduler(
                    scheduler.run(),
                    direct_input_runtime.run(),
                    worker_cancellation,
                )
                .await
            }
            _ => panic!("Distributed runtime must have exactly one input driver"),
        }
    }
}

#[cfg(test)]
mod input_tests {
    use std::cell::Cell;

    use super::*;
    use crate::InputStream;

    async fn cooperative_worker(
        cancellation_token: CancellationToken,
        cancellation_observed: Rc<Cell<bool>>,
        cleanups: Rc<Cell<usize>>,
    ) -> anyhow::Result<()> {
        cancellation_token.cancelled().await;
        cancellation_observed.set(true);
        cleanups.set(cleanups.get() + 1);
        Ok(())
    }

    #[test]
    fn distributed_task_orchestration_propagates_worker_errors() {
        smol::block_on(async {
            let scheduler = futures::future::pending::<anyhow::Result<()>>();
            let worker = futures::future::ready(Err(anyhow::anyhow!("input failed")));

            let error = run_with_stay_alive_scheduler(scheduler, worker, CancellationToken::new())
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
            CancellationToken::new(),
        )
        .now_or_never();
        assert!(result.is_none());
    }

    #[test]
    fn distributed_task_orchestration_preserves_simultaneous_scheduler_and_worker_errors() {
        let result = smol::block_on(run_with_stay_alive_scheduler(
            futures::future::ready(Err(anyhow::anyhow!("scheduler failed"))),
            futures::future::ready(Err(anyhow::anyhow!("worker failed"))),
            CancellationToken::new(),
        ));

        let error = result.expect_err("simultaneous failures should be returned");
        let message = error.to_string();
        assert!(message.contains("scheduler failed"));
        assert!(message.contains("worker failed"));
    }

    #[test]
    fn scheduler_completion_requests_cancellation_and_awaits_worker_cleanup() {
        let cancellation_token = CancellationToken::new();
        let cancellation_observed = Rc::new(Cell::new(false));
        let cleanups = Rc::new(Cell::new(0));
        let result = smol::block_on(run_with_stay_alive_scheduler(
            futures::future::ready(Ok(())),
            cooperative_worker(
                cancellation_token.clone(),
                cancellation_observed.clone(),
                cleanups.clone(),
            ),
            cancellation_token.clone(),
        ));

        assert!(result.is_ok());
        assert!(smol::block_on(cancellation_token.is_cancelled()));
        assert!(cancellation_observed.get());
        assert_eq!(cleanups.get(), 1);
    }

    #[test]
    fn scheduler_error_cancels_worker_and_preserves_scheduler_error() {
        let cancellation_token = CancellationToken::new();
        let cancellation_observed = Rc::new(Cell::new(false));
        let cleanups = Rc::new(Cell::new(0));
        let result = smol::block_on(run_with_stay_alive_scheduler(
            futures::future::ready(Err(anyhow::anyhow!("scheduler failed"))),
            cooperative_worker(
                cancellation_token.clone(),
                cancellation_observed.clone(),
                cleanups.clone(),
            ),
            cancellation_token,
        ));

        let error = result.expect_err("scheduler failure should be returned");
        assert_eq!(error.to_string(), "scheduler failed");
        assert!(cancellation_observed.get());
        assert_eq!(cleanups.get(), 1);
    }

    #[test]
    fn scheduler_and_worker_errors_are_combined_when_both_complete() {
        let result = combine_runtime_results(
            Err(anyhow::anyhow!("scheduler failed")),
            Err(anyhow::anyhow!("worker cleanup failed")),
        )
        .expect_err("both errors should be retained");

        assert!(result.to_string().contains("scheduler failed"));
        assert!(result.to_string().contains("worker cleanup failed"));
    }

    #[test]
    fn scheduler_input_preserves_event_ticks_and_packed_step_boundaries() {
        smol::block_on(async {
            let events = Box::pin(futures::stream::iter([Ok(crate::InputBatch::from_ticks(
                vec![
                    vec![crate::InputUpdate::new(VarName::new("x"), Value::Int(1))],
                    vec![crate::InputUpdate::new(VarName::new("x"), Value::Int(2))],
                ],
            )
            .unwrap())]));
            let event_ticks = input::into_tick_stream(events)
                .map(Result::unwrap)
                .collect::<Vec<_>>()
                .await;
            assert_eq!(event_ticks.len(), 2);
            assert_eq!(event_ticks[0][0].value, Value::Int(1));
            assert_eq!(event_ticks[1][0].value, Value::Int(2));

            let step_batch = crate::InputBatch::packed_rows(
                vec![VarName::new("x"), VarName::new("y")].into_boxed_slice(),
                vec![Value::Int(1), Value::Int(10), Value::Int(2), Value::Int(20)],
            )
            .unwrap();
            let steps = Box::pin(futures::stream::iter([Ok(step_batch)]));
            let step_ticks = input::into_tick_stream(steps)
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
                input: (Box::pin(futures::stream::iter([Err(crate::InputError::source(
                    "input failed",
                ))])) as InputStream<Value>)
                    .into(),
                constraint_input_index: ConstraintInputIndex::new(std::iter::empty()),
                constraint_sender,
                planning_context: None,
                cancellation_token: CancellationToken::new(),
            };

            let error = runtime.run().await.unwrap_err();
            assert_eq!(error.to_string(), "input source error: input failed");
        });
    }

    #[test]
    fn direct_scheduler_input_treats_closed_send_after_cancellation_as_clean_shutdown() {
        smol::block_on(async {
            let cancellation_token = CancellationToken::new();
            let cancellation_for_tick = cancellation_token.clone();
            let input: InputStream<Value> = Box::pin(futures::stream::once(async move {
                cancellation_for_tick.cancel();
                Ok(crate::InputBatch::update(VarName::new("x"), Value::Int(1)))
            }));
            let (constraint_sender, constraint_receiver) = spsc::channel(1);
            drop(constraint_receiver);
            let runtime = DirectSchedulerInputRuntime {
                input: input.into(),
                constraint_input_index: ConstraintInputIndex::new([VarName::new("x")]),
                constraint_sender,
                planning_context: None,
                cancellation_token: cancellation_token.clone(),
            };

            assert!(runtime.run().await.is_ok());
            assert!(cancellation_token.is_cancelled().await);
        });
    }

    #[test]
    fn direct_scheduler_does_not_advance_constraints_for_planning_only_ticks() {
        smol::block_on(async {
            let planning_context = PlanningContext::new(false);
            let (constraint_sender, constraint_receiver) = spsc::channel(1);
            let runtime = DirectSchedulerInputRuntime {
                input: (Box::pin(futures::stream::iter([Ok(crate::InputBatch::update(
                    VarName::new("planning_only"),
                    Value::Int(1),
                ))])) as InputStream<Value>)
                    .into(),
                constraint_input_index: ConstraintInputIndex::new(std::iter::empty()),
                constraint_sender,
                planning_context: Some(planning_context.clone()),
                cancellation_token: CancellationToken::new(),
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
