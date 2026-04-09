use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use async_trait::async_trait;
use futures::{future::LocalBoxFuture, join};
use smol::LocalExecutor;
use tracing::debug;

use crate::{
    InputProvider, Monitor, Specification, Value, VarName,
    core::{AbstractMonitorBuilder, OutputHandler, Runnable, to_typed_stream},
    distributed::{
        distribution_graphs::{LabelledDistributionGraph, NodeName},
        scheduling::{
            ReplanningCondition, Scheduler,
            communication::{NullSchedulerCommunicator, SchedulerCommunicator},
            planners::{
                constrained::StaticOptimizedSchedulerPlanner,
                core::{
                    CentralisedSchedulerPlanner, SchedulerPlanner, StaticFixedSchedulerPlanner,
                },
                random::RandomSchedulerPlanner,
            },
        },
        solvers::brute_solver::BruteForceDistConstraintSolver,
    },
    io::{
        mqtt::dist_graph_provider::{self, DistGraphProvider, StaticDistGraphProvider},
        testing::NullOutputHandler,
    },
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

use super::asynchronous::{AbstractAsyncMonitorBuilder, AsyncMonitorBuilder, AsyncMonitorRunner};

#[cfg(feature = "ros")]
use crate::io::ros::ros_scheduler_communicator::RosSchedulerCommunicator;

#[derive(Debug, Clone)]
pub enum DistGraphMode {
    Static(LabelledDistributionGraph),
    MQTTCentralised(
        /// Locations
        BTreeMap<NodeName, String>,
    ),
    MQTTRandom(
        /// Locations
        BTreeMap<NodeName, String>,
    ),
    MQTTStaticOptimized(
        /// Locations
        BTreeMap<NodeName, String>,
        /// Output variables containing distribution constraints
        Vec<VarName>,
    ),
    MQTTDynamicOptimized(
        /// Locations
        BTreeMap<NodeName, String>,
        /// Output variables containing distribution constraints
        Vec<VarName>,
    ),
    ROSCentralised(
        /// Locations (logical node -> RVData source_robot_id)
        BTreeMap<NodeName, String>,
        /// ROS topic used by distribution graph provider
        String,
    ),
    ROSRandom(
        /// Locations (logical node -> RVData source_robot_id)
        BTreeMap<NodeName, String>,
        /// ROS topic used by distribution graph provider
        String,
    ),
    ROSStaticOptimized(
        /// Locations (logical node -> RVData source_robot_id)
        BTreeMap<NodeName, String>,
        /// Output variables containing distribution constraints
        Vec<VarName>,
        /// ROS topic used by distribution graph provider
        String,
    ),
    ROSDynamicOptimized(
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
}

impl<AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>>, S: MonitoringSemantics<AC>>
    AbstractAsyncMonitorBuilder<AC> for DistAsyncMonitorBuilder<AC, S>
where
    AC::Spec: Localisable,
{
    fn context_builder(mut self, context_builder: DistributedContextBuilder<AC>) -> Self {
        self.context_builder = Some(context_builder);
        self
    }
}

pub struct DistAsyncMonitorBuilder<AC: AsyncConfig, S: MonitoringSemantics<AC>> {
    pub async_monitor_builder: AsyncMonitorBuilder<AC, S>,
    input: Option<Box<dyn InputProvider<Val = AC::Val>>>,
    pub context_builder: Option<<<AC as AsyncConfig>::Ctx as StreamContext>::Builder>,
    dist_graph_mode: Option<DistGraphMode>,
    scheduler_mode: Option<SchedulerCommunication>,
}

impl<AC: AsyncConfig, S: MonitoringSemantics<AC>> DistAsyncMonitorBuilder<AC, S> {
    pub fn static_dist_graph(mut self, graph: LabelledDistributionGraph) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::Static(graph));
        self
    }

    pub fn mqtt_centralised_dist_graph(mut self, locations: BTreeMap<NodeName, String>) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::MQTTCentralised(locations));
        self
    }

    pub fn mqtt_random_dist_graph(mut self, locations: BTreeMap<NodeName, String>) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::MQTTRandom(locations));
        self
    }

    pub fn mqtt_optimized_static_dist_graph(
        mut self,
        locations: BTreeMap<NodeName, String>,
        dist_constraints: Vec<VarName>,
    ) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::MQTTStaticOptimized(
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
        self.dist_graph_mode = Some(DistGraphMode::MQTTDynamicOptimized(
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
        self.dist_graph_mode = Some(DistGraphMode::ROSCentralised(locations, dist_graph_topic));
        self
    }

    pub fn ros_random_dist_graph(
        mut self,
        locations: BTreeMap<NodeName, String>,
        dist_graph_topic: String,
    ) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::ROSRandom(locations, dist_graph_topic));
        self
    }

    pub fn ros_optimized_static_dist_graph(
        mut self,
        locations: BTreeMap<NodeName, String>,
        dist_constraints: Vec<VarName>,
        dist_graph_topic: String,
    ) -> Self {
        self.dist_graph_mode = Some(DistGraphMode::ROSStaticOptimized(
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
        self.dist_graph_mode = Some(DistGraphMode::ROSDynamicOptimized(
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
}
impl<AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>>, S: MonitoringSemantics<AC>>
    DistAsyncMonitorBuilder<AC, S>
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

impl<S, AC> AbstractMonitorBuilder<AC::Spec, AC::Val> for DistAsyncMonitorBuilder<AC, S>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>>,
    AC::Spec: Localisable,
{
    type Mon = DistributedMonitorRunner<AC, S>;

    fn new() -> Self {
        DistAsyncMonitorBuilder {
            async_monitor_builder: AsyncMonitorBuilder::new(),
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

    fn build(self) -> Self::Mon {
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
        let executor = self
            .async_monitor_builder
            .executor
            .as_ref()
            .expect("Executor")
            .clone();
        let var_names = self
            .async_monitor_builder
            .model
            .as_ref()
            .expect("Var names not set")
            .output_vars();
        let input_vars = self
            .async_monitor_builder
            .model
            .as_ref()
            .unwrap()
            .input_vars();
        let output_vars = self
            .async_monitor_builder
            .model
            .as_ref()
            .unwrap()
            .output_vars();
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
            DistGraphMode::MQTTCentralised(locations) => {
                debug!("Creating MQTT dist graph provider");
                let location_names = locations.keys().cloned().collect();
                let dist_graph_provider = Box::new(
                    dist_graph_provider::MQTTDistGraphProvider::new(
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
            DistGraphMode::MQTTRandom(locations) => {
                debug!("Creating random dist graph stream");
                let location_names = locations.keys().cloned().collect();
                let dist_graph_provider = Box::new(
                    dist_graph_provider::MQTTDistGraphProvider::new(
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
            DistGraphMode::MQTTStaticOptimized(locations, dist_constraints) => {
                debug!("Creating static optimized dist graph provider");
                let location_names = locations.keys().cloned().collect();
                let dist_graph_provider = Box::new(
                    dist_graph_provider::MQTTDistGraphProvider::new(
                        executor.clone(),
                        "central".to_string().into(),
                        locations,
                    )
                    .expect("Failed to create MQTT dist graph provider"),
                );

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
                    input_vars,
                    output_vars,
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
            DistGraphMode::MQTTDynamicOptimized(locations, dist_constraints) => {
                debug!("Creating dynamic optimized dist graph provider");
                let location_names = locations.keys().cloned().collect();
                let dist_graph_provider = Box::new(
                    dist_graph_provider::MQTTDistGraphProvider::new(
                        executor.clone(),
                        "central".to_string().into(),
                        locations,
                    )
                    .expect("Failed to create MQTT dist graph provider"),
                );

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
                    input_vars,
                    output_vars,
                    replay_history,
                };
                let planner: Box<dyn SchedulerPlanner> =
                    Box::new(StaticOptimizedSchedulerPlanner::new(solver));

                (
                    planner,
                    location_names,
                    dist_graph_provider,
                    dist_constraints,
                    ReplanningCondition::ConstraintsFail,
                )
            }
            DistGraphMode::ROSCentralised(_locations, _dist_graph_topic) => {
                debug!(
                    "Creating ROS dist graph provider with topic: {}",
                    _dist_graph_topic
                );
                #[cfg(feature = "ros")]
                {
                    let location_names: Vec<NodeName> = _locations.keys().cloned().collect();
                    let provider = ros_dist_graph_provider::ROSDistGraphProvider::new(
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
            DistGraphMode::ROSRandom(_locations, _dist_graph_topic) => {
                debug!(
                    "Creating ROS random dist graph stream with topic: {}",
                    _dist_graph_topic
                );
                #[cfg(feature = "ros")]
                {
                    let location_names: Vec<NodeName> = _locations.keys().cloned().collect();
                    let provider = ros_dist_graph_provider::ROSDistGraphProvider::new(
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
            DistGraphMode::ROSStaticOptimized(_locations, _dist_constraints, _dist_graph_topic) => {
                debug!(
                    "Creating ROS static optimized dist graph provider with topic: {}",
                    _dist_graph_topic
                );
                #[cfg(feature = "ros")]
                {
                    let location_names: Vec<NodeName> = _locations.keys().cloned().collect();
                    let provider = ros_dist_graph_provider::ROSDistGraphProvider::new(
                        executor.clone(),
                        "central".to_string().into(),
                        _locations,
                        _dist_graph_topic,
                    )
                    .expect("Failed to create ROS dist graph provider");

                    let replay_history = self
                        .input
                        .as_ref()
                        .and_then(|input| input.replay_history_handle())
                        .or_else(|| {
                            self.input
                                .as_ref()
                                .and_then(|input| input.replay_history())
                                .map(
                                    crate::io::replay_history::ReplayHistory::store_all_with_snapshot,
                                )
                        });

                    let solver = BruteForceDistConstraintSolver {
                        executor: executor.clone(),
                        monitor_builder: self.partial_clone(),
                        context_builder: self.context_builder.as_ref().map(|b| b.partial_clone()),
                        dist_constraints: _dist_constraints.clone(),
                        input_vars,
                        output_vars,
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
            DistGraphMode::ROSDynamicOptimized(
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
                    let provider = ros_dist_graph_provider::ROSDistGraphProvider::new(
                        executor.clone(),
                        "central".to_string().into(),
                        _locations,
                        _dist_graph_topic,
                    )
                    .expect("Failed to create ROS dist graph provider");

                    let replay_history = self
                        .input
                        .as_ref()
                        .and_then(|input| input.replay_history_handle())
                        .or_else(|| {
                            self.input
                                .as_ref()
                                .and_then(|input| input.replay_history())
                                .map(
                                    crate::io::replay_history::ReplayHistory::store_all_with_snapshot,
                                )
                        });

                    let solver = BruteForceDistConstraintSolver {
                        executor: executor.clone(),
                        monitor_builder: self.partial_clone(),
                        context_builder: self.context_builder.as_ref().map(|b| b.partial_clone()),
                        dist_constraints: _dist_constraints.clone(),
                        input_vars,
                        output_vars,
                        replay_history,
                    };
                    let planner: Box<dyn SchedulerPlanner> =
                        Box::new(StaticOptimizedSchedulerPlanner::new(solver));

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
                    input_vars,
                    output_vars,
                    replay_history,
                };
                let planner: Box<dyn SchedulerPlanner> =
                    Box::new(StaticOptimizedSchedulerPlanner::new(solver));

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
                        .map(|x| to_typed_stream(ctx.var(x).unwrap()))
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
                dist_constraints.clone(),
            ));
            async_builder = async_builder.output(null_output);
        }

        let async_monitor = async_builder.build();

        DistributedMonitorRunner {
            async_monitor,
            scheduler: scheduler.take().unwrap(),
        }
    }

    fn async_build(self: Box<Self>) -> LocalBoxFuture<'static, Self::Mon> {
        Box::pin(async move { self.build() })
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
pub struct DistributedMonitorRunner<AC, S>
where
    AC: AsyncConfig<Ctx = DistributedContext<AC>>,
    AC::Spec: Localisable,
    S: MonitoringSemantics<AC>,
{
    pub(crate) async_monitor: AsyncMonitorRunner<AC, S>,
    // TODO: should we be responsible for building the stream of graphs
    pub(crate) scheduler: Scheduler<AC::Spec>,
}

#[async_trait(?Send)]
impl<S, AC> Monitor<AC::Spec, AC::Val> for DistributedMonitorRunner<AC, S>
where
    AC::Spec: Localisable,
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Ctx = DistributedContext<AC>>,
    S: MonitoringSemantics<AC>,
{
    fn spec(&self) -> &AC::Spec {
        self.async_monitor.spec()
    }
}

#[async_trait(?Send)]
impl<S, AC> Runnable for DistributedMonitorRunner<AC, S>
where
    AC::Spec: Localisable,
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Ctx = DistributedContext<AC>>,
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
