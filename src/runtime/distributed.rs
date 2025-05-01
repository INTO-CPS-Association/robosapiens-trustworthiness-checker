use std::{cell::RefCell, collections::BTreeMap, mem, rc::Rc};

use async_trait::async_trait;
use smol::{LocalExecutor, stream::repeat};
use tracing::debug;

use crate::{
    InputProvider, Monitor, OutputStream, Specification, Value, VarName,
    core::{AbstractMonitorBuilder, OutputHandler, Runnable, StreamData},
    dep_manage::interface::DependencyManager,
    distributed::{
        distribution_graphs::{LabelledDistributionGraph, NodeName},
        dynamic_work_scheduler::{
            BruteForceDistConstraintSolver, CentralisedSchedulerPlanner, NullSchedulerCommunicator,
            RandomSchedulerPlanner, SchedulerEnactor, SchedulerPlanner,
            StaticOptimizedSchedulerPlanner, planned_dist_graph_stream,
        },
        scheduling::SchedulerCommunicator,
    },
    io::mqtt::{
        MQTTSchedulerCommunicator,
        dist_graph_provider::{self, MQTTDistGraphProvider},
    },
    semantics::{
        AbstractContextBuilder, MonitoringSemantics, StreamContext,
        distributed::{
            contexts::{DistributedContext, DistributedContextBuilder},
            localisation::Localisable,
        },
    },
};

use super::asynchronous::{AbstractAsyncMonitorBuilder, AsyncMonitorBuilder, AsyncMonitorRunner};

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
}

impl<
    M: Specification<Expr = Expr> + Localisable,
    S: MonitoringSemantics<Expr, Value, DistributedContext<Value>>,
    Expr: 'static,
> AbstractAsyncMonitorBuilder<M, DistributedContext<Value>, Value>
    for DistAsyncMonitorBuilder<M, DistributedContext<Value>, Value, Expr, S>
{
    fn context_builder(mut self, context_builder: DistributedContextBuilder<Value>) -> Self {
        self.context_builder = Some(context_builder);
        self
    }
}

pub struct DistAsyncMonitorBuilder<
    M: Specification<Expr = Expr>,
    Ctx: StreamContext<V>,
    V: StreamData,
    Expr,
    S: MonitoringSemantics<Expr, V, Ctx>,
> {
    pub async_monitor_builder: AsyncMonitorBuilder<M, Ctx, V, Expr, S>,
    input: Option<Box<dyn InputProvider<Val = V>>>,
    pub context_builder: Option<Ctx::Builder>,
    dist_graph_mode: Option<DistGraphMode>,
    scheduler_mode: Option<SchedulerCommunication>,
}

impl<
    M: Specification<Expr = Expr>,
    Ctx: StreamContext<Val>,
    Val: StreamData,
    S: MonitoringSemantics<Expr, Val, Ctx>,
    Expr,
> DistAsyncMonitorBuilder<M, Ctx, Val, Expr, S>
{
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

    pub fn partial_clone(&self) -> Self {
        Self {
            async_monitor_builder: self.async_monitor_builder.partial_clone(),
            context_builder: self.context_builder.as_ref().map(|b| b.partial_clone()),
            dist_graph_mode: self.dist_graph_mode.as_ref().map(|b| b.clone()),
            scheduler_mode: self.scheduler_mode.as_ref().map(|b| b.clone()),
            input: None,
        }
    }
}
impl<
    M: Specification<Expr = Expr> + Localisable,
    S: MonitoringSemantics<Expr, Value, DistributedContext<Value>>,
    Expr: 'static,
> DistAsyncMonitorBuilder<M, DistributedContext<Value>, Value, Expr, S>
{
    pub fn scheduler_mode(mut self, scheduler_mode: SchedulerCommunication) -> Self {
        self.scheduler_mode = Some(scheduler_mode);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SchedulerCommunication {
    Null,
    MQTT,
}

impl<Expr: 'static, S, M> AbstractMonitorBuilder<M, Value>
    for DistAsyncMonitorBuilder<M, DistributedContext<Value>, Value, Expr, S>
where
    S: MonitoringSemantics<Expr, Value, DistributedContext<Value>>,
    M: Specification<Expr = Expr> + Localisable,
{
    type Mon = DistributedMonitorRunner<Expr, Value, S, M>;

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

    fn model(mut self, model: M) -> Self {
        self.async_monitor_builder = self.async_monitor_builder.model(model);
        self
    }

    fn input(mut self, input: Box<dyn crate::InputProvider<Val = Value>>) -> Self {
        self.input = Some(input);
        self
    }

    fn output(mut self, output: Box<dyn OutputHandler<Val = Value>>) -> Self {
        debug!("Setting output handler");
        self.async_monitor_builder = self.async_monitor_builder.output(output);
        self
    }

    fn dependencies(self, _dependencies: DependencyManager) -> Self {
        self
    }

    fn build(self) -> Self::Mon {
        let dist_graph_mode = self
            .dist_graph_mode
            .as_ref()
            .expect("Dist graph mode not set")
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
        let (dist_graph_stream, node_names, dist_graph_provider): (
            OutputStream<LabelledDistributionGraph>,
            Vec<NodeName>,
            Option<MQTTDistGraphProvider>,
        ) = match dist_graph_mode {
            DistGraphMode::Static(graph) => (
                Box::pin(repeat(graph.clone())),
                graph.dist_graph.graph.node_weights().cloned().collect(),
                None,
            ),
            DistGraphMode::MQTTCentralised(locations) => {
                debug!("Creating MQTT dist graph provider");
                let mut dist_graph_provider = dist_graph_provider::MQTTDistGraphProvider::new(
                    executor.clone(),
                    "central".to_string().into(),
                    locations.clone(),
                )
                .expect("Failed to create MQTT dist graph provider");
                let dist_graph_stream = dist_graph_provider.dist_graph_stream();
                let planner: Rc<Box<dyn SchedulerPlanner>> =
                    Rc::new(Box::new(CentralisedSchedulerPlanner {
                        var_names,
                        central_node: dist_graph_provider.central_node.clone(),
                    }));
                let labelled_dist_graph_stream =
                    planned_dist_graph_stream(dist_graph_stream, planner);
                let location_names = locations.keys().cloned().collect::<Vec<_>>();

                (
                    labelled_dist_graph_stream,
                    location_names,
                    Some(dist_graph_provider),
                )
            }
            DistGraphMode::MQTTRandom(locations) => {
                debug!("Creating random dist graph stream");
                let mut dist_graph_provider = dist_graph_provider::MQTTDistGraphProvider::new(
                    executor.clone(),
                    "central".to_string().into(),
                    locations.clone(),
                )
                .expect("Failed to create MQTT dist graph provider");
                let dist_graph_stream = dist_graph_provider.dist_graph_stream();
                let planner: Rc<Box<dyn SchedulerPlanner>> =
                    Rc::new(Box::new(RandomSchedulerPlanner { var_names }));
                let labelled_dist_graph_stream =
                    planned_dist_graph_stream(dist_graph_stream, planner);
                let location_names = locations.keys().cloned().collect::<Vec<_>>();

                (
                    labelled_dist_graph_stream,
                    location_names,
                    Some(dist_graph_provider),
                )
            }
            DistGraphMode::MQTTStaticOptimized(locations, dist_constraints) => {
                debug!("Creating static optimized dist graph provider");
                let mut dist_graph_provider = dist_graph_provider::MQTTDistGraphProvider::new(
                    executor.clone(),
                    "central".to_string().into(),
                    locations.clone(),
                )
                .expect("Failed to create MQTT dist graph provider");
                let location_names = locations.keys().cloned().collect::<Vec<_>>();
                let dist_graph_stream = dist_graph_provider.dist_graph_stream();
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

                let solver = BruteForceDistConstraintSolver {
                    executor: executor.clone(),
                    monitor_builder: self.partial_clone(),
                    context_builder: self.context_builder.as_ref().map(|b| b.partial_clone()),
                    dist_constraints,
                    input_vars,
                    output_vars,
                };
                let planner: Rc<Box<dyn SchedulerPlanner>> =
                    Rc::new(Box::new(StaticOptimizedSchedulerPlanner::new(solver)));

                let labelled_dist_graph_stream: OutputStream<LabelledDistributionGraph> =
                    planned_dist_graph_stream(dist_graph_stream, planner);

                (
                    labelled_dist_graph_stream,
                    location_names,
                    Some(dist_graph_provider),
                )
            }
        };
        let scheduler_mode = self.scheduler_mode.unwrap_or(SchedulerCommunication::Null);
        let scheduler_communicator = match scheduler_mode {
            SchedulerCommunication::Null => {
                Box::new(NullSchedulerCommunicator) as Box<dyn SchedulerCommunicator>
            }
            SchedulerCommunication::MQTT => {
                Box::new(MQTTSchedulerCommunicator::new("localhost".into()))
                    as Box<dyn SchedulerCommunicator>
            }
        };
        let scheduler = Rc::new(RefCell::new(Some(SchedulerEnactor::new(
            executor,
            scheduler_communicator,
        ))));
        let scheduler_clone = scheduler.clone();
        let context_builder = self
            .context_builder
            .unwrap_or(DistributedContextBuilder::new().graph_stream(dist_graph_stream))
            .node_names(node_names)
            .add_callback(Box::new(move |ctx| {
                let mut scheduler_borrow = scheduler_clone.borrow_mut();
                let scheduler_ref = (&mut *scheduler_borrow).as_mut().unwrap();
                scheduler_ref.dist_graph_stream(ctx.graph().unwrap());
            }));
        let async_monitor = self
            .async_monitor_builder
            .maybe_input(self.input)
            .context_builder(context_builder)
            .build();

        DistributedMonitorRunner {
            async_monitor,
            dist_graph_provider,
            scheduler: mem::take(&mut *scheduler.borrow_mut()).unwrap(),
        }
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
///  - The M type parameter is the model/specification being monitored.
pub struct DistributedMonitorRunner<Expr, Val, S, M>
where
    Val: StreamData,
    S: MonitoringSemantics<Expr, Val, DistributedContext<Val>>,
    M: Specification<Expr = Expr>,
{
    pub(crate) async_monitor: AsyncMonitorRunner<Expr, Val, S, M, DistributedContext<Val>>,
    // TODO: should we be responsible for building the stream of graphs
    #[allow(dead_code)]
    pub(crate) dist_graph_provider:
        Option<crate::io::mqtt::dist_graph_provider::MQTTDistGraphProvider>,
    pub(crate) scheduler: SchedulerEnactor,
}

#[async_trait(?Send)]
impl<Expr, M, S, V> Monitor<M, V> for DistributedMonitorRunner<Expr, V, S, M>
where
    M: Specification<Expr = Expr>,
    S: MonitoringSemantics<Expr, V, DistributedContext<V>>,
    V: StreamData,
{
    fn spec(&self) -> &M {
        self.async_monitor.spec()
    }
}

#[async_trait(?Send)]
impl<Expr, M, S, V> Runnable for DistributedMonitorRunner<Expr, V, S, M>
where
    M: Specification<Expr = Expr>,
    S: MonitoringSemantics<Expr, V, DistributedContext<V>>,
    V: StreamData,
{
    async fn run_boxed(self: Box<Self>) {
        self.scheduler.run();
        self.async_monitor.run().await;
    }

    async fn run(self: Self) {
        self.scheduler.run();
        self.async_monitor.run().await;
    }
}
