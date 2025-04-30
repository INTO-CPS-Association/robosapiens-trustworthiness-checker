use std::{cell::RefCell, collections::BTreeMap, mem, rc::Rc};

use async_stream::stream;
use async_trait::async_trait;
use futures::stream::LocalBoxStream;
use petgraph::graph::NodeIndex;
use smol::{
    LocalExecutor,
    stream::{StreamExt, repeat},
};
use tracing::{debug, info};

use crate::{
    InputProvider, Monitor, OutputStream, Specification, Value, VarName,
    core::{AbstractMonitorBuilder, OutputHandler, Runnable, StreamData},
    dep_manage::interface::DependencyManager,
    distributed::{
        distribution_graphs::{
            DistributionGraph, LabelledDistributionGraph, NodeName, graph_to_png,
            possible_labelled_dist_graphs,
        },
        dynamic_work_scheduler::{NullSchedulerCommunicator, Scheduler},
        scheduling::SchedulerCommunicator,
    },
    io::{
        mqtt::{
            MQTTSchedulerCommunicator,
            dist_graph_provider::{self, MQTTDistGraphProvider},
        },
        testing::ManualOutputHandler,
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
    async_monitor_builder: AsyncMonitorBuilder<M, Ctx, V, Expr, S>,
    input: Option<Box<dyn InputProvider<Val = V>>>,
    context_builder: Option<Ctx::Builder>,
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

    fn output_stream_for_graph(
        builder: Self,
        dist_constraints: Vec<VarName>,
        input_vars: Vec<VarName>,
        output_vars: Vec<VarName>,
        context_builder: &Option<DistributedContextBuilder<Value>>,
        executor: Rc<LocalExecutor<'static>>,
        labelled_graph: &LabelledDistributionGraph,
    ) -> OutputStream<Vec<Value>> {
        // Build a runtime for constructing the output stream
        debug!(
            "Output stream for graph with input_vars: {:?} and output_vars: {:?}",
            input_vars, output_vars
        );
        let input_provider: Box<dyn InputProvider<Val = Value>> =
            Box::new(BTreeMap::<VarName, OutputStream<Value>>::new());
        let mut output_handler =
            ManualOutputHandler::new(executor.clone(), dist_constraints.clone());
        let output_stream: OutputStream<Vec<Value>> = Box::pin(output_handler.get_output());
        let potential_dist_graph_stream = Box::pin(repeat(labelled_graph.clone()));
        let context_builder = context_builder
            .as_ref()
            .map(|b| b.partial_clone())
            .unwrap_or(DistributedContextBuilder::new().graph_stream(potential_dist_graph_stream));
        let runtime = builder
            .context_builder(context_builder)
            .static_dist_graph(labelled_graph.clone())
            .input(input_provider)
            .output(Box::new(output_handler))
            .build();
        executor.spawn(runtime.run()).detach();

        // Construct a wrapped output stream which makes sure the context starts
        output_stream
    }

    /// Finds all possible labelled distribution graphs given a set of distribution constraints
    /// and a distribution graph
    fn possible_labelled_dist_graph_stream(
        builder: Self,
        initial_graph: Rc<DistributionGraph>,
        dist_constraints: Vec<VarName>,
        input_vars: Vec<VarName>,
        output_vars: Vec<VarName>,
        context_builder: Option<DistributedContextBuilder<Value>>,
        executor: Rc<LocalExecutor<'static>>,
    ) -> OutputStream<LabelledDistributionGraph> {
        // To avoid lifetime and move issues, clone all necessary data for the async block.
        let dist_constraints = dist_constraints.clone();
        let context_builder = context_builder.map(|b| b.partial_clone());
        let executor = executor.clone();
        let non_dist_constraints: Vec<VarName> = output_vars
            .iter()
            .filter(|name| !dist_constraints.contains(name))
            .cloned()
            .collect();
        let localised_spec = builder
            .async_monitor_builder
            .model
            .as_ref()
            .unwrap()
            .localise(&dist_constraints);
        let builder = builder.model(localised_spec);

        info!("Starting optimized distributed graph generation");

        Box::pin(async_stream::stream! {
            for (i, graph) in possible_labelled_dist_graphs(initial_graph, dist_constraints.clone(), non_dist_constraints).enumerate() {
                // Clone everything needed for the async block
                let builder = builder.partial_clone();
                let dist_constraints = dist_constraints.clone();
                let context_builder = (&context_builder).as_ref().map(|b| b.partial_clone());
                let executor = executor.clone();

                info!("Testing graph {}", i);

                let mut output_stream = Self::output_stream_for_graph(
                    builder,
                    dist_constraints.clone(),
                    input_vars.clone(),
                    output_vars.clone(),
                    &context_builder,
                    executor,
                    &graph,
                );

                let first_output: Vec<Value> = output_stream.next().await.unwrap();
                let dist_constraints_hold = output_vars.iter().zip(first_output).all(|(var_name, res)| {
                    match res {
                        Value::Bool(b) => {
                            // info!("True constraint");
                            b
                        },
                        _ => {
                            panic!("Unexpected value type for variable {}", var_name);
                        }
                    }
                });
                if dist_constraints_hold {
                    info!("Found matching graph!");
                    yield graph;
                }
            }
        })
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
                let labelled_dist_graph_stream = centralised_dist_graph_stream(
                    var_names,
                    dist_graph_provider.central_node.clone(),
                    dist_graph_provider.dist_graph_stream(),
                );
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
                let labelled_dist_graph_stream =
                    random_dist_graph_stream(var_names, dist_graph_provider.dist_graph_stream());
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
                let mut dist_graph_stream = dist_graph_provider.dist_graph_stream();
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

                let builder = self.partial_clone();
                let context_builder = self.context_builder.as_ref().map(|b| b.partial_clone());
                let executor_clone = executor.clone();
                let labelled_dist_graph_stream: OutputStream<LabelledDistributionGraph> = Box::pin(
                    stream! {
                        let Some(initial_graph) = dist_graph_stream.next().await else {
                            panic!("No initial graph received")
                        };

                        info!("Initial dist graph stream {:?}", initial_graph);

                        let mut labelled_dist_graphs: LocalBoxStream<LabelledDistributionGraph> =
                            Self::possible_labelled_dist_graph_stream(builder, initial_graph,
                            dist_constraints.clone(), input_vars.clone(), output_vars.clone(),
                            context_builder, executor_clone);

                        let chosen_dist_graph: LabelledDistributionGraph = match labelled_dist_graphs.next().await {
                            Some(dist_graph) => dist_graph,
                            None => return,
                        };

                        yield chosen_dist_graph.clone();
                        info!("Labelled optimized graph: {:?}", chosen_dist_graph);
                        graph_to_png(chosen_dist_graph.clone(), "distributed_graph.png").await.unwrap();

                        while let Some(_) = dist_graph_stream.next().await {
                            yield chosen_dist_graph.clone();
                        }
                    },
                );

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
        let scheduler = Rc::new(RefCell::new(Some(Scheduler::new(
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
    pub(crate) scheduler: Scheduler,
}

fn centralised_dist_graph_stream(
    var_names: Vec<VarName>,
    central_node: NodeName,
    mut dist_graph_stream: OutputStream<Rc<DistributionGraph>>,
) -> OutputStream<LabelledDistributionGraph> {
    info!("Starting centralised_dist_graph_stream");
    Box::pin(async_stream::stream! {
        while let Some(graph) = dist_graph_stream.next().await {
            let labels = graph
                .graph
                .node_indices()
                .map(|i| {
                    let node = graph.graph.node_weight(i).unwrap();
                    (i.clone(),
                     if *node == central_node {
                        var_names.clone()
                     } else {
                        vec![]
                     })
                })
                .collect::<BTreeMap<_, _>>();
            let labelled_graph = LabelledDistributionGraph {
                dist_graph: graph,
                var_names: var_names.clone(),
                node_labels: labels,
            };
            info!("Labelled graph: {:?}", labelled_graph);
            graph_to_png(labelled_graph.clone(), "distributed_graph.png").await.unwrap();
            yield labelled_graph;
        }
    })
}

fn random_dist_graph_stream(
    var_names: Vec<VarName>,
    mut dist_graph_stream: OutputStream<Rc<DistributionGraph>>,
) -> OutputStream<LabelledDistributionGraph> {
    info!("Starting random distribution graph stream");
    Box::pin(async_stream::stream! {
        while let Some(dist_graph) = dist_graph_stream.next().await {
            info!("Received distribution graph: generating random labelling");
            let node_indicies = dist_graph.graph.node_indices().collect::<Vec<_>>();
            let location_map: BTreeMap<VarName, NodeIndex> = var_names.iter()
                .map(|var| {
                    let location_index: usize = rand::random_range(0..node_indicies.len());
                    assert!(location_index < node_indicies.len());
                    (var.clone(), node_indicies[location_index].clone())
                }).collect();

            let node_labels: BTreeMap<NodeIndex, Vec<VarName>> = dist_graph.graph
                .node_indices()
                .map(|idx| (idx,
                    var_names.iter().filter(|var| location_map[var] == idx).cloned().collect()))
                .collect();
            info!("Generated random labelling: {:?}", node_labels);

            let labelled_graph = LabelledDistributionGraph {
                dist_graph,
                var_names: var_names.clone(),
                node_labels,
            };
            info!("Labelled graph: {:?}", labelled_graph);
            graph_to_png(labelled_graph.clone(), "distributed_graph.png").await.unwrap();
            yield labelled_graph;
        }
    })
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
