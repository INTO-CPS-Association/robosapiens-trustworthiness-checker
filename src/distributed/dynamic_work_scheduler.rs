use std::cell::OnceCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use crate::InputProvider;
use crate::OutputStream;
use crate::Specification;
use crate::Value;
use crate::VarName;
use crate::core::AbstractMonitorBuilder;
use crate::core::Runnable;
use crate::distributed::distribution_graphs::graph_to_png;
use crate::distributed::distribution_graphs::possible_labelled_dist_graphs;
use crate::io::testing::ManualOutputHandler;
use crate::runtime::asynchronous::AbstractAsyncMonitorBuilder;
use crate::runtime::distributed::DistAsyncMonitorBuilder;
use crate::semantics::AbstractContextBuilder;
use crate::semantics::MonitoringSemantics;
use crate::semantics::distributed::contexts::DistributedContext;
use crate::semantics::distributed::contexts::DistributedContextBuilder;
use crate::semantics::distributed::localisation::Localisable;
use async_stream::stream;
use async_trait::async_trait;
use futures::stream::LocalBoxStream;
use petgraph::graph::NodeIndex;
use smol::LocalExecutor;
use smol::stream::StreamExt;
use smol::stream::repeat;
use tracing::debug;
use tracing::info;

use super::distribution_graphs::DistributionGraph;
use super::distribution_graphs::{LabelledDistributionGraph, NodeName};
use super::scheduling::SchedulerCommunicator;

pub struct NullSchedulerCommunicator;

#[async_trait(?Send)]
impl SchedulerCommunicator for NullSchedulerCommunicator {
    async fn schedule_work(
        &mut self,
        _node_name: NodeName,
        _work: Vec<VarName>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!("NullSchedulerCommunicator called");
        Ok(())
    }
}

#[async_trait(?Send)]
pub trait SchedulerPlanner {
    async fn plan(&self, graph: Rc<DistributionGraph>) -> Option<LabelledDistributionGraph>;
}

pub struct StaticFixedSchedulerPlanner {
    fixed_graph: LabelledDistributionGraph,
}

#[async_trait(?Send)]
impl SchedulerPlanner for StaticFixedSchedulerPlanner {
    async fn plan(&self, _graph: Rc<DistributionGraph>) -> Option<LabelledDistributionGraph> {
        Some(self.fixed_graph.clone())
    }
}

pub struct CentralisedSchedulerPlanner {
    pub var_names: Vec<VarName>,
    pub central_node: NodeName,
}

#[async_trait(?Send)]
impl SchedulerPlanner for CentralisedSchedulerPlanner {
    async fn plan(&self, graph: Rc<DistributionGraph>) -> Option<LabelledDistributionGraph> {
        let labels = graph
            .graph
            .node_indices()
            .map(|i| {
                let node = graph.graph.node_weight(i).unwrap();
                (
                    i.clone(),
                    if *node == self.central_node {
                        self.var_names.clone()
                    } else {
                        vec![]
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let labelled_graph = LabelledDistributionGraph {
            dist_graph: graph,
            var_names: self.var_names.clone(),
            node_labels: labels,
        };
        info!("Labelled graph: {:?}", labelled_graph);

        graph_to_png(labelled_graph.clone(), "distributed_graph.png")
            .await
            .unwrap();
        Some(labelled_graph)
    }
}

pub struct RandomSchedulerPlanner {
    pub var_names: Vec<VarName>,
}

#[async_trait(?Send)]
impl SchedulerPlanner for RandomSchedulerPlanner {
    async fn plan(&self, graph: Rc<DistributionGraph>) -> Option<LabelledDistributionGraph> {
        info!("Received distribution graph: generating random labelling");
        let node_indicies = graph.graph.node_indices().collect::<Vec<_>>();
        let location_map: BTreeMap<VarName, NodeIndex> = self
            .var_names
            .iter()
            .map(|var| {
                let location_index: usize = rand::random_range(0..node_indicies.len());
                assert!(location_index < node_indicies.len());
                (var.clone(), node_indicies[location_index].clone())
            })
            .collect();

        let node_labels: BTreeMap<NodeIndex, Vec<VarName>> = graph
            .graph
            .node_indices()
            .map(|idx| {
                (
                    idx,
                    self.var_names
                        .iter()
                        .filter(|var| location_map[var] == idx)
                        .cloned()
                        .collect(),
                )
            })
            .collect();
        info!("Generated random labelling: {:?}", node_labels);

        let labelled_graph = LabelledDistributionGraph {
            dist_graph: graph,
            var_names: self.var_names.clone(),
            node_labels,
        };
        info!("Labelled graph: {:?}", labelled_graph);
        graph_to_png(labelled_graph.clone(), "distributed_graph.png")
            .await
            .unwrap();

        Some(labelled_graph)
    }
}

pub struct BruteForceDistConstraintSolver<Expr, S, M>
where
    S: MonitoringSemantics<Expr, Value, DistributedContext<Value>>,
    M: Specification<Expr = Expr> + Localisable,
{
    pub executor: Rc<LocalExecutor<'static>>,
    pub monitor_builder: DistAsyncMonitorBuilder<M, DistributedContext<Value>, Value, Expr, S>,
    pub context_builder: Option<DistributedContextBuilder<Value>>,
    pub dist_constraints: Vec<VarName>,
    pub input_vars: Vec<VarName>,
    pub output_vars: Vec<VarName>,
}

impl<Expr, S, M> BruteForceDistConstraintSolver<Expr, S, M>
where
    Expr: 'static,
    S: MonitoringSemantics<Expr, Value, DistributedContext<Value>>,
    M: Specification<Expr = Expr> + Localisable,
{
    fn output_stream_for_graph(
        &self,
        monitor_builder: DistAsyncMonitorBuilder<M, DistributedContext<Value>, Value, Expr, S>,
        labelled_graph: Rc<LabelledDistributionGraph>,
    ) -> OutputStream<Vec<Value>> {
        // Build a runtime for constructing the output stream
        debug!(
            "Output stream for graph with input_vars: {:?} and output_vars: {:?}",
            self.input_vars, self.output_vars
        );
        let input_provider: Box<dyn InputProvider<Val = Value>> =
            Box::new(BTreeMap::<VarName, OutputStream<Value>>::new());
        let mut output_handler =
            ManualOutputHandler::new(self.executor.clone(), self.dist_constraints.clone());
        let output_stream: OutputStream<Vec<Value>> = Box::pin(output_handler.get_output());
        let potential_dist_graph_stream = Box::pin(repeat((*labelled_graph).clone()));
        let context_builder = self
            .context_builder
            .as_ref()
            .map(|b| b.partial_clone())
            .unwrap_or(DistributedContextBuilder::new().graph_stream(potential_dist_graph_stream));
        let runtime = monitor_builder
            .context_builder(context_builder)
            .static_dist_graph((*labelled_graph).clone())
            .input(input_provider)
            .output(Box::new(output_handler))
            .build();
        self.executor.spawn(runtime.run()).detach();

        // Construct a wrapped output stream which makes sure the context starts
        output_stream
    }

    /// Finds all possible labelled distribution graphs given a set of distribution constraints
    /// and a distribution graph
    fn possible_labelled_dist_graph_stream(
        self: Rc<Self>,
        // builder: Self,
        graph: Rc<DistributionGraph>,
        // dist_constraints: Vec<VarName>,
        // input_vars: Vec<VarName>,
        // output_vars: Vec<VarName>,
        // context_builder: Option<DistributedContextBuilder<Value>>,
        // executor: Rc<LocalExecutor<'static>>,
    ) -> OutputStream<Rc<LabelledDistributionGraph>> {
        // To avoid lifetime and move issues, clone all necessary data for the async block.
        let dist_constraints = self.dist_constraints.clone();
        let builder = self.monitor_builder.partial_clone();
        let non_dist_constraints: Vec<VarName> = self
            .output_vars
            .iter()
            .filter(|name| !dist_constraints.contains(name))
            .cloned()
            .collect();
        let localised_spec = self
            .monitor_builder
            .async_monitor_builder
            .model
            .as_ref()
            .unwrap()
            .localise(&dist_constraints);
        let builder = builder.model(localised_spec);

        info!("Starting optimized distributed graph generation");

        Box::pin(async_stream::stream! {
            for (i, labelled_graph) in possible_labelled_dist_graphs(graph, dist_constraints.clone(), non_dist_constraints).enumerate() {
                // Clone everything needed for the async block
                let labelled_graph = Rc::new(labelled_graph);
                let builder = builder.partial_clone();

                info!("Testing graph {}", i);

                let mut output_stream = self.output_stream_for_graph(
                    builder,
                    labelled_graph.clone(),
                );

                let first_output: Vec<Value> = output_stream.next().await.unwrap();
                let dist_constraints_hold = first_output.iter().all(|x| match x {
                    Value::Bool(b) => *b,
                    x => {
                        panic!("Unexpected value for dist constraint {:?}", x);
                    }
                });
                if dist_constraints_hold {
                    info!("Found matching graph!");
                    yield labelled_graph;
                }
            }
        })
    }
}

pub struct StaticOptimizedSchedulerPlanner<Expr, S, M>
where
    S: MonitoringSemantics<Expr, Value, DistributedContext<Value>>,
    M: Specification<Expr = Expr> + Localisable,
{
    solver: Rc<BruteForceDistConstraintSolver<Expr, S, M>>,
    chosen_dist_graph: OnceCell<Rc<LabelledDistributionGraph>>,
}

impl<Expr, S, M> StaticOptimizedSchedulerPlanner<Expr, S, M>
where
    Expr: 'static,
    S: MonitoringSemantics<Expr, Value, DistributedContext<Value>>,
    M: Specification<Expr = Expr> + Localisable,
{
    pub fn new(solver: BruteForceDistConstraintSolver<Expr, S, M>) -> Self {
        Self {
            solver: Rc::new(solver),
            chosen_dist_graph: OnceCell::new(),
        }
    }
}

#[async_trait(?Send)]
impl<Expr, S, M> SchedulerPlanner for StaticOptimizedSchedulerPlanner<Expr, S, M>
where
    Expr: 'static,
    S: MonitoringSemantics<Expr, Value, DistributedContext<Value>>,
    M: Specification<Expr = Expr> + Localisable,
{
    async fn plan(&self, graph: Rc<DistributionGraph>) -> Option<LabelledDistributionGraph> {
        if let Some(chosen_dist_graph) = self.chosen_dist_graph.get() {
            return Some((**chosen_dist_graph).clone());
        }

        info!("Initial dist graph stream {:?}", graph);

        let mut labelled_dist_graphs: LocalBoxStream<Rc<LabelledDistributionGraph>> = self
            .solver
            .clone()
            .possible_labelled_dist_graph_stream(graph);

        let chosen_dist_graph: Rc<LabelledDistributionGraph> =
            match labelled_dist_graphs.next().await {
                Some(dist_graph) => dist_graph,
                None => return None,
            };

        self.chosen_dist_graph
            .set(chosen_dist_graph.clone())
            .unwrap();

        info!("Labelled optimized graph: {:?}", chosen_dist_graph);
        graph_to_png((*chosen_dist_graph).clone(), "distributed_graph.png")
            .await
            .unwrap();

        Some((*chosen_dist_graph).clone())
    }
}

pub fn planned_dist_graph_stream(
    mut dist_graphs: OutputStream<Rc<DistributionGraph>>,
    planner: Rc<Box<dyn SchedulerPlanner>>,
) -> OutputStream<LabelledDistributionGraph> {
    Box::pin(stream! {
        while let Some(dist_graph) = dist_graphs.next().await {
            let labelled_dist_graph = planner.plan(dist_graph).await.unwrap();
            yield labelled_dist_graph
        }
    })
}

pub struct SchedulerEnactor {
    executor: Rc<LocalExecutor<'static>>,
    dist_graph_stream: Option<OutputStream<LabelledDistributionGraph>>,
    communicator: Box<dyn SchedulerCommunicator>,
}

impl SchedulerEnactor {
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        communicator: Box<dyn SchedulerCommunicator>,
    ) -> Self {
        SchedulerEnactor {
            executor,
            dist_graph_stream: None,
            communicator,
        }
    }

    pub fn dist_graph_stream(
        &mut self,
        stream: OutputStream<LabelledDistributionGraph>,
    ) -> &mut Self {
        self.dist_graph_stream = Some(stream);
        self
    }

    pub fn run(self) {
        if let Some(mut stream) = self.dist_graph_stream {
            self.executor
                .spawn(async move {
                    let mut communicator = self.communicator;
                    while let Some(dist_graph) = stream.next().await {
                        let nodes = dist_graph.dist_graph.graph.node_indices();
                        // is this really the best way?
                        for node in nodes {
                            let node_name = dist_graph.dist_graph.graph[node].clone();
                            let work = dist_graph.node_labels[&node].clone();
                            info!("Scheduling work {:?} for node {}", work, node_name);
                            let _ = communicator.schedule_work(node_name, work).await;
                        }
                    }
                })
                .detach();
        };
    }
}
