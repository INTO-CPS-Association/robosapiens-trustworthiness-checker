use std::{collections::BTreeMap, rc::Rc};

use async_trait::async_trait;
use petgraph::graph::NodeIndex;
use smol::{
    LocalExecutor,
    stream::{StreamExt, repeat},
};
use tracing::{debug, info};

use crate::{
    Monitor, MonitoringSemantics, OutputStream, Specification, StreamContext, VarName,
    core::{AbstractContextBuilder, AbstractMonitorBuilder, OutputHandler, Runnable, StreamData},
    dep_manage::interface::DependencyManager,
    distributed::distribution_graphs::{
        DistributionGraph, LabelledDistributionGraph, NodeName, graph_to_png,
    },
    io::mqtt::dist_graph_provider::{self, MQTTDistGraphProvider},
    semantics::distributed::combinators::{DistributedContext, DistributedContextBuilder},
};

use super::asynchronous::{AbstractAsyncMonitorBuilder, AsyncMonitorBuilder, AsyncMonitorRunner};

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

pub struct DistAsyncMonitorBuilder<
    M: Specification<Expr = Expr>,
    Ctx: StreamContext<V>,
    V: StreamData,
    Expr,
    S: MonitoringSemantics<Expr, V, Ctx>,
> {
    async_monitor_builder: AsyncMonitorBuilder<M, Ctx, V, Expr, S>,
    context_builder: Option<Ctx::Builder>,
    dist_graph_mode: Option<DistGraphMode>,
}

impl<
    M: Specification<Expr = Expr>,
    Ctx: StreamContext<V>,
    V: StreamData,
    S: MonitoringSemantics<Expr, V, Ctx>,
    Expr,
> DistAsyncMonitorBuilder<M, Ctx, V, Expr, S>
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
}

impl<
    M: Specification<Expr = Expr>,
    Val: StreamData,
    S: MonitoringSemantics<Expr, Val, DistributedContext<Val>>,
    Expr,
> AbstractAsyncMonitorBuilder<M, DistributedContext<Val>, Val>
    for DistAsyncMonitorBuilder<M, DistributedContext<Val>, Val, Expr, S>
{
    fn context_builder(mut self, context_builder: DistributedContextBuilder<Val>) -> Self {
        self.context_builder = Some(context_builder);
        self
    }
}

impl<Expr, Val, S, M> AbstractMonitorBuilder<M, Val>
    for DistAsyncMonitorBuilder<M, DistributedContext<Val>, Val, Expr, S>
where
    Val: StreamData,
    S: MonitoringSemantics<Expr, Val, DistributedContext<Val>>,
    M: Specification<Expr = Expr>,
{
    type Mon = DistributedMonitorRunner<Expr, Val, S, M>;

    fn new() -> Self {
        DistAsyncMonitorBuilder {
            async_monitor_builder: AsyncMonitorBuilder::new(),
            context_builder: None,
            dist_graph_mode: None,
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

    fn input(mut self, input: Box<dyn crate::InputProvider<Val = Val>>) -> Self {
        self.async_monitor_builder = self.async_monitor_builder.input(input);
        self
    }

    fn output(mut self, output: Box<dyn OutputHandler<Val = Val>>) -> Self {
        self.async_monitor_builder = self.async_monitor_builder.output(output);
        self
    }

    fn dependencies(self, _dependencies: DependencyManager) -> Self {
        self
    }

    fn build(self) -> Self::Mon {
        let dist_graph_mode = self.dist_graph_mode.expect("Dist graph mode not set");
        let executor = self
            .async_monitor_builder
            .executor
            .as_ref()
            .expect("Executor");
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
                let var_names = self
                    .async_monitor_builder
                    .model
                    .as_ref()
                    .unwrap()
                    .var_names();
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
                let var_names = self
                    .async_monitor_builder
                    .model
                    .as_ref()
                    .unwrap()
                    .var_names();
                let labelled_dist_graph_stream =
                    random_dist_graph_stream(var_names, dist_graph_provider.dist_graph_stream());
                let location_names = locations.keys().cloned().collect::<Vec<_>>();

                (
                    labelled_dist_graph_stream,
                    location_names,
                    Some(dist_graph_provider),
                )
            }
            DistGraphMode::MQTTStaticOptimized(_locations, _dist_constraints) => {
                todo!("Unimplemented")
            }
        };
        let context_builder = self
            .context_builder
            .unwrap_or(DistributedContextBuilder::new().graph_stream(dist_graph_stream))
            .node_names(node_names);
        let async_monitor = self
            .async_monitor_builder
            .context_builder(context_builder)
            .build();
        DistributedMonitorRunner {
            async_monitor,
            dist_graph_provider,
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
}

fn centralised_dist_graph_stream(
    var_names: Vec<VarName>,
    central_node: NodeName,
    mut dist_graph_stream: OutputStream<DistributionGraph>,
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
    mut dist_graph_stream: OutputStream<DistributionGraph>,
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
        self.async_monitor.run().await;
    }

    async fn run(self: Self) {
        self.async_monitor.run().await;
    }
}
