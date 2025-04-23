use std::{collections::BTreeMap, rc::Rc};

use async_trait::async_trait;
use smol::{
    LocalExecutor,
    stream::{StreamExt, repeat},
};
use tracing::debug;

use crate::{
    Monitor, MonitoringSemantics, OutputStream, Specification, StreamContext, VarName,
    core::{AbstractContextBuilder, AbstractMonitorBuilder, OutputHandler, StreamData},
    dep_manage::interface::DependencyManager,
    distributed::distribution_graphs::{
        DistributionGraph, GenericLabelledDistributionGraph, LabelledDistributionGraph, NodeName,
    },
    io::mqtt::dist_graph_provider::{self, MQTTDistGraphProvider},
    semantics::distributed::combinators::{DistributedContext, DistributedContextBuilder},
};

use super::asynchronous::{AbstractAsyncMonitorBuilder, AsyncMonitorBuilder, AsyncMonitorRunner};

pub enum DistGraphMode {
    Static(LabelledDistributionGraph),
    MQTTCentralised(BTreeMap<NodeName, String>),
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
        let dist_graph_mode = self.dist_graph_mode.expect("Dist graph mode");
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
                let mut dist_graph_provider = dist_graph_provider::MQTTDistGraphProvider::new(
                    executor.clone(),
                    "central_node".to_string().into(),
                    locations.clone(),
                )
                .expect("Failed to create MQTT dist graph provider");
                let labelled_dist_graph_stream = centralised_dist_graph_stream(
                    self.async_monitor_builder
                        .model
                        .as_ref()
                        .unwrap()
                        .var_names(),
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
    pub(crate) dist_graph_provider:
        Option<crate::io::mqtt::dist_graph_provider::MQTTDistGraphProvider>,
}

fn centralised_dist_graph_stream(
    var_names: Vec<VarName>,
    central_node: NodeName,
    mut dist_graph_stream: OutputStream<DistributionGraph>,
) -> OutputStream<LabelledDistributionGraph> {
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
            debug!("Labelled graph: {:?}", labelled_graph);
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

    async fn run(self) {
        self.async_monitor.run().await;
    }
}
