use std::rc::Rc;

use async_trait::async_trait;
use petgraph::visit::GraphRef;
use smol::{LocalExecutor, stream::repeat};

use crate::{
    Monitor, MonitoringSemantics, OutputStream, Specification, StreamContext,
    core::{AbstractContextBuilder, AbstractMonitorBuilder, OutputHandler, StreamData},
    dep_manage::interface::DependencyManager,
    distributed::distribution_graphs::LabelledDistributionGraph,
    semantics::distributed::combinators::{DistributedContext, DistributedContextBuilder},
};

use super::asynchronous::{AbstractAsyncMonitorBuilder, AsyncMonitorBuilder, AsyncMonitorRunner};

pub struct DistAsyncMonitorBuilder<
    M: Specification<Expr = Expr>,
    Ctx: StreamContext<V>,
    V: StreamData,
    Expr,
    S: MonitoringSemantics<Expr, V, Ctx>,
> {
    async_monitor_builder: AsyncMonitorBuilder<M, Ctx, V, Expr, S>,
    context_builder: Option<Ctx::Builder>,
    static_dist_graph: Option<LabelledDistributionGraph>,
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
        self.static_dist_graph = Some(graph);
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
            static_dist_graph: None,
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
        let static_dist_graph = self.static_dist_graph.expect("Static dist graph stream");
        let dist_graph_stream = Box::pin(repeat(static_dist_graph));
        let context_builder = self
            .context_builder
            .unwrap_or(DistributedContextBuilder::new().graph_stream(dist_graph_stream));
        let async_monitor = self
            .async_monitor_builder
            .context_builder(context_builder)
            .build();
        DistributedMonitorRunner { async_monitor }
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
    async_monitor: AsyncMonitorRunner<Expr, Val, S, M, DistributedContext<Val>>,
    // TODO: should we be responsible for building the stream of graphs
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
