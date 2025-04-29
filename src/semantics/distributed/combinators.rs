use std::cell::RefCell;
use std::mem;
use std::rc::Rc;

use crate::VarName;
use crate::core::{AbstractContextBuilder, OutputStream, StreamContext};
use crate::core::{StreamData, Value};
use crate::distributed::distribution_graphs::{
    Distance, LabelledDistributionGraph, NodeName, TaggedVarOrNodeName,
};
use crate::lang::dynamic_lola::ast::VarOrNodeName;
use crate::runtime::asynchronous::{Context as AsyncCtx, ContextBuilder, VarManager};
use async_stream::stream;
use async_trait::async_trait;
use futures::{StreamExt, join};
use smol::LocalExecutor;

impl StreamData for LabelledDistributionGraph {}

pub struct DistributedContextBuilder<Val: StreamData> {
    async_ctx_builder: ContextBuilder<Val>,
    async_ctx: Option<AsyncCtx<Val>>,
    nested_async_ctx: Option<AsyncCtx<Val>>,
    graph_name: Option<String>,
    node_names: Option<Vec<NodeName>>,
    graph_stream: Option<OutputStream<LabelledDistributionGraph>>,
    presupplied_ctx: Option<Box<DistributedContext<Val>>>,
}

impl<Val: StreamData> AbstractContextBuilder for DistributedContextBuilder<Val> {
    type Ctx = DistributedContext<Val>;
    type Val = Val;

    fn new() -> Self {
        Self {
            async_ctx_builder: ContextBuilder::new(),
            async_ctx: None,
            graph_stream: None,
            graph_name: None,
            node_names: None,
            presupplied_ctx: None,
            nested_async_ctx: None,
        }
    }

    fn executor(mut self, executor: Rc<LocalExecutor<'static>>) -> Self {
        self.async_ctx_builder = self.async_ctx_builder.executor(executor);
        self
    }

    fn var_names(mut self, var_names: Vec<VarName>) -> Self {
        self.async_ctx_builder = self.async_ctx_builder.var_names(var_names);
        self
    }

    fn input_streams(mut self, input_streams: Vec<OutputStream<Val>>) -> Self {
        self.async_ctx_builder = self.async_ctx_builder.input_streams(input_streams);
        self
    }

    fn history_length(mut self, history_length: usize) -> Self {
        self.async_ctx_builder = self.async_ctx_builder.history_length(history_length);
        self
    }

    fn partial_clone(&self) -> Self {
        Self {
            async_ctx_builder: self.async_ctx_builder.partial_clone(),
            async_ctx: None,
            graph_name: self.graph_name.clone(),
            graph_stream: None,
            node_names: self.node_names.clone(),
            presupplied_ctx: None,
            nested_async_ctx: None,
        }
    }

    fn build(self) -> DistributedContext<Val> {
        if let Some(presupplied_ctx) = self.presupplied_ctx {
            return *presupplied_ctx;
        }

        let builder = self.partial_clone();
        let ctx = match self.async_ctx {
            Some(ctx) => ctx,
            None => self
                .async_ctx_builder
                .maybe_nested(self.nested_async_ctx)
                .build(),
        };
        let executor = ctx.executor.clone();
        let graph_stream = self.graph_stream.unwrap();
        let graph_name = self.graph_name.unwrap_or("graph".into());
        let node_names = self.node_names.unwrap();
        let graph_manager = Rc::new(RefCell::new(Some(VarManager::new(
            ctx.executor.clone(),
            graph_name.into(),
            graph_stream,
        ))));
        DistributedContext {
            ctx,
            graph_manager,
            executor,
            node_names,
            builder,
        }
    }
}

impl<Val: StreamData> DistributedContextBuilder<Val> {
    pub fn graph_stream(mut self, graph_stream: OutputStream<LabelledDistributionGraph>) -> Self {
        self.graph_stream = Some(graph_stream);
        self
    }

    pub fn graph_name(mut self, graph_name: String) -> Self {
        self.graph_name = Some(graph_name);
        self
    }

    pub fn node_names(mut self, node_names: Vec<NodeName>) -> Self {
        self.node_names = Some(node_names);
        self
    }

    pub fn context(mut self, ctx: AsyncCtx<Val>) -> Self {
        self.async_ctx = Some(ctx);
        self
    }

    pub fn presupplied_ctx(mut self, ctx: DistributedContext<Val>) -> Self {
        self.presupplied_ctx = Some(Box::new(ctx));
        self
    }

    pub fn nested(mut self, ctx: AsyncCtx<Val>) -> Self {
        self.nested_async_ctx = Some(ctx);
        self
    }
}

pub struct DistributedContext<Val: StreamData> {
    ctx: AsyncCtx<Val>,
    /// Essentially a shared_ptr that we can at some time take ownership of
    node_names: Vec<NodeName>,
    graph_manager: Rc<RefCell<Option<VarManager<LabelledDistributionGraph>>>>,
    executor: Rc<LocalExecutor<'static>>,
    builder: DistributedContextBuilder<Val>,
}

#[async_trait(?Send)]
impl<Val: StreamData> StreamContext<Val> for DistributedContext<Val> {
    type Builder = DistributedContextBuilder<Val>;

    fn var(&self, x: &VarName) -> Option<OutputStream<Val>> {
        self.ctx.var(x)
    }

    fn subcontext(&self, history_length: usize) -> Self {
        self.builder
            .partial_clone()
            .context(self.ctx.subcontext(history_length))
            .graph_stream(
                self.graph_manager
                    .borrow_mut()
                    .as_mut()
                    .unwrap()
                    .subscribe(),
            )
            .build()
    }

    fn restricted_subcontext(&self, vs: ecow::EcoVec<VarName>, history_length: usize) -> Self {
        self.builder
            .partial_clone()
            .context(self.ctx.restricted_subcontext(vs, history_length))
            .graph_stream(
                self.graph_manager
                    .borrow_mut()
                    .as_mut()
                    .unwrap()
                    .subscribe(),
            )
            .build()
    }

    async fn tick(&mut self) {
        join!(
            self.ctx.tick(),
            self.graph_manager.borrow_mut().as_mut().unwrap().tick()
        );
    }

    async fn run(&mut self) {
        if !self.ctx.is_clock_started() {
            self.ctx.run().await;
            let graph_manager = mem::take(&mut *self.graph_manager.borrow_mut()).unwrap();
            self.executor.spawn(graph_manager.run()).detach();
        }
    }

    fn is_clock_started(&self) -> bool {
        self.ctx.is_clock_started()
    }

    fn clock(&self) -> usize {
        self.ctx.clock()
    }
}

impl<Val: StreamData> DistributedContext<Val> {
    fn graph(&self) -> Option<OutputStream<LabelledDistributionGraph>> {
        if self.is_clock_started() {
            panic!("Cannot request a stream after the clock has started");
        }

        let mut var_manager = self.graph_manager.borrow_mut();
        let var_manager = var_manager.as_mut().unwrap();

        Some(var_manager.subscribe())
    }

    fn disambiguate_name(&self, name: VarOrNodeName) -> Option<TaggedVarOrNodeName> {
        if self
            .node_names
            .iter()
            .any(|n| VarOrNodeName((*n).to_string()) == name)
        {
            Some(TaggedVarOrNodeName::NodeName(NodeName::new(name)))
        } else if self
            .ctx
            .var_names()
            .iter()
            .any(|n| VarOrNodeName((*n).to_string()) == name)
        {
            Some(TaggedVarOrNodeName::VarName(name.into()))
        } else {
            None
        }
    }
}

pub fn monitored_at<Val: StreamData>(
    var_name: VarName,
    node_name: NodeName,
    ctx: &DistributedContext<Val>,
) -> OutputStream<Value> {
    let mut graph_stream = ctx.graph().unwrap();

    Box::pin(stream! {
        while let Some(graph) = graph_stream.next().await {
            let idx = graph.get_node_index_by_name(&node_name).expect("Label not inside graph");
            let res = graph.node_labels
                .get(&idx)
                .is_some_and(|vec| vec.iter().any(|name| *name == var_name));
            yield Value::Bool(res);
        }
    })
}

pub fn dist<Val: StreamData>(
    u: VarOrNodeName,
    v: VarOrNodeName,
    ctx: &DistributedContext<Val>,
) -> OutputStream<Value> {
    let u = ctx
        .disambiguate_name(u)
        .expect("Could not find node or variable in the graph");
    let v = ctx
        .disambiguate_name(v)
        .expect("Could not find node or variable in the graph");
    let mut graph_stream = ctx.graph().unwrap();

    // TODO: Think about a better way to handle disconnected nodes
    // This hack should work for now as we actually do expect the nodes to be
    // connected
    Box::pin(stream! {
        while let Some(graph) = graph_stream.next().await {
            let res = graph.dist(u.clone(), v.clone());
            yield Value::Int(res.map(|x| x.try_into().unwrap()).unwrap_or(i64::MAX));
        }
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{
        core::{StreamContext, Value},
        distributed::distribution_graphs::DistributionGraph,
    };
    use futures::stream;
    use macro_rules_attribute::apply;
    use petgraph::graph::DiGraph;
    use smol_macros::test as smol_test;
    use test_log::test;

    #[test(apply(smol_test))]
    async fn test_that_test_can_test(executor: Rc<LocalExecutor<'static>>) {
        // Just a little test to check that we can do our tests... :-)
        let e: OutputStream<Value> = Box::pin(stream::iter(vec!["x + 1".into(), "x + 2".into()]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let graph_stream = Box::pin(stream::iter(vec![]));
        let mut ctx = DistributedContextBuilder::new()
            .executor(executor.clone())
            .var_names(vec!["x".into()])
            .input_streams(vec![x])
            .history_length(10)
            .graph_stream(graph_stream)
            .node_names(vec!["A".into(), "B".into(), "C".into()])
            .build();
        let exp = vec![Value::Int(2), Value::Int(4)];
        let res_stream =
            crate::semantics::untimed_untyped_lola::combinators::dynamic(&ctx, e, None, 10);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        assert_eq!(res, exp);
    }

    #[test(apply(smol_test))]
    async fn test_monitor_at_stream(executor: Rc<LocalExecutor<'static>>) {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let y = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let z = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));

        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        let c = graph.add_node("C".into());
        graph.add_edge(a, b, 0);
        graph.add_edge(b, c, 0);
        let dist_graph = DistributionGraph {
            central_monitor: a,
            graph,
        };
        let labelled_graph = LabelledDistributionGraph {
            dist_graph,
            var_names: vec!["x".into(), "y".into(), "z".into()],
            node_labels: BTreeMap::from([
                (a, vec![]),
                (b, vec!["x".into()]),
                (c, vec!["y".into(), "z".into()]),
            ]),
        };

        let graph_stream = Box::pin(stream::repeat(labelled_graph));

        let mut ctx = DistributedContextBuilder::new()
            .executor(executor.clone())
            .var_names(vec!["x".into(), "y".into(), "z".into()])
            .input_streams(vec![x, y, z])
            .history_length(10)
            .graph_stream(graph_stream)
            .node_names(vec!["A".into(), "B".into(), "C".into()])
            .build();

        let res_x = monitored_at("x".into(), "B".into(), &ctx);
        ctx.run().await;
        let res_x: Vec<_> = res_x.take(3).collect().await;

        assert_eq!(res_x, vec![true.into(), true.into(), true.into()]);
    }

    #[test(apply(smol_test))]
    async fn test_dist_stream_nodes(executor: Rc<LocalExecutor<'static>>) {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let y = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let z = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));

        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        let c = graph.add_node("C".into());
        graph.add_edge(a, b, 1);
        graph.add_edge(b, c, 1);
        let dist_graph = DistributionGraph {
            central_monitor: a,
            graph,
        };
        let labelled_graph = LabelledDistributionGraph {
            dist_graph,
            var_names: vec!["x".into(), "y".into(), "z".into()],
            node_labels: BTreeMap::from([
                (a, vec![]),
                (b, vec!["x".into()]),
                (c, vec!["y".into(), "z".into()]),
            ]),
        };

        let graph_stream = Box::pin(stream::repeat(labelled_graph));

        let mut ctx = DistributedContextBuilder::new()
            .executor(executor.clone())
            .var_names(vec!["x".into(), "y".into(), "z".into()])
            .input_streams(vec![x, y, z])
            .history_length(10)
            .graph_stream(graph_stream)
            .node_names(vec!["A".into(), "B".into(), "C".into()])
            .build();

        let res = dist(VarOrNodeName("A".into()), VarOrNodeName("C".into()), &ctx);
        ctx.run().await;
        let res: Vec<_> = res.take(3).collect().await;

        assert_eq!(res, vec![2.into(), 2.into(), 2.into()]);
    }

    #[test(apply(smol_test))]
    async fn test_dist_stream_var_node(executor: Rc<LocalExecutor<'static>>) {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let y = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let z = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));

        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        let c = graph.add_node("C".into());
        graph.add_edge(a, b, 1);
        graph.add_edge(b, c, 1);
        let dist_graph = DistributionGraph {
            central_monitor: a,
            graph,
        };
        let labelled_graph = LabelledDistributionGraph {
            dist_graph,
            var_names: vec!["x".into(), "y".into(), "z".into()],
            node_labels: BTreeMap::from([
                (a, vec![]),
                (b, vec!["x".into()]),
                (c, vec!["y".into(), "z".into()]),
            ]),
        };

        let graph_stream = Box::pin(stream::repeat(labelled_graph));

        let mut ctx = DistributedContextBuilder::new()
            .executor(executor.clone())
            .var_names(vec!["x".into(), "y".into(), "z".into()])
            .input_streams(vec![x, y, z])
            .history_length(10)
            .graph_stream(graph_stream)
            .node_names(vec!["A".into(), "B".into(), "C".into()])
            .build();

        let res = dist(VarOrNodeName("x".into()), VarOrNodeName("C".into()), &ctx);
        ctx.run().await;
        let res: Vec<_> = res.take(3).collect().await;

        assert_eq!(res, vec![1.into(), 1.into(), 1.into()]);
    }

    #[test(apply(smol_test))]
    async fn test_disambiguate_name(executor: Rc<LocalExecutor<'static>>) {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let y = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let z = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));

        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        let c = graph.add_node("C".into());
        graph.add_edge(a, b, 1);
        graph.add_edge(b, c, 1);
        let dist_graph = DistributionGraph {
            central_monitor: a,
            graph,
        };
        let labelled_graph = LabelledDistributionGraph {
            dist_graph,
            var_names: vec!["x".into(), "y".into(), "z".into()],
            node_labels: BTreeMap::from([
                (a, vec![]),
                (b, vec!["x".into()]),
                (c, vec!["y".into(), "z".into()]),
            ]),
        };

        let graph_stream = Box::pin(stream::repeat(labelled_graph));

        let ctx = DistributedContextBuilder::new()
            .executor(executor.clone())
            .var_names(vec!["x".into(), "y".into(), "z".into()])
            .input_streams(vec![x, y, z])
            .history_length(10)
            .graph_stream(graph_stream)
            .node_names(vec!["A".into(), "B".into(), "C".into()])
            .build();

        let name = ctx.disambiguate_name(VarOrNodeName("x".into()));
        assert_eq!(name, Some(TaggedVarOrNodeName::VarName("x".into())));
        let name = ctx.disambiguate_name(VarOrNodeName("A".into()));
        assert_eq!(
            name,
            Some(TaggedVarOrNodeName::NodeName(NodeName::new("A")))
        );
        let name = ctx.disambiguate_name(VarOrNodeName("D".into()));
        assert_eq!(name, None);
    }
}
