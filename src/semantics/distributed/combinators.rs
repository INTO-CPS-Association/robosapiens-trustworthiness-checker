use std::any::Any;
use std::cell::RefCell;
use std::mem;
use std::rc::Rc;

use crate::VarName;
use crate::core::{OutputStream, StreamContext, SyncStreamContext};
use crate::core::{StreamData, Value};
use crate::distributed::distribution_graphs::{LabelledDistributionGraph, NodeName};
use crate::runtime::asynchronous::{Context as AsyncCtx, VarManager};
use async_stream::stream;
use async_trait::async_trait;
use futures::StreamExt;
use smol::LocalExecutor;

impl StreamData for LabelledDistributionGraph {}

pub struct DistributedContext {
    ctx: AsyncCtx<Value>,
    /// Essentially a shared_ptr that we can at some time take ownership of
    graph_manager: Rc<RefCell<Option<VarManager<LabelledDistributionGraph>>>>,
    executor: Rc<LocalExecutor<'static>>,
}

impl StreamContext<Value> for DistributedContext {
    fn var(&self, x: &VarName) -> Option<OutputStream<Value>> {
        self.ctx.var(&x)
    }

    fn subcontext(&self, history_length: usize) -> Box<dyn crate::core::SyncStreamContext<Value>> {
        self.ctx.subcontext(history_length)
    }

    fn restricted_subcontext(
        &self,
        vs: ecow::EcoVec<VarName>,
        history_length: usize,
    ) -> Box<dyn crate::core::SyncStreamContext<Value>> {
        self.ctx.restricted_subcontext(vs, history_length)
    }
}

#[async_trait(?Send)]
impl SyncStreamContext<Value> for DistributedContext {
    async fn advance_clock(&mut self) {
        self.ctx.advance_clock().await;
        // Tick the graph_manager
        self.graph_manager
            .borrow_mut()
            .as_mut()
            .unwrap()
            .tick()
            .await;
    }

    async fn lazy_advance_clock(&mut self) {
        self.ctx.lazy_advance_clock().await;
        // Should be done lazily - but we don't care
        self.graph_manager
            .borrow_mut()
            .as_mut()
            .unwrap()
            .tick()
            .await;
    }

    async fn start_auto_clock(&mut self) {
        if !self.ctx.is_clock_started() {
            self.ctx.start_auto_clock().await;
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

    fn upcast(&self) -> &dyn StreamContext<Value> {
        self.ctx.upcast()
    }
}

impl DistributedContext {
    const GRAPH_NAME: &'static str = "graph";

    #[allow(unused)]
    fn new(
        executor: Rc<LocalExecutor<'static>>,
        var_names: Vec<VarName>,
        input_streams: Vec<OutputStream<Value>>,
        history_length: usize,
        graph_stream: OutputStream<LabelledDistributionGraph>,
    ) -> Self {
        let ctx = AsyncCtx::new(executor.clone(), var_names, input_streams, history_length);
        let graph_manager = Rc::new(RefCell::new(Some(VarManager::new(
            executor.clone(),
            VarName::new(Self::GRAPH_NAME),
            graph_stream,
        ))));
        DistributedContext {
            ctx,
            graph_manager,
            executor,
        }
    }

    // Same as Ctx.var() but gives the graph
    fn graph(&self) -> Option<OutputStream<LabelledDistributionGraph>> {
        if self.is_clock_started() {
            panic!("Cannot request a stream after the clock has started");
        }

        let mut var_manager = self.graph_manager.borrow_mut();
        let var_manager = var_manager.as_mut().unwrap();

        Some(var_manager.subscribe())
    }
}

pub fn monitored_at(
    var_name: VarName,
    label: NodeName,
    ctx: &DistributedContext,
) -> OutputStream<Value> {
    let mut graph_stream = ctx.graph().unwrap();

    Box::pin(stream! {
        loop {
            if let Some(graph) = graph_stream.next().await {
                let idx = graph.get_node_index_by_name(&label).expect("Label not inside graph");
                let res = graph.node_labels
                    .get(&idx)
                    .is_some_and(|vec| vec.iter().any(|name| *name == var_name));
                yield Value::Bool(res);
            }
            else {
                break;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{
        core::{SyncStreamContext, Value},
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
        let mut ctx = DistributedContext::new(
            executor.clone(),
            vec!["x".into()],
            vec![x],
            10,
            graph_stream,
        );
        let exp = vec![Value::Int(2), Value::Int(4)];
        let res_stream =
            crate::semantics::untimed_untyped_lola::combinators::dynamic(&ctx, e, None, 10);
        ctx.start_auto_clock().await;
        let res: Vec<Value> = res_stream.collect().await;
        assert_eq!(res, exp);
    }

    #[test(apply(smol_test))]
    async fn test_monitor_at_stream(executor: Rc<LocalExecutor<'static>>) {
        // Just a little test to check that we can do our tests... :-)
        let x = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
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

        let mut ctx = DistributedContext::new(
            executor.clone(),
            vec!["x".into(), "y".into(), "z".into()],
            vec![x, y, z],
            10,
            graph_stream,
        );

        let res_x = monitored_at("x".into(), "B".into(), &ctx);
        ctx.start_auto_clock().await;
        let res_x: Vec<_> = res_x.take(3).collect().await;

        assert_eq!(res_x, vec![true.into(), true.into(), true.into()]);
    }
}
