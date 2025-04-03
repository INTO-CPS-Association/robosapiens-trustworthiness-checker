pub mod localisation;

use std::any::Any;
use std::cell::RefCell;
use std::mem;
use std::rc::Rc;

use super::UntimedLolaSemantics;
use crate::VarName;
use crate::core::{MonitoringSemantics, OutputStream, StreamContext, SyncStreamContext};
use crate::core::{StreamData, Value};
use crate::distributed::distribution_graphs::{LabelledDistributionGraph, NodeName};
use crate::lang::dynamic_lola::ast::SExpr;
use crate::runtime::asynchronous::{Context as AsyncCtx, VarManager};
use async_stream::stream;
use async_trait::async_trait;
use futures::StreamExt;
use smol::LocalExecutor;

#[derive(Clone)]
pub struct DistributedSemantics;

pub enum DistSExpr {
    SExpr(SExpr),
    MonitoredAt(VarName, NodeName),
}

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
        self.ctx.start_auto_clock().await;

        if !self.ctx.is_clock_started() {
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

impl MonitoringSemantics<DistSExpr, Value> for DistributedSemantics {
    fn to_async_stream(expr: DistSExpr, ctx: &dyn StreamContext<Value>) -> OutputStream<Value> {
        match expr {
            DistSExpr::SExpr(expr) => UntimedLolaSemantics::to_async_stream(expr, ctx),
            DistSExpr::MonitoredAt(var_name, label) => {
                // Hack to ensure that the Context is specifically a DistributedContext
                // (Instead of refactoring everything)
                // Note that this is VERY unsafe. Undefined behavior will happen
                // TODO: Fixme
                let ctx: &(dyn Any + 'static) = unsafe { mem::transmute(ctx) };
                let ctx = ctx
                    .downcast_ref::<DistributedContext>()
                    .expect("Invalid context type");

                Box::pin(stream! {
                    let mut graph_stream = ctx.graph().unwrap();
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
        }
    }
}
