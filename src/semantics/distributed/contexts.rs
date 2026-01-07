use std::{cell::RefCell, mem, rc::Rc};

use async_trait::async_trait;
use futures::join;
use smol::LocalExecutor;

use crate::{
    OutputStream, VarName,
    distributed::distribution_graphs::{
        LabelledDistGraphStream, LabelledDistributionGraph, NodeName, TaggedVarOrNodeName,
    },
    lang::dynamic_lola::ast::VarOrNodeName,
    runtime::asynchronous::{Context as AsyncCtx, ContextBuilder as AsyncCtxBuilder, VarManager},
    semantics::{AbstractContextBuilder, AsyncConfig, StreamContext},
};

// An AsyncConfig used internally to map into AsyncCtx from DistributedContext
pub struct InnerAC<AC>
where
    AC: AsyncConfig<Ctx = DistributedContext<AC>>,
{
    phantom: std::marker::PhantomData<AC>,
}

impl<AC> AsyncConfig for InnerAC<AC>
where
    AC: AsyncConfig<Ctx = DistributedContext<AC>>,
{
    type Val = AC::Val;
    type Expr = AC::Expr;
    type Ctx = AsyncCtx<Self>;
}

pub struct DistributedContextBuilder<AC>
where
    AC: AsyncConfig<Ctx = DistributedContext<AC>>,
{
    async_ctx_builder: AsyncCtxBuilder<InnerAC<AC>>,
    async_ctx: Option<AsyncCtx<InnerAC<AC>>>,
    nested_async_ctx: Option<AsyncCtx<InnerAC<AC>>>,
    graph_name: Option<String>,
    node_names: Option<Vec<NodeName>>,
    graph_stream: Option<LabelledDistGraphStream>,
    presupplied_ctx: Option<Box<DistributedContext<AC>>>,
    built_callbacks: Vec<Box<dyn FnOnce(&mut DistributedContext<AC>)>>,
}

impl<AC> AbstractContextBuilder for DistributedContextBuilder<AC>
where
    AC: AsyncConfig<Ctx = DistributedContext<AC>>,
{
    type AC = AC;

    fn new() -> Self {
        Self {
            async_ctx_builder: AsyncCtxBuilder::new(),
            async_ctx: None,
            graph_stream: None,
            graph_name: None,
            node_names: None,
            presupplied_ctx: None,
            nested_async_ctx: None,
            built_callbacks: vec![],
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

    fn input_streams(mut self, input_streams: Vec<OutputStream<AC::Val>>) -> Self {
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
            built_callbacks: vec![],
        }
    }

    fn build(self) -> AC::Ctx {
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
        let graph_stream = self.graph_stream.expect("graph_stream not provided");
        let graph_name = self.graph_name.unwrap_or("graph".into());
        let node_names = self.node_names.unwrap();
        let graph_manager = Rc::new(RefCell::new(Some(VarManager::new(
            ctx.executor.clone(),
            graph_name.into(),
            graph_stream,
            ctx.cancellation_token(),
        ))));
        let mut ret = DistributedContext {
            ctx,
            graph_manager,
            executor,
            node_names,
            builder,
        };
        for f in self.built_callbacks {
            f(&mut ret);
        }
        ret
    }
}

impl<AC> DistributedContextBuilder<AC>
where
    AC: AsyncConfig<Ctx = DistributedContext<AC>>,
{
    pub fn graph_stream(mut self, graph_stream: LabelledDistGraphStream) -> Self {
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

    pub fn context(mut self, ctx: AsyncCtx<InnerAC<AC>>) -> Self {
        self.async_ctx = Some(ctx);
        self
    }

    pub fn presupplied_ctx(mut self, ctx: DistributedContext<AC>) -> Self {
        self.presupplied_ctx = Some(Box::new(ctx));
        self
    }

    pub fn nested(mut self, ctx: AsyncCtx<InnerAC<AC>>) -> Self {
        self.nested_async_ctx = Some(ctx);
        self
    }

    pub fn add_callback(mut self, callback: Box<dyn Fn(&mut DistributedContext<AC>)>) -> Self {
        self.built_callbacks.push(callback);
        self
    }
}

pub struct DistributedContext<AC>
where
    AC: AsyncConfig<Ctx = DistributedContext<AC>>,
{
    ctx: AsyncCtx<InnerAC<AC>>,
    /// Essentially a shared_ptr that we can at some time take ownership of
    node_names: Vec<NodeName>,
    graph_manager: Rc<RefCell<Option<VarManager<Rc<LabelledDistributionGraph>>>>>,
    executor: Rc<LocalExecutor<'static>>,
    builder: DistributedContextBuilder<AC>,
}

#[async_trait(?Send)]
impl<AC> StreamContext for DistributedContext<AC>
where
    AC: AsyncConfig<Ctx = DistributedContext<AC>>,
{
    type AC = AC;
    type Builder = DistributedContextBuilder<AC>;

    fn var(&self, x: &VarName) -> Option<OutputStream<AC::Val>> {
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

    fn cancellation_token(&self) -> crate::utils::cancellation_token::CancellationToken {
        self.ctx.cancellation_token()
    }

    fn cancel(&self) {
        self.ctx.cancel();
    }
}

impl<AC> DistributedContext<AC>
where
    AC: AsyncConfig<Ctx = DistributedContext<AC>>,
{
    pub fn graph(&self) -> Option<LabelledDistGraphStream> {
        if self.is_clock_started() {
            panic!("Cannot request a stream after the clock has started");
        }

        let mut var_manager = self.graph_manager.borrow_mut();
        let var_manager = var_manager.as_mut().unwrap();

        Some(var_manager.subscribe())
    }

    pub fn disambiguate_name(&self, name: VarOrNodeName) -> Option<TaggedVarOrNodeName> {
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
