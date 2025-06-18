use std::{marker::PhantomData, rc::Rc};

use anyhow;
use async_trait::async_trait;
use smol::LocalExecutor;
use tracing::{Level, instrument};

use crate::{
    InputProvider, Monitor, Specification, Value,
    core::{AbstractMonitorBuilder, OutputHandler, Runnable, StreamData},
    dep_manage::interface::DependencyManager,
    io::{InputProviderBuilder, builders::OutputHandlerBuilder, mqtt::MQTTLocalityReceiver},
    semantics::{MonitoringSemantics, StreamContext},
};

use super::asynchronous::{AsyncMonitorBuilder, AsyncMonitorRunner};

#[derive(Clone)]
pub struct ReconfAsyncMonitorBuilder<
    M: Specification<Expr = Expr>,
    Ctx: StreamContext<V>,
    V: StreamData,
    Expr,
    S: MonitoringSemantics<Expr, V, Ctx>,
> {
    pub(super) executor: Option<Rc<LocalExecutor<'static>>>,
    pub(crate) model: Option<M>,
    pub(super) input_builder: Option<InputProviderBuilder>,
    pub(super) output_builder: Option<OutputHandlerBuilder>,
    #[allow(dead_code)]
    reconf_provider: Option<MQTTLocalityReceiver>,
    #[allow(dead_code)]
    local_node: String,
    ctx_t: PhantomData<Ctx>,
    v_t: PhantomData<V>,
    expr_t: PhantomData<Expr>,
    semantics_t: PhantomData<S>,
}

impl<
    M: Specification<Expr = Expr>,
    Expr,
    S: MonitoringSemantics<Expr, Val, Ctx>,
    Val: StreamData,
    Ctx: StreamContext<Val>,
> AbstractMonitorBuilder<M, Val> for ReconfAsyncMonitorBuilder<M, Ctx, Val, Expr, S>
{
    type Mon = ReconfAsyncRunner<Expr, Val, S, M, Ctx>;

    fn new() -> Self {
        ReconfAsyncMonitorBuilder {
            executor: None,
            model: None,
            input_builder: None,
            output_builder: None,
            reconf_provider: None,
            local_node: "".into(),
            ctx_t: PhantomData,
            v_t: PhantomData,
            expr_t: PhantomData,
            semantics_t: PhantomData,
        }
    }

    fn executor(self, executor: Rc<LocalExecutor<'static>>) -> Self {
        Self {
            executor: Some(executor),
            ..self
        }
    }

    fn model(self, model: M) -> Self {
        Self {
            model: Some(model),
            ..self
        }
    }

    fn input(self, _input: Box<dyn InputProvider<Val = Val>>) -> Self {
        panic!("This type of builder does not take input providers - only input_builders");
    }

    fn output(self, _output: Box<dyn OutputHandler<Val = Val>>) -> Self {
        panic!("This type of builder does not take output handlers - only output_builders");
    }

    fn dependencies(self, _dependencies: DependencyManager) -> Self {
        // We don't currently use the dependencies in the async runtime
        self
    }

    fn build(self) -> ReconfAsyncRunner<Expr, Val, S, M, Ctx> {
        panic!("One does not simply build a ReconfAsyncRunner - use async_build instead!");
    }
}

impl<
    M: Specification<Expr = Expr>,
    Expr,
    S: MonitoringSemantics<Expr, Value, Ctx>,
    Ctx: StreamContext<Value>,
> ReconfAsyncMonitorBuilder<M, Ctx, Value, Expr, S>
{
    // Builds an AsyncMonitorRunner in a non-destructive way
    pub async fn async_build_async_mon(self) -> AsyncMonitorRunner<Expr, Value, S, M, Ctx> {
        let input_builder = self
            .input_builder
            .clone()
            .expect("Cannot build without input_builder");
        let output_builder = self
            .output_builder
            .clone()
            .expect("Cannot build without output_builder");
        let input = input_builder.async_build().await;
        let output = output_builder.async_build().await;
        let async_builder = AsyncMonitorBuilder::<M, Ctx, Value, Expr, S>::new()
            .executor(
                self.executor
                    .clone()
                    .expect("Cannot build without executor"),
            )
            .model(self.model.clone().expect("Cannot build without model"))
            .input(input)
            .output(output);
        async_builder.build()
    }

    pub async fn async_build(self) -> ReconfAsyncRunner<Expr, Value, S, M, Ctx> {
        todo!();
    }
}

pub struct ReconfAsyncRunner<Expr, Val, S, M, Ctx>
where
    Val: StreamData,
    Ctx: StreamContext<Val>,
    S: MonitoringSemantics<Expr, Val, Ctx>,
    M: Specification<Expr = Expr>,
{
    #[allow(dead_code)]
    async_mon_builder: AsyncMonitorBuilder<M, Ctx, Val, Expr, S>,
    #[allow(dead_code)]
    reconf_provider: MQTTLocalityReceiver,
    #[allow(dead_code)]
    local_node: String,
    async_mon: AsyncMonitorRunner<Expr, Val, S, M, Ctx>,
}

#[async_trait(?Send)]
impl<Expr, Val, S, M, Ctx> Monitor<M, Val> for ReconfAsyncRunner<Expr, Val, S, M, Ctx>
where
    Val: StreamData,
    Ctx: StreamContext<Val>,
    S: MonitoringSemantics<Expr, Val, Ctx, Val>,
    M: Specification<Expr = Expr>,
{
    fn spec(&self) -> &M {
        &self.async_mon.spec()
    }
}

#[async_trait(?Send)]
impl<Expr, Val, S, M, Ctx> Runnable for ReconfAsyncRunner<Expr, Val, S, M, Ctx>
where
    Val: StreamData,
    Ctx: StreamContext<Val>,
    S: MonitoringSemantics<Expr, Val, Ctx, Val>,
    M: Specification<Expr = Expr>,
{
    #[instrument(name="Running async Monitor", level=Level::INFO, skip(self))]
    async fn run_boxed(mut self: Box<Self>) -> anyhow::Result<()> {
        // TODO: Implement this
        Ok(())
    }
}
