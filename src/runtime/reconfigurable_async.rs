use std::{marker::PhantomData, rc::Rc};

use async_trait::async_trait;
use futures::{FutureExt, future::LocalBoxFuture, select};
use smol::LocalExecutor;
use tracing::{Level, info, instrument};

use crate::{
    InputProvider, Monitor, Specification, Value,
    core::{AbstractMonitorBuilder, OutputHandler, Runnable, StreamData},
    dep_manage::interface::DependencyManager,
    distributed::locality_receiver::LocalityReceiver,
    io::{InputProviderBuilder, builders::OutputHandlerBuilder, mqtt::MQTTLocalityReceiver},
    semantics::{MonitoringSemantics, StreamContext, distributed::localisation::Localisable},
};

use super::asynchronous::{AsyncMonitorBuilder, AsyncMonitorRunner};

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
    pub(super) input_provider: Option<Box<dyn InputProvider<Val = Value>>>,
    pub(super) output_builder: Option<OutputHandlerBuilder>,
    pub(super) output_provider: Option<Box<dyn OutputHandler<Val = Value>>>,
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
    M: Specification<Expr = Expr> + Localisable,
    Expr: 'static,
    S: MonitoringSemantics<Expr, Value, Ctx>,
    Ctx: StreamContext<Value>,
> AbstractMonitorBuilder<M, Value> for ReconfAsyncMonitorBuilder<M, Ctx, Value, Expr, S>
{
    type Mon = ReconfAsyncRunner<Expr, S, M, Ctx>;

    fn new() -> Self {
        ReconfAsyncMonitorBuilder {
            executor: None,
            model: None,
            input_builder: None,
            input_provider: None,
            output_builder: None,
            output_provider: None,
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

    fn input(self, input: Box<dyn InputProvider<Val = Value>>) -> Self {
        Self {
            input_provider: Some(input),
            ..self
        }
    }

    fn output(self, output: Box<dyn OutputHandler<Val = Value>>) -> Self {
        Self {
            output_provider: Some(output),
            ..self
        }
    }

    fn dependencies(self, _dependencies: DependencyManager) -> Self {
        // We don't currently use the dependencies in the async runtime
        self
    }

    fn build(self) -> ReconfAsyncRunner<Expr, S, M, Ctx> {
        panic!("One does not simply build a ReconfAsyncRunner - use async_build instead!");
    }

    fn async_build(self: Box<Self>) -> LocalBoxFuture<'static, Self::Mon> {
        let builder = *self;
        Box::pin(async move { builder.async_build().await })
    }
}

impl<
    M: Specification<Expr = Expr>,
    Expr: 'static,
    S: MonitoringSemantics<Expr, Value, Ctx>,
    Ctx: StreamContext<Value>,
> ReconfAsyncMonitorBuilder<M, Ctx, Value, Expr, S>
{
    pub fn input_builder(self, input_builder: InputProviderBuilder) -> Self {
        Self {
            input_builder: Some(input_builder),
            ..self
        }
    }

    pub fn output_builder(self, output_builder: OutputHandlerBuilder) -> Self {
        Self {
            output_builder: Some(output_builder),
            ..self
        }
    }

    pub fn reconf_provider(self, reconf_provider: MQTTLocalityReceiver) -> Self {
        Self {
            reconf_provider: Some(reconf_provider),
            ..self
        }
    }

    pub fn local_node(self, local_node: String) -> Self {
        Self { local_node, ..self }
    }

    // Builds an AsyncMonitorRunner in a non-destructive way
    pub async fn async_build_async_mon(self) -> AsyncMonitorRunner<Expr, Value, S, M, Ctx> {
        let input = if let Some(input_builder) = self.input_builder {
            input_builder.async_build().await
        } else if let Some(input_provider) = self.input_provider {
            input_provider
        } else {
            panic!("Cannot build without input_builder or input_provider");
        };

        let output_builder = self
            .output_builder
            .expect("Cannot build without output_builder");
        let output = output_builder.async_build().await;
        let async_builder = AsyncMonitorBuilder::<M, Ctx, Value, Expr, S>::new()
            .executor(self.executor.expect("Cannot build without executor"))
            .model(self.model.expect("Cannot build without model"))
            .input(input)
            .output(output);
        async_builder.build()
    }

    pub async fn async_build(self) -> ReconfAsyncRunner<Expr, S, M, Ctx> {
        let local_node = self.local_node.clone();
        let reconf_provider = self.reconf_provider.unwrap_or_else(|| {
            MQTTLocalityReceiver::new("localhost".into(), local_node.clone().into())
        });

        // Store the builders for later use in reconfiguration
        // This is the key fix: instead of consuming the builders to create providers once,
        // we keep the builders so we can create fresh providers for each reconfiguration
        let executor = self.executor.expect("Cannot build without executor");
        let model = self.model.expect("Cannot build without model");

        let input_builder = if self.input_builder.is_some() {
            self.input_builder
        } else if self.input_provider.is_some() {
            None // We have a direct provider, not a builder
        } else {
            panic!("Cannot build without input_builder or input_provider");
        };

        let output_builder = if self.output_builder.is_some() {
            self.output_builder
        } else if self.output_provider.is_some() {
            None // We have a direct provider, not a builder
        } else {
            panic!("Cannot build without output_builder or output_provider");
        };

        // For direct providers (non-builder case), create them now
        let input_provider = if let Some(input_provider) = self.input_provider {
            Some(input_provider)
        } else {
            None
        };

        let output_provider = if let Some(output_provider) = self.output_provider {
            Some(output_provider)
        } else {
            None
        };

        ReconfAsyncRunner {
            executor,
            model,
            input_builder,
            output_builder,
            input_provider,
            output_provider,
            reconf_provider,
            local_node,
            ctx_t: PhantomData,
            expr_t: PhantomData,
            semantics_t: PhantomData,
        }
    }
}

/// Reconfigurable Async Monitor Runner
///
/// This runner handles dynamic reconfiguration of monitoring tasks by creating fresh
/// input and output providers for each new work assignment. This design fixes the
/// "Input streams not supplied after receiving two messages" error that occurred
/// when trying to reuse AsyncMonitorBuilder instances after partial_clone().
///
/// Key design decisions:
/// - Stores builders (which are cloneable) instead of built providers (which are not)
/// - Creates fresh providers for each reconfiguration to avoid state conflicts
/// - Uses the concrete Value type throughout for compatibility with the builder system
pub struct ReconfAsyncRunner<Expr, S, M, Ctx>
where
    Expr: 'static,
    Ctx: StreamContext<Value>,
    S: MonitoringSemantics<Expr, Value, Ctx>,
    M: Specification<Expr = Expr>,
{
    executor: Rc<LocalExecutor<'static>>,
    model: M,
    /// Input provider builder - cloneable, used to create fresh providers
    input_builder: Option<InputProviderBuilder>,
    /// Output handler builder - cloneable, used to create fresh handlers
    output_builder: Option<OutputHandlerBuilder>,
    /// Direct input provider - not currently supported in reconfigurable mode
    input_provider: Option<Box<dyn InputProvider<Val = Value>>>,
    /// Direct output provider - not currently supported in reconfigurable mode
    output_provider: Option<Box<dyn OutputHandler<Val = Value>>>,
    reconf_provider: MQTTLocalityReceiver,
    local_node: String,
    ctx_t: PhantomData<Ctx>,

    expr_t: PhantomData<Expr>,
    semantics_t: PhantomData<S>,
}

#[async_trait(?Send)]
impl<Expr, S, M, Ctx> Monitor<M, Value> for ReconfAsyncRunner<Expr, S, M, Ctx>
where
    Expr: 'static,
    Ctx: StreamContext<Value>,
    S: MonitoringSemantics<Expr, Value, Ctx, Value>,
    M: Specification<Expr = Expr> + Localisable,
{
    fn spec(&self) -> &M {
        &self.model
    }
}

#[async_trait(?Send)]
impl<Expr, S, M, Ctx> Runnable for ReconfAsyncRunner<Expr, S, M, Ctx>
where
    Expr: 'static,
    Ctx: StreamContext<Value>,
    S: MonitoringSemantics<Expr, Value, Ctx, Value>,
    M: Specification<Expr = Expr> + Localisable,
{
    #[instrument(name="Running async Monitor", level=Level::INFO, skip(self))]
    async fn run_boxed(mut self: Box<Self>) -> anyhow::Result<()> {
        // Get the initial work assignment
        info!("Waiting for initial work assignment");
        let mut work_assignment = self.reconf_provider.receive().await?;

        info!("Received initial work assignment");

        loop {
            // Create fresh input and output providers for this configuration
            // This is the core fix: instead of trying to reuse providers via partial_clone()
            // (which sets input/output to None), we create completely fresh providers
            // for each reconfiguration from the stored builders
            let input = if let Some(ref input_builder) = self.input_builder {
                input_builder.clone().async_build().await
            } else if let Some(ref _input_provider) = self.input_provider {
                // This is a bit tricky since we can't clone trait objects
                // For now, this path should not be used with reconfigurable runtime
                return Err(anyhow::anyhow!(
                    "Direct input providers not supported in reconfigurable runtime"
                ));
            } else {
                return Err(anyhow::anyhow!("No input provider available"));
            };

            let output = if let Some(ref output_builder) = self.output_builder {
                output_builder.clone().async_build().await
            } else if let Some(ref _output_provider) = self.output_provider {
                // This is a bit tricky since we can't clone trait objects
                // For now, this path should not be used with reconfigurable runtime
                return Err(anyhow::anyhow!(
                    "Direct output providers not supported in reconfigurable runtime"
                ));
            } else {
                return Err(anyhow::anyhow!("No output provider available"));
            };

            // Create a new monitor with the localized model and fresh providers
            // Each reconfiguration gets a completely new AsyncMonitorBuilder instance
            // with fresh providers, avoiding the "Input streams not supplied" panic
            let localized_model = self.model.localise(&work_assignment);
            let builder = AsyncMonitorBuilder::<M, Ctx, Value, Expr, S>::new()
                .executor(self.executor.clone())
                .model(localized_model)
                .input(input)
                .output(output);
            let monitor = builder.build();

            select! {
                // Update the work assignment
                new_work_assignment = FutureExt::fuse(self.reconf_provider.receive()) => {
                    info!("Received new work assignment");
                    work_assignment = new_work_assignment?;
                    // Loop will continue and create a new monitor with the new assignment
                }

                result = FutureExt::fuse(monitor.run()) => {
                    // Monitor finished, handle result
                    result?;
                }
            }
        }
    }
}
