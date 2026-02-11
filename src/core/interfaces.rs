use async_trait::async_trait;
use clap::ValueEnum;
use futures::future::LocalBoxFuture;
use smol::LocalExecutor;
use std::fmt::Debug;
use std::{collections::BTreeMap, rc::Rc};

use crate::io::mqtt::MQTTLocalityReceiver;

use super::{StreamData, VarName};

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Semantics {
    Untimed,
    TypedUntimed,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Runtime {
    Async,
    Distributed,
    SemiSync,
    ReconfigurableSemiSync,
}

pub type OutputStream<T> = futures::stream::LocalBoxStream<'static, T>;

#[async_trait(?Send)]
pub trait InputProvider {
    type Val;

    fn var_stream(&mut self, var: &VarName) -> Option<OutputStream<Self::Val>>;

    /// Legacy interface for InputProviders. Prefer using `control_stream` for better control.
    ///
    /// Returns a task that runs the InputProvider indefinitely.
    /// Error path: Returns an Err if something goes wrong.
    /// Happy path: Some InputProviders terminate after providing all values (e.g., file ending)
    /// and return Ok.
    /// Others run indefinitely (e.g., MQTT waiting for more)
    fn run(&mut self) -> LocalBoxFuture<'static, Result<(), anyhow::Error>>;

    /// Returns an OutputStream of Result<()> indicating if the InputProvider has encountered an
    /// error (Err) or has successfully provided one batch of values (Ok).
    /// Awaiting the control_stream attempts to progress the InputProvider by one step.
    async fn control_stream(&mut self) -> OutputStream<anyhow::Result<()>>;
}

#[async_trait(?Send)]
impl<V> InputProvider for BTreeMap<VarName, OutputStream<V>> {
    type Val = V;

    // We are consuming the input stream from the map when
    // we return it to ensure single ownership and static lifetime
    fn var_stream(&mut self, var: &VarName) -> Option<OutputStream<Self::Val>> {
        self.remove(var)
    }

    fn run(&mut self) -> LocalBoxFuture<'static, Result<(), anyhow::Error>> {
        Box::pin(futures::future::pending())
    }

    async fn control_stream(&mut self) -> OutputStream<anyhow::Result<()>> {
        todo!("Not obvious how to implement this for a map")
    }
}

pub trait Specification: Debug + Clone + 'static {
    type Expr;

    fn input_vars(&self) -> Vec<VarName>;

    fn output_vars(&self) -> Vec<VarName>;

    fn var_names(&self) -> Vec<VarName> {
        self.input_vars()
            .into_iter()
            .chain(self.output_vars().into_iter())
            .collect()
    }

    fn var_expr(&self, var: &VarName) -> Option<Self::Expr>;

    fn add_input_var(&mut self, var: VarName);
}

// This could alternatively implement Sink
// The constructor (which is not specified by the trait) should provide any
// configuration details needed by the output handler (e.g. host, port,
// output file name, etc.) whilst provide_streams is called by the runtime to
// finish the setup of the output handler by providing the streams to be output,
// and finally run is called to start the output handler.
pub trait OutputHandler {
    type Val: StreamData;

    // async fn handle_output(&mut self, var: &VarName, value: V);
    // This should only be called once by the runtime to provide the streams
    fn provide_streams(&mut self, streams: Vec<OutputStream<Self::Val>>);

    fn var_names(&self) -> Vec<VarName>;

    // Essentially this is of type
    // async fn run(&mut self);
    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>>;
}

pub trait AbstractMonitorBuilder<M, V: StreamData> {
    type Mon: Runnable;

    fn new() -> Self;

    fn executor(self, ex: Rc<LocalExecutor<'static>>) -> Self;

    fn maybe_executor(self, ex: Option<Rc<LocalExecutor<'static>>>) -> Self
    where
        Self: Sized,
    {
        if let Some(ex) = ex {
            self.executor(ex)
        } else {
            self
        }
    }

    fn model(self, model: M) -> Self;

    fn maybe_model(self, model: Option<M>) -> Self
    where
        Self: Sized,
    {
        if let Some(model) = model {
            self.model(model)
        } else {
            self
        }
    }

    fn input(self, input: Box<dyn InputProvider<Val = V>>) -> Self;

    fn maybe_input(self, input: Option<Box<dyn InputProvider<Val = V>>>) -> Self
    where
        Self: Sized,
    {
        if let Some(input) = input {
            self.input(input)
        } else {
            self
        }
    }

    fn output(self, output: Box<dyn OutputHandler<Val = V>>) -> Self;

    fn maybe_output(self, output: Option<Box<dyn OutputHandler<Val = V>>>) -> Self
    where
        Self: Sized,
    {
        if let Some(output) = output {
            self.output(output)
        } else {
            self
        }
    }

    fn mqtt_reconfig_provider(self, provider: MQTTLocalityReceiver) -> Self;

    fn maybe_mqtt_reconfig_provider(self, provider: Option<MQTTLocalityReceiver>) -> Self
    where
        Self: Sized,
    {
        if let Some(provider) = provider {
            self.mqtt_reconfig_provider(provider)
        } else {
            self
        }
    }

    fn build(self) -> Self::Mon;

    fn async_build(self: Box<Self>) -> LocalBoxFuture<'static, Self::Mon>;
}

#[async_trait(?Send)]
impl Runnable for Box<dyn Runnable> {
    async fn run_boxed(mut self: Box<Self>) -> anyhow::Result<()> {
        Runnable::run_boxed(self).await
    }

    async fn run(mut self: Self) -> anyhow::Result<()> {
        Runnable::run_boxed(self).await
    }
}

#[async_trait(?Send)]
impl<M, V: StreamData> Runnable for Box<dyn Monitor<M, V>> {
    async fn run_boxed(mut self: Box<Self>) -> anyhow::Result<()> {
        Runnable::run_boxed(self).await
    }

    async fn run(mut self: Self) -> anyhow::Result<()> {
        Runnable::run_boxed(self).await
    }
}

#[async_trait(?Send)]
impl<M, V: StreamData> Monitor<M, V> for Box<dyn Monitor<M, V>> {
    fn spec(&self) -> &M {
        self.as_ref().spec()
    }
}

#[async_trait(?Send)]
pub trait Runnable {
    // Should usually wait on the output provider
    async fn run(mut self) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        Box::new(self).run_boxed().await
    }

    async fn run_boxed(mut self: Box<Self>) -> anyhow::Result<()>;
}

/*
 * A runtime monitor for a model/specification of type M over streams with
 * values of type V.
 */
#[async_trait(?Send)]
pub trait Monitor<M, V: StreamData>: Runnable {
    fn spec(&self) -> &M;
}
