use std::{collections::BTreeMap, rc::Rc};

use async_trait::async_trait;
use clap::ValueEnum;
use futures::future::LocalBoxFuture;
use smol::LocalExecutor;

use crate::dep_manage::interface::DependencyManager;

use super::{StreamData, VarName};

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Semantics {
    Untimed,
    TypedUntimed,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Runtime {
    Async,
    Constraints,
    Distributed,
    ReconfigurableAsync,
}

pub type OutputStream<T> = futures::stream::LocalBoxStream<'static, T>;

pub trait InputProvider {
    type Val;

    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Self::Val>>;

    /// Wait until the InputProvider is ready to receive messages (may never terminate in case
    /// of errors)
    fn ready(&self) -> LocalBoxFuture<'static, anyhow::Result<()>>;

    /// Input providers should run forever, unless there is an error, in which case they halt with
    /// an error result
    fn run(&mut self) -> LocalBoxFuture<'static, Result<(), anyhow::Error>>;
}

impl<V> InputProvider for Vec<(VarName, OutputStream<V>)> {
    type Val = V;

    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Self::Val>> {
        self.iter()
            .position(|(name, _)| name == var)
            .map(|index| self.swap_remove(index).1)
    }

    fn run(&mut self) -> LocalBoxFuture<'static, Result<(), anyhow::Error>> {
        Box::pin(futures::future::pending())
    }

    fn ready(&self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(futures::future::ready(Ok(())))
    }
}

impl<V> InputProvider for BTreeMap<VarName, OutputStream<V>> {
    type Val = V;

    // We are consuming the input stream from the map when
    // we return it to ensure single ownership and static lifetime
    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Self::Val>> {
        self.remove(var)
    }

    fn run(&mut self) -> LocalBoxFuture<'static, Result<(), anyhow::Error>> {
        Box::pin(futures::future::pending())
    }

    fn ready(&self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(futures::future::ready(Ok(())))
    }
}

impl<V: 'static> InputProvider for BTreeMap<VarName, Vec<V>> {
    type Val = V;

    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Self::Val>> {
        self.remove(var)
            .map(|values| Box::pin(futures::stream::iter(values.into_iter())) as OutputStream<V>)
    }

    fn run(&mut self) -> LocalBoxFuture<'static, Result<(), anyhow::Error>> {
        Box::pin(futures::future::pending())
    }

    fn ready(&self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(futures::future::ready(Ok(())))
    }
}

impl<V: 'static> InputProvider for std::collections::HashMap<VarName, Vec<V>> {
    type Val = V;

    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Self::Val>> {
        self.remove(var)
            .map(|values| Box::pin(futures::stream::iter(values.into_iter())) as OutputStream<V>)
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(futures::future::pending())
    }

    fn ready(&self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(futures::future::ready(Ok(())))
    }
}

pub trait Specification: Clone + 'static {
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

    fn dependencies(self, dependencies: DependencyManager) -> Self;

    fn maybe_dependencies(self, dependencies: Option<DependencyManager>) -> Self
    where
        Self: Sized,
    {
        if let Some(dependencies) = dependencies {
            self.dependencies(dependencies)
        } else {
            self
        }
    }

    fn build(self) -> Self::Mon;
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
 *
 * The input provider is provided as an Arc<Mutex<dyn InputProvider<V>>> to allow a dynamic
 * type of input provider to be provided and allows the output
 * to borrow from the input provider without worrying about lifetimes.
 */
#[async_trait(?Send)]
pub trait Monitor<M, V: StreamData>: Runnable {
    fn spec(&self) -> &M;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;
    use futures::stream;

    #[test]
    fn test_vec_input_provider_input_stream() {
        let mut provider = vec![
            (
                "x".into(),
                Box::pin(stream::iter(vec![Value::Int(1), Value::Int(2)])) as OutputStream<Value>,
            ),
            (
                "y".into(),
                Box::pin(stream::iter(vec![Value::Int(3), Value::Int(4)])) as OutputStream<Value>,
            ),
        ];

        // Test getting an existing stream
        let x_stream = provider.input_stream(&"x".into());
        assert!(x_stream.is_some());

        // Test getting a non-existing stream
        let z_stream = provider.input_stream(&"z".into());
        assert!(z_stream.is_none());

        // Test that the stream was removed from the provider
        let x_stream_again = provider.input_stream(&"x".into());
        assert!(x_stream_again.is_none());

        // Test that other streams are still available
        let y_stream = provider.input_stream(&"y".into());
        assert!(y_stream.is_some());
    }
}
