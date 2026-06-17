use std::{
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
};

use futures::{
    StreamExt,
    future::{LocalBoxFuture, join_all},
};
use smol::LocalExecutor;

use super::ManualOutputHandler;
use crate::core::{OutputHandler, OutputStream, StreamData, VarName};

/* Some members are defined as Option<T> as either they are provided after
 * construction by provide_streams or once they are used they are taken and
 * cannot be used again; this allows us to manage the lifetimes of our data
 * without mutexes or arcs. */
pub struct NullOutputHandler<V: StreamData> {
    var_names: BTreeSet<VarName>,
    streams: Option<BTreeMap<VarName, OutputStream<V>>>,
}

pub struct LimitedNullOutputHandler<V: StreamData> {
    executor: Rc<LocalExecutor<'static>>,
    manual_output_handler: ManualOutputHandler<V>,
    limit: usize,
}

impl<V: StreamData> LimitedNullOutputHandler<V> {
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        var_names: BTreeSet<VarName>,
        limit: usize,
    ) -> Self {
        let combined_output_handler = ManualOutputHandler::new(executor.clone(), var_names);

        Self {
            executor,
            manual_output_handler: combined_output_handler,
            limit,
        }
    }
}

impl<V: StreamData> OutputHandler for LimitedNullOutputHandler<V> {
    type Val = V;

    fn provide_streams(&mut self, streams: BTreeMap<VarName, OutputStream<V>>) {
        self.manual_output_handler.provide_streams(streams);
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let output_stream = self.manual_output_handler.get_output();
        self.executor
            .spawn(output_stream.take(self.limit).collect::<Vec<_>>())
            .detach();
        self.manual_output_handler.run()
    }
}

impl<V: StreamData> NullOutputHandler<V> {
    pub fn new(_executor: Rc<LocalExecutor<'static>>, var_names: BTreeSet<VarName>) -> Self {
        Self {
            var_names,
            streams: None,
        }
    }
}

impl<V: StreamData> OutputHandler for NullOutputHandler<V> {
    type Val = V;

    fn provide_streams(&mut self, streams: BTreeMap<VarName, OutputStream<V>>) {
        assert!(
            self.var_names == streams.keys().cloned().collect(),
            "Variable names provided do not match variable names provided in constructor."
        );
        self.streams = Some(streams);
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let streams = self
            .streams
            .take()
            .expect("Output streams not provided to NullOutputHandler");
        Box::pin(async move {
            join_all(
                streams
                    .into_values()
                    .map(|stream| stream.collect::<Vec<_>>()),
            )
            .await;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::async_test;
    use crate::core::{OutputStream, Value};
    use futures::stream;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    use super::*;
    use macro_rules_attribute::apply;

    #[apply(async_test)]
    async fn test_run_null_output_handler(executor: Rc<LocalExecutor<'static>>) {
        let x_stream: OutputStream<Value> = Box::pin(stream::iter((0..10).map(|x| (x * 2).into())));
        let y_stream: OutputStream<Value> =
            Box::pin(stream::iter((0..10).map(|x| (x * 2 + 1).into())));
        let mut handler: NullOutputHandler<Value> =
            NullOutputHandler::new(executor.clone(), BTreeSet::from(["x".into(), "y".into()]));

        let streams = BTreeMap::from([("x".into(), x_stream), ("y".into(), y_stream)]);
        handler.provide_streams(streams);

        let task = executor.spawn(handler.run());

        task.await.expect("Failed to run handler");
    }

    #[apply(async_test)]
    async fn test_run_limited_output_handler(executor: Rc<LocalExecutor<'static>>) {
        let x_stream: OutputStream<Value> = Box::pin(stream::iter((0..).map(|x| (x * 2).into())));
        let y_stream: OutputStream<Value> =
            Box::pin(stream::iter((0..).map(|x| (x * 2 + 1).into())));
        let mut handler: LimitedNullOutputHandler<Value> = LimitedNullOutputHandler::new(
            executor.clone(),
            BTreeSet::from(["x".into(), "y".into()]),
            10,
        );

        let streams = BTreeMap::from([("x".into(), x_stream), ("y".into(), y_stream)]);
        handler.provide_streams(streams);

        let task = executor.spawn(handler.run());

        task.await.expect("Failed to run handler");
    }
}
