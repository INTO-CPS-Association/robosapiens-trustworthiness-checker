use std::rc::Rc;

use futures::StreamExt;
use futures::future::LocalBoxFuture;
use smol::LocalExecutor;

use crate::core::{OutputHandler, OutputStream, StreamData, VarName};
use crate::io::testing::ManualOutputHandler;

/* Some members are defined as Option<T> as either they are provided after
 * construction by provide_streams or once they are used they are taken and
 * cannot be used again; this allows us to manage the lifetimes of our data
 * without mutexes or arcs. */
pub struct StdoutOutputHandler<V: StreamData> {
    executor: Rc<LocalExecutor<'static>>,
    manual_output_handler: ManualOutputHandler<V>,
}

impl<V: StreamData> StdoutOutputHandler<V> {
    pub fn new(executor: Rc<LocalExecutor<'static>>, var_names: Vec<VarName>) -> Self {
        let combined_output_handler = ManualOutputHandler::new(executor.clone(), var_names);

        Self {
            executor,
            manual_output_handler: combined_output_handler,
        }
    }
}

impl<V: StreamData> OutputHandler for StdoutOutputHandler<V> {
    type Val = V;

    fn var_names(&self) -> Vec<VarName> {
        self.manual_output_handler.var_names()
    }

    fn provide_streams(&mut self, streams: Vec<OutputStream<V>>) {
        self.manual_output_handler.provide_streams(streams);
    }

    fn run(&mut self) -> LocalBoxFuture<'static, ()> {
        let output_stream = self.manual_output_handler.get_output();
        let mut enumerated_outputs = output_stream.enumerate();
        let task = self.executor.spawn(self.manual_output_handler.run());
        let var_names = self
            .var_names()
            .iter()
            .map(|x| x.name())
            .collect::<Vec<_>>();

        Box::pin(async move {
            while let Some((i, output)) = enumerated_outputs.next().await {
                for (var, data) in var_names.iter().zip(output) {
                    println!("{}[{}] = {:?}", var, i, data);
                }
            }
            task.await;
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::core::{OutputStream, Value};
    use futures::stream;

    use super::*;
    use macro_rules_attribute::apply;
    use smol_macros::test as smol_test;
    use test_log::test;

    #[test(apply(smol_test))]
    async fn test_run_stdout_output_handler(executor: Rc<LocalExecutor<'static>>) {
        let x_stream: OutputStream<Value> = Box::pin(stream::iter((0..10).map(|x| (x * 2).into())));
        let y_stream: OutputStream<Value> =
            Box::pin(stream::iter((0..10).map(|x| (x * 2 + 1).into())));
        let mut handler: StdoutOutputHandler<Value> =
            StdoutOutputHandler::new(executor.clone(), vec!["x".into(), "y".into()]);

        handler.provide_streams(vec![x_stream, y_stream].into_iter().collect());

        let task = executor.spawn(handler.run());

        task.await;
    }
}
