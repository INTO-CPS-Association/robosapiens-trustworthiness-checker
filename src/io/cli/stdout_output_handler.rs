use std::collections::BTreeMap;
use std::rc::Rc;

use async_trait::async_trait;
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

#[async_trait(?Send)]
impl<V: StreamData> OutputHandler for StdoutOutputHandler<V> {
    type Val = V;

    fn provide_streams(&mut self, streams: BTreeMap<VarName, OutputStream<V>>) {
        self.manual_output_handler.provide_streams(streams);
    }

    fn run(&mut self) -> LocalBoxFuture<'static, ()> {
        let output_stream = self.manual_output_handler.get_output();
        let mut enumerated_outputs = output_stream.enumerate();
        let task = self.executor.spawn(self.manual_output_handler.run());

        Box::pin(async move {
            while let Some((i, output)) = enumerated_outputs.next().await {
                for (var, data) in output {
                    println!("{}[{}] = {:?}", var, i, data);
                }
            }
            task.await;
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::core::{OutputStream, Value, VarName};
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
        let mut handler: StdoutOutputHandler<Value> = StdoutOutputHandler::new(
            executor.clone(),
            vec![VarName("x".to_string()), VarName("y".to_string())],
        );

        handler.provide_streams(
            vec![
                (VarName("x".to_string()), x_stream),
                (VarName("y".to_string()), y_stream),
            ]
            .into_iter()
            .collect(),
        );

        let task = executor.spawn(handler.run());

        task.await;
    }
}
