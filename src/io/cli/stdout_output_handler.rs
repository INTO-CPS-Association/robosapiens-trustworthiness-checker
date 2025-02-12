use std::{collections::BTreeMap, future::Future, pin::Pin};

use async_trait::async_trait;
use futures::StreamExt;

use crate::core::{OutputHandler, OutputStream, StreamData, VarName};
use crate::io::testing::ManualOutputHandler;

/* Some members are defined as Option<T> as either they are provided after
 * construction by provide_streams or once they are used they are taken and
 * cannot be used again; this allows us to manage the lifetimes of our data
 * without mutexes or arcs. */
pub struct StdoutOutputHandler<V: StreamData> {
    manual_output_handler: ManualOutputHandler<V>,
}

impl<V: StreamData> StdoutOutputHandler<V> {
    pub fn new(var_names: Vec<VarName>) -> Self {
        let combined_output_handler = ManualOutputHandler::new(var_names);

        Self {
            manual_output_handler: combined_output_handler,
        }
    }
}

#[async_trait]
impl<V: StreamData> OutputHandler<V> for StdoutOutputHandler<V> {
    fn provide_streams(&mut self, streams: BTreeMap<VarName, OutputStream<V>>) {
        self.manual_output_handler.provide_streams(streams);
    }

    fn run(&mut self) -> Pin<Box<dyn Future<Output = ()> + 'static + Send>> {
        let output_stream = self.manual_output_handler.get_output();
        let mut enumerated_outputs = output_stream.enumerate();
        let task = tokio::spawn(self.manual_output_handler.run());

        Box::pin(async move {
            while let Some((i, output)) = enumerated_outputs.next().await {
                for (var, data) in output {
                    println!("{}[{}] = {:?}", var, i, data);
                }
            }
            task.await.unwrap();
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::core::{OutputStream, Value, VarName};
    use futures::stream;

    use super::*;
    use test_log::test;

    #[test(tokio::test)]
    async fn test_run_stdout_output_handler() {
        let x_stream: OutputStream<Value> = Box::pin(stream::iter((0..10).map(|x| (x * 2).into())));
        let y_stream: OutputStream<Value> =
            Box::pin(stream::iter((0..10).map(|x| (x * 2 + 1).into())));
        let mut handler: StdoutOutputHandler<Value> =
            StdoutOutputHandler::new(vec![VarName("x".to_string()), VarName("y".to_string())]);

        handler.provide_streams(
            vec![
                (VarName("x".to_string()), x_stream),
                (VarName("y".to_string()), y_stream),
            ]
            .into_iter()
            .collect(),
        );

        let task = tokio::spawn(handler.run());

        //
        task.await.unwrap();
    }
}
