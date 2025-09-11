use std::rc::Rc;

use futures::future::LocalBoxFuture;
use futures::{FutureExt, StreamExt, select_biased};
use smol::LocalExecutor;
use tracing::info;

use crate::core::{OutputHandler, OutputStream, StreamData, VarName};
use crate::io::testing::ManualOutputHandler;

/* Some members are defined as Option<T> as either they are provided after
 * construction by provide_streams or once they are used they are taken and
 * cannot be used again; this allows us to manage the lifetimes of our data
 * without mutexes or arcs. */
pub struct StdoutOutputHandler<V: StreamData> {
    manual_output_handler: ManualOutputHandler<V>,
    aux_info: Vec<VarName>,
}

impl<V: StreamData> StdoutOutputHandler<V> {
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        var_names: Vec<VarName>,
        aux_info: Vec<VarName>,
    ) -> Self {
        let combined_output_handler = ManualOutputHandler::new(executor, var_names);

        Self {
            manual_output_handler: combined_output_handler,
            aux_info,
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

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let executor = self.manual_output_handler.executor.clone();
        let output_stream = self.manual_output_handler.get_output();
        let enumerated_outputs = output_stream.enumerate();
        let mut task = FutureExt::fuse(executor.spawn(self.manual_output_handler.run()));
        let var_names = self
            .var_names()
            .iter()
            .map(|x| x.name())
            .collect::<Vec<_>>();
        let aux_names = self.aux_info.iter().map(|x| x.name()).collect::<Vec<_>>();
        let mut outputs = StreamExt::fuse(enumerated_outputs);

        Box::pin(async move {
            loop {
                // TODO: check that we do not incorrectly drop results in between the
                // end of the handler, and the output stream finishing being consumed.
                // Should the output stream always end when the output has finished, or do
                // we need to manually detect the end of the output as the current code is doing?
                select_biased! {
                    res = outputs.next() => {
                        match res {
                            Some((i, output)) => for (var, data) in var_names.iter().zip(output) {
                                info!(?var, ?i, ?data, "Handling output");
                                if !aux_names.contains(var) {
                                    println!("{}[{}] = {:?}", var, i, data);
                                }
                            }
                            None => {
                                return Ok(())
                            }
                        }
                    }
                    res = task => {
                        // TODO: should this stop us whenever there is a result, or only if it is
                        // successful?
                        return res
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::core::{OutputStream, Value};
    use futures::stream;
    use tracing::info;

    use super::*;
    use crate::async_test;
    use macro_rules_attribute::apply;

    #[apply(async_test)]
    async fn test_run_stdout_output_handler(executor: Rc<LocalExecutor<'static>>) {
        let x_stream: OutputStream<Value> = Box::pin(stream::iter((0..10).map(|x| (x * 2).into())));
        let y_stream: OutputStream<Value> =
            Box::pin(stream::iter((0..10).map(|x| (x * 2 + 1).into())));
        let mut handler: StdoutOutputHandler<Value> =
            StdoutOutputHandler::new(executor.clone(), vec!["x".into(), "y".into()], vec![]);

        handler.provide_streams(vec![x_stream, y_stream].into_iter().collect());

        handler.run().await.expect("Failed to run output handler");
        info!("Finished running output handler");
    }
}
