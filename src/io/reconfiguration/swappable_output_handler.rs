use async_unsync::oneshot;
use futures::{FutureExt, future::LocalBoxFuture, select_biased};
use smol::LocalExecutor;
use std::{mem, rc::Rc};
use tracing::{debug, info};

use crate::io::reconfiguration::comm;
use crate::{OutputStream, Value, VarName, core::OutputHandler};

type BoxOutputHandler = Box<dyn OutputHandler<Val = Value>>;

/// An OutputHandler that wraps an inner output handler that can be swapped after calling `run`.
pub struct SwappableOutputHandler {
    // Current inner handler
    output_handler: Option<BoxOutputHandler>,
    /// Channel for requesting a swap - messages are a tuple for sending BoxOutputHandler back to
    /// the requester and the new BoxOutputHandler to use
    swap_requests: comm::InternalComm<(oneshot::Sender<BoxOutputHandler>, BoxOutputHandler)>,
    pub executor: Rc<LocalExecutor<'static>>,
}

impl SwappableOutputHandler {
    pub fn new(executor: Rc<LocalExecutor<'static>>, output_handler: BoxOutputHandler) -> Self {
        let swap_requests = comm::InternalComm::new(16);
        let output_handler = Some(output_handler);
        Self {
            output_handler,
            swap_requests,
            executor,
        }
    }

    /// Swap the inner output handler.
    /// Can be called after `run` has been called.
    /// Returns the old handler via a oneshot channel.
    pub async fn swap(&mut self, handler: BoxOutputHandler) -> anyhow::Result<BoxOutputHandler> {
        let (tx, rx): (
            oneshot::Sender<BoxOutputHandler>,
            oneshot::Receiver<BoxOutputHandler>,
        ) = oneshot::channel().into_split();

        // Requests are served immediately if Handler is not running (indicated by State == Setup)
        if self.swap_requests.state() == comm::InternalCommState::Setup {
            let old_handler = mem::take(&mut self.output_handler)
                .expect("Output handler should be Some if not running");
            self.output_handler = Some(handler);
            tx.send(old_handler)
                .expect("Failed to send old handler back to swap caller");
        } else {
            // Requests are queued if the handler is running
            match &mut self.swap_requests {
                comm::InternalComm::Sender(sender) => {
                    if let Err(e) = sender.send((tx, handler)).await {
                        panic!("Failed to send swap request: {}", e);
                    }
                }
                _ => {
                    // Internal logical error - should not happen
                    // (Means that self.swap_requests == Receiver)
                    panic!("Failed to take sender for swap request");
                }
            }
        }
        rx.await.map_err(|e| {
            anyhow::anyhow!("Failed to receive old handler back from swap caller: {:?}",)
        })
    }
    type Val = Value;

    fn var_names(&self) -> Vec<VarName> {
        if let Some(oh) = &self.output_handler {
            oh.var_names()
        } else {
            // If needed we can support this with some back-and-forth messaging when we swapping
            // OutputHandlers. The inverse pattern of how we send swap requests
            panic!("var_names called when OutputHandler is running. Not supported.")
        }
    }

    fn provide_streams(&mut self, streams: Vec<OutputStream<Value>>) {
        info!("Providing streams to SwappableOutputHandler");
        if let Some(oh) = self.output_handler.as_mut() {
            oh.provide_streams(streams)
        } else {
            panic!("Streams provided when inner OutputHandler taken");
        }
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let mut output_handler =
            mem::take(&mut self.output_handler).expect("Inner OutputHandler already taken");
        let executor = self.executor.clone();
        let mut handler_task = FutureExt::fuse(executor.spawn(output_handler.run()));
        let mut receiver = self
            .swap_requests
            .split_receiver()
            .expect("Failed to split requesters for internal communication")
            .take_receiver()
            // Should never fail as split was successful
            .expect("Failed to take receiver for internal communication");

        Box::pin(async move {
            loop {
                select_biased! {
                    // Handle new requesters
                    maybe_tx = receiver.recv().fuse() => {
                        match maybe_tx {
                            Some((tx, handler)) => {
                                // Swap the inner handler
                                let old_handler = mem::replace(&mut output_handler, handler);
                                // Note that reassigning the handler_task drops (cancels) the
                                // old task
                                handler_task = FutureExt::fuse(executor.spawn(output_handler.run()));
                                let e = tx.send(old_handler); //.expect("Failed to send old handler back to swap caller");
                                if let Err(e) = e {
                                    panic!("SwappableOutputHandler: Failed to send old handler back to swap caller: {}", e);
                                }
                            }
                            None => {
                                unreachable!("InternalComm channel closed before output_stream finished. Should not happen.")
                            }
                        }
                    }
                    res = handler_task => {
                        match res {
                            Ok(_) => debug!("Stream generator completed. Waiting for new swaps."),
                            Err(e) => {
                                debug!("Stream generator task failed with error: {:?}. Propagating error.", e);
                                return Err(e);
                            },
                        }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use futures::stream;
    use macro_rules_attribute::apply;
    use smol::Timer;

    use super::*;
    use crate::{async_test, io::testing::ManualOutputHandler};
    use tc_testutils::streams::{TickSender, tick_streams};

    fn gen_data_streams(n: i64) -> (Vec<VarName>, Vec<OutputStream<Value>>, Vec<Vec<Value>>) {
        let x_stream: OutputStream<Value> =
            Box::pin(stream::iter((0..n).map(|x| ((x * 2).into()))));
        let y_stream: OutputStream<Value> =
            Box::pin(stream::iter((0..n).map(|x| ((x * 2 + 1).into()))));
        let stream_names = vec!["x".into(), "y".into()];
        let streams = vec![x_stream, y_stream];
        let expected = (0..n)
            .map(|x| vec![(x * 2).into(), (x * 2 + 1).into()])
            .collect::<Vec<Vec<Value>>>();
        (stream_names, streams, expected)
    }

    fn gen_default_streams() -> (Vec<VarName>, Vec<OutputStream<Value>>, Vec<Vec<Value>>) {
        gen_data_streams(10)
    }

    fn gen_default_tick_streams(
        ex: Rc<LocalExecutor<'static>>,
    ) -> (
        TickSender,
        Vec<VarName>,
        Vec<OutputStream<Value>>,
        Vec<Vec<Value>>,
    ) {
        let (names, streams, expected) = gen_default_streams();
        let (tick_sender, streams) = tick_streams(ex, streams);
        (tick_sender, names, streams, expected)
    }

    /// Tests that swapping before running works as expected
    #[apply(async_test)]
    async fn swap_before_run(ex: Rc<LocalExecutor<'static>>) {
        // Get the streams
        let (mut tick_sender, stream_names, streams, expected) =
            gen_default_tick_streams(ex.clone());
        // Initially create with a useless handler
        let mut unused_oh = Box::new(ManualOutputHandler::new(ex.clone(), vec![]));
        unused_oh.provide_streams(vec![]);
        let mut handler = SwappableOutputHandler::new(ex.clone(), unused_oh);
        // Swap before running:
        let mut inner = Box::new(ManualOutputHandler::new(ex.clone(), stream_names));
        inner.provide_streams(streams);
        // Get manual output:
        let mut output = inner.get_output();
        // Swap the handler
        let _ = handler.swap(inner).await.expect("Unsuccesful swap");
        ex.spawn(handler.run()).detach();
        for exp in expected {
            tick_sender.send(()).await.expect("Failed to send tick");
            let res = output.next().await;
            assert_eq!(res, Some(exp));
        }
        tick_sender.send(()).await.expect("Failed to send tick");
        let res = output.next().await;
        assert_eq!(res, None);
    }

    /// Tests swapping after having called run with an empty OutputHandler
    #[apply(async_test)]
    async fn swap_after_run_empty(ex: Rc<LocalExecutor<'static>>) {
        // Get the streams
        let (mut tick_sender, stream_names, streams, expected) =
            gen_default_tick_streams(ex.clone());
        // Initially create with a useless handler
        let mut empty_handler = Box::new(ManualOutputHandler::new(ex.clone(), vec![]));
        empty_handler.provide_streams(vec![]);
        let mut handler = SwappableOutputHandler::new(ex.clone(), empty_handler);
        ex.spawn(handler.run()).detach();

        // Wait a bit to make sure handler gets to run
        // (Using a small delay to make sure we are not immediately rescheduled)
        Timer::after(Duration::from_millis(5)).await;

        // Swap after running:
        let mut inner = Box::new(ManualOutputHandler::new(ex.clone(), stream_names));
        inner.provide_streams(streams);
        // Get manual output:
        let mut output = inner.get_output();
        // Swap the handler
        let _ = handler.swap(inner).await.expect("Unsuccesful swap");
        for exp in expected {
            tick_sender.send(()).await.expect("Failed to send tick");
            let res = output.next().await;
            assert_eq!(res, Some(exp));
        }
        tick_sender.send(()).await.expect("Failed to send tick");
        let res = output.next().await;
        assert_eq!(res, None);
    }

    /// Tests swapping after having called run where the inner OutputHandler has already done some work
    #[apply(async_test)]
    async fn swap_after_run_partial(ex: Rc<LocalExecutor<'static>>) {
        // Get the streams
        let (mut tick_sender1, stream_names1, streams1, expected1) =
            gen_default_tick_streams(ex.clone());
        let mut expected1 = expected1.into_iter();
        // Start of with handler1
        let mut handler1 = Box::new(ManualOutputHandler::new(ex.clone(), stream_names1));
        handler1.provide_streams(streams1);
        let mut output1 = handler1.get_output();
        let mut swap_handler = SwappableOutputHandler::new(ex.clone(), handler1);
        ex.spawn(swap_handler.run()).detach();

        // Get a few samples from handler1:
        tick_sender1.send(()).await.expect("Failed to send tick");
        assert_eq!(output1.next().await, expected1.next());
        tick_sender1.send(()).await.expect("Failed to send tick");
        assert_eq!(output1.next().await, expected1.next());

        // Now swap with a new one:
        // Get the streams
        let (mut tick_sender2, stream_names2, streams2, expected2) =
            gen_default_tick_streams(ex.clone());
        // Start of with handler1
        let mut handler2 = Box::new(ManualOutputHandler::new(ex.clone(), stream_names2));
        handler2.provide_streams(streams2);
        let mut output2 = handler2.get_output();
        let _ = swap_handler.swap(handler2).await.expect("Unsuccesful swap");

        // Wait a bit to make sure handler gets to run
        // Note: Test have previously passed as false positive without this because of other tasks
        // being starved
        Timer::after(Duration::from_millis(5)).await;

        // Samples from handler2 should be as if starting fresh:
        for exp in expected2 {
            tick_sender2.send(()).await.expect("Failed to send tick");
            let res = output2.next().await;
            assert_eq!(res, Some(exp));
        }
        tick_sender2.send(()).await.expect("Failed to send tick");
        let res = output2.next().await;
        assert_eq!(res, None);
    }

    /// Tests swapping after having called run where the inner OutputHandler has ran to completion
    #[apply(async_test)]
    async fn swap_after_run_full(ex: Rc<LocalExecutor<'static>>) {
        // Get the streams
        let (mut tick_sender1, stream_names1, streams1, expected1) =
            gen_default_tick_streams(ex.clone());
        // Start of with handler1
        let mut handler1 = Box::new(ManualOutputHandler::new(ex.clone(), stream_names1));
        handler1.provide_streams(streams1);
        let mut output1 = handler1.get_output();
        let mut swap_handler = SwappableOutputHandler::new(ex.clone(), handler1);
        ex.spawn(swap_handler.run()).detach();

        // Run to completion of handler1:
        for exp in expected1 {
            tick_sender1.send(()).await.expect("Failed to send tick");
            let res = output1.next().await;
            assert_eq!(res, Some(exp));
        }
        tick_sender1.send(()).await.expect("Failed to send tick");
        let res = output1.next().await;
        assert_eq!(res, None);

        // Wait a bit to make sure handler gets to run
        // Note: Test have previously passed as false positive without this because of other tasks
        // being starved
        Timer::after(Duration::from_millis(5)).await;

        // Now swap with a new one:
        // Get the streams
        let (mut tick_sender2, stream_names2, streams2, expected2) =
            gen_default_tick_streams(ex.clone());
        // Start of with handler1
        let mut handler2 = Box::new(ManualOutputHandler::new(ex.clone(), stream_names2));
        handler2.provide_streams(streams2);
        let mut output2 = handler2.get_output();
        let _ = swap_handler.swap(handler2).await.expect("Unsuccesful swap");

        // Wait a bit to make sure handler gets to run
        // Note: Test have previously passed as false positive without this because of other tasks
        // being starved
        Timer::after(Duration::from_millis(5)).await;

        // Samples from handler2 should be as if starting fresh:
        for exp in expected2 {
            tick_sender2.send(()).await.expect("Failed to send tick");
            let res = output2.next().await;
            assert_eq!(res, Some(exp));
        }
        tick_sender2.send(()).await.expect("Failed to send tick");
        let res = output2.next().await;
        assert_eq!(res, None);
    }
}
