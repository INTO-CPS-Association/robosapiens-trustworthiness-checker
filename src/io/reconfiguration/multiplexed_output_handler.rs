use async_stream::stream;
use async_unsync::oneshot;
use futures::{
    FutureExt, StreamExt,
    future::{LocalBoxFuture, join_all},
    select_biased,
};
use smol::LocalExecutor;
use std::{mem, rc::Rc};
use tracing::info;
use unsync::broadcast::{Receiver as BroadcastReceiver, Sender as BroadcastSender, channel};

use crate::io::reconfiguration::comm;
use crate::{OutputStream, Value, VarName, core::OutputHandler, io::testing::ManualOutputHandler};

/// An OutputHandler that multiplexes a single inner output handler to multiple clients.
pub struct MultiplexedOutputHandler {
    // Inner handler. Must be manual because it is the only one that allows chaining
    manual_output_handler: ManualOutputHandler<Value>,
    /// Senders for broadcasting data to multiple subscribers
    senders: Option<Vec<BroadcastSender<Value>>>,
    /// Channel for requesting new subscriptions
    requesters: comm::InternalComm<oneshot::Sender<Vec<OutputStream<Value>>>>,
}

impl MultiplexedOutputHandler {
    pub fn new(executor: Rc<LocalExecutor<'static>>, var_names: Vec<VarName>) -> Self {
        let senders = Some(var_names.iter().map(|_| channel(128)).collect::<Vec<_>>());
        let manual_output_handler = ManualOutputHandler::new(executor.clone(), var_names);
        let requesters = comm::InternalComm::new(128);

        Self {
            manual_output_handler,
            senders,
            requesters,
        }
    }

    /// Subscribe to the senders and convert to streams
    fn create_streams(senders: &mut Vec<BroadcastSender<Value>>) -> Vec<OutputStream<Value>> {
        fn recv_to_stream(recv: BroadcastReceiver<Value>) -> OutputStream<Value> {
            Box::pin(stream! {
                let mut recv = recv;
                while let Some(data) = recv.recv().await {
                    yield data;
                }
            })
        }
        senders
            .iter_mut()
            .map(|sender| {
                let recv = sender.subscribe();
                recv_to_stream(recv)
            })
            .collect::<Vec<OutputStream<Value>>>()
    }

    /// Subscribe to the multiplexed output handler.
    /// Can be called after `run` has been called.
    pub async fn subscribe(&mut self) -> anyhow::Result<Vec<OutputStream<Value>>> {
        let (tx, rx): (
            oneshot::Sender<Vec<OutputStream<Value>>>,
            oneshot::Receiver<Vec<OutputStream<Value>>>,
        ) = oneshot::channel().into_split();

        // Requests are served immediately if Handler is not running (indicated by State == Setup)
        if self.requesters.state() == comm::InternalCommState::Setup {
            let senders = self
                .senders
                .as_mut()
                .expect("Senders should be available handler is not running");
            let streams = Self::create_streams(senders);
            tx.send(streams)
                .expect("Failed to send streams to the subscriber");
        } else {
            // Requests are queued if the handler is running
            match &mut self.requesters {
                comm::InternalComm::Sender(sender) => {
                    if let Err(e) = sender.send(tx).await {
                        panic!("Failed to send subscription request: {}", e);
                    }
                }
                _ => {
                    // Internal logical error - should not happen
                    // (Means that self.requesters == Receiver)
                    panic!("Failed to take sender for subscription request");
                }
            }
        }
        rx.await
            .map_err(|e| anyhow::anyhow!("Failed to receive streams for the subscriber: {:?}", e))
    }
}

impl OutputHandler for MultiplexedOutputHandler {
    type Val = Value;

    fn var_names(&self) -> Vec<VarName> {
        self.manual_output_handler.var_names()
    }

    fn provide_streams(&mut self, streams: Vec<OutputStream<Value>>) {
        info!("Providing streams to MultiplexedOutputHandler");
        self.manual_output_handler.provide_streams(streams);
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let executor = self.manual_output_handler.executor.clone();
        let output_stream = self.manual_output_handler.get_output();
        let mut handler_task = FutureExt::fuse(executor.spawn(self.manual_output_handler.run()));
        let mut output_stream = StreamExt::fuse(output_stream);
        let mut senders = mem::take(&mut self.senders).expect("Senders already_taken");
        let mut receiver = self
            .requesters
            .split_receiver()
            .expect("Failed to split requesters for internal communication")
            .take_receiver()
            // Should never fail as split was successful
            .expect("Failed to take receiver for internal communication");

        Box::pin(async move {
            loop {
                select_biased! {
                    // Ordering is (unfortunately) quite important here:
                    // 1. We prioritize new receivers such that they can get served ASAP
                    // 2. We then forward new data from the output stream to all receivers
                    // 3. We then potentially end if the handler_task is finished
                    //
                    // One big concern was regarding potentially dropping data:
                    // What happens if both a request and an output_stream are Poll::Ready()?
                    // Answer: The request is handled, and the future generated from the
                    // output_stream.next() is dropped. However, this is okay because since it has not
                    // been polled, the data is still available in the stream.

                    // Handle new requesters
                    maybe_tx = receiver.recv().fuse() => {
                        match maybe_tx {
                            Some(tx) => {
                                let subscription = Self::create_streams(&mut senders);
                                if tx.send(subscription).is_err() {
                                    tracing::warn!("Failed to send subscription");
                                }
                            }
                            None => {
                                unreachable!("InternalComm channel closed before output_stream finished. Should not happen.")
                            }
                        }
                    }
                    // Receive values from the stream
                    res = output_stream.next() => {
                        match res {
                            Some(data) => {
                                // Build a Vec of futures
                                let send_futs = senders.iter_mut()
                                    .zip(data.into_iter())
                                    .map(|(s, d)| {
                                        s.send(d)
                                    });
                                // Await them all
                                let _ = join_all(send_futs).await;
                            }
                            None => {
                                // output_stream ended
                                return Ok(())
                            }
                        }
                    }
                    res = handler_task => {
                        return res
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::async_test;
    use futures::stream;
    use macro_rules_attribute::apply;
    use tc_testutils::streams::{TickSender, tick_streams};

    use super::*;

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

    #[apply(async_test)]
    async fn single_multiplexed_output_handler(ex: Rc<LocalExecutor<'static>>) {
        // Get the streams
        let (mut tick_sender, stream_names, streams, expected) =
            gen_default_tick_streams(ex.clone());
        // Create Multiplexer
        let mut handler = MultiplexedOutputHandler::new(ex.clone(), stream_names.clone());
        handler.provide_streams(streams);
        // Create subscriber to the main handler
        let sub_streams = handler.subscribe().await.unwrap();
        let mut sub_handler = Box::new(ManualOutputHandler::new(ex.clone(), stream_names));
        sub_handler.provide_streams(sub_streams);
        // Get manual output:
        let mut output = sub_handler.get_output();
        ex.spawn(handler.run()).detach();
        ex.spawn(sub_handler.run()).detach();
        for exp in expected {
            tick_sender.send(()).await.expect("Failed to send tick");
            let res = output.next().await;
            assert_eq!(res, Some(exp));
        }
        // All data sent, output should end now
        tick_sender.send(()).await.expect("Failed to send tick");
        let res = output.next().await;
        assert_eq!(res, None);
    }

    #[apply(async_test)]
    async fn single_multiplexed_output_handler_run_before_subscribe(
        ex: Rc<LocalExecutor<'static>>,
    ) {
        // Get the streams
        let (mut tick_sender, stream_names, streams, expected) =
            gen_default_tick_streams(ex.clone());
        // Create Multiplexer
        let mut handler = MultiplexedOutputHandler::new(ex.clone(), stream_names.clone());
        handler.provide_streams(streams);
        // We run it before subscribing
        ex.spawn(handler.run()).detach();
        // Create subscriber to the main handler
        let sub_streams = handler.subscribe().await.unwrap();
        let mut sub_handler = Box::new(ManualOutputHandler::new(ex.clone(), stream_names));
        sub_handler.provide_streams(sub_streams);
        // Get manual output:
        let mut output = sub_handler.get_output();
        ex.spawn(sub_handler.run()).detach();
        for exp in expected {
            tick_sender.send(()).await.expect("Failed to send tick");
            let res = output.next().await;
            assert_eq!(res, Some(exp));
        }
        // All data sent, output should end now
        tick_sender.send(()).await.expect("Failed to send tick");
        let res = output.next().await;
        assert_eq!(res, None);
    }

    #[apply(async_test)]
    async fn dual_multiplexed_output_handler(ex: Rc<LocalExecutor<'static>>) {
        // Get the streams
        let (mut tick_sender, stream_names, streams, expected) =
            gen_default_tick_streams(ex.clone());
        // Create Multiplexer
        let mut handler = MultiplexedOutputHandler::new(ex.clone(), stream_names.clone());
        handler.provide_streams(streams);
        // Create subscriber to the main handler
        let (sub_streams1, sub_streams2) = (
            handler.subscribe().await.unwrap(),
            handler.subscribe().await.unwrap(),
        );
        let (mut sub_handler1, mut sub_handler2) = (
            Box::new(ManualOutputHandler::new(ex.clone(), stream_names.clone())),
            Box::new(ManualOutputHandler::new(ex.clone(), stream_names.clone())),
        );
        sub_handler1.provide_streams(sub_streams1);
        sub_handler2.provide_streams(sub_streams2);
        // Get manual output:
        let (mut output1, mut output2) = (sub_handler1.get_output(), sub_handler2.get_output());
        ex.spawn(handler.run()).detach();
        ex.spawn(sub_handler1.run()).detach();
        ex.spawn(sub_handler2.run()).detach();
        // Run everything and assert the output is as expected
        for exp in expected {
            tick_sender.send(()).await.expect("Failed to send tick");
            let res = output1.next().await;
            assert_eq!(res, Some(exp.clone()));
            let res = output2.next().await;
            assert_eq!(res, Some(exp));
        }
        // All data sent, output should end now
        tick_sender.send(()).await.expect("Failed to send tick");
        let res = output1.next().await;
        assert_eq!(res, None);
        let res = output2.next().await;
        assert_eq!(res, None);
    }

    #[apply(async_test)]
    async fn switching_multiplexed_output_handler(ex: Rc<LocalExecutor<'static>>) {
        // Get the streams
        let (mut tick_sender, stream_names, streams, expected) =
            gen_default_tick_streams(ex.clone());
        let mut expected = expected.into_iter();
        // Create Multiplexer
        let mut handler = MultiplexedOutputHandler::new(ex.clone(), stream_names.clone());
        handler.provide_streams(streams);
        {
            // Create subscriber to the main handler
            let sub_streams1 = handler
                .subscribe()
                .await
                .expect("Failed to receive new streams");
            let mut sub_handler1 =
                Box::new(ManualOutputHandler::new(ex.clone(), stream_names.clone()));
            sub_handler1.provide_streams(sub_streams1);
            // Get manual output:
            let mut output1 = sub_handler1.get_output();
            ex.spawn(sub_handler1.run()).detach();
            ex.spawn(handler.run()).detach();
            tick_sender.send(()).await.expect("Failed to send tick");
            assert_eq!(output1.next().await, expected.next());
            {
                // Create a lazy subscribed handler:
                let sub_streams2 = handler
                    .subscribe()
                    .await
                    .expect("Failed to receive new streams");
                let mut sub_handler2 =
                    Box::new(ManualOutputHandler::new(ex.clone(), stream_names.clone()));
                sub_handler2.provide_streams(sub_streams2);
                // Get manual output:
                let mut output2 = sub_handler2.get_output();
                ex.spawn(sub_handler2.run()).detach();
                tick_sender.send(()).await.expect("Failed to send tick");
                let exp = expected.next();
                assert_eq!(output1.next().await, exp.clone());
                assert_eq!(output2.next().await, exp.clone());
            }
            // Going out of scope should drop handler2 but handler1 still works
            tick_sender.send(()).await.expect("Failed to send tick");
            assert_eq!(output1.next().await, expected.next());
            for exp in expected {
                tick_sender.send(()).await.expect("Failed to send tick");
                let res = output1.next().await;
                assert_eq!(res, Some(exp));
            }
            // All data sent, output1 should end now
            tick_sender.send(()).await.expect("Failed to send tick");
            let res = output1.next().await;
            assert_eq!(res, None);
        }
    }
}
