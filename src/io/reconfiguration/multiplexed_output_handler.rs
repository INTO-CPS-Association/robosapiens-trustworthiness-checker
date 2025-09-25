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
use unsync::spsc::{Receiver as SpscReceiver, Sender as SpscSender};

use crate::{OutputStream, Value, VarName, core::OutputHandler, io::testing::ManualOutputHandler};

/// Communication module
mod comm {
    // Note: mod is needed to disallow constructing Consumers directly
    use super::*;

    /// Internal Communication for structs that spawn tasks and need to communicate with them
    /// The idea is that the receiver is taken by the spawned task, and the sender is kept.
    /// This is an abstraction for a pattern that is used in multiple places.
    pub struct InternalComm<T> {
        sender: SpscSender<T>,
        receiver: Option<SpscReceiver<T>>, // None when taken by a separate task
    }
    pub struct InternalCommReceiver<T> {
        consumer: SpscReceiver<T>,
    }

    impl<T> InternalComm<T> {
        pub fn new(buffer: usize) -> Self {
            let (tx, rx) = unsync::spsc::channel::<T>(buffer);
            Self {
                sender: tx,
                receiver: Some(rx),
            }
        }

        #[allow(dead_code)]
        pub fn consumer_taken(self) -> bool {
            self.receiver.is_none()
        }

        pub async fn produce(&mut self, value: T) -> Result<(), unsync::spsc::SendError<T>> {
            self.sender.send(value).await
        }

        pub fn take_consumer(&mut self) -> InternalCommReceiver<T> {
            if let Some(c) = mem::take(&mut self.receiver) {
                InternalCommReceiver::new(c)
            } else {
                panic!("Consumer already taken");
            }
        }
    }

    impl<T> InternalCommReceiver<T> {
        fn new(consumer: SpscReceiver<T>) -> Self {
            Self { consumer }
        }

        pub async fn consume(&mut self) -> Option<T> {
            self.consumer.recv().await
        }
    }
}

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
    pub async fn subscribe(&mut self) -> oneshot::Receiver<Vec<OutputStream<Value>>> {
        let (tx, rx): (
            oneshot::Sender<Vec<OutputStream<Value>>>,
            oneshot::Receiver<Vec<OutputStream<Value>>>,
        ) = oneshot::channel().into_split();

        // Requests are served immediately if Handler is not running (indicated by senders != None)
        if let Some(senders) = &mut self.senders {
            // let streams = Self::subscribe_inner(&mut senders);
            let streams = Self::create_streams(senders);
            tx.send(streams)
                .expect("Failed to send streams to the subscriber");
        }
        // Requests are queued if the handler is running
        else {
            if let Err(e) = self.requesters.produce(tx).await {
                panic!("Failed to send subscription request: {}", e);
            }
        }
        rx
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
        let mut consumer = self.requesters.take_consumer();

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
                    maybe_tx = consumer.consume().fuse() => {
                        match maybe_tx {
                            Some(tx) => {
                                let subscription = Self::create_streams(&mut senders);
                                if tx.send(subscription).is_err() {
                                    tracing::warn!("Failed to send subscription");
                                }
                            }
                            None => {
                                unreachable!("Producer/Consumer channel closed before output_stream finished. Should not happen.")
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
    type TickSender = SpscSender<()>;
    use crate::async_test;
    use futures::stream;
    use macro_rules_attribute::apply;
    use smol::Timer;
    use std::time::Duration;

    use super::*;

    // Helper stream that synchronizes other streams by only progressing when a tick is received
    // We need these to control the flow of the OutputStream tests - essentially we are making them
    // dependent on in InputStream (but without the dependencies)
    fn tick_stream(mut stream: OutputStream<Value>) -> (TickSender, OutputStream<Value>) {
        let (tick_sender, mut tick_receiver) = unsync::spsc::channel::<()>(10);
        let synced_stream = Box::pin(stream! {
            while let Some(_) = tick_receiver.recv().await {
                if let Some(vals) = stream.next().await {
                 yield vals;
             } else {
                 return;
             }
        }});
        (tick_sender, synced_stream)
    }

    // Helper function that creates synchronizes multiple streams by only letting them progress
    // when a master tick is received.
    fn tick_streams(
        ex: Rc<LocalExecutor<'static>>,
        streams: Vec<OutputStream<Value>>,
    ) -> (TickSender, Vec<OutputStream<Value>>) {
        // Create individually synched streams
        let (mut follower_senders, synced_streams): (Vec<_>, Vec<_>) =
            streams.into_iter().map(|s| tick_stream(s)).unzip();
        // Create basis for single sync stream
        let (leader_sender, mut leader_receiver) = unsync::spsc::channel::<()>(10);
        // Actual single sync stream:
        let synced_stream = Box::pin(stream! {
            while let Some(_) = leader_receiver.recv().await {
                let futs = follower_senders.iter_mut().map(|s| s.send(()));
                join_all(futs).await;
        }});
        // Make synced_stream run indefinitely - just waiting and forwarding ticks:
        ex.spawn(async move {
            let mut synced_stream = synced_stream;
            while let Some(_) = synced_stream.next().await {}
        })
        .detach();
        (leader_sender, synced_streams)
    }

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

    // Meta-test, checks that tick_stream behaves as expected
    #[apply(async_test)]
    async fn tick_stream_test(_ex: Rc<LocalExecutor<'static>>) {
        let (_, mut streams, expected) = gen_default_streams();
        let stream = streams.pop().unwrap();
        let expected = expected.into_iter().map(|x| x.last().cloned().unwrap());
        // Converts them to a single tick-synchronized stream
        let (mut tick_sender, mut stream) = tick_stream(stream);
        let mut expected = expected.into_iter();

        // Wait 5 ms - to make sure that stream has progressed if it was capable of doing so
        // before sending a tick:
        Timer::after(Duration::from_millis(5)).await;
        assert_eq!(stream.next().now_or_never(), None);

        // Send ticks and check that the stream progresses
        tick_sender.send(()).await.expect("Failed to send tick");
        assert_eq!(stream.next().await, expected.next());

        // Wait 5 ms - to make sure that stream has progressed if it was capable of doing so
        // before sending a tick:
        Timer::after(Duration::from_millis(5)).await;
        assert_eq!(stream.next().now_or_never(), None);

        // Send ticks and check that the stream progresses
        tick_sender.send(()).await.expect("Failed to send tick");
        assert_eq!(stream.next().await, expected.next());

        // Let it finish:
        while let Some(d) = expected.next() {
            tick_sender.send(()).await.expect("Failed to send tick");
            assert_eq!(stream.next().await, Some(d));
        }

        // Check that the stream is done
        tick_sender.send(()).await.expect("Failed to send tick");
        assert_eq!(stream.next().await, None);
    }

    // Meta-test, checks that tick_streams behaves as expected
    #[apply(async_test)]
    async fn tick_streams_test(ex: Rc<LocalExecutor<'static>>) {
        let (_, streams, expected) = gen_default_streams();
        // Converts them to multiple streams synchronized by a single tick stream
        let (mut tick_sender, mut streams) = tick_streams(ex, streams);
        let mut expected = expected.into_iter();

        // Wait 5 ms - to make sure that streams have progressed if they were capable of doing so
        // before sending a tick:
        Timer::after(Duration::from_millis(5)).await;
        streams.iter_mut().for_each(|s| {
            assert_eq!(s.next().now_or_never(), None);
        });

        // Send tick and check that the streams progress
        tick_sender.send(()).await.expect("Failed to send tick");
        for (s, d) in streams.iter_mut().zip(expected.next().unwrap()) {
            assert_eq!(s.next().await, Some(d));
        }

        // Wait 5 ms - to make sure that streams have progressed if they were capable of doing so
        // before sending a tick:
        Timer::after(Duration::from_millis(5)).await;
        streams.iter_mut().for_each(|s| {
            assert_eq!(s.next().now_or_never(), None);
        });

        // Send tick and check that the streams progress
        tick_sender.send(()).await.expect("Failed to send tick");
        for (s, d) in streams.iter_mut().zip(expected.next().unwrap()) {
            assert_eq!(s.next().await, Some(d));
        }

        // Let them finish:
        while let Some(ds) = expected.next() {
            tick_sender.send(()).await.expect("Failed to send tick");
            for (s, d) in streams.iter_mut().zip(ds) {
                assert_eq!(s.next().await, Some(d));
            }
        }

        // Check that the streams are done
        tick_sender.send(()).await.expect("Failed to send tick");
        streams.iter_mut().for_each(|s| {
            assert_eq!(s.next().now_or_never(), None);
        });
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
        let sub_streams = handler.subscribe().await.await.unwrap();
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
        let sub_streams = handler.subscribe().await.await.unwrap();
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
            handler.subscribe().await.await.unwrap(),
            handler.subscribe().await.await.unwrap(),
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
            tick_sender.send(()).await.expect("Failed to send tick");
            let res = output1.next().await;
            assert_eq!(res, None);
        }
    }
}
