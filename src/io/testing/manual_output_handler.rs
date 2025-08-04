use std::{mem, rc::Rc};

use crate::utils::cancellation_token::{CancellationToken, DropGuard};
use async_stream::stream;
use async_unsync::{bounded, oneshot};
use futures::FutureExt;
use futures::StreamExt;
use futures::future::{LocalBoxFuture, join_all};
use smol::LocalExecutor;
use tracing::{Level, debug, instrument};

use crate::{
    core::{OutputHandler, OutputStream, StreamData, VarName},
    stream_utils::{drop_guard_stream, oneshot_to_stream},
};

/* Some members are defined as Option<T> as either they are provided after
 * construction by provide_streams or once they are used they are taken and
 * cannot be used again; this allows us to manage the lifetimes of our data
 * without mutexes or arcs. */
pub struct ManualOutputHandler<V: StreamData> {
    var_names: Vec<VarName>,
    #[allow(dead_code)]
    executor: Rc<LocalExecutor<'static>>,
    stream_senders: Option<Vec<oneshot::Sender<OutputStream<V>>>>,
    stream_receivers: Option<Vec<oneshot::Receiver<OutputStream<V>>>>,
    output_receiver: Option<oneshot::Receiver<OutputStream<Vec<V>>>>,
    output_sender: Option<oneshot::Sender<OutputStream<Vec<V>>>>,
    output_cancellation: CancellationToken,
}

impl<V: StreamData> ManualOutputHandler<V> {
    pub fn new(executor: Rc<LocalExecutor<'static>>, var_names: Vec<VarName>) -> Self {
        let (stream_senders, stream_receivers): (
            Vec<oneshot::Sender<OutputStream<V>>>,
            Vec<oneshot::Receiver<OutputStream<V>>>,
        ) = var_names
            .iter()
            .map(|_| oneshot::channel().into_split())
            .unzip();
        let (output_sender, output_receiver) = oneshot::channel().into_split();
        let cancellation = CancellationToken::new();

        // Add debug task to monitor when cancellation is triggered
        let cancellation_debug = cancellation.clone();
        executor
            .spawn(async move {
                cancellation_debug.cancelled().await;
                debug!("ManualOutputHandler: Cancellation token was triggered!");
            })
            .detach();

        Self {
            var_names,
            executor,
            stream_senders: Some(stream_senders),
            stream_receivers: Some(stream_receivers),
            output_receiver: Some(output_receiver),
            output_sender: Some(output_sender),
            output_cancellation: cancellation,
        }
    }

    pub fn get_output(&mut self) -> OutputStream<Vec<V>> {
        let receiver = self
            .output_receiver
            .take()
            .expect("Output receiver missing");
        let drop_guard: Rc<DropGuard> = Rc::new(self.output_cancellation.clone().drop_guard());
        debug!("ManualOutputHandler: Created drop guard for output stream");
        drop_guard_stream(oneshot_to_stream(receiver), drop_guard)
    }
}

impl<V: StreamData> OutputHandler for ManualOutputHandler<V> {
    type Val = V;

    fn var_names(&self) -> Vec<VarName> {
        self.var_names.clone()
    }

    #[instrument(skip(self, streams))]
    fn provide_streams(&mut self, streams: Vec<OutputStream<V>>) {
        debug!(name: "Providing streams",
            num_streams = self.var_names.len());
        for (stream, sender) in streams.into_iter().zip(
            self.stream_senders
                .take()
                .expect("Stream senders not found"),
        ) {
            assert!(sender.send(stream).is_ok());
        }
    }

    #[instrument(name="Running ManualOutputHandler", level=Level::INFO,
                 skip(self))]
    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let receivers: Vec<oneshot::Receiver<OutputStream<V>>> =
            mem::take(&mut self.stream_receivers).expect("Stream receivers already taken");

        debug!(
            name = "Running ManualOutputHandler",
            num_streams = receivers.len()
        );
        let mut streams: Vec<_> = receivers
            .into_iter()
            .map(|mut r| r.try_recv().unwrap())
            .collect();

        let (result_tx, result_rx) = oneshot::channel().into_split();
        let (cancel_tx, cancel_rx) = oneshot::channel().into_split();
        let output_cancellation = self.output_cancellation.clone();

        // Spawn a task to monitor cancellation and send completion signal
        self.executor.spawn({
            let output_cancellation = output_cancellation.clone();
            async move {
                output_cancellation.cancelled().await;
                debug!("ManualOutputHandler: Cancellation monitor detected cancellation, sending completion signal");
                let _ = cancel_tx.send(());
            }
        }).detach();

        mem::take(&mut self.output_sender)
            .expect("Output sender not found")
            .send(Box::pin(stream! {
                loop {
                    let num_streams = streams.len();
                    debug!("ManualOutputHandler: Stream generator loop iteration, {} streams", num_streams);
                    let nexts = join_all(streams.iter_mut().map(|s| s.next()));
                    debug!("ManualOutputHandler: Waiting for values from streams");

                    if let Some(vals) = nexts.await
                        .into_iter()
                        .collect::<Option<Vec<V>>>()
                    {
                        // Collect the values into a Vec<V>
                        let output = vals;
                        // Output the combined data
                        debug!(name = "Outputting data", ?output);
                        yield output;
                    } else {
                        // One of the streams has ended, so we should stop
                        debug!(
                            "Stopping ManualOutputHandler with len(nexts) = {}",
                            num_streams
                        );
                        break;
                    }
                }
                debug!("ManualOutputHandler: Stream generator completed naturally");
                let _ = result_tx.send(Ok(()));
            }))
            .expect("Output sender channel dropped");

        Box::pin(async move {
            debug!("ManualOutputHandler: Waiting for result or drop signal");
            futures::select! {
                result = result_rx.fuse() => {
                    match result {
                        Ok(res) => {
                            debug!(
                                "ManualOutputHandler: Received completion signal from stream generator: {:?}",
                                res
                            );
                            res
                        }
                        Err(_) => {
                            debug!("ManualOutputHandler: Output stream was dropped, completing handler");
                            Ok(())
                        }
                    }
                }
                _ = cancel_rx.fuse() => {
                    debug!("ManualOutputHandler: Received cancellation signal, completing handler");
                    Ok(())
                }
            }
        })
    }
}

pub struct AsyncManualOutputHandler<V: StreamData> {
    #[allow(dead_code)]
    executor: Rc<LocalExecutor<'static>>,
    var_names: Vec<VarName>,
    stream_senders: Option<Vec<oneshot::Sender<OutputStream<V>>>>,
    stream_receivers: Option<Vec<oneshot::Receiver<OutputStream<V>>>>,
    output_sender: Option<bounded::Sender<(VarName, V)>>,
    output_receiver: Option<bounded::Receiver<(VarName, V)>>,
}

impl<V: StreamData> OutputHandler for AsyncManualOutputHandler<V> {
    type Val = V;

    fn var_names(&self) -> Vec<VarName> {
        self.var_names.clone()
    }

    fn provide_streams(&mut self, streams: Vec<OutputStream<V>>) {
        for (stream, sender) in streams.into_iter().zip(self.stream_senders.take().unwrap()) {
            assert!(sender.send(stream).is_ok());
        }
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let receivers = mem::take(&mut self.stream_receivers).expect("Stream receivers not found");
        let streams: Vec<_> = receivers
            .into_iter()
            .map(|mut r| r.try_recv().unwrap())
            .collect();
        let output_sender = mem::take(&mut self.output_sender).expect("Output sender not found");
        let var_names = self.var_names.clone();

        Box::pin(async move {
            futures::future::join_all(
                streams
                    .into_iter()
                    .zip(var_names)
                    .map(|(stream, var_name)| {
                        let mut stream = stream;
                        let output_sender = output_sender.clone();
                        async move {
                            while let Some(data) = stream.next().await {
                                let _ = output_sender.send((var_name.clone(), data)).await;
                            }
                        }
                    })
                    .collect::<Vec<_>>(),
            )
            .await;

            Ok(())
        })
    }
}

impl<V: StreamData> AsyncManualOutputHandler<V> {
    pub fn new(executor: Rc<LocalExecutor<'static>>, var_names: Vec<VarName>) -> Self {
        let (stream_senders, stream_receivers): (
            Vec<oneshot::Sender<OutputStream<V>>>,
            Vec<oneshot::Receiver<OutputStream<V>>>,
        ) = var_names
            .iter()
            .map(|_| oneshot::channel().into_split())
            .unzip();
        let (output_sender, output_receiver) = bounded::channel(10).into_split();
        Self {
            executor,
            var_names,
            stream_senders: Some(stream_senders),
            stream_receivers: Some(stream_receivers),
            output_receiver: Some(output_receiver),
            output_sender: Some(output_sender),
        }
    }

    pub fn get_output(&mut self) -> OutputStream<(VarName, V)> {
        let mut out = self
            .output_receiver
            .take()
            .expect("Output receiver missing");
        Box::pin(stream! {
            while let Some(data) = out.recv().await {
                yield data;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::async_test;
    use std::cmp::Ordering;
    use std::collections::BTreeSet;

    use super::*;
    use crate::{OutputStream, Value, VarName};
    use futures::StreamExt;
    use futures::select;
    use futures::stream;
    use macro_rules_attribute::apply;
    use tracing::info;

    // Implement Eq for Value - only available for testing since this is not
    // true for floats
    impl Eq for Value {}

    // Ordering of Value - only available for testing
    impl Ord for Value {
        fn cmp(&self, other: &Self) -> Ordering {
            use Value::*;

            // Define ordering of variants
            let variant_order = |value: &Value| match value {
                Unknown => 0,
                Unit => 1,
                Bool(_) => 2,
                Int(_) => 3,
                Float(_) => 4,
                Str(_) => 5,
                List(_) => 6,
            };

            // First compare based on variant order
            let self_order = variant_order(self);
            let other_order = variant_order(other);

            if self_order != other_order {
                return self_order.cmp(&other_order);
            }

            // Compare within the same variant
            match (self, other) {
                (Bool(a), Bool(b)) => a.cmp(b),
                (Int(a), Int(b)) => a.cmp(b),
                // Compare floats as ordered floats (with NaNs at either end of
                // the ordering) for the purposes of this test
                (Float(a), Float(b)) => {
                    ordered_float::OrderedFloat(*a).cmp(&ordered_float::OrderedFloat(*b))
                }
                (Str(a), Str(b)) => a.cmp(b),
                (List(a), List(b)) => a.cmp(b), // Vec<Value> implements Ord if Value does
                _ => Ordering::Equal, // Unit and Unknown are considered equal within their kind
            }
        }
    }

    impl PartialOrd for Value {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    #[apply(async_test)]
    async fn sync_test_combined_output(ex: Rc<LocalExecutor>) {
        let x_stream: OutputStream<Value> = Box::pin(stream::iter((0..10).map(|x| (x * 2).into())));
        let y_stream: OutputStream<Value> =
            Box::pin(stream::iter((0..10).map(|x| (x * 2 + 1).into())));
        let xy_expected: Vec<Vec<Value>> = (0..10)
            .map(|x| vec![(x * 2).into(), (x * 2 + 1).into()])
            .collect();
        let mut handler: ManualOutputHandler<Value> =
            ManualOutputHandler::new(ex.clone(), vec!["x".into(), "y".into()]);

        handler.provide_streams(vec![x_stream, y_stream]);

        let run_fut = handler.run();
        let output_stream = handler.get_output();

        let task = ex.spawn(run_fut);

        let output: Vec<Vec<Value>> = output_stream.collect().await;

        assert_eq!(output, xy_expected);

        task.await.expect("Failed to run handler");
    }

    #[apply(async_test)]
    async fn async_test_combined_output(executor: Rc<LocalExecutor<'static>>) {
        // Helper to create a named stream with delay
        fn create_stream(
            name: &str,
            multiplier: i64,
            offset: i64,
        ) -> (VarName, OutputStream<Value>) {
            let var_name = name.into();
            // Delay to force expected ordering of the streams
            let stream =
                Box::pin(stream::iter(0..10).map(move |x| (multiplier * x + offset).into()));
            (var_name, stream)
        }

        // Prepare input streams
        let (x_name, x_stream) = create_stream("x", 2, 0);
        let (y_name, y_stream) = create_stream("y", 2, 1);

        // Prepare expected output
        let expected_output: BTreeSet<_> = (0..10)
            .flat_map(|x| {
                vec![
                    (x_name.clone(), (x * 2).into()),
                    (y_name.clone(), (x * 2 + 1).into()),
                ]
            })
            .collect();

        // Initialize the handler
        let mut handler =
            AsyncManualOutputHandler::new(executor.clone(), vec![x_name.clone(), y_name.clone()]);
        handler.provide_streams(vec![x_stream, y_stream].into_iter().collect::<Vec<_>>());

        // Run the handler and validate output
        let output_stream = handler.get_output();
        let task = executor.spawn(handler.run());
        let results = output_stream.collect::<BTreeSet<_>>().await;

        assert_eq!(results, expected_output);
        task.await.expect("Failed to run handler");
    }

    #[apply(async_test)]
    async fn test_manual_output_handler_hang(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        info!("Creating ManualOutputHandler...");
        let mut handler = ManualOutputHandler::new(executor.clone(), vec!["test".into()]);

        // Create a test stream that never ends
        let test_stream: OutputStream<Value> = Box::pin(stream::iter((0..).map(|x| Value::Int(x))));

        info!("Providing infinite stream to handler...");
        handler.provide_streams(vec![test_stream]);

        info!("Getting output stream...");
        let output_stream = handler.get_output();

        info!("Starting handler task...");
        let handler_task = executor.spawn(handler.run());

        info!("Taking only 3 items from output stream...");
        let outputs = output_stream.take(3).collect::<Vec<_>>().await;
        info!("Collected outputs: {:?}", outputs);

        info!("Output stream dropped, waiting for handler to complete...");

        // Add a short timeout to see if the handler completes
        let timeout_future = smol::Timer::after(std::time::Duration::from_secs(2));

        select! {
            result = handler_task.fuse() => {
                info!("Handler completed successfully: {:?}", result);
            }
            _ = FutureExt::fuse(timeout_future) => {
                info!("Handler did not complete within timeout");
                return Err(anyhow::anyhow!("Handler hung"));
            }
        }

        Ok(())
    }
}
