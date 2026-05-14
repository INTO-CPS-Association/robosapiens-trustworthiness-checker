use std::collections::BTreeSet;
use std::{collections::BTreeMap, mem, rc::Rc};

use crate::utils::cancellation_token::{CancellationToken, DropGuard};
use anyhow::anyhow;
use async_stream::stream;
use async_unsync::oneshot;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::{LocalBoxFuture, join_all};
use smol::LocalExecutor;
use tracing::error;
use tracing::warn;
use tracing::{Level, debug, enabled, instrument};

use crate::{
    core::{OutputHandler, OutputStream, StreamData, VarName},
    stream_utils::{drop_guard_stream, oneshot_to_stream},
};

/* Some members are defined as Option<T> as either they are provided after
 * construction by provide_streams or once they are used they are taken and
 * cannot be used again; this allows us to manage the lifetimes of our data
 * without mutexes or arcs. */
// NOTE: This OutputHandler does NOT automatically forward NoVal values to implement async
// streams, as it would result in too many NoVals being sent when chaining this to other
// OutputHandlers.
pub struct ManualOutputHandler<V: StreamData> {
    pub var_names: BTreeSet<VarName>,
    #[allow(dead_code)]
    pub executor: Rc<LocalExecutor<'static>>,
    stream_senders: Option<BTreeMap<VarName, oneshot::Sender<OutputStream<V>>>>,
    stream_receivers: Option<BTreeMap<VarName, oneshot::Receiver<OutputStream<V>>>>,
    output_receiver: Option<oneshot::Receiver<OutputStream<BTreeMap<VarName, V>>>>,
    output_sender: Option<oneshot::Sender<OutputStream<BTreeMap<VarName, V>>>>,
    output_cancellation: CancellationToken,
}

impl<V: StreamData> ManualOutputHandler<V> {
    pub fn new(executor: Rc<LocalExecutor<'static>>, var_names: BTreeSet<VarName>) -> Self {
        let mut stream_senders: BTreeMap<VarName, oneshot::Sender<OutputStream<V>>> =
            BTreeMap::new();
        let mut stream_receivers: BTreeMap<VarName, oneshot::Receiver<OutputStream<V>>> =
            BTreeMap::new();

        for (_, var_name) in var_names.iter().enumerate() {
            let (tx, rx) = oneshot::channel().into_split();
            stream_senders.insert(var_name.clone(), tx);
            stream_receivers.insert(var_name.clone(), rx);
        }
        let (output_sender, output_receiver) = oneshot::channel().into_split();
        let cancellation = CancellationToken::new();

        // Add debug task to monitor when cancellation is triggered
        if enabled!(Level::DEBUG) {
            let cancellation_debug = cancellation.clone();
            executor
                .spawn(async move {
                    cancellation_debug.cancelled().await;
                    debug!("ManualOutputHandler: Cancellation token was triggered!");
                })
                .detach();
        }

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

    pub fn get_output(&mut self) -> OutputStream<BTreeMap<VarName, V>> {
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

    #[instrument(skip(self, streams))]
    fn provide_streams(&mut self, streams: BTreeMap<VarName, OutputStream<V>>) {
        debug!(num_streams = streams.len(), "Providing streams",);
        let mut senders = self
            .stream_senders
            .take()
            .expect("Stream senders not found");
        assert!(
            self.var_names == streams.keys().cloned().collect(),
            "Variable names provided do not match variable names provided in constructor."
        );
        for (var_name, stream) in streams {
            let sender = senders
                .remove(&var_name)
                .unwrap_or_else(|| panic!("No sender found for variable: {}", var_name));
            assert!(sender.send(stream).is_ok());
        }
    }

    #[instrument(name="Running ManualOutputHandler", level=Level::INFO, skip(self))]
    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let mut receivers: BTreeMap<VarName, oneshot::Receiver<OutputStream<V>>> =
            mem::take(&mut self.stream_receivers).expect("Stream receivers already taken");

        debug!(num_streams = receivers.len(), "Running ManualOutputHandler");

        let mut streams: anyhow::Result<BTreeMap<VarName, OutputStream<V>>> = Ok(BTreeMap::new());
        for name in self.var_names.iter() {
            match receivers.remove(name) {
                Some(mut rx) => match rx.try_recv() {
                    Ok(stream) => {
                        debug!("Received stream for variable: {}", name);
                        streams.as_mut().unwrap().insert(name.clone(), stream);
                    }
                    Err(_) => {
                        error!(
                            "Stream sender was dropped before providing stream for variable: {}",
                            name
                        );
                        streams = Err(anyhow!("Stream sender dropped for variable: {}", name));
                        break;
                    }
                },
                None => {
                    error!("No receiver found for variable: {}", name);
                    streams = Err(anyhow!("No receiver found for variable: {}", name));
                    break;
                }
            }
        }

        let (result_tx, result_rx) = oneshot::channel().into_split();
        let output_cancellation = self.output_cancellation.clone();
        let output_sender = mem::take(&mut self.output_sender).expect("Output sender not found");

        Box::pin(async move {
            let mut streams = match streams {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        "ManualOutputHandler: Stream generator failed with error: {}",
                        e
                    );
                    return Err(e);
                }
            };

            // The logic behind the output stream
            let output_logic = stream! {
                if streams.is_empty() {
                    debug!("ManualOutputHandler: No streams provided, output stream will be empty");
                    let _ = result_tx.send(Ok(()));
                    return;
                }
                loop {
                    let num_streams = streams.len();
                    debug!("ManualOutputHandler: Stream generator loop iteration, {} streams", num_streams);

                    let nexts = join_all(streams.iter_mut().map(|(n, s)| s.next().map(|v| match v {
                        Some(val) => Some((n.clone(), val)),
                        None => None,
                    })));
                    debug!("ManualOutputHandler: Waiting for values from streams");

                    if let Some(vals) = nexts.await
                        .into_iter()
                        .collect::<Option<BTreeMap<VarName, V>>>()
                    {
                        // Collect the values into a Vec<V>
                        let output = vals;
                        // Output the combined data
                        debug!(?output, "Outputting data");
                        yield output;
                    } else {
                        // One of the streams ended, so we should stop
                        debug!(
                            "Stopping ManualOutputHandler with len(nexts) = {}",
                            num_streams
                        );
                        break;
                    }
                }
                debug!("ManualOutputHandler: Stream generator completed naturally");
                let _ = result_tx.send(Ok(()));
            };

            // Send it to self.output_receiver
            output_sender
                .send(Box::pin(output_logic))
                .expect("Output sender channel dropped");

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
                _ = output_cancellation.cancelled().fuse() => {
                    debug!("ManualOutputHandler: Received cancellation signal, completing handler");
                    Ok(())
                }
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
    use crate::{OutputStream, Value};
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
                NoVal => 0,
                Deferred => 1,
                Unit => 2,
                Bool(_) => 3,
                Int(_) => 4,
                Float(_) => 5,
                Str(_) => 6,
                List(_) => 7,
                Map(_) => 8,
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
                _ => Ordering::Equal, // Unit and Deferred are considered equal within their kind
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
        let xy_expected: Vec<BTreeMap<VarName, Value>> = (0..10)
            .map(|x| {
                BTreeMap::from([
                    ("x".into(), (x * 2).into()),
                    ("y".into(), (x * 2 + 1).into()),
                ])
            })
            .collect();
        let mut handler: ManualOutputHandler<Value> =
            ManualOutputHandler::new(ex.clone(), BTreeSet::from(["x".into(), "y".into()]));

        let streams = BTreeMap::from([("x".into(), x_stream), ("y".into(), y_stream)]);
        handler.provide_streams(streams);

        let run_fut = handler.run();
        let output_stream = handler.get_output();

        let task = ex.spawn(run_fut);

        let output: Vec<BTreeMap<VarName, Value>> = output_stream.collect().await;

        assert_eq!(output, xy_expected);

        task.await.expect("Failed to run handler");
    }

    #[apply(async_test)]
    async fn test_manual_output_handler_completion(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        use futures::stream;

        info!("Creating ManualOutputHandler with infinite input stream");
        let mut handler =
            ManualOutputHandler::new(executor.clone(), BTreeSet::from(["test".into()]));

        // Create an infinite stream
        let infinite_stream: OutputStream<Value> =
            Box::pin(stream::iter((0..).map(|x| Value::Int(x))));
        let streams = BTreeMap::from([("test".into(), infinite_stream)]);
        handler.provide_streams(streams);

        let output_stream = handler.get_output();
        let handler_task = executor.spawn(handler.run());

        info!("Taking only 2 items from infinite stream");
        let outputs = output_stream.take(2).collect::<Vec<_>>().await;
        info!("Collected outputs: {:?}", outputs);

        info!("Output stream dropped, waiting for handler to complete");
        let timeout_future = smol::Timer::after(std::time::Duration::from_secs(2));

        futures::select! {
            result = handler_task.fuse() => {
                info!("Handler completed: {:?}", result);
                result?;
            }
            _ = futures::FutureExt::fuse(timeout_future) => {
                return Err(anyhow::anyhow!("ManualOutputHandler did not complete after output stream was dropped"));
            }
        }

        Ok(())
    }

    #[apply(async_test)]
    async fn test_manual_output_handler_hang(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        info!("Creating ManualOutputHandler...");
        let mut handler =
            ManualOutputHandler::new(executor.clone(), BTreeSet::from(["test".into()]));

        // Create a test stream that never ends
        let test_stream: OutputStream<Value> = Box::pin(stream::iter((0..).map(|x| Value::Int(x))));

        info!("Providing infinite stream to handler...");
        let streams = BTreeMap::from([("test".into(), test_stream)]);
        handler.provide_streams(streams);

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
