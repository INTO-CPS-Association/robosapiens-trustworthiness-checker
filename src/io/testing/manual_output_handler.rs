use std::{collections::BTreeMap, future::Future, mem, pin::Pin};

use async_trait::async_trait;
use futures::future::join_all;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tracing::{Level, debug, info, instrument};

use crate::core::{OutputHandler, OutputStream, StreamData, VarName};

/* Some members are defined as Option<T> as either they are provided after
 * construction by provide_streams or once they are used they are taken and
 * cannot be used again; this allows us to manage the lifetimes of our data
 * without mutexes or arcs. */
pub struct ManualOutputHandler<V: StreamData> {
    var_names: Vec<VarName>,
    stream_senders: Option<Vec<oneshot::Sender<OutputStream<V>>>>,
    stream_receivers: Option<Vec<oneshot::Receiver<OutputStream<V>>>>,
    output_sender: Option<mpsc::Sender<BTreeMap<VarName, V>>>,
    output_receiver: Option<mpsc::Receiver<BTreeMap<VarName, V>>>,
}

impl<V: StreamData> ManualOutputHandler<V> {
    pub fn new(var_names: Vec<VarName>) -> Self {
        let (stream_senders, stream_receivers): (
            Vec<oneshot::Sender<OutputStream<V>>>,
            Vec<oneshot::Receiver<OutputStream<V>>>,
        ) = var_names.iter().map(|_| oneshot::channel()).unzip();
        let (output_sender, output_receiver) = mpsc::channel(10);
        Self {
            var_names,
            stream_senders: Some(stream_senders),
            stream_receivers: Some(stream_receivers),
            output_receiver: Some(output_receiver),
            output_sender: Some(output_sender),
        }
    }

    pub fn get_output(&mut self) -> OutputStream<BTreeMap<VarName, V>> {
        Box::pin(ReceiverStream::new(
            self.output_receiver
                .take()
                .expect("Output receiver missing"),
        ))
    }
}

#[async_trait]
impl<V: StreamData> OutputHandler<V> for ManualOutputHandler<V> {
    #[instrument(skip(self, streams))]
    fn provide_streams(&mut self, mut streams: BTreeMap<VarName, OutputStream<V>>) {
        debug!(name: "Providing streams",
            num_streams = self.var_names.len());
        for (var_name, sender) in self
            .var_names
            .iter()
            .zip(self.stream_senders.take().unwrap())
        {
            let stream = streams
                .remove(var_name)
                .expect(format!("Stream for {} not found", var_name).as_str());
            assert!(sender.send(stream).is_ok());
        }
    }

    #[instrument(name="Running ManualOutputHandler", level=Level::INFO,
                 skip(self))]
    fn run(&mut self) -> Pin<Box<dyn Future<Output = ()> + 'static + Send>> {
        let receivers = mem::take(&mut self.stream_receivers).expect("Stream receivers not found");
        info!(
            name = "Running ManualOutputHandler",
            num_streams = receivers.len()
        );
        let mut streams: Vec<_> = receivers
            .into_iter()
            .map(|mut r| r.try_recv().unwrap())
            .collect();
        let output_sender = mem::take(&mut self.output_sender).expect("Output sender not found");
        let var_names = self.var_names.clone();

        // let receivers = receivers;
        // let mut streams = streams;
        // let output_sender = output_sender;

        Box::pin(async move {
            loop {
                let nexts = streams.iter_mut().map(|s| s.next());

                // Stop outputting when any of the streams ends, otherwise collect
                // all of the values
                if let Some(vals) = join_all(nexts)
                    .await
                    .into_iter()
                    .collect::<Option<Vec<V>>>()
                {
                    // Combine the values into a single map
                    let output: BTreeMap<VarName, V> =
                        var_names.iter().cloned().zip(vals.into_iter()).collect();
                    // Output the combined data
                    debug!(name = "Outputting data", ?output);
                    output_sender.send(output).await.unwrap();
                } else {
                    // One of the streams has ended, so we should stop
                    info!(
                        "Stopping ManualOutputHandler with len(nexts) = {}",
                        streams.len()
                    );
                    break;
                }
            }
        })
    }
}

pub struct AsyncManualOutputHandler<V: StreamData> {
    var_names: Vec<VarName>,
    stream_senders: Option<Vec<oneshot::Sender<OutputStream<V>>>>,
    stream_receivers: Option<Vec<oneshot::Receiver<OutputStream<V>>>>,
    output_sender: Option<mpsc::Sender<(VarName, V)>>,
    output_receiver: Option<mpsc::Receiver<(VarName, V)>>,
}

#[async_trait]
impl<V: StreamData> OutputHandler<V> for AsyncManualOutputHandler<V> {
    fn provide_streams(&mut self, mut streams: BTreeMap<VarName, OutputStream<V>>) {
        for (var_name, sender) in self
            .var_names
            .iter()
            .zip(self.stream_senders.take().unwrap())
        {
            let stream = streams
                .remove(var_name)
                .expect(format!("Stream for {} not found", var_name).as_str());
            assert!(sender.send(stream).is_ok());
        }
    }

    fn run(&mut self) -> Pin<Box<dyn Future<Output = ()> + 'static + Send>> {
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
        })
    }
}

impl<V: StreamData> AsyncManualOutputHandler<V> {
    pub fn new(var_names: Vec<VarName>) -> Self {
        let (stream_senders, stream_receivers): (
            Vec<oneshot::Sender<OutputStream<V>>>,
            Vec<oneshot::Receiver<OutputStream<V>>>,
        ) = var_names.iter().map(|_| oneshot::channel()).unzip();
        let (output_sender, output_receiver) = mpsc::channel(10);
        Self {
            var_names,
            stream_senders: Some(stream_senders),
            stream_receivers: Some(stream_receivers),
            output_receiver: Some(output_receiver),
            output_sender: Some(output_sender),
        }
    }

    pub fn get_output(&mut self) -> OutputStream<(VarName, V)> {
        Box::pin(ReceiverStream::new(
            self.output_receiver
                .take()
                .expect("Output receiver missing"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::collections::BTreeSet;

    use super::*;
    use crate::{OutputStream, Value, VarName};
    use futures::StreamExt;
    use futures::stream;
    use test_log::test;

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
                Str(_) => 4,
                List(_) => 5,
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

    #[test(tokio::test)]
    async fn sync_test_combined_output() {
        let x_stream: OutputStream<Value> = Box::pin(stream::iter((0..10).map(|x| (x * 2).into())));
        let y_stream: OutputStream<Value> =
            Box::pin(stream::iter((0..10).map(|x| (x * 2 + 1).into())));
        let xy_expected: Vec<BTreeMap<VarName, Value>> = (0..10)
            .map(|x| {
                vec![
                    (VarName("x".to_string()), (x * 2).into()),
                    (VarName("y".to_string()), (x * 2 + 1).into()),
                ]
                .into_iter()
                .collect()
            })
            .collect();
        let mut handler: ManualOutputHandler<Value> =
            ManualOutputHandler::new(vec![VarName("x".to_string()), VarName("y".to_string())]);

        handler.provide_streams(
            vec![
                (VarName("x".to_string()), x_stream),
                (VarName("y".to_string()), y_stream),
            ]
            .into_iter()
            .collect(),
        );

        //
        let output_stream = handler.get_output();

        let task = tokio::spawn(handler.run());

        let output: Vec<BTreeMap<VarName, Value>> = output_stream.collect().await;

        assert_eq!(output, xy_expected);

        task.await.unwrap();
    }

    #[test(tokio::test)]
    async fn async_test_combined_output() {
        // Helper to create a named stream with delay
        fn create_stream(
            name: &str,
            multiplier: i64,
            offset: i64,
        ) -> (VarName, OutputStream<Value>) {
            let var_name = VarName(name.to_string());
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
        let mut handler = AsyncManualOutputHandler::new(vec![x_name.clone(), y_name.clone()]);
        handler.provide_streams(
            vec![(x_name, x_stream), (y_name, y_stream)]
                .into_iter()
                .collect::<BTreeMap<_, _>>(),
        );

        // Run the handler and validate output
        let output_stream = handler.get_output();
        let task = tokio::spawn(handler.run());
        let results = output_stream.collect::<BTreeSet<_>>().await;

        assert_eq!(results, expected_output);
        task.await.unwrap();
    }
}
