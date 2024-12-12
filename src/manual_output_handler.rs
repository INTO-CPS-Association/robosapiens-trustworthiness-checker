use std::{collections::BTreeMap, future::Future, mem, pin::Pin};

use async_trait::async_trait;
use futures::future::join_all;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

use crate::{
    core::{OutputHandler, OutputStream, StreamData, VarName},
    Value,
};

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
                    output_sender.send(output).await.unwrap();
                } else {
                    // One of the streams has ended, so we should stop
                    break;
                }
            }
        })
    }
}


pub struct AsyncManualOutputHandler {
    var_names: Vec<VarName>,
    stream_senders: Option<Vec<oneshot::Sender<OutputStream<Value>>>>,
    stream_receivers: Option<Vec<oneshot::Receiver<OutputStream<Value>>>>,
    output_sender: Option<mpsc::Sender<(VarName, Value)>>,
    output_receiver: Option<mpsc::Receiver<(VarName, Value)>>,
}

#[async_trait]
impl OutputHandler<Value> for AsyncManualOutputHandler {
    fn provide_streams(&mut self, mut streams: BTreeMap<VarName, OutputStream<Value>>) {
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
                        {
                            let mut stream = stream;
                            let output_sender = output_sender.clone();
                            async move {
                                while let Some(data) = stream.next().await {
                                    let _ = output_sender.send((var_name.clone(), data)).await;
                                }
                            }
                        }
                    })
                    .collect::<Vec<_>>(),
            )
            .await;
        })
    }
}

impl AsyncManualOutputHandler {
    pub fn new(var_names: Vec<VarName>) -> Self {
        let (stream_senders, stream_receivers): (
            Vec<oneshot::Sender<OutputStream<Value>>>,
            Vec<oneshot::Receiver<OutputStream<Value>>>,
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

    pub fn get_output(&mut self) -> OutputStream<(VarName, Value)> {
        Box::pin(ReceiverStream::new(
            self.output_receiver
                .take()
                .expect("Output receiver missing"),
        ))
    }
}


#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use std::time::Duration;

    // use tokio_stream as stream;

    use crate::{OutputStream, Value, VarName};
    use futures::stream;
    use tokio::time::interval;
    use tokio_stream::wrappers::IntervalStream;

    use super::*;
    #[tokio::test]
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

    #[tokio::test]
    async fn async_test_combined_output() {
        // Helper to create a named stream with delay
        fn create_stream(name: &str, multiplier: i64, offset: i64) -> (VarName, OutputStream<Value>) {
            let var_name = VarName(name.to_string());
            // Delay to force expected ordering of the streams
            let interval = IntervalStream::new(interval(Duration::from_millis(5)));
            let stream = Box::pin(
                stream::iter(0..10).zip(interval).map(move |(x, _)| (multiplier * x + offset).into()),
            );
            (var_name, stream)
        }

        // Prepare input streams
        let (x_name, x_stream) = create_stream("x", 2, 0);
        let (y_name, y_stream) = create_stream("y", 2, 1);

        // Prepare expected output
        let expected_output: Vec<_> = (0..10)
            .flat_map(|x| vec![
                (x_name.clone(), (x * 2).into()),
                (y_name.clone(), (x * 2 + 1).into()),
            ])
            .collect();

        // Initialize the handler
        let mut handler = AsyncManualOutputHandler::new(vec![x_name.clone(), y_name.clone()]);
        handler.provide_streams(
            vec![(x_name, x_stream), (y_name, y_stream)]
                .into_iter()
                .collect::<BTreeMap<_, _>>(),
        );

        // Run the handler and validate output
        let mut output_stream = handler.get_output();
        let task = tokio::spawn(handler.run());

        for expected in &expected_output {
            let value = output_stream.next().await.unwrap();
            assert_eq!(value, *expected);
        }

        task.await.unwrap();
    }
}
