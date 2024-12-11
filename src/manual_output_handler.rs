use std::{collections::BTreeMap, future::Future, mem, pin::Pin};

use async_trait::async_trait;
use futures::future::join_all;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    // use tokio_stream as stream;

    use crate::{OutputStream, Value, VarName};
    use futures::stream;

    use super::*;

    #[tokio::test]
    async fn test_combined_output() {
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
}
