use std::collections::BTreeMap;

use futures::{StreamExt, future::join_all};
use unsync::spsc::{self, Sender};

use crate::{InputBatch, InputStream, InputUpdate, LocalStream, VarName};

const CHANNEL_SIZE: usize = 10;

pub struct ManualInputController<V> {
    sender: Sender<InputBatch<V>>,
}

impl<V> ManualInputController<V> {
    /// Send one validated simultaneous input tick.
    pub async fn send_tick(&mut self, updates: Vec<InputUpdate<V>>) -> anyhow::Result<()> {
        let tick = InputBatch::tick(updates)?;
        self.sender
            .send(tick)
            .await
            .map_err(|_| anyhow::anyhow!("manual input stopped"))
    }
}

/// Create a manually driven input stream and its controller.
pub fn channel<V: 'static>() -> (InputStream<V>, ManualInputController<V>) {
    let (sender, receiver) = spsc::channel(CHANNEL_SIZE);
    let mut batches = crate::stream_utils::channel_to_output_stream(receiver);
    (
        Box::pin(async_stream::stream! {
            while let Some(batch) = batches.next().await {
                yield Ok(batch);
            }
        }),
        ManualInputController { sender },
    )
}

pub(crate) fn from_streams<V: 'static>(
    streams: BTreeMap<VarName, LocalStream<V>>,
) -> InputStream<V> {
    let mut streams = streams.into_iter().collect::<Vec<_>>();
    Box::pin(async_stream::try_stream! {
        loop {
            let values = join_all(streams.iter_mut().map(|(_, stream)| stream.next())).await;
            let updates = streams
                .iter()
                .map(|(var, _)| var.clone())
                .zip(values)
                .filter_map(|(var, value)| value.map(|value| InputUpdate::new(var, value)))
                .collect::<Vec<_>>();
            if updates.is_empty() {
                return;
            }
            yield InputBatch::tick(updates)?;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_input_delivers_simultaneous_steps() {
        smol::block_on(async {
            // The controller channel carries typed steps, so the stream is a
            // `InputStream` of one simultaneous tick per sent step.
            let (mut stream, mut controller): (InputStream<i32>, _) = channel();
            controller
                .send_tick(vec![
                    InputUpdate::new("x".into(), 1),
                    InputUpdate::new("y".into(), 2),
                ])
                .await
                .unwrap();

            let batch = stream.next().await.unwrap().unwrap();
            assert_eq!(batch.tick_count(), 1);
            assert_eq!(
                batch.ticks().next().unwrap().to_updates(),
                [
                    InputUpdate::new("x".into(), 1),
                    InputUpdate::new("y".into(), 2),
                ]
            );
        });
    }

    #[test]
    fn send_tick_rejects_duplicate_variables() {
        smol::block_on(async {
            let (_stream, mut controller) = channel();
            let error = controller
                .send_tick(vec![
                    InputUpdate::new("x".into(), 1),
                    InputUpdate::new("x".into(), 2),
                ])
                .await
                .unwrap_err();

            assert!(error.to_string().contains("duplicate variable `x`"));
        });
    }

    #[test]
    fn send_tick_rejects_empty_ticks() {
        smol::block_on(async {
            let (_stream, mut controller) = channel::<()>();
            let error = controller.send_tick(Vec::new()).await.unwrap_err();
            assert_eq!(
                error.to_string(),
                "input tick must contain at least one update"
            );
        });
    }
}
