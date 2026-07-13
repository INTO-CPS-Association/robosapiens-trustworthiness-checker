use std::collections::BTreeMap;

use futures::{StreamExt, future::join_all};
use unsync::spsc::{self, Sender};

use crate::{InputBatch, InputEvent, InputStream, OutputStream, VarName};

const CHANNEL_SIZE: usize = 10;

pub struct ManualInputController<V> {
    sender: Sender<InputBatch<V>>,
}

impl<V> ManualInputController<V> {
    /// Send one validated simultaneous input step.
    pub async fn send_step(&mut self, events: Vec<InputEvent<V>>) -> anyhow::Result<()> {
        let tick = InputBatch::step(events)?;
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
    streams: BTreeMap<VarName, OutputStream<V>>,
) -> InputStream<V> {
    let mut streams = streams.into_iter().collect::<Vec<_>>();
    Box::pin(async_stream::try_stream! {
        loop {
            let values = join_all(streams.iter_mut().map(|(_, stream)| stream.next())).await;
            let events = streams
                .iter()
                .map(|(var, _)| var.clone())
                .zip(values)
                .filter_map(|(var, value)| value.map(|value| crate::InputEvent::new(var, value)))
                .collect::<Vec<_>>();
            if events.is_empty() {
                return;
            }
            yield InputBatch::step(events)?;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_step_rejects_duplicate_variables() {
        smol::block_on(async {
            let (_stream, mut controller) = channel();
            let error = controller
                .send_step(vec![
                    InputEvent::new("x".into(), 1),
                    InputEvent::new("x".into(), 2),
                ])
                .await
                .unwrap_err();

            assert!(error.to_string().contains("duplicate variable `x`"));
        });
    }

    #[test]
    fn send_step_rejects_empty_steps() {
        smol::block_on(async {
            let (_stream, mut controller) = channel::<()>();
            let error = controller.send_step(Vec::new()).await.unwrap_err();
            assert_eq!(
                error.to_string(),
                "input step must contain at least one event"
            );
        });
    }
}
