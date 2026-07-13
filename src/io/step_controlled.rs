use async_channel::{Receiver, Sender};
use futures::StreamExt;

use crate::{InputBatch, InputStream, into_tick_stream};

/// Handle for releasing and awaiting logical input ticks one at a time.
pub struct InputController {
    permits: Sender<()>,
    completed: Receiver<()>,
}

impl InputController {
    /// Release one logical tick and wait until the runtime requests the next.
    /// The acknowledgement means the runtime has requested the next tick.
    pub async fn advance(&self) -> anyhow::Result<()> {
        self.permits
            .send(())
            .await
            .map_err(|_| anyhow::anyhow!("controlled input stream has stopped"))?;
        self.completed.recv().await.map_err(|_| {
            anyhow::anyhow!("controlled input stream stopped before completing the tick")
        })
    }
}

/// Adds deterministic delivery control to a self-driving batch input stream.
///
/// Unwrapped streams pay neither a branch nor storage cost for step control.
pub fn controlled<V: 'static>(inner: InputStream<V>) -> (InputStream<V>, InputController) {
    let (permits, permit_receiver) = async_channel::bounded(1);
    let (completed, completions) = async_channel::bounded(1);
    let mut ticks = into_tick_stream(inner);
    let stepped = Box::pin(async_stream::try_stream! {
        while let Some(tick) = ticks.next().await {
            let tick = tick?;
            if permit_receiver.recv().await.is_err() {
                return;
            }
            yield InputBatch::step(tick)?;
            let _ = completed.send(()).await;
        }
    });
    (
        stepped,
        InputController {
            permits,
            completed: completions,
        },
    )
}

#[cfg(test)]
mod tests {
    use futures::{FutureExt, StreamExt};
    use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

    use crate::{Value, VarName, io::map};

    use super::controlled;

    #[test]
    fn releases_exactly_one_logical_batch_per_permit() {
        smol::block_on(async {
            let inner = map::input_stream(BTreeMap::from([(
                VarName::new("x"),
                vec![Value::Int(1), Value::Int(2)],
            )]));
            let (mut batches, controller) = controlled(inner);

            assert!(batches.next().now_or_never().is_none());
            let values = Rc::new(RefCell::new(Vec::new()));
            let consumed = values.clone();
            let drive = async move {
                while let Some(batch) = batches.next().await {
                    let batch = batch.unwrap();
                    consumed
                        .borrow_mut()
                        .push(batch.ticks().next().unwrap()[0].value.clone());
                }
            };
            let control = async {
                controller.advance().await.unwrap();
                assert_eq!(&*values.borrow(), &[Value::Int(1)]);
                controller.advance().await.unwrap();
                assert_eq!(&*values.borrow(), &[Value::Int(1), Value::Int(2)]);
            };
            futures::join!(drive, control);
        });
    }
}
