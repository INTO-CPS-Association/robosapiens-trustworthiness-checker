use async_channel::{Receiver, Sender};
use futures::StreamExt;

use crate::{InputBatch, InputStream, into_tick_stream};

/// Handle for releasing and awaiting logical input ticks one at a time.
pub struct InputController {
    permits: Sender<()>,
    completed: Receiver<()>,
}

impl InputController {
    /// Allow one logical tick and wait until the runtime is ready to accept
    /// another. With [`crate::ExecutionPolicy::Synchronous`], readiness means
    /// the released tick's downstream computation has completed.
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

/// Adds deterministic tick-by-tick delivery control to an input stream.
///
/// Unwrapped streams pay neither a branch nor storage cost for input control.
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
    use std::{cell::RefCell, collections::BTreeMap, num::NonZeroUsize, rc::Rc};

    use crate::{InputBatch, InputEvent, InputStream, Value, VarName, io::map};

    use super::controlled;

    #[test]
    fn releases_exactly_one_logical_tick_per_permit() {
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

    #[test]
    fn splits_packed_transport_batches_at_logical_tick_boundaries() {
        smol::block_on(async {
            let packed = InputBatch::packed_steps(
                NonZeroUsize::new(2).unwrap(),
                vec![
                    InputEvent::new("x".into(), 1),
                    InputEvent::new("y".into(), 2),
                    InputEvent::new("x".into(), 3),
                    InputEvent::new("y".into(), 4),
                ],
            )
            .unwrap();
            let input: InputStream<i32> = Box::pin(futures::stream::iter([Ok(packed)]));
            let (mut batches, controller) = controlled(input);

            let observed = Rc::new(RefCell::new(Vec::new()));
            let consumed = observed.clone();
            let drive = async move {
                while let Some(batch) = batches.next().await {
                    consumed.borrow_mut().push(
                        batch
                            .unwrap()
                            .ticks()
                            .next()
                            .unwrap()
                            .iter()
                            .map(|event| event.value)
                            .collect::<Vec<_>>(),
                    );
                }
            };
            let control = async {
                controller.advance().await.unwrap();
                assert_eq!(&*observed.borrow(), &[vec![1, 2]]);
                controller.advance().await.unwrap();
                assert_eq!(&*observed.borrow(), &[vec![1, 2], vec![3, 4]]);
            };
            futures::join!(drive, control);
        });
    }
}
