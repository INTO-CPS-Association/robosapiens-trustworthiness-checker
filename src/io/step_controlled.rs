use async_channel::{Receiver, Sender};
use futures::StreamExt;

use crate::InputBatch;

enum Completion {
    Tick,
    Terminal,
}

struct Permit {
    completion: Sender<Completion>,
}

fn terminal_error() -> anyhow::Error {
    anyhow::anyhow!("controlled input stream terminated before another data tick")
}

async fn terminate_pending(permit_receiver: &Receiver<Permit>) {
    permit_receiver.close();
    while let Ok(permit) = permit_receiver.try_recv() {
        let _ = permit.completion.send(Completion::Terminal).await;
    }
}

/// Handle for releasing and awaiting controlled input batches one logical tick
/// at a time.
pub struct InputController {
    permits: Sender<Permit>,
}

impl InputController {
    pub async fn advance(&self) -> anyhow::Result<()> {
        let (completion, completed) = async_channel::bounded(1);
        self.permits
            .send(Permit { completion })
            .await
            .map_err(|_| terminal_error())?;
        match completed.recv().await.map_err(|_| terminal_error())? {
            Completion::Tick => Ok(()),
            Completion::Terminal => Err(terminal_error()),
        }
    }
}

/// Gate one logical data tick per permit. This adapter is data-only; control
/// messages are handled by the reconfigurable input adapter.
pub fn controlled<V: 'static>(
    inner: impl Into<crate::io::OpenedInput<V>>,
) -> (crate::io::OpenedInput<V>, InputController) {
    let (permits, permit_receiver) = async_channel::bounded::<Permit>(1);
    let input = inner.into().map_stream(|mut batches| {
        Box::pin(async_stream::try_stream! {
            while let Some(batch) = batches.next().await {
                for tick in batch?.into_ticks() {
                    let Ok(permit) = permit_receiver.recv().await else {
                        return;
                    };
                    yield InputBatch::from_ticks(vec![tick])?;
                    let _ = permit.completion.send(Completion::Tick).await;
                }
            }
            terminate_pending(&permit_receiver).await;
        })
    });
    (input, InputController { permits })
}

#[cfg(test)]
mod tests {
    use super::controlled;
    use crate::{InputBatch, InputStream, InputUpdate, Value, VarName};
    use futures::{FutureExt, StreamExt};
    use std::{cell::RefCell, rc::Rc};

    #[test]
    fn releases_exactly_one_logical_tick_per_permit() {
        smol::block_on(async {
            let input: InputStream<Value> =
                Box::pin(futures::stream::iter([Ok(InputBatch::from_ticks(vec![
                    vec![InputUpdate::new(VarName::new("x"), Value::Int(1))],
                    vec![InputUpdate::new(VarName::new("x"), Value::Int(2))],
                ])
                .unwrap())]));
            let (mut batches, controller) = controlled(input);
            assert!(batches.next().now_or_never().is_none());
            let values = Rc::new(RefCell::new(Vec::new()));
            let consumed = values.clone();
            let drive = async move {
                while let Some(batch) = batches.next().await {
                    let batch = batch.unwrap();
                    consumed
                        .borrow_mut()
                        .push(batch.ticks().next().unwrap().to_updates()[0].value.clone());
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
    fn gates_each_row_of_a_packed_batch() {
        smol::block_on(async {
            let packed = InputBatch::packed_rows(
                vec![VarName::new("x"), VarName::new("y")],
                vec![1, 2, 3, 4],
            )
            .unwrap();
            let input: InputStream<i32> = Box::pin(futures::stream::iter([Ok(packed)]));
            let (mut batches, controller) = controlled(input);
            let rows = Rc::new(RefCell::new(Vec::new()));
            let consumed = rows.clone();
            let drive = async move {
                while let Some(batch) = batches.next().await {
                    consumed.borrow_mut().push(
                        batch
                            .unwrap()
                            .ticks()
                            .next()
                            .unwrap()
                            .to_updates()
                            .into_iter()
                            .map(|update| update.value)
                            .collect::<Vec<_>>(),
                    );
                }
            };
            let control = async {
                controller.advance().await.unwrap();
                assert_eq!(&*rows.borrow(), &[vec![1, 2]]);
                controller.advance().await.unwrap();
                assert_eq!(&*rows.borrow(), &[vec![1, 2], vec![3, 4]]);
            };
            futures::join!(drive, control);
        });
    }

    #[test]
    fn eof_does_not_acknowledge_a_nonexistent_tick() {
        smol::block_on(async {
            let input: InputStream<()> = Box::pin(futures::stream::empty());
            let (mut output, controller) = controlled(input);
            assert!(output.next().await.is_none());
            assert!(controller.advance().await.is_err());
        });
    }

    #[test]
    fn normal_termination_after_the_last_tick_does_not_deadlock() {
        smol::block_on(async {
            let input: InputStream<()> = Box::pin(futures::stream::iter([Ok(InputBatch::update(
                VarName::new("x"),
                (),
            ))]));
            let (mut output, controller) = controlled(input);
            let drive = async move {
                assert!(output.next().await.unwrap().is_ok());
                assert!(output.next().await.is_none());
            };
            let control = async {
                controller.advance().await.unwrap();
                assert!(controller.advance().await.is_err());
            };
            futures::join!(drive, control);
        });
    }
}
