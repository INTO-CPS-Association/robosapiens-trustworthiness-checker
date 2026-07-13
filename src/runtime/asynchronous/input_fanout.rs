use std::collections::BTreeMap;

use async_stream::try_stream;
use futures::StreamExt;
use unsync::spsc;

use crate::core::DeferrableStreamData;
use crate::{InputStream, OutputStream, VarName};

const CHANNEL_SIZE: usize = 10;

pub(super) struct InputFanout<V> {
    pub streams: BTreeMap<VarName, OutputStream<V>>,
    pub drive: OutputStream<anyhow::Result<()>>,
}

pub(super) fn fan_out_input<V>(
    input: InputStream<V>,
    selection: std::collections::BTreeSet<VarName>,
) -> InputFanout<V>
where
    V: DeferrableStreamData,
{
    let indices = selection
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, var)| (var, index))
        .collect::<BTreeMap<_, _>>();
    let (senders, streams): (Vec<_>, Vec<_>) = indices
        .values()
        .map(|_| {
            let (sender, receiver) = spsc::channel(CHANNEL_SIZE);
            let output: OutputStream<V> = crate::stream_utils::channel_to_output_stream(receiver);
            (Some(sender), output)
        })
        .unzip();
    let width = senders.len();
    let mut senders = senders;
    let tick_indices = indices.clone();
    let drive = Box::pin(try_stream! {
        let mut ticks = crate::into_tick_stream(input);
        while let Some(tick) = ticks.next().await {
            let mut values = vec![V::no_val_value(); width];
            for event in tick? {
                let index = tick_indices.get(&event.var).copied().ok_or_else(|| {
                    anyhow::anyhow!(
                        "input stream emitted undeclared async variable `{}`",
                        event.var
                    )
                })?;
                values[index] = event.value;
            }
            for (sender, value) in senders.iter_mut().zip(values) {
                if let Some(active) = sender
                    && active.send(value).await.is_err()
                {
                    *sender = None;
                }
            }
            yield ();
            if senders.iter().all(Option::is_none) {
                return;
            }
        }
    });

    let streams = indices.into_keys().zip(streams).collect();
    InputFanout { streams, drive }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use futures::StreamExt;

    use crate::io::map;
    use crate::{InputBatch, InputEvent, InputStream, Value, VarName};

    use super::fan_out_input;

    #[test]
    fn step_batches_become_synchronized_variable_streams() {
        smol::block_on(async {
            let input = map::input_stream(BTreeMap::from([
                (VarName::new("x"), vec![Value::Int(1), Value::Int(2)]),
                (VarName::new("y"), vec![Value::Int(10), Value::Int(20)]),
            ]));
            let fanout = fan_out_input(
                input,
                std::collections::BTreeSet::from([VarName::new("x"), VarName::new("y")]),
            );
            let mut streams = fanout.streams;
            let mut drive = fanout.drive;
            let mut x = streams.remove(&VarName::new("x")).unwrap();
            let mut y = streams.remove(&VarName::new("y")).unwrap();

            for expected in [(1, 10), (2, 20)] {
                let (step, x, y) = futures::join!(drive.next(), x.next(), y.next());
                assert!(matches!(step, Some(Ok(()))));
                assert_eq!(x, Some(Value::Int(expected.0)));
                assert_eq!(y, Some(Value::Int(expected.1)));
            }
            assert!(drive.next().await.is_none());
        });
    }

    #[test]
    fn undeclared_event_variable_is_reported_by_driver() {
        smol::block_on(async {
            let input: InputStream<Value> =
                Box::pin(futures::stream::iter([Ok(InputBatch::events(vec![
                    InputEvent::new(VarName::new("unknown"), Value::Int(1)),
                ]))]));
            let mut drive =
                fan_out_input(input, std::collections::BTreeSet::from([VarName::new("x")])).drive;

            let error = drive.next().await.unwrap().unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("undeclared async variable `unknown`")
            );
        });
    }

    #[test]
    fn malformed_fixed_width_steps_are_reported_by_driver() {
        smol::block_on(async {
            let input: InputStream<Value> =
                Box::pin(futures::stream::iter([InputBatch::packed_steps(
                    std::num::NonZeroUsize::new(2).unwrap(),
                    vec![InputEvent::new(VarName::new("x"), Value::Int(1))],
                )]));
            let mut drive =
                fan_out_input(input, std::collections::BTreeSet::from([VarName::new("x")])).drive;

            let error = drive.next().await.unwrap().unwrap_err();
            assert!(error.to_string().contains("not divisible"));
        });
    }
}
