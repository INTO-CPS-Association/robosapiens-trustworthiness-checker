use crate::core::{FileInputValue, InputBatch, InputStream, Value, VarName};
pub use crate::lang::untimed_input::UntimedInputFileData;
use crate::lang::untimed_input::parser::PackedUntimedInput;
use std::collections::BTreeSet;

const FILE_INPUT_BATCH_TICKS: usize = 1_024;

impl FileInputValue for Value {
    fn decode_file_value(payload: &str) -> anyhow::Result<Self> {
        let mut remaining = payload;
        let value = crate::lang::core::parser::val_or_container(&mut remaining)
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        anyhow::ensure!(
            remaining.trim().is_empty(),
            "unexpected trailing input after file value: {:?}",
            remaining.trim()
        );
        Ok(value)
    }

    fn missing_value() -> Self {
        Value::NoVal
    }
}

/// Stream the selected variables from parsed untimed input data.
pub fn input_stream(data: UntimedInputFileData, vars: BTreeSet<VarName>) -> InputStream<Value> {
    if vars.is_empty() {
        return Box::pin(futures::stream::empty());
    }

    let vars = vars.into_iter().collect::<Vec<_>>();
    let tick_width = vars.len();
    Box::pin(async_stream::try_stream! {
        if let Some(max_key) = data.keys().max().copied() {
            let mut start = 0;
            while start <= max_key {
                let end = start.saturating_add(FILE_INPUT_BATCH_TICKS - 1).min(max_key);
                let ticks = end - start + 1;
                let mut values = Vec::with_capacity(ticks * tick_width);
                for time in start..=end {
                    let row = data.get(&time);
                    values.extend(vars.iter().map(|var| {
                        row
                            .and_then(|data_for_time| data_for_time.get(var).cloned())
                            .unwrap_or(Value::NoVal)
                    }));
                }
                yield InputBatch::trusted_packed_rows(vars.clone().into_boxed_slice(), values);
                if end == max_key {
                    break;
                }
                start = end + 1;
            }
        }
    })
}

/// Stream sparse file rows in bounded, fixed-layout batches.
pub(crate) fn packed_input_stream<V: FileInputValue>(
    data: PackedUntimedInput<V>,
) -> InputStream<V> {
    let PackedUntimedInput {
        layout,
        rows,
        end_time,
    } = data;
    if layout.is_empty() {
        return Box::pin(futures::stream::empty());
    }

    let tick_width = layout.len();
    Box::pin(async_stream::try_stream! {
        if let Some(max_key) = end_time {
            let mut start = 0;
            while start <= max_key {
                let end = start.saturating_add(FILE_INPUT_BATCH_TICKS - 1).min(max_key);
                let ticks = end - start + 1;
                let mut values = Vec::with_capacity(ticks * tick_width);
                for time in start..=end {
                    if let Some(row) = rows.get(&time) {
                        values.extend(row.iter().cloned());
                    } else {
                        values.extend(std::iter::repeat_with(V::missing_value).take(tick_width));
                    }
                }
                yield InputBatch::trusted_packed_rows(layout.clone(), values);
                if end == max_key {
                    break;
                }
                start = end + 1;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use crate::async_test;
    use futures::StreamExt;
    use macro_rules_attribute::apply;
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::core::{InputEvent, Value};

    fn sample_data() -> UntimedInputFileData {
        let mut data: UntimedInputFileData = BTreeMap::new();
        data.insert(0, {
            let mut map = BTreeMap::new();
            map.insert("x".into(), Value::Int(1));
            map.insert("y".into(), Value::Int(10));
            map
        });
        data.insert(1, {
            let mut map = BTreeMap::new();
            map.insert("x".into(), Value::Int(2));
            map.insert("y".into(), Value::Int(11));
            map
        });
        data
    }

    #[apply(async_test)]
    async fn input_batches_preserve_file_rows() {
        let vars = BTreeSet::from(["x".into(), "y".into()]);
        let ticks = crate::into_tick_stream(input_stream(sample_data(), vars))
            .map(Result::unwrap)
            .collect::<Vec<_>>()
            .await;

        assert_eq!(
            ticks,
            vec![
                vec![
                    InputEvent::new("x".into(), Value::Int(1)),
                    InputEvent::new("y".into(), Value::Int(10))
                ],
                vec![
                    InputEvent::new("x".into(), Value::Int(2)),
                    InputEvent::new("y".into(), Value::Int(11))
                ],
            ]
        );
    }

    #[apply(async_test)]
    async fn selected_input_emits_only_configured_variables() {
        let ticks = crate::into_tick_stream(input_stream(
            sample_data(),
            BTreeSet::from([VarName::new("y")]),
        ))
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .await;

        assert!(
            ticks
                .iter()
                .flatten()
                .all(|event| event.var == VarName::new("y"))
        );
    }

    #[apply(async_test)]
    async fn timed_file_input_preserves_delivery_ticks_and_signal_time() {
        use crate::core::StreamData;
        use crate::runtime::mstlo::{MstloTimedValue, MstloValue};
        use std::time::Duration;

        let packed = crate::lang::untimed_input::parser::packed_untimed_input::<MstloTimedValue>(
            "0: x = {\"time\": 0, \"value\": 7.0}\n2: x = {\"time\": 1000, \"value\": 4.0}",
            BTreeSet::from([VarName::new("x")]),
        )
        .unwrap();
        let mut stream = packed_input_stream(packed);
        let first = stream.next().await.unwrap().unwrap();
        let mut ticks = first.ticks();
        assert_eq!(
            ticks.next().unwrap().to_events()[0].value,
            MstloTimedValue::new(Duration::ZERO, MstloValue::Float(7.0))
        );
        assert!(ticks.next().unwrap().to_events()[0].value.is_no_val());
        assert_eq!(
            ticks.next().unwrap().to_events()[0].value,
            MstloTimedValue::new(Duration::from_millis(1000), MstloValue::Float(4.0))
        );
    }

    #[apply(async_test)]
    async fn packed_input_stream_yields_before_expanding_a_large_gap() {
        let packed = crate::lang::untimed_input::parser::packed_untimed_input(
            "0: x = 1\n1000000000: x = 2",
            BTreeSet::from([VarName::new("x")]),
        )
        .unwrap();
        let mut stream = packed_input_stream(packed);
        let first_batch = stream.next().await.unwrap().unwrap();
        let mut ticks = first_batch.ticks();

        assert_eq!(
            ticks.next().unwrap().to_events(),
            [InputEvent::new("x".into(), Value::Int(1))]
        );
        assert_eq!(ticks.count(), FILE_INPUT_BATCH_TICKS - 1);
    }
}
