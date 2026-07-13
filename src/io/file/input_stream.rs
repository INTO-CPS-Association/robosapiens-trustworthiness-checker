use crate::core::{InputBatch, InputStream, Value, VarName};
pub use crate::lang::untimed_input::UntimedInputFileData;
use std::collections::BTreeSet;

/// Stream the selected variables from parsed untimed input data.
pub fn input_stream(data: UntimedInputFileData, vars: BTreeSet<VarName>) -> InputStream<Value> {
    if vars.is_empty() {
        return Box::pin(futures::stream::empty());
    }

    let vars = vars.into_iter().collect::<Vec<_>>();
    Box::pin(async_stream::try_stream! {
        if let Some(max_key) = data.keys().max().copied() {
            for time in 0..=max_key {
                let row = data.get(&time);
                let batch = vars.iter().map(|var| {
                    let value = row
                        .and_then(|data_for_time| data_for_time.get(var).cloned())
                        .unwrap_or(Value::NoVal);
                    crate::InputEvent::new(var.clone(), value)
                }).collect();
                yield InputBatch::step(batch)?;
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
}
