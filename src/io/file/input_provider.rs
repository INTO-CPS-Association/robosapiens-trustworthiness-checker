use crate::core::{InputProvider, OutputStream, Value, VarName};
pub use crate::lang::untimed_input::UntimedInputFileData;
use async_trait::async_trait;
use futures::{StreamExt, stream};
use std::collections::{BTreeMap, BTreeSet};

/// Returns an iterator over the values for a given key in the UntimedInputFileData.
/// None if no keys are present.
fn input_file_data_iter(
    data: UntimedInputFileData,
    key: VarName,
) -> Box<dyn Iterator<Item = Value> + 'static> {
    let max_index = data.keys().max();
    if let Some(max_key) = max_index {
        Box::new((0..=*max_key).map(move |time| {
            data.get(&time)
                .and_then(|data_for_time| data_for_time.get(&key).cloned())
                .unwrap_or(Value::NoVal)
        }))
    } else {
        Box::new(std::iter::empty())
    }
}

#[derive(Clone, Debug)]
pub struct FileInputProvider {
    data: UntimedInputFileData,
}

impl FileInputProvider {
    pub fn new(data: UntimedInputFileData) -> Self {
        Self { data }
    }

    pub fn data(&self) -> &UntimedInputFileData {
        &self.data
    }
}

#[async_trait(?Send)]
impl InputProvider for FileInputProvider {
    type Val = Value;

    fn var_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        let iter = input_file_data_iter(self.data.clone(), var.clone());
        Some(Box::pin(stream::iter(iter)))
    }

    fn batched_event_stream(
        &mut self,
        vars: &BTreeSet<VarName>,
    ) -> Option<OutputStream<Vec<(VarName, Value)>>> {
        if vars.is_empty() {
            return None;
        }

        let data = self.data.clone();
        let vars = vars.iter().cloned().collect::<Vec<_>>();
        Some(Box::pin(async_stream::stream! {
            if let Some(max_key) = data.keys().max().copied() {
                for time in 0..=max_key {
                    let row = data.get(&time);
                    let batch = vars
                        .iter()
                        .map(|var| {
                            let value = row
                                .and_then(|data_for_time| data_for_time.get(var).cloned())
                                .unwrap_or(Value::NoVal);
                            (var.clone(), value)
                        })
                        .collect::<Vec<_>>();
                    yield batch;
                }
            }
        }))
    }

    async fn control_stream(&mut self) -> OutputStream<anyhow::Result<()>> {
        let max_index = self.data.keys().max();
        if let Some(max_key) = max_index {
            stream::repeat_with(|| Ok(()))
                .take(*max_key + 1)
                .boxed_local()
        } else {
            stream::empty().boxed_local()
        }
    }
}

/// Backward-compatible InputProvider implementation for the legacy data type.
///
/// This preserves existing call sites that pass `UntimedInputFileData` directly,
/// while preserving existing call sites that pass input data directly.
#[async_trait(?Send)]
impl InputProvider for UntimedInputFileData {
    type Val = Value;

    fn var_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        FileInputProvider::new(self.clone()).var_stream(var)
    }

    fn batched_event_stream(
        &mut self,
        vars: &BTreeSet<VarName>,
    ) -> Option<OutputStream<Vec<(VarName, Value)>>> {
        FileInputProvider::new(self.clone()).batched_event_stream(vars)
    }

    async fn control_stream(&mut self) -> OutputStream<anyhow::Result<()>> {
        FileInputProvider::new(self.clone()).control_stream().await
    }
}

pub fn input_data_for_vars(data: &UntimedInputFileData, vars: &[VarName]) -> UntimedInputFileData {
    let var_set = vars.iter().cloned().collect::<BTreeSet<_>>();
    data.iter()
        .map(|(t, row)| {
            let filtered: BTreeMap<VarName, Value> = row
                .iter()
                .filter(|(name, _)| var_set.contains(*name))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            (*t, filtered)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::async_test;
    use futures::StreamExt;
    use macro_rules_attribute::apply;
    use std::collections::BTreeMap;

    use super::*;
    use crate::core::Value;
    use test_log::test;

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
    async fn stream_output_matches_data() {
        let mut provider = FileInputProvider::new(sample_data());
        let x_stream = provider.var_stream(&"x".into()).unwrap();
        let vals = x_stream.collect::<Vec<_>>().await;
        assert_eq!(vals, vec![Value::Int(1), Value::Int(2)]);
    }

    #[test]
    fn input_data_for_vars_filters() {
        let data = sample_data();
        let filtered = input_data_for_vars(&data, &["x".into()]);
        assert_eq!(filtered.get(&0).unwrap().len(), 1);
        assert!(filtered.get(&0).unwrap().contains_key(&"x".into()));
    }

    #[apply(async_test)]
    async fn batched_event_stream_preserves_file_rows() {
        let mut provider = FileInputProvider::new(sample_data());
        let vars = BTreeSet::from(["x".into(), "y".into()]);
        let batches = provider
            .batched_event_stream(&vars)
            .expect("file input should provide event batches")
            .collect::<Vec<_>>()
            .await;

        assert_eq!(
            batches,
            vec![
                vec![("x".into(), Value::Int(1)), ("y".into(), Value::Int(10))],
                vec![("x".into(), Value::Int(2)), ("y".into(), Value::Int(11))],
            ]
        );
    }
}
