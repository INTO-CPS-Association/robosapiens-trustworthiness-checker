use std::collections::BTreeSet;
use std::future::pending;

use futures::future::LocalBoxFuture;
use futures::stream;

use crate::core::Value;
use crate::core::{InputProvider, OutputStream, VarName};
pub use crate::lang::untimed_input::UntimedInputFileData;

// Returns an iterator over the values for a given key in the UntimedInputFileData.
// None if no keys are present.
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

impl InputProvider for UntimedInputFileData {
    type Val = Value;

    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        let input_file_data_iter = input_file_data_iter(self.clone(), var.clone());
        Some(Box::pin(stream::iter(input_file_data_iter)))
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(pending())
    }

    fn ready(&self) -> LocalBoxFuture<'static, Result<(), anyhow::Error>> {
        Box::pin(futures::future::ready(Ok(())))
    }

    // TODO: Technically a bug here. It returns vars seen in the input file, not the ones defined
    // in the model. If an input_stream is defined in the model but not used as an input there is a
    // discrepancy here. Requires having access to the model inside UntimedInputFileData.
    fn vars(&self) -> Vec<VarName> {
        let uniques: BTreeSet<VarName> = self
            .values()
            .flat_map(|inner| inner.keys())
            .cloned()
            .collect();
        uniques.into_iter().collect()
    }
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

    #[test]
    fn test_input_file_data_iter() {
        let mut data: UntimedInputFileData = BTreeMap::new();
        data.insert(0, {
            let mut map = BTreeMap::new();
            map.insert("x".into(), Value::Int(1));
            map
        });
        data.insert(1, {
            let mut map = BTreeMap::new();
            map.insert("x".into(), Value::Int(2));
            map
        });
        data.insert(2, {
            let mut map = BTreeMap::new();
            map.insert("x".into(), Value::Int(3));
            map
        });

        let iter = super::input_file_data_iter(data, "x".into());
        let vec: Vec<Value> = iter.collect();
        assert_eq!(vec, vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
    }

    #[apply(async_test)]
    async fn test_input_file_as_stream() {
        let mut data: UntimedInputFileData = BTreeMap::new();
        data.insert(0, {
            let mut map = BTreeMap::new();
            map.insert("x".into(), Value::Int(1));
            map
        });
        data.insert(1, {
            let mut map = BTreeMap::new();
            map.insert("x".into(), Value::Int(2));
            map
        });
        data.insert(2, {
            let mut map = BTreeMap::new();
            map.insert("x".into(), Value::Int(3));
            map
        });

        let input_stream = data.input_stream(&"x".into()).unwrap();
        let input_vec = input_stream.collect::<Vec<_>>().await;
        assert_eq!(input_vec, vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
    }

    #[apply(async_test)]
    async fn test_input_file_as_stream_multi_var() {
        let mut data: UntimedInputFileData = BTreeMap::new();
        data.insert(0, {
            let mut map = BTreeMap::new();
            map.insert("x".into(), Value::Int(1));
            map.insert("y".into(), Value::Int(2));
            map
        });
        data.insert(1, {
            let mut map = BTreeMap::new();
            map.insert("x".into(), Value::Int(2));
            map.insert("y".into(), Value::Int(3));
            map
        });
        data.insert(2, {
            let mut map = BTreeMap::new();
            map.insert("x".into(), Value::Int(3));
            map.insert("y".into(), Value::Int(4));
            map
        });

        let x_stream = data.input_stream(&"x".into()).unwrap();
        let x_vec = x_stream.collect::<Vec<_>>().await;
        assert_eq!(x_vec, vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let y_stream = data.input_stream(&"y".into()).unwrap();
        let y_vec = y_stream.collect::<Vec<_>>().await;
        assert_eq!(y_vec, vec![Value::Int(2), Value::Int(3), Value::Int(4)]);
    }

    #[apply(async_test)]
    async fn test_input_file_vars() {
        let mut data: UntimedInputFileData = BTreeMap::new();
        data.insert(0, {
            let mut map = BTreeMap::new();
            map.insert("x".into(), Value::Int(1));
            map.insert("y".into(), Value::Int(2));
            map
        });
        data.insert(1, {
            let mut map = BTreeMap::new();
            map.insert("x".into(), Value::Int(2));
            map.insert("y".into(), Value::Int(3));
            map
        });
        data.insert(2, {
            let mut map = BTreeMap::new();
            map.insert("x".into(), Value::Int(3));
            map.insert("y".into(), Value::Int(4));
            map
        });
        assert_eq!(data.vars(), vec!["x".into(), "y".into()]);
    }
}
