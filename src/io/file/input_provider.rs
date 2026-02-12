use crate::core::{InputProvider, OutputStream, Value, VarName};
pub use crate::lang::untimed_input::UntimedInputFileData;
use async_trait::async_trait;
use futures::{StreamExt, stream};

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

#[async_trait(?Send)]
impl InputProvider for UntimedInputFileData {
    type Val = Value;

    fn var_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        let input_file_data_iter = input_file_data_iter(self.clone(), var.clone());
        Some(Box::pin(stream::iter(input_file_data_iter)))
    }

    async fn control_stream(&mut self) -> OutputStream<anyhow::Result<()>> {
        let max_index = self.keys().max();
        if let Some(max_key) = max_index {
            stream::repeat_with(|| Ok(()))
                .take(*max_key + 1)
                .boxed_local()
        } else {
            stream::repeat_with(|| Ok(())).boxed_local()
        }
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

        let input_stream = data.var_stream(&"x".into()).unwrap();
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

        let x_stream = data.var_stream(&"x".into()).unwrap();
        let x_vec = x_stream.collect::<Vec<_>>().await;
        assert_eq!(x_vec, vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let y_stream = data.var_stream(&"y".into()).unwrap();
        let y_vec = y_stream.collect::<Vec<_>>().await;
        assert_eq!(y_vec, vec![Value::Int(2), Value::Int(3), Value::Int(4)]);
    }
}
