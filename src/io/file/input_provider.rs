use std::future::pending;

use futures::future::LocalBoxFuture;
use futures::stream;

use crate::core::Value;
use crate::core::{InputProvider, OutputStream, VarName};
pub use crate::lang::untimed_input::UntimedInputFileData;

fn input_file_data_iter(
    data: UntimedInputFileData,
    key: VarName,
) -> impl Iterator<Item = Value> + 'static {
    let keys = data.keys();
    let max_key = *keys.max().unwrap_or(&0);
    (0..=max_key).map(move |time| match data.get(&time) {
        Some(data_for_time) => match data_for_time.get(&key.clone()) {
            Some(value) => value.clone(),
            None => Value::Unknown,
        },
        None => Value::Unknown,
    })
}

impl InputProvider for UntimedInputFileData {
    type Val = Value;

    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        Some(Box::pin(stream::iter(input_file_data_iter(
            self.clone(),
            var.clone(),
        ))))
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(pending())
    }

    fn ready(&self) -> LocalBoxFuture<'static, Result<(), anyhow::Error>> {
        Box::pin(futures::future::ready(Ok(())))
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use macro_rules_attribute::apply;
    use smol_macros::test as smol_test;
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

    #[test(apply(smol_test))]
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
}
