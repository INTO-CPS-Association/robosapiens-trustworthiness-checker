use futures::stream;

use crate::ast::InputFileData;
use crate::core::Value;
use crate::core::{InputProvider, OutputStream, VarName};

fn input_file_data_iter(
    data: InputFileData,
    key: VarName,
) -> impl Iterator<Item = Value> + 'static {
    let keys = data.keys();
    let max_key = keys.max().unwrap_or(&0).clone();
    (0..=max_key).map(move |time| match data.get(&time) {
        Some(data_for_time) => match data_for_time.get(&key.clone()) {
            Some(value) => value.clone(),
            None => Value::Unknown,
        },
        None => Value::Unknown,
    })
}

impl InputProvider<Value> for InputFileData {
    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        Some(Box::pin(stream::iter(input_file_data_iter(
            self.clone(),
            var.clone(),
        ))))
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use std::collections::BTreeMap;

    use crate::ast::InputFileData;
    use crate::core::{Value, VarName};
    use crate::InputProvider;

    #[test]
    fn test_input_file_data_iter() {
        let mut data: InputFileData = BTreeMap::new();
        data.insert(0, {
            let mut map = BTreeMap::new();
            map.insert(VarName("x".into()), Value::Int(1));
            map
        });
        data.insert(1, {
            let mut map = BTreeMap::new();
            map.insert(VarName("x".into()), Value::Int(2));
            map
        });
        data.insert(2, {
            let mut map = BTreeMap::new();
            map.insert(VarName("x".into()), Value::Int(3));
            map
        });

        let iter = super::input_file_data_iter(data, VarName("x".into()));
        let vec: Vec<Value> = iter.collect();
        assert_eq!(
            vec,
            vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3)
            ]
        );
    }

    #[tokio::test]
    async fn test_input_file_as_stream() {
        let mut data: InputFileData = BTreeMap::new();
        data.insert(0, {
            let mut map = BTreeMap::new();
            map.insert(VarName("x".into()), Value::Int(1));
            map
        });
        data.insert(1, {
            let mut map = BTreeMap::new();
            map.insert(VarName("x".into()), Value::Int(2));
            map
        });
        data.insert(2, {
            let mut map = BTreeMap::new();
            map.insert(VarName("x".into()), Value::Int(3));
            map
        });

        let input_stream = data.input_stream(&VarName("x".into())).unwrap();
        let input_vec = input_stream.collect::<Vec<_>>().await;
        assert_eq!(
            input_vec,
            vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3)
            ]
        );
    }
}
