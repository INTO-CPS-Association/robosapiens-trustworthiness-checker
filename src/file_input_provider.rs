use core::panic;

use futures::{stream, StreamExt};

use crate::ast::{InputFileData, UntypedStreams};
use crate::core::TypeSystem;
use crate::core::{InputProvider, OutputStream, StreamSystem, TypeAnnotated, Value, VarName};
use crate::lola_streams::{LOLAStream, TypedStreams};
use crate::lola_type_system::{LOLATypeSystem, StreamType};
use crate::ConcreteStreamData;

fn input_file_data_iter(
    data: InputFileData,
    key: VarName,
) -> impl Iterator<Item = ConcreteStreamData> + 'static {
    let keys = data.keys();
    let max_key = keys.max().unwrap_or(&0).clone();
    (0..=max_key).map(move |time| match data.get(&time) {
        Some(data_for_time) => match data_for_time.get(&key.clone()) {
            Some(value) => value.clone(),
            None => ConcreteStreamData::Unknown,
        },
        None => ConcreteStreamData::Unknown,
    })
}

impl InputProvider<UntypedStreams> for InputFileData {
    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<ConcreteStreamData>> {
        Some(Box::pin(stream::iter(input_file_data_iter(
            self.clone(),
            var.clone(),
        ))))
    }
}

impl<T: TypeAnnotated<LOLATypeSystem>> InputProvider<TypedStreams> for (InputFileData, T) {
    fn input_stream(&mut self, var: &VarName) -> Option<LOLAStream> {
        let (data, ta) = self;
        let base_stream = data.input_stream(var)?;
        let var_type = ta.type_of_var(var)?;
        let var_type_clone = var_type.clone();
        let converting_stream = Box::pin(base_stream.map(move |data| match data {
            ConcreteStreamData::Int(i) => {
                let value = i.to_typed_value();
                assert_eq!(LOLATypeSystem::type_of_value(&value), var_type);
                value
            }
            ConcreteStreamData::Str(s) => {
                let value = s.to_typed_value();
                assert_eq!(LOLATypeSystem::type_of_value(&value), var_type);
                value
            }
            ConcreteStreamData::Bool(b) => {
                let value = b.to_typed_value();
                assert_eq!(LOLATypeSystem::type_of_value(&value), var_type);
                value
            }
            ConcreteStreamData::Unit => {
                let value = ().to_typed_value();
                assert_eq!(LOLATypeSystem::type_of_value(&value), var_type);
                value
            }
            ConcreteStreamData::Unknown => {
                panic!("Unknown data type in input stream")
            }
        }));
        Some(TypedStreams::to_typed_stream(
            var_type_clone,
            converting_stream,
        ))
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use std::collections::BTreeMap;

    use crate::ast::InputFileData;
    use crate::core::{ConcreteStreamData, VarName};
    use crate::InputProvider;

    #[test]
    fn test_input_file_data_iter() {
        let mut data: InputFileData = BTreeMap::new();
        data.insert(0, {
            let mut map = BTreeMap::new();
            map.insert(VarName("x".into()), ConcreteStreamData::Int(1));
            map
        });
        data.insert(1, {
            let mut map = BTreeMap::new();
            map.insert(VarName("x".into()), ConcreteStreamData::Int(2));
            map
        });
        data.insert(2, {
            let mut map = BTreeMap::new();
            map.insert(VarName("x".into()), ConcreteStreamData::Int(3));
            map
        });

        let iter = super::input_file_data_iter(data, VarName("x".into()));
        let vec: Vec<ConcreteStreamData> = iter.collect();
        assert_eq!(
            vec,
            vec![
                ConcreteStreamData::Int(1),
                ConcreteStreamData::Int(2),
                ConcreteStreamData::Int(3)
            ]
        );
    }

    #[tokio::test]
    async fn test_input_file_as_stream() {
        let mut data: InputFileData = BTreeMap::new();
        data.insert(0, {
            let mut map = BTreeMap::new();
            map.insert(VarName("x".into()), ConcreteStreamData::Int(1));
            map
        });
        data.insert(1, {
            let mut map = BTreeMap::new();
            map.insert(VarName("x".into()), ConcreteStreamData::Int(2));
            map
        });
        data.insert(2, {
            let mut map = BTreeMap::new();
            map.insert(VarName("x".into()), ConcreteStreamData::Int(3));
            map
        });

        let input_stream = data.input_stream(&VarName("x".into())).unwrap();
        let input_vec = input_stream.collect::<Vec<_>>().await;
        assert_eq!(
            input_vec,
            vec![
                ConcreteStreamData::Int(1),
                ConcreteStreamData::Int(2),
                ConcreteStreamData::Int(3)
            ]
        );
    }
}
