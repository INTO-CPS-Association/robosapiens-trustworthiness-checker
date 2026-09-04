use crate::{LocalStream, Value, core::StreamData, core::values::PartialStreamValue};
use futures::StreamExt;

use std::fmt::Debug;

pub fn to_typed_stream<T>(stream: LocalStream<Value>) -> LocalStream<T>
where
    T: TryFrom<Value> + Debug,
    <T as TryFrom<Value>>::Error: Debug,
{
    Box::pin(stream.map(|x| x.try_into().expect("Type error")))
}

pub fn to_typed_stream_vec<T>(stream: LocalStream<Vec<Value>>) -> LocalStream<Vec<T>>
where
    T: TryFrom<Value> + Debug,
    <T as TryFrom<Value>>::Error: Debug,
{
    Box::pin(stream.map(|xs| {
        xs.into_iter()
            .map(|x| x.try_into().expect("Type error"))
            .collect()
    }))
}

pub fn from_typed_stream<T: Into<Value> + StreamData>(
    stream: LocalStream<T>,
) -> LocalStream<Value> {
    Box::pin(stream.map(|x| x.into()))
}

pub fn to_typed_partial_stream<T>(stream: LocalStream<Value>) -> LocalStream<PartialStreamValue<T>>
where
    T: TryFrom<Value> + Debug,
    <T as TryFrom<Value>>::Error: Debug,
{
    Box::pin(stream.map(|x| match x {
        Value::NoVal => PartialStreamValue::NoVal,
        Value::Deferred => PartialStreamValue::Deferred,
        x => PartialStreamValue::Known(x.try_into().expect("Type error when casting stream")),
    }))
}

pub fn from_typed_partial_stream<T: Into<Value> + StreamData>(
    stream: LocalStream<PartialStreamValue<T>>,
) -> LocalStream<Value> {
    Box::pin(stream.map(|x| match x {
        PartialStreamValue::NoVal => Value::NoVal,
        PartialStreamValue::Deferred => Value::Deferred,
        PartialStreamValue::Known(value) => value.into(),
    }))
}
