use crate::{OutputStream, Value, core::StreamData};
use futures::StreamExt;
use std::fmt::Debug;

pub fn to_typed_stream<T: TryFrom<Value, Error = ()> + Debug>(
    stream: OutputStream<Value>,
) -> OutputStream<T> {
    Box::pin(stream.map(|x| x.try_into().expect("Type error")))
}

pub fn to_typed_stream_vec<T: TryFrom<Value, Error = ()> + Debug>(
    stream: OutputStream<Vec<Value>>,
) -> OutputStream<Vec<T>> {
    Box::pin(stream.map(|xs| {
        xs.into_iter()
            .map(|x| x.try_into().expect("Type error"))
            .collect()
    }))
}

pub fn from_typed_stream<T: Into<Value> + StreamData>(
    stream: OutputStream<T>,
) -> OutputStream<Value> {
    Box::pin(stream.map(|x| x.into()))
}
