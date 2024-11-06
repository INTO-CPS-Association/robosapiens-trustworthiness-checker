use futures::{stream::BoxStream, StreamExt};

use crate::{
    core::{ConcreteStreamData, StreamSystem, StreamTransformationFn, TypeSystem},
    lola_type_system::{LOLATypeSystem, StreamType},
    OutputStream,
};

pub struct TypedStreams {}

impl StreamSystem for TypedStreams {
    type TypeSystem = LOLATypeSystem;
    type TypedStream = LOLAStream;

    fn transform_stream(
        transformation: impl StreamTransformationFn,
        stream: Self::TypedStream,
    ) -> Self::TypedStream {
        match stream {
            LOLAStream::Int(stream) => LOLAStream::Int(Box::pin(transformation.transform(stream))),
            LOLAStream::Str(stream) => LOLAStream::Str(Box::pin(transformation.transform(stream))),
            LOLAStream::Bool(stream) => {
                LOLAStream::Bool(Box::pin(transformation.transform(stream)))
            }
            LOLAStream::Unit(stream) => {
                LOLAStream::Unit(Box::pin(transformation.transform(stream)))
            }
        }
    }

    fn to_typed_stream(
        typ: <Self::TypeSystem as TypeSystem>::Type,
        stream: OutputStream<<LOLATypeSystem as TypeSystem>::TypedValue>,
    ) -> Self::TypedStream {
        match typ {
            StreamType::Int => LOLAStream::Int(Box::pin(stream.map(|v| match v {
                ConcreteStreamData::Int(i) => i,
                _ => panic!("Invalid stream type specialization in runtime"),
            }))),
            StreamType::Str => LOLAStream::Str(Box::pin(stream.map(|v| match v {
                ConcreteStreamData::Str(s) => s,
                _ => panic!("Invalid stream type specialization in runtime"),
            }))),
            StreamType::Bool => LOLAStream::Bool(Box::pin(stream.map(|v| match v {
                ConcreteStreamData::Bool(b) => b,
                _ => panic!("Invalid stream type specialization in runtime"),
            }))),
            StreamType::Unit => LOLAStream::Unit(Box::pin(stream.map(|v| match v {
                ConcreteStreamData::Unit => (),
                _ => panic!("Invalid stream type specialization in runtime"),
            }))),
        }
    }

    fn type_of_stream(value: &Self::TypedStream) -> <Self::TypeSystem as TypeSystem>::Type {
        match value {
            LOLAStream::Int(_) => StreamType::Int,
            LOLAStream::Str(_) => StreamType::Str,
            LOLAStream::Bool(_) => StreamType::Bool,
            LOLAStream::Unit(_) => StreamType::Unit,
        }
    }
}

pub enum LOLAStream {
    Int(BoxStream<'static, i64>),
    Str(BoxStream<'static, String>),
    Bool(BoxStream<'static, bool>),
    Unit(BoxStream<'static, ()>),
}

impl futures::Stream for LOLAStream {
    type Item = ConcreteStreamData;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.get_mut() {
            LOLAStream::Int(pin) => pin
                .poll_next_unpin(cx)
                .map(|opt| opt.map(|v| ConcreteStreamData::Int(v))),
            LOLAStream::Str(pin) => pin
                .poll_next_unpin(cx)
                .map(|opt| opt.map(|v| ConcreteStreamData::Str(v))),
            LOLAStream::Bool(pin) => pin
                .poll_next_unpin(cx)
                .map(|opt| opt.map(|v| ConcreteStreamData::Bool(v))),
            LOLAStream::Unit(pin) => pin
                .poll_next_unpin(cx)
                .map(|opt| opt.map(|_| ConcreteStreamData::Unit)),
        }
    }
}

impl From<(StreamType, OutputStream<ConcreteStreamData>)> for LOLAStream {
    fn from((typ, x): (StreamType, OutputStream<ConcreteStreamData>)) -> Self {
        match typ {
            StreamType::Int => LOLAStream::Int(Box::pin(x.map(|v| match v {
                ConcreteStreamData::Int(i) => i,
                _ => panic!("Invalid stream type specialization in runtime"),
            }))),
            StreamType::Str => LOLAStream::Str(Box::pin(x.map(|v| match v {
                ConcreteStreamData::Str(s) => s,
                _ => panic!("Invalid stream type specialization in runtime"),
            }))),
            StreamType::Bool => LOLAStream::Bool(Box::pin(x.map(|v| match v {
                ConcreteStreamData::Bool(b) => b,
                _ => panic!("Invalid stream type specialization in runtime"),
            }))),
            StreamType::Unit => LOLAStream::Unit(Box::pin(x.map(|v| match v {
                ConcreteStreamData::Unit => (),
                _ => panic!("Invalid stream type specialization in runtime"),
            }))),
        }
    }
}
