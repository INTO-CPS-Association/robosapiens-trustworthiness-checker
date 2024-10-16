use futures::stream;
use futures::stream::BoxStream;
use std::{collections::BTreeMap, pin::Pin};
use trustworthiness_checker::{lola_streams::LOLAStream, ConcreteStreamData, VarName};

// Dead code is allowed in this file since cargo does not correctly
// track when functions are used in tests.

#[allow(dead_code)]
pub fn input_streams1() -> BTreeMap<VarName, BoxStream<'static, ConcreteStreamData>> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        VarName("x".into()),
        Box::pin(stream::iter(
            vec![ConcreteStreamData::Int(1), ConcreteStreamData::Int(3)].into_iter(),
        )) as Pin<Box<dyn futures::Stream<Item = ConcreteStreamData> + std::marker::Send>>,
    );
    input_streams.insert(
        VarName("y".into()),
        Box::pin(stream::iter(
            vec![ConcreteStreamData::Int(2), ConcreteStreamData::Int(4)].into_iter(),
        )) as Pin<Box<dyn futures::Stream<Item = ConcreteStreamData> + std::marker::Send>>,
    );
    input_streams
}

#[allow(dead_code)]
pub fn input_streams2() -> BTreeMap<VarName, BoxStream<'static, ConcreteStreamData>> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        VarName("x".into()),
        Box::pin(stream::iter(
            vec![ConcreteStreamData::Int(1), ConcreteStreamData::Int(3)].into_iter(),
        )) as Pin<Box<dyn futures::Stream<Item = ConcreteStreamData> + std::marker::Send>>,
    );
    input_streams.insert(
        VarName("y".into()),
        Box::pin(stream::iter(
            vec![ConcreteStreamData::Int(2), ConcreteStreamData::Int(4)].into_iter(),
        )) as Pin<Box<dyn futures::Stream<Item = ConcreteStreamData> + std::marker::Send>>,
    );
    input_streams.insert(
        VarName("s".into()),
        Box::pin(stream::iter(
            vec![
                ConcreteStreamData::Str("x+y".to_string()),
                ConcreteStreamData::Str("x+y".to_string()),
            ]
            .into_iter(),
        )) as Pin<Box<dyn futures::Stream<Item = ConcreteStreamData> + std::marker::Send>>,
    );
    input_streams
}

#[allow(dead_code)]
pub fn input_streams3() -> BTreeMap<VarName, LOLAStream> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        VarName("x".into()),
        LOLAStream::Int(Box::pin(stream::iter(vec![1, 3].into_iter()))),
    );
    input_streams.insert(
        VarName("y".into()),
        LOLAStream::Int(Box::pin(stream::iter(vec![2, 4].into_iter()))),
    );
    input_streams
}

#[allow(dead_code)]
pub fn input_streams4() -> BTreeMap<VarName, LOLAStream> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        VarName("x".into()),
        LOLAStream::Str(Box::pin(stream::iter(
            vec!["a".to_string(), "c".to_string()].into_iter(),
        ))),
    );
    input_streams.insert(
        VarName("y".into()),
        LOLAStream::Str(Box::pin(stream::iter(
            vec!["b".to_string(), "d".to_string()].into_iter(),
        ))),
    );
    input_streams
}

#[allow(dead_code)]
pub fn spec_simple_add_monitor() -> &'static str {
    "in x\n\
     in y\n\
     out z\n\
     z = x + y"
}

#[allow(dead_code)]
pub fn spec_simple_add_monitor_typed() -> &'static str {
    "in x: Int\n\
     in y: Int\n\
     out z: Int\n\
     z = x + y"
}

#[allow(dead_code)]
pub fn spec_typed_string_concat() -> &'static str {
    "in x: Str\n\
     in y: Str\n\
     out z: Str\n\
     z = x + y"
}

#[allow(dead_code)]
pub fn spec_count_monitor() -> &'static str {
    "out x\n\
     x = 1 + (x)[-1, 0]"
}

#[allow(dead_code)]
pub fn spec_eval_monitor() -> &'static str {
    "in x\n\
    in y\n\
    in s\n\
    out z\n\
    out w\n\
    z = x + y\n\
    w = eval(s)"
}
