use futures::stream;
use futures::stream::BoxStream;
use std::{collections::BTreeMap, pin::Pin};
use trustworthiness_checker::{OutputStream, Value, VarName};

// Dead code is allowed in this file since cargo does not correctly
// track when functions are used in tests.

#[allow(dead_code)]
pub fn input_empty() -> BTreeMap<VarName, BoxStream<'static, Value>> {
    BTreeMap::new()
}

// TODO: Make the input streams have 3 values...

#[allow(dead_code)]
pub fn input_streams1() -> BTreeMap<VarName, BoxStream<'static, Value>> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        VarName("x".into()),
        Box::pin(stream::iter(vec![Value::Int(1), Value::Int(3)].into_iter()))
            as Pin<Box<dyn futures::Stream<Item = Value> + std::marker::Send>>,
    );
    input_streams.insert(
        VarName("y".into()),
        Box::pin(stream::iter(vec![Value::Int(2), Value::Int(4)].into_iter()))
            as Pin<Box<dyn futures::Stream<Item = Value> + std::marker::Send>>,
    );
    input_streams
}

#[allow(dead_code)]
pub fn input_streams2() -> BTreeMap<VarName, BoxStream<'static, Value>> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        VarName("x".into()),
        Box::pin(stream::iter(vec![Value::Int(1), Value::Int(3)].into_iter()))
            as Pin<Box<dyn futures::Stream<Item = Value> + std::marker::Send>>,
    );
    input_streams.insert(
        VarName("y".into()),
        Box::pin(stream::iter(vec![Value::Int(2), Value::Int(4)].into_iter()))
            as Pin<Box<dyn futures::Stream<Item = Value> + std::marker::Send>>,
    );
    input_streams.insert(
        VarName("s".into()),
        Box::pin(stream::iter(
            vec![Value::Str("x+y".to_string()), Value::Str("x+y".to_string())].into_iter(),
        )) as Pin<Box<dyn futures::Stream<Item = Value> + std::marker::Send>>,
    );
    input_streams
}

#[allow(dead_code)]
pub fn input_streams3() -> BTreeMap<VarName, OutputStream<Value>> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        VarName("x".into()),
        Box::pin(stream::iter(vec![Value::Int(1), Value::Int(3)].into_iter()))
            as OutputStream<Value>,
    );
    input_streams.insert(
        VarName("y".into()),
        Box::pin(stream::iter(vec![Value::Int(2), Value::Int(4)].into_iter())),
    );
    input_streams
}

#[allow(dead_code)]
pub fn input_streams4() -> BTreeMap<VarName, OutputStream<Value>> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        VarName("x".into()),
        Box::pin(stream::iter(
            vec![Value::Str("a".to_string()), Value::Str("c".to_string())].into_iter(),
        )) as OutputStream<Value>,
    );
    input_streams.insert(
        VarName("y".into()),
        Box::pin(stream::iter(
            vec![Value::Str("b".to_string()), Value::Str("d".to_string())].into_iter(),
        )),
    );
    input_streams
}

#[allow(dead_code)]
pub fn spec_empty() -> &'static str {
    ""
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
     z = x ++ y"
}

#[allow(dead_code)]
pub fn spec_typed_count_monitor() -> &'static str {
    "out x: Int\n\
     x = 1 + (x)[-1, 0]"
}

#[allow(dead_code)]
pub fn spec_typed_eval_monitor() -> &'static str {
    "in x: Int\n\
    in y: Int\n\
    in s: Str\n\
    out z: Int\n\
    out w: Int\n\
    z = x + y\n\
    w = eval(s)"
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
