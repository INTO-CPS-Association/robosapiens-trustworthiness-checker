use futures::stream;
use futures::stream::BoxStream;
use std::{collections::BTreeMap, pin::Pin};
use trustworthiness_checker::{
    ast::{LOLASpecification, SExpr},
    ConcreteStreamData, VarName,
};

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

pub fn spec_simple_add_monitor() -> &'static str {
    "in x\n\
     in y\n\
     out z\n\
     z = x + y"   
}

pub fn spec_count_monitor() -> &'static str {
    "out x\n\
     x = 1 + (x)[-1, 0]"
}

pub fn spec_eval_monitor() -> &'static str {
   "in x\n\
    in y\n\
    in s\n\
    out z\n\
    out w\n\
    z = x + y\n\
    w = eval(s)"
}
