use futures::stream;
use futures::stream::BoxStream;
use std::{collections::BTreeMap, pin::Pin};
use trustworthiness_checker::{
    ast::{LOLASpecification, SExpr},
    StreamData, VarName,
};

pub fn input_streams1() -> BTreeMap<VarName, BoxStream<'static, StreamData>> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        VarName("x".into()),
        Box::pin(stream::iter(
            vec![StreamData::Int(1), StreamData::Int(3)].into_iter(),
        )) as Pin<Box<dyn futures::Stream<Item = StreamData> + std::marker::Send>>,
    );
    input_streams.insert(
        VarName("y".into()),
        Box::pin(stream::iter(
            vec![StreamData::Int(2), StreamData::Int(4)].into_iter(),
        )) as Pin<Box<dyn futures::Stream<Item = StreamData> + std::marker::Send>>,
    );
    input_streams
}

pub fn input_streams2() -> BTreeMap<VarName, BoxStream<'static, StreamData>> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        VarName("x".into()),
        Box::pin(stream::iter(
            vec![StreamData::Int(1), StreamData::Int(3)].into_iter(),
        )) as Pin<Box<dyn futures::Stream<Item = StreamData> + std::marker::Send>>,
    );
    input_streams.insert(
        VarName("y".into()),
        Box::pin(stream::iter(
            vec![StreamData::Int(2), StreamData::Int(4)].into_iter(),
        )) as Pin<Box<dyn futures::Stream<Item = StreamData> + std::marker::Send>>,
    );
    input_streams.insert(
        VarName("s".into()),
        Box::pin(stream::iter(
            vec![
                StreamData::Str("x+y".to_string()),
                StreamData::Str("x+y".to_string()),
            ]
            .into_iter(),
        )) as Pin<Box<dyn futures::Stream<Item = StreamData> + std::marker::Send>>,
    );
    input_streams
}

pub fn spec_simple_add_monitor() -> LOLASpecification {
    LOLASpecification {
        input_vars: vec![VarName("x".into()), VarName("y".into())],
        output_vars: vec![VarName("z".into())],
        exprs: vec![(
            VarName("z".into()),
            SExpr::Plus(
                Box::new(SExpr::Var(VarName("x".into()))),
                Box::new(SExpr::Var(VarName("y".into()))),
            ),
        )]
        .into_iter()
        .collect(),
    }
}

pub fn spec_count_monitor() -> LOLASpecification {
    LOLASpecification {
        input_vars: vec![],
        output_vars: vec![VarName("x".into())],
        exprs: vec![(
            VarName("x".into()),
            SExpr::Plus(
                Box::new(SExpr::Val(StreamData::Int(1))),
                Box::new(SExpr::Index(
                    Box::new(SExpr::Var(VarName("x".into()))),
                    -1,
                    StreamData::Int(0),
                )),
            ),
        )]
        .into_iter()
        .collect(),
    }
}

pub fn spec_eval_monitor() -> LOLASpecification {
    LOLASpecification {
        input_vars: vec![
            VarName("x".into()),
            VarName("y".into()),
            VarName("s".into()),
        ],
        output_vars: vec![VarName("z".into()), VarName("w".into())],
        exprs: vec![
            (
                VarName("z".into()),
                SExpr::Plus(
                    Box::new(SExpr::Var(VarName("x".into()))),
                    Box::new(SExpr::Var(VarName("y".into()))),
                ),
            ),
            (
                VarName("w".into()),
                SExpr::Eval(Box::new(SExpr::Var(VarName("z".into())))),
            ),
        ]
        .into_iter()
        .collect(),
    }
}
