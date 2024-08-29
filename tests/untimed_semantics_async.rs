// Test untimed monitoring of LOLA specifications with the async runtime

use futures::stream;
use futures::stream::{BoxStream, StreamExt};
use std::{collections::BTreeMap, pin::Pin};
use trustworthiness_checker::{
    ast::{LOLASpecification, SExpr},
    async_runtime::AsyncMonitorRunner,
    monitoring_semantics::UNTIMED_LOLA_SEMANTICS,
    Monitor, StreamData, VarName,
};

fn input_streams1() -> BTreeMap<VarName, BoxStream<'static, StreamData>> {
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

#[tokio::test]
async fn test_simple_add_monitor() {
    let input_streams = input_streams1();
    let spec = LOLASpecification {
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
    };
    let mut async_monitor = AsyncMonitorRunner::new(spec, UNTIMED_LOLA_SEMANTICS, input_streams);
    let outputs: Vec<(usize, BTreeMap<VarName, StreamData>)> = async_monitor
        .monitor_outputs()
        .take(2)
        .enumerate()
        .collect()
        .await;
    assert_eq!(
        outputs,
        vec![
            (
                0,
                vec![(VarName("z".into()), StreamData::Int(3))]
                    .into_iter()
                    .collect(),
            ),
            (
                1,
                vec![(VarName("z".into()), StreamData::Int(7))]
                    .into_iter()
                    .collect(),
            ),
        ]
    );
}

#[tokio::test]
async fn test_count_monitor() {
    let input_streams: BTreeMap<VarName, BoxStream<'static, StreamData>> = BTreeMap::new();
    let spec = LOLASpecification {
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
    };
    let mut async_monitor = AsyncMonitorRunner::new(spec, UNTIMED_LOLA_SEMANTICS, input_streams);
    let outputs: Vec<(usize, BTreeMap<VarName, StreamData>)> = async_monitor
        .monitor_outputs()
        .take(4)
        .enumerate()
        .collect()
        .await;
    assert_eq!(
        outputs,
        vec![
            (
                0,
                vec![(VarName("x".into()), StreamData::Int(1))]
                    .into_iter()
                    .collect(),
            ),
            (
                1,
                vec![(VarName("x".into()), StreamData::Int(2))]
                    .into_iter()
                    .collect(),
            ),
            (
                2,
                vec![(VarName("x".into()), StreamData::Int(3))]
                    .into_iter()
                    .collect(),
            ),
            (
                3,
                vec![(VarName("x".into()), StreamData::Int(4))]
                    .into_iter()
                    .collect(),
            ),
        ]
    );
}
