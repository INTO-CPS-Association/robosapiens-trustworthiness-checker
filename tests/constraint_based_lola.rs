// Test untimed monitoring of LOLA specifications with the async runtime

use futures::stream;
use futures::stream::{BoxStream, StreamExt};
use std::{collections::BTreeMap, pin::Pin};
use trustworthiness_checker::constraint_based_runtime::{
    ConstraintBasedMonitor, ValStreamCollection,
};
use trustworthiness_checker::constraint_solver::SExprConstraintStore;
use trustworthiness_checker::core::IndexedVarName;
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

fn input_streams2() -> BTreeMap<VarName, BoxStream<'static, StreamData>> {
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

#[tokio::test]
async fn test_simple_add_monitor() {
    let input_streams = input_streams1();
    let binding = ValStreamCollection(input_streams);
    let constraint_store = SExprConstraintStore {
        resolved: vec![],
        unresolved: vec![(
            VarName("z".into()),
            SExpr::Plus(
                Box::new(SExpr::Var(VarName("x".into()))),
                Box::new(SExpr::Var(VarName("y".into()))),
            ),
        )],
    };
    let mut monitor = ConstraintBasedMonitor::new(
        vec![VarName("x".into()), VarName("y".into())],
        vec![VarName("z".into()), VarName("w".into())],
        constraint_store,
        binding,
    );
    let outputs: Vec<(usize, BTreeMap<VarName, StreamData>)> =
        monitor.monitor_outputs().enumerate().collect().await;
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

#[ignore="currently we can't handle recursive constraints in the solver as need a way to handle the inner indexes"]
#[tokio::test]
async fn test_count_monitor() {
    let input_streams = BTreeMap::new();
    let binding = ValStreamCollection(input_streams);
    let constraint_store = SExprConstraintStore {
        resolved: vec![],
        unresolved: vec![(
            VarName("x".into()),
            SExpr::Plus(
                Box::new(SExpr::Val(StreamData::Int(1))),
                Box::new(SExpr::Index(
                    Box::new(SExpr::Var(VarName("x".into()))),
                    -1,
                    StreamData::Int(0),
                )),
            ),
        )],
    };
    let mut monitor =
        ConstraintBasedMonitor::new(vec![], vec![VarName("x".into())], constraint_store, binding);
    // let constraints: Vec<SExprConstraintStore<IndexedVarName>> = monitor.stream_constraints().take(3).collect().await;
    let outputs: Vec<(usize, BTreeMap<VarName, StreamData>)> = monitor
        .monitor_outputs()
        .take(5)
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
            (
                4,
                vec![(VarName("x".into()), StreamData::Int(5))]
                    .into_iter()
                    .collect(),
            ),
        ]
    );
}

#[tokio::test]
async fn test_eval_monitor() {
    let input_streams = input_streams2();
    let binding = ValStreamCollection(input_streams);
    let constraint_store = SExprConstraintStore {
        resolved: vec![],
        unresolved: vec![
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
        ],
    };
    let mut monitor = ConstraintBasedMonitor::new(
        vec![
            VarName("x".into()),
            VarName("y".into()),
            VarName("s".into()),
        ],
        vec![VarName("z".into()), VarName("w".into())],
        constraint_store,
        binding,
    );
    let outputs: Vec<(usize, BTreeMap<VarName, StreamData>)> =
        monitor.monitor_outputs().enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (
                0,
                vec![
                    (VarName("z".into()), StreamData::Int(3)),
                    (VarName("w".into()), StreamData::Int(3))
                ]
                .into_iter()
                .collect(),
            ),
            (
                1,
                vec![
                    (VarName("z".into()), StreamData::Int(7)),
                    (VarName("w".into()), StreamData::Int(7))
                ]
                .into_iter()
                .collect(),
            ),
        ]
    );
}
