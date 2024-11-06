// Test untimed monitoring of LOLA specifications with the async runtime

use futures::stream::{BoxStream, StreamExt};
use std::collections::BTreeMap;
use trustworthiness_checker::ast::UntypedLOLA;
use trustworthiness_checker::UntimedLolaSemantics;
use trustworthiness_checker::{
    async_runtime::AsyncMonitorRunner, lola_specification, ConcreteStreamData, Monitor, VarName,
};
mod lola_fixtures;
use lola_fixtures::*;

#[tokio::test]
async fn test_simple_add_monitor() {
    let input_streams = input_streams1();
    let spec = lola_specification(&mut spec_simple_add_monitor()).unwrap();
    let mut async_monitor =
        AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(spec, input_streams);
    let outputs: Vec<(usize, BTreeMap<VarName, ConcreteStreamData>)> =
        async_monitor.monitor_outputs().enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (
                0,
                vec![(VarName("z".into()), ConcreteStreamData::Int(3))]
                    .into_iter()
                    .collect(),
            ),
            (
                1,
                vec![(VarName("z".into()), ConcreteStreamData::Int(7))]
                    .into_iter()
                    .collect(),
            ),
        ]
    );
}

#[tokio::test]
async fn test_simple_add_monitor_does_not_go_away() {
    let input_streams = input_streams1();
    let spec = lola_specification(&mut spec_simple_add_monitor()).unwrap();
    let outputs = {
        let mut async_monitor =
            AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(spec, input_streams);
        async_monitor.monitor_outputs()
    };
    let outputs: Vec<(usize, BTreeMap<VarName, ConcreteStreamData>)> =
        outputs.enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (
                0,
                vec![(VarName("z".into()), ConcreteStreamData::Int(3))]
                    .into_iter()
                    .collect(),
            ),
            (
                1,
                vec![(VarName("z".into()), ConcreteStreamData::Int(7))]
                    .into_iter()
                    .collect(),
            ),
        ]
    );
}

#[tokio::test]
async fn test_count_monitor() {
    let input_streams: BTreeMap<VarName, BoxStream<'static, ConcreteStreamData>> = BTreeMap::new();
    let spec = lola_specification(&mut spec_count_monitor()).unwrap();
    let mut async_monitor =
        AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(spec, input_streams);
    let outputs: Vec<(usize, BTreeMap<VarName, ConcreteStreamData>)> = async_monitor
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
                vec![(VarName("x".into()), ConcreteStreamData::Int(1))]
                    .into_iter()
                    .collect(),
            ),
            (
                1,
                vec![(VarName("x".into()), ConcreteStreamData::Int(2))]
                    .into_iter()
                    .collect(),
            ),
            (
                2,
                vec![(VarName("x".into()), ConcreteStreamData::Int(3))]
                    .into_iter()
                    .collect(),
            ),
            (
                3,
                vec![(VarName("x".into()), ConcreteStreamData::Int(4))]
                    .into_iter()
                    .collect(),
            ),
        ]
    );
}

#[tokio::test]
async fn test_eval_monitor() {
    let input_streams = input_streams2();
    let spec = lola_specification(&mut spec_eval_monitor()).unwrap();
    let mut async_monitor =
        AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(spec, input_streams);
    let outputs: Vec<(usize, BTreeMap<VarName, ConcreteStreamData>)> =
        async_monitor.monitor_outputs().enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (
                0,
                vec![
                    (VarName("z".into()), ConcreteStreamData::Int(3)),
                    (VarName("w".into()), ConcreteStreamData::Int(3))
                ]
                .into_iter()
                .collect(),
            ),
            (
                1,
                vec![
                    (VarName("z".into()), ConcreteStreamData::Int(7)),
                    (VarName("w".into()), ConcreteStreamData::Int(7))
                ]
                .into_iter()
                .collect(),
            ),
        ]
    );
}
