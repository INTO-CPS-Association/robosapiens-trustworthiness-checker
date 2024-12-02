// Test untimed monitoring of LOLA specifications with the async runtime

use futures::stream::{BoxStream, StreamExt};
use std::collections::BTreeMap;
use trustworthiness_checker::queuing_runtime::QueuingMonitorRunner;
use trustworthiness_checker::type_checking::type_check;
use trustworthiness_checker::{
    async_runtime::AsyncMonitorRunner, lola_specification, Monitor, VarName,
};
use trustworthiness_checker::{TypedUntimedLolaSemantics, Value};
mod lola_fixtures;
use lola_fixtures::*;

#[tokio::test]
async fn test_simple_add_monitor() {
    let mut input_streams = input_streams3();
    let spec = lola_specification(&mut spec_simple_add_monitor_typed()).unwrap();
    let spec = type_check(spec).expect("Type check failed");
    let mut async_monitor =
        AsyncMonitorRunner::<_, _, TypedUntimedLolaSemantics, _>::new(spec, &mut input_streams);
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
        async_monitor.monitor_outputs().enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (
                0,
                vec![(VarName("z".into()), Value::Int(3))]
                    .into_iter()
                    .collect(),
            ),
            (
                1,
                vec![(VarName("z".into()), Value::Int(7))]
                    .into_iter()
                    .collect(),
            ),
        ]
    );
}

#[tokio::test]
async fn test_concat_monitor() {
    let mut input_streams = input_streams4();
    let spec = lola_specification(&mut spec_typed_string_concat()).unwrap();
    let spec = type_check(spec).expect("Type check failed");
    // let mut async_monitor =
    // AsyncMonitorRunner::<_, _, TypedUntimedLolaSemantics, _>::new(spec, input_streams);
    let mut async_monitor =
        QueuingMonitorRunner::<_, _, TypedUntimedLolaSemantics, _>::new(spec, &mut input_streams);
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
        async_monitor.monitor_outputs().enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (
                0,
                vec![(VarName("z".into()), Value::Str("ab".into()))]
                    .into_iter()
                    .collect(),
            ),
            (
                1,
                vec![(VarName("z".into()), Value::Str("cd".into()))]
                    .into_iter()
                    .collect(),
            ),
        ]
    );
}

#[tokio::test]
async fn test_count_monitor() {
    let mut input_streams: BTreeMap<VarName, BoxStream<'static, Value>> = BTreeMap::new();
    let spec = lola_specification(&mut spec_typed_count_monitor()).unwrap();
    let spec = type_check(spec).expect("Type check failed");
    let mut async_monitor =
        AsyncMonitorRunner::<_, _, TypedUntimedLolaSemantics, _>::new(spec, &mut input_streams);
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = async_monitor
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
                vec![(VarName("x".into()), Value::Int(1))]
                    .into_iter()
                    .collect(),
            ),
            (
                1,
                vec![(VarName("x".into()), Value::Int(2))]
                    .into_iter()
                    .collect(),
            ),
            (
                2,
                vec![(VarName("x".into()), Value::Int(3))]
                    .into_iter()
                    .collect(),
            ),
            (
                3,
                vec![(VarName("x".into()), Value::Int(4))]
                    .into_iter()
                    .collect(),
            ),
        ]
    );
}

#[tokio::test]
#[ignore = "Not currently working"]
async fn test_eval_monitor() {
    let mut input_streams = input_streams2();
    let spec = lola_specification(&mut spec_typed_eval_monitor()).unwrap();
    let spec = type_check(spec).expect("Type check failed");
    println!("{:?}", spec);
    let mut async_monitor =
        AsyncMonitorRunner::<_, _, TypedUntimedLolaSemantics, _>::new(spec, &mut input_streams);
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
        async_monitor.monitor_outputs().enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (
                0,
                vec![
                    (VarName("z".into()), Value::Int(3)),
                    (VarName("w".into()), Value::Int(3))
                ]
                .into_iter()
                .collect(),
            ),
            (
                1,
                vec![
                    (VarName("z".into()), Value::Int(7)),
                    (VarName("w".into()), Value::Int(7))
                ]
                .into_iter()
                .collect(),
            ),
        ]
    );
}
