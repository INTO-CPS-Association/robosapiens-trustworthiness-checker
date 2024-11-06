// Test untimed monitoring of LOLA specifications with the async runtime

use futures::stream::StreamExt;
use std::collections::BTreeMap;
use trustworthiness_checker::queuing_runtime::QueuingMonitorRunner;
use trustworthiness_checker::type_checking::type_check;
use trustworthiness_checker::{
    async_runtime::AsyncMonitorRunner, lola_specification, Monitor, VarName,
};
use trustworthiness_checker::{ConcreteStreamData, TypedUntimedLolaSemantics};
mod lola_fixtures;
use lola_fixtures::*;

#[tokio::test]
async fn test_simple_add_monitor() {
    let input_streams = input_streams3();
    let spec = lola_specification(&mut spec_simple_add_monitor_typed()).unwrap();
    let spec = type_check(spec).expect("Type check failed");
    let mut async_monitor =
        AsyncMonitorRunner::<_, _, TypedUntimedLolaSemantics, _>::new(spec, input_streams);
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
async fn test_concat_monitor() {
    let input_streams = input_streams4();
    let spec = lola_specification(&mut spec_typed_string_concat()).unwrap();
    let spec = type_check(spec).expect("Type check failed");
    // let mut async_monitor =
    // AsyncMonitorRunner::<_, _, TypedUntimedLolaSemantics, _>::new(spec, input_streams);
    let mut async_monitor =
        QueuingMonitorRunner::<_, _, TypedUntimedLolaSemantics, _>::new(spec, input_streams);
    let outputs: Vec<(usize, BTreeMap<VarName, ConcreteStreamData>)> =
        async_monitor.monitor_outputs().enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (
                0,
                vec![(VarName("z".into()), ConcreteStreamData::Str("ab".into()))]
                    .into_iter()
                    .collect(),
            ),
            (
                1,
                vec![(VarName("z".into()), ConcreteStreamData::Str("cd".into()))]
                    .into_iter()
                    .collect(),
            ),
        ]
    );
}
