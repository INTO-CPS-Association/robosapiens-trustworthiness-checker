// Test untimed monitoring of LOLA specifications with the async runtime

use futures::stream::StreamExt;
use trustworthiness_checker::queuing_runtime::QueuingMonitorRunner;
use std::collections::BTreeMap;
use trustworthiness_checker::core::TypeCheckableSpecification;
use trustworthiness_checker::lola_type_system::LOLATypedValue;
use trustworthiness_checker::TypedUntimedLolaSemantics;
use trustworthiness_checker::{
    async_runtime::AsyncMonitorRunner, lola_specification, Monitor, VarName,
};
mod lola_fixtures;
use lola_fixtures::*;

#[tokio::test]
async fn test_simple_add_monitor() {
    let input_streams = input_streams3();
    let spec = lola_specification(&mut spec_simple_add_monitor_typed()).unwrap();
    let spec = spec.type_check().expect("Type check failed");
    let mut async_monitor =
        AsyncMonitorRunner::<_, _, TypedUntimedLolaSemantics, _>::new(spec, input_streams);
    let outputs: Vec<(usize, BTreeMap<VarName, LOLATypedValue>)> =
        async_monitor.monitor_outputs().enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (
                0,
                vec![(VarName("z".into()), LOLATypedValue::Int(3))]
                    .into_iter()
                    .collect(),
            ),
            (
                1,
                vec![(VarName("z".into()), LOLATypedValue::Int(7))]
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
    let spec = spec.type_check().expect("Type check failed");
    // let mut async_monitor =
    // AsyncMonitorRunner::<_, _, TypedUntimedLolaSemantics, _>::new(spec, input_streams);
    let mut async_monitor =
        QueuingMonitorRunner::<_, _, TypedUntimedLolaSemantics, _>::new(spec, input_streams);
    let outputs: Vec<(usize, BTreeMap<VarName, LOLATypedValue>)> =
        async_monitor.monitor_outputs().enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (
                0,
                vec![(VarName("z".into()), LOLATypedValue::Str("ab".into()))]
                    .into_iter()
                    .collect(),
            ),
            (
                1,
                vec![(VarName("z".into()), LOLATypedValue::Str("cd".into()))]
                    .into_iter()
                    .collect(),
            ),
        ]
    );
}
