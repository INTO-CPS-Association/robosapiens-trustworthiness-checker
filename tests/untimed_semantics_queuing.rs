// Test untimed monitoring of LOLA specifications with the async runtime

use futures::stream::{BoxStream, StreamExt};
use std::collections::BTreeMap;
use trustworthiness_checker::dep_manage::interface::{DependencyKind, create_dependency_manager};
use trustworthiness_checker::io::testing::ManualOutputHandler;
use trustworthiness_checker::lola_fixtures::*;
use trustworthiness_checker::{Monitor, Value, VarName, runtime::queuing::QueuingMonitorRunner};
use trustworthiness_checker::{lola_specification, semantics::UntimedLolaSemantics};

use test_log::test;

#[test(tokio::test)]
async fn test_simple_add_monitor() {
    let mut input_streams = input_streams1();
    let spec = lola_specification(&mut spec_simple_add_monitor()).unwrap();
    let mut output_handler = Box::new(ManualOutputHandler::new(spec.output_vars.clone()));
    let outputs = output_handler.get_output();
    let async_monitor = QueuingMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    tokio::spawn(async_monitor.run());
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (0, BTreeMap::from([(VarName("z".into()), Value::Int(3))]),),
            (1, BTreeMap::from([(VarName("z".into()), Value::Int(7))]),),
        ]
    );
}

#[test(tokio::test)]
async fn test_count_monitor() {
    let mut input_streams: BTreeMap<VarName, BoxStream<'static, Value>> = BTreeMap::new();
    let spec = lola_specification(&mut spec_count_monitor()).unwrap();
    let mut output_handler = Box::new(ManualOutputHandler::new(spec.output_vars.clone()));
    let outputs = output_handler.get_output();
    let async_monitor = QueuingMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    tokio::spawn(async_monitor.run());
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
        outputs.take(4).enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (0, BTreeMap::from([(VarName("x".into()), Value::Int(1))]),),
            (1, BTreeMap::from([(VarName("x".into()), Value::Int(2))]),),
            (2, BTreeMap::from([(VarName("x".into()), Value::Int(3))]),),
            (3, BTreeMap::from([(VarName("x".into()), Value::Int(4))]),),
        ]
    );
}

#[test(tokio::test)]
async fn test_eval_monitor() {
    let mut input_streams = input_streams2();
    let spec = lola_specification(&mut spec_eval_monitor()).unwrap();
    let mut output_handler = Box::new(ManualOutputHandler::new(spec.output_vars.clone()));
    let outputs = output_handler.get_output();
    let async_monitor = QueuingMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    tokio::spawn(async_monitor.run());
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (
                0,
                BTreeMap::from([
                    (VarName("z".into()), Value::Int(3)),
                    (VarName("w".into()), Value::Int(3))
                ]),
            ),
            (
                1,
                BTreeMap::from([
                    (VarName("z".into()), Value::Int(7)),
                    (VarName("w".into()), Value::Int(7))
                ]),
            ),
        ]
    );
}

#[test(tokio::test)]
async fn test_multiple_parameters() {
    let mut input_streams = input_streams1();
    let mut spec = "in x\nin y\nout r1\nout r2\nr1 =x+y\nr2 = x * y";
    let spec = lola_specification(&mut spec).unwrap();
    let mut output_handler = Box::new(ManualOutputHandler::new(spec.output_vars.clone()));
    let outputs = output_handler.get_output();
    let async_monitor = QueuingMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    tokio::spawn(async_monitor.run());
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
    assert_eq!(outputs.len(), 2);
    assert_eq!(
        outputs,
        vec![
            (
                0,
                BTreeMap::from([
                    (VarName("r1".into()), Value::Int(3)),
                    (VarName("r2".into()), Value::Int(2)),
                ]),
            ),
            (
                1,
                BTreeMap::from([
                    (VarName("r1".into()), Value::Int(7)),
                    (VarName("r2".into()), Value::Int(12)),
                ]),
            ),
        ]
    );
}

#[test(tokio::test)]
async fn test_defer_stream_1() {
    let mut input_streams = input_streams_defer_1();
    let spec = lola_specification(&mut spec_defer()).unwrap();
    let mut output_handler = Box::new(ManualOutputHandler::new(spec.output_vars.clone()));
    let outputs = output_handler.get_output();
    let async_monitor = QueuingMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    tokio::spawn(async_monitor.run());
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
    assert_eq!(outputs.len(), 15);
    let expected_outputs = vec![
        (0, BTreeMap::from([(VarName("z".into()), Value::Unknown)])),
        (1, BTreeMap::from([(VarName("z".into()), Value::Int(2))])),
        (2, BTreeMap::from([(VarName("z".into()), Value::Int(3))])),
        (3, BTreeMap::from([(VarName("z".into()), Value::Int(4))])),
        (4, BTreeMap::from([(VarName("z".into()), Value::Int(5))])),
        (5, BTreeMap::from([(VarName("z".into()), Value::Int(6))])),
        (6, BTreeMap::from([(VarName("z".into()), Value::Int(7))])),
        (7, BTreeMap::from([(VarName("z".into()), Value::Int(8))])),
        (8, BTreeMap::from([(VarName("z".into()), Value::Int(9))])),
        (9, BTreeMap::from([(VarName("z".into()), Value::Int(10))])),
        (10, BTreeMap::from([(VarName("z".into()), Value::Int(11))])),
        (11, BTreeMap::from([(VarName("z".into()), Value::Int(12))])),
        (12, BTreeMap::from([(VarName("z".into()), Value::Int(13))])),
        (13, BTreeMap::from([(VarName("z".into()), Value::Int(14))])),
        (14, BTreeMap::from([(VarName("z".into()), Value::Int(15))])),
    ];
    for (x, y) in outputs.iter().zip(expected_outputs.iter()) {
        assert_eq!(x, y);
    }
}
