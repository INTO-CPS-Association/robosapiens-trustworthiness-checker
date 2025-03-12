// Test untimed monitoring of LOLA specifications with the async runtime

use futures::stream::{BoxStream, StreamExt};
use std::collections::BTreeMap;
use test_log::test;
use tracing::info;
use trustworthiness_checker::dep_manage::interface::{DependencyKind, create_dependency_manager};
use trustworthiness_checker::io::testing::ManualOutputHandler;
use trustworthiness_checker::lang::dynamic_lola::type_checker::type_check;
use trustworthiness_checker::lola_fixtures::*;
use trustworthiness_checker::runtime::queuing::QueuingMonitorRunner;
use trustworthiness_checker::{
    Monitor, VarName, lola_specification, runtime::asynchronous::AsyncMonitorRunner,
};
use trustworthiness_checker::{Value, semantics::TypedUntimedLolaSemantics};

#[test(tokio::test)]
async fn test_simple_add_monitor() {
    let mut input_streams = input_streams3();
    let spec_untyped = lola_specification(&mut spec_simple_add_monitor_typed()).unwrap();
    let spec = type_check(spec_untyped.clone()).expect("Type check failed");
    let mut output_handler = Box::new(ManualOutputHandler::new(spec.output_vars.clone()));
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, TypedUntimedLolaSemantics, _>::new(
        spec,
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, Box::new(spec_untyped)),
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
async fn test_concat_monitor() {
    let mut input_streams = input_streams4();
    let spec_untyped = lola_specification(&mut spec_typed_string_concat()).unwrap();
    let spec = type_check(spec_untyped.clone()).expect("Type check failed");
    // let mut async_monitor =
    // AsyncMonitorRunner::<_, _, TypedUntimedLolaSemantics, _>::new(spec, input_streams);
    let mut output_handler = Box::new(ManualOutputHandler::new(spec.output_vars.clone()));
    let outputs = output_handler.get_output();
    let async_monitor = QueuingMonitorRunner::<_, _, TypedUntimedLolaSemantics, _>::new(
        spec,
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, Box::new(spec_untyped)),
    );
    tokio::spawn(async_monitor.run());
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (
                0,
                BTreeMap::from([(VarName("z".into()), Value::Str("ab".into()))]),
            ),
            (
                1,
                BTreeMap::from([(VarName("z".into()), Value::Str("cd".into()))]),
            ),
        ]
    );
}

#[test(tokio::test)]
async fn test_count_monitor() {
    let mut input_streams: BTreeMap<VarName, BoxStream<'static, Value>> = BTreeMap::new();
    let spec_untyped = lola_specification(&mut spec_typed_count_monitor()).unwrap();
    let spec = type_check(spec_untyped.clone()).expect("Type check failed");
    let mut output_handler = Box::new(ManualOutputHandler::new(spec.output_vars.clone()));
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, TypedUntimedLolaSemantics, _>::new(
        spec,
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, Box::new(spec_untyped)),
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
#[ignore = "Not currently working"]
async fn test_eval_monitor() {
    let mut input_streams = input_streams2();
    let spec_untyped = lola_specification(&mut spec_typed_eval_monitor()).unwrap();
    let spec = type_check(spec_untyped.clone()).expect("Type check failed");
    info!("{:?}", spec);
    let mut output_handler = Box::new(ManualOutputHandler::new(spec.output_vars.clone()));
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, TypedUntimedLolaSemantics, _>::new(
        spec,
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, Box::new(spec_untyped)),
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
    let mut input_streams = input_streams3();
    let mut spec = "in x : Int\nin y : Int\nout r1 : Int\nout r2 : Int\nr1 =x+y\nr2 = x * y";
    let spec_untyped = lola_specification(&mut spec).unwrap();
    let spec = type_check(spec_untyped.clone()).expect("Type check failed");
    info!("{:?}", spec);
    let mut output_handler = Box::new(ManualOutputHandler::new(spec.output_vars.clone()));
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, TypedUntimedLolaSemantics, _>::new(
        spec,
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, Box::new(spec_untyped)),
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
