use futures::stream::StreamExt;
use macro_rules_attribute::apply;
use smol::LocalExecutor;
use smol_macros::test as smol_test;
use std::collections::BTreeMap;
use std::rc::Rc;
use test_log::test;
#[macro_use]
extern crate approx;

use tracing::info;
use trustworthiness_checker::dep_manage::interface::{DependencyKind, create_dependency_manager};
use trustworthiness_checker::io::testing::ManualOutputHandler;
use trustworthiness_checker::lang::dynamic_lola::type_checker::{
    TypedLOLASpecification, type_check,
};
use trustworthiness_checker::{
    Monitor, VarName, lola_specification, runtime::asynchronous::AsyncMonitorRunner,
};
use trustworthiness_checker::{OutputStream, lola_fixtures::*};
use trustworthiness_checker::{Value, semantics::TypedUntimedLolaSemantics};

fn output_handler(
    executor: Rc<LocalExecutor<'static>>,
    spec: TypedLOLASpecification,
) -> Box<ManualOutputHandler<Value>> {
    Box::new(ManualOutputHandler::new(executor, spec.output_vars.clone()))
}

#[test(apply(smol_test))]
async fn test_simple_add_monitor(executor: Rc<LocalExecutor<'static>>) {
    let input_streams = input_streams3();
    let spec_untyped = lola_specification(&mut spec_simple_add_monitor_typed()).unwrap();
    let spec = type_check(spec_untyped.clone()).expect("Type check failed");
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, TypedUntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec,
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec_untyped),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![(0, vec![Value::Int(3)]), (1, vec![Value::Int(7)]),]
    );
}

#[test(apply(smol_test))]
async fn test_simple_modulo_monitor_typed(executor: Rc<LocalExecutor<'static>>) {
    let input_streams = input_streams3();
    let spec_untyped = lola_specification(&mut spec_simple_modulo_monitor_typed()).unwrap();
    let spec = type_check(spec_untyped.clone()).expect("Type check failed");
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, TypedUntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec,
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec_untyped),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![(0, vec![Value::Int(0)]), (1, vec![Value::Int(1)]),]
    );
}

#[test(apply(smol_test))]
async fn test_simple_add_monitor_float(executor: Rc<LocalExecutor<'static>>) {
    let input_streams = input_streams_float();
    let spec_untyped = lola_specification(&mut spec_simple_add_monitor_typed_float()).unwrap();
    let spec = type_check(spec_untyped.clone()).expect("Type check failed");
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, TypedUntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec,
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec_untyped),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
    assert_eq!(outputs.len(), 2);
    match outputs[0].1[0] {
        Value::Float(f) => assert_abs_diff_eq!(f, 3.7, epsilon = 1e-4),
        _ => panic!("Expected float"),
    }
    match outputs[1].1[0] {
        Value::Float(f) => assert_abs_diff_eq!(f, 7.7, epsilon = 1e-4),
        _ => panic!("Expected float"),
    }
}

#[test(apply(smol_test))]
async fn test_concat_monitor(executor: Rc<LocalExecutor<'static>>) {
    let input_streams = input_streams4();
    let spec_untyped = lola_specification(&mut spec_typed_string_concat()).unwrap();
    let spec = type_check(spec_untyped.clone()).expect("Type check failed");
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, TypedUntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec,
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec_untyped),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (0, vec![Value::Str("ab".into())]),
            (1, vec![Value::Str("cd".into())]),
        ]
    );
}

#[test(apply(smol_test))]
async fn test_count_monitor(executor: Rc<LocalExecutor<'static>>) {
    let input_streams: BTreeMap<VarName, OutputStream<Value>> = BTreeMap::new();
    let spec_untyped = lola_specification(&mut spec_typed_count_monitor()).unwrap();
    let spec = type_check(spec_untyped.clone()).expect("Type check failed");
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, TypedUntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec,
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec_untyped),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, Vec<Value>)> = outputs.take(4).enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (0, vec![Value::Int(1)]),
            (1, vec![Value::Int(2)]),
            (2, vec![Value::Int(3)]),
            (3, vec![Value::Int(4)]),
        ]
    );
}

#[test(apply(smol_test))]
#[ignore = "Not currently working"]
async fn test_eval_monitor(executor: Rc<LocalExecutor<'static>>) {
    let input_streams = input_streams2();
    let spec_untyped = lola_specification(&mut spec_typed_dynamic_monitor()).unwrap();
    let spec = type_check(spec_untyped.clone()).expect("Type check failed");
    info!("{:?}", spec);
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, TypedUntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec,
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec_untyped),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (0, vec![Value::Int(3), Value::Int(3)]),
            (1, vec![Value::Int(7), Value::Int(7)]),
        ]
    );
}

#[test(apply(smol_test))]
async fn test_multiple_parameters(executor: Rc<LocalExecutor<'static>>) {
    let input_streams = input_streams3();
    let mut spec = "in x : Int\nin y : Int\nout r1 : Int\nout r2 : Int\nr1 =x+y\nr2 = x * y";
    let spec_untyped = lola_specification(&mut spec).unwrap();
    let spec = type_check(spec_untyped.clone()).expect("Type check failed");
    info!("{:?}", spec);
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, TypedUntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec,
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec_untyped),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
    assert_eq!(outputs.len(), 2);
    assert_eq!(
        outputs,
        vec![
            (0, vec![Value::Int(3), Value::Int(2)]),
            (1, vec![Value::Int(7), Value::Int(12)]),
        ]
    );
}
