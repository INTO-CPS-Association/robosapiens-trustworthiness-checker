// Test untimed monitoring of LOLA specifications with the async runtime

use futures::stream::StreamExt;
use macro_rules_attribute::apply;
use smol::LocalExecutor;
use smol_macros::test as smol_test;
use std::collections::BTreeMap;
use std::rc::Rc;
use test_log::test;
use trustworthiness_checker::dep_manage::interface::{DependencyKind, create_dependency_manager};
use trustworthiness_checker::io::testing::ManualOutputHandler;
use trustworthiness_checker::semantics::UntimedLolaSemantics;
use trustworthiness_checker::{
    Monitor, Value, VarName, lola_specification, runtime::asynchronous::AsyncMonitorRunner,
};
use trustworthiness_checker::{OutputStream, lola_fixtures::*};

fn output_handler(
    executor: Rc<LocalExecutor<'static>>,
    spec: trustworthiness_checker::LOLASpecification,
) -> Box<ManualOutputHandler<Value>> {
    Box::new(ManualOutputHandler::new(executor, spec.output_vars.clone()))
}

#[test(apply(smol_test))]
async fn test_simple_add_monitor(executor: Rc<LocalExecutor<'static>>) {
    let mut input_streams = input_streams1();
    let spec = lola_specification(&mut spec_simple_add_monitor()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (0, BTreeMap::from([("z".into(), Value::Int(3))]),),
            (1, BTreeMap::from([("z".into(), Value::Int(7))]),),
        ]
    );
}

#[test(apply(smol_test))]
async fn test_simple_add_monitor_large_input(executor: Rc<LocalExecutor<'static>>) {
    let mut input_streams = input_streams_simple_add_untyped(100);
    let spec = lola_specification(&mut spec_simple_add_monitor()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
    assert_eq!(outputs.len(), 100);
}

#[test(apply(smol_test))]
async fn test_simple_add_monitor_does_not_go_away(executor: Rc<LocalExecutor<'static>>) {
    let mut input_streams = input_streams1();
    let spec = lola_specification(&mut spec_simple_add_monitor()).unwrap();
    let outputs = {
        let mut output_handler = output_handler(executor.clone(), spec.clone());
        let outputs = output_handler.get_output();
        let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
            executor.clone(),
            spec.clone(),
            &mut input_streams,
            output_handler,
            create_dependency_manager(DependencyKind::Empty, spec),
        );
        executor.spawn(async_monitor.run()).detach();
        outputs
    };
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (0, BTreeMap::from([("z".into(), Value::Int(3))]),),
            (1, BTreeMap::from([("z".into(), Value::Int(7))]),),
        ]
    );
}

#[test(apply(smol_test))]
async fn test_count_monitor(executor: Rc<LocalExecutor<'static>>) {
    let mut input_streams: BTreeMap<VarName, OutputStream<Value>> = BTreeMap::new();
    let spec = lola_specification(&mut spec_count_monitor()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
        outputs.take(4).enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (0, BTreeMap::from([("x".into(), Value::Int(1))]),),
            (1, BTreeMap::from([("x".into(), Value::Int(2))]),),
            (2, BTreeMap::from([("x".into(), Value::Int(3))]),),
            (3, BTreeMap::from([("x".into(), Value::Int(4))]),),
        ]
    );
}

#[test(apply(smol_test))]
async fn test_eval_monitor(executor: Rc<LocalExecutor<'static>>) {
    let mut input_streams = input_streams2();
    let spec = lola_specification(&mut spec_eval_monitor()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![
            (
                0,
                BTreeMap::from([("z".into(), Value::Int(3)), ("w".into(), Value::Int(3))]),
            ),
            (
                1,
                BTreeMap::from([("z".into(), Value::Int(7)), ("w".into(), Value::Int(7))]),
            ),
        ]
    );
}

#[test(apply(smol_test))]
async fn test_multiple_parameters(executor: Rc<LocalExecutor<'static>>) {
    let mut input_streams = input_streams1();
    let mut spec = "in x\nin y\nout r1\nout r2\nr1 =x+y\nr2 = x * y";
    let spec = lola_specification(&mut spec).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
    assert_eq!(outputs.len(), 2);
    assert_eq!(
        outputs,
        vec![
            (
                0,
                BTreeMap::from([("r1".into(), Value::Int(3)), ("r2".into(), Value::Int(2)),]),
            ),
            (
                1,
                BTreeMap::from([("r1".into(), Value::Int(7)), ("r2".into(), Value::Int(12)),]),
            ),
        ]
    );
}

#[test(apply(smol_test))]
async fn test_maple_sequence(executor: Rc<LocalExecutor<'static>>) {
    let mut input_streams = maple_valid_input_stream(10);
    let spec = lola_specification(&mut spec_maple_sequence()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
    let maple_outputs = outputs
        .into_iter()
        .map(|(i, o)| (i, o[&"maple".into()].clone()));
    let expected_outputs = vec![
        (0, Value::Bool(true)),
        (1, Value::Bool(true)),
        (2, Value::Bool(true)),
        (3, Value::Bool(true)),
        (4, Value::Bool(true)),
        (5, Value::Bool(true)),
        (6, Value::Bool(true)),
        (7, Value::Bool(true)),
        (8, Value::Bool(true)),
        (9, Value::Bool(true)),
    ];

    assert_eq!(maple_outputs.collect::<Vec<_>>(), expected_outputs);
}

#[test(apply(smol_test))]
async fn test_defer_stream_1(executor: Rc<LocalExecutor<'static>>) {
    let mut input_streams = input_streams_defer_1();
    let spec = lola_specification(&mut spec_defer()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
    let expected_outputs = vec![
        (0, BTreeMap::from([("z".into(), Value::Unknown)])),
        (1, BTreeMap::from([("z".into(), Value::Int(2))])),
        (2, BTreeMap::from([("z".into(), Value::Int(3))])),
        (3, BTreeMap::from([("z".into(), Value::Int(4))])),
        (4, BTreeMap::from([("z".into(), Value::Int(5))])),
        (5, BTreeMap::from([("z".into(), Value::Int(6))])),
        (6, BTreeMap::from([("z".into(), Value::Int(7))])),
        (7, BTreeMap::from([("z".into(), Value::Int(8))])),
        (8, BTreeMap::from([("z".into(), Value::Int(9))])),
        (9, BTreeMap::from([("z".into(), Value::Int(10))])),
        (10, BTreeMap::from([("z".into(), Value::Int(11))])),
        (11, BTreeMap::from([("z".into(), Value::Int(12))])),
        (12, BTreeMap::from([("z".into(), Value::Int(13))])),
        (13, BTreeMap::from([("z".into(), Value::Int(14))])),
        (14, BTreeMap::from([("z".into(), Value::Int(15))])),
    ];
    assert_eq!(outputs.len(), expected_outputs.len());
    for (x, y) in outputs.iter().zip(expected_outputs.iter()) {
        assert_eq!(x, y);
    }
}

#[test(apply(smol_test))]
async fn test_defer_stream_2(executor: Rc<LocalExecutor<'static>>) {
    let mut input_streams = input_streams_defer_2();
    let spec = lola_specification(&mut spec_defer()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
    let expected_outputs = vec![
        (0, BTreeMap::from([("z".into(), Value::Unknown)])),
        (1, BTreeMap::from([("z".into(), Value::Unknown)])),
        (2, BTreeMap::from([("z".into(), Value::Unknown)])),
        (3, BTreeMap::from([("z".into(), Value::Int(4))])),
        (4, BTreeMap::from([("z".into(), Value::Int(5))])),
        (5, BTreeMap::from([("z".into(), Value::Int(6))])),
        (6, BTreeMap::from([("z".into(), Value::Int(7))])),
        (7, BTreeMap::from([("z".into(), Value::Int(8))])),
        (8, BTreeMap::from([("z".into(), Value::Int(9))])),
        (9, BTreeMap::from([("z".into(), Value::Int(10))])),
        (10, BTreeMap::from([("z".into(), Value::Int(11))])),
        (11, BTreeMap::from([("z".into(), Value::Int(12))])),
        (12, BTreeMap::from([("z".into(), Value::Int(13))])),
        (13, BTreeMap::from([("z".into(), Value::Int(14))])),
        (14, BTreeMap::from([("z".into(), Value::Int(15))])),
    ];
    assert_eq!(outputs.len(), expected_outputs.len());
    for (x, y) in outputs.iter().zip(expected_outputs.iter()) {
        assert_eq!(x, y);
    }
}

#[test(apply(smol_test))]
async fn test_defer_stream_3(executor: Rc<LocalExecutor<'static>>) {
    let mut input_streams = input_streams_defer_3();
    let spec = lola_specification(&mut spec_defer()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
    let expected_outputs = vec![
        (0, BTreeMap::from([("z".into(), Value::Unknown)])),
        (1, BTreeMap::from([("z".into(), Value::Unknown)])),
        (2, BTreeMap::from([("z".into(), Value::Unknown)])),
        (3, BTreeMap::from([("z".into(), Value::Unknown)])),
        (4, BTreeMap::from([("z".into(), Value::Unknown)])),
        (5, BTreeMap::from([("z".into(), Value::Unknown)])),
        (6, BTreeMap::from([("z".into(), Value::Unknown)])),
        (7, BTreeMap::from([("z".into(), Value::Unknown)])),
        (8, BTreeMap::from([("z".into(), Value::Unknown)])),
        (9, BTreeMap::from([("z".into(), Value::Unknown)])),
        (10, BTreeMap::from([("z".into(), Value::Unknown)])),
        (11, BTreeMap::from([("z".into(), Value::Unknown)])),
        (12, BTreeMap::from([("z".into(), Value::Int(13))])),
        (13, BTreeMap::from([("z".into(), Value::Int(14))])),
        (14, BTreeMap::from([("z".into(), Value::Int(15))])),
    ];
    assert_eq!(outputs.len(), expected_outputs.len());
    for (x, y) in outputs.iter().zip(expected_outputs.iter()) {
        assert_eq!(x, y);
    }
}

#[test(apply(smol_test))]
async fn test_defer_stream_4(executor: Rc<LocalExecutor<'static>>) {
    let mut input_streams = input_streams_defer_4();
    let spec = lola_specification(&mut spec_defer()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
    // Notice one output "too many". This is expected behaviour (at least with a global default
    // history_length = 10 for defer) since once e = x[-1, 0] has arrived
    // the stream for z = defer(e) will continue as long as x[-1, 0] keeps
    // producing values (making use of its history) which can continue beyond
    // the lifetime of the stream for e (since it does not depend on e any more
    // once a value has been received). This differs from the behaviour of
    // eval(e) which stops if e stops.
    //
    // See also: Comment on sindex combinator.
    let expected_outputs = vec![
        (0, BTreeMap::from([("z".into(), Value::Unknown)])),
        (1, BTreeMap::from([("z".into(), Value::Unknown)])),
        (2, BTreeMap::from([("z".into(), Value::Int(1))])),
        (3, BTreeMap::from([("z".into(), Value::Int(2))])),
        (4, BTreeMap::from([("z".into(), Value::Int(3))])),
        (5, BTreeMap::from([("z".into(), Value::Int(4))])),
    ];
    assert_eq!(outputs.len(), expected_outputs.len());
    for (x, y) in outputs.iter().zip(expected_outputs.iter()) {
        assert_eq!(x, y);
    }
}

#[test(apply(smol_test))]
async fn test_future_indexing(executor: Rc<LocalExecutor<'static>>) {
    let mut input_streams = input_streams_indexing();
    let spec = lola_specification(&mut spec_future_indexing()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
    assert_eq!(outputs.len(), 6);
    let expected_outputs = vec![
        (
            0,
            BTreeMap::from([("z".into(), Value::Int(1)), ("a".into(), Value::Int(0))]),
        ),
        (
            1,
            BTreeMap::from([("z".into(), Value::Int(2)), ("a".into(), Value::Int(1))]),
        ),
        (
            2,
            BTreeMap::from([("z".into(), Value::Int(3)), ("a".into(), Value::Int(2))]),
        ),
        (
            3,
            BTreeMap::from([("z".into(), Value::Int(4)), ("a".into(), Value::Int(3))]),
        ),
        (
            4,
            BTreeMap::from([("z".into(), Value::Int(5)), ("a".into(), Value::Int(4))]),
        ),
        (
            5,
            BTreeMap::from([
                ("z".into(), Value::Int(0)), // The default value
                ("a".into(), Value::Int(5)),
            ]),
        ),
    ];
    assert_eq!(outputs, expected_outputs);
}

#[test(apply(smol_test))]
async fn test_past_indexing(executor: Rc<LocalExecutor<'static>>) {
    let mut input_streams = input_streams_indexing();
    let spec = lola_specification(&mut spec_past_indexing()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
    assert_eq!(outputs.len(), 7); // NOTE: 1 "too" many. See comment sindex combinator
    let expected_outputs = vec![
        (0, BTreeMap::from([("z".into(), Value::Int(42))])),
        (1, BTreeMap::from([("z".into(), Value::Int(0))])),
        (2, BTreeMap::from([("z".into(), Value::Int(1))])),
        (3, BTreeMap::from([("z".into(), Value::Int(2))])),
        (4, BTreeMap::from([("z".into(), Value::Int(3))])),
        (5, BTreeMap::from([("z".into(), Value::Int(4))])),
        (6, BTreeMap::from([("z".into(), Value::Int(5))])),
    ];
    assert_eq!(outputs, expected_outputs);
}
