use approx::assert_abs_diff_eq;
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
    let input_streams = input_streams1();
    let spec = lola_specification(&mut spec_simple_add_monitor()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec.clone(),
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![(0, vec![Value::Int(3)]), (1, vec![Value::Int(7)]),]
    );
}

#[test(apply(smol_test))]
async fn test_simple_modulo_monitor(executor: Rc<LocalExecutor<'static>>) {
    let input_streams = input_streams1();
    let spec = lola_specification(&mut spec_simple_modulo_monitor()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec.clone(),
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
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
    let spec = lola_specification(&mut spec_simple_add_monitor_typed_float()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec.clone(),
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
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
async fn test_simple_add_monitor_large_input(executor: Rc<LocalExecutor<'static>>) {
    let input_streams = input_streams_simple_add(100);
    let spec = lola_specification(&mut spec_simple_add_monitor()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec.clone(),
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
    assert_eq!(outputs.len(), 100);
}

#[test(apply(smol_test))]
async fn test_simple_add_monitor_does_not_go_away(executor: Rc<LocalExecutor<'static>>) {
    let input_streams = input_streams1();
    let spec = lola_specification(&mut spec_simple_add_monitor()).unwrap();
    let outputs = {
        let mut output_handler = output_handler(executor.clone(), spec.clone());
        let outputs = output_handler.get_output();
        let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
            executor.clone(),
            spec.clone(),
            Box::new(input_streams),
            output_handler,
            create_dependency_manager(DependencyKind::Empty, spec),
        );
        executor.spawn(async_monitor.run()).detach();
        outputs
    };
    let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
    assert_eq!(
        outputs,
        vec![(0, vec![Value::Int(3)]), (1, vec![Value::Int(7)]),]
    );
}

#[test(apply(smol_test))]
async fn test_count_monitor(executor: Rc<LocalExecutor<'static>>) {
    let input_streams: BTreeMap<VarName, OutputStream<Value>> = BTreeMap::new();
    let spec = lola_specification(&mut spec_count_monitor()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec.clone(),
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
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
async fn test_eval_monitor(executor: Rc<LocalExecutor<'static>>) {
    let input_streams = input_streams2();
    let spec = lola_specification(&mut spec_dynamic_monitor()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec.clone(),
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
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
async fn test_restricted_dynamic_monitor(executor: Rc<LocalExecutor<'static>>) {
    let input_streams = input_streams2();
    let spec = lola_specification(&mut spec_dynamic_restricted_monitor()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec.clone(),
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
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
    let input_streams = input_streams1();
    let mut spec = "in x\nin y\nout r1\nout r2\nr1 =x+y\nr2 = x * y";
    let spec = lola_specification(&mut spec).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec.clone(),
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
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

#[test(apply(smol_test))]
async fn test_maple_sequence(executor: Rc<LocalExecutor<'static>>) {
    let input_streams = maple_valid_input_stream(10);
    let spec = lola_specification(&mut spec_maple_sequence()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec.clone(),
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec.clone()),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
    let maple_index = spec
        .output_vars
        .iter()
        .position(|v| *v == "maple".into())
        .unwrap();
    let maple_outputs = outputs
        .into_iter()
        .map(|(i, o)| (i, o[maple_index].clone()));
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
    let input_streams = input_streams_defer_1();
    let spec = lola_specification(&mut spec_defer()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec.clone(),
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
    let expected_outputs = vec![
        (0, vec![Value::Unknown]),
        (1, vec![Value::Int(2)]),
        (2, vec![Value::Int(3)]),
        (3, vec![Value::Int(4)]),
        (4, vec![Value::Int(5)]),
        (5, vec![Value::Int(6)]),
        (6, vec![Value::Int(7)]),
        (7, vec![Value::Int(8)]),
        (8, vec![Value::Int(9)]),
        (9, vec![Value::Int(10)]),
        (10, vec![Value::Int(11)]),
        (11, vec![Value::Int(12)]),
        (12, vec![Value::Int(13)]),
        (13, vec![Value::Int(14)]),
        (14, vec![Value::Int(15)]),
    ];
    assert_eq!(outputs.len(), expected_outputs.len());
    for (x, y) in outputs.iter().zip(expected_outputs.iter()) {
        assert_eq!(x, y);
    }
}

#[test(apply(smol_test))]
async fn test_defer_stream_2(executor: Rc<LocalExecutor<'static>>) {
    let input_streams = input_streams_defer_2();
    let spec = lola_specification(&mut spec_defer()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec.clone(),
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
    let expected_outputs = vec![
        (0, vec![Value::Unknown]),
        (1, vec![Value::Unknown]),
        (2, vec![Value::Unknown]),
        (3, vec![Value::Int(4)]),
        (4, vec![Value::Int(5)]),
        (5, vec![Value::Int(6)]),
        (6, vec![Value::Int(7)]),
        (7, vec![Value::Int(8)]),
        (8, vec![Value::Int(9)]),
        (9, vec![Value::Int(10)]),
        (10, vec![Value::Int(11)]),
        (11, vec![Value::Int(12)]),
        (12, vec![Value::Int(13)]),
        (13, vec![Value::Int(14)]),
        (14, vec![Value::Int(15)]),
    ];
    assert_eq!(outputs.len(), expected_outputs.len());
    for (x, y) in outputs.iter().zip(expected_outputs.iter()) {
        assert_eq!(x, y);
    }
}

#[test(apply(smol_test))]
async fn test_defer_stream_3(executor: Rc<LocalExecutor<'static>>) {
    let input_streams = input_streams_defer_3();
    let spec = lola_specification(&mut spec_defer()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec.clone(),
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
    let expected_outputs = vec![
        (0, vec![Value::Unknown]),
        (1, vec![Value::Unknown]),
        (2, vec![Value::Unknown]),
        (3, vec![Value::Unknown]),
        (4, vec![Value::Unknown]),
        (5, vec![Value::Unknown]),
        (6, vec![Value::Unknown]),
        (7, vec![Value::Unknown]),
        (8, vec![Value::Unknown]),
        (9, vec![Value::Unknown]),
        (10, vec![Value::Unknown]),
        (11, vec![Value::Unknown]),
        (12, vec![Value::Int(13)]),
        (13, vec![Value::Int(14)]),
        (14, vec![Value::Int(15)]),
    ];
    assert_eq!(outputs.len(), expected_outputs.len());
    for (x, y) in outputs.iter().zip(expected_outputs.iter()) {
        assert_eq!(x, y);
    }
}

#[test(apply(smol_test))]
async fn test_defer_stream_4(executor: Rc<LocalExecutor<'static>>) {
    let input_streams = input_streams_defer_4();
    let spec = lola_specification(&mut spec_defer()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec.clone(),
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
    // Notice one output "too many". This is expected behaviour (at least with a global default
    // history_length = 10 for defer) since once e = x[-1, 0] has arrived
    // the stream for z = defer(e) will continue as long as x[-1, 0] keeps
    // producing values (making use of its history) which can continue beyond
    // the lifetime of the stream for e (since it does not depend on e any more
    // once a value has been received). This differs from the behaviour of
    // defer(e) which stops if e stops.
    //
    // See also: Comment on sindex combinator.
    let expected_outputs = vec![
        (0, vec![Value::Unknown]),
        (1, vec![Value::Unknown]),
        (2, vec![Value::Int(1)]),
        (3, vec![Value::Int(2)]),
        (4, vec![Value::Int(3)]),
        (5, vec![Value::Int(4)]),
    ];
    assert_eq!(outputs.len(), expected_outputs.len());
    for (x, y) in outputs.iter().zip(expected_outputs.iter()) {
        assert_eq!(x, y);
    }
}

#[test(apply(smol_test))]
async fn test_future_indexing(executor: Rc<LocalExecutor<'static>>) {
    let input_streams = input_streams_indexing();
    let spec = lola_specification(&mut spec_future_indexing()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec.clone(),
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
    assert_eq!(outputs.len(), 6);
    let expected_outputs = vec![
        (0, vec![Value::Int(1), Value::Int(0)]),
        (1, vec![Value::Int(2), Value::Int(1)]),
        (2, vec![Value::Int(3), Value::Int(2)]),
        (3, vec![Value::Int(4), Value::Int(3)]),
        (4, vec![Value::Int(5), Value::Int(4)]),
        (5, vec![Value::Unknown, Value::Int(5)]), // Stream ends - last z is Unknown
    ];
    assert_eq!(outputs, expected_outputs);
}

#[test(apply(smol_test))]
async fn test_past_indexing(executor: Rc<LocalExecutor<'static>>) {
    let input_streams = input_streams_indexing();
    let spec = lola_specification(&mut spec_past_indexing()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        spec.clone(),
        Box::new(input_streams),
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
    assert_eq!(outputs.len(), 7); // NOTE: 1 "too" many. See comment sindex combinator
    let expected_outputs = vec![
        (0, vec![Value::Unknown]),
        (1, vec![Value::Int(0)]),
        (2, vec![Value::Int(1)]),
        (3, vec![Value::Int(2)]),
        (4, vec![Value::Int(3)]),
        (5, vec![Value::Int(4)]),
        (6, vec![Value::Int(5)]),
    ];
    assert_eq!(outputs, expected_outputs);
}
