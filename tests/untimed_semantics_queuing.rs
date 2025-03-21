use futures::stream::StreamExt;
use macro_rules_attribute::apply;
use smol::LocalExecutor;
use smol_macros::test as smol_test;
use std::collections::BTreeMap;
use std::rc::Rc;
use test_log::test;
use trustworthiness_checker::dep_manage::interface::{DependencyKind, create_dependency_manager};
use trustworthiness_checker::io::testing::ManualOutputHandler;
use trustworthiness_checker::{Monitor, Value, VarName, runtime::queuing::QueuingMonitorRunner};
use trustworthiness_checker::{OutputStream, lola_fixtures::*};
use trustworthiness_checker::{lola_specification, semantics::UntimedLolaSemantics};

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
    let async_monitor = QueuingMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
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
async fn test_simple_add_monitor_large_input(executor: Rc<LocalExecutor<'static>>) {
    let mut input_streams = input_streams_simple_add(100);
    let spec = lola_specification(&mut spec_simple_add_monitor()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = QueuingMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
    assert_eq!(outputs.len(), 100);
}

#[test(apply(smol_test))]
async fn test_count_monitor(executor: Rc<LocalExecutor<'static>>) {
    let mut input_streams: BTreeMap<VarName, OutputStream<Value>> = BTreeMap::new();
    let spec = lola_specification(&mut spec_count_monitor()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = QueuingMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
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
    let mut input_streams = input_streams2();
    let spec = lola_specification(&mut spec_eval_monitor()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = QueuingMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
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
    let mut input_streams = input_streams1();
    let mut spec = "in x\nin y\nout r1\nout r2\nr1 =x+y\nr2 = x * y";
    let spec = lola_specification(&mut spec).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = QueuingMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
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
async fn test_defer_stream_1(executor: Rc<LocalExecutor<'static>>) {
    let mut input_streams = input_streams_defer_1();
    let spec = lola_specification(&mut spec_defer()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = QueuingMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
    assert_eq!(outputs.len(), 15);
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
    let async_monitor = QueuingMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
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
    let async_monitor = QueuingMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
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
    let async_monitor = QueuingMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
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
    // eval(e) which stops if e stops.
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
    let async_monitor = QueuingMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
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
        (5, vec![Value::Int(0), Value::Int(5)]), // The default value for z is 0
    ];
    assert_eq!(outputs, expected_outputs);
}

#[test(apply(smol_test))]
async fn test_past_indexing(executor: Rc<LocalExecutor<'static>>) {
    let mut input_streams = input_streams_indexing();
    let spec = lola_specification(&mut spec_past_indexing()).unwrap();
    let mut output_handler = output_handler(executor.clone(), spec.clone());
    let outputs = output_handler.get_output();
    let async_monitor = QueuingMonitorRunner::<_, _, UntimedLolaSemantics, _>::new(
        executor.clone(),
        spec.clone(),
        &mut input_streams,
        output_handler,
        create_dependency_manager(DependencyKind::Empty, spec),
    );
    executor.spawn(async_monitor.run()).detach();
    let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
    assert_eq!(outputs.len(), 7); // NOTE: 1 "too" many. See comment sindex combinator
    let expected_outputs = vec![
        (0, vec![Value::Int(42)]),
        (1, vec![Value::Int(0)]),
        (2, vec![Value::Int(1)]),
        (3, vec![Value::Int(2)]),
        (4, vec![Value::Int(3)]),
        (5, vec![Value::Int(4)]),
        (6, vec![Value::Int(5)]),
    ];
    assert_eq!(outputs, expected_outputs);
}
