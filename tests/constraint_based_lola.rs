use futures::stream;
use futures::stream::StreamExt;
use smol::LocalExecutor;
use std::collections::BTreeMap;
use std::rc::Rc;
use trustworthiness_checker::OutputStream;
use trustworthiness_checker::runtime::constraints::ConstraintBasedMonitor;
use trustworthiness_checker::{
    LOLASpecification, io::testing::ManualOutputHandler, lola_specification,
};
use trustworthiness_checker::{Monitor, Value, VarName};

pub fn input_streams1() -> BTreeMap<VarName, OutputStream<Value>> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        "x".into(),
        Box::pin(stream::iter(
            vec![Value::Int(1), 3.into(), 5.into()].into_iter(),
        )) as OutputStream<Value>,
    );
    input_streams.insert(
        "y".into(),
        Box::pin(stream::iter(
            vec![Value::Int(2), 4.into(), 6.into()].into_iter(),
        )) as OutputStream<Value>,
    );
    input_streams
}

pub fn new_input_stream(
    map: BTreeMap<VarName, Vec<Value>>,
) -> BTreeMap<VarName, OutputStream<Value>> {
    let mut input_streams = BTreeMap::new();
    for (name, values) in map {
        input_streams.insert(
            name,
            Box::pin(stream::iter(values.into_iter())) as OutputStream<Value>,
        );
    }
    input_streams
}

fn output_handler(
    executor: Rc<LocalExecutor<'static>>,
    spec: LOLASpecification,
) -> Box<ManualOutputHandler<Value>> {
    Box::new(ManualOutputHandler::new(executor, spec.output_vars.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;
    use strum::IntoEnumIterator;

    use macro_rules_attribute::apply;
    use smol_macros::test as smol_test;
    use test_log::test;

    use trustworthiness_checker::dep_manage::interface::{
        DependencyKind, create_dependency_manager,
    };
    use trustworthiness_checker::lola_fixtures::{
        input_empty, input_streams4, input_streams5, input_streams_float, input_streams_simple_add, spec_empty, spec_simple_add_monitor, spec_simple_add_monitor_typed_float, spec_simple_modulo_monitor
    };

    #[test(apply(smol_test))]
    async fn test_simple_add_monitor(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams1();
            let spec = lola_specification(&mut spec_simple_add_monitor()).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs,
                vec![
                    (0, vec![3.into()]),
                    (1, vec![7.into()]),
                    (2, vec![11.into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_simple_modulo(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams1();
            let spec = lola_specification(&mut spec_simple_modulo_monitor()).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs,
                vec![
                    (0, vec![0.into()]),
                    (1, vec![1.into()]),
                    (2, vec![1.into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_simple_add_monitor_float(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams_float();
            let spec = lola_specification(&mut spec_simple_add_monitor_typed_float()).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
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
    }

    #[test(apply(smol_test))]
    async fn test_simple_add_monitor_large_input(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams_simple_add(100);
            let spec = lola_specification(&mut spec_simple_add_monitor()).unwrap();
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(outputs.len(), 100);
        }
    }

    #[test(apply(smol_test))]
    #[ignore = "Cannot have empty spec or inputs"]
    async fn test_runtime_initialization(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_empty();
            let spec = lola_specification(&mut spec_empty()).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = Box::new(output_handler.get_output());
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<Vec<Value>> = outputs.collect().await;
            assert_eq!(outputs.len(), 0);
        }
    }

    #[test(apply(smol_test))]
    async fn test_var(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams1();
            let mut spec = "in x\nout z\nz =x";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, vec![1.into()]),
                    (1, vec![3.into()]),
                    (2, vec![5.into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_literal_expression(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams1();
            let mut spec = "out z\nz =42";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.take(3).enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, vec![42.into()]),
                    (1, vec![42.into()]),
                    (2, vec![42.into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_addition(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams1();
            let mut spec = "in x\nout z\nz =x+1";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, vec![2.into()]),
                    (1, vec![4.into()]),
                    (2, vec![6.into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_subtraction(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams1();
            let mut spec = "in x\nout z\nz =x-10";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, vec![Value::Int(-9)]),
                    (1, vec![Value::Int(-7)]),
                    (2, vec![Value::Int(-5)]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_index_past(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams1();
            let mut spec = "in x\nout z\nz =x[-1, 0]";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (
                        // Resolved to default on first step
                        0,
                        vec![0.into()],
                    ),
                    (
                        // Resolving to previous value on second step
                        1,
                        vec![1.into()],
                    ),
                    (
                        // Resolving to previous value on second step
                        2,
                        vec![3.into()],
                    ),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_index_past_mult_dependencies(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            // Specifically tests past indexing that the cleaner does not delete dependencies too early
            let mut input_streams = input_streams1();
            let mut spec = "in x\nout z1\nout z2\nz2 = x[-2, 0]\nz1 = x[-1, 0]";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (
                        // Both resolve to default
                        0,
                        vec![0.into(), 0.into()],
                    ),
                    (
                        // z1 resolves to prev, z2 resolves to default
                        1,
                        vec![1.into(), 0.into()],
                    ),
                    (
                        // z1 resolves to prev, z2 resolves to prev_prev
                        2,
                        vec![3.into(), 1.into()],
                    ),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_index_future(executor: Rc<LocalExecutor<'static>>) {
        for kind in [
            DependencyKind::Empty, // DependencyKind::DepGraph // Not supported correctly for
                                   // future indexing
        ] {
            let mut input_streams = input_streams1();
            let mut spec = "in x\nout z\nz =x[1, 0]";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(outputs.len(), 2);
            assert_eq!(
                outputs,
                vec![
                    (
                        // Resolved to index 1 on first step
                        0,
                        vec![3.into()],
                    ),
                    (1, vec![5.into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_if_else_expression(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams5();
            let mut spec = "in x\nin y\nout z\nz =if(x) then y else false"; // And-gate
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, vec![true.into()]),
                    (1, vec![false.into()]),
                    (2, vec![false.into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_string_append(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams4();
            let mut spec = "in x\nin y\nout z\nz =x++y";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 2);
            assert_eq!(
                outputs,
                vec![(0, vec!["ab".into()]), (1, vec!["cd".into()]),]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_multiple_parameters(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams1();
            let mut spec = "in x\nin y\nout r1\nout r2\nr1 =x+y\nr2 = x * y";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, vec![3.into(), 2.into()]),
                    (1, vec![7.into(), 12.into()]),
                    (2, vec![11.into(), 30.into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_default_no_unknown(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let v = vec![0.into(), 1.into(), 2.into()];
            let mut input_streams = new_input_stream(BTreeMap::from([("x".into(), v)]));
            let mut spec = "in x\nout y\ny=default(x, 42)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, vec![0.into()]),
                    (1, vec![1.into()]),
                    (2, vec![2.into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_default_all_unknown(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let v = vec![Value::Unknown, Value::Unknown, Value::Unknown];
            let mut input_streams = new_input_stream(BTreeMap::from([("x".into(), v)]));
            let mut spec = "in x\nout y\ny=default(x, 42)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, vec![42.into()]),
                    (1, vec![42.into()]),
                    (2, vec![42.into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_default_one_unknown(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let v = vec![0.into(), Value::Unknown, 2.into()];
            let mut input_streams = new_input_stream(BTreeMap::from([("x".into(), v)]));
            let mut spec = "in x\nout y\ny=default(x, 42)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, vec![0.into()]),
                    (1, vec![42.into()]),
                    (2, vec![2.into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_counter(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let mut input_streams = new_input_stream(BTreeMap::from([]));
            let mut spec = "out y\ny=y[-1, 0] + 1";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().take(3).collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, vec![1.into()]),
                    (1, vec![2.into()]),
                    (2, vec![3.into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_defer(executor: Rc<LocalExecutor<'static>>) {
        // Notice that even though we first say "x + 1", "x + 2", it continues evaluating "x + 1"
        for kind in DependencyKind::iter() {
            let x = vec![0.into(), 1.into(), 2.into()];
            let e = vec!["x + 1".into(), "x + 2".into(), "x + 3".into()];
            let mut input_streams =
                new_input_stream(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
            let mut spec = "in x\nin e\nout z\nz = defer(e)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, vec![1.into()]),
                    (1, vec![2.into()]),
                    (2, vec![3.into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_defer_x_squared(executor: Rc<LocalExecutor<'static>>) {
        // This test is interesting since we use x twice in the eval strings
        for kind in DependencyKind::iter() {
            let x = vec![1.into(), 2.into(), 3.into()];
            let e = vec!["x * x".into(), "x * x + 1".into(), "x * x + 2".into()];
            let mut input_streams =
                new_input_stream(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
            let mut spec = "in x\nin e\nout z\nz = defer(e)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, vec![1.into()]),
                    (1, vec![4.into()]),
                    (2, vec![9.into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_defer_unknown(executor: Rc<LocalExecutor<'static>>) {
        // Using unknown to represent no data on the stream
        for kind in DependencyKind::iter() {
            let x = vec![0.into(), 1.into(), 2.into()];
            let e = vec![Value::Unknown, "x + 1".into(), "x + 2".into()];
            let mut input_streams =
                new_input_stream(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
            let mut spec = "in x\nin e\nout z\nz = defer(e)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, vec![Value::Unknown]),
                    (1, vec![2.into()]),
                    (2, vec![3.into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_defer_unknown2(executor: Rc<LocalExecutor<'static>>) {
        // Unknown followed by property followed by unknown returns [U; val; val].
        for kind in DependencyKind::iter() {
            let x = vec![0.into(), 1.into(), 2.into()];
            let e = vec![Value::Unknown, "x + 1".into(), Value::Unknown];
            let mut input_streams =
                new_input_stream(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
            let mut spec = "in x\nin e\nout z\nz = defer(e)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, vec![Value::Unknown]),
                    (1, vec![2.into()]),
                    (2, vec![3.into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_defer_dependency(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let x = vec![0.into(), 1.into(), 2.into(), 3.into()];
            let e = vec![
                Value::Unknown,
                "x[-1, 42]".into(),
                Value::Unknown,
                Value::Unknown,
            ];
            let mut input_streams =
                new_input_stream(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
            let mut spec = "in x\nin e\nout z\nz = defer(e)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 4);
            let sample_1 = if kind == DependencyKind::DepGraph {
                // Because the new dependency is added at time idx 1 and requires knowledge
                // of cleaned memory, it is not solved at this time index.
                (1, vec![Value::Unknown])
            } else {
                // Might need more cases in the future
                (1, vec![0.into()])
            };
            assert_eq!(
                outputs,
                vec![
                    (0, vec![Value::Unknown]),
                    sample_1,
                    (2, vec![1.into()]),
                    (3, vec![2.into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_update_both_init(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let x = vec!["x0".into(), "x1".into(), "x2".into()];
            let y = vec!["y0".into(), "y1".into(), "y2".into()];
            let mut input_streams =
                new_input_stream(BTreeMap::from([("x".into(), x), ("y".into(), y)]));
            let mut spec = "in x\nin y\nout z\nz = update(x, y)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, vec!["y0".into()]),
                    (1, vec!["y1".into()]),
                    (2, vec!["y2".into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_update_first_x_then_y(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let x = vec!["x0".into(), "x1".into(), "x2".into(), "x3".into()];
            let y = vec![Value::Unknown, "y1".into(), Value::Unknown, "y3".into()];
            let mut input_streams =
                new_input_stream(BTreeMap::from([("x".into(), x), ("y".into(), y)]));
            let mut spec = "in x\nin y\nout z\nz = update(x, y)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 4);
            assert_eq!(
                outputs,
                vec![
                    (0, vec!["x0".into()]),
                    (1, vec!["y1".into()]),
                    (2, vec![Value::Unknown]),
                    (3, vec!["y3".into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_update_defer(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let x = vec!["x0".into(), "x1".into(), "x2".into(), "x3".into()];
            let e = vec![Value::Unknown, "x".into(), "x".into(), "x".into()];
            let mut input_streams =
                new_input_stream(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
            let mut spec = "in x\nin e\nout z\nz = update(\"def\", defer(e))";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 4);
            assert_eq!(
                outputs,
                vec![
                    (0, vec!["def".into()]),
                    (1, vec!["x1".into()]),
                    (2, vec!["x2".into()]),
                    (3, vec!["x3".into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_defer_update(executor: Rc<LocalExecutor<'static>>) {
        // This spec is essentially a "picker". The first input to provide a value is selected
        // for the rest of execution
        for kind in DependencyKind::iter() {
            let x = vec![Value::Unknown, "x".into(), "x_lost".into(), "x_sad".into()];
            let y = vec![
                Value::Unknown,
                "y".into(),
                "y_won!".into(),
                "y_happy".into(),
            ];
            let mut input_streams =
                new_input_stream(BTreeMap::from([("x".into(), x), ("y".into(), y)]));
            let mut spec = "in x\nin y\nout z\nz = defer(update(x, y))";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 4);
            assert_eq!(
                outputs,
                vec![
                    (0, vec![Value::Unknown]),
                    (1, vec!["y".into()]),
                    (2, vec!["y_won!".into()]),
                    (3, vec!["y_happy".into()]),
                ]
            );
        }
    }

    #[test(apply(smol_test))]
    async fn test_recursive_update(executor: Rc<LocalExecutor<'static>>) {
        // This is essentially the same as z = x, but it tests recursively using update
        for kind in DependencyKind::iter() {
            let x = vec!["x0".into(), "x1".into(), "x2".into(), "x3".into()];
            let mut input_streams = new_input_stream(BTreeMap::from([("x".into(), x)]));
            let mut spec = "in x\nout z\nz = update(x, z)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 4);
            assert_eq!(
                outputs,
                vec![
                    (0, vec!["x0".into()]),
                    (1, vec!["x1".into()]),
                    (2, vec!["x2".into()]),
                    (3, vec!["x3".into()]),
                ]
            );
        }
    }

    // NOTE: While this test is interesting, it cannot work due to how defer is handled.
    // When defer receives a prop stream it changes state from being a defer expression into
    // the received prop stream. Thus, it cannot be used recursively.
    // This is the reason why we also need eval for the constraint based runtime.
    #[test(apply(smol_test))]
    async fn test_recursive_update_defer(executor: Rc<LocalExecutor<'static>>) {
        for kind in DependencyKind::iter() {
            let x = vec!["0".into(), "1".into(), "2".into(), "3".into()];
            let mut input_streams = new_input_stream(BTreeMap::from([("x".into(), x)]));
            let mut spec = "in x\nout z\nz = update(defer(x), z)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(executor.clone(), spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                executor.clone(),
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert!(outputs.len() == 4);
            // The outcommented are if update(defer(x), z) worked as we initially envisioned
            assert_eq!(
                outputs,
                vec![
                    (0, vec![0.into()]),
                    (
                        1,
                        vec![0.into()],
                        // vec![1.into()],
                    ),
                    (
                        2,
                        vec![0.into()],
                        // vec![2.into()],
                    ),
                    (
                        3,
                        vec![0.into()],
                        // vec![3.into()]),
                    ),
                ]
            );
        }
    }
}
