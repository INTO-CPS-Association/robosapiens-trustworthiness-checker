// Test untimed monitoring of LOLA specifications with the async runtime

use futures::stream;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use std::collections::BTreeMap;
use std::pin::Pin;
use trustworthiness_checker::runtime::constraints::ConstraintBasedMonitor;
use trustworthiness_checker::{
    LOLASpecification, io::testing::ManualOutputHandler, lola_specification,
};
use trustworthiness_checker::{Monitor, Value, VarName};

pub fn input_streams1() -> BTreeMap<VarName, BoxStream<'static, Value>> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        VarName("x".into()),
        Box::pin(stream::iter(
            vec![Value::Int(1), 3.into(), 5.into()].into_iter(),
        )) as Pin<Box<dyn futures::Stream<Item = Value> + std::marker::Send>>,
    );
    input_streams.insert(
        VarName("y".into()),
        Box::pin(stream::iter(
            vec![Value::Int(2), 4.into(), 6.into()].into_iter(),
        )) as Pin<Box<dyn futures::Stream<Item = Value> + std::marker::Send>>,
    );
    input_streams
}

pub fn new_input_stream(
    map: BTreeMap<VarName, Vec<Value>>,
) -> BTreeMap<VarName, BoxStream<'static, Value>> {
    let mut input_streams = BTreeMap::new();
    for (name, values) in map {
        input_streams.insert(
            name,
            Box::pin(stream::iter(values.into_iter()))
                as Pin<Box<dyn futures::Stream<Item = Value> + std::marker::Send>>,
        );
    }
    input_streams
}

fn output_handler(spec: LOLASpecification) -> Box<ManualOutputHandler<Value>> {
    Box::new(ManualOutputHandler::new(spec.output_vars.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use strum::IntoEnumIterator;
    use test_log::test;
    use trustworthiness_checker::dep_manage::interface::{
        DependencyKind, create_dependency_manager,
    };
    use trustworthiness_checker::lola_fixtures::{
        input_empty, input_streams4, input_streams5, spec_empty, spec_simple_add_monitor,
    };

    #[test(tokio::test)]
    async fn test_simple_add_monitor() {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams1();
            let spec = lola_specification(&mut spec_simple_add_monitor()).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert_eq!(
                outputs,
                vec![
                    (0, BTreeMap::from([(VarName("z".into()), 3.into())]),),
                    (1, BTreeMap::from([(VarName("z".into()), 7.into())]),),
                    (2, BTreeMap::from([(VarName("z".into()), 11.into())]),),
                ]
            );
        }
    }

    #[test(tokio::test)]
    #[ignore = "Cannot have empty spec or inputs"]
    async fn test_runtime_initialization() {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_empty();
            let spec = lola_specification(&mut spec_empty()).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = Box::new(output_handler.get_output());
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<BTreeMap<VarName, Value>> = outputs.collect().await;
            assert_eq!(outputs.len(), 0);
        }
    }

    #[test(tokio::test)]
    async fn test_var() {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams1();
            let mut spec = "in x\nout z\nz =x";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, BTreeMap::from([(VarName("z".into()), 1.into())]),),
                    (1, BTreeMap::from([(VarName("z".into()), 3.into())]),),
                    (2, BTreeMap::from([(VarName("z".into()), 5.into())]),),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_literal_expression() {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams1();
            let mut spec = "out z\nz =42";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.take(3).enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, BTreeMap::from([(VarName("z".into()), 42.into())]),),
                    (1, BTreeMap::from([(VarName("z".into()), 42.into())]),),
                    (2, BTreeMap::from([(VarName("z".into()), 42.into())]),),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_addition() {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams1();
            let mut spec = "in x\nout z\nz =x+1";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, BTreeMap::from([(VarName("z".into()), 2.into())]),),
                    (1, BTreeMap::from([(VarName("z".into()), 4.into())]),),
                    (2, BTreeMap::from([(VarName("z".into()), 6.into())]),),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_subtraction() {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams1();
            let mut spec = "in x\nout z\nz =x-10";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (
                        0,
                        BTreeMap::from([(VarName("z".into()), Value::Int(-9))]).into(),
                    ),
                    (
                        1,
                        BTreeMap::from([(VarName("z".into()), Value::Int(-7))]).into(),
                    ),
                    (
                        2,
                        BTreeMap::from([(VarName("z".into()), Value::Int(-5))]).into(),
                    ),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_index_past() {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams1();
            let mut spec = "in x\nout z\nz =x[-1, 0]";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (
                        // Resolved to default on first step
                        0,
                        BTreeMap::from([(VarName("z".into()), 0.into())]),
                    ),
                    (
                        // Resolving to previous value on second step
                        1,
                        BTreeMap::from([(VarName("z".into()), 1.into())]),
                    ),
                    (
                        // Resolving to previous value on second step
                        2,
                        BTreeMap::from([(VarName("z".into()), 3.into())]),
                    ),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_index_past_mult_dependencies() {
        for kind in DependencyKind::iter() {
            // Specifically tests past indexing that the cleaner does not delete dependencies too early
            let mut input_streams = input_streams1();
            let mut spec = "in x\nout z1\nout z2\nz2 = x[-2, 0]\nz1 = x[-1, 0]";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (
                        // Both resolve to default
                        0,
                        BTreeMap::from([
                            (VarName("z1".into()), 0.into()),
                            (VarName("z2".into()), 0.into())
                        ])
                        .into(),
                    ),
                    (
                        // z1 resolves to prev, z2 resolves to default
                        1,
                        BTreeMap::from([
                            (VarName("z1".into()), 1.into()),
                            (VarName("z2".into()), 0.into())
                        ])
                        .into(),
                    ),
                    (
                        // z1 resolves to prev, z2 resolves to prev_prev
                        2,
                        BTreeMap::from([
                            (VarName("z1".into()), 3.into()),
                            (VarName("z2".into()), 1.into())
                        ])
                        .into(),
                    ),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_index_future() {
        for kind in [
            DependencyKind::Empty, // DependencyKind::DepGraph // Not supported correctly for
                                   // future indexing
        ] {
            let mut input_streams = input_streams1();
            let mut spec = "in x\nout z\nz =x[1, 0]";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = Box::new(ManualOutputHandler::new(spec.output_vars.clone()));
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert_eq!(outputs.len(), 2);
            assert_eq!(
                outputs,
                vec![
                    (
                        // Resolved to index 1 on first step
                        0,
                        BTreeMap::from([(VarName("z".into()), 3.into())]),
                    ),
                    (1, BTreeMap::from([(VarName("z".into()), 5.into())]),),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_if_else_expression() {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams5();
            let mut spec = "in x\nin y\nout z\nz =if(x) then y else false"; // And-gate
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (
                        0,
                        BTreeMap::from([(VarName("z".into()), true.into())]).into(),
                    ),
                    (
                        1,
                        BTreeMap::from([(VarName("z".into()), false.into())]).into(),
                    ),
                    (
                        2,
                        BTreeMap::from([(VarName("z".into()), false.into())]).into(),
                    ),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_string_append() {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams4();
            let mut spec = "in x\nin y\nout z\nz =x++y";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 2);
            assert_eq!(
                outputs,
                vec![
                    (
                        0,
                        BTreeMap::from([(VarName("z".into()), "ab".into())]).into(),
                    ),
                    (
                        1,
                        BTreeMap::from([(VarName("z".into()), "cd".into())]).into(),
                    ),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_multiple_parameters() {
        for kind in DependencyKind::iter() {
            let mut input_streams = input_streams1();
            let mut spec = "in x\nin y\nout r1\nout r2\nr1 =x+y\nr2 = x * y";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (
                        0,
                        BTreeMap::from([
                            (VarName("r1".into()), 3.into()),
                            (VarName("r2".into()), 2.into()),
                        ])
                        .into(),
                    ),
                    (
                        1,
                        BTreeMap::from([
                            (VarName("r1".into()), 7.into()),
                            (VarName("r2".into()), 12.into()),
                        ])
                        .into(),
                    ),
                    (
                        2,
                        BTreeMap::from([
                            (VarName("r1".into()), 11.into()),
                            (VarName("r2".into()), 30.into()),
                        ])
                        .into(),
                    ),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_default_no_unknown() {
        for kind in DependencyKind::iter() {
            let v = vec![0.into(), 1.into(), 2.into()];
            let mut input_streams = new_input_stream(BTreeMap::from([("x".into(), v)]));
            let mut spec = "in x\nout y\ny=default(x, 42)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, BTreeMap::from([(VarName("y".into()), 0.into())]),),
                    (1, BTreeMap::from([(VarName("y".into()), 1.into())]),),
                    (2, BTreeMap::from([(VarName("y".into()), 2.into())]),),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_default_all_unknown() {
        for kind in DependencyKind::iter() {
            let v = vec![Value::Unknown, Value::Unknown, Value::Unknown];
            let mut input_streams = new_input_stream(BTreeMap::from([("x".into(), v)]));
            let mut spec = "in x\nout y\ny=default(x, 42)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, BTreeMap::from([(VarName("y".into()), 42.into())]),),
                    (1, BTreeMap::from([(VarName("y".into()), 42.into())]),),
                    (2, BTreeMap::from([(VarName("y".into()), 42.into())]),),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_default_one_unknown() {
        for kind in DependencyKind::iter() {
            let v = vec![0.into(), Value::Unknown, 2.into()];
            let mut input_streams = new_input_stream(BTreeMap::from([("x".into(), v)]));
            let mut spec = "in x\nout y\ny=default(x, 42)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, BTreeMap::from([(VarName("y".into()), 0.into())]),),
                    (1, BTreeMap::from([(VarName("y".into()), 42.into())]),),
                    (2, BTreeMap::from([(VarName("y".into()), 2.into())]),),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_counter() {
        for kind in DependencyKind::iter() {
            let mut input_streams = new_input_stream(BTreeMap::from([]));
            let mut spec = "out y\ny=y[-1, 0] + 1";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().take(3).collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, BTreeMap::from([(VarName("y".into()), 1.into())]),),
                    (1, BTreeMap::from([(VarName("y".into()), 2.into())]),),
                    (2, BTreeMap::from([(VarName("y".into()), 3.into())]),),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_defer() {
        // Notice that even though we first say "x + 1", "x + 2", it continues evaluating "x + 1"
        for kind in DependencyKind::iter() {
            let x = vec![0.into(), 1.into(), 2.into()];
            let e = vec!["x + 1".into(), "x + 2".into(), "x + 3".into()];
            let mut input_streams =
                new_input_stream(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
            let mut spec = "in x\nin e\nout z\nz = defer(e)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, BTreeMap::from([(VarName("z".into()), 1.into())]),),
                    (1, BTreeMap::from([(VarName("z".into()), 2.into())]),),
                    (2, BTreeMap::from([(VarName("z".into()), 3.into())]),),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_defer_x_squared() {
        // This test is interesting since we use x twice in the eval strings
        for kind in DependencyKind::iter() {
            let x = vec![1.into(), 2.into(), 3.into()];
            let e = vec!["x * x".into(), "x * x + 1".into(), "x * x + 2".into()];
            let mut input_streams =
                new_input_stream(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
            let mut spec = "in x\nin e\nout z\nz = defer(e)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (0, BTreeMap::from([(VarName("z".into()), 1.into())]),),
                    (1, BTreeMap::from([(VarName("z".into()), 4.into())]),),
                    (2, BTreeMap::from([(VarName("z".into()), 9.into())]),),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_defer_unknown() {
        // Using unknown to represent no data on the stream
        for kind in DependencyKind::iter() {
            let x = vec![0.into(), 1.into(), 2.into()];
            let e = vec![Value::Unknown, "x + 1".into(), "x + 2".into()];
            let mut input_streams =
                new_input_stream(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
            let mut spec = "in x\nin e\nout z\nz = defer(e)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (
                        0,
                        BTreeMap::from([(VarName("z".into()), Value::Unknown)]).into(),
                    ),
                    (1, BTreeMap::from([(VarName("z".into()), 2.into())]),),
                    (2, BTreeMap::from([(VarName("z".into()), 3.into())]),),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_defer_unknown2() {
        // Unknown followed by property followed by unknown returns [U; val; val].
        for kind in DependencyKind::iter() {
            let x = vec![0.into(), 1.into(), 2.into()];
            let e = vec![Value::Unknown, "x + 1".into(), Value::Unknown];
            let mut input_streams =
                new_input_stream(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
            let mut spec = "in x\nin e\nout z\nz = defer(e)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (
                        0,
                        BTreeMap::from([(VarName("z".into()), Value::Unknown)]).into(),
                    ),
                    (1, BTreeMap::from([(VarName("z".into()), 2.into())]),),
                    (2, BTreeMap::from([(VarName("z".into()), 3.into())]),),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_defer_dependency() {
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
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 4);
            let sample_1 = if kind == DependencyKind::DepGraph {
                // Because the new dependency is added at time idx 1 and requires knowledge
                // of cleaned memory, it is not solved at this time index.
                (1, BTreeMap::from([(VarName("z".into()), Value::Unknown)]))
            } else {
                // Might need more cases in the future
                (1, BTreeMap::from([(VarName("z".into()), 0.into())]))
            };
            assert_eq!(
                outputs,
                vec![
                    (
                        0,
                        BTreeMap::from([(VarName("z".into()), Value::Unknown)]).into(),
                    ),
                    sample_1,
                    (2, BTreeMap::from([(VarName("z".into()), 1.into())]),),
                    (3, BTreeMap::from([(VarName("z".into()), 2.into())]),),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_update_both_init() {
        for kind in DependencyKind::iter() {
            let x = vec!["x0".into(), "x1".into(), "x2".into()];
            let y = vec!["y0".into(), "y1".into(), "y2".into()];
            let mut input_streams =
                new_input_stream(BTreeMap::from([("x".into(), x), ("y".into(), y)]));
            let mut spec = "in x\nin y\nout z\nz = update(x, y)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 3);
            assert_eq!(
                outputs,
                vec![
                    (
                        0,
                        BTreeMap::from([(VarName("z".into()), "y0".into())]).into(),
                    ),
                    (
                        1,
                        BTreeMap::from([(VarName("z".into()), "y1".into())]).into(),
                    ),
                    (
                        2,
                        BTreeMap::from([(VarName("z".into()), "y2".into())]).into(),
                    ),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_update_first_x_then_y() {
        for kind in DependencyKind::iter() {
            let x = vec!["x0".into(), "x1".into(), "x2".into(), "x3".into()];
            let y = vec![Value::Unknown, "y1".into(), Value::Unknown, "y3".into()];
            let mut input_streams =
                new_input_stream(BTreeMap::from([("x".into(), x), ("y".into(), y)]));
            let mut spec = "in x\nin y\nout z\nz = update(x, y)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 4);
            assert_eq!(
                outputs,
                vec![
                    (
                        0,
                        BTreeMap::from([(VarName("z".into()), "x0".into())]).into(),
                    ),
                    (
                        1,
                        BTreeMap::from([(VarName("z".into()), "y1".into())]).into(),
                    ),
                    (
                        2,
                        BTreeMap::from([(VarName("z".into()), Value::Unknown)]).into(),
                    ),
                    (
                        3,
                        BTreeMap::from([(VarName("z".into()), "y3".into())]).into(),
                    ),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_update_defer() {
        for kind in DependencyKind::iter() {
            let x = vec!["x0".into(), "x1".into(), "x2".into(), "x3".into()];
            let e = vec![Value::Unknown, "x".into(), "x".into(), "x".into()];
            let mut input_streams =
                new_input_stream(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
            let mut spec = "in x\nin e\nout z\nz = update(\"def\", defer(e))";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 4);
            assert_eq!(
                outputs,
                vec![
                    (
                        0,
                        BTreeMap::from([(VarName("z".into()), "def".into())]).into(),
                    ),
                    (
                        1,
                        BTreeMap::from([(VarName("z".into()), "x1".into())]).into(),
                    ),
                    (
                        2,
                        BTreeMap::from([(VarName("z".into()), "x2".into())]).into(),
                    ),
                    (
                        3,
                        BTreeMap::from([(VarName("z".into()), "x3".into())]).into(),
                    ),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_defer_update() {
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
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 4);
            assert_eq!(
                outputs,
                vec![
                    (
                        0,
                        BTreeMap::from([(VarName("z".into()), Value::Unknown)]).into(),
                    ),
                    (
                        1,
                        BTreeMap::from([(VarName("z".into()), "y".into())]).into(),
                    ),
                    (
                        2,
                        BTreeMap::from([(VarName("z".into()), "y_won!".into())]).into(),
                    ),
                    (
                        3,
                        BTreeMap::from([(VarName("z".into()), "y_happy".into())]).into(),
                    ),
                ]
            );
        }
    }

    #[test(tokio::test)]
    async fn test_recursive_update() {
        // This is essentially the same as z = x, but it tests recursively using update
        for kind in DependencyKind::iter() {
            let x = vec!["x0".into(), "x1".into(), "x2".into(), "x3".into()];
            let mut input_streams = new_input_stream(BTreeMap::from([("x".into(), x)]));
            let mut spec = "in x\nout z\nz = update(x, z)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 4);
            assert_eq!(
                outputs,
                vec![
                    (
                        0,
                        BTreeMap::from([(VarName("z".into()), "x0".into())]).into(),
                    ),
                    (
                        1,
                        BTreeMap::from([(VarName("z".into()), "x1".into())]).into(),
                    ),
                    (
                        2,
                        BTreeMap::from([(VarName("z".into()), "x2".into())]).into(),
                    ),
                    (
                        3,
                        BTreeMap::from([(VarName("z".into()), "x3".into())]).into(),
                    ),
                ]
            );
        }
    }

    // NOTE: While this test is interesting, it cannot work due to how defer is handled.
    // When defer receives a prop stream it changes state from being a defer expression into
    // the received prop stream. Thus, it cannot be used recursively.
    // This is the reason why we also need eval for the constraint based runtime.
    #[test(tokio::test)]
    async fn test_recursive_update_defer() {
        for kind in DependencyKind::iter() {
            let x = vec!["0".into(), "1".into(), "2".into(), "3".into()];
            let mut input_streams = new_input_stream(BTreeMap::from([("x".into(), x)]));
            let mut spec = "in x\nout z\nz = update(defer(x), z)";
            let spec = lola_specification(&mut spec).unwrap();
            let mut output_handler = output_handler(spec.clone());
            let outputs = output_handler.get_output();
            let monitor = ConstraintBasedMonitor::new(
                spec.clone(),
                &mut input_streams,
                output_handler,
                create_dependency_manager(kind, spec),
            );
            tokio::spawn(monitor.run());
            let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
                outputs.enumerate().collect().await;
            assert!(outputs.len() == 4);
            // The outcommented are if update(defer(x), z) worked as we initially envisioned
            assert_eq!(
                outputs,
                vec![
                    (0, BTreeMap::from([(VarName("z".into()), 0.into())]),),
                    (
                        1,
                        BTreeMap::from([(VarName("z".into()), 0.into())]),
                        // BTreeMap::from([(VarName("z".into()), 1.into())]),
                    ),
                    (
                        2,
                        BTreeMap::from([(VarName("z".into()), 0.into())]),
                        // BTreeMap::from([(VarName("z".into()), 2.into())]),
                    ),
                    (
                        3,
                        BTreeMap::from([(VarName("z".into()), 0.into())]),
                        // BTreeMap::from([(VarName("z".into()), 3.into())]),
                    ),
                ]
            );
        }
    }
}
