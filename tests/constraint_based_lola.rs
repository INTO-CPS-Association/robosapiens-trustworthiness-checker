// Test untimed monitoring of LOLA specifications with the async runtime

use futures::stream::StreamExt;
use std::collections::BTreeMap;
use trustworthiness_checker::runtime::constraints::ConstraintBasedMonitor;
use trustworthiness_checker::{
    io::testing::ManualOutputHandler, lola_specification, LOLASpecification,
};
use trustworthiness_checker::{Monitor, Value, VarName};
mod lola_fixtures;
use futures::stream;
use futures::stream::BoxStream;
use lola_fixtures::*;
use std::pin::Pin;

pub fn input_streams1() -> BTreeMap<VarName, BoxStream<'static, Value>> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        VarName("x".into()),
        Box::pin(stream::iter(
            vec![Value::Int(1), Value::Int(3), Value::Int(5)].into_iter(),
        )) as Pin<Box<dyn futures::Stream<Item = Value> + std::marker::Send>>,
    );
    input_streams.insert(
        VarName("y".into()),
        Box::pin(stream::iter(
            vec![Value::Int(2), Value::Int(4), Value::Int(6)].into_iter(),
        )) as Pin<Box<dyn futures::Stream<Item = Value> + std::marker::Send>>,
    );
    input_streams
}

fn output_handler(spec: LOLASpecification) -> Box<ManualOutputHandler<Value>> {
    Box::new(ManualOutputHandler::new(spec.output_vars.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test(tokio::test)]
    async fn test_simple_add_monitor() {
        let mut input_streams = input_streams1();
        let spec = lola_specification(&mut spec_simple_add_monitor()).unwrap();
        let mut output_handler = output_handler(spec.clone());
        let outputs = output_handler.get_output();
        let monitor = ConstraintBasedMonitor::new(spec, &mut input_streams, output_handler);
        tokio::spawn(monitor.run());
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
        assert_eq!(
            outputs,
            vec![
                (
                    0,
                    vec![(VarName("z".into()), Value::Int(3))]
                        .into_iter()
                        .collect(),
                ),
                (
                    1,
                    vec![(VarName("z".into()), Value::Int(7))]
                        .into_iter()
                        .collect(),
                ),
                (
                    2,
                    vec![(VarName("z".into()), Value::Int(11))]
                        .into_iter()
                        .collect(),
                ),
            ]
        );
    }

    #[test(tokio::test)]
    #[ignore = "Cannot have empty spec or inputs"]
    async fn test_runtime_initialization() {
        let mut input_streams = input_empty();
        let spec = lola_specification(&mut spec_empty()).unwrap();
        let mut output_handler = output_handler(spec.clone());
        let outputs = Box::new(output_handler.get_output());
        let monitor = ConstraintBasedMonitor::new(spec, &mut input_streams, output_handler);
        tokio::spawn(monitor.run());
        let outputs: Vec<BTreeMap<VarName, Value>> = outputs.collect().await;
        assert_eq!(outputs.len(), 0);
    }

    #[test(tokio::test)]
    async fn test_var() {
        let mut input_streams = input_streams1();
        let mut spec = "in x\nout z\nz =x";
        let spec = lola_specification(&mut spec).unwrap();
        let mut output_handler = output_handler(spec.clone());
        let outputs = output_handler.get_output();
        let monitor = ConstraintBasedMonitor::new(spec, &mut input_streams, output_handler);
        tokio::spawn(monitor.run());
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
        assert!(outputs.len() == 3);
        assert_eq!(
            outputs,
            vec![
                (
                    0,
                    vec![(VarName("z".into()), Value::Int(1))]
                        .into_iter()
                        .collect(),
                ),
                (
                    1,
                    vec![(VarName("z".into()), Value::Int(3))]
                        .into_iter()
                        .collect(),
                ),
                (
                    2,
                    vec![(VarName("z".into()), Value::Int(5))]
                        .into_iter()
                        .collect(),
                ),
            ]
        );
    }

    #[test(tokio::test)]
    async fn test_literal_expression() {
        let mut input_streams = input_streams1();
        let mut spec = "out z\nz =42";
        let spec = lola_specification(&mut spec).unwrap();
        let mut output_handler = output_handler(spec.clone());
        let outputs = output_handler.get_output();
        let monitor = ConstraintBasedMonitor::new(spec, &mut input_streams, output_handler);
        tokio::spawn(monitor.run());
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            outputs.take(3).enumerate().collect().await;
        assert!(outputs.len() == 3);
        assert_eq!(
            outputs,
            vec![
                (
                    0,
                    vec![(VarName("z".into()), Value::Int(42))]
                        .into_iter()
                        .collect(),
                ),
                (
                    1,
                    vec![(VarName("z".into()), Value::Int(42))]
                        .into_iter()
                        .collect(),
                ),
                (
                    2,
                    vec![(VarName("z".into()), Value::Int(42))]
                        .into_iter()
                        .collect(),
                ),
            ]
        );
    }

    #[test(tokio::test)]
    async fn test_addition() {
        let mut input_streams = input_streams1();
        let mut spec = "in x\nout z\nz =x+1";
        let spec = lola_specification(&mut spec).unwrap();
        let mut output_handler = output_handler(spec.clone());
        let outputs = output_handler.get_output();
        let monitor = ConstraintBasedMonitor::new(spec, &mut input_streams, output_handler);
        tokio::spawn(monitor.run());
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
        assert!(outputs.len() == 3);
        assert_eq!(
            outputs,
            vec![
                (
                    0,
                    vec![(VarName("z".into()), Value::Int(2))]
                        .into_iter()
                        .collect(),
                ),
                (
                    1,
                    vec![(VarName("z".into()), Value::Int(4))]
                        .into_iter()
                        .collect(),
                ),
                (
                    2,
                    vec![(VarName("z".into()), Value::Int(6))]
                        .into_iter()
                        .collect(),
                ),
            ]
        );
    }

    #[test(tokio::test)]
    async fn test_subtraction() {
        let mut input_streams = input_streams1();
        let mut spec = "in x\nout z\nz =x-10";
        let spec = lola_specification(&mut spec).unwrap();
        let mut output_handler = output_handler(spec.clone());
        let outputs = output_handler.get_output();
        let monitor = ConstraintBasedMonitor::new(spec, &mut input_streams, output_handler);
        tokio::spawn(monitor.run());
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
        assert!(outputs.len() == 3);
        assert_eq!(
            outputs,
            vec![
                (
                    0,
                    vec![(VarName("z".into()), Value::Int(-9))]
                        .into_iter()
                        .collect(),
                ),
                (
                    1,
                    vec![(VarName("z".into()), Value::Int(-7))]
                        .into_iter()
                        .collect(),
                ),
                (
                    2,
                    vec![(VarName("z".into()), Value::Int(-5))]
                        .into_iter()
                        .collect(),
                ),
            ]
        );
    }

    #[test(tokio::test)]
    async fn test_index_past() {
        let mut input_streams = input_streams1();
        let mut spec = "in x\nout z\nz =x[-1, 0]";
        let spec = lola_specification(&mut spec).unwrap();
        let mut output_handler = output_handler(spec.clone());
        let outputs = output_handler.get_output();
        let monitor = ConstraintBasedMonitor::new(spec, &mut input_streams, output_handler);
        tokio::spawn(monitor.run());
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
        assert!(outputs.len() == 3);
        assert_eq!(
            outputs,
            vec![
                (
                    // Resolved to default on first step
                    0,
                    vec![(VarName("z".into()), Value::Int(0))]
                        .into_iter()
                        .collect(),
                ),
                (
                    // Resolving to previous value on second step
                    1,
                    vec![(VarName("z".into()), Value::Int(1))]
                        .into_iter()
                        .collect(),
                ),
                (
                    // Resolving to previous value on second step
                    2,
                    vec![(VarName("z".into()), Value::Int(3))]
                        .into_iter()
                        .collect(),
                ),
            ]
        );
    }

    #[test(tokio::test)]
    async fn test_index_past_mult_dependencies() {
        // Specifically tests past indexing that the cleaner does not delete dependencies too early
        let mut input_streams = input_streams1();
        let mut spec = "in x\nout z1\nout z2\nz2 = x[-2, 0]\nz1 = x[-1, 0]";
        let spec = lola_specification(&mut spec).unwrap();
        let mut output_handler = output_handler(spec.clone());
        let outputs = output_handler.get_output();
        let monitor = ConstraintBasedMonitor::new(spec, &mut input_streams, output_handler);
        tokio::spawn(monitor.run());
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
        assert!(outputs.len() == 3);
        assert_eq!(
            outputs,
            vec![
                (
                    // Both resolve to default
                    0,
                    vec![
                        (VarName("z1".into()), Value::Int(0)),
                        (VarName("z2".into()), Value::Int(0))
                    ]
                    .into_iter()
                    .collect(),
                ),
                (
                    // z1 resolves to prev, z2 resolves to default
                    1,
                    vec![
                        (VarName("z1".into()), Value::Int(1)),
                        (VarName("z2".into()), Value::Int(0))
                    ]
                    .into_iter()
                    .collect(),
                ),
                (
                    // z1 resolves to prev, z2 resolves to prev_prev
                    2,
                    vec![
                        (VarName("z1".into()), Value::Int(3)),
                        (VarName("z2".into()), Value::Int(1))
                    ]
                    .into_iter()
                    .collect(),
                ),
            ]
        );
    }

    #[test(tokio::test)]
    async fn test_index_future() {
        let mut input_streams = input_streams1();
        let mut spec = "in x\nout z\nz =x[1, 0]";
        let spec = lola_specification(&mut spec).unwrap();
        let mut output_handler = Box::new(ManualOutputHandler::new(spec.output_vars.clone()));
        let outputs = output_handler.get_output();
        let monitor = ConstraintBasedMonitor::new(spec, &mut input_streams, output_handler);
        tokio::spawn(monitor.run());
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
        assert_eq!(outputs.len(), 2);
        assert_eq!(
            outputs,
            vec![
                (
                    // Resolved to index 1 on first step
                    0,
                    vec![(VarName("z".into()), Value::Int(3))]
                        .into_iter()
                        .collect(),
                ),
                (
                    1,
                    vec![(VarName("z".into()), Value::Int(5))]
                        .into_iter()
                        .collect(),
                ),
            ]
        );
    }

    #[test(tokio::test)]
    async fn test_if_else_expression() {
        let mut input_streams = input_streams5();
        let mut spec = "in x\nin y\nout z\nz =if(x) then y else false"; // And-gate
        let spec = lola_specification(&mut spec).unwrap();
        let mut output_handler = output_handler(spec.clone());
        let outputs = output_handler.get_output();
        let monitor = ConstraintBasedMonitor::new(spec, &mut input_streams, output_handler);
        tokio::spawn(monitor.run());
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
        assert!(outputs.len() == 3);
        assert_eq!(
            outputs,
            vec![
                (
                    0,
                    vec![(VarName("z".into()), Value::Bool(true))]
                        .into_iter()
                        .collect(),
                ),
                (
                    1,
                    vec![(VarName("z".into()), Value::Bool(false))]
                        .into_iter()
                        .collect(),
                ),
                (
                    2,
                    vec![(VarName("z".into()), Value::Bool(false))]
                        .into_iter()
                        .collect(),
                ),
            ]
        );
    }

    #[test(tokio::test)]
    async fn test_string_append() {
        let mut input_streams = input_streams4();
        let mut spec = "in x\nin y\nout z\nz =x++y";
        let spec = lola_specification(&mut spec).unwrap();
        let mut output_handler = output_handler(spec.clone());
        let outputs = output_handler.get_output();
        let monitor = ConstraintBasedMonitor::new(spec, &mut input_streams, output_handler);
        tokio::spawn(monitor.run());
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
        assert!(outputs.len() == 2);
        assert_eq!(
            outputs,
            vec![
                (
                    0,
                    vec![(VarName("z".into()), Value::Str("ab".to_string()))]
                        .into_iter()
                        .collect(),
                ),
                (
                    1,
                    vec![(VarName("z".into()), Value::Str("cd".to_string()))]
                        .into_iter()
                        .collect(),
                ),
            ]
        );
    }

    #[test(tokio::test)]
    async fn test_multiple_parameters() {
        let mut input_streams = input_streams1();
        let mut spec = "in x\nin y\nout r1\nout r2\nr1 =x+y\nr2 = x * y";
        let spec = lola_specification(&mut spec).unwrap();
        let mut output_handler = output_handler(spec.clone());
        let outputs = output_handler.get_output();
        let monitor = ConstraintBasedMonitor::new(spec, &mut input_streams, output_handler);
        tokio::spawn(monitor.run());
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = outputs.enumerate().collect().await;
        assert!(outputs.len() == 3);
        assert_eq!(
            outputs,
            vec![
                (
                    0,
                    vec![
                        (VarName("r1".into()), Value::Int(3)),
                        (VarName("r2".into()), Value::Int(2)),
                    ]
                    .into_iter()
                    .collect(),
                ),
                (
                    1,
                    vec![
                        (VarName("r1".into()), Value::Int(7)),
                        (VarName("r2".into()), Value::Int(12)),
                    ]
                    .into_iter()
                    .collect(),
                ),
                (
                    2,
                    vec![
                        (VarName("r1".into()), Value::Int(11)),
                        (VarName("r2".into()), Value::Int(30)),
                    ]
                    .into_iter()
                    .collect(),
                ),
            ]
        );
    }
}
