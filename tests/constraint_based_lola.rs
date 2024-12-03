// Test untimed monitoring of LOLA specifications with the async runtime

use futures::stream::StreamExt;
use trustworthiness_checker::constraint_solver::ConstraintStore;
use std::collections::BTreeMap;
use trustworthiness_checker::constraint_based_runtime::ConstraintBasedMonitor;
use trustworthiness_checker::lola_specification;
use trustworthiness_checker::{Monitor, Value, VarName};
mod lola_fixtures;
use lola_fixtures::*;

#[tokio::test]
async fn test_simple_add_monitor() {
    let mut input_streams = input_streams1();
    let spec = lola_specification(&mut spec_simple_add_monitor()).unwrap();
    let mut monitor = ConstraintBasedMonitor::new(spec, &mut input_streams);
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
        monitor.monitor_outputs().enumerate().collect().await;
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
        ]
    );
}

fn env_len_assertions(env: &ConstraintStore, i_len: usize, r_len: usize, u_len: usize) {
    fn nested_map_len<T1, T2>(map: &BTreeMap<T1, Vec<T2>>) -> usize {
        let mut s = 0;
        for (_, inner) in map {
            s += inner.len();
        }
        s
    }
    assert_eq!(nested_map_len(&env.input_streams), i_len);
    assert_eq!(nested_map_len(&env.outputs_resolved), r_len);
    assert_eq!(nested_map_len(&env.outputs_unresolved), u_len);
}

#[ignore = "Cannot have empty spec or inputs"]
async fn test_runtime_initialization() {
    let mut input_streams = input_empty();
    let spec = lola_specification(&mut spec_empty()).unwrap();
    let mut monitor = ConstraintBasedMonitor::new(spec, &mut input_streams);
    let outputs: Vec< BTreeMap<VarName, Value>> = monitor.monitor_outputs().collect().await;
    assert_eq!(outputs.len(), 0);
}

#[tokio::test]
async fn test_var() {
    let mut input_streams = input_streams1();
    let mut spec = "in x\nout z\nz =x";
    let spec = lola_specification(&mut spec).unwrap();
    let mut monitor = ConstraintBasedMonitor::new(spec, &mut input_streams);
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
    monitor.monitor_outputs().enumerate().collect().await;
    assert!(outputs.len() == 2);
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
        ]
    );
}

#[tokio::test]
async fn test_literal_expression() {
    // NOTE: This test makes less sense with async RV
    let mut input_streams = input_streams1();
    let mut spec = "in x\nout z\nz =42+x-x";
    let spec = lola_specification(&mut spec).unwrap();
    let mut monitor = ConstraintBasedMonitor::new(spec, &mut input_streams);
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
    monitor.monitor_outputs().enumerate().collect().await;
    assert!(outputs.len() == 2);
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
        ]
    );
}

#[tokio::test]
async fn test_addition() {
    let mut input_streams = input_streams1();
    let mut spec = "in x\nout z\nz =x+1";
    let spec = lola_specification(&mut spec).unwrap();
    let mut monitor = ConstraintBasedMonitor::new(spec, &mut input_streams);
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
    monitor.monitor_outputs().enumerate().collect().await;
    assert!(outputs.len() == 2);
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
        ]
    );
}

#[tokio::test]
async fn test_subtraction() {
    let mut input_streams = input_streams1();
    let mut spec = "in x\nout z\nz =x-10";
    let spec = lola_specification(&mut spec).unwrap();
    let mut monitor = ConstraintBasedMonitor::new(spec, &mut input_streams);
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
    monitor.monitor_outputs().enumerate().collect().await;
    assert!(outputs.len() == 2);
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
        ]
    );
}

#[tokio::test]
async fn test_index_past() {
    let mut input_streams = input_streams1();
    let mut spec = "in x\nout z\nz =x[-1, 0]";
    let spec = lola_specification(&mut spec).unwrap();
    let mut monitor = ConstraintBasedMonitor::new(spec, &mut input_streams);
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
    monitor.monitor_outputs().enumerate().collect().await;
    assert!(outputs.len() == 2);
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
        ]
    );
}


