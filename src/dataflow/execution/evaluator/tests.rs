use super::*;
use crate::VarName;
use crate::core::BinaryOperator;
use crate::dataflow::ContextTransferPolicy;
use crate::dataflow::execution::evaluator_state::*;
use crate::dataflow::execution::reconfigurable_expressions::{
    ReconfigurableExpressionActivation, SharedReconfigurableExpressionCache,
};
use crate::dataflow::ir::*;

fn program(graph: BoundEvaluationGraph) -> Rc<StreamProgram> {
    Rc::new(StreamProgram::new(
        graph,
        Rc::new(EnvironmentLayout::default()),
    ))
}

fn add_graph(left: i64, right: i64) -> BoundEvaluationGraph {
    BoundEvaluationGraph::new(
        vec![BoundOp::Binary {
            op: BinaryOperator::Add,
            lhs: BoundRef::Const(Value::Int(left)),
            rhs: BoundRef::Const(Value::Int(right)),
        }],
        vec![None],
        BoundRef::Node(NodeId::new(0)),
    )
}

#[test]
fn cloned_evaluator_has_independent_state() {
    let program = program(add_graph(1, 2));
    let source = Evaluator::new(Rc::clone(&program));
    reset_state_clone_count();
    let mut candidate = source.clone();

    assert_eq!(state_clone_count(), 1);
    assert!(Rc::ptr_eq(&source.program, &candidate.program));
    assert_eq!(source.canonical.node_values[0], Value::NoVal);
    assert_eq!(candidate.canonical.node_values[0], Value::NoVal);

    assert_eq!(
        candidate.evaluate_static_and_stage(&[], None),
        Value::Int(3)
    );

    assert_eq!(source.canonical.node_values[0], Value::NoVal);
    assert_eq!(candidate.canonical.node_values[0], Value::Int(3));
    assert_eq!(state_clone_count(), 1);
}

#[test]
fn cloned_evaluator_has_independent_lazy_branch_state() {
    let graph = BoundEvaluationGraph::new(
        vec![BoundOp::If {
            cond: BoundRef::Const(Value::Bool(true)),
            then_branch: add_graph(1, 2),
            else_branch: add_graph(3, 4),
        }],
        vec![None],
        BoundRef::Node(NodeId::new(0)),
    );
    let program = program(graph);
    let source = Evaluator::new(Rc::clone(&program));
    let mut candidate = source.clone();

    let branch_value = |evaluator: &Evaluator| match &evaluator.canonical.node_states[0] {
        NodeState::LazyIf(lazy) => lazy.then_state.node_values[0].clone(),
        _ => panic!("expected lazy branch state"),
    };
    assert_eq!(branch_value(&source), Value::NoVal);
    assert_eq!(branch_value(&candidate), Value::NoVal);

    assert_eq!(
        candidate.evaluate_static_and_stage(&[], None),
        Value::Int(3)
    );

    assert_eq!(branch_value(&source), Value::NoVal);
    assert_eq!(branch_value(&candidate), Value::Int(3));
}

#[test]
fn unique_repeated_evaluation_does_not_clone_canonical_state() {
    let mut evaluator = Evaluator::new(program(add_graph(4, 5)));
    reset_state_clone_count();

    for _ in 0..16 {
        assert_eq!(
            evaluator.evaluate_static_and_stage(&[], None),
            Value::Int(9)
        );
    }

    assert_eq!(state_clone_count(), 0);
}

#[test]
fn exact_nested_state_transfer_discards_target_scratch_values() {
    let program = program(add_graph(6, 7));
    let mut source = Evaluator::new(Rc::clone(&program));
    let mut target = Evaluator::new(Rc::clone(&program));
    assert_eq!(source.evaluate_static_and_stage(&[], None), Value::Int(13));
    target.state_mut().node_values[0] = Value::Bool(true);

    target.rewrite_exact_nested_from(&mut source);
    assert_eq!(source.canonical.node_values[0], Value::Bool(true));
    assert_eq!(target.canonical.node_values[0], Value::NoVal);
}

fn dynamic_program(prefix_nodes: usize) -> Rc<StreamProgram> {
    dynamic_program_with_variables(prefix_nodes, &["x"])
}

fn dynamic_program_with_variables(
    prefix_nodes: usize,
    variable_names: &[&str],
) -> Rc<StreamProgram> {
    let variables = variable_names
        .iter()
        .map(|name| VarName::new(*name))
        .collect::<Vec<_>>();
    let environment = Rc::new(EnvironmentLayout::from_variables(variables.iter().cloned()));
    let spec = BoundReconfigurableExpressionSpec {
        input: BoundRef::Const(Value::Str("x[1]".into())),
        scope: ReconfigurableExpressionScope::Restricted {
            allowed_variables: variables.into_iter().collect(),
        },
        kind: ReconfigurableExpressionKind::Dynamic,
        typing: None,
    };
    let mut nodes = (0..prefix_nodes)
        .map(|index| BoundOp::Binary {
            op: BinaryOperator::Add,
            lhs: BoundRef::Const(Value::Int(index as i64)),
            rhs: BoundRef::Const(Value::Int(1)),
        })
        .collect::<Vec<_>>();
    let dynamic_node = NodeId::new(nodes.len());
    nodes.push(BoundOp::Reconfigurable(spec));
    let graph = BoundEvaluationGraph::new(
        nodes,
        vec![None; prefix_nodes + 1],
        BoundRef::Node(dynamic_node),
    );
    Rc::new(StreamProgram::new(graph, environment))
}

fn reconfigure(
    evaluator: &mut Evaluator,
    source: &str,
    transfer: ContextTransferPolicy,
) -> (ReconfigurableExpressionActivation, bool) {
    let node = NodeId::new(evaluator.program.graph.nodes.len() - 1);
    let mut shared_template_cache = SharedReconfigurableExpressionCache::default();
    evaluator
        .reconfigure_expression(
            node,
            Value::Str(source.into()),
            transfer,
            &mut shared_template_cache,
        )
        .unwrap()
}

fn seed_active_dynamic_body(evaluator: &mut Evaluator) {
    seed_active_dynamic_body_with_environment(evaluator, &[Value::Int(1)], None);
}

fn seed_active_dynamic_body_with_environment(
    evaluator: &mut Evaluator,
    environment_values: &[Value],
    retained_environment_values: Option<&[Value]>,
) {
    let node = NodeId::new(evaluator.program.graph.nodes.len() - 1);
    let environment_values = {
        let state = evaluator.state_mut();
        let NodeState::Reconfigurable(expression) = &mut state.node_states[node.index()] else {
            panic!("test program must end in a dynamic node");
        };
        expression.update_environment(environment_values, retained_environment_values);
        expression.environment_values.clone()
    };
    let state = evaluator.state_mut();
    let NodeState::Reconfigurable(expression) = &mut state.node_states[node.index()] else {
        panic!("test program must end in a dynamic node");
    };
    expression
        .active_expression
        .as_mut()
        .expect("test dynamic body must be active")
        .evaluator
        .evaluate_and_commit(&environment_values, None)
        .unwrap();
}

fn evaluate_active_dynamic_body(evaluator: &mut Evaluator, input: i64) -> Value {
    evaluate_active_dynamic_body_with_environment(evaluator, &[Value::Int(input)], None)
}

fn evaluate_active_dynamic_body_with_environment(
    evaluator: &mut Evaluator,
    environment_values: &[Value],
    retained_environment_values: Option<&[Value]>,
) -> Value {
    let node = NodeId::new(evaluator.program.graph.nodes.len() - 1);
    let state = evaluator.state_mut();
    let NodeState::Reconfigurable(expression) = &mut state.node_states[node.index()] else {
        panic!("test program must end in a dynamic node");
    };
    expression.update_environment(environment_values, retained_environment_values);
    let environment_values = expression.environment_values.clone();
    expression
        .active_expression
        .as_mut()
        .expect("test dynamic body must be active")
        .evaluator
        .evaluate_and_commit(&environment_values, None)
        .unwrap()
}

#[test]
fn exact_nested_expression_reconfiguration_keeps_warm_state() {
    let mut evaluator = Evaluator::new(dynamic_program(1));
    reconfigure(
        &mut evaluator,
        "x[1]",
        ContextTransferPolicy::MatchingStreamState,
    );
    seed_active_dynamic_body(&mut evaluator);

    let (_, state_preserved) = reconfigure(
        &mut evaluator,
        "x[1]",
        ContextTransferPolicy::MatchingStreamState,
    );
    assert!(state_preserved);
    assert_eq!(
        evaluate_active_dynamic_body(&mut evaluator, 2),
        Value::Int(1)
    );
}

#[test]
fn changed_nested_expression_reconfiguration_starts_cold() {
    let mut evaluator = Evaluator::new(dynamic_program(1));
    reconfigure(
        &mut evaluator,
        "x[1]",
        ContextTransferPolicy::MatchingStreamState,
    );
    seed_active_dynamic_body(&mut evaluator);

    let (_, state_preserved) = reconfigure(
        &mut evaluator,
        "x[1] + 2",
        ContextTransferPolicy::MatchingStreamState,
    );
    assert!(!state_preserved);
    assert_eq!(
        evaluate_active_dynamic_body(&mut evaluator, 2),
        Value::Deferred
    );
}

#[test]
fn changed_nested_expression_with_new_free_variable_starts_cold() {
    let mut evaluator = Evaluator::new(dynamic_program_with_variables(1, &["x", "y"]));
    reconfigure(
        &mut evaluator,
        "x[1]",
        ContextTransferPolicy::MatchingStreamState,
    );
    seed_active_dynamic_body_with_environment(
        &mut evaluator,
        &[Value::Int(1), Value::Int(10)],
        None,
    );

    let (_, state_preserved) = reconfigure(
        &mut evaluator,
        "x[1] + y",
        ContextTransferPolicy::MatchingStreamState,
    );
    assert!(!state_preserved);
    assert_eq!(
        evaluate_active_dynamic_body_with_environment(
            &mut evaluator,
            &[Value::Int(2), Value::Int(3)],
            None,
        ),
        Value::Deferred
    );
}

#[test]
fn no_transfer_reuses_environment_storage_but_refreshes_target_slots() {
    let mut evaluator = Evaluator::new(dynamic_program_with_variables(1, &["x", "y"]));
    reconfigure(&mut evaluator, "x[1] + y", ContextTransferPolicy::None);
    seed_active_dynamic_body_with_environment(
        &mut evaluator,
        &[Value::Int(1), Value::Int(99)],
        None,
    );
    let storage = evaluator
        .canonical
        .reconfigurable_expression_state(NodeId::new(1))
        .environment_values
        .as_ptr();

    let (_, state_preserved) = reconfigure(&mut evaluator, "y", ContextTransferPolicy::None);
    assert!(!state_preserved);
    let expression = evaluator
        .canonical
        .reconfigurable_expression_state(NodeId::new(1));
    assert_eq!(expression.environment_values.as_ptr(), storage);
    assert_eq!(expression.environment_values[1], Value::Int(99));

    assert_eq!(
        evaluate_active_dynamic_body_with_environment(
            &mut evaluator,
            &[Value::NoVal, Value::NoVal],
            Some(&[Value::NoVal, Value::Int(7)]),
        ),
        Value::Int(7)
    );
}

#[test]
fn nested_expression_reconfiguration_reports_only_actual_state_preservation() {
    let mut evaluator = Evaluator::new(dynamic_program(64));

    let (activation, state_preserved) = reconfigure(
        &mut evaluator,
        "x[1]",
        ContextTransferPolicy::MatchingStreamState,
    );
    assert!(matches!(
        activation,
        ReconfigurableExpressionActivation::Activated { .. }
    ));
    assert!(!state_preserved, "fresh activation has no donor state");

    seed_active_dynamic_body(&mut evaluator);
    let (_, state_preserved) = reconfigure(&mut evaluator, "x[1] + 1", ContextTransferPolicy::None);
    assert!(!state_preserved, "None must not claim state preservation");

    seed_active_dynamic_body(&mut evaluator);
    let (_, state_preserved) = reconfigure(
        &mut evaluator,
        "x[1] + 2",
        ContextTransferPolicy::MatchingStreamState,
    );
    assert!(!state_preserved, "a changed body must start cold");
}

#[test]
fn changed_nested_expression_reconfiguration_does_not_clone_evaluator_state() {
    let mut evaluator = Evaluator::new(dynamic_program(128));
    reset_state_clone_count();

    reconfigure(
        &mut evaluator,
        "x[1]",
        ContextTransferPolicy::MatchingStreamState,
    );
    assert_eq!(state_clone_count(), 0);

    seed_active_dynamic_body(&mut evaluator);
    evaluator.state_mut().node_values[0] = Value::Int(99);
    reset_state_clone_count();

    let (_, state_preserved) = reconfigure(
        &mut evaluator,
        "x[1] + 1",
        ContextTransferPolicy::MatchingStreamState,
    );
    assert!(!state_preserved);
    assert_eq!(state_clone_count(), 0);
    assert_eq!(evaluator.canonical.node_values[0], Value::Int(99));
}

#[test]
fn changed_nested_expression_publishes_a_cold_body() {
    let mut evaluator = Evaluator::new(dynamic_program(1));
    reconfigure(
        &mut evaluator,
        "x[1]",
        ContextTransferPolicy::MatchingStreamState,
    );
    seed_active_dynamic_body(&mut evaluator);

    let (activation, state_preserved) = reconfigure(
        &mut evaluator,
        "x + 1",
        ContextTransferPolicy::MatchingStreamState,
    );
    assert!(matches!(
        activation,
        ReconfigurableExpressionActivation::Replaced { .. }
    ));
    assert!(!state_preserved);
    assert_eq!(
        evaluate_active_dynamic_body(&mut evaluator, 2),
        Value::Int(3)
    );
}

#[test]
fn invalid_nested_expression_reconfiguration_is_atomic_for_the_live_state() {
    let mut evaluator = Evaluator::new(dynamic_program(1));
    reconfigure(
        &mut evaluator,
        "x[1]",
        ContextTransferPolicy::MatchingStreamState,
    );
    let node_values_before = evaluator.canonical.node_values.clone();
    let template_before = Rc::clone(
        &evaluator
            .canonical
            .reconfigurable_expression_state(NodeId::new(1))
            .active_expression
            .as_ref()
            .expect("test dynamic body must be active")
            .template,
    );

    let mut shared_template_cache = SharedReconfigurableExpressionCache::default();
    assert!(
        evaluator
            .reconfigure_expression(
                NodeId::new(1),
                Value::Str("(".into()),
                ContextTransferPolicy::MatchingStreamState,
                &mut shared_template_cache,
            )
            .is_err()
    );
    assert_eq!(evaluator.canonical.node_values, node_values_before);
    assert!(Rc::ptr_eq(
        &evaluator
            .canonical
            .reconfigurable_expression_state(NodeId::new(1))
            .active_expression
            .as_ref()
            .unwrap()
            .template,
        &template_before
    ));
}
