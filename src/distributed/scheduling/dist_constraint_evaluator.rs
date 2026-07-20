use std::{
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
};

use contiguous_tree::TreeCursorExt;

use async_stream::stream;
use ecow::{EcoString, EcoVec};
use futures::{FutureExt, StreamExt, future::pending, pin_mut};

use crate::{
    DsrvSpecification, OutputStream, Specification, Value, VarName,
    distributed::distribution_graphs::{LabelledDistributionGraph, NodeName},
    lang::dsrv::ast::{BoolBinOp, CompBinOp, ExprRef, ExprView, NumericalBinOp, SBinOp, StrBinOp},
};

#[cfg(test)]
const COMPACT_CONSTRAINT_EVALUATION_INTERVAL: std::time::Duration =
    std::time::Duration::from_millis(20);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementLabelling {
    labels_by_node: BTreeMap<NodeName, EcoVec<VarName>>,
}

impl PlacementLabelling {
    pub fn from_labelled_graph(graph: &LabelledDistributionGraph) -> Self {
        let labels_by_node = graph
            .node_labels
            .iter()
            .map(|(node_idx, vars)| {
                (
                    graph.dist_graph.graph[*node_idx].clone(),
                    vars.iter().cloned().collect::<EcoVec<_>>(),
                )
            })
            .collect();
        Self { labels_by_node }
    }

    fn monitors_at(&self, var: &VarName, node: &NodeName) -> bool {
        self.labels_by_node
            .get(node)
            .is_some_and(|vars| vars.iter().any(|candidate| candidate == var))
    }
}

pub type PlacementLabellingStream = OutputStream<Rc<PlacementLabelling>>;
pub type ConstraintInputEvent = (usize, Value);
pub type ConstraintInputBatch = Vec<ConstraintInputEvent>;

#[derive(Debug, Clone)]
pub struct ConstraintInputIndex {
    vars: EcoVec<VarName>,
    indices: BTreeMap<VarName, usize>,
}

impl ConstraintInputIndex {
    pub fn new(vars: impl IntoIterator<Item = VarName>) -> Self {
        let vars = vars.into_iter().collect::<EcoVec<_>>();
        let indices = vars
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, var)| (var, index))
            .collect();
        Self { vars, indices }
    }

    pub fn index_of(&self, var: &VarName) -> Option<usize> {
        self.indices.get(var).copied()
    }

    pub fn len(&self) -> usize {
        self.vars.len()
    }

    fn initial_values(&self) -> Vec<Value> {
        vec![Value::NoVal; self.vars.len()]
    }
}

pub fn dist_constraint_stream(
    spec: DsrvSpecification,
    constraints: Vec<VarName>,
    labelling_stream: PlacementLabellingStream,
    input_streams: BTreeMap<VarName, OutputStream<Value>>,
) -> OutputStream<bool> {
    let input_index = ConstraintInputIndex::new(input_streams.keys().cloned());
    let mut input_events =
        futures::stream::select_all(input_streams.into_iter().map(|(var, stream)| {
            let index = input_index
                .index_of(&var)
                .expect("input stream variable missing from compact input index");
            Box::pin(stream.map(move |value| (index, value))) as OutputStream<ConstraintInputEvent>
        }));

    dist_constraint_event_stream(
        spec,
        constraints,
        labelling_stream,
        input_index,
        Box::pin(stream! {
            while let Some(event) = input_events.next().await {
                yield vec![event];
            }
        }),
    )
}

pub fn dist_constraint_event_stream(
    spec: DsrvSpecification,
    constraints: Vec<VarName>,
    mut labelling_stream: PlacementLabellingStream,
    input_index: ConstraintInputIndex,
    mut input_events: OutputStream<ConstraintInputBatch>,
) -> OutputStream<bool> {
    assert_supported_constraints(&spec, &constraints);
    let mut latest_inputs = input_index.initial_values();

    Box::pin(stream! {
        let mut latest_labelling = None;
        let mut labelling_done = false;
        let mut inputs_done = false;
        loop {
            if labelling_done && inputs_done {
                return;
            }

            let next_labelling = async {
                if labelling_done {
                    pending().await
                } else {
                    labelling_stream.next().await
                }
            }.fuse();
            let next_input = async {
                if inputs_done {
                    pending().await
                } else {
                    input_events.next().await
                }
            }.fuse();
            pin_mut!(next_labelling, next_input);

            futures::select_biased! {
                labelling = next_labelling => {
                    match labelling {
                        Some(labelling) => latest_labelling = Some(labelling),
                        None => {
                            labelling_done = true;
                            continue;
                        }
                    };
                }
                input = next_input => {
                    match input {
                        Some(batch) => {
                            for (index, value) in batch {
                                record_latest_input(&mut latest_inputs, index, value);
                            }
                        }
                        None => {
                            inputs_done = true;
                            continue;
                        }
                    };
                }
            }

            let Some(labelling) = latest_labelling.as_ref() else {
                continue;
            };

            let mut evaluator = TickEvaluator {
                spec: &spec,
                labelling,
                input_index: &input_index,
                env: &latest_inputs,
                cache: BTreeMap::new(),
                stack: Vec::new(),
            };

            let holds = constraints.iter().all(|constraint| {
                matches!(evaluator.eval_var(constraint), Value::Bool(true))
            });
            yield holds;
        }
    })
}

fn record_latest_input(latest_inputs: &mut [Value], index: usize, value: Value) {
    if !matches!(value, Value::NoVal | Value::Deferred) {
        let Some(slot) = latest_inputs.get_mut(index) else {
            panic!("Input event index {index} is outside the compact input table");
        };
        *slot = value;
    }
}

pub fn dist_constraint_input_vars(
    spec: &DsrvSpecification,
    constraints: &[VarName],
) -> BTreeSet<VarName> {
    let input_vars = spec.input_vars();
    let mut deps = BTreeSet::new();
    let mut seen = BTreeSet::new();
    for constraint in constraints {
        collect_var_deps(spec, &input_vars, constraint, &mut seen, &mut deps);
    }
    deps
}

fn collect_var_deps(
    spec: &DsrvSpecification,
    input_vars: &BTreeSet<VarName>,
    var: &VarName,
    seen: &mut BTreeSet<VarName>,
    deps: &mut BTreeSet<VarName>,
) {
    if input_vars.contains(var) {
        deps.insert(var.clone());
        return;
    }
    if !seen.insert(var.clone()) {
        return;
    }
    let expr = spec
        .var_expr(var)
        .unwrap_or_else(|| panic!("Unknown variable `{var}` in distribution constraint"));
    collect_expr_deps(spec, input_vars, expr.as_ref(), seen, deps);
}

fn collect_expr_deps(
    spec: &DsrvSpecification,
    input_vars: &BTreeSet<VarName>,
    expr: ExprRef<'_>,
    seen: &mut BTreeSet<VarName>,
    deps: &mut BTreeSet<VarName>,
) {
    for node in expr.postorder() {
        match node.view() {
            ExprView::If(..)
            | ExprView::Val(_)
            | ExprView::MonitoredAt(_, _)
            | ExprView::BinOp(..)
            | ExprView::Not(_)
            | ExprView::Abs(_)
            | ExprView::MGet(_, _)
            | ExprView::SGet(_, _) => {}
            ExprView::Var(var) => collect_var_deps(spec, input_vars, var, seen, deps),
            ExprView::Dist(_, _) => {
                panic!("dist(...) is unsupported in compact distribution constraints")
            }
            _ => panic!(
                "Unsupported expression in compact distribution constraint evaluator: {:?}",
                node.kind()
            ),
        }
    }
}

fn assert_supported_constraints(spec: &DsrvSpecification, constraints: &[VarName]) {
    for constraint in constraints {
        let expr = spec
            .var_expr(constraint)
            .unwrap_or_else(|| panic!("Distribution constraint `{constraint}` has no expression"));
        assert_supported_expr(expr.as_ref());
    }
}

fn assert_supported_expr(expr: ExprRef<'_>) {
    for node in expr.postorder() {
        match node.view() {
            ExprView::If(..)
            | ExprView::Val(_)
            | ExprView::Var(_)
            | ExprView::MonitoredAt(_, _)
            | ExprView::BinOp(..)
            | ExprView::Not(_)
            | ExprView::Abs(_)
            | ExprView::MGet(_, _)
            | ExprView::SGet(_, _) => {}
            ExprView::Dist(_, _) => {
                panic!("dist(...) is unsupported in compact distribution constraints")
            }
            _ => panic!(
                "Unsupported expression in compact distribution constraint evaluator: {:?}",
                node.kind()
            ),
        }
    }
}

struct TickEvaluator<'a> {
    spec: &'a DsrvSpecification,
    labelling: &'a PlacementLabelling,
    input_index: &'a ConstraintInputIndex,
    env: &'a [Value],
    cache: BTreeMap<VarName, Value>,
    stack: Vec<VarName>,
}

impl TickEvaluator<'_> {
    fn eval_var(&mut self, var: &VarName) -> Value {
        if let Some(index) = self.input_index.index_of(var) {
            return self.env[index].clone();
        }
        if let Some(value) = self.cache.get(var) {
            return value.clone();
        }
        if self.stack.iter().any(|candidate| candidate == var) {
            panic!("Cycle while evaluating distribution constraint variable `{var}`");
        }

        let expr = self
            .spec
            .var_expr(var)
            .unwrap_or_else(|| panic!("Unknown variable `{var}` in distribution constraint"));
        self.stack.push(var.clone());
        let value = self.eval_expr(expr.as_ref());
        self.stack.pop();
        self.cache.insert(var.clone(), value.clone());
        value
    }

    fn eval_expr(&mut self, expr: ExprRef<'_>) -> Value {
        match expr.view() {
            ExprView::If(cond, then_expr, else_expr) => match self.eval_expr(cond) {
                Value::Bool(true) => self.eval_expr(then_expr),
                Value::Bool(false) => self.eval_expr(else_expr),
                _ => Value::NoVal,
            },
            ExprView::Val(value) => value.clone(),
            ExprView::Var(var) => self.eval_var(var),
            ExprView::BinOp(left, right, op) => {
                let left = self.eval_expr(left);
                let right = self.eval_expr(right);
                eval_binop(left, right, op)
            }
            ExprView::Not(child) => match self.eval_expr(child) {
                Value::Bool(value) => Value::Bool(!value),
                _ => Value::NoVal,
            },
            ExprView::Abs(child) => match self.eval_expr(child) {
                Value::Int(value) => Value::Int(value.abs()),
                Value::Float(value) => Value::Float(value.abs()),
                _ => Value::NoVal,
            },
            ExprView::MGet(child, key) | ExprView::SGet(child, key) => {
                match self.eval_expr(child) {
                    Value::Map(map) => map.get(key).cloned().unwrap_or(Value::NoVal),
                    _ => Value::NoVal,
                }
            }
            ExprView::MonitoredAt(var, node) => Value::Bool(self.labelling.monitors_at(var, node)),
            ExprView::Dist(_, _) => panic!("dist(...) is unsupported in compact evaluator"),
            _ => panic!("unsupported expression in compact distribution constraint evaluator"),
        }
    }
}

fn eval_binop(left: Value, right: Value, op: &SBinOp) -> Value {
    match op {
        SBinOp::BOp(BoolBinOp::And) => match (left, right) {
            (Value::Bool(left), Value::Bool(right)) => Value::Bool(left && right),
            _ => Value::NoVal,
        },
        SBinOp::BOp(BoolBinOp::Or) => match (left, right) {
            (Value::Bool(left), Value::Bool(right)) => Value::Bool(left || right),
            _ => Value::NoVal,
        },
        SBinOp::BOp(BoolBinOp::Impl) => match (left, right) {
            (Value::Bool(left), Value::Bool(right)) => Value::Bool(!left || right),
            _ => Value::NoVal,
        },
        SBinOp::COp(op) => eval_comparison(left, right, op),
        SBinOp::NOp(op) => eval_numeric(left, right, op),
        SBinOp::SOp(StrBinOp::Concat) => match (left, right) {
            (Value::Str(left), Value::Str(right)) => {
                let mut out = EcoString::from(left.as_str());
                out.push_str(right.as_str());
                Value::Str(out)
            }
            _ => Value::NoVal,
        },
    }
}

fn eval_comparison(left: Value, right: Value, op: &CompBinOp) -> Value {
    match op {
        CompBinOp::Eq => Value::Bool(left == right),
        CompBinOp::Le => compare_numbers(left, right, |left, right| left <= right),
        CompBinOp::Lt => compare_numbers(left, right, |left, right| left < right),
        CompBinOp::Ge => compare_numbers(left, right, |left, right| left >= right),
        CompBinOp::Gt => compare_numbers(left, right, |left, right| left > right),
    }
}

fn eval_numeric(left: Value, right: Value, op: &NumericalBinOp) -> Value {
    match (left, right) {
        (Value::Int(left), Value::Int(right)) => match op {
            NumericalBinOp::Add => Value::Int(left + right),
            NumericalBinOp::Sub => Value::Int(left - right),
            NumericalBinOp::Mul => Value::Int(left * right),
            NumericalBinOp::Div => Value::Int(left / right),
            NumericalBinOp::Mod => Value::Int(left % right),
        },
        (left, right) => {
            let Some(left) = number_as_f64(left) else {
                return Value::NoVal;
            };
            let Some(right) = number_as_f64(right) else {
                return Value::NoVal;
            };
            match op {
                NumericalBinOp::Add => Value::Float(left + right),
                NumericalBinOp::Sub => Value::Float(left - right),
                NumericalBinOp::Mul => Value::Float(left * right),
                NumericalBinOp::Div => Value::Float(left / right),
                NumericalBinOp::Mod => Value::Float(left % right),
            }
        }
    }
}

fn compare_numbers(left: Value, right: Value, f: impl FnOnce(f64, f64) -> bool) -> Value {
    let Some(left) = number_as_f64(left) else {
        return Value::NoVal;
    };
    let Some(right) = number_as_f64(right) else {
        return Value::NoVal;
    };
    Value::Bool(f(left, right))
}

fn number_as_f64(value: Value) -> Option<f64> {
    match value {
        Value::Int(value) => Some(value as f64),
        Value::Float(value) => Some(value),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, rc::Rc};

    use futures::{StreamExt, stream};
    use petgraph::graph::DiGraph;

    use macro_rules_attribute::apply;

    use crate::{
        async_test, distributed::distribution_graphs::DistributionGraph, dsrv_specification,
    };

    use super::*;

    fn graph_with_x_at_b() -> LabelledDistributionGraph {
        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        graph.add_edge(a, b, 1);

        LabelledDistributionGraph {
            dist_graph: Rc::new(DistributionGraph {
                central_monitor: a,
                graph,
            }),
            var_names: vec!["x".into()],
            node_labels: BTreeMap::from([(a, vec![]), (b, vec!["x".into()])]),
        }
    }

    #[apply(async_test)]
    async fn dist_constraint_evaluator_evaluate_guarded_monitored_at(
        _executor: Rc<smol::LocalExecutor<'static>>,
    ) {
        let mut src = r#"
in gate
out distX
distX = if gate then monitored_at(x, "B") else true
"#;
        let spec = dsrv_specification(&mut src).unwrap();
        let labelling = Rc::new(PlacementLabelling::from_labelled_graph(&graph_with_x_at_b()));
        let labelling_stream = Box::pin(stream::iter(vec![labelling]));
        let input_streams = BTreeMap::from([(
            "gate".into(),
            Box::pin(stream! {
                yield true.into();
                smol::Timer::after(COMPACT_CONSTRAINT_EVALUATION_INTERVAL * 2).await;
                yield false.into();
            }) as OutputStream<Value>,
        )]);

        let result: Vec<_> =
            dist_constraint_stream(spec, vec!["distX".into()], labelling_stream, input_streams)
                .take(2)
                .collect()
                .await;

        assert_eq!(result, vec![false, true]);
    }

    #[apply(async_test)]
    async fn dist_constraint_evaluator_re_evaluate_on_input_change_after_labelling(
        _executor: Rc<smol::LocalExecutor<'static>>,
    ) {
        let mut src = r#"
in gate
out distX
distX = if gate then monitored_at(x, "A") else true
"#;
        let spec = dsrv_specification(&mut src).unwrap();
        let labelling = Rc::new(PlacementLabelling::from_labelled_graph(&graph_with_x_at_b()));
        let labelling_stream = Box::pin(stream::iter(vec![labelling]));
        let input_streams = BTreeMap::from([(
            "gate".into(),
            Box::pin(stream! {
                yield false.into();
                smol::Timer::after(COMPACT_CONSTRAINT_EVALUATION_INTERVAL * 2).await;
                yield true.into();
                smol::Timer::after(COMPACT_CONSTRAINT_EVALUATION_INTERVAL * 2).await;
                yield false.into();
            }) as OutputStream<Value>,
        )]);

        let result: Vec<_> =
            dist_constraint_stream(spec, vec!["distX".into()], labelling_stream, input_streams)
                .take(4)
                .collect()
                .await;

        assert_eq!(result, vec![false, true, false, true]);
    }

    #[apply(async_test)]
    async fn dist_constraint_evaluator_keep_latest_real_input_on_noval(
        _executor: Rc<smol::LocalExecutor<'static>>,
    ) {
        let mut src = r#"
in gate
out distX
distX = gate
"#;
        let spec = dsrv_specification(&mut src).unwrap();
        let labelling = Rc::new(PlacementLabelling::from_labelled_graph(&graph_with_x_at_b()));
        let labelling_stream = Box::pin(stream::iter(vec![labelling]));
        let input_streams = BTreeMap::from([(
            "gate".into(),
            Box::pin(stream! {
                yield true.into();
                smol::Timer::after(COMPACT_CONSTRAINT_EVALUATION_INTERVAL * 2).await;
                yield Value::NoVal;
                smol::Timer::after(COMPACT_CONSTRAINT_EVALUATION_INTERVAL * 2).await;
                yield false.into();
            }) as OutputStream<Value>,
        )]);

        let result: Vec<_> =
            dist_constraint_stream(spec, vec!["distX".into()], labelling_stream, input_streams)
                .take(4)
                .collect()
                .await;

        assert_eq!(result, vec![false, true, true, false]);
    }

    #[apply(async_test)]
    async fn dist_constraint_evaluator_emits_repeated_false_for_fresh_input(
        _executor: Rc<smol::LocalExecutor<'static>>,
    ) {
        let mut src = r#"
in gate
out distX
distX = gate && monitored_at(x, "A")
"#;
        let spec = dsrv_specification(&mut src).unwrap();
        let labelling = Rc::new(PlacementLabelling::from_labelled_graph(&graph_with_x_at_b()));
        let labelling_stream = Box::pin(stream::iter(vec![labelling]));
        let input_index = ConstraintInputIndex::new(vec!["gate".into()]);
        let gate_index = input_index.index_of(&"gate".into()).unwrap();
        let input_events = Box::pin(stream::iter(vec![
            vec![(gate_index, Value::Bool(true))],
            vec![(gate_index, Value::Bool(true))],
        ]));

        let result: Vec<_> = dist_constraint_event_stream(
            spec,
            vec!["distX".into()],
            labelling_stream,
            input_index,
            input_events,
        )
        .take(3)
        .collect()
        .await;

        assert_eq!(result, vec![false, false, false]);
    }

    #[apply(async_test)]
    async fn dist_constraint_evaluator_support_input_independent_monitored_at(
        _executor: Rc<smol::LocalExecutor<'static>>,
    ) {
        let mut src = r#"
out distX
distX = monitored_at(x, "B")
"#;
        let spec = dsrv_specification(&mut src).unwrap();
        let labelling = Rc::new(PlacementLabelling::from_labelled_graph(&graph_with_x_at_b()));
        let labelling_stream = Box::pin(stream::iter(vec![labelling]));
        let input_index = ConstraintInputIndex::new(Vec::<VarName>::new());
        let input_events = Box::pin(stream::pending()) as OutputStream<ConstraintInputBatch>;

        let result: Vec<_> = dist_constraint_event_stream(
            spec,
            vec!["distX".into()],
            labelling_stream,
            input_index,
            input_events,
        )
        .take(1)
        .collect()
        .await;

        assert_eq!(result, vec![true]);
    }

    #[apply(async_test)]
    async fn dist_constraint_evaluator_evaluate_each_input_batch(
        _executor: Rc<smol::LocalExecutor<'static>>,
    ) {
        let mut src = r#"
in gate
out distX
distX = if gate then monitored_at(x, "A") else true
"#;
        let spec = dsrv_specification(&mut src).unwrap();
        let labelling = Rc::new(PlacementLabelling::from_labelled_graph(&graph_with_x_at_b()));
        let labelling_stream = Box::pin(stream::iter(vec![labelling]));
        let input_index = ConstraintInputIndex::new(vec!["gate".into()]);
        let gate_index = input_index.index_of(&"gate".into()).unwrap();
        let input_events = Box::pin(stream::iter(vec![
            vec![(gate_index, Value::Bool(false))],
            vec![(gate_index, Value::Bool(true))],
            vec![(gate_index, Value::Bool(false))],
        ]));

        let result: Vec<_> = dist_constraint_event_stream(
            spec,
            vec!["distX".into()],
            labelling_stream,
            input_index,
            input_events,
        )
        .take(4)
        .collect()
        .await;

        assert_eq!(result, vec![false, true, false, true]);
    }

    #[test]
    #[should_panic(expected = "dist(...) is unsupported")]
    fn dist_constraint_evaluator_panic_on_dist() {
        let mut src = r#"
out distX
distX = dist(A, B) == 1
"#;
        let spec = dsrv_specification(&mut src).unwrap();
        let labelling = Rc::new(PlacementLabelling::from_labelled_graph(&graph_with_x_at_b()));
        let labelling_stream = Box::pin(stream::repeat(labelling));
        let _ = dist_constraint_stream(
            spec,
            vec!["distX".into()],
            labelling_stream,
            BTreeMap::new(),
        );
    }
}
