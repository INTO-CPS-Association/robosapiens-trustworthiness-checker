use std::{
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
};

use async_stream::stream;
use ecow::EcoVec;
use futures::{FutureExt, StreamExt, future::pending, pin_mut};

use crate::{
    DsrvSpecification, OutputStream, Value, VarName,
    distributed::{
        distribution_constraint::{ConstraintProfile, DistributionConstraintPlan},
        distribution_graphs::{LabelledDistributionGraph, NodeName},
    },
};

pub use crate::distributed::distribution_constraint::ConstraintLoweringError;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DistConstraintEvaluatorError {
    Lowering(ConstraintLoweringError),
    MissingInputs { variables: BTreeSet<VarName> },
    DuplicateInputs { variables: BTreeSet<VarName> },
}

impl std::fmt::Display for DistConstraintEvaluatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lowering(error) => error.fmt(f),
            Self::MissingInputs { variables } => write!(
                f,
                "Compact distribution constraint input index is missing required variables: {variables:?}"
            ),
            Self::DuplicateInputs { variables } => write!(
                f,
                "Compact distribution constraint input index contains duplicate variables: {variables:?}"
            ),
        }
    }
}

impl std::error::Error for DistConstraintEvaluatorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Lowering(error) => Some(error),
            Self::MissingInputs { .. } | Self::DuplicateInputs { .. } => None,
        }
    }
}

impl From<ConstraintLoweringError> for DistConstraintEvaluatorError {
    fn from(error: ConstraintLoweringError) -> Self {
        Self::Lowering(error)
    }
}

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

    fn bindings(&self, values: &[Value]) -> BTreeMap<VarName, Value> {
        self.vars
            .iter()
            .cloned()
            .zip(values.iter().cloned())
            .collect()
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
    labelling_stream: PlacementLabellingStream,
    input_index: ConstraintInputIndex,
    input_events: OutputStream<ConstraintInputBatch>,
) -> OutputStream<bool> {
    try_dist_constraint_event_stream(
        spec,
        constraints,
        labelling_stream,
        input_index,
        input_events,
    )
    .unwrap_or_else(|error| panic!("{error}"))
}

pub fn try_dist_constraint_event_stream(
    spec: DsrvSpecification,
    constraints: Vec<VarName>,
    mut labelling_stream: PlacementLabellingStream,
    input_index: ConstraintInputIndex,
    mut input_events: OutputStream<ConstraintInputBatch>,
) -> Result<OutputStream<bool>, DistConstraintEvaluatorError> {
    let plan =
        DistributionConstraintPlan::lower(&spec, constraints, ConstraintProfile::CompactEvaluator)?;

    let mut seen_inputs = BTreeSet::new();
    let duplicate_inputs = input_index
        .vars
        .iter()
        .filter(|variable| !seen_inputs.insert((*variable).clone()))
        .cloned()
        .collect::<BTreeSet<_>>();
    if !duplicate_inputs.is_empty() {
        return Err(DistConstraintEvaluatorError::DuplicateInputs {
            variables: duplicate_inputs,
        });
    }

    let missing_inputs = plan
        .input_dependencies()
        .difference(&seen_inputs)
        .cloned()
        .collect::<BTreeSet<_>>();
    if !missing_inputs.is_empty() {
        return Err(DistConstraintEvaluatorError::MissingInputs {
            variables: missing_inputs,
        });
    }

    let mut latest_inputs = input_index.initial_values();

    Ok(Box::pin(stream! {
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

            let bindings = input_index.bindings(&latest_inputs);
            let monitors_at = |variable: &VarName, node: &NodeName| {
                labelling.monitors_at(variable, node)
            };
            let holds = plan.roots().iter().all(|constraint| {
                matches!(
                    plan.evaluate_var(constraint, &bindings, Some(&monitors_at)),
                    Some(Value::Bool(true))
                )
            });
            yield holds;
        }
    }))
}

fn record_latest_input(latest_inputs: &mut [Value], index: usize, value: Value) {
    if !matches!(value, Value::NoVal | Value::Deferred) {
        let Some(slot) = latest_inputs.get_mut(index) else {
            panic!("Input event index {index} is outside the compact input table");
        };
        *slot = value;
    }
}

pub fn try_dist_constraint_input_vars(
    spec: &DsrvSpecification,
    constraints: &[VarName],
) -> Result<BTreeSet<VarName>, ConstraintLoweringError> {
    DistributionConstraintPlan::lower(
        spec,
        constraints.iter().cloned(),
        ConstraintProfile::CompactEvaluator,
    )
    .map(|plan| plan.input_dependencies().clone())
}

pub fn dist_constraint_input_vars(
    spec: &DsrvSpecification,
    constraints: &[VarName],
) -> BTreeSet<VarName> {
    try_dist_constraint_input_vars(spec, constraints).unwrap_or_else(|error| panic!("{error}"))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, rc::Rc};

    use futures::{StreamExt, stream};
    use petgraph::graph::DiGraph;

    use macro_rules_attribute::apply;

    use crate::{async_test, distributed::distribution_graphs::DistributionGraph};

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
        let src = r#"
in gate
out distX
distX = if gate then monitored_at(x, "B") else true
"#;
        let spec = (src)
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");
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
        let src = r#"
in gate
out distX
distX = if gate then monitored_at(x, "A") else true
"#;
        let spec = (src)
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");
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
        let src = r#"
in gate
out distX
distX = gate
"#;
        let spec = (src)
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");
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
        let src = r#"
in gate
out distX
distX = gate && monitored_at(x, "A")
"#;
        let spec = (src)
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");
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
        let src = r#"
out distX
distX = monitored_at(x, "B")
"#;
        let spec = (src)
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");
        let labelling = Rc::new(PlacementLabelling::from_labelled_graph(&graph_with_x_at_b()));
        let labelling_stream = Box::pin(stream::iter(vec![labelling]));
        let input_index = ConstraintInputIndex::new(Vec::<VarName>::new());
        let input_events = Box::pin(stream::pending()) as OutputStream<ConstraintInputBatch>;

        let stream = try_dist_constraint_event_stream(
            spec,
            vec!["distX".into()],
            labelling_stream,
            input_index,
            input_events,
        )
        .expect("input-independent constraint should accept an empty input index");
        let result: Vec<_> = stream.take(1).collect().await;

        assert_eq!(result, vec![true]);
    }

    #[test]
    fn dist_constraint_evaluator_rejects_missing_required_input() {
        let spec = ("in gate\nout distX\ndistX = gate")
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");
        let labelling_stream = Box::pin(stream::pending()) as PlacementLabellingStream;
        let input_events = Box::pin(stream::pending()) as OutputStream<ConstraintInputBatch>;

        let error = try_dist_constraint_event_stream(
            spec,
            vec!["distX".into()],
            labelling_stream,
            ConstraintInputIndex::new(Vec::<VarName>::new()),
            input_events,
        )
        .err()
        .expect("missing required input should be rejected");

        assert_eq!(
            error,
            DistConstraintEvaluatorError::MissingInputs {
                variables: BTreeSet::from(["gate".into()]),
            }
        );
    }

    #[test]
    fn dist_constraint_evaluator_rejects_duplicate_input_variables() {
        let spec = ("in gate\nout distX\ndistX = gate")
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");
        let labelling_stream = Box::pin(stream::pending()) as PlacementLabellingStream;
        let input_events = Box::pin(stream::pending()) as OutputStream<ConstraintInputBatch>;

        let error = try_dist_constraint_event_stream(
            spec,
            vec!["distX".into()],
            labelling_stream,
            ConstraintInputIndex::new(["gate".into(), "gate".into()]),
            input_events,
        )
        .err()
        .expect("duplicate input variables should be rejected");

        assert_eq!(
            error,
            DistConstraintEvaluatorError::DuplicateInputs {
                variables: BTreeSet::from(["gate".into()]),
            }
        );
    }

    #[apply(async_test)]
    async fn dist_constraint_evaluator_evaluate_each_input_batch(
        _executor: Rc<smol::LocalExecutor<'static>>,
    ) {
        let src = r#"
in gate
out distX
distX = if gate then monitored_at(x, "A") else true
"#;
        let spec = (src)
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");
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
        let src = r#"
out distX
distX = dist(A, B) == 1
"#;
        let spec = (src)
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");
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
