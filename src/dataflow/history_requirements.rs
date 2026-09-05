use std::rc::Rc;

use crate::VarName;

use super::environment::{EnvironmentLayout, EnvironmentSlot};
use super::ir::{BoundEvaluationGraph, BoundOp, BoundRef, StreamFunction, StreamProgram};

#[cfg(test)]
use super::ir::NodeId;

/// Dense, named-variable history bounds for one environment layout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct HistoryRequirements {
    depths: Box<[usize]>,
}

/// One positive history requirement exposed to runtime consumers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct VariableHistoryRequirement {
    pub(super) slot: EnvironmentSlot,
    pub(super) depth: usize,
}

impl VariableHistoryRequirement {
    #[inline]
    pub(super) fn slot(self) -> EnvironmentSlot {
        self.slot
    }

    #[inline]
    pub(super) fn depth(self) -> usize {
        self.depth
    }
}

impl HistoryRequirements {
    fn empty(_width: usize) -> Self {
        Self {
            depths: Box::new([]),
        }
    }

    /// Analyse all statically compiled top-level stream programs.
    ///
    /// The variable slices and program slice are parallel. A named stream is charged for a
    /// historical read of that stream; its equation is analysed separately for reads of its own
    /// environment variables.
    pub(super) fn analyze(
        input_variables: &[VarName],
        stream_variables: &[VarName],
        stream_programs: &[Rc<StreamProgram>],
        environment_layout: &EnvironmentLayout,
    ) -> Self {
        debug_assert_eq!(stream_variables.len(), stream_programs.len());
        debug_assert!(
            input_variables
                .iter()
                .chain(stream_variables)
                .all(|variable| environment_layout.slot(variable).is_some())
        );
        debug_assert!(
            stream_programs
                .iter()
                .all(|program| program.environment_layout.as_ref() == environment_layout)
        );

        let width = environment_layout.len();
        if stream_programs
            .iter()
            .all(|program| !graph_may_require_history(&program.graph))
        {
            return Self::empty(width);
        }
        let base_environment = (0..width)
            .map(|index| Bounds::basis(EnvironmentSlot::new(index), width))
            .collect::<Vec<_>>();
        let mut depths = vec![0; width];

        for (index, program) in stream_programs.iter().enumerate() {
            let recursive_output = stream_variables
                .get(index)
                .and_then(|variable| environment_layout.slot(variable))
                .map(|slot| Bounds::basis(slot, width));
            let result = analyse_graph(
                &program.graph,
                &base_environment,
                width,
                recursive_output.as_ref(),
            );
            merge_positive_depths(&mut depths, &result.output.depths);
        }

        Self {
            depths: depths.into_boxed_slice(),
        }
    }

    /// Analyse one already-bound graph against its enclosing environment.
    pub(super) fn analyze_graph(
        graph: &BoundEvaluationGraph,
        environment_layout: &EnvironmentLayout,
    ) -> Self {
        let width = environment_layout.len();
        if !graph_may_require_history(graph) {
            return Self::empty(width);
        }
        let base_environment = (0..width)
            .map(|index| Bounds::basis(EnvironmentSlot::new(index), width))
            .collect::<Vec<_>>();
        let result = analyse_graph(graph, &base_environment, width, None);
        let mut depths = vec![0; width];
        merge_positive_depths(&mut depths, &result.output.depths);
        Self {
            depths: depths.into_boxed_slice(),
        }
    }

    /// Return the depth for a slot, or zero for a slot outside this layout.
    #[inline]
    pub(super) fn depth(&self, slot: EnvironmentSlot) -> usize {
        self.depths.get(slot.index()).copied().unwrap_or_default()
    }

    /// Iterate over positive-depth entries only.
    #[inline]
    pub(super) fn iter(&self) -> impl Iterator<Item = VariableHistoryRequirement> + '_ {
        self.depths
            .iter()
            .enumerate()
            .filter_map(|(index, &depth)| {
                (depth > 0).then_some(VariableHistoryRequirement {
                    slot: EnvironmentSlot::new(index),
                    depth,
                })
            })
    }
}

fn graph_may_require_history(graph: &BoundEvaluationGraph) -> bool {
    graph.has_temporal_state()
        || graph.nodes.iter().any(|operation| match operation {
            BoundOp::Apply { .. }
            | BoundOp::Partial { .. }
            | BoundOp::Fix { .. }
            | BoundOp::ListMap { .. }
            | BoundOp::ListFilter { .. }
            | BoundOp::ListFold { .. } => true,
            BoundOp::If {
                then_branch,
                else_branch,
                ..
            } => graph_may_require_history(then_branch) || graph_may_require_history(else_branch),
            BoundOp::DirectApply { func, .. } | BoundOp::RecursiveApply { func, .. } => {
                graph_may_require_history(&func.program.graph)
            }
            _ => false,
        })
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Bounds {
    depths: Vec<Option<usize>>,
}

impl Bounds {
    fn zero(width: usize) -> Self {
        Self {
            depths: vec![None; width],
        }
    }

    fn basis(slot: EnvironmentSlot, width: usize) -> Self {
        let mut bounds = Self::zero(width);
        if let Some(depth) = bounds.depths.get_mut(slot.index()) {
            *depth = Some(0);
        }
        bounds
    }

    fn shifted(mut self, offset: u64) -> Self {
        let offset = usize::try_from(offset).unwrap_or(usize::MAX);
        for depth in &mut self.depths {
            if let Some(depth) = depth {
                *depth = depth.saturating_add(offset);
            }
        }
        self
    }

    fn union_assign(&mut self, other: &Self) {
        merge_bounds(&mut self.depths, &other.depths);
    }
}

struct GraphAnalysis {
    output: Bounds,
}

fn analyse_graph(
    graph: &BoundEvaluationGraph,
    environment: &[Bounds],
    width: usize,
    recursive_output: Option<&Bounds>,
) -> GraphAnalysis {
    let mut nodes = Vec::with_capacity(graph.nodes.len());
    let mut functions = Vec::with_capacity(graph.nodes.len());

    for operation in &graph.nodes {
        let (bounds, function) = analyse_operation(
            operation,
            &nodes,
            &functions,
            environment,
            width,
            recursive_output,
        );
        nodes.push(bounds);
        functions.push(function);
    }

    let output = resolve_ref(&graph.output, &nodes, environment, width);
    GraphAnalysis { output }
}

fn analyse_operation(
    operation: &BoundOp,
    nodes: &[Bounds],
    functions: &[Option<StreamFunction>],
    environment: &[Bounds],
    width: usize,
    recursive_output: Option<&Bounds>,
) -> (Bounds, Option<StreamFunction>) {
    let mut bounds = Bounds::zero(width);
    operation.for_each_operand(|operand| {
        bounds.union_assign(&resolve_ref(operand, nodes, environment, width));
    });

    match operation {
        BoundOp::Delay { offset, .. } => (bounds.shifted(*offset), None),
        BoundOp::RecursiveDelay { offset } => (
            recursive_output
                .map(|output| output.clone().shifted(offset.get()))
                .unwrap_or_else(|| Bounds::zero(width)),
            None,
        ),
        BoundOp::If {
            then_branch,
            else_branch,
            ..
        } => {
            let then_result = analyse_graph(then_branch, environment, width, recursive_output);
            let else_result = analyse_graph(else_branch, environment, width, recursive_output);
            bounds.union_assign(&then_result.output);
            bounds.union_assign(&else_result.output);
            (bounds, None)
        }
        BoundOp::Function { func } => (bounds, Some(func.clone())),
        BoundOp::DirectApply { func, args } => (
            analyse_static_application(func, args, nodes, environment, width, recursive_output),
            None,
        ),
        BoundOp::RecursiveApply { func, args } => (
            analyse_static_application(func, args, nodes, environment, width, recursive_output),
            None,
        ),
        BoundOp::Apply { func, args } => {
            let mut result = bounds;
            if let Some(function) = static_function(func, functions) {
                result.union_assign(&analyse_function_body(
                    function,
                    args,
                    nodes,
                    environment,
                    width,
                    recursive_output,
                ));
            }
            (result, None)
        }
        BoundOp::Partial { .. }
        | BoundOp::Fix { .. }
        | BoundOp::ListMap { .. }
        | BoundOp::ListFilter { .. }
        | BoundOp::ListFold { .. } => (bounds, None),
        BoundOp::RecursiveCall { .. } => (bounds, None),
        BoundOp::Reconfigurable(_) => (bounds, None),
        BoundOp::Unary { .. }
        | BoundOp::Binary { .. }
        | BoundOp::Default { .. }
        | BoundOp::Init { .. }
        | BoundOp::IsDefined { .. }
        | BoundOp::When { .. }
        | BoundOp::Update { .. }
        | BoundOp::Latch { .. }
        | BoundOp::List(..)
        | BoundOp::Tuple(..)
        | BoundOp::Map(..)
        | BoundOp::LIndex { .. }
        | BoundOp::LAppend { .. }
        | BoundOp::LConcat { .. }
        | BoundOp::LHead { .. }
        | BoundOp::LTail { .. }
        | BoundOp::LLen { .. }
        | BoundOp::MGet { .. }
        | BoundOp::MRemove { .. }
        | BoundOp::MInsert { .. }
        | BoundOp::MHasKey { .. }
        | BoundOp::TGet { .. } => (bounds, None),
    }
}

fn analyse_static_application(
    function: &StreamFunction,
    args: &[BoundRef],
    nodes: &[Bounds],
    environment: &[Bounds],
    width: usize,
    recursive_output: Option<&Bounds>,
) -> Bounds {
    let mut result = Bounds::zero(width);
    if function.capture_slots.len() + function.parameters.len()
        != function.program.environment_layout.len()
        || args.len() != function.parameters.len()
    {
        return Bounds::zero(width);
    }

    let mut local_environment = Vec::with_capacity(function.program.environment_layout.len());
    for slot in &function.capture_slots {
        local_environment.push(
            environment
                .get(slot.index())
                .cloned()
                .unwrap_or_else(|| Bounds::zero(width)),
        );
    }
    for argument in args {
        local_environment.push(resolve_ref(argument, nodes, environment, width));
    }

    let body = analyse_graph(
        &function.program.graph,
        &local_environment,
        width,
        recursive_output,
    );
    result.union_assign(&body.output);
    result
}

fn analyse_function_body(
    function: &StreamFunction,
    args: &[BoundRef],
    nodes: &[Bounds],
    environment: &[Bounds],
    width: usize,
    recursive_output: Option<&Bounds>,
) -> Bounds {
    analyse_static_application(function, args, nodes, environment, width, recursive_output)
}

fn static_function<'a>(
    reference: &BoundRef,
    functions: &'a [Option<StreamFunction>],
) -> Option<&'a StreamFunction> {
    let BoundRef::Node(node) = reference else {
        return None;
    };
    functions.get(node.index()).and_then(Option::as_ref)
}

fn resolve_ref(
    reference: &BoundRef,
    nodes: &[Bounds],
    environment: &[Bounds],
    width: usize,
) -> Bounds {
    match reference {
        BoundRef::Const(_) => Bounds::zero(width),
        BoundRef::External(slot) => environment
            .get(slot.index())
            .cloned()
            .unwrap_or_else(|| Bounds::zero(width)),
        BoundRef::Node(node) => nodes
            .get(node.index())
            .cloned()
            .unwrap_or_else(|| Bounds::zero(width)),
    }
}

fn merge_bounds(target: &mut [Option<usize>], source: &[Option<usize>]) {
    for (target, source) in target.iter_mut().zip(source) {
        *target = match (*target, *source) {
            (Some(target), Some(source)) => Some(target.max(source)),
            (None, source) => source,
            (target, None) => target,
        };
    }
}

fn merge_positive_depths(target: &mut [usize], source: &[Option<usize>]) {
    for (target, source) in target.iter_mut().zip(source) {
        if let Some(source) = source {
            *target = (*target).max(*source);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;
    use std::num::NonZeroU64;

    fn names(names: &[&str]) -> Vec<VarName> {
        names.iter().map(|name| VarName::new(name)).collect()
    }

    fn program(
        layout: &Rc<EnvironmentLayout>,
        nodes: Vec<BoundOp>,
        output: BoundRef,
    ) -> Rc<StreamProgram> {
        let signatures = vec![None; nodes.len()];
        Rc::new(StreamProgram::new(
            BoundEvaluationGraph::new(nodes, signatures, output),
            Rc::clone(layout),
        ))
    }

    fn external(layout: &EnvironmentLayout, name: &str) -> BoundRef {
        BoundRef::External(
            layout
                .slot(&VarName::new(name))
                .expect("test variable must be in the layout"),
        )
    }

    fn requirement_set(requirements: &HistoryRequirements) -> Vec<(usize, usize)> {
        requirements
            .iter()
            .map(|requirement| (requirement.slot.index(), requirement.depth))
            .collect()
    }

    #[test]
    fn source_variable_gets_the_delay_not_the_target() {
        let layout = Rc::new(EnvironmentLayout::from_variables(names(&["x", "a"])));
        let graph = program(
            &layout,
            vec![BoundOp::Delay {
                input: external(&layout, "x"),
                offset: 3,
            }],
            BoundRef::Node(NodeId::new(0)),
        );
        let requirements =
            HistoryRequirements::analyze(&names(&["x"]), &names(&["a"]), &[graph], &layout);

        assert_eq!(
            requirements.depth(layout.slot(&VarName::new("x")).unwrap()),
            3
        );
        assert_eq!(
            requirements.depth(layout.slot(&VarName::new("a")).unwrap()),
            0
        );
        assert_eq!(requirement_set(&requirements), vec![(0, 3)]);
    }

    #[test]
    fn repeated_reads_keep_the_maximum_depth() {
        let layout = Rc::new(EnvironmentLayout::from_variables(names(&[
            "x", "a", "b", "c",
        ])));
        let programs = [1, 2, 3]
            .into_iter()
            .enumerate()
            .map(|(_, offset)| {
                program(
                    &layout,
                    vec![BoundOp::Delay {
                        input: external(&layout, "x"),
                        offset,
                    }],
                    BoundRef::Node(NodeId::new(0)),
                )
            })
            .collect::<Vec<_>>();
        let requirements = HistoryRequirements::analyze(
            &names(&["x"]),
            &names(&["a", "b", "c"]),
            &programs,
            &layout,
        );

        assert_eq!(requirement_set(&requirements), vec![(0, 3)]);
    }

    #[test]
    fn nested_delays_accumulate_through_graph_nodes() {
        let layout = Rc::new(EnvironmentLayout::from_variables(names(&["x", "a"])));
        let graph = program(
            &layout,
            vec![
                BoundOp::Delay {
                    input: external(&layout, "x"),
                    offset: 1,
                },
                BoundOp::Delay {
                    input: BoundRef::Node(NodeId::new(0)),
                    offset: 2,
                },
            ],
            BoundRef::Node(NodeId::new(1)),
        );
        let requirements =
            HistoryRequirements::analyze(&names(&["x"]), &names(&["a"]), &[graph], &layout);

        assert_eq!(requirement_set(&requirements), vec![(0, 3)]);
    }

    #[test]
    fn both_branches_contribute_to_the_bound() {
        let layout = Rc::new(EnvironmentLayout::from_variables(names(&["x", "a"])));
        let branch = |offset| {
            BoundEvaluationGraph::new(
                vec![BoundOp::Delay {
                    input: external(&layout, "x"),
                    offset,
                }],
                vec![None],
                BoundRef::Node(NodeId::new(0)),
            )
        };
        let graph = program(
            &layout,
            vec![BoundOp::If {
                cond: BoundRef::Const(Value::Bool(true)),
                then_branch: branch(2),
                else_branch: branch(4),
            }],
            BoundRef::Node(NodeId::new(0)),
        );
        let requirements =
            HistoryRequirements::analyze(&names(&["x"]), &names(&["a"]), &[graph], &layout);

        assert_eq!(requirement_set(&requirements), vec![(0, 4)]);
    }

    #[test]
    fn direct_function_application_maps_parameter_history_to_argument() {
        let outer = Rc::new(EnvironmentLayout::from_variables(names(&["x", "a"])));
        let local = Rc::new(EnvironmentLayout::from_variables(names(&["p"])));
        let function_program = Rc::new(StreamProgram::new(
            BoundEvaluationGraph::new(
                vec![BoundOp::Delay {
                    input: BoundRef::External(local.slot(&VarName::new("p")).unwrap()),
                    offset: 2,
                }],
                vec![None],
                BoundRef::Node(NodeId::new(0)),
            ),
            Rc::clone(&local),
        ));
        let function = StreamFunction {
            parameters: names(&["p"]).into_iter().collect(),
            program: function_program,
            display: "f".into(),
            capture_slots: Vec::new(),
        };
        let graph = program(
            &outer,
            vec![BoundOp::DirectApply {
                func: function,
                args: vec![external(&outer, "x")],
            }],
            BoundRef::Node(NodeId::new(0)),
        );
        let requirements =
            HistoryRequirements::analyze(&names(&["x"]), &names(&["a"]), &[graph], &outer);

        assert_eq!(requirement_set(&requirements), vec![(0, 2)]);
    }

    #[test]
    fn recursive_delay_is_charged_to_the_containing_stream() {
        let layout = Rc::new(EnvironmentLayout::from_variables(names(&["a"])));
        let graph = program(
            &layout,
            vec![BoundOp::RecursiveDelay {
                offset: NonZeroU64::new(2).unwrap(),
            }],
            BoundRef::Node(NodeId::new(0)),
        );
        let requirements = HistoryRequirements::analyze(&[], &names(&["a"]), &[graph], &layout);

        assert_eq!(requirement_set(&requirements), vec![(0, 2)]);
    }

    #[test]
    fn named_stream_reads_are_charged_to_the_named_source() {
        let layout = Rc::new(EnvironmentLayout::from_variables(names(&["x", "a", "b"])));
        let first = program(
            &layout,
            vec![BoundOp::Delay {
                input: external(&layout, "x"),
                offset: 3,
            }],
            BoundRef::Node(NodeId::new(0)),
        );
        let second = program(
            &layout,
            vec![BoundOp::Delay {
                input: external(&layout, "a"),
                offset: 2,
            }],
            BoundRef::Node(NodeId::new(0)),
        );
        let requirements = HistoryRequirements::analyze(
            &names(&["x"]),
            &names(&["a", "b"]),
            &[first, second],
            &layout,
        );

        assert_eq!(
            requirements.depth(layout.slot(&VarName::new("x")).unwrap()),
            3
        );
        assert_eq!(
            requirements.depth(layout.slot(&VarName::new("a")).unwrap()),
            2
        );
        assert_eq!(
            requirements.depth(layout.slot(&VarName::new("b")).unwrap()),
            0
        );
    }
}
