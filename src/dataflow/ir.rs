use super::environment::{EnvironmentLayout, EnvironmentSlot};
use super::execution::specialization;
use super::*;
use crate::core::{BinaryOperator, UnaryOperator};
use crate::lang::dsrv::ast::DynamicExprScope;
use std::num::NonZeroU64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct NodeId(usize);

impl NodeId {
    pub(super) fn new(index: usize) -> Self {
        Self(index)
    }

    pub(super) fn index(self) -> usize {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ScalarKind {
    Int,
    Float,
    Bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ScalarSignature {
    Unary {
        input: ScalarKind,
        output: ScalarKind,
    },
    Binary {
        left: ScalarKind,
        right: ScalarKind,
        output: ScalarKind,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DynamicExpressionMode {
    Dynamic,
    Defer,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum DynamicExpressionScope {
    Automatic,
    Restricted { allowed_variables: EcoVec<VarName> },
}

impl DynamicExpressionScope {
    pub(super) fn from_ast(scope: DynamicExprScope) -> Self {
        match scope {
            DynamicExprScope::Automatic => Self::Automatic,
            DynamicExprScope::Explicit(allowed_variables) => Self::Restricted { allowed_variables },
        }
    }

    pub(super) fn allowed_variables(&self) -> Option<&EcoVec<VarName>> {
        match self {
            Self::Automatic => None,
            Self::Restricted { allowed_variables } => Some(allowed_variables),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct UnboundFunction {
    pub(super) parameters: EcoVec<VarName>,
    pub(super) graph: UnboundEvaluationGraph,
    pub(super) display: EcoString,
}

impl UnboundFunction {
    pub(super) fn new(
        parameters: EcoVec<VarName>,
        graph: UnboundEvaluationGraph,
        display: EcoString,
    ) -> Self {
        Self {
            parameters,
            graph,
            display,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct StreamFunction {
    pub(super) parameters: EcoVec<VarName>,
    pub(super) program: Rc<StreamProgram>,
    pub(super) display: EcoString,
    pub(super) capture_slots: Vec<EnvironmentSlot>,
}

pub(super) trait GraphReference: Clone + std::fmt::Debug + PartialEq {
    type Function: Clone + std::fmt::Debug + PartialEq;

    fn function_has_temporal_state(function: &Self::Function) -> bool;
}

impl GraphReference for VarName {
    type Function = UnboundFunction;

    fn function_has_temporal_state(function: &Self::Function) -> bool {
        function.graph.has_temporal_state()
    }
}

impl GraphReference for EnvironmentSlot {
    type Function = StreamFunction;

    fn function_has_temporal_state(function: &Self::Function) -> bool {
        function.program.graph.has_temporal_state()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct EvaluationGraph<E: GraphReference> {
    pub(super) nodes: Vec<StreamOp<E>>,
    pub(super) scalar_signatures: Vec<Option<ScalarSignature>>,
    pub(super) output: DataRef<E>,
    pub(super) recursive_delays: Vec<NodeId>,
}

impl<E: GraphReference> EvaluationGraph<E> {
    pub(super) fn new(
        nodes: Vec<StreamOp<E>>,
        scalar_signatures: Vec<Option<ScalarSignature>>,
        output: DataRef<E>,
    ) -> Self {
        debug_assert_eq!(nodes.len(), scalar_signatures.len());
        Self {
            nodes,
            scalar_signatures,
            output,
            recursive_delays: Vec::new(),
        }
    }

    pub(super) fn is_fallible(&self) -> bool {
        self.nodes.iter().any(|op| match op {
            StreamOp::Dynamic(_) => true,
            StreamOp::If {
                then_branch,
                else_branch,
                ..
            } => then_branch.is_fallible() || else_branch.is_fallible(),
            _ => false,
        })
    }

    pub(super) fn has_temporal_state(&self) -> bool {
        self.nodes.iter().any(|op| {
            op.temporal_operator_name().is_some()
                || match op {
                    StreamOp::If {
                        then_branch,
                        else_branch,
                        ..
                    } => then_branch.has_temporal_state() || else_branch.has_temporal_state(),
                    StreamOp::DirectApply { func, .. } | StreamOp::RecursiveApply { func, .. } => {
                        E::function_has_temporal_state(func)
                    }
                    _ => false,
                }
        })
    }
}

impl BoundEvaluationGraph {
    pub(super) fn debug_assert_valid(&self, environment_len: usize) {
        fn assert_ref(reference: &BoundRef, node_limit: usize, environment_len: usize) {
            match reference {
                BoundRef::Const(_) => {}
                BoundRef::External(id) => debug_assert!(id.index() < environment_len),
                BoundRef::Node(id) => debug_assert!(
                    id.index() < node_limit,
                    "node {} references unevaluated node {}",
                    node_limit,
                    id.index()
                ),
            }
        }

        assert_ref(&self.output, self.nodes.len(), environment_len);
        debug_assert_eq!(self.nodes.len(), self.scalar_signatures.len());
        for (index, op) in self.nodes.iter().enumerate() {
            op.for_each_operand(|operand| assert_ref(operand, index, environment_len));
            match op {
                BoundOp::If {
                    then_branch,
                    else_branch,
                    ..
                } => {
                    then_branch.debug_assert_valid(environment_len);
                    else_branch.debug_assert_valid(environment_len);
                }
                BoundOp::Function { func }
                | BoundOp::DirectApply { func, .. }
                | BoundOp::RecursiveApply { func, .. } => {
                    func.program
                        .graph
                        .debug_assert_valid(func.program.environment_layout.len());
                }
                _ => {}
            }
        }

        let recursive_nodes = self
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(index, op)| op.is_recursive_delay().then(|| NodeId::new(index)))
            .collect::<Vec<_>>();
        debug_assert_eq!(self.recursive_delays, recursive_nodes);
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct StreamProgram {
    pub(super) graph: BoundEvaluationGraph,
    pub(super) environment_layout: Rc<EnvironmentLayout>,
    pub(super) evaluation_mode: EvaluationMode,
    pub(super) requires_temporal_commit: bool,
    pub(super) specialization_plan: Option<Rc<specialization::Plan>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum EvaluationMode {
    Infallible,
    Fallible,
}

impl StreamProgram {
    pub(super) fn new(
        graph: BoundEvaluationGraph,
        environment_layout: Rc<EnvironmentLayout>,
    ) -> Self {
        let evaluation_mode = if graph.is_fallible() {
            EvaluationMode::Fallible
        } else {
            EvaluationMode::Infallible
        };
        let requires_temporal_commit = graph_requires_temporal_commit(&graph);
        let specialization_plan = (evaluation_mode == EvaluationMode::Infallible)
            .then(|| specialization::Plan::new(&graph))
            .flatten()
            .map(Rc::new);
        Self {
            graph,
            environment_layout,
            evaluation_mode,
            requires_temporal_commit,
            specialization_plan,
        }
    }

    pub(super) fn has_reconfiguration_points(&self) -> bool {
        graph_has_reconfiguration_points(&self.graph)
    }

    pub(super) fn reconfiguration_points(&self) -> impl Iterator<Item = (NodeId, &BoundRef)> {
        self.graph
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(index, op)| match op {
                BoundOp::Dynamic(spec) => Some((NodeId::new(index), &spec.input)),
                _ => None,
            })
    }

    pub(super) fn can_resolve_dependencies_before_evaluation(&self) -> bool {
        graph_supports_early_dependency_resolution(&self.graph, |source| {
            matches!(source, BoundRef::Const(_) | BoundRef::External(_))
        })
    }

    #[inline]
    pub(super) fn requires_temporal_commit(&self) -> bool {
        self.requires_temporal_commit
    }

    #[inline]
    pub(super) fn is_infallible(&self) -> bool {
        self.evaluation_mode == EvaluationMode::Infallible
    }
}

fn graph_has_reconfiguration_points(graph: &BoundEvaluationGraph) -> bool {
    graph.nodes.iter().any(|op| match op {
        BoundOp::Dynamic(_) => true,
        BoundOp::If {
            then_branch,
            else_branch,
            ..
        } => {
            graph_has_reconfiguration_points(then_branch)
                || graph_has_reconfiguration_points(else_branch)
        }
        _ => false,
    })
}

fn graph_supports_early_dependency_resolution(
    graph: &BoundEvaluationGraph,
    source_is_available: impl Copy + Fn(&BoundRef) -> bool,
) -> bool {
    graph.nodes.iter().all(|op| match op {
        BoundOp::Dynamic(spec) => source_is_available(&spec.input),
        BoundOp::If {
            then_branch,
            else_branch,
            ..
        } => !then_branch.is_fallible() && !else_branch.is_fallible(),
        _ => true,
    })
}

fn graph_requires_temporal_commit(graph: &BoundEvaluationGraph) -> bool {
    graph.nodes.iter().any(|op| match op {
        BoundOp::Delay { offset, .. } => *offset > 0,
        BoundOp::RecursiveDelay { .. } | BoundOp::DirectApply { .. } | BoundOp::Dynamic(_) => true,
        BoundOp::If {
            then_branch,
            else_branch,
            ..
        } => {
            graph_requires_temporal_commit(then_branch)
                || graph_requires_temporal_commit(else_branch)
        }
        _ => false,
    })
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum DataRef<E> {
    Const(Value),
    External(E),
    Node(NodeId),
}

pub(super) type UnboundRef = DataRef<VarName>;
pub(super) type BoundRef = DataRef<EnvironmentSlot>;
pub(super) type UnboundEvaluationGraph = EvaluationGraph<VarName>;
pub(super) type BoundEvaluationGraph = EvaluationGraph<EnvironmentSlot>;
pub(super) type UnboundOp = StreamOp<VarName>;
pub(super) type BoundOp = StreamOp<EnvironmentSlot>;
pub(super) type UnboundDynamicExpressionSpec = DynamicExpressionSpec<VarName>;
pub(super) type BoundDynamicExpressionSpec = DynamicExpressionSpec<EnvironmentSlot>;
#[derive(Clone, Debug, PartialEq)]
pub(super) struct DynamicExpressionTyping {
    pub(super) environment: Rc<StreamTypeEnvironment>,
    pub(super) expected_type: TCType,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct DynamicExpressionSpec<E> {
    pub(super) input: DataRef<E>,
    pub(super) scope: DynamicExpressionScope,
    pub(super) mode: DynamicExpressionMode,
    /// Type information for typed graphs; `None` for untyped graphs.
    pub(super) typing: Option<DynamicExpressionTyping>,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum StreamOp<E: GraphReference> {
    Unary {
        op: UnaryOperator,
        arg: DataRef<E>,
    },
    Binary {
        op: BinaryOperator,
        lhs: DataRef<E>,
        rhs: DataRef<E>,
    },
    If {
        cond: DataRef<E>,
        then_branch: EvaluationGraph<E>,
        else_branch: EvaluationGraph<E>,
    },
    Delay {
        input: DataRef<E>,
        offset: u64,
    },
    RecursiveDelay {
        offset: NonZeroU64,
    },
    Default {
        input: DataRef<E>,
        fallback: DataRef<E>,
    },
    Init {
        input: DataRef<E>,
        initial: DataRef<E>,
    },
    IsDefined {
        input: DataRef<E>,
    },
    When {
        input: DataRef<E>,
    },
    Update {
        base: DataRef<E>,
        update: DataRef<E>,
    },
    Latch {
        value: DataRef<E>,
        trigger: DataRef<E>,
    },
    List(Vec<DataRef<E>>),
    Tuple(Vec<DataRef<E>>),
    Map(BTreeMap<EcoString, DataRef<E>>),
    LIndex {
        list: DataRef<E>,
        index: DataRef<E>,
    },
    LAppend {
        list: DataRef<E>,
        value: DataRef<E>,
    },
    LConcat {
        lhs: DataRef<E>,
        rhs: DataRef<E>,
    },
    LHead {
        list: DataRef<E>,
    },
    LTail {
        list: DataRef<E>,
    },
    LLen {
        list: DataRef<E>,
    },
    MGet {
        map: DataRef<E>,
        key: EcoString,
    },
    MRemove {
        map: DataRef<E>,
        key: EcoString,
    },
    MInsert {
        map: DataRef<E>,
        key: EcoString,
        value: DataRef<E>,
    },
    MHasKey {
        map: DataRef<E>,
        key: EcoString,
    },
    TGet {
        tuple: DataRef<E>,
        index: usize,
    },
    Dynamic(DynamicExpressionSpec<E>),
    Function {
        func: E::Function,
    },
    Apply {
        func: DataRef<E>,
        args: Vec<DataRef<E>>,
    },
    /// A statically known lambda application with one persistent program instance.
    DirectApply {
        func: E::Function,
        args: Vec<DataRef<E>>,
    },
    RecursiveApply {
        func: E::Function,
        args: Vec<DataRef<E>>,
    },
    RecursiveCall {
        args: Vec<DataRef<E>>,
    },
    Partial {
        func: DataRef<E>,
        args: Vec<DataRef<E>>,
        display: EcoString,
    },
    Fix {
        func: DataRef<E>,
        display: EcoString,
    },
    ListMap {
        func: DataRef<E>,
        list: DataRef<E>,
    },
    ListFilter {
        func: DataRef<E>,
        list: DataRef<E>,
    },
    ListFold {
        func: DataRef<E>,
        init: DataRef<E>,
        list: DataRef<E>,
    },
}

impl<E: GraphReference> StreamOp<E> {
    /// Visit every direct operand used by dependency analysis.
    pub(super) fn for_each_operand(&self, mut visit: impl FnMut(&DataRef<E>)) {
        match self {
            StreamOp::Unary { arg, .. } => visit(arg),
            StreamOp::Binary { lhs, rhs, .. }
            | StreamOp::Default {
                input: lhs,
                fallback: rhs,
            }
            | StreamOp::Init {
                input: lhs,
                initial: rhs,
            }
            | StreamOp::Update {
                base: lhs,
                update: rhs,
            }
            | StreamOp::Latch {
                value: lhs,
                trigger: rhs,
            }
            | StreamOp::LIndex {
                list: lhs,
                index: rhs,
            }
            | StreamOp::LAppend {
                list: lhs,
                value: rhs,
            }
            | StreamOp::LConcat { lhs, rhs } => {
                visit(lhs);
                visit(rhs);
            }
            StreamOp::If { cond, .. } => visit(cond),
            StreamOp::Delay { input, .. }
            | StreamOp::IsDefined { input }
            | StreamOp::When { input }
            | StreamOp::LHead { list: input }
            | StreamOp::LTail { list: input }
            | StreamOp::LLen { list: input }
            | StreamOp::MGet { map: input, .. }
            | StreamOp::MRemove { map: input, .. }
            | StreamOp::MHasKey { map: input, .. }
            | StreamOp::TGet { tuple: input, .. }
            | StreamOp::Fix { func: input, .. } => visit(input),
            StreamOp::Dynamic(DynamicExpressionSpec { input, .. }) => visit(input),
            StreamOp::List(items) | StreamOp::Tuple(items) => {
                items.into_iter().for_each(&mut visit)
            }
            StreamOp::Map(items) => items.into_iter().for_each(|(_, value)| visit(value)),
            StreamOp::MInsert { map, value, .. }
            | StreamOp::ListMap {
                func: map,
                list: value,
            }
            | StreamOp::ListFilter {
                func: map,
                list: value,
            } => {
                visit(map);
                visit(value);
            }
            StreamOp::ListFold { func, init, list } => {
                visit(func);
                visit(init);
                visit(list);
            }
            StreamOp::Apply { func, args } | StreamOp::Partial { func, args, .. } => {
                visit(func);
                args.into_iter().for_each(&mut visit);
            }
            StreamOp::DirectApply { args, .. }
            | StreamOp::RecursiveApply { args, .. }
            | StreamOp::RecursiveCall { args } => args.into_iter().for_each(&mut visit),
            StreamOp::Function { .. } | StreamOp::RecursiveDelay { .. } => {}
        }
    }

    pub(super) fn is_recursive_delay(&self) -> bool {
        matches!(self, StreamOp::RecursiveDelay { .. })
    }

    pub(super) fn temporal_operator_name(&self) -> Option<&'static str> {
        match self {
            Self::Delay { .. } | Self::RecursiveDelay { .. } => Some("sindex"),
            Self::Init { .. } => Some("init"),
            Self::When { .. } => Some("when"),
            Self::Update { .. } => Some("update"),
            Self::Latch { .. } => Some("latch"),
            Self::Dynamic(_) => Some("dynamic/defer"),
            _ => None,
        }
    }
}
