use super::environment::{EnvironmentLayout, EnvironmentSlot};
use super::reconfiguration::StreamStateKey;
use super::*;
use crate::core::{BinaryOperator, UnaryOperator};
use crate::lang::dsrv::ast::DynamicExprScope;

use std::fmt::Write as _;
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
pub(super) enum ReconfigurableExpressionKind {
    Dynamic,
    Deferred,
}

/// The variables a nested `dynamic`/`defer` body may reference.
///
/// Binding resolves both forms to a concrete variable list, but the two are
/// kept apart: an `Automatic` list is derived from the enclosing program, while
/// a `Restricted` list is written in the specification. Only the latter is part
/// of the owning stream's semantic identity.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum ReconfigurableExpressionScope {
    Automatic { allowed_variables: EcoVec<VarName> },
    Restricted { allowed_variables: EcoVec<VarName> },
}

impl ReconfigurableExpressionScope {
    pub(super) fn from_ast(scope: DynamicExprScope) -> Self {
        match scope {
            DynamicExprScope::Automatic => Self::Automatic {
                allowed_variables: EcoVec::new(),
            },
            DynamicExprScope::Explicit(allowed_variables) => Self::Restricted { allowed_variables },
        }
    }

    pub(super) fn allowed_variables(&self) -> &EcoVec<VarName> {
        match self {
            Self::Automatic { allowed_variables } | Self::Restricted { allowed_variables } => {
                allowed_variables
            }
        }
    }

    pub(super) fn with_allowed_variables(&self, allowed_variables: EcoVec<VarName>) -> Self {
        match self {
            Self::Automatic { .. } => Self::Automatic { allowed_variables },
            Self::Restricted { .. } => Self::Restricted { allowed_variables },
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

#[derive(Clone, Debug)]
pub(super) struct StreamProgram {
    pub(super) graph: BoundEvaluationGraph,
    pub(super) environment_layout: Rc<EnvironmentLayout>,
    pub(super) state_key: StreamStateKey,
    pub(super) evaluation_mode: EvaluationMode,
    pub(super) requires_temporal_commit: bool,
}

impl PartialEq for StreamProgram {
    fn eq(&self, other: &Self) -> bool {
        self.graph == other.graph
            && self.environment_layout == other.environment_layout
            && self.state_key == other.state_key
            && self.evaluation_mode == other.evaluation_mode
            && self.requires_temporal_commit == other.requires_temporal_commit
    }
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
        let descriptor = canonical_graph_descriptor(&graph, environment_layout.as_ref());
        let state_key = StreamStateKey::from_canonical(descriptor.as_bytes());
        Self {
            graph,
            environment_layout,
            state_key,
            evaluation_mode,
            requires_temporal_commit,
        }
    }

    #[inline]
    pub(super) fn state_key(&self) -> StreamStateKey {
        self.state_key
    }

    pub(super) fn has_reconfigurable_expressions(&self) -> bool {
        graph_has_reconfigurable_expressions(&self.graph)
    }

    pub(super) fn reconfigurable_expressions(
        &self,
    ) -> impl Iterator<Item = (NodeId, &BoundRef, ReconfigurableExpressionKind)> {
        self.graph
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(index, op)| match op {
                BoundOp::Dynamic(spec) => Some((NodeId::new(index), &spec.input, spec.kind)),
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

fn graph_has_reconfigurable_expressions(graph: &BoundEvaluationGraph) -> bool {
    graph.nodes.iter().any(|op| match op {
        BoundOp::Dynamic(_) => true,
        BoundOp::If {
            then_branch,
            else_branch,
            ..
        } => {
            graph_has_reconfigurable_expressions(then_branch)
                || graph_has_reconfigurable_expressions(else_branch)
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

/// Build a deterministic semantic descriptor for a bound graph.  References to runtime slots are
/// rewritten to variable names; node numbers are only structural edges inside this descriptor and
/// are never exposed as portable state identities.  The descriptor intentionally excludes schedule,
/// plan, quickening, and native-tier data.
pub(super) fn canonical_graph_descriptor(
    graph: &BoundEvaluationGraph,
    layout: &EnvironmentLayout,
) -> String {
    let mut descriptor = String::from("graph-v2{");
    append_graph_descriptor(&mut descriptor, graph, layout);
    descriptor.push('}');
    descriptor
}

fn append_graph_descriptor(
    descriptor: &mut String,
    graph: &BoundEvaluationGraph,
    layout: &EnvironmentLayout,
) {
    descriptor.push_str("nodes[");
    for (index, (op, signature)) in graph.nodes.iter().zip(&graph.scalar_signatures).enumerate() {
        let _ = write!(descriptor, "n{index}/");
        append_op_descriptor(descriptor, op, layout);
        descriptor.push('/');
        append_scalar_signature(descriptor, signature);
        descriptor.push(';');
    }
    descriptor.push_str("];out=");
    append_ref_descriptor(descriptor, &graph.output, layout);
    descriptor.push_str(";recursive=");
    for node in &graph.recursive_delays {
        let _ = write!(descriptor, "n{};", node.index());
    }
}

fn append_scalar_signature(descriptor: &mut String, signature: &Option<ScalarSignature>) {
    match signature {
        None => descriptor.push('-'),
        Some(ScalarSignature::Unary { input, output }) => {
            descriptor.push_str("u:");
            append_scalar_kind(descriptor, *input);
            descriptor.push(':');
            append_scalar_kind(descriptor, *output);
        }
        Some(ScalarSignature::Binary {
            left,
            right,
            output,
        }) => {
            descriptor.push_str("b:");
            append_scalar_kind(descriptor, *left);
            descriptor.push(':');
            append_scalar_kind(descriptor, *right);
            descriptor.push(':');
            append_scalar_kind(descriptor, *output);
        }
    }
}

fn append_scalar_kind(descriptor: &mut String, kind: ScalarKind) {
    descriptor.push_str(match kind {
        ScalarKind::Int => "int",
        ScalarKind::Float => "float",
        ScalarKind::Bool => "bool",
    });
}

fn append_ref_descriptor(
    descriptor: &mut String,
    reference: &BoundRef,
    layout: &EnvironmentLayout,
) {
    match reference {
        DataRef::Const(value) => {
            descriptor.push_str("const(");
            append_value_descriptor(descriptor, value);
            descriptor.push(')');
        }
        DataRef::External(slot) => {
            descriptor.push_str("external(");
            if let Some(variable) = layout.variable(*slot) {
                append_identifier(descriptor, variable.name());
                if let Some(stream_type) = layout.stream_type(*slot) {
                    descriptor.push(':');
                    append_stream_type(descriptor, stream_type);
                }
            } else {
                descriptor.push_str("unknown");
            }
            descriptor.push(')');
        }
        DataRef::Node(node) => {
            let _ = write!(descriptor, "node{}", node.index());
        }
    }
}

fn append_op_descriptor(descriptor: &mut String, operation: &BoundOp, layout: &EnvironmentLayout) {
    match operation {
        StreamOp::Unary { op, arg } => {
            descriptor.push_str("unary:");
            descriptor.push_str(op.name());
            descriptor.push('(');
            append_ref_descriptor(descriptor, arg, layout);
            descriptor.push(')');
        }
        StreamOp::Binary { op, lhs, rhs } => {
            descriptor.push_str("binary:");
            descriptor.push_str(op.name());
            descriptor.push('(');
            append_ref_descriptor(descriptor, lhs, layout);
            descriptor.push(',');
            append_ref_descriptor(descriptor, rhs, layout);
            descriptor.push(')');
        }
        StreamOp::If {
            cond,
            then_branch,
            else_branch,
        } => {
            descriptor.push_str("if(");
            append_ref_descriptor(descriptor, cond, layout);
            descriptor.push_str(",then{");
            append_graph_descriptor(descriptor, then_branch, layout);
            descriptor.push_str("},else{");
            append_graph_descriptor(descriptor, else_branch, layout);
            descriptor.push_str("})");
        }
        StreamOp::Delay { input, offset } => {
            let _ = write!(descriptor, "delay{}(", offset);
            append_ref_descriptor(descriptor, input, layout);
            descriptor.push(')');
        }
        StreamOp::RecursiveDelay { offset } => {
            let _ = write!(descriptor, "recursive-delay{}", offset);
        }
        StreamOp::Default { input, fallback } => {
            descriptor.push_str("default(");
            append_ref_descriptor(descriptor, input, layout);
            descriptor.push(',');
            append_ref_descriptor(descriptor, fallback, layout);
            descriptor.push(')');
        }
        StreamOp::Init { input, initial } => {
            descriptor.push_str("init(");
            append_ref_descriptor(descriptor, input, layout);
            descriptor.push(',');
            append_ref_descriptor(descriptor, initial, layout);
            descriptor.push(')');
        }
        StreamOp::IsDefined { input } => {
            descriptor.push_str("is-defined(");
            append_ref_descriptor(descriptor, input, layout);
            descriptor.push(')');
        }
        StreamOp::When { input } => {
            descriptor.push_str("when(");
            append_ref_descriptor(descriptor, input, layout);
            descriptor.push(')');
        }
        StreamOp::Update { base, update } => {
            descriptor.push_str("update(");
            append_ref_descriptor(descriptor, base, layout);
            descriptor.push(',');
            append_ref_descriptor(descriptor, update, layout);
            descriptor.push(')');
        }
        StreamOp::Latch { value, trigger } => {
            descriptor.push_str("latch(");
            append_ref_descriptor(descriptor, value, layout);
            descriptor.push(',');
            append_ref_descriptor(descriptor, trigger, layout);
            descriptor.push(')');
        }
        StreamOp::List(items) | StreamOp::Tuple(items) => {
            descriptor.push_str(if matches!(operation, StreamOp::List(_)) {
                "list["
            } else {
                "tuple["
            });
            for item in items {
                append_ref_descriptor(descriptor, item, layout);
                descriptor.push(',');
            }
            descriptor.push(']');
        }
        StreamOp::Map(items) => {
            descriptor.push_str("map[");
            for (key, value) in items {
                append_identifier(descriptor, key.as_str());
                descriptor.push('=');
                append_ref_descriptor(descriptor, value, layout);
                descriptor.push(',');
            }
            descriptor.push(']');
        }
        StreamOp::LIndex { list, index } => {
            descriptor.push_str("index(");
            append_ref_descriptor(descriptor, list, layout);
            descriptor.push(',');
            append_ref_descriptor(descriptor, index, layout);
            descriptor.push(')');
        }
        StreamOp::LAppend { list, value } => {
            descriptor.push_str("append(");
            append_ref_descriptor(descriptor, list, layout);
            descriptor.push(',');
            append_ref_descriptor(descriptor, value, layout);
            descriptor.push(')');
        }
        StreamOp::LConcat { lhs, rhs } => {
            descriptor.push_str("concat(");
            append_ref_descriptor(descriptor, lhs, layout);
            descriptor.push(',');
            append_ref_descriptor(descriptor, rhs, layout);
            descriptor.push(')');
        }
        StreamOp::LHead { list } => append_unary_named(descriptor, "head", list, layout),
        StreamOp::LTail { list } => append_unary_named(descriptor, "tail", list, layout),
        StreamOp::LLen { list } => append_unary_named(descriptor, "len", list, layout),
        StreamOp::MGet { map, key } => append_map_named(descriptor, "get", map, key, layout),
        StreamOp::MRemove { map, key } => append_map_named(descriptor, "remove", map, key, layout),
        StreamOp::MHasKey { map, key } => append_map_named(descriptor, "has-key", map, key, layout),
        StreamOp::MInsert { map, key, value } => {
            descriptor.push_str("insert(");
            append_ref_descriptor(descriptor, map, layout);
            descriptor.push(',');
            append_identifier(descriptor, key.as_str());
            descriptor.push(',');
            append_ref_descriptor(descriptor, value, layout);
            descriptor.push(')');
        }
        StreamOp::TGet { tuple, index } => {
            let _ = write!(descriptor, "tuple-get{}(", index);
            append_ref_descriptor(descriptor, tuple, layout);
            descriptor.push(')');
        }
        StreamOp::Dynamic(spec) => {
            descriptor.push_str("dynamic(");
            append_ref_descriptor(descriptor, &spec.input, layout);
            descriptor.push_str(",mode=");
            descriptor.push_str(match spec.kind {
                ReconfigurableExpressionKind::Dynamic => "dynamic",
                ReconfigurableExpressionKind::Deferred => "defer",
            });
            descriptor.push_str(",scope=");
            // An automatic scope is derived from the whole program's variable
            // set, so naming its members here would make every dynamic stream
            // depend on unrelated declarations. A body that actually reads a
            // changed variable is caught by that body's own descriptor.
            match &spec.scope {
                ReconfigurableExpressionScope::Automatic { .. } => descriptor.push_str("automatic"),
                ReconfigurableExpressionScope::Restricted { allowed_variables } => {
                    let mut allowed = allowed_variables.iter().collect::<Vec<_>>();
                    allowed.sort_by_key(|variable| variable.name());
                    for variable in allowed {
                        append_identifier(descriptor, &variable.name());
                        if let Some(typing) = &spec.typing
                            && let Some(stream_type) = typing.environment.get(variable)
                        {
                            descriptor.push(':');
                            append_stream_type(descriptor, stream_type);
                        }
                        descriptor.push(',');
                    }
                }
            }
            if let Some(typing) = &spec.typing {
                descriptor.push_str(",type=");
                append_tc_type(descriptor, &typing.expected_type);
            }
            descriptor.push(')');
        }
        StreamOp::Function { func }
        | StreamOp::DirectApply { func, .. }
        | StreamOp::RecursiveApply { func, .. } => {
            descriptor.push_str("function(");
            for parameter in &func.parameters {
                append_identifier(descriptor, parameter.name());
                descriptor.push(',');
            }
            descriptor.push_str("captures=");
            for slot in &func.capture_slots {
                if let Some(variable) = layout.variable(*slot) {
                    append_identifier(descriptor, variable.name());
                }
                descriptor.push(',');
            }
            descriptor.push_str("body{");
            append_graph_descriptor(
                descriptor,
                &func.program.graph,
                &func.program.environment_layout,
            );
            descriptor.push_str("})");
        }
        StreamOp::Apply { func, args } => append_apply(descriptor, "apply", func, args, layout),
        StreamOp::Partial { func, args, .. } => {
            append_apply(descriptor, "partial", func, args, layout)
        }
        StreamOp::RecursiveCall { args } => {
            descriptor.push_str("recursive-call[");
            for arg in args {
                append_ref_descriptor(descriptor, arg, layout);
                descriptor.push(',');
            }
            descriptor.push(']');
        }
        StreamOp::Fix { func, .. } => append_unary_named(descriptor, "fix", func, layout),
        StreamOp::ListMap { func, list } => {
            append_binary_named(descriptor, "list-map", func, list, layout)
        }
        StreamOp::ListFilter { func, list } => {
            append_binary_named(descriptor, "list-filter", func, list, layout)
        }
        StreamOp::ListFold { func, init, list } => {
            descriptor.push_str("list-fold(");
            append_ref_descriptor(descriptor, func, layout);
            descriptor.push(',');
            append_ref_descriptor(descriptor, init, layout);
            descriptor.push(',');
            append_ref_descriptor(descriptor, list, layout);
            descriptor.push(')');
        }
    }
}

fn append_unary_named(
    descriptor: &mut String,
    name: &str,
    reference: &BoundRef,
    layout: &EnvironmentLayout,
) {
    descriptor.push_str(name);
    descriptor.push('(');
    append_ref_descriptor(descriptor, reference, layout);
    descriptor.push(')');
}

fn append_binary_named(
    descriptor: &mut String,
    name: &str,
    left: &BoundRef,
    right: &BoundRef,
    layout: &EnvironmentLayout,
) {
    descriptor.push_str(name);
    descriptor.push('(');
    append_ref_descriptor(descriptor, left, layout);
    descriptor.push(',');
    append_ref_descriptor(descriptor, right, layout);
    descriptor.push(')');
}

fn append_map_named(
    descriptor: &mut String,
    name: &str,
    reference: &BoundRef,
    key: &EcoString,
    layout: &EnvironmentLayout,
) {
    descriptor.push_str(name);
    descriptor.push('(');
    append_ref_descriptor(descriptor, reference, layout);
    descriptor.push(',');
    append_identifier(descriptor, key.as_str());
    descriptor.push(')');
}

fn append_apply(
    descriptor: &mut String,
    name: &str,
    function: &BoundRef,
    args: &[BoundRef],
    layout: &EnvironmentLayout,
) {
    descriptor.push_str(name);
    descriptor.push('(');
    append_ref_descriptor(descriptor, function, layout);
    descriptor.push(';');
    for arg in args {
        append_ref_descriptor(descriptor, arg, layout);
        descriptor.push(',');
    }
    descriptor.push(')');
}

fn append_identifier(descriptor: &mut String, identifier: impl AsRef<str>) {
    descriptor.push_str(identifier.as_ref().len().to_string().as_str());
    descriptor.push(':');
    descriptor.push_str(identifier.as_ref());
}

fn append_value_descriptor(descriptor: &mut String, value: &Value) {
    match value {
        Value::Int(value) => {
            let _ = write!(descriptor, "int:{value}");
        }
        Value::Float(value) => {
            let _ = write!(descriptor, "float:{:016x}", value.to_bits());
        }
        Value::Str(value) => {
            descriptor.push_str("str:");
            append_identifier(descriptor, value.as_str());
        }
        Value::Bool(value) => {
            let _ = write!(descriptor, "bool:{value}");
        }
        Value::Function(function) => {
            descriptor.push_str("function:");
            append_identifier(descriptor, function.display_source());
        }
        Value::List(values) => {
            descriptor.push_str("list[");
            for value in values {
                append_value_descriptor(descriptor, value);
                descriptor.push(',');
            }
            descriptor.push(']');
        }
        Value::Tuple(values) => {
            descriptor.push_str("tuple[");
            for value in values {
                append_value_descriptor(descriptor, value);
                descriptor.push(',');
            }
            descriptor.push(']');
        }
        Value::Map(values) => {
            descriptor.push_str("map[");
            for (key, value) in values {
                append_identifier(descriptor, key.as_str());
                descriptor.push('=');
                append_value_descriptor(descriptor, value);
                descriptor.push(',');
            }
            descriptor.push(']');
        }
        Value::Unit => descriptor.push_str("unit"),
        Value::Deferred => descriptor.push_str("deferred"),
        Value::NoVal => descriptor.push_str("no-val"),
    }
}

fn append_tc_type(descriptor: &mut String, type_: &crate::lang::dsrv::type_checker::TCType) {
    use crate::lang::dsrv::type_checker::TCType;
    match type_ {
        TCType::Int => descriptor.push_str("int"),
        TCType::Float => descriptor.push_str("float"),
        TCType::Str => descriptor.push_str("str"),
        TCType::Bool => descriptor.push_str("bool"),
        TCType::Unit => descriptor.push_str("unit"),
        TCType::Map(value) => {
            descriptor.push_str("map<");
            append_tc_type(descriptor, value);
            descriptor.push('>');
        }
        TCType::Expr(value) => {
            descriptor.push_str("expr<");
            append_tc_type(descriptor, value);
            descriptor.push('>');
        }
        TCType::Tuple(values) => {
            descriptor.push_str("tuple<");
            for value in values {
                append_tc_type(descriptor, value);
                descriptor.push(',');
            }
            descriptor.push('>');
        }
        TCType::List(value) => {
            descriptor.push_str("list<");
            append_tc_type(descriptor, value);
            descriptor.push('>');
        }
        TCType::Struct(fields, extra) => {
            descriptor.push_str("struct<");
            for (name, value) in fields {
                append_identifier(descriptor, name.as_str());
                descriptor.push(':');
                append_tc_type(descriptor, value);
                descriptor.push(',');
            }
            let _ = write!(descriptor, ";extra={extra}>");
        }
        TCType::Function(parameters, output) => {
            descriptor.push_str("fn<");
            for parameter in parameters {
                append_tc_type(descriptor, parameter);
                descriptor.push(',');
            }
            descriptor.push_str("->");
            append_tc_type(descriptor, output);
            descriptor.push('>');
        }
        TCType::EmptyList => descriptor.push_str("empty-list"),
        TCType::EmptyMap => descriptor.push_str("empty-map"),
        TCType::Any => descriptor.push_str("any"),
        TCType::Unknown => descriptor.push_str("unknown"),
    }
}

fn append_stream_type(descriptor: &mut String, type_: &StreamType) {
    match type_ {
        StreamType::Int => descriptor.push_str("int"),
        StreamType::Float => descriptor.push_str("float"),
        StreamType::Str => descriptor.push_str("str"),
        StreamType::Bool => descriptor.push_str("bool"),
        StreamType::Unit => descriptor.push_str("unit"),
        StreamType::List(value) => {
            descriptor.push_str("list<");
            append_stream_type(descriptor, value);
            descriptor.push('>');
        }
        StreamType::Tuple(values) => {
            descriptor.push_str("tuple<");
            for value in values {
                append_stream_type(descriptor, value);
                descriptor.push(',');
            }
            descriptor.push('>');
        }
        StreamType::Map(value) => {
            descriptor.push_str("map<");
            append_stream_type(descriptor, value);
            descriptor.push('>');
        }
        StreamType::Expr(value) => {
            descriptor.push_str("expr<");
            append_stream_type(descriptor, value);
            descriptor.push('>');
        }
        StreamType::Struct(fields, extra) => {
            descriptor.push_str("struct<");
            for (name, value) in fields {
                append_identifier(descriptor, name.as_str());
                descriptor.push(':');
                append_stream_type(descriptor, value);
                descriptor.push(',');
            }
            let _ = write!(descriptor, ";extra={extra}>");
        }
        StreamType::Function(parameters, output) => {
            descriptor.push_str("fn<");
            for parameter in parameters {
                append_stream_type(descriptor, parameter);
                descriptor.push(',');
            }
            descriptor.push_str("->");
            append_stream_type(descriptor, output);
            descriptor.push('>');
        }
        StreamType::Any => descriptor.push_str("any"),
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct ReconfigurableExpressionTyping {
    pub(super) environment: Rc<StreamTypeEnvironment>,
    pub(super) expected_type: TCType,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct DynamicExpressionSpec<E> {
    pub(super) input: DataRef<E>,
    pub(super) scope: ReconfigurableExpressionScope,
    /// The explicit `dynamic` or `defer` occurrence that owns this nested body.
    pub(super) kind: ReconfigurableExpressionKind,
    /// Type information for typed graphs; `None` for untyped graphs.
    pub(super) typing: Option<ReconfigurableExpressionTyping>,
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
