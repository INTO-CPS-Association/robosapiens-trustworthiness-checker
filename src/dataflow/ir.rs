use super::environment::{EnvironmentLayout, EnvironmentSlot};
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
        Self {
            graph,
            environment_layout,
            evaluation_mode,
            requires_temporal_commit,
        }
    }

    pub(super) fn has_reconfiguration_points(&self) -> bool {
        graph_has_reconfiguration_points(&self.graph)
    }

    pub(super) fn reconfiguration_points(
        &self,
    ) -> impl Iterator<Item = (NodeId, &BoundRef, DynamicExpressionMode)> {
        self.graph
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(index, op)| match op {
                BoundOp::Dynamic(spec) => Some((NodeId::new(index), &spec.input, spec.mode)),
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

/// Return a conservative semantic identity for one node and every node that precedes it in the
/// bound DAG. This is used only while planning compatible state transfer. Including the complete
/// prefix may reject some safe migrations after unrelated insertions, but it prevents a stateful
/// node from inheriting history when a same-numbered upstream node changed meaning.
pub(super) fn node_semantic_descriptor(
    graph: &BoundEvaluationGraph,
    node: NodeId,
    layout: &EnvironmentLayout,
) -> String {
    let mut prefix = graph.clone();
    prefix.nodes.truncate(node.index() + 1);
    prefix.scalar_signatures.truncate(node.index() + 1);
    prefix
        .recursive_delays
        .retain(|candidate| candidate.index() <= node.index());
    prefix.output = BoundRef::Node(node);
    canonical_graph_descriptor(&prefix, layout)
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
            descriptor.push_str(match spec.mode {
                DynamicExpressionMode::Dynamic => "dynamic",
                DynamicExpressionMode::Defer => "defer",
            });
            descriptor.push_str(",scope=");
            match &spec.scope {
                DynamicExpressionScope::Automatic => descriptor.push_str("automatic"),
                DynamicExpressionScope::Restricted { allowed_variables } => {
                    let mut names = allowed_variables
                        .iter()
                        .map(VarName::name)
                        .collect::<Vec<_>>();
                    names.sort();
                    for name in names {
                        append_identifier(descriptor, name);
                        descriptor.push(',');
                    }
                }
            }
            if let Some(typing) = &spec.typing {
                descriptor.push_str(",type=");
                append_tc_type(descriptor, &typing.expected_type);
                descriptor.push_str(",environment=");
                for (variable, stream_type) in typing.environment.iter() {
                    append_identifier(descriptor, variable.name());
                    descriptor.push(':');
                    append_stream_type(descriptor, stream_type);
                    descriptor.push(',');
                }
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

/// Compare two bound programs by the names of their external variables rather than by
/// replacement-local environment slot numbers. This is the compatibility check used by runtime
/// context transfer when an interface change shifts existing slots.
pub(super) fn graphs_semantically_equal(
    left: &BoundEvaluationGraph,
    left_layout: &EnvironmentLayout,
    right: &BoundEvaluationGraph,
    right_layout: &EnvironmentLayout,
) -> bool {
    left.nodes.len() == right.nodes.len()
        && left.scalar_signatures == right.scalar_signatures
        && left.recursive_delays == right.recursive_delays
        && refs_equal(&left.output, left_layout, &right.output, right_layout)
        && left
            .nodes
            .iter()
            .zip(&right.nodes)
            .all(|(left, right)| ops_equal(left, left_layout, right, right_layout))
}

fn refs_equal(
    left: &BoundRef,
    left_layout: &EnvironmentLayout,
    right: &BoundRef,
    right_layout: &EnvironmentLayout,
) -> bool {
    match (left, right) {
        (DataRef::Const(left), DataRef::Const(right)) => left == right,
        (DataRef::Node(left), DataRef::Node(right)) => left == right,
        (DataRef::External(left), DataRef::External(right)) => {
            left_layout.variable(*left) == right_layout.variable(*right)
        }
        _ => false,
    }
}

fn refs_slice_equal(
    left: &[BoundRef],
    left_layout: &EnvironmentLayout,
    right: &[BoundRef],
    right_layout: &EnvironmentLayout,
) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| refs_equal(left, left_layout, right, right_layout))
}

fn functions_equal(
    left: &StreamFunction,
    left_layout: &EnvironmentLayout,
    right: &StreamFunction,
    right_layout: &EnvironmentLayout,
) -> bool {
    left.parameters == right.parameters
        && left.display == right.display
        && left
            .capture_slots
            .iter()
            .map(|slot| left_layout.variable(*slot))
            .eq(right
                .capture_slots
                .iter()
                .map(|slot| right_layout.variable(*slot)))
        && graphs_semantically_equal(
            &left.program.graph,
            &left.program.environment_layout,
            &right.program.graph,
            &right.program.environment_layout,
        )
}

pub(super) fn ops_equal(
    left: &BoundOp,
    left_layout: &EnvironmentLayout,
    right: &BoundOp,
    right_layout: &EnvironmentLayout,
) -> bool {
    match (left, right) {
        (
            StreamOp::Unary {
                op: left_op,
                arg: left_arg,
            },
            StreamOp::Unary {
                op: right_op,
                arg: right_arg,
            },
        ) => left_op == right_op && refs_equal(left_arg, left_layout, right_arg, right_layout),
        (
            StreamOp::Binary {
                op: left_op,
                lhs: left_lhs,
                rhs: left_rhs,
            },
            StreamOp::Binary {
                op: right_op,
                lhs: right_lhs,
                rhs: right_rhs,
            },
        ) => {
            left_op == right_op
                && refs_equal(left_lhs, left_layout, right_lhs, right_layout)
                && refs_equal(left_rhs, left_layout, right_rhs, right_layout)
        }
        (
            StreamOp::If {
                cond: left_cond,
                then_branch: left_then,
                else_branch: left_else,
            },
            StreamOp::If {
                cond: right_cond,
                then_branch: right_then,
                else_branch: right_else,
            },
        ) => {
            refs_equal(left_cond, left_layout, right_cond, right_layout)
                && graphs_semantically_equal(left_then, left_layout, right_then, right_layout)
                && graphs_semantically_equal(left_else, left_layout, right_else, right_layout)
        }
        (
            StreamOp::Delay {
                input: left_input,
                offset: left_offset,
            },
            StreamOp::Delay {
                input: right_input,
                offset: right_offset,
            },
        ) => {
            left_offset == right_offset
                && refs_equal(left_input, left_layout, right_input, right_layout)
        }
        (StreamOp::RecursiveDelay { offset: left }, StreamOp::RecursiveDelay { offset: right }) => {
            left == right
        }
        (
            StreamOp::Default {
                input: left_input,
                fallback: left_fallback,
            },
            StreamOp::Default {
                input: right_input,
                fallback: right_fallback,
            },
        ) => {
            refs_equal(left_input, left_layout, right_input, right_layout)
                && refs_equal(left_fallback, left_layout, right_fallback, right_layout)
        }
        (
            StreamOp::Init {
                input: left_input,
                initial: left_initial,
            },
            StreamOp::Init {
                input: right_input,
                initial: right_initial,
            },
        ) => {
            refs_equal(left_input, left_layout, right_input, right_layout)
                && refs_equal(left_initial, left_layout, right_initial, right_layout)
        }
        (StreamOp::IsDefined { input: left }, StreamOp::IsDefined { input: right })
        | (StreamOp::When { input: left }, StreamOp::When { input: right }) => {
            refs_equal(left, left_layout, right, right_layout)
        }
        (
            StreamOp::Update {
                base: left_base,
                update: left_update,
            },
            StreamOp::Update {
                base: right_base,
                update: right_update,
            },
        ) => {
            refs_equal(left_base, left_layout, right_base, right_layout)
                && refs_equal(left_update, left_layout, right_update, right_layout)
        }
        (
            StreamOp::Latch {
                value: left_value,
                trigger: left_trigger,
            },
            StreamOp::Latch {
                value: right_value,
                trigger: right_trigger,
            },
        ) => {
            refs_equal(left_value, left_layout, right_value, right_layout)
                && refs_equal(left_trigger, left_layout, right_trigger, right_layout)
        }
        (StreamOp::List(left), StreamOp::List(right))
        | (StreamOp::Tuple(left), StreamOp::Tuple(right)) => {
            refs_slice_equal(left, left_layout, right, right_layout)
        }
        (StreamOp::Map(left), StreamOp::Map(right)) => {
            left.len() == right.len()
                && left.iter().zip(right).all(
                    |((left_key, left_value), (right_key, right_value))| {
                        left_key == right_key
                            && refs_equal(left_value, left_layout, right_value, right_layout)
                    },
                )
        }
        (
            StreamOp::LIndex {
                list: left_list,
                index: left_index,
            },
            StreamOp::LIndex {
                list: right_list,
                index: right_index,
            },
        ) => {
            refs_equal(left_list, left_layout, right_list, right_layout)
                && refs_equal(left_index, left_layout, right_index, right_layout)
        }
        (
            StreamOp::LAppend {
                list: left_list,
                value: left_value,
            },
            StreamOp::LAppend {
                list: right_list,
                value: right_value,
            },
        ) => {
            refs_equal(left_list, left_layout, right_list, right_layout)
                && refs_equal(left_value, left_layout, right_value, right_layout)
        }
        (
            StreamOp::LConcat {
                lhs: left_lhs,
                rhs: left_rhs,
            },
            StreamOp::LConcat {
                lhs: right_lhs,
                rhs: right_rhs,
            },
        ) => {
            refs_equal(left_lhs, left_layout, right_lhs, right_layout)
                && refs_equal(left_rhs, left_layout, right_rhs, right_layout)
        }
        (StreamOp::LHead { list: left }, StreamOp::LHead { list: right })
        | (StreamOp::LTail { list: left }, StreamOp::LTail { list: right })
        | (StreamOp::LLen { list: left }, StreamOp::LLen { list: right }) => {
            refs_equal(left, left_layout, right, right_layout)
        }
        (
            StreamOp::Fix {
                func: left_func,
                display: left_display,
            },
            StreamOp::Fix {
                func: right_func,
                display: right_display,
            },
        ) => {
            left_display == right_display
                && refs_equal(left_func, left_layout, right_func, right_layout)
        }
        (
            StreamOp::MGet {
                map: left,
                key: left_key,
            },
            StreamOp::MGet {
                map: right,
                key: right_key,
            },
        )
        | (
            StreamOp::MRemove {
                map: left,
                key: left_key,
            },
            StreamOp::MRemove {
                map: right,
                key: right_key,
            },
        )
        | (
            StreamOp::MHasKey {
                map: left,
                key: left_key,
            },
            StreamOp::MHasKey {
                map: right,
                key: right_key,
            },
        ) => left_key == right_key && refs_equal(left, left_layout, right, right_layout),
        (
            StreamOp::MInsert {
                map: left_map,
                key: left_key,
                value: left_value,
            },
            StreamOp::MInsert {
                map: right_map,
                key: right_key,
                value: right_value,
            },
        ) => {
            left_key == right_key
                && refs_equal(left_map, left_layout, right_map, right_layout)
                && refs_equal(left_value, left_layout, right_value, right_layout)
        }
        (
            StreamOp::TGet {
                tuple: left,
                index: left_index,
            },
            StreamOp::TGet {
                tuple: right,
                index: right_index,
            },
        ) => left_index == right_index && refs_equal(left, left_layout, right, right_layout),
        (StreamOp::Dynamic(left), StreamOp::Dynamic(right)) => {
            // Scope authorization and the checked environment may grow when a root interface adds an
            // otherwise-unused input. The active body is revalidated against the replacement scope
            // during transfer, so slot-independent continuity is determined by its mode, expected
            // result type, and source owner rather than by the replacement-local scope vector.
            left.mode == right.mode
                && left.typing.as_ref().map(|typing| &typing.expected_type)
                    == right.typing.as_ref().map(|typing| &typing.expected_type)
                && refs_equal(&left.input, left_layout, &right.input, right_layout)
        }
        (StreamOp::Function { func: left }, StreamOp::Function { func: right })
        | (StreamOp::DirectApply { func: left, .. }, StreamOp::DirectApply { func: right, .. })
        | (
            StreamOp::RecursiveApply { func: left, .. },
            StreamOp::RecursiveApply { func: right, .. },
        ) => functions_equal(left, left_layout, right, right_layout),
        (
            StreamOp::Apply {
                func: left_func,
                args: left_args,
            },
            StreamOp::Apply {
                func: right_func,
                args: right_args,
            },
        ) => {
            refs_equal(left_func, left_layout, right_func, right_layout)
                && refs_slice_equal(left_args, left_layout, right_args, right_layout)
        }
        (
            StreamOp::Partial {
                func: left_func,
                args: left_args,
                display: left_display,
            },
            StreamOp::Partial {
                func: right_func,
                args: right_args,
                display: right_display,
            },
        ) => {
            refs_equal(left_func, left_layout, right_func, right_layout)
                && refs_slice_equal(left_args, left_layout, right_args, right_layout)
                && left_display == right_display
        }
        (StreamOp::RecursiveCall { args: left }, StreamOp::RecursiveCall { args: right }) => {
            refs_slice_equal(left, left_layout, right, right_layout)
        }
        (
            StreamOp::ListMap {
                func: left_func,
                list: left_list,
            },
            StreamOp::ListMap {
                func: right_func,
                list: right_list,
            },
        )
        | (
            StreamOp::ListFilter {
                func: left_func,
                list: left_list,
            },
            StreamOp::ListFilter {
                func: right_func,
                list: right_list,
            },
        ) => {
            refs_equal(left_func, left_layout, right_func, right_layout)
                && refs_equal(left_list, left_layout, right_list, right_layout)
        }
        (
            StreamOp::ListFold {
                func: left_func,
                init: left_init,
                list: left_list,
            },
            StreamOp::ListFold {
                func: right_func,
                init: right_init,
                list: right_list,
            },
        ) => {
            refs_equal(left_func, left_layout, right_func, right_layout)
                && refs_equal(left_init, left_layout, right_init, right_layout)
                && refs_equal(left_list, left_layout, right_list, right_layout)
        }
        _ => false,
    }
}

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
