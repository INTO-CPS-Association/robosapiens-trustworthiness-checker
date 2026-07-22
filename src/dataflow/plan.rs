use super::*;
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct EnvironmentId(usize);

impl EnvironmentId {
    pub(super) fn new(index: usize) -> Self {
        Self(index)
    }

    pub(super) fn index(self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DataflowUnaryOp {
    Not,
    Neg,
    Sin,
    Cos,
    Tan,
    Abs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DataflowBinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Or,
    And,
    Impl,
    Concat,
    Eq,
    Le,
    Lt,
    Ge,
    Gt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DataflowDynamicMode {
    Dynamic,
    Defer,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum DataflowDynamicScope {
    Automatic,
    Restricted(EcoVec<VarName>),
}

impl DataflowDynamicScope {
    pub(super) fn from_ast(scope: DynamicExprScope) -> Self {
        match scope {
            DynamicExprScope::Automatic => Self::Automatic,
            DynamicExprScope::Explicit(vars) => Self::Restricted(vars),
        }
    }

    pub(super) fn vars(&self) -> Option<&EcoVec<VarName>> {
        match self {
            Self::Automatic => None,
            Self::Restricted(vars) => Some(vars),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct DataflowFunctionDef<E> {
    pub(super) params: EcoVec<VarName>,
    pub(super) plan: Rc<Plan<E>>,
    pub(super) display: EcoString,
    pub(super) capture_sources: Vec<EnvironmentId>,
}

impl DataflowFunctionDef<VarName> {
    pub(super) fn new(params: EcoVec<VarName>, body: UnboundPlanBody, display: EcoString) -> Self {
        Self {
            params,
            plan: Rc::new(Plan::new(body, Rc::new(EnvironmentLayout::new()))),
            display,
            capture_sources: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct PlanBody<E> {
    pub(super) nodes: Vec<DataflowOp<E>>,
    pub(super) output: DataRef<E>,
    pub(super) recursive_delays: Vec<NodeId>,
}

impl<E> PlanBody<E> {
    pub(super) fn new(nodes: Vec<DataflowOp<E>>, output: DataRef<E>) -> Self {
        Self {
            nodes,
            output,
            recursive_delays: Vec::new(),
        }
    }

    pub(super) fn is_fallible(&self) -> bool {
        self.nodes.iter().any(|op| match op {
            DataflowOp::Dynamic(_) => true,
            DataflowOp::If {
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
                    DataflowOp::If {
                        then_branch,
                        else_branch,
                        ..
                    } => then_branch.has_temporal_state() || else_branch.has_temporal_state(),
                    DataflowOp::DirectApply { func, .. }
                    | DataflowOp::DirectFixApply { func, .. } => {
                        func.plan.body.has_temporal_state()
                    }
                    _ => false,
                }
        })
    }
}

impl BoundPlanBody {
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
                | BoundOp::DirectFixApply { func, .. } => {
                    func.plan
                        .body
                        .debug_assert_valid(func.plan.environment.len());
                }
                _ => {}
            }
        }

        let recursive_nodes = self
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(index, op)| op.is_recursive_sindex().then(|| NodeId::new(index)))
            .collect::<Vec<_>>();
        debug_assert_eq!(self.recursive_delays, recursive_nodes);
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct Plan<E> {
    pub(super) body: PlanBody<E>,
    pub(super) environment: Rc<EnvironmentLayout>,
    pub(super) evaluation: PlanMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PlanMode {
    Static,
    Dynamic,
}

impl<E> Plan<E> {
    pub(super) fn new(body: PlanBody<E>, environment: Rc<EnvironmentLayout>) -> Self {
        let evaluation = if body.is_fallible() {
            PlanMode::Dynamic
        } else {
            PlanMode::Static
        };
        Self {
            body,
            environment,
            evaluation,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum DataRef<E> {
    Const(Value),
    External(E),
    Node(NodeId),
}

pub(super) type UnboundRef = DataRef<VarName>;
pub(super) type BoundRef = DataRef<EnvironmentId>;
pub(super) type UnboundPlanBody = PlanBody<VarName>;
pub(super) type BoundPlanBody = PlanBody<EnvironmentId>;
pub(super) type UnboundOp = DataflowOp<VarName>;
pub(super) type BoundOp = DataflowOp<EnvironmentId>;
pub(super) type UnboundDynamicSpec = DynamicSpec<VarName>;
pub(super) type BoundDynamicSpec = DynamicSpec<EnvironmentId>;
pub(super) type UnboundFunctionDef = DataflowFunctionDef<VarName>;
pub(super) type BoundFunctionDef = DataflowFunctionDef<EnvironmentId>;
pub(super) type ExecutablePlan = Plan<EnvironmentId>;

#[derive(Clone, Debug, PartialEq)]
pub(super) struct DynamicSpec<E> {
    pub(super) input: DataRef<E>,
    pub(super) scope: DataflowDynamicScope,
    pub(super) mode: DataflowDynamicMode,
    /// Type information for typed plans; `None` for untyped plans.
    pub(super) typed: Option<(Rc<StreamTypeEnvironment>, TCType)>,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum DataflowOp<E> {
    Unary {
        op: DataflowUnaryOp,
        arg: DataRef<E>,
    },
    Binary {
        op: DataflowBinaryOp,
        lhs: DataRef<E>,
        rhs: DataRef<E>,
    },
    If {
        cond: DataRef<E>,
        then_branch: PlanBody<E>,
        else_branch: PlanBody<E>,
    },
    SIndex {
        input: DataRef<E>,
        offset: u64,
    },
    RecursiveSIndex {
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
    Dynamic(DynamicSpec<E>),
    Function {
        func: DataflowFunctionDef<E>,
    },
    Apply {
        func: DataRef<E>,
        args: Vec<DataRef<E>>,
    },
    /// A statically known lambda application with one persistent plan instance.
    DirectApply {
        func: DataflowFunctionDef<E>,
        args: Vec<DataRef<E>>,
    },
    DirectFixApply {
        func: DataflowFunctionDef<E>,
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

impl<E> DataflowOp<E> {
    /// Visit every direct operand used by dependency analysis.
    pub(super) fn for_each_operand(&self, mut visit: impl FnMut(&DataRef<E>)) {
        match self {
            DataflowOp::Unary { arg, .. } => visit(arg),
            DataflowOp::Binary { lhs, rhs, .. }
            | DataflowOp::Default {
                input: lhs,
                fallback: rhs,
            }
            | DataflowOp::Init {
                input: lhs,
                initial: rhs,
            }
            | DataflowOp::Update {
                base: lhs,
                update: rhs,
            }
            | DataflowOp::Latch {
                value: lhs,
                trigger: rhs,
            }
            | DataflowOp::LIndex {
                list: lhs,
                index: rhs,
            }
            | DataflowOp::LAppend {
                list: lhs,
                value: rhs,
            }
            | DataflowOp::LConcat { lhs, rhs } => {
                visit(lhs);
                visit(rhs);
            }
            DataflowOp::If { cond, .. } => visit(cond),
            DataflowOp::SIndex { input, .. }
            | DataflowOp::IsDefined { input }
            | DataflowOp::When { input }
            | DataflowOp::LHead { list: input }
            | DataflowOp::LTail { list: input }
            | DataflowOp::LLen { list: input }
            | DataflowOp::MGet { map: input, .. }
            | DataflowOp::MRemove { map: input, .. }
            | DataflowOp::MHasKey { map: input, .. }
            | DataflowOp::TGet { tuple: input, .. }
            | DataflowOp::Fix { func: input, .. } => visit(input),
            DataflowOp::Dynamic(DynamicSpec { input, .. }) => visit(input),
            DataflowOp::List(items) | DataflowOp::Tuple(items) => {
                items.into_iter().for_each(&mut visit)
            }
            DataflowOp::Map(items) => items.into_iter().for_each(|(_, value)| visit(value)),
            DataflowOp::MInsert { map, value, .. }
            | DataflowOp::ListMap {
                func: map,
                list: value,
            }
            | DataflowOp::ListFilter {
                func: map,
                list: value,
            } => {
                visit(map);
                visit(value);
            }
            DataflowOp::ListFold { func, init, list } => {
                visit(func);
                visit(init);
                visit(list);
            }
            DataflowOp::Apply { func, args } | DataflowOp::Partial { func, args, .. } => {
                visit(func);
                args.into_iter().for_each(&mut visit);
            }
            DataflowOp::DirectApply { args, .. }
            | DataflowOp::DirectFixApply { args, .. }
            | DataflowOp::RecursiveCall { args } => args.into_iter().for_each(&mut visit),
            DataflowOp::Function { .. } | DataflowOp::RecursiveSIndex { .. } => {}
        }
    }

    pub(super) fn is_recursive_sindex(&self) -> bool {
        matches!(self, DataflowOp::RecursiveSIndex { .. })
    }

    pub(super) fn temporal_operator_name(&self) -> Option<&'static str> {
        match self {
            Self::SIndex { .. } | Self::RecursiveSIndex { .. } => Some("sindex"),
            Self::Init { .. } => Some("init"),
            Self::When { .. } => Some("when"),
            Self::Update { .. } => Some("update"),
            Self::Latch { .. } => Some("latch"),
            Self::Dynamic(_) => Some("dynamic/defer"),
            _ => None,
        }
    }
}
