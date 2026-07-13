use ecow::{EcoString, EcoVec};

use crate::core::{Specification, StreamTypeAscription, VarName};
use crate::core::{StreamType, Value};
use crate::distributed::distribution_graphs::NodeName;
use std::collections::BTreeSet;
use std::fmt::Error;
use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum RuntimeScope {
    Automatic(Option<VarName>),
    Explicit(EcoVec<VarName>, Option<VarName>),
}

impl RuntimeScope {
    pub fn explicit(vars: EcoVec<VarName>) -> Self {
        Self::Explicit(vars, None)
    }

    pub fn explicit_vars(&self) -> Option<&EcoVec<VarName>> {
        match self {
            Self::Automatic(_) => None,
            Self::Explicit(vars, _) => Some(vars),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &VarName> {
        self.explicit_vars().into_iter().flatten()
    }

    pub fn owner(&self) -> Option<&VarName> {
        match self {
            Self::Automatic(owner) | Self::Explicit(_, owner) => owner.as_ref(),
        }
    }

    pub fn resolve<T>(&self, vars: &BTreeMap<VarName, T>) -> Option<EcoVec<VarName>> {
        match self {
            Self::Automatic(None) => None,
            Self::Automatic(Some(owner)) => {
                Some(vars.keys().filter(|var| *var != owner).cloned().collect())
            }
            Self::Explicit(vars, _) => Some(vars.clone()),
        }
    }
}

impl From<EcoVec<VarName>> for RuntimeScope {
    fn from(vars: EcoVec<VarName>) -> Self {
        Self::Explicit(vars, None)
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct RuntimeExpr<E> {
    pub source: Box<E>,
    pub result_type: StreamTypeAscription,
    pub scope: RuntimeScope,
}

impl<E> RuntimeExpr<E> {
    pub fn automatic(source: Box<E>, result_type: StreamTypeAscription) -> Self {
        Self {
            source,
            result_type,
            scope: RuntimeScope::Automatic(None),
        }
    }

    pub fn explicit(
        source: Box<E>,
        result_type: StreamTypeAscription,
        vars: EcoVec<VarName>,
    ) -> Self {
        Self {
            source,
            result_type,
            scope: RuntimeScope::Explicit(vars, None),
        }
    }
}

impl RuntimeExpr<SpannedExpr> {
    fn fmt_with_name(&self, name: &str, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let result_type = match &self.result_type {
            StreamTypeAscription::Unascribed => String::new(),
            StreamTypeAscription::Ascribed(result_type) => format!(": {result_type}"),
        };
        match &self.scope {
            RuntimeScope::Automatic(_) => write!(f, "{name}({}{result_type})", self.source),
            RuntimeScope::Explicit(vars, _) => {
                let vars = vars
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{name}({}{result_type}, {{{vars}}})", self.source)
            }
        }
    }
}

use crate::lang::dsrv::span::{Span, Spanned};

// Numerical Binary Operations
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum NumericalBinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl Display for NumericalBinOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use NumericalBinOp::*;
        match self {
            Add => write!(f, "+"),
            Sub => write!(f, "-"),
            Mul => write!(f, "*"),
            Div => write!(f, "/"),
            Mod => write!(f, "%"),
        }
    }
}

// Integer Binary Operations
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum IntBinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl TryFrom<NumericalBinOp> for IntBinOp {
    type Error = ();

    fn try_from(op: NumericalBinOp) -> Result<IntBinOp, ()> {
        match op {
            NumericalBinOp::Add => Ok(IntBinOp::Add),
            NumericalBinOp::Sub => Ok(IntBinOp::Sub),
            NumericalBinOp::Mul => Ok(IntBinOp::Mul),
            NumericalBinOp::Div => Ok(IntBinOp::Div),
            NumericalBinOp::Mod => Ok(IntBinOp::Mod),
        }
    }
}

// Floating point binary operations
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum FloatBinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl TryFrom<NumericalBinOp> for FloatBinOp {
    type Error = ();

    fn try_from(op: NumericalBinOp) -> Result<FloatBinOp, ()> {
        match op {
            NumericalBinOp::Add => Ok(FloatBinOp::Add),
            NumericalBinOp::Sub => Ok(FloatBinOp::Sub),
            NumericalBinOp::Mul => Ok(FloatBinOp::Mul),
            NumericalBinOp::Div => Ok(FloatBinOp::Div),
            NumericalBinOp::Mod => Ok(FloatBinOp::Mod),
        }
    }
}

// Bool Binary Operations
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum BoolBinOp {
    Or,
    And,
    Impl, // Implication
}

impl Display for BoolBinOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use BoolBinOp::*;
        match self {
            Or => write!(f, "||"),
            And => write!(f, "&&"),
            Impl => write!(f, "=>"),
        }
    }
}

// Str Binary Operations
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum StrBinOp {
    Concat,
}

impl Display for StrBinOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StrBinOp::Concat => write!(f, "++"),
        }
    }
}

// Comparison Binary Operations
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum CompBinOp {
    Eq,
    Le,
    Ge,
    Lt,
    Gt,
}

impl Display for CompBinOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use CompBinOp::*;
        match self {
            Eq => write!(f, "=="),
            Le => write!(f, "<="),
            Ge => write!(f, ">="),
            Lt => write!(f, "<"),
            Gt => write!(f, ">"),
        }
    }
}

// Stream BinOp
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum SBinOp {
    NOp(NumericalBinOp),
    BOp(BoolBinOp),
    SOp(StrBinOp),
    COp(CompBinOp),
}

impl Display for SBinOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SBinOp::NOp(op) => write!(f, "{op}"),
            SBinOp::BOp(op) => write!(f, "{op}"),
            SBinOp::SOp(op) => write!(f, "{op}"),
            SBinOp::COp(op) => write!(f, "{op}"),
        }
    }
}

// Helper function to specify binary operations from a string
impl From<&str> for SBinOp {
    fn from(s: &str) -> Self {
        match s {
            "+" => SBinOp::NOp(NumericalBinOp::Add),
            "-" => SBinOp::NOp(NumericalBinOp::Sub),
            "*" => SBinOp::NOp(NumericalBinOp::Mul),
            "/" => SBinOp::NOp(NumericalBinOp::Div),
            "||" => SBinOp::BOp(BoolBinOp::Or),
            "&&" => SBinOp::BOp(BoolBinOp::And),
            "++" => SBinOp::SOp(StrBinOp::Concat),
            "==" => SBinOp::COp(CompBinOp::Eq),
            "<=" => SBinOp::COp(CompBinOp::Le),
            _ => panic!("Invalid binary operation: {}", s),
        }
    }
}

#[derive(Clone, PartialEq, Debug, serde::Serialize)]
pub struct VarOrNodeName(pub String);

impl Display for VarOrNodeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Into<VarName> for VarOrNodeName {
    fn into(self) -> VarName {
        self.0.into()
    }
}

impl Into<NodeName> for VarOrNodeName {
    fn into(self) -> NodeName {
        self.0.into()
    }
}

impl Into<String> for VarOrNodeName {
    fn into(self) -> String {
        self.0
    }
}

pub type SpannedExpr = Spanned<SExpr>;

#[derive(Clone, PartialEq, Debug, serde::Serialize)]
pub enum SExpr {
    // if-then-else
    If(Box<SpannedExpr>, Box<SpannedExpr>, Box<SpannedExpr>),

    // Stream indexing
    SIndex(
        // Inner SExpr e
        Box<SpannedExpr>,
        // Index i
        u64,
    ),

    // Arithmetic Stream expression
    Val(Value),

    BinOp(Box<SpannedExpr>, Box<SpannedExpr>, SBinOp),

    Var(VarName),

    // Dynamic, continuously updatable properties
    Dynamic(RuntimeExpr<SpannedExpr>),
    // Deferred properties
    Defer(RuntimeExpr<SpannedExpr>),
    // Update between properties
    Update(Box<SpannedExpr>, Box<SpannedExpr>),
    // Default value for properties (replaces Deferred with an alternative
    // stream)
    Default(Box<SpannedExpr>, Box<SpannedExpr>),
    IsDefined(Box<SpannedExpr>), // True when .0 is not Deferred
    When(Box<SpannedExpr>),      // Becomes true after the first time .0 is not Deferred

    // Asynchronous operations
    Latch(Box<SpannedExpr>, Box<SpannedExpr>),
    Init(Box<SpannedExpr>, Box<SpannedExpr>),

    // Unary expressions (refactor if more are added...)
    Not(Box<SpannedExpr>),

    // First-class function expressions
    Lambda(EcoVec<(VarName, StreamType)>, Box<SpannedExpr>),
    Apply(Box<SpannedExpr>, EcoVec<SpannedExpr>),
    Fix(Box<SpannedExpr>),
    Partial(Box<SpannedExpr>, EcoVec<SpannedExpr>),

    // List and list expressions
    List(EcoVec<SpannedExpr>),
    Tuple(EcoVec<SpannedExpr>),
    LIndex(Box<SpannedExpr>, Box<SpannedExpr>), // List index: First is list, second is index
    LAppend(Box<SpannedExpr>, Box<SpannedExpr>), // List append -- First is list, second is el to add
    LConcat(Box<SpannedExpr>, Box<SpannedExpr>), // List concat -- First is list, second is other list
    LHead(Box<SpannedExpr>),                     // List head -- get first element of list
    LTail(Box<SpannedExpr>),                     // List tail -- get all but first element of list
    LLen(Box<SpannedExpr>),                      // List length -- returns length of the list
    LMap(Box<SpannedExpr>, Box<SpannedExpr>),
    LFilter(Box<SpannedExpr>, Box<SpannedExpr>),
    LFold(Box<SpannedExpr>, Box<SpannedExpr>, Box<SpannedExpr>),

    // Map and struct expressions
    Map(BTreeMap<EcoString, SpannedExpr>), // Map from String to SExpr
    Struct(BTreeMap<EcoString, SpannedExpr>), // Struct record from field name to SExpr
    ObjectLiteral(BTreeMap<EcoString, SpannedExpr>), // JSON-style object literal resolved by expected type
    MGet(Box<SpannedExpr>, EcoString),               // Get from map or struct
    SGet(Box<SpannedExpr>, EcoString),               // Dot field access for typed structs only
    MInsert(Box<SpannedExpr>, EcoString, Box<SpannedExpr>), // Insert into map -- First is map, second is key, third is value
    MRemove(Box<SpannedExpr>, EcoString), // Remove from map -- First is map, second is key
    MHasKey(Box<SpannedExpr>, EcoString), // Check if map has key -- First is map, second is key

    // Trigonometric functions
    Sin(Box<SpannedExpr>),
    Cos(Box<SpannedExpr>),
    Tan(Box<SpannedExpr>),

    // Other math functions
    Abs(Box<SpannedExpr>),

    // Distribution Constraint Specific
    MonitoredAt(VarName, NodeName),
    Dist(VarOrNodeName, VarOrNodeName),
}

#[derive(Clone, PartialEq, Debug, serde::Serialize)]
pub enum STopDecl {
    Input(VarName, Option<StreamType>, Span),
    Output(VarName, Option<StreamType>, Span),
    Aux(VarName, Option<StreamType>, Span),
    Assignment(VarName, SpannedExpr, Span),
}

impl SExpr {
    pub fn inputs(&self) -> Vec<VarName> {
        use SExpr::*;
        match self {
            If(b, e1, e2) => {
                let mut inputs = b.inputs();
                inputs.extend(e1.inputs());
                inputs.extend(e2.inputs());
                inputs
            }
            SIndex(s, _) => s.inputs(),
            Val(_) => vec![],
            BinOp(e1, e2, _) => {
                let mut inputs = e1.inputs();
                inputs.extend(e2.inputs());
                inputs
            }
            Var(v) => vec![v.clone()],
            Not(b) => b.inputs(),
            Lambda(_, body) => body.inputs(),
            Apply(func, args) => {
                let mut inputs = func.inputs();
                for arg in args {
                    inputs.extend(arg.inputs());
                }
                inputs
            }
            Fix(func) => func.inputs(),
            Partial(func, args) => {
                let mut inputs = func.inputs();
                for arg in args {
                    inputs.extend(arg.inputs());
                }
                inputs
            }
            Dynamic(runtime) | Defer(runtime) => runtime.source.inputs(),
            Update(e1, e2) => {
                let mut inputs = e1.inputs();
                inputs.extend(e2.inputs());
                inputs
            }
            Default(e1, e2) => {
                let mut inputs = e1.inputs();
                inputs.extend(e2.inputs());
                inputs
            }
            IsDefined(e) => e.inputs(),
            When(e) => e.inputs(),
            Latch(e1, e2) => {
                let mut inputs = e1.inputs();
                inputs.extend(e2.inputs());
                inputs
            }
            Init(e1, e2) => {
                let mut inputs = e1.inputs();
                inputs.extend(e2.inputs());
                inputs
            }
            List(es) | Tuple(es) => {
                let mut inputs = vec![];
                for e in es {
                    inputs.extend(e.inputs());
                }
                inputs
            }
            Map(map) | Struct(map) | ObjectLiteral(map) => {
                let mut inputs = vec![];
                for (_, e) in map {
                    inputs.extend(e.inputs());
                }
                inputs
            }
            LIndex(e1, e2) | LAppend(e1, e2) | LConcat(e1, e2) | LMap(e1, e2) | LFilter(e1, e2) => {
                let mut inputs = e1.inputs();
                inputs.extend(e2.inputs());
                inputs
            }
            LFold(func, init, list) => {
                let mut inputs = func.inputs();
                inputs.extend(init.inputs());
                inputs.extend(list.inputs());
                inputs
            }
            MGet(e, _)
            | SGet(e, _)
            | MInsert(e, _, _)
            | MRemove(e, _)
            | MHasKey(e, _)
            | LHead(e)
            | LTail(e)
            | LLen(e)
            | Sin(e)
            | Cos(e)
            | Tan(e)
            | Abs(e) => e.inputs(),
            MonitoredAt(_, _) => {
                vec![]
            }
            Dist(_, _) => {
                vec![]
            }
        }
    }
}

/// An untypechecked/untyped DSRV specification
///
/// Note that this may contain unchecked type annotations, which later by used by the gradual
/// or strict type system, which the specification is type-checked
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct UntypedDsrvSpecification {
    pub input_vars: BTreeSet<VarName>,
    pub output_vars: BTreeSet<VarName>,
    pub aux_vars: BTreeSet<VarName>,
    pub stream_vars: BTreeSet<VarName>,
    pub exprs: BTreeMap<VarName, SpannedExpr>,
    pub type_annotations: BTreeMap<VarName, StreamType>,
}

impl UntypedDsrvSpecification {
    /// Records which stream contains each runtime expression without rewriting
    /// its declared scope. Scope resolution and cycle checks belong to semantic
    /// validation, where the complete dependency graph is available.
    fn annotate_runtime_owners(
        _input_vars: &BTreeSet<VarName>,
        _output_vars: &BTreeSet<VarName>,
        exprs: &BTreeMap<VarName, SpannedExpr>,
    ) -> BTreeMap<VarName, SpannedExpr> {
        fn traverse_expr(expr: SpannedExpr, owner: &VarName) -> SpannedExpr {
            let span = expr.span; // Store the original span to reuse in the output expression
            let new_kind = match expr.node {
                SExpr::Dynamic(mut runtime) => {
                    runtime.source = Box::new(traverse_expr(*runtime.source, owner));
                    if matches!(runtime.scope, RuntimeScope::Automatic(_)) {
                        runtime.scope = RuntimeScope::Automatic(Some(owner.clone()));
                    } else if let RuntimeScope::Explicit(vars, _) = runtime.scope {
                        runtime.scope = RuntimeScope::Explicit(vars, Some(owner.clone()));
                    }
                    SExpr::Dynamic(runtime)
                }
                SExpr::Defer(mut runtime) => {
                    runtime.source = Box::new(traverse_expr(*runtime.source, owner));
                    if matches!(runtime.scope, RuntimeScope::Automatic(_)) {
                        runtime.scope = RuntimeScope::Automatic(Some(owner.clone()));
                    } else if let RuntimeScope::Explicit(vars, _) = runtime.scope {
                        runtime.scope = RuntimeScope::Explicit(vars, Some(owner.clone()));
                    }
                    SExpr::Defer(runtime)
                }
                SExpr::Var(v) => SExpr::Var(v.clone()),
                SExpr::Val(v) => SExpr::Val(v.clone()),
                SExpr::When(sexpr) => SExpr::When(Box::new(traverse_expr(*sexpr, owner))),
                SExpr::Not(sexpr) => SExpr::Not(Box::new(traverse_expr(*sexpr, owner))),
                SExpr::Lambda(params, body) => {
                    SExpr::Lambda(params, Box::new(traverse_expr(*body, owner)))
                }
                SExpr::Apply(func, args) => {
                    let new_args = args
                        .into_iter()
                        .map(|arg| traverse_expr(arg, owner))
                        .collect();
                    SExpr::Apply(Box::new(traverse_expr(*func, owner)), new_args)
                }
                SExpr::Fix(func) => SExpr::Fix(Box::new(traverse_expr(*func, owner))),
                SExpr::Partial(func, args) => {
                    let new_args = args
                        .into_iter()
                        .map(|arg| traverse_expr(arg, owner))
                        .collect();
                    SExpr::Partial(Box::new(traverse_expr(*func, owner)), new_args)
                }
                SExpr::SIndex(sexpr, i) => SExpr::SIndex(Box::new(traverse_expr(*sexpr, owner)), i),
                SExpr::Sin(sexpr) => SExpr::Sin(Box::new(traverse_expr(*sexpr, owner))),
                SExpr::Cos(sexpr) => SExpr::Cos(Box::new(traverse_expr(*sexpr, owner))),
                SExpr::Tan(sexpr) => SExpr::Tan(Box::new(traverse_expr(*sexpr, owner))),
                SExpr::Abs(sexpr) => SExpr::Abs(Box::new(traverse_expr(*sexpr, owner))),
                SExpr::MonitoredAt(v, n) => SExpr::MonitoredAt(v, n),
                SExpr::Dist(v, u) => SExpr::Dist(v, u),
                SExpr::LTail(sexpr) => SExpr::LTail(Box::new(traverse_expr(*sexpr, owner))),
                SExpr::LHead(sexpr) => SExpr::LHead(Box::new(traverse_expr(*sexpr, owner))),
                SExpr::LLen(sexpr) => SExpr::LLen(Box::new(traverse_expr(*sexpr, owner))),
                SExpr::IsDefined(sexpr) => SExpr::IsDefined(Box::new(traverse_expr(*sexpr, owner))),
                SExpr::BinOp(sexpr, sexpr1, sbin_op) => SExpr::BinOp(
                    Box::new(traverse_expr(*sexpr, owner)),
                    Box::new(traverse_expr(*sexpr1, owner)),
                    sbin_op.clone(),
                ),
                SExpr::Latch(sexpr1, sexpr2) => SExpr::Latch(
                    Box::new(traverse_expr(*sexpr1, owner)),
                    Box::new(traverse_expr(*sexpr2, owner)),
                ),
                SExpr::Init(sexpr1, sexpr2) => SExpr::Init(
                    Box::new(traverse_expr(*sexpr1, owner)),
                    Box::new(traverse_expr(*sexpr2, owner)),
                ),
                SExpr::LIndex(sexpr, sexpr1) => SExpr::LIndex(
                    Box::new(traverse_expr(*sexpr, owner)),
                    Box::new(traverse_expr(*sexpr1, owner)),
                ),
                SExpr::LAppend(sexpr, sexpr1) => SExpr::LAppend(
                    Box::new(traverse_expr(*sexpr, owner)),
                    Box::new(traverse_expr(*sexpr1, owner)),
                ),
                SExpr::LConcat(sexpr, sexpr1) => SExpr::LConcat(
                    Box::new(traverse_expr(*sexpr, owner)),
                    Box::new(traverse_expr(*sexpr1, owner)),
                ),
                SExpr::LMap(func, list) => SExpr::LMap(
                    Box::new(traverse_expr(*func, owner)),
                    Box::new(traverse_expr(*list, owner)),
                ),
                SExpr::LFilter(func, list) => SExpr::LFilter(
                    Box::new(traverse_expr(*func, owner)),
                    Box::new(traverse_expr(*list, owner)),
                ),
                SExpr::LFold(func, init, list) => SExpr::LFold(
                    Box::new(traverse_expr(*func, owner)),
                    Box::new(traverse_expr(*init, owner)),
                    Box::new(traverse_expr(*list, owner)),
                ),
                SExpr::Update(sexpr, sexpr1) => SExpr::Update(
                    Box::new(traverse_expr(*sexpr, owner)),
                    Box::new(traverse_expr(*sexpr1, owner)),
                ),
                SExpr::Default(sexpr, sexpr1) => SExpr::Default(
                    Box::new(traverse_expr(*sexpr, owner)),
                    Box::new(traverse_expr(*sexpr1, owner)),
                ),
                SExpr::If(sexpr, sexpr1, sexpr2) => SExpr::If(
                    Box::new(traverse_expr(*sexpr, owner)),
                    Box::new(traverse_expr(*sexpr1, owner)),
                    Box::new(traverse_expr(*sexpr2, owner)),
                ),
                SExpr::List(vec) => {
                    //Added recursive traversal for lists.
                    let new_vec = vec
                        .into_iter()
                        .map(|sexpr| traverse_expr(sexpr, owner))
                        .collect();

                    SExpr::List(new_vec)
                }
                SExpr::Tuple(vec) => {
                    let new_vec = vec
                        .into_iter()
                        .map(|sexpr| traverse_expr(sexpr, owner))
                        .collect();

                    SExpr::Tuple(new_vec)
                }
                SExpr::Map(map) => {
                    let new_map = map
                        .into_iter()
                        .map(|(k, v)| (k, traverse_expr(v, owner)))
                        .collect();
                    SExpr::Map(new_map)
                }
                SExpr::Struct(map) => {
                    let map = map
                        .into_iter()
                        .map(|(key, value)| (key, traverse_expr(value, owner)))
                        .collect();
                    SExpr::Struct(map)
                }
                SExpr::ObjectLiteral(map) => {
                    let map = map
                        .into_iter()
                        .map(|(key, value)| (key, traverse_expr(value, owner)))
                        .collect();
                    SExpr::ObjectLiteral(map)
                }
                SExpr::MGet(map, k) => SExpr::MGet(Box::new(traverse_expr(*map.clone(), owner)), k),
                SExpr::SGet(st, k) => SExpr::SGet(Box::new(traverse_expr(*st.clone(), owner)), k),
                SExpr::MInsert(map, k, v) => SExpr::MInsert(
                    Box::new(traverse_expr(*map, owner)),
                    k,
                    Box::new(traverse_expr(*v, owner)),
                ),
                SExpr::MRemove(map, k) => SExpr::MRemove(Box::new(traverse_expr(*map, owner)), k),
                SExpr::MHasKey(map, k) => SExpr::MHasKey(Box::new(traverse_expr(*map, owner)), k),
            };
            Spanned {
                node: new_kind,
                span,
            }
        }

        exprs
            .iter()
            .map(|(name, expr)| (name.clone(), traverse_expr(expr.clone(), name)))
            .collect()
    }

    pub fn exprs(&self) -> BTreeMap<VarName, SpannedExpr> {
        self.exprs.clone()
    }

    pub fn new<Expr>(
        input_vars: BTreeSet<VarName>,
        output_vars: BTreeSet<VarName>,
        exprs: BTreeMap<VarName, Expr>,
        type_annotations: BTreeMap<VarName, StreamType>,
        aux_vars: impl IntoIterator<Item = VarName>,
    ) -> Self
    where
        Expr: Into<SpannedExpr>,
    {
        let aux_vars = aux_vars.into_iter().collect::<BTreeSet<_>>();
        let stream_vars = output_vars
            .iter()
            .cloned()
            .chain(aux_vars.iter().cloned())
            .collect();
        let exprs = exprs
            .into_iter()
            .map(|(name, expr)| (name, expr.into()))
            .collect();
        let exprs = Self::annotate_runtime_owners(&input_vars, &stream_vars, &exprs);
        UntypedDsrvSpecification {
            input_vars,
            output_vars,
            aux_vars,
            stream_vars,
            exprs,
            type_annotations,
        }
    }
}

impl Specification for UntypedDsrvSpecification {
    type Expr = SpannedExpr;

    fn input_vars(&self) -> BTreeSet<VarName> {
        self.input_vars.clone()
    }

    fn output_vars(&self) -> BTreeSet<VarName> {
        self.output_vars
            .difference(&self.aux_vars())
            .cloned()
            .collect()
    }

    fn aux_vars(&self) -> BTreeSet<VarName> {
        self.aux_vars.clone()
    }

    fn stream_vars(&self) -> BTreeSet<VarName> {
        self.stream_vars.clone()
    }

    fn var_expr(&self, var: &VarName) -> Option<SpannedExpr> {
        self.exprs.get(var).cloned()
    }

    fn add_input_var(&mut self, var: VarName) {
        let input_vars = self
            .input_vars
            .iter()
            .cloned()
            .chain(std::iter::once(var))
            .collect();
        // Call new so we make sure that fix_dynamic is also called
        *self = UntypedDsrvSpecification::new(
            input_vars,
            self.output_vars.clone(),
            self.exprs.clone(),
            self.type_annotations.clone(),
            self.aux_vars.clone(),
        );
    }

    fn type_annotations(&self) -> BTreeMap<VarName, StreamType> {
        self.type_annotations.clone()
    }
}

impl Display for SpannedExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use SBinOp::*;
        use SExpr::*;
        match &self.node {
            If(b, e1, e2) => write!(f, "(if {} then {} else {})", b, e1, e2),
            SIndex(s, i) => write!(f, "{}[{}]", s, i),
            Val(n) => write!(f, "{}", n),
            BinOp(e1, e2, NOp(NumericalBinOp::Add)) => write!(f, "({} + {})", e1, e2),
            BinOp(e1, e2, NOp(NumericalBinOp::Sub)) => write!(f, "({} - {})", e1, e2),
            BinOp(e1, e2, NOp(NumericalBinOp::Mul)) => write!(f, "({} * {})", e1, e2),
            BinOp(e1, e2, NOp(NumericalBinOp::Div)) => write!(f, "({} / {})", e1, e2),
            BinOp(e1, e2, NOp(NumericalBinOp::Mod)) => write!(f, "({} % {})", e1, e2),
            BinOp(e1, e2, BOp(BoolBinOp::Or)) => write!(f, "({} || {})", e1, e2),
            BinOp(e1, e2, BOp(BoolBinOp::And)) => write!(f, "({} && {})", e1, e2),
            BinOp(e1, e2, BOp(BoolBinOp::Impl)) => write!(f, "({} => {})", e1, e2),
            BinOp(e1, e2, SOp(StrBinOp::Concat)) => write!(f, "({} ++ {})", e1, e2),
            BinOp(e1, e2, COp(CompBinOp::Eq)) => write!(f, "({} == {})", e1, e2),
            BinOp(e1, e2, COp(CompBinOp::Le)) => write!(f, "({} <= {})", e1, e2),
            BinOp(e1, e2, COp(CompBinOp::Lt)) => write!(f, "({} < {})", e1, e2),
            BinOp(e1, e2, COp(CompBinOp::Ge)) => write!(f, "({} >= {})", e1, e2),
            BinOp(e1, e2, COp(CompBinOp::Gt)) => write!(f, "({} > {})", e1, e2),
            Not(b) => write!(f, "!{}", b),
            Lambda(params, body) => {
                let params = params
                    .iter()
                    .map(|(name, typ)| format!("{}: {}", name, typ))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "\\{} -> {}", params, body)
            }
            Apply(func, args) => {
                let args = args
                    .iter()
                    .map(|arg| format!("{}", arg))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{}({})", func, args)
            }
            Fix(func) => write!(f, "fix({})", func),
            Partial(func, args) => {
                let args = args
                    .iter()
                    .map(|arg| format!("{}", arg))
                    .collect::<Vec<_>>()
                    .join(", ");
                if args.is_empty() {
                    write!(f, "partial({})", func)
                } else {
                    write!(f, "partial({}, {})", func, args)
                }
            }
            Var(v) => write!(f, "{}", v),
            MonitoredAt(u, v) => {
                write!(f, "monitored_at({}, {})", u, v)
            }
            Dist(u, v) => {
                write!(f, "dist({}, {})", u, v)
            }
            Dynamic(runtime) => runtime.fmt_with_name("dynamic", f),
            Defer(runtime) => runtime.fmt_with_name("defer", f),
            Update(e1, e2) => write!(f, "update({}, {})", e1, e2),
            Default(e, v) => write!(f, "default({}, {})", e, v),
            IsDefined(sexpr) => write!(f, "is_defined({})", sexpr),
            When(sexpr) => write!(f, "when({})", sexpr),
            Latch(e1, e2) => write!(f, "latch({}, {})", e1, e2),
            Init(e1, e2) => write!(f, "init({}, {})", e1, e2),
            List(es) => {
                let es_str: Vec<String> = es.iter().map(|e| format!("{}", e)).collect();
                write!(f, "[{}]", es_str.join(", "))
            }
            Tuple(es) => {
                let es_str: Vec<String> = es.iter().map(|e| format!("{}", e)).collect();
                write!(f, "Tuple({})", es_str.join(", "))
            }
            LIndex(e, i) => write!(f, "List.get({}, {})", e, i),
            LAppend(lst, el) => write!(f, "List.append({}, {})", lst, el),
            LConcat(lst1, lst2) => write!(f, "List.concat({}, {})", lst1, lst2),
            LHead(lst) => write!(f, "List.head({})", lst),
            LTail(lst) => write!(f, "List.tail({})", lst),
            LLen(lst) => write!(f, "List.len({})", lst),
            LMap(func, lst) => write!(f, "List.map({}, {})", func, lst),
            LFilter(func, lst) => write!(f, "List.filter({}, {})", func, lst),
            LFold(func, init, lst) => write!(f, "List.fold({}, {}, {})", func, init, lst),
            Map(map) => {
                let map_str: Vec<String> =
                    map.iter().map(|(k, v)| format!("{:?}: {}", k, v)).collect();
                write!(f, "Map({})", map_str.join(", "))
            }
            Struct(map) => {
                let map_str: Vec<String> =
                    map.iter().map(|(k, v)| format!("{:?}: {}", k, v)).collect();
                write!(f, "Struct({})", map_str.join(", "))
            }
            ObjectLiteral(map) => {
                let map_str: Vec<String> =
                    map.iter().map(|(k, v)| format!("{:?}: {}", k, v)).collect();
                write!(f, "{{{}}}", map_str.join(", "))
            }
            MGet(map, k) => write!(f, "Map.get({}, {:?})", map, k),
            SGet(st, k) => write!(f, "{}.{}", st, k),
            MInsert(map, k, v) => write!(f, "Map.insert({}, {:?}, {})", map, k, v),
            MRemove(map, k) => write!(f, "Map.remove({}, {:?})", map, k),
            MHasKey(map, k) => write!(f, "Map.has_key({}, {:?})", map, k),
            Sin(v) => write!(f, "sin({})", v),
            Cos(v) => write!(f, "cos({})", v),
            Tan(v) => write!(f, "tan({})", v),
            Abs(v) => write!(f, "abs({})", v),
        }
    }
}

impl Display for UntypedDsrvSpecification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let aux_vars = self.aux_vars();
        let out_vars = self.output_vars().into_iter().collect::<Vec<VarName>>();
        if self.type_annotations.is_empty() {
            for v in self.input_vars.iter() {
                writeln!(f, "in {}", v)?;
            }
            for v in out_vars.iter() {
                writeln!(f, "out {}", v)?;
            }
            for v in aux_vars.iter() {
                writeln!(f, "aux {}", v)?;
            }
            for (v, e) in self.exprs.iter() {
                writeln!(f, "{} = {}", v, SpannedExpr::from(e.clone()))?;
            }
        } else {
            for v in self.input_vars.iter() {
                let typ = self.type_annotations.get(v).ok_or(Error)?;
                writeln!(f, "in {}: {}", v, typ)?;
            }
            for v in out_vars.iter() {
                let typ = self.type_annotations.get(v).ok_or(Error)?;
                writeln!(f, "out {}: {}", v, typ)?;
            }
            for v in aux_vars.iter() {
                let typ = self.type_annotations.get(v).ok_or(Error)?;
                writeln!(f, "aux {}: {}", v, typ)?;
            }
            for (v, e) in self.exprs.iter() {
                writeln!(f, "{} = {}", v, SpannedExpr::from(e.clone()))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod generation {
    use super::*;

    use proptest::prelude::*;

    use crate::{
        UntypedDsrvSpecification, VarName,
        lang::dsrv::ast::{BoolBinOp, NumericalBinOp, SBinOp, SpannedExpr, StrBinOp},
    };
    type SExpr = SpannedExpr;
    // Mixed type expressions. Note that these are not fully recursively mixed-type as we switch to
    // single type expressions within the individual branches of the mixed type expression
    pub fn arb_mixed_sexpr(vars: Vec<VarName>) -> impl Strategy<Value = SExpr> {
        let bool_leaf = prop_oneof![
            any::<bool>().prop_map(SExpr::Val),
            proptest::sample::select(vars.clone()).prop_map(|x| SExpr::Var(x.clone())),
        ];

        let int_cmp = prop_oneof![
            (arb_int_sexpr(vars.clone()), arb_int_sexpr(vars.clone())).prop_map(|(a, b)| {
                SExpr::BinOp(Box::new(a), Box::new(b), SBinOp::COp(CompBinOp::Eq))
            }),
            (arb_int_sexpr(vars.clone()), arb_int_sexpr(vars.clone())).prop_map(|(a, b)| {
                SExpr::BinOp(Box::new(a), Box::new(b), SBinOp::COp(CompBinOp::Le))
            }),
            (arb_int_sexpr(vars.clone()), arb_int_sexpr(vars.clone())).prop_map(|(a, b)| {
                SExpr::BinOp(Box::new(a), Box::new(b), SBinOp::COp(CompBinOp::Lt))
            }),
            (arb_int_sexpr(vars.clone()), arb_int_sexpr(vars.clone())).prop_map(|(a, b)| {
                SExpr::BinOp(Box::new(a), Box::new(b), SBinOp::COp(CompBinOp::Ge))
            }),
            (arb_int_sexpr(vars.clone()), arb_int_sexpr(vars.clone())).prop_map(|(a, b)| {
                SExpr::BinOp(Box::new(a), Box::new(b), SBinOp::COp(CompBinOp::Gt))
            }),
        ];

        let float_cmp = prop_oneof![
            (arb_float_sexpr(vars.clone()), arb_float_sexpr(vars.clone())).prop_map(|(a, b)| {
                SExpr::BinOp(Box::new(a), Box::new(b), SBinOp::COp(CompBinOp::Eq))
            }),
            (arb_float_sexpr(vars.clone()), arb_float_sexpr(vars.clone())).prop_map(|(a, b)| {
                SExpr::BinOp(Box::new(a), Box::new(b), SBinOp::COp(CompBinOp::Le))
            }),
            (arb_float_sexpr(vars.clone()), arb_float_sexpr(vars.clone())).prop_map(|(a, b)| {
                SExpr::BinOp(Box::new(a), Box::new(b), SBinOp::COp(CompBinOp::Lt))
            }),
            (arb_float_sexpr(vars.clone()), arb_float_sexpr(vars.clone())).prop_map(|(a, b)| {
                SExpr::BinOp(Box::new(a), Box::new(b), SBinOp::COp(CompBinOp::Ge))
            }),
            (arb_float_sexpr(vars.clone()), arb_float_sexpr(vars.clone())).prop_map(|(a, b)| {
                SExpr::BinOp(Box::new(a), Box::new(b), SBinOp::COp(CompBinOp::Gt))
            }),
        ];

        let string_cmp = prop_oneof![
            (
                arb_string_sexpr(vars.clone()),
                arb_string_sexpr(vars.clone())
            )
                .prop_map(|(a, b)| {
                    SExpr::BinOp(Box::new(a), Box::new(b), SBinOp::COp(CompBinOp::Eq))
                }),
        ];

        let comparison_leaf = prop_oneof![int_cmp, float_cmp, string_cmp];

        prop_oneof![bool_leaf, comparison_leaf].prop_recursive(5, 50, 10, |inner| {
            prop_oneof![
                (inner.clone(), inner.clone()).prop_map(|(a, b)| SExpr::BinOp(
                    Box::new(a),
                    Box::new(b),
                    SBinOp::BOp(BoolBinOp::Or)
                )),
                (inner.clone(), inner.clone()).prop_map(|(a, b)| SExpr::BinOp(
                    Box::new(a),
                    Box::new(b),
                    SBinOp::BOp(BoolBinOp::And)
                )),
                (inner.clone(), inner.clone()).prop_map(|(a, b)| SExpr::BinOp(
                    Box::new(a),
                    Box::new(b),
                    SBinOp::BOp(BoolBinOp::Impl)
                )),
                (inner.clone(), inner.clone(), inner.clone()).prop_map(|(c, t, e)| SExpr::If(
                    Box::new(c),
                    Box::new(t),
                    Box::new(e),
                )),
                inner.clone().prop_map(|a| SExpr::Not(Box::new(a))),
            ]
        })
    }

    pub fn arb_boolean_sexpr(vars: Vec<VarName>) -> impl Strategy<Value = SExpr> {
        let leaf = prop_oneof![
            any::<bool>().prop_map(|x| SExpr::Val(x)),
            proptest::sample::select(vars.clone()).prop_map(|x| SExpr::Var(x.clone())),
        ];
        leaf.prop_recursive(5, 50, 10, |inner| {
            prop_oneof![
                (inner.clone(), inner.clone()).prop_map(|(a, b)| SExpr::BinOp(
                    Box::new(a),
                    Box::new(b),
                    SBinOp::BOp(BoolBinOp::Or)
                )),
                (inner.clone(), inner.clone()).prop_map(|(a, b)| SExpr::BinOp(
                    Box::new(a),
                    Box::new(b),
                    SBinOp::BOp(BoolBinOp::And)
                )),
                (inner.clone(), inner.clone()).prop_map(|(a, b)| SExpr::BinOp(
                    Box::new(a),
                    Box::new(b),
                    SBinOp::BOp(BoolBinOp::And)
                )),
            ]
        })
    }

    pub fn arb_int_sexpr(vars: Vec<VarName>) -> impl Strategy<Value = SExpr> {
        let leaf = prop_oneof![
            any::<i64>().prop_map(SExpr::Val),
            proptest::sample::select(vars.clone()).prop_map(|x| SExpr::Var(x.clone())),
        ];
        leaf.prop_recursive(5, 50, 10, move |inner| {
            prop_oneof![
                (inner.clone(), inner.clone()).prop_map(|(a, b)| SExpr::BinOp(
                    Box::new(a),
                    Box::new(b),
                    SBinOp::NOp(NumericalBinOp::Add)
                )),
                (inner.clone(), inner.clone()).prop_map(|(a, b)| SExpr::BinOp(
                    Box::new(a),
                    Box::new(b),
                    SBinOp::NOp(NumericalBinOp::Sub)
                )),
                (inner.clone(), inner.clone()).prop_map(|(a, b)| SExpr::BinOp(
                    Box::new(a),
                    Box::new(b),
                    SBinOp::NOp(NumericalBinOp::Mul)
                )),
                (inner.clone(), inner.clone()).prop_map(|(a, b)| SExpr::BinOp(
                    Box::new(a),
                    Box::new(b),
                    SBinOp::NOp(NumericalBinOp::Div)
                )),
                (inner.clone(), inner.clone()).prop_map(|(a, b)| SExpr::BinOp(
                    Box::new(a),
                    Box::new(b),
                    SBinOp::NOp(NumericalBinOp::Mod)
                )),
                (
                    arb_boolean_sexpr(vars.clone()),
                    inner.clone(),
                    inner.clone()
                )
                    .prop_map(|(c, t, e)| SExpr::If(
                        Box::new(c),
                        Box::new(t),
                        Box::new(e),
                    )),
            ]
        })
    }

    pub fn arb_float_sexpr(vars: Vec<VarName>) -> impl Strategy<Value = SExpr> {
        let leaf = prop_oneof![
            any::<f64>()
                .prop_filter("finite non-integer float", |x| x.is_finite()
                    && x.fract() != 0.0)
                .prop_map(SExpr::Val),
            proptest::sample::select(vars.clone()).prop_map(|x| SExpr::Var(x.clone())),
        ];
        leaf.prop_recursive(5, 50, 10, move |inner| {
            prop_oneof![
                (inner.clone(), inner.clone()).prop_map(|(a, b)| SExpr::BinOp(
                    Box::new(a),
                    Box::new(b),
                    SBinOp::NOp(NumericalBinOp::Add)
                )),
                (inner.clone(), inner.clone()).prop_map(|(a, b)| SExpr::BinOp(
                    Box::new(a),
                    Box::new(b),
                    SBinOp::NOp(NumericalBinOp::Sub)
                )),
                (inner.clone(), inner.clone()).prop_map(|(a, b)| SExpr::BinOp(
                    Box::new(a),
                    Box::new(b),
                    SBinOp::NOp(NumericalBinOp::Mul)
                )),
                (inner.clone(), inner.clone()).prop_map(|(a, b)| SExpr::BinOp(
                    Box::new(a),
                    Box::new(b),
                    SBinOp::NOp(NumericalBinOp::Div)
                )),
                (inner.clone(), inner.clone()).prop_map(|(a, b)| SExpr::BinOp(
                    Box::new(a),
                    Box::new(b),
                    SBinOp::NOp(NumericalBinOp::Mod)
                )),
                (
                    arb_boolean_sexpr(vars.clone()),
                    inner.clone(),
                    inner.clone()
                )
                    .prop_map(|(c, t, e)| SExpr::If(
                        Box::new(c),
                        Box::new(t),
                        Box::new(e),
                    )),
                inner.clone().prop_map(|a| SExpr::Sin(Box::new(a))),
                inner.clone().prop_map(|a| SExpr::Cos(Box::new(a))),
                inner.clone().prop_map(|a| SExpr::Tan(Box::new(a))),
                inner.clone().prop_map(|a| SExpr::Abs(Box::new(a))),
            ]
        })
    }

    pub fn arb_string_sexpr(vars: Vec<VarName>) -> impl Strategy<Value = SExpr> {
        let leaf = prop_oneof![
            "[a-zA-Z0-9 _-]{1,24}".prop_map(|s| SExpr::Val(Value::Str(s.into()))),
            proptest::sample::select(vars.clone()).prop_map(|x| SExpr::Var(x.clone())),
        ];

        leaf.prop_recursive(5, 50, 10, move |inner| {
            prop_oneof![
                (inner.clone(), inner.clone()).prop_map(|(a, b)| SExpr::BinOp(
                    Box::new(a),
                    Box::new(b),
                    SBinOp::SOp(StrBinOp::Concat)
                )),
                (
                    arb_boolean_sexpr(vars.clone()),
                    inner.clone(),
                    inner.clone()
                )
                    .prop_map(|(c, t, e)| SExpr::If(
                        Box::new(c),
                        Box::new(t),
                        Box::new(e)
                    )),
                (inner.clone(), inner.clone())
                    .prop_map(|(a, b)| SExpr::Default(Box::new(a), Box::new(b))),
                inner.clone().prop_map(|a| SExpr::When(Box::new(a))),
                (inner.clone(), inner.clone())
                    .prop_map(|(a, b)| SExpr::Update(Box::new(a), Box::new(b))),
                (inner.clone(), inner.clone())
                    .prop_map(|(a, b)| SExpr::Latch(Box::new(a), Box::new(b))),
            ]
        })
    }

    pub fn arb_boolean_dsrv_spec() -> impl Strategy<Value = UntypedDsrvSpecification> {
        (
            // Generate a hash set of inputs from 'a' to 'h' with at least one element.
            prop::collection::hash_set("[a-h]", 1..5),
            // Generate a hash set of outputs from 'i' to 'z'. Could be empty.
            prop::collection::hash_set("[i-z]", 0..5),
        )
            .prop_flat_map(|(input_set, output_set)| {
                // Convert the sets into Vec<VarName>
                let input_vars: BTreeSet<VarName> =
                    input_set.into_iter().map(|s| s.into()).collect();
                let output_vars: BTreeSet<_> = output_set.into_iter().map(|s| s.into()).collect();

                // Combine input and output variables.
                let all_vars = input_vars
                    .clone()
                    .into_iter()
                    .chain(output_vars.clone().into_iter())
                    .collect::<Vec<VarName>>();

                // Create a strategy for generating the expression map.
                // For each key (chosen from the union of variables) generate an expression.
                prop::collection::btree_map(
                    prop::sample::select(all_vars.clone()),
                    arb_boolean_sexpr(all_vars.clone()),
                    0..=all_vars.len(),
                )
                .prop_map(move |exprs| {
                    UntypedDsrvSpecification::new(
                        input_vars.clone(),
                        output_vars.clone(),
                        exprs,
                        BTreeMap::new(),
                        Vec::new(),
                    )
                })
            })
    }

    pub fn arb_dsrv_spec() -> impl Strategy<Value = UntypedDsrvSpecification> {
        (
            prop::collection::btree_set("[a-h]", 0..5),
            prop::collection::btree_set("[i-z]", 0..5),
        )
            .prop_flat_map(|(input_set, stream_set)| {
                let input_vars = input_set
                    .into_iter()
                    .map(VarName::from)
                    .collect::<BTreeSet<_>>();
                let stream_vars = stream_set
                    .into_iter()
                    .map(VarName::from)
                    .collect::<BTreeSet<_>>();
                let mut vars = input_vars
                    .iter()
                    .chain(&stream_vars)
                    .cloned()
                    .collect::<Vec<_>>();
                // Keep expression generation defined for empty declarations and include an
                // undeclared name so unavailable-reference handling is exercised routinely.
                vars.push(VarName::new("unknown"));
                let expression = prop_oneof![
                    arb_boolean_sexpr(vars.clone()).boxed(),
                    arb_int_sexpr(vars.clone()).boxed(),
                    arb_float_sexpr(vars.clone()).boxed(),
                    arb_string_sexpr(vars.clone()).boxed(),
                    arb_mixed_sexpr(vars.clone()).boxed(),
                ];

                prop::collection::btree_map("[a-z]".prop_map(VarName::from), expression, 0..8)
                    .prop_map(move |exprs| {
                        UntypedDsrvSpecification::new(
                            input_vars.clone(),
                            stream_vars.clone(),
                            exprs,
                            BTreeMap::new(),
                            Vec::new(),
                        )
                    })
            })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use proptest::prelude::*;
    use tracing::info;

    use super::generation::{
        arb_boolean_sexpr, arb_float_sexpr, arb_int_sexpr, arb_mixed_sexpr, arb_string_sexpr,
    };
    use super::{RuntimeExpr, RuntimeScope, SExpr as Expr, UntypedDsrvSpecification, VarName};
    use crate::core::{StreamType, StreamTypeAscription};
    use crate::dsrv_fixtures::{
        spec_simple_add_aux_monitor, spec_simple_add_aux_typed_monitor, spec_simple_add_monitor,
        spec_simple_add_monitor_typed,
    };
    use crate::dsrv_specification;
    use crate::lang::dsrv::ast::SpannedExpr as SExpr;
    use crate::lang::dsrv::lalr_parser::parse_sexpr;
    use crate::lang::dsrv::parser::sexpr as parse_sexpr_comb;
    use crate::lang::dsrv::span::strip_span;
    use ecow::{EcoVec, eco_vec};

    #[test]
    fn nested_runtime_expressions_are_annotated_with_their_owner() {
        let owner = VarName::new("z");
        let dynamic = Expr::Dynamic(RuntimeExpr::automatic(
            Box::new(Expr::Var("source".into()).into()),
            StreamTypeAscription::Ascribed(StreamType::Int),
        ))
        .into();
        let defer = Expr::Defer(RuntimeExpr::explicit(
            Box::new(Expr::Var("source".into()).into()),
            StreamTypeAscription::Ascribed(StreamType::Int),
            eco_vec!["source".into()],
        ))
        .into();
        let expressions = BTreeMap::from([(
            owner.clone(),
            Expr::Tuple(
                vec![
                    Expr::Struct(BTreeMap::from([("dynamic".into(), dynamic)])).into(),
                    Expr::ObjectLiteral(BTreeMap::from([("defer".into(), defer)])).into(),
                ]
                .into(),
            )
            .into(),
        )]);

        let annotated = UntypedDsrvSpecification::annotate_runtime_owners(
            &BTreeSet::new(),
            &BTreeSet::from([owner.clone()]),
            &expressions,
        );
        let Expr::Tuple(containers) = &annotated[&owner].node else {
            panic!("expected tuple containing nested runtime expressions");
        };
        let Expr::Struct(fields) = &containers[0].node else {
            panic!("expected struct container");
        };
        let Expr::Dynamic(runtime) = &fields["dynamic"].node else {
            panic!("expected dynamic expression");
        };
        assert_eq!(runtime.scope, RuntimeScope::Automatic(Some(owner.clone())));

        let Expr::ObjectLiteral(fields) = &containers[1].node else {
            panic!("expected object-literal container");
        };
        let Expr::Defer(runtime) = &fields["defer"].node else {
            panic!("expected defer expression");
        };
        assert_eq!(
            runtime.scope,
            RuntimeScope::Explicit(eco_vec!["source".into()], Some(owner))
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn test_prop_format_works(e in arb_boolean_sexpr(vec!["a".into(), "b".into()])) {
            let _ = format!("{}", e);
        }

        #[test]
        fn test_prop_display_parse_roundtrip(e in arb_boolean_sexpr(vec!["a".into(), "b".into()])) {
            let formatted = format!("{}", e);
            let parsed = parse_sexpr(&formatted).expect("Display output should be parsable");
            prop_assert_eq!(strip_span(&parsed), strip_span(&e));
        }

        #[test]
        fn test_prop_display_parse_roundtrip_int(e in arb_int_sexpr(vec!["a".into(), "b".into()])) {
            let formatted = format!("{}", e);
            let parsed = parse_sexpr(&formatted).expect("Display output should be parsable");
            prop_assert_eq!(strip_span(&parsed), strip_span(&e));
        }

        #[test]
        fn test_prop_display_parse_roundtrip_float(e in arb_float_sexpr(vec!["a".into(), "b".into()])) {
            let formatted = format!("{}", e);
            let parsed = parse_sexpr(&formatted).expect("Display output should be parsable");
            prop_assert_eq!(strip_span(&parsed), strip_span(&e));
        }

        #[test]
        fn test_prop_display_parse_roundtrip_string(e in arb_string_sexpr(vec!["a".into(), "b".into()])) {
            let formatted = format!("{}", e);
            info!("Testing roundtrip on {formatted} ({e:?})");
            let parsed = parse_sexpr(&formatted).expect(format!("Display output {formatted} should be parsable").as_str());
            prop_assert_eq!(strip_span(&parsed), strip_span(&e));
        }

        #[test]
        fn test_prop_inputs_works(e in arb_boolean_sexpr(vec!["a".into(), "b".into()])) {
            let valid_inputs: Vec<VarName> = vec!["a".into(), "b".into()];
            let inputs = e.inputs();
            for input in inputs.iter() {
                assert!(valid_inputs.contains(input));
            }
        }

        #[test]
        fn test_prop_display_parse_roundtrip_mixed(e in arb_mixed_sexpr(vec!["a".into(), "b".into()])) {
            let formatted = format!("{}", e);
            let parsed = parse_sexpr(&formatted).expect("Mixed display output should be parsable");
            prop_assert_eq!(strip_span(&parsed), strip_span(&e));
        }

        #[test]
        fn test_prop_inputs_works_mixed(e in arb_mixed_sexpr(vec!["a".into(), "b".into()])) {
            let valid_inputs: Vec<VarName> = vec!["a".into(), "b".into()];
            let inputs = e.inputs();
            for input in inputs.iter() {
                assert!(valid_inputs.contains(input));
            }
        }
    }

    #[test]
    fn test_display_simple_add() {
        let spec = dsrv_specification(&mut spec_simple_add_monitor()).unwrap();
        let res = format!("{}", spec);
        let expected = "in x\nin y\nout z\nz = (x + y)\n";
        assert_eq!(res, expected);
    }

    #[test]
    fn test_display_simple_add_typed() {
        let spec = dsrv_specification(&mut spec_simple_add_monitor_typed()).unwrap();
        let res = format!("{}", spec);
        let expected = "in x: Int\nin y: Int\nout z: Int\nz = (x + y)\n";
        assert_eq!(res, expected);
    }

    #[test]
    fn test_display_simple_add_aux() {
        let spec = dsrv_specification(&mut spec_simple_add_aux_monitor()).unwrap();
        let res = format!("{}", spec);
        let expected = "in x\nin y\nout z\naux u\naux w\nu = x\nw = y\nz = (u + w)";
        assert_eq!(
            res.lines().collect::<BTreeSet<_>>(),
            expected.lines().collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn test_display_simple_add_aux_typed() {
        let spec = dsrv_specification(&mut spec_simple_add_aux_typed_monitor()).unwrap();
        let res = format!("{}", spec);
        let expected =
            "in x: Int\nin y: Int\nout z: Int\naux u: Int\naux w: Int\nu = x\nw = y\nz = (u + w)";
        assert_eq!(
            res.lines().collect::<BTreeSet<_>>(),
            expected.lines().collect::<BTreeSet<_>>()
        );
    }

    fn assert_display_roundtrips_both_parsers(expr: &SExpr) {
        let formatted = format!("{}", expr);

        let parsed_lalr = parse_sexpr(&formatted).expect("LALR parser should parse display output");
        assert_eq!(strip_span(&parsed_lalr), strip_span(expr));

        let mut input = formatted.as_str();
        let parsed_comb =
            parse_sexpr_comb(&mut input).expect("Combinator parser should parse display output");
        assert_eq!(strip_span(&parsed_comb), strip_span(expr));
    }

    #[test]
    fn test_display_parse_roundtrip_dynamic_type_ascriptions_both_parsers() {
        assert_display_roundtrips_both_parsers(&SExpr::Dynamic(
            Box::new(SExpr::Var("x".into())),
            StreamTypeAscription::Ascribed(StreamType::Int),
        ));
        assert_display_roundtrips_both_parsers(&SExpr::RestrictedDynamic(
            Box::new(SExpr::Var("x".into())),
            StreamTypeAscription::Ascribed(StreamType::Int),
            eco_vec!["x".into(), "y".into()],
        ));
        assert_display_roundtrips_both_parsers(&SExpr::Defer(
            Box::new(SExpr::Var("x".into())),
            StreamTypeAscription::Ascribed(StreamType::Int),
            EcoVec::new(),
        ));
        assert_display_roundtrips_both_parsers(&SExpr::Defer(
            Box::new(SExpr::Var("x".into())),
            StreamTypeAscription::Ascribed(StreamType::Int),
            eco_vec!["x".into(), "y".into()],
        ));
    }

    #[test]
    fn test_display_parse_roundtrip_list_literal_both_parsers() {
        let expr = SExpr::List(eco_vec![SExpr::Val(1), SExpr::Val(2)]);
        assert_eq!(format!("{}", expr), "[1, 2]");
        assert_display_roundtrips_both_parsers(&expr);
    }

    #[test]
    fn test_display_parse_roundtrip_map_key_quoting_mget_both_parsers() {
        let expr = SExpr::MGet(Box::new(SExpr::Var("records".into())), "target".into());
        let formatted = format!("{}", expr);

        let parsed_lalr = parse_sexpr(&formatted).expect("LALR parser should parse display output");
        assert_eq!(strip_span(&parsed_lalr), strip_span(&expr));

        let mut input = formatted.as_str();
        let parsed_comb =
            parse_sexpr_comb(&mut input).expect("Combinator parser should parse display output");
        assert_eq!(strip_span(&parsed_comb), strip_span(&expr));
    }

    #[test]
    fn test_display_parse_roundtrip_map_key_quoting_minsert_both_parsers() {
        let expr = SExpr::MInsert(
            Box::new(SExpr::Var("m".into())),
            "key".into(),
            Box::new(SExpr::Val(42)),
        );
        let formatted = format!("{}", expr);

        let parsed_lalr = parse_sexpr(&formatted).expect("LALR parser should parse display output");
        assert_eq!(strip_span(&parsed_lalr), strip_span(&expr));

        let mut input = formatted.as_str();
        let parsed_comb =
            parse_sexpr_comb(&mut input).expect("Combinator parser should parse display output");
        assert_eq!(strip_span(&parsed_comb), strip_span(&expr));
    }

    #[test]
    fn test_display_parse_roundtrip_map_key_quoting_mremove_both_parsers() {
        let expr = SExpr::MRemove(Box::new(SExpr::Var("m".into())), "key".into());
        let formatted = format!("{}", expr);

        let parsed_lalr = parse_sexpr(&formatted).expect("LALR parser should parse display output");
        assert_eq!(strip_span(&parsed_lalr), strip_span(&expr));

        let mut input = formatted.as_str();
        let parsed_comb =
            parse_sexpr_comb(&mut input).expect("Combinator parser should parse display output");
        assert_eq!(strip_span(&parsed_comb), strip_span(&expr));
    }

    #[test]
    fn test_display_parse_roundtrip_map_key_quoting_mhas_key_both_parsers() {
        let expr = SExpr::MHasKey(Box::new(SExpr::Var("m".into())), "key".into());
        let formatted = format!("{}", expr);

        let parsed_lalr = parse_sexpr(&formatted).expect("LALR parser should parse display output");
        assert_eq!(strip_span(&parsed_lalr), strip_span(&expr));

        let mut input = formatted.as_str();
        let parsed_comb =
            parse_sexpr_comb(&mut input).expect("Combinator parser should parse display output");
        assert_eq!(strip_span(&parsed_comb), strip_span(&expr));
    }

    #[test]
    fn test_display_parse_roundtrip_map_literal_key_quoting_both_parsers() {
        let expr = SExpr::Map(BTreeMap::from([("quoted".into(), SExpr::Val(true))]));
        let formatted = format!("{}", expr);

        let parsed_lalr = parse_sexpr(&formatted).expect("LALR parser should parse display output");
        assert_eq!(strip_span(&parsed_lalr), strip_span(&expr));

        let mut input = formatted.as_str();
        let parsed_comb =
            parse_sexpr_comb(&mut input).expect("Combinator parser should parse display output");
        assert_eq!(strip_span(&parsed_comb), strip_span(&expr));
    }
}
