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

// Str Binary Operations
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum StrBinOp {
    Concat,
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
            SBinOp::NOp(op) => write!(f, "Numerical {:?}", op),
            SBinOp::BOp(op) => write!(f, "Boolean {:?}", op),
            SBinOp::SOp(op) => write!(f, "String {:?}", op),
            SBinOp::COp(op) => write!(f, "Comparison {}", op),
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
    Dynamic(Box<SpannedExpr>, StreamTypeAscription),
    RestrictedDynamic(Box<SpannedExpr>, StreamTypeAscription, EcoVec<VarName>),
    // Deferred properties
    Defer(Box<SpannedExpr>, StreamTypeAscription, EcoVec<VarName>),
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

    // List and list expressions
    List(EcoVec<SpannedExpr>),
    LIndex(Box<SpannedExpr>, Box<SpannedExpr>), // List index: First is list, second is index
    LAppend(Box<SpannedExpr>, Box<SpannedExpr>), // List append -- First is list, second is el to add
    LConcat(Box<SpannedExpr>, Box<SpannedExpr>), // List concat -- First is list, second is other list
    LHead(Box<SpannedExpr>),                     // List head -- get first element of list
    LTail(Box<SpannedExpr>),                     // List tail -- get all but first element of list
    LLen(Box<SpannedExpr>),                      // List length -- returns length of the list

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
            Dynamic(e, _) => e.inputs(),
            RestrictedDynamic(_, _, vs) => vs.iter().cloned().collect(),
            Defer(e, _, _) => e.inputs(),
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
            List(es) => {
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
            LIndex(e1, e2) | LAppend(e1, e2) | LConcat(e1, e2) => {
                let mut inputs = e1.inputs();
                inputs.extend(e2.inputs());
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
    // NOTE: This is a hack that ensures that when we create subcontexts for usage in DUPs,
    // the subcontexts do not refer to the lhs of assignments.
    // I.e., if we have an assignment `z = dynamic(s)` then the subcontext provided for
    // `dynamic(s)` does not allow access to `z`.
    // This is necessary in order to allow the clock to advance correctly.
    // This is an incomplete solution as it unnecessarily restricts the DUP `s = "w[1]"`
    // which should be legal.
    //
    // TODO: Get rid of this hack. What we need to do is:
    // 1. Introduce dependency graphs as a general language feature that the contexts have access to
    //    (have them as properties inside the Contexts)
    // 2. When creating subcontexts, only allow variables that do not introduce
    // zero-weight cycles.
    //    2.5. Given the specification `y = dynamic(s); x = dynamic(s)`, this also needs to disallow
    //    the `dynamic(s)` subcontext for `y` to access `x`, and disallow the
    //    the `dynamic(s)` subcontext for `x` to access `y`.
    // 3. Step 2. may require more information than just a dependency graph provides,
    // as it might need information about subexpressions. E.g., something like
    // `z = dynamic(s)` should disallow the context in `dynamic(s)` to access `z`,
    // but something like `z = (dynamic(s))[1]` should allow the subcontext of `dynamic(s)`
    // to access `z`, because the subcontext is delayed by the SIndex.
    // 3.5. We discussed whether this should be in statements, live in Context, etc. Might require
    //   a lot of thought.
    // 4. Profit - this hack is no longer needed and we have a more correct solution
    fn fix_dynamic(
        input_vars: &BTreeSet<VarName>,
        output_vars: &BTreeSet<VarName>,
        exprs: &BTreeMap<VarName, SpannedExpr>,
    ) -> BTreeMap<VarName, SpannedExpr> {
        // Helper function to do the changes...
        fn traverse_expr(expr: SpannedExpr, vars: &EcoVec<VarName>) -> SpannedExpr {
            let span = expr.span; // Store the original span to reuse in the output expression
            let new_kind = match expr.node {
                // March to node in the span and do the necessary changes
                // Transform Dynamic into RestrictedDynamic without the lhs of the assignment
                SExpr::Dynamic(sexpr, sta) => SExpr::RestrictedDynamic(
                    Box::new(traverse_expr(*sexpr, vars)),
                    sta,
                    vars.clone(),
                ),
                SExpr::RestrictedDynamic(sexpr, sta, eco_vec) => {
                    // Cannot contain anything that is not inside `vars`
                    let new_restricted = eco_vec
                        .iter()
                        .filter(|&var| vars.contains(var))
                        .cloned()
                        .collect();
                    SExpr::RestrictedDynamic(
                        Box::new(traverse_expr(*sexpr, vars)),
                        sta,
                        new_restricted,
                    )
                }
                SExpr::Defer(sexpr, sta, _) => {
                    // Disallow Defer to use the lhs of the assignment
                    SExpr::Defer(Box::new(traverse_expr(*sexpr, vars)), sta, vars.clone())
                }
                SExpr::Var(v) => SExpr::Var(v.clone()),
                SExpr::Val(v) => SExpr::Val(v.clone()),
                SExpr::When(sexpr) => SExpr::When(Box::new(traverse_expr(*sexpr, vars))),
                SExpr::Not(sexpr) => SExpr::Not(Box::new(traverse_expr(*sexpr, vars))),
                SExpr::SIndex(sexpr, i) => SExpr::SIndex(Box::new(traverse_expr(*sexpr, vars)), i),
                SExpr::Sin(sexpr) => SExpr::Sin(Box::new(traverse_expr(*sexpr, vars))),
                SExpr::Cos(sexpr) => SExpr::Cos(Box::new(traverse_expr(*sexpr, vars))),
                SExpr::Tan(sexpr) => SExpr::Tan(Box::new(traverse_expr(*sexpr, vars))),
                SExpr::Abs(sexpr) => SExpr::Abs(Box::new(traverse_expr(*sexpr, vars))),
                SExpr::MonitoredAt(v, n) => SExpr::MonitoredAt(v, n),
                SExpr::Dist(v, u) => SExpr::Dist(v, u),
                SExpr::LTail(sexpr) => SExpr::LTail(Box::new(traverse_expr(*sexpr, vars))),
                SExpr::LHead(sexpr) => SExpr::LHead(Box::new(traverse_expr(*sexpr, vars))),
                SExpr::LLen(sexpr) => SExpr::LLen(Box::new(traverse_expr(*sexpr, vars))),
                SExpr::IsDefined(sexpr) => SExpr::IsDefined(Box::new(traverse_expr(*sexpr, vars))),
                SExpr::BinOp(sexpr, sexpr1, sbin_op) => SExpr::BinOp(
                    Box::new(traverse_expr(*sexpr, vars)),
                    Box::new(traverse_expr(*sexpr1, vars)),
                    sbin_op.clone(),
                ),
                SExpr::Latch(sexpr1, sexpr2) => SExpr::Latch(
                    Box::new(traverse_expr(*sexpr1, vars)),
                    Box::new(traverse_expr(*sexpr2, vars)),
                ),
                SExpr::Init(sexpr1, sexpr2) => SExpr::Init(
                    Box::new(traverse_expr(*sexpr1, vars)),
                    Box::new(traverse_expr(*sexpr2, vars)),
                ),
                SExpr::LIndex(sexpr, sexpr1) => SExpr::LIndex(
                    Box::new(traverse_expr(*sexpr, vars)),
                    Box::new(traverse_expr(*sexpr1, vars)),
                ),
                SExpr::LAppend(sexpr, sexpr1) => SExpr::LAppend(
                    Box::new(traverse_expr(*sexpr, vars)),
                    Box::new(traverse_expr(*sexpr1, vars)),
                ),
                SExpr::LConcat(sexpr, sexpr1) => SExpr::LConcat(
                    Box::new(traverse_expr(*sexpr, vars)),
                    Box::new(traverse_expr(*sexpr1, vars)),
                ),
                SExpr::Update(sexpr, sexpr1) => SExpr::Update(
                    Box::new(traverse_expr(*sexpr, vars)),
                    Box::new(traverse_expr(*sexpr1, vars)),
                ),
                SExpr::Default(sexpr, sexpr1) => SExpr::Default(
                    Box::new(traverse_expr(*sexpr, vars)),
                    Box::new(traverse_expr(*sexpr1, vars)),
                ),
                SExpr::If(sexpr, sexpr1, sexpr2) => SExpr::If(
                    Box::new(traverse_expr(*sexpr, vars)),
                    Box::new(traverse_expr(*sexpr1, vars)),
                    Box::new(traverse_expr(*sexpr2, vars)),
                ),
                SExpr::List(vec) => {
                    //Added recursive traversal for lists.
                    let new_vec = vec
                        .into_iter()
                        .map(|sexpr| traverse_expr(sexpr, vars))
                        .collect();

                    SExpr::List(new_vec)
                }
                SExpr::Map(map) => {
                    let new_map = map
                        .into_iter()
                        .map(|(k, v)| (k, traverse_expr(v, vars)))
                        .collect();
                    SExpr::Map(new_map)
                }
                SExpr::Struct(map) => {
                    let m = map.clone(); // TODO: Delete when no
                    // longer cloning and just iter() instead of into_iter()...
                    m.into_iter().for_each(|(_, v)| {
                        traverse_expr(v, vars);
                    });
                    SExpr::Struct(map)
                }
                SExpr::ObjectLiteral(map) => {
                    let m = map.clone(); // TODO: Delete when no
                    // longer cloning and just iter() instead of into_iter()...
                    m.into_iter().for_each(|(_, v)| {
                        traverse_expr(v, vars);
                    });
                    SExpr::ObjectLiteral(map)
                }
                SExpr::MGet(map, k) => SExpr::MGet(Box::new(traverse_expr(*map.clone(), vars)), k),
                SExpr::SGet(st, k) => SExpr::SGet(Box::new(traverse_expr(*st.clone(), vars)), k),
                SExpr::MInsert(map, k, v) => SExpr::MInsert(
                    Box::new(traverse_expr(*map, vars)),
                    k,
                    Box::new(traverse_expr(*v, vars)),
                ),
                SExpr::MRemove(map, k) => SExpr::MRemove(Box::new(traverse_expr(*map, vars)), k),
                SExpr::MHasKey(map, k) => SExpr::MHasKey(Box::new(traverse_expr(*map, vars)), k),
            };
            Spanned {
                node: new_kind,
                span,
            }
        }

        let vars: EcoVec<VarName> = input_vars
            .iter()
            .cloned()
            .chain(output_vars.iter().cloned())
            .collect();

        exprs
            .iter()
            .map(|(name, expr)| {
                let vars: EcoVec<VarName> = vars.iter().filter(|&n| name != n).cloned().collect();
                (name.clone(), traverse_expr(expr.clone(), &vars))
            })
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
        let exprs = Self::fix_dynamic(&input_vars, &stream_vars, &exprs);
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
            Var(v) => write!(f, "{}", v),
            MonitoredAt(u, v) => {
                write!(f, "monitored_at({}, {})", u, v)
            }
            Dist(u, v) => {
                write!(f, "dist({}, {})", u, v)
            }
            Dynamic(e, sta) => match sta {
                StreamTypeAscription::Unascribed => write!(f, "dynamic({})", e),
                StreamTypeAscription::Ascribed(st) => write!(f, "dynamic({}, {})", e, st),
            },
            RestrictedDynamic(e, sta, vs) => {
                let env = vs
                    .iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<String>>()
                    .join(", ");
                match sta {
                    StreamTypeAscription::Unascribed => write!(f, "dynamic({}, {{{}}})", e, env,),
                    StreamTypeAscription::Ascribed(sta) => {
                        write!(f, "dynamic({}, {}, {{{}}})", e, sta, env)
                    }
                }
            }
            Defer(e, sta, _) => match sta {
                StreamTypeAscription::Unascribed => write!(f, "defer({})", e),
                StreamTypeAscription::Ascribed(sta) => write!(f, "defer({}, {})", e, sta),
            },
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
            LIndex(e, i) => write!(f, "List.get({}, {})", e, i),
            LAppend(lst, el) => write!(f, "List.append({}, {})", lst, el),
            LConcat(lst1, lst2) => write!(f, "List.concat({}, {})", lst1, lst2),
            LHead(lst) => write!(f, "List.head({})", lst),
            LTail(lst) => write!(f, "List.tail({})", lst),
            LLen(lst) => write!(f, "List.len({})", lst),
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
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use proptest::prelude::*;
    use tracing::info;

    use super::VarName;
    use super::generation::{
        arb_boolean_sexpr, arb_float_sexpr, arb_int_sexpr, arb_mixed_sexpr, arb_string_sexpr,
    };
    use crate::dsrv_fixtures::{
        spec_simple_add_aux_monitor, spec_simple_add_aux_typed_monitor, spec_simple_add_monitor,
        spec_simple_add_monitor_typed,
    };
    use crate::dsrv_specification;
    use crate::lang::dsrv::ast::SpannedExpr as SExpr;
    use crate::lang::dsrv::lalr_parser::parse_sexpr;
    use crate::lang::dsrv::parser::sexpr as parse_sexpr_comb;
    use crate::lang::dsrv::span::strip_span;

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
