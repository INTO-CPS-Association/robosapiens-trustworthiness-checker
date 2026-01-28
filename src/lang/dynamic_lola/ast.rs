use ecow::{EcoString, EcoVec};

use crate::core::{Specification, StreamTypeAscription, VarName};
use crate::core::{StreamType, Value};
use crate::distributed::distribution_graphs::NodeName;
use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
};

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

// Stream BinOp
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum SBinOp {
    NOp(NumericalBinOp),
    BOp(BoolBinOp),
    SOp(StrBinOp),
    COp(CompBinOp),
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

#[derive(Clone, PartialEq, Debug, serde::Serialize)]
pub enum SExpr {
    // if-then-else
    If(Box<Self>, Box<Self>, Box<Self>),

    // Stream indexing
    SIndex(
        // Inner SExpr e
        Box<Self>,
        // Index i
        u64,
    ),

    // Arithmetic Stream expression
    Val(Value),

    BinOp(Box<Self>, Box<Self>, SBinOp),

    Var(VarName),

    // Dynamic, continuously updatable properties
    Dynamic(Box<Self>, StreamTypeAscription),
    RestrictedDynamic(Box<Self>, StreamTypeAscription, EcoVec<VarName>),
    // Deferred properties
    Defer(Box<Self>, StreamTypeAscription),
    // Update between properties
    Update(Box<Self>, Box<Self>),
    // Default value for properties (replaces Deferred with an alternative
    // stream)
    Default(Box<Self>, Box<Self>),
    IsDefined(Box<Self>), // True when .0 is not Deferred
    When(Box<Self>),      // Becomes true after the first time .0 is not Deferred

    // Asynchronous operations
    Latch(Box<Self>, Box<Self>),
    Init(Box<Self>, Box<Self>),

    // Unary expressions (refactor if more are added...)
    Not(Box<Self>),

    // List and list expressions
    List(EcoVec<Self>),
    LIndex(Box<Self>, Box<Self>), // List index: First is list, second is index
    LAppend(Box<Self>, Box<Self>), // List append -- First is list, second is el to add
    LConcat(Box<Self>, Box<Self>), // List concat -- First is list, second is other list
    LHead(Box<Self>),             // List head -- get first element of list
    LTail(Box<Self>),             // List tail -- get all but first element of list
    LLen(Box<Self>),              // List length -- returns length of the list

    // Map and map expressions
    Map(BTreeMap<EcoString, Self>), // Map from String to SExpr
    MGet(Box<Self>, EcoString),     // Get from map
    MInsert(Box<Self>, EcoString, Box<Self>), // Insert into map -- First is map, second is key, third is value
    MRemove(Box<Self>, EcoString),            // Remove from map -- First is map, second is key
    MHasKey(Box<Self>, EcoString),            // Check if map has key -- First is map, second is key

    // Trigonometric functions
    Sin(Box<Self>),
    Cos(Box<Self>),
    Tan(Box<Self>),

    // Other math functions
    Abs(Box<Self>),

    // Distribution Constraint Specific
    MonitoredAt(VarName, NodeName),
    Dist(VarOrNodeName, VarOrNodeName),
}

#[derive(Clone, PartialEq, Debug, serde::Serialize)]
pub enum STopDecl {
    Input(VarName, Option<StreamType>),
    Output(VarName, Option<StreamType>),
    Aux(VarName, Option<StreamType>),
    Assignment(VarName, SExpr),
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
            Defer(e, _) => e.inputs(),
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
            Map(map) => {
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

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct LOLASpecification {
    pub input_vars: Vec<VarName>,
    pub output_vars: Vec<VarName>,
    pub exprs: BTreeMap<VarName, SExpr>,
    pub type_annotations: BTreeMap<VarName, StreamType>,
    pub aux_info: Vec<VarName>,
}

impl LOLASpecification {
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
        input_vars: &Vec<VarName>,
        output_vars: &Vec<VarName>,
        exprs: &BTreeMap<VarName, SExpr>,
    ) -> BTreeMap<VarName, SExpr> {
        // Helper function to do the changes...
        fn traverse_expr(expr: SExpr, vars: &EcoVec<VarName>) -> SExpr {
            match expr {
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
                SExpr::Defer(sexpr, sta) => {
                    SExpr::Defer(Box::new(traverse_expr(*sexpr, vars)), sta)
                }
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
                    let v = vec.clone(); // TODO: Delete when no longer cloning and
                    // just iter() instead of into_iter()...
                    v.into_iter().for_each(|sexpr| {
                        traverse_expr(sexpr, vars);
                    });
                    SExpr::List(vec)
                }
                SExpr::Map(map) => {
                    let m = map.clone(); // TODO: Delete when no
                    // longer cloning and just iter() instead of into_iter()...
                    m.into_iter().for_each(|(_, v)| {
                        traverse_expr(v, vars);
                    });
                    SExpr::Map(map)
                }
                SExpr::MGet(map, k) => SExpr::MGet(Box::new(traverse_expr(*map.clone(), vars)), k),
                SExpr::MInsert(map, k, v) => SExpr::MInsert(
                    Box::new(traverse_expr(*map, vars)),
                    k,
                    Box::new(traverse_expr(*v, vars)),
                ),
                SExpr::MRemove(map, k) => SExpr::MRemove(Box::new(traverse_expr(*map, vars)), k),
                SExpr::MHasKey(map, k) => SExpr::MHasKey(Box::new(traverse_expr(*map, vars)), k),
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

    pub fn exprs(&self) -> BTreeMap<VarName, SExpr> {
        self.exprs.clone()
    }

    pub fn new(
        input_vars: Vec<VarName>,
        output_vars: Vec<VarName>,
        exprs: BTreeMap<VarName, SExpr>,
        type_annotations: BTreeMap<VarName, StreamType>,
        aux_info: Vec<VarName>,
    ) -> Self {
        let exprs = Self::fix_dynamic(&input_vars, &output_vars, &exprs);
        LOLASpecification {
            input_vars,
            output_vars,
            exprs,
            type_annotations,
            aux_info,
        }
    }
}

impl Specification for LOLASpecification {
    type Expr = SExpr;

    fn input_vars(&self) -> Vec<VarName> {
        self.input_vars.clone()
    }

    fn output_vars(&self) -> Vec<VarName> {
        self.output_vars.clone()
    }

    fn var_expr(&self, var: &VarName) -> Option<SExpr> {
        Some(self.exprs.get(var)?.clone())
    }
}

impl Display for SExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use SBinOp::*;
        use SExpr::*;
        match self {
            If(b, e1, e2) => write!(f, "if {} then {} else {}", b, e1, e2),
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
            BinOp(e1, e2, COp(CompBinOp::Lt)) => write!(f, "({} <= {})", e1, e2),
            BinOp(e1, e2, COp(CompBinOp::Ge)) => write!(f, "({} <= {})", e1, e2),
            BinOp(e1, e2, COp(CompBinOp::Gt)) => write!(f, "({} <= {})", e1, e2),
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
            Defer(e, sta) => match sta {
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
                    map.iter().map(|(k, v)| format!("{}: {}", k, v)).collect();
                write!(f, "{{{}}}", map_str.join(", "))
            }
            MGet(map, k) => write!(f, "Map.get({}, {})", map, k),
            MInsert(map, k, v) => write!(f, "Map.insert({}, {}, {})", map, k, v),
            MRemove(map, k) => write!(f, "Map.remove({}, {})", map, k),
            MHasKey(map, k) => write!(f, "Map.has_key({}, {})", map, k),
            Sin(v) => write!(f, "sin({})", v),
            Cos(v) => write!(f, "cos({})", v),
            Tan(v) => write!(f, "tan({})", v),
            Abs(v) => write!(f, "abs({})", v),
        }
    }
}

#[cfg(test)]
pub mod generation {
    use super::*;

    use proptest::prelude::*;

    use crate::{
        LOLASpecification, SExpr, VarName,
        lang::dynamic_lola::ast::{BoolBinOp, SBinOp},
    };

    pub fn arb_boolean_sexpr(vars: Vec<VarName>) -> impl Strategy<Value = SExpr> {
        let leaf = prop_oneof![
            any::<bool>().prop_map(|x| SExpr::Val(x.into())),
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

    pub fn arb_boolean_lola_spec() -> impl Strategy<Value = LOLASpecification> {
        (
            // Generate a hash set of inputs from 'a' to 'h' with at least one element.
            prop::collection::hash_set("[a-h]", 1..5),
            // Generate a hash set of outputs from 'i' to 'z'. Could be empty.
            prop::collection::hash_set("[i-z]", 0..5),
        )
            .prop_flat_map(|(input_set, output_set)| {
                // Convert the sets into Vec<VarName>
                let input_vars: Vec<VarName> = input_set.into_iter().map(|s| s.into()).collect();
                let output_vars: Vec<VarName> = output_set.into_iter().map(|s| s.into()).collect();

                // Combine input and output variables.
                let mut all_vars = input_vars.clone();
                all_vars.extend(output_vars.clone());
                all_vars.sort();
                all_vars.dedup();

                // Create a strategy for generating the expression map.
                // For each key (chosen from the union of variables) generate an expression.
                prop::collection::btree_map(
                    prop::sample::select(all_vars.clone()),
                    arb_boolean_sexpr(all_vars.clone()),
                    0..=all_vars.len(),
                )
                .prop_map(move |exprs| LOLASpecification {
                    input_vars: input_vars.clone(),
                    output_vars: output_vars.clone(),
                    aux_info: vec![],
                    exprs,
                    type_annotations: BTreeMap::new(),
                })
            })
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::VarName;
    use super::generation::arb_boolean_sexpr;

    proptest! {
        #[test]
        fn test_prop_format_works(e in arb_boolean_sexpr(vec!["a".into(), "b".into()])) {
            let _ = format!("{}", e);
        }

        #[test]
        fn test_prop_inputs_works(e in arb_boolean_sexpr(vec!["a".into(), "b".into()])) {
            let valid_inputs: Vec<VarName> = vec!["a".into(), "b".into()];
            let inputs = e.inputs();
            for input in inputs.iter() {
                assert!(valid_inputs.contains(input));
            }
        }
    }
}
