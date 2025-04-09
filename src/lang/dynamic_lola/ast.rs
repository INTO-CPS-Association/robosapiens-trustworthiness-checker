use ecow::EcoVec;

use crate::core::{Specification, VarName};
use crate::core::{StreamType, Value};
use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
};

// Numerical Binary Operations
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NumericalBinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

// Integer Binary Operations
#[derive(Clone, Debug, PartialEq, Eq)]
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
#[derive(Clone, Debug, PartialEq, Eq)]
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoolBinOp {
    Or,
    And,
}

// Str Binary Operations
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StrBinOp {
    Concat,
}

// Comparison Binary Operations
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompBinOp {
    Eq,
    Le,
    Ge,
    Lt,
    Gt,
}

// Stream BinOp
#[derive(Clone, Debug, PartialEq, Eq)]
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
            _ => panic!("Unknown binary operation: {}", s),
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum SExpr {
    // if-then-else
    If(Box<Self>, Box<Self>, Box<Self>),

    // Stream indexing
    SIndex(
        // Inner SExpr e
        Box<Self>,
        // Index i
        isize,
    ),

    // Arithmetic Stream expression
    Val(Value),

    BinOp(Box<Self>, Box<Self>, SBinOp),

    Var(VarName),

    // Dynamic, continuously updatable properties
    Dynamic(Box<Self>),
    RestrictedDynamic(Box<Self>, EcoVec<VarName>),
    // Deferred properties
    Defer(Box<Self>),
    // Update between properties
    Update(Box<Self>, Box<Self>),
    // Default value for properties (replaces Unknown with an alternative
    // stream)
    Default(Box<Self>, Box<Self>),
    IsDefined(Box<Self>), // True when .0 is not Unknown
    When(Box<Self>),      // Becomes true after the first time .0 is not Unknown

    // Unary expressions (refactor if more are added...)
    Not(Box<Self>),

    // List and list expressions
    List(Vec<Self>),
    LIndex(Box<Self>, Box<Self>), // List index: First is list, second is index
    LAppend(Box<Self>, Box<Self>), // List append -- First is list, second is el to add
    LConcat(Box<Self>, Box<Self>), // List concat -- First is list, second is other list
    LHead(Box<Self>),             // List head -- get first element of list
    LTail(Box<Self>),             // List tail -- get all but first element of list

    // Trigonometric functions
    Sin(Box<Self>),
    Cos(Box<Self>),
    Tan(Box<Self>),
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
            // TODO: is this correct?
            Dynamic(e) => e.inputs(),
            RestrictedDynamic(_, vs) => vs.iter().cloned().collect(),
            Defer(e) => e.inputs(),
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
            List(es) => {
                let mut inputs = vec![];
                for e in es {
                    inputs.extend(e.inputs());
                }
                inputs
            }
            LIndex(e, i) => {
                let mut inputs = e.inputs();
                inputs.extend(i.inputs());
                inputs
            }
            LAppend(lst, el) => {
                let mut inputs = lst.inputs();
                inputs.extend(el.inputs());
                inputs
            }
            LConcat(lst1, lst2) => {
                let mut inputs = lst1.inputs();
                inputs.extend(lst2.inputs());
                inputs
            }
            LHead(lst) => lst.inputs(),
            LTail(lst) => lst.inputs(),
            Sin(v) => v.inputs(),
            Cos(v) => v.inputs(),
            Tan(v) => v.inputs(),
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct LOLASpecification {
    pub input_vars: Vec<VarName>,
    pub output_vars: Vec<VarName>,
    pub exprs: BTreeMap<VarName, SExpr>,
    pub type_annotations: BTreeMap<VarName, StreamType>,
}

impl LOLASpecification {
    // NOTE: This is a hack that ensures that when we create subcontexts for usage in DUPs,
    // the subcontexts do not refer to the lhs of assignments.
    // I.e., if we have an assignment `z = dynamic(s)` then the subcontext provided for
    // `dynamic(s)` does not allow access to `z`.
    // This is necessary in order to allow the clock to advance correctly.
    // This is an incomplete solution as it unnecessarily restricts the DUP `s = "w[-1]"`
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
    // but something like `z = (dynamic(s))[-1]` should allow the subcontext of `dynamic(s)`
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
                // Fixes:
                SExpr::Dynamic(sexpr) => {
                    SExpr::RestrictedDynamic(Box::new(traverse_expr(*sexpr, vars)), vars.clone())
                }
                SExpr::RestrictedDynamic(sexpr, eco_vec) => {
                    // Cannot contain anything that is not inside `vars`
                    let new_restricted = eco_vec
                        .iter()
                        .cloned()
                        .filter(|var| vars.contains(var))
                        .collect();
                    SExpr::RestrictedDynamic(Box::new(traverse_expr(*sexpr, vars)), new_restricted)
                }
                // Terminators:
                SExpr::Var(v) => SExpr::Var(v.clone()),
                SExpr::Val(v) => SExpr::Val(v.clone()),
                // Unary:
                SExpr::When(sexpr) => SExpr::When(Box::new(traverse_expr(*sexpr, vars))),
                SExpr::Not(sexpr) => SExpr::Not(Box::new(traverse_expr(*sexpr, vars))),
                SExpr::SIndex(sexpr, i) => SExpr::SIndex(Box::new(traverse_expr(*sexpr, vars)), i),
                SExpr::Sin(sexpr) => SExpr::Sin(Box::new(traverse_expr(*sexpr, vars))),
                SExpr::Cos(sexpr) => SExpr::Cos(Box::new(traverse_expr(*sexpr, vars))),
                SExpr::Tan(sexpr) => SExpr::Tan(Box::new(traverse_expr(*sexpr, vars))),
                SExpr::LTail(sexpr) => SExpr::LTail(Box::new(traverse_expr(*sexpr, vars))),
                SExpr::LHead(sexpr) => SExpr::LHead(Box::new(traverse_expr(*sexpr, vars))),
                SExpr::Defer(sexpr) => SExpr::Defer(Box::new(traverse_expr(*sexpr, vars))),
                SExpr::IsDefined(sexpr) => SExpr::IsDefined(Box::new(traverse_expr(*sexpr, vars))),
                // Binary:
                SExpr::BinOp(sexpr, sexpr1, sbin_op) => SExpr::BinOp(
                    Box::new(traverse_expr(*sexpr, vars)),
                    Box::new(traverse_expr(*sexpr1, vars)),
                    sbin_op.clone(),
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
                // Ternary:
                SExpr::If(sexpr, sexpr1, sexpr2) => SExpr::If(
                    Box::new(traverse_expr(*sexpr, vars)),
                    Box::new(traverse_expr(*sexpr1, vars)),
                    Box::new(traverse_expr(*sexpr2, vars)),
                ),
                // Collection
                SExpr::List(sexprs) => SExpr::List(
                    sexprs
                        .iter()
                        .cloned()
                        .map(|sexpr| traverse_expr(sexpr, vars))
                        .collect(),
                ),
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
                let vars: EcoVec<VarName> = vars.iter().cloned().filter(|n| name != n).collect();
                (name.clone(), traverse_expr(expr.clone(), &vars))
            })
            .collect()
    }

    pub fn new(
        input_vars: Vec<VarName>,
        output_vars: Vec<VarName>,
        exprs: BTreeMap<VarName, SExpr>,
        type_annotations: BTreeMap<VarName, StreamType>,
    ) -> Self {
        let exprs = Self::fix_dynamic(&input_vars, &output_vars, &exprs);
        LOLASpecification {
            input_vars,
            output_vars,
            exprs,
            type_annotations,
        }
    }
}

impl Debug for LOLASpecification {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // Format the expressions map ordered lexicographically by key
        // rather than by variable ordering
        let exprs_by_name: BTreeMap<String, &SExpr> =
            self.exprs.iter().map(|(k, v)| (k.to_string(), v)).collect();
        let exprs_formatted = format!(
            "{{{}}}",
            exprs_by_name
                .iter()
                .map(|(k, v)| format!("{:?}: {:?}", VarName::new(k), v))
                .collect::<Vec<String>>()
                .join(", ")
        );

        // Type annotations ordered lexicographically by name
        let type_annotations_by_name: BTreeMap<String, &StreamType> = self
            .type_annotations
            .iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        let type_annotations_formatted = format!(
            "{{{}}}",
            type_annotations_by_name
                .iter()
                .map(|(k, v)| format!("{:?}: {:?}", VarName::new(k), v))
                .collect::<Vec<String>>()
                .join(", ")
        );

        write!(
            f,
            "LOLASpecification {{ input_vars: {:?}, output_vars: {:?}, exprs: {}, type_annotations: {} }}",
            self.input_vars, self.output_vars, exprs_formatted, type_annotations_formatted
        )
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
            BinOp(e1, e2, SOp(StrBinOp::Concat)) => write!(f, "({} ++ {})", e1, e2),
            BinOp(e1, e2, COp(CompBinOp::Eq)) => write!(f, "({} == {})", e1, e2),
            BinOp(e1, e2, COp(CompBinOp::Le)) => write!(f, "({} <= {})", e1, e2),
            BinOp(e1, e2, COp(CompBinOp::Lt)) => write!(f, "({} <= {})", e1, e2),
            BinOp(e1, e2, COp(CompBinOp::Ge)) => write!(f, "({} <= {})", e1, e2),
            BinOp(e1, e2, COp(CompBinOp::Gt)) => write!(f, "({} <= {})", e1, e2),
            Not(b) => write!(f, "!{}", b),
            Var(v) => write!(f, "{}", v),
            Dynamic(e) => write!(f, "dynamic({})", e),
            RestrictedDynamic(e, vs) => write!(
                f,
                "dynamic({}, {{{}}})",
                e,
                vs.iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            Defer(e) => write!(f, "defer({})", e),
            Update(e1, e2) => write!(f, "update({}, {})", e1, e2),
            Default(e, v) => write!(f, "default({}, {})", e, v),
            IsDefined(sexpr) => write!(f, "is_defined({})", sexpr),
            When(sexpr) => write!(f, "when({})", sexpr),
            List(es) => {
                let es_str: Vec<String> = es.iter().map(|e| format!("{}", e)).collect();
                write!(f, "[{}]", es_str.join(", "))
            }
            LIndex(e, i) => write!(f, "List.get({}, {})", e, i),
            LAppend(lst, el) => write!(f, "List.append({}, {})", lst, el),
            LConcat(lst1, lst2) => write!(f, "List.concat({}, {})", lst1, lst2),
            LHead(lst) => write!(f, "List.head({})", lst),
            LTail(lst) => write!(f, "List.tail({})", lst),
            Sin(v) => write!(f, "sin({})", v),
            Cos(v) => write!(f, "cos({})", v),
            Tan(v) => write!(f, "tan({})", v),
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
                assert!(valid_inputs.contains(&input));
            }
        }
    }
}
