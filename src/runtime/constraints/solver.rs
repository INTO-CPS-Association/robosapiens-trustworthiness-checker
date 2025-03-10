use std::collections::BTreeMap;
use std::fmt::Debug;

use crate::core::Value;
use crate::core::{IndexedVarName, VarName};
use crate::lang::dynamic_lola::ast::*;
use crate::lang::dynamic_lola::parser::lola_expression;

pub type SyncStream<T> = BTreeMap<VarName, Vec<(usize, T)>>;
pub type ValStream = SyncStream<Value>;
pub type SExprStream = SyncStream<SExpr<IndexedVarName>>;

#[derive(Debug, Clone)]
// A ConstraintStore is the environment for the streams
pub struct ConstraintStore {
    pub input_streams: ValStream,
    pub output_exprs: BTreeMap<VarName, SExpr<VarName>>,
    pub outputs_resolved: ValStream,
    pub outputs_unresolved: SExprStream,
}

pub fn model_constraints(model: LOLASpecification) -> ConstraintStore {
    let mut constraints = ConstraintStore::default();
    for (var, sexpr) in model.exprs.iter() {
        constraints
            .output_exprs
            .insert(VarName(var.0.clone()), sexpr.clone());
    }
    constraints
}

impl Default for ConstraintStore {
    fn default() -> Self {
        ConstraintStore {
            input_streams: BTreeMap::new(),
            output_exprs: BTreeMap::new(),
            outputs_resolved: BTreeMap::new(),
            outputs_unresolved: BTreeMap::new(),
        }
    }
}

impl ConstraintStore {
    // Looks up the variable name inside the map. Returns the value at the given index if the var and value exists.
    pub fn get_value_from_stream<'a, T: Clone>(
        name: &VarName,
        idx: &usize,
        map: &'a BTreeMap<VarName, Vec<(usize, T)>>,
    ) -> Option<&'a T> {
        let inner = map.get(name)?;
        inner.iter().find(|(i, _)| i == idx).map(|(_, v)| v)
    }

    pub fn get_from_input_streams(&self, name: &VarName, idx: &usize) -> Option<&Value> {
        Self::get_value_from_stream(name, idx, &self.input_streams)
    }

    pub fn get_from_outputs_resolved(&self, name: &VarName, idx: &usize) -> Option<&Value> {
        Self::get_value_from_stream(name, idx, &self.outputs_resolved)
    }

    pub fn get_from_outputs_unresolved(
        &self,
        name: &VarName,
        idx: &usize,
    ) -> Option<&SExpr<IndexedVarName>> {
        Self::get_value_from_stream(name, idx, &self.outputs_unresolved)
    }
}

impl PartialEq for ConstraintStore {
    fn eq(&self, other: &Self) -> bool {
        self.input_streams == other.input_streams
            && self.outputs_resolved == other.outputs_resolved
            && self.outputs_unresolved == other.outputs_unresolved
            && self.output_exprs == other.output_exprs
    }
}
impl Eq for ConstraintStore {}

pub enum SimplifyResult<T> {
    Resolved(Value),
    Unresolved(T),
}

use SimplifyResult::*;
use winnow::Parser;

fn binop_table(v1: Value, v2: Value, op: SBinOp) -> Value {
    use SBinOp::*;
    use Value::*;

    match (v1, v2, op) {
        (Int(i1), Int(i2), IOp(iop)) => match iop {
            IntBinOp::Add => Int(i1 + i2),
            IntBinOp::Sub => Int(i1 - i2),
            IntBinOp::Mul => Int(i1 * i2),
            IntBinOp::Div => Int(i1 / i2),
        },
        (Bool(b1), Bool(b2), BOp(bop)) => match bop {
            BoolBinOp::Or => Bool(b1 || b2),
            BoolBinOp::And => Bool(b1 && b2),
        },
        (Str(s1), Str(s2), SOp(sop)) => {
            match sop {
                StrBinOp::Concat => {
                    // TODO: Probably more efficient way to concat than to create a new string
                    Str(format!("{}{}", s1, s2))
                }
            }
        }
        (v1, v2, op) => {
            unreachable!(
                "Trying to solve BinOp with incorrect Value types. v1: {:?}. op: {:?}. v2: {:?}",
                v1, op, v2
            );
        }
    }
}

pub trait ConvertToAbsolute {
    type Output;
    fn to_absolute(&self, base_time: usize) -> Self::Output;
}

impl ConvertToAbsolute for SExpr<VarName> {
    type Output = SExpr<IndexedVarName>;

    fn to_absolute(&self, base_time: usize) -> Self::Output {
        match self {
            SExpr::Val(val) => SExpr::Val(val.clone()),
            SExpr::BinOp(lhs, rhs, op) => SExpr::BinOp(
                Box::new(lhs.to_absolute(base_time)),
                Box::new(rhs.to_absolute(base_time)),
                op.clone(),
            ),
            SExpr::Var(name) => SExpr::Var(IndexedVarName(name.0.clone(), base_time)),
            SExpr::SIndex(expr, offset, default) => {
                // Determine if it is something that can eventually be solved. If not, transform it to a lit
                let absolute_time = base_time as isize + offset;
                if absolute_time < 0 {
                    SExpr::Val(default.clone())
                } else {
                    SExpr::SIndex(
                        Box::new(expr.to_absolute(base_time)),
                        absolute_time,
                        default.clone(),
                    )
                }
            }
            SExpr::If(bexpr, if_expr, else_expr) => SExpr::If(
                Box::new(bexpr.to_absolute(base_time)),
                Box::new(if_expr.to_absolute(base_time)),
                Box::new(else_expr.to_absolute(base_time)),
            ),
            SExpr::Eval(_) => todo!(),
            // If we have reached this point with defer then we know that we haven't received a
            // property
            SExpr::Defer(_) => SExpr::Val(Value::Unknown),
            SExpr::Update(_, _) => todo!(),
            SExpr::Default(expr, default) => {
                SExpr::Default(Box::new(expr.to_absolute(base_time)), default.clone())
            }
            SExpr::Not(_) => todo!(),
            SExpr::List(_) => todo!(),
            SExpr::LIndex(_, _) => todo!(),
            SExpr::LAppend(_, _) => todo!(),
            SExpr::LConcat(_, _) => todo!(),
            SExpr::LHead(_) => todo!(),
            SExpr::LTail(_) => todo!(),
        }
    }
}

pub trait Simplifiable {
    fn simplify(&self, base_time: usize, store: &ConstraintStore) -> SimplifyResult<Box<Self>>;
}

// SExprA
impl Simplifiable for SExpr<IndexedVarName> {
    fn simplify(&self, base_time: usize, store: &ConstraintStore) -> SimplifyResult<Box<Self>> {
        match self {
            SExpr::Val(i) => Resolved(i.clone()),
            SExpr::BinOp(e1, e2, op) => {
                match (e1.simplify(base_time, store), e2.simplify(base_time, store)) {
                    (Resolved(e1), Resolved(e2)) => Resolved(binop_table(e1, e2, op.clone())),
                    // Does not reuse the previous e1 and e2s as the subexpressions may have been simplified
                    (Unresolved(ue), Resolved(re)) | (Resolved(re), Unresolved(ue)) => Unresolved(
                        Box::new(SExpr::BinOp(ue, Box::new(SExpr::Val(re)), op.clone())),
                    ),
                    (Unresolved(e1), Unresolved(e2)) => {
                        Unresolved(Box::new(SExpr::BinOp(e1, e2, op.clone())))
                    }
                }
            }
            SExpr::Var(idx_var_name) => {
                let var_name = VarName(idx_var_name.0.clone());
                // Check if we have a value inside resolved or input values
                if let Some(v) = store
                    .get_from_outputs_resolved(&var_name, &base_time)
                    .or_else(|| store.get_from_input_streams(&var_name, &base_time))
                {
                    return Resolved(v.clone());
                }
                // Otherwise it must be inside unresolved
                if let Some(expr) = store.get_from_outputs_unresolved(&var_name, &base_time) {
                    Unresolved(Box::new(expr.clone()))
                } else {
                    Resolved(Value::Unknown)
                }
            }
            SExpr::SIndex(expr, idx_time, default) => {
                // Should not be negative at this stage since it was indexed...
                let uidx_time = *idx_time as usize;
                if uidx_time <= base_time {
                    expr.simplify(uidx_time, store)
                } else {
                    Unresolved(Box::new(SExpr::SIndex(
                        expr.clone(),
                        *idx_time,
                        default.clone(),
                    )))
                }
            }
            SExpr::If(bexpr, if_expr, else_expr) => match bexpr.simplify(base_time, store) {
                Resolved(Value::Bool(true)) => if_expr.simplify(base_time, store),
                Resolved(Value::Bool(false)) => else_expr.simplify(base_time, store),
                Unresolved(expr) => Unresolved(Box::new(SExpr::If(
                    expr,
                    if_expr.clone(),
                    else_expr.clone(),
                ))),
                Resolved(v) => unreachable!(
                    "Solving SExprA did not yield a boolean as the conditional to if-statement: v={:?}",
                    v
                ),
            },
            SExpr::Eval(_) => todo!(),
            SExpr::Defer(_) => unreachable!("Defer should not be reachable as an IndexedVarName"),
            SExpr::Update(_, _) => todo!(),
            SExpr::Default(sexpr, default) => match sexpr.simplify(base_time, store) {
                Resolved(v) if v == Value::Unknown => Resolved(default.clone()),
                Resolved(v) => Resolved(v),
                Unresolved(sexpr) => Unresolved(Box::new(SExpr::Default(sexpr, default.clone()))),
            },
            SExpr::Not(_) => todo!(),
            SExpr::List(_) => todo!(),
            SExpr::LIndex(_, _) => todo!(),
            SExpr::LAppend(_, _) => todo!(),
            SExpr::LConcat(_, _) => todo!(),
            SExpr::LHead(_) => todo!(),
            SExpr::LTail(_) => todo!(),
        }
    }
}

// SExprR
impl Simplifiable for SExpr<VarName> {
    fn simplify(&self, base_time: usize, store: &ConstraintStore) -> SimplifyResult<Box<Self>> {
        match self {
            SExpr::Val(i) => Resolved(i.clone()),
            SExpr::BinOp(e1, e2, op) => {
                match (e1.simplify(base_time, store), e2.simplify(base_time, store)) {
                    (Resolved(e1), Resolved(e2)) => Resolved(binop_table(e1, e2, op.clone())),
                    // Does not reuse the previous e1 and e2s as the subexpressions may have been simplified
                    (Unresolved(ue), Resolved(re)) | (Resolved(re), Unresolved(ue)) => Unresolved(
                        Box::new(SExpr::BinOp(ue, Box::new(SExpr::Val(re)), op.clone())),
                    ),
                    (Unresolved(e1), Unresolved(e2)) => {
                        Unresolved(Box::new(SExpr::BinOp(e1, e2, op.clone())))
                    }
                }
            }
            SExpr::Var(name) => Unresolved(Box::new(SExpr::Var(name.clone()))),
            SExpr::SIndex(expr, rel_time, default) => {
                if *rel_time == 0 {
                    expr.simplify(base_time, store)
                } else {
                    // Attempt to partially solve the expression and return unresolved
                    match expr.simplify(base_time, store) {
                        Unresolved(expr) => Unresolved(Box::new(SExpr::SIndex(
                            expr.clone(),
                            *rel_time,
                            default.clone(),
                        ))),
                        Resolved(val) => Unresolved(Box::new(SExpr::SIndex(
                            Box::new(SExpr::Val(val)),
                            *rel_time,
                            default.clone(),
                        ))),
                    }
                }
            }
            SExpr::If(bexpr, if_expr, else_expr) => match bexpr.simplify(base_time, store) {
                Resolved(Value::Bool(true)) => if_expr.simplify(base_time, store),
                Resolved(Value::Bool(false)) => else_expr.simplify(base_time, store),
                Unresolved(expr) => Unresolved(Box::new(SExpr::If(
                    expr,
                    if_expr.clone(),
                    else_expr.clone(),
                ))),
                Resolved(v) => unreachable!(
                    "Solving SExpr did not yield a boolean as the conditional to if-statement: v={:?}",
                    v
                ),
            },
            SExpr::Eval(_) => todo!(),
            SExpr::Defer(expr) => {
                // Important to remember here that what we return here is the new "state" of the
                // defer in `output_exprs`.
                //
                // Find the defer str - a bit ugly with all the edge cases
                let defer_s = match expr.simplify(base_time, store) {
                    // Resolved: Only if the defer property is a string
                    Resolved(v) => match v {
                        Value::Str(defer_s) => defer_s,
                        Value::Unknown => return Unresolved(Box::new(SExpr::Defer(expr.clone()))),
                        val => panic!("Invalid defer property type {:?}", val),
                    },
                    Unresolved(expr) => match *expr.clone() {
                        // Var: Try to look it up
                        SExpr::Var(name) => {
                            let expr_opt = store
                                .get_from_input_streams(&name, &base_time)
                                .or_else(|| store.get_from_outputs_resolved(&name, &base_time));
                            if let Some(val) = expr_opt {
                                match val {
                                    Value::Str(defer_s) => defer_s.clone(),
                                    Value::Unknown => {
                                        let def = SExpr::Defer(Box::new(SExpr::Var(name)));
                                        return Unresolved(Box::new(def));
                                    }
                                    val => panic!("Invalid defer property type {:?}", val),
                                }
                            } else {
                                return Unresolved(Box::new(SExpr::Defer(Box::new(SExpr::Var(
                                    name,
                                )))));
                            }
                        }
                        simplified => {
                            // Last chance - try to manually solve it.
                            // Needed in case we are wrapped in e.g., Default or TimeIndex
                            let expr_abs =
                                simplified.to_absolute(base_time).simplify(base_time, store);
                            match expr_abs {
                                Resolved(Value::Str(defer_s)) => defer_s,
                                Resolved(Value::Unknown) | Unresolved(_) => {
                                    return Unresolved(Box::new(SExpr::Defer(Box::new(
                                        simplified,
                                    ))));
                                }
                                Resolved(val) => panic!("Invalid defer property type {:?}", val),
                            }
                        }
                    },
                };
                let defer_parse = &mut defer_s.as_str();
                let expr = lola_expression
                    .parse_next(defer_parse)
                    .expect("Parsing the defer string resulted in an invalid expression.");
                expr.simplify(base_time, store)
            }
            SExpr::Update(_, _) => todo!(),
            SExpr::Default(sexpr, default) => match sexpr.simplify(base_time, store) {
                Resolved(v) if v == Value::Unknown => Resolved(default.clone()),
                Resolved(v) => Resolved(v),
                Unresolved(sexpr) => Unresolved(Box::new(SExpr::Default(sexpr, default.clone()))),
            },
            SExpr::Not(_) => todo!(),
            SExpr::List(_) => todo!(),
            SExpr::LIndex(_, _) => todo!(),
            SExpr::LAppend(_, _) => todo!(),
            SExpr::LConcat(_, _) => todo!(),
            SExpr::LHead(_) => todo!(),
            SExpr::LTail(_) => todo!(),
        }
    }
}
