use std::collections::BTreeMap;
use std::fmt::Debug;

use crate::ast::*;
use crate::core::Value;
use crate::core::{IndexedVarName, VarName};

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
    pub resolved_this_step: ValStream,
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
            resolved_this_step: BTreeMap::new(),
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

    pub fn get_from_output_exprs(&self, name: &VarName) -> Option<&SExpr<VarName>> {
        self.output_exprs.get(name)
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
            SExpr::Index(expr, offset, default) => {
                // Determine if it is something that can eventually be solved. If not, transform it to a lit
                let absolute_time = base_time as isize + offset;
                if absolute_time < 0 {
                    SExpr::Val(default.clone())
                } else {
                    SExpr::Index(
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
            SExpr::Defer(_) => todo!(),
            SExpr::Update(_, _) => todo!(),
            SExpr::Eq(_, _) => todo!(),
            SExpr::Le(_, _) => todo!(),
            SExpr::Not(_) => todo!(),
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
                    (Unresolved(ue), Resolved(re)) | (Resolved(re), Unresolved(ue)) => {
                        Unresolved(Box::new(SExpr::BinOp(ue, Box::new(SExpr::Val(re)), op.clone())))
                    }
                    (Unresolved(e1), Unresolved(e2)) => Unresolved(Box::new(SExpr::BinOp(e1, e2, op.clone()))),
                }
            }
            SExpr::Var(name) => {
                let name= VarName(name.0.clone());
                // Check if we have a value inside resolved or input values
                if let Some(v) = store.get_from_outputs_resolved(&name, &base_time)
                    .or_else(|| store.get_from_input_streams(&name, &base_time)) {
                    return Resolved(v.clone());
                }
                // Otherwise it must be inside unresolved
                if let Some(expr) = store.get_from_outputs_unresolved(&name, &base_time) {
                    Unresolved(Box::new(expr.clone()))
                } else {
                    unreachable!("Var({:?}, {:?}) does not exist", name, base_time);
                }
            }
            SExpr::Index(expr, idx_time, default) => {
                // Should not be negative at this stage since it was indexed...
                let uidx_time = *idx_time as usize;
                if uidx_time <= base_time {
                    expr.simplify(uidx_time, store)
                } else {
                    Unresolved(Box::new(SExpr::Index(expr.clone(), *idx_time, default.clone())))
                }
            }
            SExpr::If(bexpr, if_expr, else_expr) => {
                match bexpr.simplify(base_time, store) {
                    Resolved(Value::Bool(true)) => if_expr.simplify(base_time, store),
                    Resolved(Value::Bool(false)) => else_expr.simplify(base_time, store),
                    Unresolved(expr) => Unresolved(Box::new(SExpr::If(expr, if_expr.clone(), else_expr.clone()))),
                    Resolved(v) => unreachable!("Solving SExprA did not yield a boolean as the conditional to if-statement: v={:?}", v),
                }
            }
            SExpr::Eval(_) => todo!(),
            SExpr::Defer(_) => todo!(),
            SExpr::Update(_, _) => todo!(),
            SExpr::Eq(_, _) => todo!(),
            SExpr::Le(_, _) => todo!(),
            SExpr::Not(_) => todo!(),
        }
    }
}

impl Simplifiable for SExpr<VarName> {
    fn simplify(&self, base_time: usize, store: &ConstraintStore) -> SimplifyResult<Box<Self>> {
        // Implement function
        match self {
            SExpr::Val(i) => Resolved(i.clone()),
            SExpr::BinOp(e1, e2, op) => {
                match (e1.simplify(base_time, store), e2.simplify(base_time, store)) {
                    (Resolved(e1), Resolved(e2)) => Resolved(binop_table(e1, e2, op.clone())),
                    // Does not reuse the previous e1 and e2s as the subexpressions may have been simplified
                    (Unresolved(ue), Resolved(re)) | (Resolved(re), Unresolved(ue)) => {
                        Unresolved(Box::new(SExpr::BinOp(ue, Box::new(SExpr::Val(re)), op.clone())))
                    }
                    (Unresolved(e1), Unresolved(e2)) => Unresolved(Box::new(SExpr::BinOp(e1, e2, op.clone()))),
                }
            }
            SExpr::Var(name) => {
                Unresolved(Box::new(SExpr::Var(name.clone())))
            }
            SExpr::Index(expr, rel_time, default) => {
                if *rel_time == 0 {
                    expr.simplify(base_time, store)
                } else {
                    // Attempt to partially solve the expression and return unresolved
                    match expr.simplify(base_time, store) {
                        Unresolved(expr) => Unresolved(Box::new(SExpr::Index(expr.clone(), *rel_time, default.clone()))),
                        Resolved(val) => Unresolved(Box::new(SExpr::Index( Box::new(SExpr::Val(val)), *rel_time, default.clone()))),
                    }
                }
            }
            SExpr::If(bexpr, if_expr, else_expr) => {
                match bexpr.simplify(base_time, store) {
                    Resolved(Value::Bool(true)) => if_expr.simplify(base_time, store),
                    Resolved(Value::Bool(false)) => else_expr.simplify(base_time, store),
                    Unresolved(expr) => Unresolved(Box::new(SExpr::If(expr, if_expr.clone(), else_expr.clone()))),
                    Resolved(v) => unreachable!("Solving SExpr did not yield a boolean as the conditional to if-statement: v={:?}", v),
                }
            }
            SExpr::Eval(_) => todo!(),
            SExpr::Defer(_) => todo!(),
            SExpr::Update(_, _) => todo!(),
            SExpr::Eq(_, _) => todo!(),
            SExpr::Le(_, _) => todo!(),
            SExpr::Not(_) => todo!(),
        }
    }
}
