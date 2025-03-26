use std::collections::BTreeMap;
use std::fmt::Debug;

use crate::core::Value;
use crate::core::VarName;
use crate::dep_manage::interface::DependencyManager;
use crate::lang::dynamic_lola::ast::*;
use crate::lang::dynamic_lola::parser::lola_expression;

// An SExpr with an absolute time
// Identical to SExpr except SIndex is unsigned, Var has a time index and certain DUP functions are
// removed (because they are not needed)
#[derive(Clone, PartialEq, Debug)]
pub enum SExprAbs {
    // if-then-else
    If(Box<Self>, Box<Self>, Box<Self>),

    // Stream indexing
    SIndex(
        // Inner SExpr e
        Box<Self>,
        // Index i
        usize,
        // Default c
        Value,
    ),

    // Arithmetic Stream expression
    Val(Value),

    BinOp(Box<Self>, Box<Self>, SBinOp),

    Var(usize, VarName),

    // Eval
    Eval(Box<Self>),
    Default(Box<Self>, Box<Self>),
    When(Box<Self>), // Becomes true after the first time .0 is not Unknown

    // Unary expressions (refactor if more are added...)
    Not(Box<Self>),

    // List and list expressions
    List(Vec<Self>),
    LIndex(Box<Self>, Box<Self>), // List index: First is list, second is index
    LAppend(Box<Self>, Box<Self>), // List append -- First is list, second is el to add
    LConcat(Box<Self>, Box<Self>), // List concat -- First is list, second is other list
    LHead(Box<Self>),             // List head -- get first element of list
    LTail(Box<Self>),             // List tail -- get all but first element of list
}

pub type SyncStream<T> = BTreeMap<VarName, Vec<(usize, T)>>;
pub type ValStream = SyncStream<Value>;
pub type SExprStream = SyncStream<SExprAbs>;

#[derive(Debug, Clone)]
// A ConstraintStore is the environment for the streams
pub struct ConstraintStore {
    pub input_streams: ValStream,
    pub output_exprs: BTreeMap<VarName, SExpr>,
    pub outputs_resolved: ValStream,
    pub outputs_unresolved: SExprStream,
}

pub fn model_constraints(model: LOLASpecification) -> ConstraintStore {
    let mut constraints = ConstraintStore::default();
    for (var, sexpr) in model.exprs.iter() {
        constraints.output_exprs.insert(var.clone(), sexpr.clone());
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

    pub fn get_from_outputs_unresolved(&self, name: &VarName, idx: &usize) -> Option<&SExprAbs> {
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

#[derive(Debug, Clone)]
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
        (Int(i1), Int(i2), NOp(iop)) => match iop {
            NumericalBinOp::Add => Int(i1 + i2),
            NumericalBinOp::Sub => Int(i1 - i2),
            NumericalBinOp::Mul => Int(i1 * i2),
            NumericalBinOp::Div => Int(i1 / i2),
            NumericalBinOp::Mod => Int(i1 % i2),
        },
        (Float(i1), Int(i2), NOp(iop)) => match iop {
            NumericalBinOp::Add => Float(i1 + i2 as f32),
            NumericalBinOp::Sub => Float(i1 - i2 as f32),
            NumericalBinOp::Mul => Float(i1 * i2 as f32),
            NumericalBinOp::Div => Float(i1 / i2 as f32),
            NumericalBinOp::Mod => Float(i1 % i2 as f32),
        },
        (Int(i1), Float(i2), NOp(iop)) => match iop {
            NumericalBinOp::Add => Float(i1 as f32 + i2),
            NumericalBinOp::Sub => Float(i1 as f32 - i2),
            NumericalBinOp::Mul => Float(i1 as f32 * i2),
            NumericalBinOp::Div => Float(i1 as f32 / i2),
            NumericalBinOp::Mod => Float(i1 as f32 % i2),
        },
        (Float(i1), Float(i2), NOp(iop)) => match iop {
            NumericalBinOp::Add => Float(i1 + i2),
            NumericalBinOp::Sub => Float(i1 - i2),
            NumericalBinOp::Mul => Float(i1 * i2),
            NumericalBinOp::Div => Float(i1 / i2),
            NumericalBinOp::Mod => Float(i1 % i2),
        },
        (Bool(b1), Bool(b2), BOp(bop)) => match bop {
            BoolBinOp::Or => Bool(b1 || b2),
            BoolBinOp::And => Bool(b1 && b2),
        },
        (Str(mut s1), Str(s2), SOp(sop)) => match sop {
            StrBinOp::Concat => {
                s1.push_str(&s2);
                Str(s1)
            }
        },
        (Str(s1), Str(s2), COp(sop)) => match sop {
            CompBinOp::Eq => Bool(s1 == s2),
            CompBinOp::Le => Bool(s1 <= s2),
        },
        (v1, v2, op) => {
            unreachable!(
                "Trying to solve BinOp with incorrect Value types. v1: {:?}. op: {:?}. v2: {:?}",
                v1, op, v2
            );
        }
    }
}

impl SExpr {
    pub fn to_absolute(&self, base_time: usize) -> SExprAbs {
        match self {
            SExpr::Val(val) => SExprAbs::Val(val.clone()),
            SExpr::BinOp(lhs, rhs, op) => SExprAbs::BinOp(
                Box::new(lhs.to_absolute(base_time)),
                Box::new(rhs.to_absolute(base_time)),
                op.clone(),
            ),
            SExpr::Var(name) => SExprAbs::Var(base_time, name.clone()),
            SExpr::SIndex(expr, offset, default) => {
                // Determine if it is something that can eventually be solved. If not, transform it to a lit
                let absolute_time = base_time as isize + offset;
                if absolute_time < 0 {
                    SExprAbs::Val(default.clone())
                } else {
                    SExprAbs::SIndex(
                        Box::new(expr.to_absolute(base_time)),
                        absolute_time.abs() as usize,
                        default.clone(),
                    )
                }
            }
            SExpr::If(bexpr, if_expr, else_expr) => SExprAbs::If(
                Box::new(bexpr.to_absolute(base_time)),
                Box::new(if_expr.to_absolute(base_time)),
                Box::new(else_expr.to_absolute(base_time)),
            ),
            SExpr::Eval(_) => todo!(),
            SExpr::Defer(_) => SExprAbs::Val(Value::Unknown),
            SExpr::Update(lhs, _) => lhs.to_absolute(base_time),
            SExpr::Default(expr, default) => SExprAbs::Default(
                Box::new(expr.to_absolute(base_time)),
                Box::new(default.to_absolute(base_time)),
            ),
            SExpr::Not(_) => todo!(),
            SExpr::List(_) => todo!(),
            SExpr::LIndex(_, _) => todo!(),
            SExpr::LAppend(_, _) => todo!(),
            SExpr::LConcat(_, _) => todo!(),
            SExpr::LHead(_) => todo!(),
            SExpr::LTail(_) => todo!(),
            SExpr::IsDefined(_) => todo!(),
            SExpr::When(_) => todo!(),
        }
    }
}

pub trait Simplifiable {
    fn simplify(
        &self,
        base_time: usize,
        store: &ConstraintStore,
        var: &VarName,
        deps: &mut DependencyManager,
    ) -> SimplifyResult<Box<Self>>;
}

impl Simplifiable for SExprAbs {
    fn simplify(
        &self,
        base_time: usize,
        store: &ConstraintStore,
        var: &VarName,
        deps: &mut DependencyManager,
    ) -> SimplifyResult<Box<Self>> {
        match self {
            SExprAbs::Val(i) => Resolved(i.clone()),
            SExprAbs::BinOp(e1, e2, op) => {
                match (
                    e1.simplify(base_time, store, var, deps),
                    e2.simplify(base_time, store, var, deps),
                ) {
                    (Resolved(e1), Resolved(e2)) => Resolved(binop_table(e1, e2, op.clone())),
                    // Does not reuse the previous e1 and e2s as the subexpressions may have been simplified
                    (Unresolved(ue), Resolved(re)) | (Resolved(re), Unresolved(ue)) => Unresolved(
                        Box::new(SExprAbs::BinOp(ue, Box::new(SExprAbs::Val(re)), op.clone())),
                    ),
                    (Unresolved(e1), Unresolved(e2)) => {
                        Unresolved(Box::new(SExprAbs::BinOp(e1, e2, op.clone())))
                    }
                }
            }
            SExprAbs::Var(_, var_name) => {
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
            SExprAbs::SIndex(expr, idx_time, default) => {
                // Should not be negative at this stage since it was indexed...
                let uidx_time = *idx_time as usize;
                if uidx_time <= base_time {
                    expr.simplify(uidx_time, store, var, deps)
                } else {
                    Unresolved(Box::new(SExprAbs::SIndex(
                        expr.clone(),
                        *idx_time,
                        default.clone(),
                    )))
                }
            }
            SExprAbs::If(bexpr, if_expr, else_expr) => {
                match bexpr.simplify(base_time, store, var, deps) {
                    Resolved(Value::Bool(true)) => if_expr.simplify(base_time, store, var, deps),
                    Resolved(Value::Bool(false)) => else_expr.simplify(base_time, store, var, deps),
                    Unresolved(expr) => Unresolved(Box::new(SExprAbs::If(
                        expr,
                        if_expr.clone(),
                        else_expr.clone(),
                    ))),
                    Resolved(v) => unreachable!(
                        "Solving SExprAbs did not yield a boolean as the conditional to if-statement: v={:?}",
                        v
                    ),
                }
            }
            SExprAbs::Eval(_) => todo!(),
            SExprAbs::Default(sexpr, default) => {
                match sexpr.simplify(base_time, store, var, deps) {
                    Resolved(v) if v == Value::Unknown => {
                        default.simplify(base_time, store, var, deps)
                    }
                    Resolved(v) => Resolved(v),
                    Unresolved(sexpr) => {
                        Unresolved(Box::new(SExprAbs::Default(sexpr, default.clone())))
                    }
                }
            }
            SExprAbs::Not(_) => todo!(),
            SExprAbs::List(_) => todo!(),
            SExprAbs::LIndex(_, _) => todo!(),
            SExprAbs::LAppend(_, _) => todo!(),
            SExprAbs::LConcat(_, _) => todo!(),
            SExprAbs::LHead(_) => todo!(),
            SExprAbs::LTail(_) => todo!(),
            SExprAbs::When(_) => todo!(),
        }
    }
}

impl SExpr {
    fn is_solveable(&self, base_time: usize, store: &ConstraintStore) -> bool {
        match self {
            SExpr::Val(_) => true,
            SExpr::BinOp(e1, e2, _) => {
                e1.is_solveable(base_time, store) && e2.is_solveable(base_time, store)
            }
            SExpr::Var(name) => {
                // NOTE: Might return false if the Var simply hasn't been resolved yet
                // (i.e. it is in `outputs_unresolved`)
                let val = store
                    .get_from_outputs_resolved(&name, &base_time)
                    .or_else(|| store.get_from_input_streams(&name, &base_time));
                val.is_some() && val != Some(&Value::Unknown)
            }
            SExpr::SIndex(expr, rel_time, _) => {
                let new_time = (base_time as isize) + *rel_time;
                if new_time < 0 {
                    true
                } else {
                    expr.is_solveable(new_time as usize, store)
                }
            }
            SExpr::If(bexpr, if_expr, else_expr) => {
                bexpr.is_solveable(base_time, store)
                    && if_expr.is_solveable(base_time, store)
                    && else_expr.is_solveable(base_time, store)
            }
            SExpr::Defer(sexpr) => sexpr.is_solveable(base_time, store),
            SExpr::Eval(_) => todo!(),
            SExpr::Update(_, rhs) => {
                // Technically: (is_solveable(lhs) && is_solveable(rhs)) || is_solveable(rhs)
                // Remember: Solveable means the it can be solved indefinitely not just at current
                // time instant
                rhs.is_solveable(base_time, store)
            }
            SExpr::Default(_, _) => true,
            SExpr::Not(_) => todo!(),
            SExpr::List(_) => todo!(),
            SExpr::LIndex(_, _) => todo!(),
            SExpr::LAppend(_, _) => todo!(),
            SExpr::LConcat(_, _) => todo!(),
            SExpr::LHead(_) => todo!(),
            SExpr::LTail(_) => todo!(),
            SExpr::IsDefined(_) => todo!(),
            SExpr::When(_) => todo!(),
        }
    }
}

// SExprR
impl Simplifiable for SExpr {
    fn simplify(
        &self,
        base_time: usize,
        store: &ConstraintStore,
        var: &VarName,
        deps: &mut DependencyManager,
    ) -> SimplifyResult<Box<Self>> {
        match self {
            SExpr::Val(i) => Resolved(i.clone()),
            SExpr::BinOp(e1, e2, op) => {
                match (
                    e1.simplify(base_time, store, var, deps),
                    e2.simplify(base_time, store, var, deps),
                ) {
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
                    expr.simplify(base_time, store, var, deps)
                } else {
                    // Attempt to partially solve the expression and return unresolved
                    match expr.simplify(base_time, store, var, deps) {
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
            SExpr::If(bexpr, if_expr, else_expr) => {
                match bexpr.simplify(base_time, store, var, deps) {
                    Resolved(Value::Bool(true)) => if_expr.simplify(base_time, store, var, deps),
                    Resolved(Value::Bool(false)) => else_expr.simplify(base_time, store, var, deps),
                    Unresolved(expr) => Unresolved(Box::new(SExpr::If(
                        expr,
                        if_expr.clone(),
                        else_expr.clone(),
                    ))),
                    Resolved(v) => unreachable!(
                        "Solving SExpr did not yield a boolean as the conditional to if-statement: v={:?}",
                        v
                    ),
                }
            }
            SExpr::Eval(_) => todo!(),
            SExpr::Defer(expr) => {
                // Important to remember here that what we return here is the new "state" of the
                // defer in `output_exprs`.
                //
                // Find the defer str - a bit ugly with all the edge cases
                let defer_s = match expr.simplify(base_time, store, var, deps) {
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
                            let expr_abs = simplified
                                .to_absolute(base_time)
                                .simplify(base_time, store, var, deps);
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
                let new_expr = lola_expression
                    .parse_next(&mut defer_s.as_ref())
                    .expect("Parsing the defer string resulted in an invalid expression.");
                let res = new_expr.simplify(base_time, store, var, deps);
                match &res {
                    Resolved(val) => {
                        deps.remove_dependency(var, expr);
                        deps.add_dependency(var, &SExpr::Val(val.clone()));
                    }
                    Unresolved(new_expr) => {
                        deps.remove_dependency(var, expr);
                        deps.add_dependency(var, new_expr);
                    }
                }
                res
            }
            SExpr::Update(lhs, rhs) => {
                if rhs.is_solveable(base_time, store) {
                    return rhs.simplify(base_time, store, var, deps);
                }
                match lhs.simplify(base_time, store, var, deps) {
                    Resolved(val) => Unresolved(Box::new(SExpr::Update(
                        Box::new(SExpr::Val(val)),
                        rhs.clone(),
                    ))),
                    Unresolved(sexpr) => Unresolved(Box::new(SExpr::Update(sexpr, rhs.clone()))),
                }
            }
            SExpr::Default(sexpr, default) => match sexpr.simplify(base_time, store, var, deps) {
                Resolved(v) if v == Value::Unknown => default.simplify(base_time, store, var, deps),
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
            SExpr::IsDefined(_) => todo!(),
            SExpr::When(_) => todo!(),
        }
    }
}
