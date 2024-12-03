use std::collections::BTreeMap;
use std::{fmt::Debug, fmt::Display, mem};

use winnow::Parser;

use crate::ast::*;
use crate::core::Value;
use crate::core::{IndexedVarName, VarName};

pub type SExprConstraint<VarT> = (VarT, SExpr<VarT>);
pub type SExprConstraintSolved<VarT> = (VarT, Value);

// TODO: Replace Stream with IndexedVarName
pub type Stream<T> = BTreeMap<VarName, Vec<(usize, T)>>;
pub type ValStream = Stream<Value>;
pub type SExprStream = Stream<SExpr<IndexedVarName>>;

#[derive(Debug)]
pub struct SExprConstraintStore<VarT: Debug> {
    pub input_streams: ValStream,
    pub output_exprs: BTreeMap<VarName, SExpr<VarName>>,
    pub outputs_resolved: ValStream,
    pub outputs_unresolved: SExprStream,
    pub resolved: Vec<SExprConstraintSolved<VarT>>,
    pub unresolved: Vec<SExprConstraint<VarT>>,
}

pub fn model_constraints(model: LOLASpecification) -> SExprConstraintStore<VarName> {
    let mut constraints = SExprConstraintStore::default();
    for (var, sexpr) in model.exprs.iter() {
        constraints.add_constraint((VarName(var.0.clone()), sexpr.clone()));
    }
    constraints
}

impl<VarT: Debug> Default for SExprConstraintStore<VarT> {
    fn default() -> Self {
        SExprConstraintStore {
            input_streams: BTreeMap::new(),
            output_exprs: BTreeMap::new(),
            outputs_resolved: BTreeMap::new(),
            outputs_unresolved: BTreeMap::new(),
            resolved: Vec::new(),
            unresolved: Vec::new(),
        }
    }
}

impl<VarT: Debug> SExprConstraintStore<VarT> {
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

impl<VarT: Clone + Eq + Debug> SExprConstraintStore<VarT> {
    pub fn resolved_exprs(&self) -> Vec<SExprConstraint<VarT>> {
        self.resolved
            .iter()
            .map(|(v, s)| (v.clone(), SExpr::Val(s.clone())))
            .collect()
    }

    fn sort_resolved(&mut self) {
        for (v, sexpr) in mem::take(&mut self.unresolved).into_iter() {
            if let SExpr::Val(x) = sexpr {
                self.resolved.push((v, x));
            } else {
                self.unresolved.push((v, sexpr));
            }
        }

        for x in self.unresolved.iter() {
            assert!(!is_constraint_resolved(x));
        }
    }

    pub fn add_constraint<'a>(&'a mut self, (v, s): SExprConstraint<VarT>) -> &'a mut Self {
        self.unresolved.push((v, s));
        self.sort_resolved();
        self
    }

    pub fn add_constraints<'a>(&'a mut self, cs: Vec<SExprConstraint<VarT>>) -> &'a mut Self {
        self.unresolved.extend(cs);
        self.sort_resolved();
        self
    }

    pub fn add_resolved<'a>(&'a mut self, (v, s): SExprConstraintSolved<VarT>) -> &'a mut Self {
        self.resolved.push((v, s));
        self
    }

    pub fn add_resolveds<'a>(&'a mut self, cs: Vec<SExprConstraintSolved<VarT>>) -> &'a mut Self {
        self.resolved.extend(cs);
        self
    }

    pub fn extend<'a>(&'a mut self, cs: SExprConstraintStore<VarT>) -> &'a mut Self {
        self.add_constraints(cs.unresolved)
            .add_resolveds(cs.resolved)
    }

    pub fn match_var(&self, v: &VarT) -> Option<SExpr<VarT>> {
        for (var, sexpr) in self.resolved_exprs().iter() {
            if *v == *var {
                return Some(sexpr.clone());
            }
        }

        for (var, sexpr) in self.unresolved.iter() {
            if *v == *var {
                return Some(sexpr.clone());
            }
        }

        return None;
    }
}

impl<VarT: Display + Debug> Display for SExprConstraintStore<VarT> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Resolved: [\n")?;
        for (var, sexpr) in self.resolved.iter() {
            write!(f, "  {} -> {},\n", var, sexpr)?;
        }
        write!(f, "]\nUnresolved: [\n")?;
        for (var, sexpr) in self.unresolved.iter() {
            write!(f, "  {} -> {},\n", var, sexpr)?;
        }
        write!(f, "]\n")
    }
}

impl<VarT: Eq + Debug> PartialEq for SExprConstraintStore<VarT> {
    fn eq(&self, other: &Self) -> bool {
        self.resolved == other.resolved && self.unresolved == other.unresolved
    }
}
impl<VarT: Eq + Debug> Eq for SExprConstraintStore<VarT> {}

impl<VarT: Clone + Debug> Clone for SExprConstraintStore<VarT> {
    fn clone(&self) -> Self {
        SExprConstraintStore {
            resolved: self.resolved.clone(),
            unresolved: self.unresolved.clone(),
            input_streams: self.input_streams.clone(),
            output_exprs: self.output_exprs.clone(),
            outputs_resolved: self.outputs_resolved.clone(),
            outputs_unresolved: self.outputs_unresolved.clone(),
        }
    }
}

fn is_constraint_resolved<VarT: Debug>((_, s): &SExprConstraint<VarT>) -> bool {
    matches!(s, SExpr::Val(_))
}

fn to_indexed_expr(s: &SExpr<VarName>, current_index: usize) -> SExpr<IndexedVarName> {
    use SExpr::*;
    match s {
        Val(n) => Val(n.clone()),
        BinOp(a, b, op) => BinOp(
            Box::new(to_indexed_expr(a, current_index)),
            Box::new(to_indexed_expr(b, current_index)),
            op.clone(),
        ),
        Eq(a, b) => Eq(
            Box::new(to_indexed_expr(a, current_index)),
            Box::new(to_indexed_expr(b, current_index)),
        ),
        Le(a, b) => Le(
            Box::new(to_indexed_expr(a, current_index)),
            Box::new(to_indexed_expr(b, current_index)),
        ),
        Not(b) => Not(Box::new(to_indexed_expr(b, current_index))),
        Var(VarName(v)) => Var(IndexedVarName(v.clone(), current_index)),
        Index(s, i, c) => Index(Box::new(to_indexed_expr(s, current_index)), *i, c.clone()),
        If(b, e1, e2) => If(
            Box::new(to_indexed_expr(b, current_index)),
            Box::new(to_indexed_expr(e1, current_index)),
            Box::new(to_indexed_expr(e2, current_index)),
        ),
        Eval(s) => Eval(Box::new(to_indexed_expr(s, current_index))),
        Defer(s) => Defer(Box::new(to_indexed_expr(s, current_index))),
        Update(e1, e2) => Update(
            Box::new(to_indexed_expr(e1, current_index)),
            Box::new(to_indexed_expr(e2, current_index)),
        ),
    }
}

pub fn to_indexed_constraints(
    cs: &SExprConstraintStore<VarName>,
    current_index: usize,
) -> SExprConstraintStore<IndexedVarName> {
    let resolved: Vec<SExprConstraintSolved<IndexedVarName>> = cs
        .resolved
        .iter()
        .map(|(v, x)| match v {
            VarName(u) => (IndexedVarName(u.clone(), current_index), x.clone()),
        })
        .collect();
    let unresolved: Vec<SExprConstraint<IndexedVarName>> = cs
        .unresolved
        .iter()
        .map(|(v, s)| match v {
            VarName(u) => (
                IndexedVarName(u.clone(), current_index),
                to_indexed_expr(s, current_index),
            ),
        })
        .collect();
    // TODO: Fix these...
    let input_streams = BTreeMap::new();
    let output_exprs = BTreeMap::new();
    let outputs_resolved = BTreeMap::new();
    let outputs_unresolved = BTreeMap::new();
    let time = 0;
    SExprConstraintStore {
        resolved,
        unresolved,
        input_streams,
        output_exprs,
        outputs_resolved,
        outputs_unresolved,
    }
}

// Trait for indexing a variable producing a new SExpr
pub trait IndexableVar: Debug {
    fn index(&self, i: isize, c: &Value) -> SExpr<Self>
    where
        Self: Sized;
}

impl IndexableVar for VarName {
    // For unindexed variables, indexing just produces the same expression
    fn index(&self, i: isize, c: &Value) -> SExpr<VarName> {
        SExpr::Index(Box::new(SExpr::Var(self.clone())), i, c.clone())
    }
}

impl IndexableVar for IndexedVarName {
    // For indexed variables, we can actually attempt to change the index on the underlying variable
    fn index(&self, i: isize, c: &Value) -> SExpr<IndexedVarName> {
        use SExpr::*;
        match self {
            // If the shifted index is positive, we can just shift the index
            // attached to the variable
            IndexedVarName(name, j) if i.wrapping_add_unsigned(*j) >= 0 => Var(IndexedVarName(
                name.clone(),
                i.wrapping_add_unsigned(*j) as usize,
            )),
            // If not the indexed variable is replaced with the default value
            IndexedVarName(_, _) => Val(c.clone()),
        }
    }
}

pub trait PartialEvaluable<VarT: Eq + Clone + IndexableVar> {
    fn partial_eval(&self, cs: &SExprConstraintStore<VarT>, time: usize) -> Self;
}

impl PartialEvaluable<IndexedVarName> for SExpr<IndexedVarName> {
    fn partial_eval(&self, cs: &SExprConstraintStore<IndexedVarName>, time: usize) -> Self {
        use SBinOp::*;
        use SExpr::*;
        use Value::*;
        match self {
            Val(s) => Val(s.clone()),
            BinOp(a, b, op) if *op == IOp(IntBinOp::Add) || *op == IOp(IntBinOp::Mul) => {
                let a_s = a.partial_eval(cs, time);
                let b_s = b.partial_eval(cs, time);
                match (a_s, b_s) {
                    // TODO: Sort other datatypes after exprs
                    (Val(Int(n1)), Val(Int(n2))) => Val(Int(if *op == IOp(IntBinOp::Add) {
                        n1 + n2
                    } else {
                        n1 * n2
                    })),
                    (Val(Int(n1)), b1) => BinOp(Box::new(b1), Box::new(Val(Int(n1))), op.clone())
                        .partial_eval(cs, time),
                    (BinOp(a1, b1, inner_op), c1) if inner_op == *op => {
                        BinOp(a1, Box::new(BinOp(b1, Box::new(c1), op.clone())), inner_op)
                            .partial_eval(cs, time)
                    }
                    // Explicitly match the variables to avoid use after move
                    (a_ss, b_ss) => BinOp(Box::new(a_ss), Box::new(b_ss), op.clone()),
                }
            }
            BinOp(a, b, op) if *op == IOp(IntBinOp::Sub) => {
                let a_s = a.partial_eval(cs, time);
                let b_s = b.partial_eval(cs, time);
                match (a_s, b_s) {
                    (Val(Int(n1)), Val(Int(n2))) => Val(Int(n1 - n2)),
                    (a_ss, b_ss) => BinOp(Box::new(a_ss), Box::new(b_ss), op.clone()),
                }
            }
            BinOp(a, b, op) if *op == IOp(IntBinOp::Div) => {
                let a_s = a.partial_eval(cs, time);
                let b_s = b.partial_eval(cs, time);
                match (a_s, b_s) {
                    (Val(Int(n1)), Val(Int(n2))) => Val(Int(n1 / n2)),
                    (a_ss, b_ss) => BinOp(Box::new(a_ss), Box::new(b_ss), op.clone()),
                }
            }
            Eq(a, b) => {
                let a_s = a.partial_eval(cs, time);
                let b_s = b.partial_eval(cs, time);
                match (a_s, b_s) {
                    (SExpr::Val(x1), SExpr::Val(x2)) => Val((x1 == x2).into()),
                    (a_ss, b_ss) => Eq(Box::new(a_ss), Box::new(b_ss)),
                }
            }
            BinOp(a, b, op) if *op == BOp(BoolBinOp::And) || *op == BOp(BoolBinOp::Or) => {
                let a_s = a.partial_eval(cs, time);
                let b_s = b.partial_eval(cs, time);
                match (a_s, b_s) {
                    // TODO: Sort other datatypes after exprs
                    (Val(Bool(n1)), Val(Bool(n2))) => Val((if *op == BOp(BoolBinOp::And) {
                        n1 && n2
                    } else {
                        n1 || n2
                    })
                    .into()),
                    (Val(Bool(n1)), b1) => BinOp(Box::new(b1), Box::new(Val(Bool(n1))), op.clone())
                        .partial_eval(cs, time),
                    (BinOp(a1, b1, inner_op), c1) if inner_op == *op => {
                        BinOp(a1, Box::new(BinOp(b1, Box::new(c1), op.clone())), inner_op)
                            .partial_eval(cs, time)
                    }
                    // Explicitly match the variables to avoid use after move
                    (a_ss, b_ss) => BinOp(Box::new(a_ss), Box::new(b_ss), op.clone()),
                }
            }
            Le(a, b) => {
                let a_s = a.partial_eval(cs, time);
                let b_s = b.partial_eval(cs, time);
                match (a_s, b_s) {
                    (SExpr::Val(Int(x1)), SExpr::Val(Int(x2))) => Val((x1 <= x2).into()),
                    (a_ss, b_ss) => Le(Box::new(a_ss), Box::new(b_ss)),
                }
            }
            Not(b) => {
                let b_s = b.partial_eval(cs, time);
                match b_s {
                    Val(Value::Bool(b1)) => Val((!b1).into()),
                    _ => Not(Box::new(b_s)),
                }
            }

            BinOp(_, _, _) => unreachable!("Covered in other cases"),
            If(b, e1, e2) => {
                let b_s = b.partial_eval(cs, time);
                let e1_s = e1.partial_eval(cs, time);
                let e2_s = e2.partial_eval(cs, time);
                match b_s {
                    SExpr::Val(Value::Bool(true)) => e1_s,
                    SExpr::Val(Value::Bool(false)) => e2_s,
                    _ if e1_s == e2_s => e1_s,
                    _ => If(Box::new(b_s), Box::new(e1_s), Box::new(e2_s)),
                }
            }
            Index(s, i, c) => {
                let shifted_time: isize = TryInto::<isize>::try_into(time).unwrap() + *i;
                println!("shifted_time {:?}", shifted_time);
                if shifted_time >= 0 {
                    // panic!("before crash");
                    s.partial_eval(cs, shifted_time.try_into().unwrap())
                } else {
                    SExpr::Val(c.clone())
                }
            }
            Var(var) => match cs.match_var(var) {
                Some(sexpr) => sexpr,
                None => Var(var.clone()),
            },
            Eval(s) => match s.partial_eval(cs, time) {
                Val(Str(s)) => match crate::parser::lola_expression.parse_next(&mut s.as_str()) {
                    Ok(s_s) => to_indexed_expr(&s_s, 0).partial_eval(cs, time),
                    Err(_) => Eval(Box::new(Val(Str(s)))),
                },
                Val(x) => Val(x),
                s_s => Eval(Box::new(s_s)),
            },
            Defer(s) => match s.partial_eval(cs, time) {
                Val(Str(s)) => match crate::parser::lola_expression.parse_next(&mut s.as_str()) {
                    Ok(s_s) => to_indexed_expr(&s_s, 0).partial_eval(cs, time),
                    Err(_) => Defer(Box::new(Val(Str(s)))),
                },
                Val(x) => Val(x),
                s_s => Defer(Box::new(s_s)),
            },
            Update(_, _) => {
                todo!("MHK: Not sure how to implement this")
            }
        }
    }
}

impl SExprConstraintStore<IndexedVarName> {
    pub fn solve_step(&self, time: usize) -> SExprConstraintStore<IndexedVarName> {
        let unresolved = self
            .unresolved
            .iter()
            .map(|(v, s)| (v.clone(), s.partial_eval(self, time)))
            .collect();

        SExprConstraintStore::default()
            .add_constraints(unresolved)
            .add_resolveds(self.resolved.clone())
            .to_owned()
    }

    pub fn solve(&mut self, time: usize) {
        let cs_new = self.solve_step(time);

        if cs_new == *self {
            return;
        } else {
            *self = cs_new;

            self.solve(time);
        }
    }
}

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
    fn simplify(
        &self,
        base_time: usize,
        store: &SExprConstraintStore<VarName>,
    ) -> SimplifyResult<Box<Self>>;
}

// SExprA
impl Simplifiable for SExpr<IndexedVarName> {
    fn simplify(
        &self,
        base_time: usize,
        store: &SExprConstraintStore<VarName>,
    ) -> SimplifyResult<Box<Self>> {
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
    fn simplify(
        &self,
        base_time: usize,
        store: &SExprConstraintStore<VarName>,
    ) -> SimplifyResult<Box<Self>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::SExpr;

    use crate::core::IndexedVarName;

    fn recursive_constraints() -> SExprConstraintStore<VarName> {
        SExprConstraintStore {
            resolved: vec![],
            unresolved: vec![(
                VarName("x".into()),
                SExpr::BinOp(
                    Box::new(SExpr::Val(Value::Int(1))),
                    Box::new(SExpr::Index(
                        Box::new(SExpr::Var(VarName("x".into()))),
                        -1,
                        Value::Int(0),
                    )),
                    SBinOp::IOp(IntBinOp::Add),
                ),
            )],
            input_streams: BTreeMap::new(),
            output_exprs: BTreeMap::new(),
            outputs_resolved: BTreeMap::new(),
            outputs_unresolved: BTreeMap::new(),
        }
    }

    #[test]
    fn test_to_indexed_constraints() {
        assert_eq!(
            to_indexed_constraints(&recursive_constraints(), 0),
            SExprConstraintStore {
                resolved: vec![],
                unresolved: vec![(
                    IndexedVarName("x".into(), 0),
                    SExpr::BinOp(
                        Box::new(SExpr::Val(Value::Int(1))),
                        Box::new(SExpr::Index(
                            Box::new(SExpr::Var(IndexedVarName("x".into(), 0))),
                            -1,
                            Value::Int(0),
                        )),
                        SBinOp::IOp(IntBinOp::Add),
                    ),
                )],
                input_streams: BTreeMap::new(),
                output_exprs: BTreeMap::new(),
                outputs_resolved: BTreeMap::new(),
                outputs_unresolved: BTreeMap::new(),
            }
        );
        assert_eq!(
            to_indexed_constraints(&recursive_constraints(), 4),
            SExprConstraintStore {
                resolved: vec![],
                unresolved: vec![(
                    IndexedVarName("x".into(), 4),
                    SExpr::BinOp(
                        Box::new(SExpr::Val(Value::Int(1))),
                        Box::new(SExpr::Index(
                            Box::new(SExpr::Var(IndexedVarName("x".into(), 4))),
                            -1,
                            Value::Int(0),
                        ),),
                        SBinOp::IOp(IntBinOp::Add),
                    ),
                )],
                input_streams: BTreeMap::new(),
                output_exprs: BTreeMap::new(),
                outputs_resolved: BTreeMap::new(),
                outputs_unresolved: BTreeMap::new(),
            }
        );
    }

    #[ignore = "currently we can't handle recursive constraints in the solver as need a way to handle the inner indexes"]
    #[test]
    fn test_solve_indexed_constraints() {
        let mut constraints = to_indexed_constraints(&recursive_constraints(), 0);
        constraints.solve(0);
        // constraints.solve_step(0);
        // constraints.solve_step(0);
        assert_eq!(
            constraints,
            SExprConstraintStore {
                resolved: vec![(IndexedVarName("x".into(), 0), Value::Int(1)),],
                unresolved: vec![],
                input_streams: BTreeMap::new(),
                output_exprs: BTreeMap::new(),
                outputs_resolved: BTreeMap::new(),
                outputs_unresolved: BTreeMap::new(),
            }
        );
        let constraints = to_indexed_constraints(&recursive_constraints(), 1);
        constraints.solve_step(1);
        // constraints.solve_step(0);
        // constraints.solve_step(0);
        assert_eq!(
            constraints,
            SExprConstraintStore {
                resolved: vec![(IndexedVarName("x".into(), 1), Value::Int(0)),],
                unresolved: vec![],
                input_streams: BTreeMap::new(),
                output_exprs: BTreeMap::new(),
                outputs_resolved: BTreeMap::new(),
                outputs_unresolved: BTreeMap::new(),
            }
        )
    }
}
