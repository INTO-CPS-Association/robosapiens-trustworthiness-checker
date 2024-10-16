use std::{fmt::Debug, fmt::Display, mem};

use winnow::Parser;

use crate::ast::*;
use crate::core::{ConcreteStreamData, IndexedVarName, VarName};

pub type SExprConstraint<VarT> = (VarT, SExpr<VarT>);
pub type SExprConstraintSolved<VarT> = (VarT, ConcreteStreamData);

#[derive(Debug)]
pub struct SExprConstraintStore<VarT: Debug> {
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
            resolved: Vec::new(),
            unresolved: Vec::new(),
        }
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
        }
    }
}

fn is_constraint_resolved<VarT: Debug>((_, s): &SExprConstraint<VarT>) -> bool {
    matches!(s, SExpr::Val(_))
}

fn to_indexed_bexpr(b: &BExpr<VarName>) -> BExpr<IndexedVarName> {
    use BExpr::*;
    match b {
        Val(b) => Val(b.clone()),
        Eq(a, c) => Eq(
            Box::new(to_indexed_expr(a, 0)),
            Box::new(to_indexed_expr(c, 0)),
        ),
        Le(a, c) => Le(
            Box::new(to_indexed_expr(a, 0)),
            Box::new(to_indexed_expr(c, 0)),
        ),
        Not(b) => Not(Box::new(to_indexed_bexpr(b))),
        And(a, b) => And(Box::new(to_indexed_bexpr(a)), Box::new(to_indexed_bexpr(b))),
        Or(a, b) => Or(Box::new(to_indexed_bexpr(a)), Box::new(to_indexed_bexpr(b))),
    }
}

fn to_indexed_expr(s: &SExpr<VarName>, current_index: usize) -> SExpr<IndexedVarName> {
    use SExpr::*;
    match s {
        Val(n) => Val(n.clone()),
        BinOp(a, b, op) => BinOp(
            Box::new(to_indexed_expr(a, current_index)),
            Box::new(to_indexed_expr(b, current_index)),
            *op,
        ),
        Var(VarName(v)) => Var(IndexedVarName(v.clone(), current_index)),
        Index(s, i, c) => Index(Box::new(to_indexed_expr(s, current_index)), *i, c.clone()),
        If(b, e1, e2) => If(
            Box::new(to_indexed_bexpr(b)),
            Box::new(to_indexed_expr(e1, current_index)),
            Box::new(to_indexed_expr(e2, current_index)),
        ),
        Eval(s) => Eval(Box::new(to_indexed_expr(s, current_index))),
        Defer(s) => Defer(Box::new(to_indexed_expr(s, current_index))),
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
    SExprConstraintStore {
        resolved,
        unresolved,
    }
}
pub trait PartialEvaluable<VarT: Eq + Clone + IndexableVar> {
    fn partial_eval(&self, cs: &SExprConstraintStore<VarT>, time: usize) -> Self;
}

impl PartialEvaluable<IndexedVarName> for SExpr<IndexedVarName> {
    fn partial_eval(&self, cs: &SExprConstraintStore<IndexedVarName>, time: usize) -> Self {
        use ConcreteStreamData::*;
        use SExpr::*;
        match self {
            Val(s) => Val(s.clone()),
            BinOp(a, b, op) if *op == SBinOp::Plus || *op == SBinOp::Mult => {
                let a_s = a.partial_eval(cs, time);
                let b_s = b.partial_eval(cs, time);
                match (a_s, b_s) {
                    // TODO: Sort other datatypes after exprs
                    (Val(Int(n1)), Val(Int(n2))) => Val(Int(n1 + n2)),
                    (Val(Int(n1)), b1) => {
                        BinOp(Box::new(b1), Box::new(Val(Int(n1))), *op).partial_eval(cs, time)
                    }
                    (BinOp(a1, b1, inner_op), c1) if inner_op == *op => {
                        BinOp(a1, Box::new(BinOp(b1, Box::new(c1), *op)), inner_op)
                            .partial_eval(cs, time)
                    }
                    // Explicitly match the variables to avoid use after move
                    (a_ss, b_ss) => BinOp(Box::new(a_ss), Box::new(b_ss), *op),
                }
            }
            BinOp(a, b, op) if *op == SBinOp::Minus => {
                let a_s = a.partial_eval(cs, time);
                let b_s = b.partial_eval(cs, time);
                match (a_s, b_s) {
                    (Val(Int(n1)), Val(Int(n2))) => Val(Int(n1 - n2)),
                    (a_ss, b_ss) => BinOp(Box::new(a_ss), Box::new(b_ss), *op),
                }
            }
            BinOp(_, _, _) => unreachable!("Covered in other cases"),
            If(b, e1, e2) => {
                let b_s = b.partial_eval(cs, time);
                let e1_s = e1.partial_eval(cs, time);
                let e2_s = e2.partial_eval(cs, time);
                match b_s {
                    BExpr::Val(true) => e1_s,
                    BExpr::Val(false) => e2_s,
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
        }
    }
}

impl PartialEvaluable<IndexedVarName> for BExpr<IndexedVarName> {
    fn partial_eval(&self, cs: &SExprConstraintStore<IndexedVarName>, time: usize) -> Self {
        use BExpr::*;
        use ConcreteStreamData::*;
        match self {
            Val(bool) => Val(*bool),
            Eq(a, b) => {
                let a_s = a.partial_eval(cs, time);
                let b_s = b.partial_eval(cs, time);
                match (a_s, b_s) {
                    (SExpr::Val(x1), SExpr::Val(x2)) => Val(x1 == x2),
                    (a_ss, b_ss) => Eq(Box::new(a_ss), Box::new(b_ss)),
                }
            }
            Le(a, b) => {
                let a_s = a.partial_eval(cs, time);
                let b_s = b.partial_eval(cs, time);
                match (a_s, b_s) {
                    (SExpr::Val(Int(x1)), SExpr::Val(Int(x2))) => Val(x1 <= x2),
                    (a_ss, b_ss) => Le(Box::new(a_ss), Box::new(b_ss)),
                }
            }
            Not(b) => {
                let b_s = b.partial_eval(cs, time);
                match b_s {
                    Val(b1) => Val(!b1),
                    _ => Not(Box::new(b_s)),
                }
            }
            And(a, b) => {
                let a_s = a.partial_eval(cs, time);
                let b_s = b.partial_eval(cs, time);
                match (a_s, b_s) {
                    (Val(b1), Val(b2)) => Val(b1 && b2),
                    (Val(a1), b1) => And(Box::new(b1), Box::new(Val(a1))).partial_eval(cs, time),
                    (And(a1, b1), c1) => {
                        And(a1, Box::new(And(b1, Box::new(c1)))).partial_eval(cs, time)
                    }
                    (a_ss, b_ss) => And(Box::new(a_ss), Box::new(b_ss)),
                }
            }
            Or(a, b) => {
                let a_s = a.partial_eval(cs, time);
                let b_s = b.partial_eval(cs, time);
                match (a_s, b_s) {
                    (Val(b1), Val(b2)) => Val(b1 || b2),
                    (Val(a1), b1) => Or(Box::new(b1), Box::new(Val(a1))).partial_eval(cs, time),
                    (Or(a1, b1), c1) => {
                        Or(a1, Box::new(Or(b1, Box::new(c1)))).partial_eval(cs, time)
                    }
                    (a_ss, b_ss) => Or(Box::new(a_ss), Box::new(b_ss)),
                }
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
                    Box::new(SExpr::Val(ConcreteStreamData::Int(1))),
                    Box::new(SExpr::Index(
                        Box::new(SExpr::Var(VarName("x".into()))),
                        -1,
                        ConcreteStreamData::Int(0),
                    )),
                    SBinOp::Plus,
                ),
            )],
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
                        Box::new(SExpr::Val(ConcreteStreamData::Int(1))),
                        Box::new(SExpr::Index(
                            Box::new(SExpr::Var(IndexedVarName("x".into(), 0))),
                            -1,
                            ConcreteStreamData::Int(0),
                        )),
                        SBinOp::Plus
                    ),
                )],
            }
        );
        assert_eq!(
            to_indexed_constraints(&recursive_constraints(), 4),
            SExprConstraintStore {
                resolved: vec![],
                unresolved: vec![(
                    IndexedVarName("x".into(), 4),
                    SExpr::BinOp(
                        Box::new(SExpr::Val(ConcreteStreamData::Int(1))),
                        Box::new(SExpr::Index(
                            Box::new(SExpr::Var(IndexedVarName("x".into(), 4))),
                            -1,
                            ConcreteStreamData::Int(0),
                        ),),
                        SBinOp::Plus,
                    ),
                )],
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
                resolved: vec![(IndexedVarName("x".into(), 0), ConcreteStreamData::Int(1)),],
                unresolved: vec![],
            }
        );
        let constraints = to_indexed_constraints(&recursive_constraints(), 1);
        constraints.solve_step(1);
        // constraints.solve_step(0);
        // constraints.solve_step(0);
        assert_eq!(
            constraints,
            SExprConstraintStore {
                resolved: vec![(IndexedVarName("x".into(), 1), ConcreteStreamData::Int(0)),],
                unresolved: vec![],
            }
        )
    }
}
