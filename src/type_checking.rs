use crate::ast::UntypedLOLA;
use crate::core::{
    ExpressionTyping, SemanticErrors, SemanticResult, TypeAnnotated, TypeCheckableSpecification,
    TypeContext,
};
use crate::lola_type_system::{BoolTypeSystem, LOLATypeSystem, StreamType};
use crate::{
    ast::{BExpr, SBinOp, SExpr},
    core::{SemanticError, TypeCheckableHelper, TypeSystem, Value},
    ConcreteStreamData, VarName,
};
use crate::{LOLASpecification, Specification, StreamExpr};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::Deref;

impl ExpressionTyping for BoolTypeSystem {
    type TypeSystem = BoolTypeSystem;
    type TypedExpr = SExprBool;

    fn type_of_expr(_: &Self::TypedExpr) -> <BoolTypeSystem as TypeSystem>::Type {
        StreamType::Bool
    }
}

impl StreamExpr<StreamType> for SExprTE {
    fn var(typ: StreamType, var: &VarName) -> Self {
        match typ {
            StreamType::Int => SExprTE::Int(SExprInt::Var(var.clone())),
            StreamType::Str => SExprTE::Str(SExprStr::Var(var.clone())),
            StreamType::Bool => SExprTE::Bool(SExprT::Var(var.clone())),
            StreamType::Unit => SExprTE::Unit(SExprT::Var(var.clone())),
        }
    }
}

impl ExpressionTyping for LOLATypeSystem {
    type TypeSystem = LOLATypeSystem;
    type TypedExpr = SExprTE;

    fn type_of_expr(expr: &Self::TypedExpr) -> <LOLATypeSystem as TypeSystem>::Type {
        match expr {
            SExprTE::Int(_) => StreamType::Int,
            SExprTE::Str(_) => StreamType::Str,
            SExprTE::Bool(_) => StreamType::Bool,
            SExprTE::Unit(_) => StreamType::Unit,
        }
    }
}

// Stream expressions - now with types
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SExprT<ValT: Value<LOLATypeSystem>> {
    If(Box<SExprBool>, Box<Self>, Box<Self>),

    // Stream indexing
    Index(
        // Inner SExpr e
        Box<Self>,
        // Index i
        isize,
        // Default c
        ValT,
    ),

    // Arithmetic Stream expression
    Val(ValT),

    Var(VarName),
}

#[derive(Clone, PartialEq, Eq, Debug)]

pub enum SExprBool {
    Val(bool),
    EqInt(SExprInt, SExprInt),
    EqStr(SExprStr, SExprStr),
    EqBool(SExprT<bool>, SExprT<bool>),
    EqUnit(SExprT<()>, SExprT<()>),
    LeInt(SExprInt, SExprInt),
    Not(Box<Self>),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SExprInt {
    If(Box<SExprBool>, Box<Self>, Box<Self>),

    // Stream indexing
    Index(
        // Inner SExpr e
        Box<Self>,
        // Index i
        isize,
        // Default c
        i64,
    ),

    // Arithmetic Stream expression
    Val(i64),

    BinOp(Box<Self>, Box<Self>, SBinOp),

    Var(VarName),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SExprStr {
    If(Box<SExprBool>, Box<Self>, Box<Self>),

    // Stream indexing
    Index(
        // Inner SExpr e
        Box<Self>,
        // Index i
        isize,
        // Default c
        String,
    ),

    // Arithmetic Stream expression
    Val(String),

    Var(VarName),

    // Eval
    Eval(Box<Self>),
}

// Stream expression typed enum
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SExprTE {
    Int(SExprInt),
    Str(SExprStr),
    Bool(SExprT<bool>),
    Unit(SExprT<()>),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TypedLOLASpecification {
    pub input_vars: Vec<VarName>,
    pub output_vars: Vec<VarName>,
    pub exprs: BTreeMap<VarName, SExprTE>,
    pub type_annotations: BTreeMap<VarName, StreamType>,
}

impl Specification<LOLATypeSystem> for TypedLOLASpecification {
    fn input_vars(&self) -> Vec<VarName> {
        self.input_vars.clone()
    }

    fn output_vars(&self) -> Vec<VarName> {
        self.output_vars.clone()
    }

    fn var_expr(&self, var: &VarName) -> Option<SExprTE> {
        self.exprs.get(var).cloned()
    }
}

impl TypeCheckableSpecification<UntypedLOLA, LOLATypeSystem, TypedLOLASpecification>
    for LOLASpecification
{
    fn type_check(&self) -> SemanticResult<TypedLOLASpecification> {
        let type_context = self.type_annotations.clone();
        let mut typed_exprs = BTreeMap::new();
        let mut errors = vec![];
        for (var, expr) in self.exprs.iter() {
            let mut ctx = type_context.clone();
            let typed_expr = expr.type_check_raw(&mut ctx, &mut errors);
            typed_exprs.insert(var, typed_expr);
        }
        if errors.is_empty() {
            Ok(TypedLOLASpecification {
                input_vars: self.input_vars.clone(),
                output_vars: self.output_vars.clone(),
                exprs: typed_exprs
                    .into_iter()
                    .map(|(k, v)| (k.clone(), v.unwrap()))
                    .collect(),
                type_annotations: self.type_annotations.clone(),
            })
        } else {
            Err(errors)
        }
    }
}

impl TypeAnnotated<LOLATypeSystem> for TypedLOLASpecification {
    fn type_of_var(&self, var: &VarName) -> Option<StreamType> {
        println!("Type of var: {:?}", var);
        self.type_annotations.get(var).cloned()
    }
}

impl TypeCheckableHelper<LOLATypeSystem> for ConcreteStreamData {
    fn type_check_raw(
        &self,
        _: &mut crate::core::TypeContext<LOLATypeSystem>,
        errs: &mut crate::core::SemanticErrors,
    ) -> Result<SExprTE, ()> {
        match self {
            ConcreteStreamData::Int(v) => Ok(SExprTE::Int(SExprInt::Val(*v))),
            ConcreteStreamData::Str(v) => Ok(SExprTE::Str(SExprStr::Val(v.clone()))),
            ConcreteStreamData::Bool(v) => Ok(SExprTE::Bool(SExprT::Val(*v))),
            ConcreteStreamData::Unit => Ok(SExprTE::Unit(SExprT::Val(()))),
            ConcreteStreamData::Unknown => {
                errs.push(crate::core::SemanticError::UnknownError(
                    format!(
                        "Stream expression {:?} not assigned a type before semantic analysis",
                        self
                    )
                    .into(),
                ));
                Err(())
            }
        }
    }
}

// Type check a binary operation
impl TypeCheckableHelper<LOLATypeSystem> for (SBinOp, &SExpr<VarName>, &SExpr<VarName>) {
    fn type_check_raw(
        &self,
        ctx: &mut crate::core::TypeContext<LOLATypeSystem>,
        errs: &mut crate::core::SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let (op, se1, se2) = *self;
        let se1_check = se1.type_check_raw(ctx, errs);
        let se2_check = se2.type_check_raw(ctx, errs);

        match (se1_check, se2_check) {
            (Ok(SExprTE::Int(se1)), Ok(SExprTE::Int(se2))) => Ok(SExprTE::Int(SExprInt::BinOp(
                Box::new(se1.clone()),
                Box::new(se2.clone()),
                op,
            ))),
            // Any other case where sub-expressions are Ok, but `op` is not supported
            (Ok(ste1), Ok(ste2)) => {
                errs.push(SemanticError::TypeError(
                    format!(
                        "Cannot apply binary function {:?} to expressions of type {:?} and {:?}",
                        op, ste1, ste2
                    )
                    .into(),
                ));
                Err(())
            }
            // If the underlying values already result in an error then simply propagate
            (Ok(_), Err(_)) | (Err(_), Ok(_)) | (Err(_), Err(_)) => Err(()),
        }
    }
}

// Type check an if expression
impl TypeCheckableHelper<LOLATypeSystem> for (&BExpr<VarName>, &SExpr<VarName>, &SExpr<VarName>) {
    fn type_check_raw(
        &self,
        ctx: &mut crate::core::TypeContext<LOLATypeSystem>,
        errs: &mut crate::core::SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let (b, se1, se2) = *self;
        let b_check = b.type_check_raw(ctx, errs);
        let se1_check = se1.type_check_raw(ctx, errs);
        let se2_check = se2.type_check_raw(ctx, errs);

        match (b_check, se1_check, se2_check) {
            (Ok(b), Ok(ste1), Ok(ste2)) => {
                // Matching on type-checked expressions. If same then Ok, else error.
                match (ste1, ste2) {
                    (SExprTE::Int(se1), SExprTE::Int(se2)) => Ok(SExprTE::Int(SExprInt::If(
                        Box::new(b.clone()),
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Str(se1), SExprTE::Str(se2)) => Ok(SExprTE::Str(SExprStr::If(
                        Box::new(b.clone()),
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Bool(se1), SExprTE::Bool(se2)) => Ok(SExprTE::Bool(SExprT::If(
                        Box::new(b.clone()),
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Unit(se1), SExprTE::Unit(se2)) => Ok(SExprTE::Unit(SExprT::If(
                        Box::new(b.clone()),
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (stenum1, stenum2) => {
                        errs.push(SemanticError::TypeError(
                            format!(
                                "Cannot create if-expression with two different types: {:?} and {:?}",
                                stenum1, stenum2
                            )
                                .into(),
                        ));
                        Err(())
                    }
                }
            }
            // If there's already an error in any branch, propagate the error
            _ => Err(()),
        }
    }
}

// Type check an index expression
impl TypeCheckableHelper<LOLATypeSystem> for (&SExpr<VarName>, isize, &ConcreteStreamData) {
    fn type_check_raw(
        &self,
        ctx: &mut crate::core::TypeContext<LOLATypeSystem>,
        errs: &mut crate::core::SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let (inner, idx, default) = *self;
        let inner_check = inner.type_check_raw(ctx, errs);

        match inner_check {
            Ok(ste) => match (ste, default) {
                (SExprTE::Int(se), ConcreteStreamData::Int(def)) => Ok(SExprTE::Int(
                    SExprInt::Index(Box::new(se.clone()), idx, *def),
                )),
                (SExprTE::Str(se), ConcreteStreamData::Str(def)) => Ok(SExprTE::Str(
                    SExprStr::Index(Box::new(se.clone()), idx, def.clone()),
                )),
                (SExprTE::Bool(se), ConcreteStreamData::Bool(def)) => Ok(SExprTE::Bool(
                    SExprT::Index(Box::new(se.clone()), idx, *def),
                )),
                (SExprTE::Unit(se), ConcreteStreamData::Unit) => {
                    Ok(SExprTE::Unit(SExprT::Index(Box::new(se.clone()), idx, ())))
                }
                (se, def) => {
                    errs.push(SemanticError::TypeError(
                        format!(
                            "Mismatched type in Index expression, expression and default does not match: {:?}",
                            (se, def)
                        )
                            .into(),
                    ));
                    Err(())
                }
            },
            // If there's already an error just propagate it
            Err(_) => Err(()),
        }
    }
}

// Type check a variable
impl TypeCheckableHelper<LOLATypeSystem> for VarName {
    fn type_check_raw(
        &self,
        ctx: &mut crate::core::TypeContext<LOLATypeSystem>,
        errs: &mut crate::core::SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let type_opt = ctx.get(self);
        match type_opt {
            Some(t) => match t {
                StreamType::Int => Ok(SExprTE::Int(SExprInt::Var(self.clone()))),
                StreamType::Str => Ok(SExprTE::Str(SExprStr::Var(self.clone()))),
                StreamType::Bool => Ok(SExprTE::Bool(SExprT::Var(self.clone()))),
                StreamType::Unit => Ok(SExprTE::Unit(SExprT::Var(self.clone()))),
            },
            None => {
                errs.push(SemanticError::UndeclaredVariable(
                    format!("Usage of undeclared variable: {:?}", self).into(),
                ));
                Err(())
            }
        }
    }
}

// Type check a boolean expression
impl TypeCheckableHelper<BoolTypeSystem> for BExpr<VarName> {
    fn type_check_raw(
        &self,
        ctx: &mut crate::core::TypeContext<LOLATypeSystem>,
        errs: &mut crate::core::SemanticErrors,
    ) -> Result<SExprBool, ()> {
        match self {
            BExpr::Val(b) => Ok(SExprBool::Val(*b)),
            BExpr::Eq(se1, se2) => {
                let se1_check = se1.type_check_raw(ctx, errs)?;
                let se2_check = se2.type_check_raw(ctx, errs)?;
                match (se1_check, se2_check) {
                    (SExprTE::Int(se1), SExprTE::Int(se2)) => Ok(SExprBool::EqInt(se1, se2)),
                    (SExprTE::Str(se1), SExprTE::Str(se2)) => Ok(SExprBool::EqStr(se1, se2)),
                    (SExprTE::Bool(se1), SExprTE::Bool(se2)) => Ok(SExprBool::EqBool(se1, se2)),
                    (SExprTE::Unit(se1), SExprTE::Unit(se2)) => Ok(SExprBool::EqUnit(se1, se2)),
                    (se1, se2) => {
                        errs.push(SemanticError::TypeError(
                            format!(
                                "Cannot compare expressions of different types: {:?} and {:?}",
                                se1, se2
                            )
                            .into(),
                        ));
                        Err(())
                    }
                }
            }
            BExpr::Le(se1, se2) => {
                let se1_check = se1.type_check_raw(ctx, errs)?;
                let se2_check = se2.type_check_raw(ctx, errs)?;
                match (se1_check, se2_check) {
                    (SExprTE::Int(se1), SExprTE::Int(se2)) => Ok(SExprBool::LeInt(se1, se2)),
                    (se1, se2) => {
                        errs.push(SemanticError::TypeError(
                            format!(
                                "Cannot compare expressions of different types: {:?} and {:?}",
                                se1, se2
                            )
                            .into(),
                        ));
                        Err(())
                    }
                }
            }
            BExpr::Not(b) => b.type_check_raw(ctx, errs),
            BExpr::And(b1, b2) => {
                let b1_check = b1.type_check_raw(ctx, errs)?;
                let b2_check = b2.type_check_raw(ctx, errs)?;
                Ok(SExprBool::And(Box::new(b1_check), Box::new(b2_check)))
            }
            BExpr::Or(b1, b2) => {
                let b1_check = b1.type_check_raw(ctx, errs)?;
                let b2_check = b2.type_check_raw(ctx, errs)?;
                Ok(SExprBool::Or(Box::new(b1_check), Box::new(b2_check)))
            }
        }
    }
}

// Type check an expression
impl TypeCheckableHelper<LOLATypeSystem> for SExpr<VarName> {
    fn type_check_raw(
        &self,
        ctx: &mut TypeContext<LOLATypeSystem>,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        match self {
            SExpr::Val(sdata) => sdata.type_check_raw(ctx, errs),
            SExpr::BinOp(se1, se2, op) => {
                (op.clone(), se1.deref(), se2.deref()).type_check_raw(ctx, errs)
            }
            SExpr::If(b, se1, se2) => {
                (b.deref(), se1.deref(), se2.deref()).type_check_raw(ctx, errs)
            }
            SExpr::Index(inner, idx, default) => {
                (inner.deref(), *idx, default).type_check_raw(ctx, errs)
            }
            SExpr::Var(id) => id.type_check_raw(ctx, errs),
            SExpr::Eval(_) => todo!("Implement support for Eval (to be renamed)"),
            SExpr::Defer(_) => todo!("Implement support for Defer"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{iter::zip, mem::discriminant};

    use crate::core::{SemanticResult, TypeCheckable, TypeContext};

    use super::*;

    type SExprV = SExpr<VarName>;
    type SemantResultStr = SemanticResult<SExprTE>;
    type BExprStr = BExpr<VarName>;

    trait AsExpr<Expr, BoolExpr> {
        fn if_expr(b: BoolExpr, texpr: Expr, fexpr: Expr) -> Self;
    }

    trait AsIntExpr<Expr> {
        fn binop_expr(lhs: Expr, rhs: Expr, op: SBinOp) -> Self;
    }

    impl AsIntExpr<Box<SExprInt>> for SExprInt {
        fn binop_expr(lhs: Box<SExprInt>, rhs: Box<SExprInt>, op: SBinOp) -> Self {
            SExprInt::BinOp(lhs, rhs, op)
        }
    }

    impl AsIntExpr<Box<SExprV>> for SExprV {
        fn binop_expr(lhs: Box<SExprV>, rhs: Box<SExprV>, op: SBinOp) -> Self {
            SExprV::BinOp(lhs, rhs, op)
        }
    }

    impl AsExpr<Box<SExprV>, Box<BExprStr>> for SExprV {
        fn if_expr(b: Box<BExprStr>, texpr: Box<SExprV>, fexpr: Box<SExprV>) -> Self {
            SExprV::If(b, texpr, fexpr)
        }
    }

    fn check_correct_error_type(result: &SemantResultStr, expected: &SemantResultStr) {
        // Checking that error type is correct but not the specific message
        if let (Err(res_errs), Err(exp_errs)) = (&result, &expected) {
            assert_eq!(res_errs.len(), exp_errs.len());
            let mut errs = zip(res_errs, exp_errs);
            assert!(
                errs.all(|(res, exp)| discriminant(res) == discriminant(exp)),
                "Error variants do not match: got {:?}, expected {:?}",
                res_errs,
                exp_errs
            );
        } else {
            // We didn't receive error - make assertion fail with nice output
            let msg = format!(
                "Expected error: {:?}. Received result: {:?}",
                expected, result
            );
            assert!(false, "{}", msg);
        }
    }

    fn check_correct_error_types(results: &Vec<SemantResultStr>, expected: &Vec<SemantResultStr>) {
        assert_eq!(
            results.len(),
            expected.len(),
            "Result and expected vectors must have the same length"
        );

        // Iterate over both vectors and call check_correct_error_type on each pair
        for (result, exp) in results.iter().zip(expected.iter()) {
            check_correct_error_type(result, exp);
        }
    }

    // // Helper function that returns all the sbinop variants at the time of writing these tests
    // // (Not guaranteed to be maintained)
    fn all_sbinop_variants() -> Vec<SBinOp> {
        vec![SBinOp::Plus, SBinOp::Minus, SBinOp::Mult]
    }

    // Function to generate combinations to use in tests, e.g., for binops
    fn generate_combinations<T, Expr, F>(
        variants_a: &[Expr],
        variants_b: &[Expr],
        generate_expr: F,
    ) -> Vec<T>
    where
        // T: AsExpr<Box<Expr>>,
        Expr: Clone,
        F: Fn(Box<Expr>, Box<Expr>) -> T,
    {
        let mut vals = Vec::new();

        for a in variants_a.iter() {
            for b in variants_b.iter() {
                vals.push(generate_expr(Box::new(a.clone()), Box::new(b.clone())));
            }
        }

        vals
    }

    // Example usage for binary operations
    fn generate_binop_combinations<T, Expr>(
        variants_a: &[Expr],
        variants_b: &[Expr],
        sbinops: Vec<SBinOp>,
    ) -> Vec<T>
    where
        T: AsIntExpr<Box<Expr>>,
        Expr: Clone,
    {
        let mut vals = Vec::new();

        for op in sbinops {
            vals.extend(generate_combinations(variants_a, variants_b, |lhs, rhs| {
                T::binop_expr(lhs, rhs, op)
            }));
        }

        vals
    }

    // // Example usage for if-expressions
    fn generate_if_combinations<T, Expr, BoolExpr: Clone>(
        variants_a: &[Expr],
        variants_b: &[Expr],
        b_expr: Box<BoolExpr>,
    ) -> Vec<T>
    where
        T: AsExpr<Box<Expr>, Box<BoolExpr>>,
        Expr: Clone,
    {
        generate_combinations(variants_a, variants_b, |lhs, rhs| {
            T::if_expr(b_expr.clone(), lhs, rhs)
        })
    }

    #[test]
    fn test_vals_ok() {
        // Checks that vals returns the expected typed AST after semantic analysis
        let vals = vec![
            SExprV::Val(ConcreteStreamData::Int(1)),
            SExprV::Val(ConcreteStreamData::Str("".into())),
            SExprV::Val(ConcreteStreamData::Bool(true)),
            SExprV::Val(ConcreteStreamData::Unit),
        ];
        let results = vals
            .iter()
            .map(TypeCheckable::<LOLATypeSystem>::type_check_with_default);
        let expected: Vec<SemantResultStr> = vec![
            Ok(SExprTE::Int(SExprInt::Val(1))),
            Ok(SExprTE::Str(SExprStr::Val("".into()))),
            Ok(SExprTE::Bool(SExprT::Val(true))),
            Ok(SExprTE::Unit(SExprT::Val(()))),
        ];

        assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_unknown_err() {
        // Checks that if a Val is unknown during semantic analysis it produces a UnknownError
        let val = SExprV::Val(ConcreteStreamData::Unknown);
        let result = val.type_check_with_default();
        let expected: SemantResultStr = Err(vec![SemanticError::UnknownError("".into())]);
        check_correct_error_type(&result, &expected);
    }

    #[test]
    fn test_plus_err_ident_types() {
        // Checks that if we add two identical types together that are not addable,
        let vals = vec![
            SExprV::BinOp(
                Box::new(SExprV::Val(ConcreteStreamData::Bool(false))),
                Box::new(SExprV::Val(ConcreteStreamData::Bool(false))),
                SBinOp::Plus,
            ),
            SExprV::BinOp(
                Box::new(SExprV::Val(ConcreteStreamData::Unit)),
                Box::new(SExprV::Val(ConcreteStreamData::Unit)),
                SBinOp::Plus,
            ),
        ];
        let results = vals
            .iter()
            .map(TypeCheckable::type_check_with_default)
            .collect();
        let expected: Vec<SemantResultStr> = vec![
            Err(vec![SemanticError::TypeError("".into())]),
            Err(vec![SemanticError::TypeError("".into())]),
        ];
        check_correct_error_types(&results, &expected);
    }

    // #[ignore = "Not implemented yet"]
    #[test]
    fn test_binop_err_diff_types() {
        // panic!("Not implemented yet");
        // Checks that calling a BinOp on two different types results in a TypeError

        // Create a vector of all ConcreteStreamData variants (except Unknown)
        let variants = vec![
            SExprV::Val(ConcreteStreamData::Int(0)),
            SExprV::Val(ConcreteStreamData::Str("".into())),
            SExprV::Val(ConcreteStreamData::Bool(true)),
            SExprV::Val(ConcreteStreamData::Unit),
        ];

        // Create a vector of all SBinOp variants
        let sbinops = all_sbinop_variants();

        let vals_tmp = generate_binop_combinations(&variants, &variants, sbinops);
        let vals = vals_tmp.into_iter().filter(|bin_op| {
            match bin_op {
                SExprV::BinOp(left, right, _) => {
                    // Only keep values where left != right
                    left != right
                }
                _ => true, // Keep non-BinOps (unused in this case)
            }
        });

        let results = vals
            .map(|x| TypeCheckable::type_check_with_default(&x))
            .collect::<Vec<_>>();

        // Since all combinations of different types should yield an error,
        // we'll expect each result to be an Err with a type error.
        let expected: Vec<SemantResultStr> = results
            .iter()
            .map(|_| Err(vec![SemanticError::TypeError("".into())]))
            .collect();

        check_correct_error_types(&results, &expected);
    }

    #[test]
    fn test_plus_err_unknown() {
        // Checks that if either value is unknown then Plus does not generate further errors
        let vals = vec![
            SExprV::BinOp(
                Box::new(SExprV::Val(ConcreteStreamData::Int(0))),
                Box::new(SExprV::Val(ConcreteStreamData::Unknown)),
                SBinOp::Plus,
            ),
            SExprV::BinOp(
                Box::new(SExprV::Val(ConcreteStreamData::Unknown)),
                Box::new(SExprV::Val(ConcreteStreamData::Int(0))),
                SBinOp::Plus,
            ),
            SExprV::BinOp(
                Box::new(SExprV::Val(ConcreteStreamData::Unknown)),
                Box::new(SExprV::Val(ConcreteStreamData::Unknown)),
                SBinOp::Plus,
            ),
        ];
        let results = vals.iter().map(TypeCheckable::type_check_with_default);
        let expected_err_lens = vec![1, 1, 2];

        // For each result, check that we got errors and that we got the correct amount:
        for (res, exp_err_len) in zip(results, expected_err_lens) {
            match res {
                Err(errs) => {
                    assert_eq!(
                        errs.len(),
                        exp_err_len,
                        "Expected {} errors but got {}: {:?}",
                        exp_err_len,
                        errs.len(),
                        errs
                    );
                    // TODO: Check that it is actually UnknownErrors
                }
                Ok(_) => {
                    assert!(
                        false,
                        "Expected an error but got a successful result: {:?}",
                        res
                    );
                }
            }
        }
    }

    #[test]
    fn test_int_binop_ok() {
        // Checks that if we BinOp two Ints together it results in typed AST after semantic analysis
        let int_val = vec![SExprV::Val(ConcreteStreamData::Int(0))];
        let sbinops = all_sbinop_variants();
        let vals: Vec<SExpr<VarName>> =
            generate_binop_combinations(&int_val, &int_val, sbinops.clone());
        let results = vals
            .iter()
            .map(TypeCheckable::<LOLATypeSystem>::type_check_with_default);

        let int_t_val = vec![SExprInt::Val(0)];

        // Generate the different combinations and turn them into "Ok" results
        let expected_tmp: Vec<SExprInt> =
            generate_binop_combinations(&int_t_val, &int_t_val, sbinops);
        let expected = expected_tmp.into_iter().map(|v| Ok(SExprTE::Int(v)));
        assert!(results.eq(expected.into_iter()));
    }

    #[ignore = "String concatenation not implemented yet"]
    #[test]
    fn test_str_plus_ok() {
        // Checks that if we add two Strings together it results in typed AST after semantic analysis
        // let str_val = vec![SExprV::Val(ConcreteStreamData::Str("".into()))];
        // let sbinops = vec![SBinOp::Plus];
        // let vals: Vec<SExpr<VarName>> =
        //     generate_binop_combinations(&str_val, &str_val, sbinops.clone());
        // let results = vals.iter().map(TypeCheckable::type_check_with_default);

        // let str_t_val = vec![SExprTStr::<String>::Val("".into())];

        // // Generate the different combinations and turn them into "Ok" results
        // let expected_tmp: Vec<SExprTStr<String>> =
        //     generate_binop_combinations(&str_t_val, &str_t_val, sbinops);
        // let expected = expected_tmp.into_iter().map(|v| Ok(SExprTE::Str(v)));
        // assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_if_ok() {
        // Checks that typechecking if-statements with identical types for if- and else- part results in correct typed AST

        // Create a vector of all ConcreteStreamData variants (except Unknown)
        let val_variants = vec![
            SExprV::Val(ConcreteStreamData::Int(0)),
            SExprV::Val(ConcreteStreamData::Str("".into())),
            SExprV::Val(ConcreteStreamData::Bool(true)),
            SExprV::Val(ConcreteStreamData::Unit),
        ];

        // Create a vector of all SBinOp variants
        let bexpr = Box::new(BExprStr::Val(true));
        let bexpr_checked = Box::new(SExprBool::Val(true));

        let vals_tmp = generate_if_combinations(&val_variants, &val_variants, bexpr.clone());

        // Only consider cases where true and false cases are equal
        let vals = vals_tmp.into_iter().filter(|bin_op| {
            match bin_op {
                SExprV::If(_, t, f) => t == f,
                _ => true, // Keep non-ifs (unused in this case)
            }
        });
        let results = vals.map(|x| x.type_check_with_default());

        let expected: Vec<SemantResultStr> = vec![
            Ok(SExprTE::Int(SExprInt::If(
                bexpr_checked.clone(),
                Box::new(SExprInt::Val(0)),
                Box::new(SExprInt::Val(0)),
            ))),
            Ok(SExprTE::Str(SExprStr::If(
                bexpr_checked.clone(),
                Box::new(SExprStr::Val("".into())),
                Box::new(SExprStr::Val("".into())),
            ))),
            Ok(SExprTE::Bool(SExprT::If(
                bexpr_checked.clone(),
                Box::new(SExprT::Val(true)),
                Box::new(SExprT::Val(true)),
            ))),
            Ok(SExprTE::Unit(SExprT::If(
                bexpr_checked.clone(),
                Box::new(SExprT::Val(())),
                Box::new(SExprT::Val(())),
            ))),
        ];

        assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_if_err() {
        // Checks that creating an if-expression with two different types results in a TypeError

        // Create a vector of all ConcreteStreamData variants (except Unknown)
        let variants = vec![
            SExprV::Val(ConcreteStreamData::Int(0)),
            SExprV::Val(ConcreteStreamData::Str("".into())),
            SExprV::Val(ConcreteStreamData::Bool(true)),
            SExprV::Val(ConcreteStreamData::Unit),
        ];

        let bexpr = Box::new(BExprStr::Val(true));

        let vals_tmp = generate_if_combinations(&variants, &variants, bexpr.clone());
        let vals = vals_tmp.into_iter().filter(|bin_op| {
            match bin_op {
                SExprV::If(_, t, f) => t != f,
                _ => true, // Keep non-BinOps (unused in this case)
            }
        });

        let results = vals
            .map(|x| x.type_check_with_default())
            .collect::<Vec<_>>();

        // Since all combinations of different types should yield an error,
        // we'll expect each result to be an Err with a type error.
        let expected: Vec<SemantResultStr> = results
            .iter()
            .map(|_| Err(vec![SemanticError::TypeError("".into())]))
            .collect();

        check_correct_error_types(&results, &expected);
    }

    #[test]
    fn test_var_ok() {
        // Checks that Vars are correctly typechecked if they exist in the context

        let variant_names = vec!["int", "str", "bool", "unit"];
        let variant_types = vec![
            StreamType::Int,
            StreamType::Str,
            StreamType::Bool,
            StreamType::Unit,
        ];
        let vals = variant_names
            .clone()
            .into_iter()
            .map(|n| SExprV::Var(VarName(n.into())));

        // Fake context/environment that simulates type-checking context
        let mut ctx = TypeContext::<LOLATypeSystem>::new();
        for (n, t) in variant_names.into_iter().zip(variant_types.into_iter()) {
            ctx.insert(VarName(n.into()), t);
        }

        let results = vals.into_iter().map(|sexpr| sexpr.type_check(&mut ctx));

        let expected = vec![
            Ok(SExprTE::Int(SExprInt::Var(VarName("int".into())))),
            Ok(SExprTE::Str(SExprStr::Var(VarName("str".into())))),
            Ok(SExprTE::Bool(SExprT::Var(VarName("bool".into())))),
            Ok(SExprTE::Unit(SExprT::Var(VarName("unit".into())))),
        ];

        assert!(results.eq(expected));
    }

    #[test]
    fn test_var_err() {
        // Checks that Vars produce UndeclaredVariable errors if they do not exist in the context

        let val = SExprV::Var(VarName("undeclared_name".into()));
        let result = val.type_check_with_default();
        let expected: SemantResultStr = Err(vec![SemanticError::UndeclaredVariable("".into())]);
        check_correct_error_type(&result, &expected);
    }
    // TODO: Test that any SExpr leaf is a Val. If not it should return a Type-Error

    #[test]
    fn test_dodgy_if() {
        let dodgy_bexpr = BExpr::Eq(
            Box::new(SExprV::Val(ConcreteStreamData::Int(0))),
            Box::new(SExprV::BinOp(
                Box::new(SExprV::Val(ConcreteStreamData::Int(3))),
                Box::new(SExprV::Val(ConcreteStreamData::Str("Banana".into()))),
                SBinOp::Plus,
            )),
        );
        let sexpr = SExprV::If(
            Box::new(dodgy_bexpr),
            Box::new(SExprV::Val(ConcreteStreamData::Int(1))),
            Box::new(SExprV::Val(ConcreteStreamData::Int(2))),
        );
        if let Ok(_) = sexpr.type_check_with_default() {
            assert!(false, "Expected type error but got a successful result");
        }
    }
}
