use crate::{
    ast::{BExpr, SBinOp, SExpr},
    ConcreteStreamData,
};

use std::fmt::Debug;

// Trait defining the allowed types for expression values
pub trait SExprValue: Clone + Debug + PartialEq + Eq {}
impl SExprValue for i64 {}
impl SExprValue for String {}
impl SExprValue for bool {}
impl SExprValue for () {}

// Stream expressions - now with types
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SExprT<ValT: SExprValue, VarT: Debug> {
    If(Box<BExpr<VarT>>, Box<Self>, Box<Self>),

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

    BinOp(Box<Self>, Box<Self>, SBinOp),

    Var(VarT),

    // Eval
    Eval(Box<Self>),
}

// Stream expression typed enum
#[derive(Debug, PartialEq, Eq)]
pub enum SExprTE<VarT: Debug> {
    IntT(SExprT<i64, VarT>),
    StrT(SExprT<String, VarT>),
    BoolT(SExprT<bool, VarT>),
    UnitT,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SemantError {
    TypeError(String),
    UnknownError(String),
    UndeclaredVariable(String),
}

pub type SemantErrors = Vec<SemantError>;

pub type TypeContext<VarT> = Vec<VarT>;

pub type SemantResult<VarT> = Result<SExprTE<VarT>, SemantErrors>;

pub fn type_check_expr<VarT: Debug>(
    sexpr: SExpr<VarT>,
    ctx: &mut TypeContext<VarT>,
    errs: &mut SemantErrors,
) -> Result<SExprTE<VarT>, ()>
where
    VarT: Clone,
{
    match sexpr {
        SExpr::Val(sdata) => match sdata {
            ConcreteStreamData::Int(v) => Ok(SExprTE::IntT(SExprT::Val(v))),
            ConcreteStreamData::Str(v) => Ok(SExprTE::StrT(SExprT::Val(v))),
            ConcreteStreamData::Bool(v) => Ok(SExprTE::BoolT(SExprT::Val(v))),
            ConcreteStreamData::Unit => Ok(SExprTE::UnitT),
            ConcreteStreamData::Unknown => {
                errs.push(SemantError::UnknownError(
                    format!(
                        "Stream expression {:?} not assigned a type before semantic analysis",
                        sdata
                    )
                    .into(),
                ));
                Err(())
            }
        },
        SExpr::BinOp(se1, se2, op) => {
            let se1_check = type_check_expr(*se1, ctx, errs);
            let se2_check = type_check_expr(*se2, ctx, errs);
            match (se1_check, se2_check) {
                (Ok(SExprTE::IntT(se1)), Ok(SExprTE::IntT(se2))) => Ok(SExprTE::IntT(
                    SExprT::BinOp(Box::new(se1.clone()), Box::new(se2.clone()), op),
                )),
                (Ok(SExprTE::StrT(se1)), Ok(SExprTE::StrT(se2))) if op == SBinOp::Plus => {
                    Ok(SExprTE::StrT(SExprT::BinOp(
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                        op,
                    )))
                }
                // Any other case where we are otherwise OK
                (Ok(ste1), Ok(ste2)) => {
                    errs.push(SemantError::TypeError(
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
        _ => {
            errs.push(SemantError::TypeError("Not implemented".into()));
            Err(())
        }
    }
}

pub fn type_check<VarT>(sexpr: SExpr<VarT>) -> SemantResult<VarT>
where
    VarT: Debug,
    VarT: Clone,
{
    let mut context = Vec::new();
    let mut errors = Vec::new();
    let res = type_check_expr(sexpr, &mut context, &mut errors);
    match res {
        Ok(se) => Ok(se),
        Err(()) => Err(errors),
    }
}

#[cfg(test)]
mod tests {
    use std::{iter::zip, mem::discriminant};

    use super::*;

    type SemantResultStr = SemantResult<String>;
    type SExprStr = SExpr<String>;
    type SExprTStr<Val> = SExprT<Val, String>;

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
            assert!(matches!(result, _expected));
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

    #[test]
    fn test_vals_ok() {
        // Checks that vals returns the expected typed AST after semantic analysis
        let vals = vec![
            SExprStr::Val(ConcreteStreamData::Int(1)),
            SExprStr::Val(ConcreteStreamData::Str("".into())),
            SExprStr::Val(ConcreteStreamData::Bool(true)),
            SExprStr::Val(ConcreteStreamData::Unit),
        ];
        let results = vals.into_iter().map(type_check);
        let expected: Vec<SemantResultStr> = vec![
            Ok(SExprTE::IntT(SExprT::Val(1))),
            Ok(SExprTE::StrT(SExprT::Val("".into()))),
            Ok(SExprTE::BoolT(SExprT::Val(true))),
            Ok(SExprTE::UnitT),
        ];

        assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_unknown_err() {
        // Checks that if a Val is unknown during semantic analysis it produces a UnknownError
        let val = SExprStr::Val(ConcreteStreamData::Unknown);
        let result = type_check(val);
        let expected: SemantResultStr = Err(vec![SemantError::UnknownError("".into())]);
        check_correct_error_type(&result, &expected);
    }

    #[test]
    fn test_plus_ok() {
        // Checks that if we plus two Ints or Strings together it results in typed AST after semantic analysis
        let vals = vec![
            SExprStr::BinOp(
                Box::new(SExprStr::Val(ConcreteStreamData::Int(0))),
                Box::new(SExprStr::Val(ConcreteStreamData::Int(0))),
                SBinOp::Plus,
            ),
            SExprStr::BinOp(
                Box::new(SExprStr::Val(ConcreteStreamData::Str("".into()))),
                Box::new(SExprStr::Val(ConcreteStreamData::Str("".into()))),
                SBinOp::Plus,
            ),
        ];
        let results = vals.into_iter().map(type_check);
        let int_val = Box::new(SExprTStr::Val(0));
        let str_val = Box::new(SExprTStr::Val("".into()));
        let expected: Vec<SemantResultStr> = vec![
            Ok(SExprTE::IntT(SExprTStr::BinOp(
                int_val.clone(),
                int_val,
                SBinOp::Plus,
            ))),
            Ok(SExprTE::StrT(SExprTStr::BinOp(
                str_val.clone(),
                str_val,
                SBinOp::Plus,
            ))),
        ];

        assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_plus_err_ident_types() {
        // Checks that if we add two identical types together that are not addable,
        let vals = vec![
            SExprStr::BinOp(
                Box::new(SExprStr::Val(ConcreteStreamData::Bool(false))),
                Box::new(SExprStr::Val(ConcreteStreamData::Bool(false))),
                SBinOp::Plus,
            ),
            SExprStr::BinOp(
                Box::new(SExprStr::Val(ConcreteStreamData::Unit)),
                Box::new(SExprStr::Val(ConcreteStreamData::Unit)),
                SBinOp::Plus,
            ),
        ];
        let results = vals.into_iter().map(type_check).collect();
        let expected: Vec<SemantResultStr> = vec![
            Err(vec![SemantError::TypeError("".into())]),
            Err(vec![SemantError::TypeError("".into())]),
        ];
        check_correct_error_types(&results, &expected);
    }

    #[test]
    fn test_plus_err_unknown() {
        // Checks that if either value is unknown then Plus does not generate further errors
        // Checks that if we add two identical types together that are not addable,
        let vals = vec![
            SExprStr::BinOp(
                Box::new(SExprStr::Val(ConcreteStreamData::Int(0))),
                Box::new(SExprStr::Val(ConcreteStreamData::Unknown)),
                SBinOp::Plus,
            ),
            SExprStr::BinOp(
                Box::new(SExprStr::Val(ConcreteStreamData::Unknown)),
                Box::new(SExprStr::Val(ConcreteStreamData::Int(0))),
                SBinOp::Plus,
            ),
            SExprStr::BinOp(
                Box::new(SExprStr::Val(ConcreteStreamData::Unknown)),
                Box::new(SExprStr::Val(ConcreteStreamData::Unknown)),
                SBinOp::Plus,
            ),
        ];
        let results = vals.into_iter().map(type_check);
        let expected_err_lens = vec![1, 1, 2];
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
}
