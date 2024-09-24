use crate::{
    ast::{BExpr, SExpr},
    ConcreteStreamData,
};

use std::fmt::Debug;

// Stream expression typed
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SExprT<ValT, VarT: Debug> {
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
    Plus(Box<Self>, Box<Self>),
    Minus(Box<Self>, Box<Self>),
    Mult(Box<Self>, Box<Self>),
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
                errs.push(SemantError::TypeError(
                    format!(
                        "Stream expression {:?} not assigned a type before semantic analysis",
                        sdata
                    )
                    .into(),
                ));
                Err(())
            }
        },
        SExpr::Plus(se1, se2) => {
            let se1_check = type_check_expr(*se1, ctx, errs);
            let se2_check = type_check_expr(*se2, ctx, errs);
            match (se1_check, se2_check) {
                (Ok(SExprTE::IntT(se1)), Ok(SExprTE::IntT(se2))) => Ok(SExprTE::IntT(
                    SExprT::Plus(Box::new(se1.clone()), Box::new(se2.clone())),
                )),
                (Ok(SExprTE::StrT(se1)), Ok(SExprTE::StrT(se2))) => Ok(SExprTE::StrT(
                    SExprT::Plus(Box::new(se1.clone()), Box::new(se2.clone())),
                )),
                // Any other case where we are otherwise OK
                (Ok(ste1), Ok(ste2)) => {
                    errs.push(SemantError::TypeError(
                        format!(
                        "Cannot apply binary function Plus to expressions of type {:?} and {:?}",
                        ste1, ste2
                    )
                        .into(),
                    ));
                    Err(())
                }
                _ => {
                    errs.push(SemantError::TypeError("Not implemented".into()));
                    Err(())
                }
            }
        }
        _ => {
            errs.push(SemantError::TypeError("Not implemented".into()));
            Err(())
        }
    }
}

pub fn type_check<VarT: Debug>(sexpr: SExpr<VarT>) -> SemantResult<VarT>
where
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
    use std::iter::zip;

    use super::*;

    type SemantResultStr = SemantResult<String>;
    type SExprStr = SExpr<String>;

    #[test]
    fn test_vals_ok() {
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

        assert_eq!(results.len(), expected.len());

        for (res, exp) in zip(results, expected) {
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_unknown_err() {
        let val = SExprStr::Val(ConcreteStreamData::Unknown);
        let result = type_check(val);
        let _expected: SemantResultStr = Err(vec![SemantError::TypeError("".into())]);

        // Checking that error is returned of correct type but not the specific message
        if let (Err(res_errs), Err(exp_errs)) = (&result, &_expected) {
            assert_eq!(res_errs.len(), exp_errs.len());

            assert!(res_errs
                .iter()
                .all(|e| matches!(e, SemantError::TypeError(_))));
        } else {
            // We didn't receive error - make assertion fail with nice output
            assert!(matches!(result, _expected));
        }
    }
}
