use crate::{
    ast::{BExpr, SExpr},
    ConcreteStreamData,
};

use std::{error, fmt::Debug};

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
    use super::*;

    type SemantResultStr = SemantResult<String>;

    #[test]
    fn test_int_val() {
        let val: SExpr<String> = SExpr::Val(ConcreteStreamData::Int(1));
        let result = type_check(val);
        let expected: SemantResultStr = Ok(SExprTE::IntT(SExprT::Val(1)));

        assert_eq!(result, expected);
    }

    #[test]
    fn test_string_val() {
        let val: SExpr<String> = SExpr::Val(ConcreteStreamData::Str("Hello".into()));
        let result = type_check(val);
        let expected: SemantResultStr = Ok(SExprTE::StrT(SExprT::Val("Hello".into())));

        assert_eq!(result, expected);
    }
}
