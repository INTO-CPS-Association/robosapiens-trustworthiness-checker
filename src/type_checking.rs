use crate::{
    ast::{BExpr, SExpr},
    StreamData,
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
}

#[derive(Debug, PartialEq, Eq)]
pub enum ErrorType {
    TypeError(String),
    UndefinedVariable(String),
}

pub fn type_check_expr(sexpr: SExpr<String>) -> Result<SExprTE<String>, ErrorType> {
    match sexpr {
        SExpr::Val(sdata) => match sdata {
            StreamData::Int(v) => Ok(SExprTE::IntT(SExprT::Val(v))),
            StreamData::Str(v) => Ok(SExprTE::StrT(SExprT::Val(v))),
            _ => Err(ErrorType::TypeError("Not implemented".into())),
        },
        SExpr::Plus(se1, se2) => {
            let se1_check = type_check_expr(*se1);
            let se2_check = type_check_expr(*se2);
            match (se1_check, se2_check) {
                (Ok(SExprTE::IntT(se1)), Ok(SExprTE::IntT(se2))) => Ok(SExprTE::IntT(
                    SExprT::Plus(Box::new(se1.clone()), Box::new(se2.clone())),
                )),
                _ => Err(ErrorType::TypeError("Not implemented".into())),
            }
        }

        _ => Err(ErrorType::TypeError("Not implemented".into())),
    }
}
