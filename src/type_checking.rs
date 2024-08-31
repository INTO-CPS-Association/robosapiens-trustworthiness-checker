use crate::{
    ast::{BExpr, SExpr},
    StreamData,
};

use std::{collections::BTreeMap, fmt::Debug, fmt::Display};

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SExprInt<VarT: Debug> {
    // if-then-else
    If(Box<BExpr<VarT>>, Box<SExprInt<VarT>>, Box<SExprInt<VarT>>),

    // Stream indexing
    Index(
        // Inner SExpr e
        Box<SExprInt<VarT>>,
        // Index i
        isize,
        // Default c
        i64,
    ),

    // Arithmetic Stream expression
    Val(i64),
    Plus(Box<SExprInt<VarT>>, Box<SExprInt<VarT>>),
    Minus(Box<SExprInt<VarT>>, Box<SExprInt<VarT>>),
    Mult(Box<SExprInt<VarT>>, Box<SExprInt<VarT>>),
    Var(VarT),

    // Eval
    Eval(Box<SExprInt<VarT>>),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SExprStr<VarT: Debug> {
    If(Box<BExpr<VarT>>, Box<SExprStr<VarT>>, Box<SExprStr<VarT>>),

    // Stream indexing
    Index(
        // Inner SExpr e
        Box<SExprStr<VarT>>,
        // Index i
        isize,
        // Default c
        String,
    ),

    // Arithmetic Stream expression
    Val(String),
    Plus(Box<SExprStr<VarT>>, Box<SExprStr<VarT>>),
    Minus(Box<SExprStr<VarT>>, Box<SExprStr<VarT>>),
    Mult(Box<SExprStr<VarT>>, Box<SExprStr<VarT>>),
    Var(VarT),

    // Eval
    Eval(Box<SExprStr<VarT>>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum SExprT<VarT: Debug> {
    IntT(SExprInt<VarT>),
    StrT(SExprStr<VarT>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum ErrorType {
    TypeError(String),
    UndefinedVariable(String),
}

pub fn type_check_expr(sexpr: SExpr<String>) -> Result<SExprT<String>, ErrorType> {
    match sexpr {
        SExpr::Val(sdata) => match sdata {
            StreamData::Int(v) => Ok(SExprT::IntT(SExprInt::Val(v))),
            StreamData::Str(v) => Ok(SExprT::StrT(SExprStr::Val(v))),
            _ => Err(ErrorType::TypeError("Not implemented".into())),
        },
        SExpr::Plus(se1, se2) => {
            let se1_check = type_check_expr(*se1);
            let se2_check = type_check_expr(*se2);
            match (se1_check, se2_check) {
                (Ok(SExprT::IntT(se1)), Ok(SExprT::IntT(se2))) => Ok(SExprT::IntT(SExprInt::Plus(
                    Box::new(se1.clone()),
                    Box::new(se2.clone()),
                ))),
                _ => Err(ErrorType::TypeError("Not implemented".into())),
            }
        }

        _ => Err(ErrorType::TypeError("Not implemented".into())),
    }
}
