use crate::core::{IndexedVarName, Specification, VarName};
use crate::core::{StreamType, Value};
use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
};

// Integer Binary Operations
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IntBinOp {
    Add,
    Sub,
    Mul,
    Div,
}

// Bool Binary Operations
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoolBinOp {
    Or,
    And,
}

// Str Binary Operations
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StrBinOp {
    Concat,
}

// Comparison Binary Operations
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompBinOp {
    Eq,
    Le,
}

// Stream BinOp
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SBinOp {
    IOp(IntBinOp),
    BOp(BoolBinOp),
    SOp(StrBinOp),
    COp(CompBinOp),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SExpr<VarT: Debug> {
    // if-then-else
    If(Box<Self>, Box<Self>, Box<Self>),

    // Stream indexing
    Index(
        // Inner SExpr e
        Box<Self>,
        // Index i
        isize,
        // Default c
        Value,
    ),

    // Arithmetic Stream expression
    Val(Value),

    BinOp(Box<Self>, Box<Self>, SBinOp),

    Var(VarT),

    // Eval
    Eval(Box<Self>),
    Defer(Box<Self>),
    Update(Box<Self>, Box<Self>),

    // Unary expressions (refactor if more are added...)
    Not(Box<Self>),

    // List and list expressions
    List(Vec<Self>),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct LOLASpecification {
    pub input_vars: Vec<VarName>,
    pub output_vars: Vec<VarName>,
    pub exprs: BTreeMap<VarName, SExpr<VarName>>,
    pub type_annotations: BTreeMap<VarName, StreamType>,
}

impl Specification<SExpr<VarName>> for LOLASpecification {
    fn input_vars(&self) -> Vec<VarName> {
        self.input_vars.clone()
    }

    fn output_vars(&self) -> Vec<VarName> {
        self.output_vars.clone()
    }

    fn var_expr(&self, var: &VarName) -> Option<SExpr<VarName>> {
        Some(self.exprs.get(var)?.clone())
    }
}

impl VarName {
    pub fn to_indexed(&self, i: usize) -> IndexedVarName {
        IndexedVarName(self.0.clone(), i)
    }
}

impl Display for IndexedVarName {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let IndexedVarName(name, index) = self;
        write!(f, "{}[{}]", name, index)
    }
}

impl<VarT: Display + Debug> Display for SExpr<VarT> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use SBinOp::*;
        use SExpr::*;
        match self {
            If(b, e1, e2) => write!(f, "if {} then {} else {}", b, e1, e2),
            Index(s, i, c) => write!(f, "{}[{},{}]", s, i, c),
            Val(n) => write!(f, "{}", n),
            BinOp(e1, e2, IOp(IntBinOp::Add)) => write!(f, "({} + {})", e1, e2),
            BinOp(e1, e2, IOp(IntBinOp::Sub)) => write!(f, "({} - {})", e1, e2),
            BinOp(e1, e2, IOp(IntBinOp::Mul)) => write!(f, "({} * {})", e1, e2),
            BinOp(e1, e2, IOp(IntBinOp::Div)) => write!(f, "({} / {})", e1, e2),
            BinOp(e1, e2, BOp(BoolBinOp::Or)) => write!(f, "({} || {})", e1, e2),
            BinOp(e1, e2, BOp(BoolBinOp::And)) => write!(f, "({} && {})", e1, e2),
            BinOp(e1, e2, SOp(StrBinOp::Concat)) => write!(f, "({} ++ {})", e1, e2),
            BinOp(e1, e2, COp(CompBinOp::Eq)) => write!(f, "({} == {})", e1, e2),
            BinOp(e1, e2, COp(CompBinOp::Le)) => write!(f, "({} <= {})", e1, e2),
            Not(b) => write!(f, "!{}", b),
            Var(v) => write!(f, "{}", v),
            Eval(e) => write!(f, "eval({})", e),
            Defer(e) => write!(f, "defer({})", e),
            Update(e1, e2) => write!(f, "update({}, {})", e1, e2),
            List(es) => {
                let es_str: Vec<String> = es.iter().map(|e| format!("{}", e)).collect();
                write!(f, "[{}]", es_str.join(", "))
            }
        }
    }
}

pub type InputFileData = BTreeMap<usize, BTreeMap<VarName, Value>>;
