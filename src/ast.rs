use crate::core::{ConcreteStreamData, StreamType};
use crate::core::{IndexedVarName, Specification, VarName};
use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
};

// TODO: Make BExpr part of SExpr 
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum BExpr<VarT: Debug> {
    Val(bool),
    Eq(Box<SExpr<VarT>>, Box<SExpr<VarT>>),
    Le(Box<SExpr<VarT>>, Box<SExpr<VarT>>),
    Not(Box<BExpr<VarT>>),
    And(Box<BExpr<VarT>>, Box<BExpr<VarT>>),
    Or(Box<BExpr<VarT>>, Box<BExpr<VarT>>),
}

// TODO: Refactor SBinOp to use impl below
// Stream BinOp
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SBinOp {
    Plus,
    Minus,
    Mult,
}

// // Integer Binary Operations
// #[derive(Clone, Debug, PartialEq)]
// pub enum IntBinOp {
//     Add,
//     Sub,
//     Mul,
//     Div,
// }
// 
// // Bool Binary Operations
// #[derive(Clone, Debug, PartialEq)]
// pub enum BoolBinOp {
//     Or,
//     And,
// }
// 
// // Str Binary Operations
// #[derive(Clone, Debug, PartialEq)]
// pub enum StrBinOp {
//     Concat,
// }
// 
// // Stream BinOp
// #[derive(Clone, Debug, PartialEq)]
// pub enum SBinOp {
//     IOp(IntBinOp),
//     BOp(BoolBinOp),
//     SOp(StrBinOp),
// }

// TODO: Remove generic VarT
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SExpr<VarT: Debug> {
    // if-then-else
    If(Box<BExpr<VarT>>, Box<Self>, Box<Self>),

    // Stream indexing
    Index(
        // Inner SExpr e
        Box<Self>,
        // Index i
        isize,
        // Default c
        ConcreteStreamData,
    ),

    // Arithmetic Stream expression
    Val(ConcreteStreamData),

    BinOp(Box<Self>, Box<Self>, SBinOp),

    Var(VarT),

    // Eval
    Eval(Box<Self>),
    Defer(Box<Self>),
    Update(Box<Self>, Box<Self>),
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
        match self {
            SExpr::If(b, e1, e2) => write!(f, "if {} then {} else {}", b, e1, e2),
            SExpr::Index(s, i, c) => write!(f, "{}[{},{}]", s, i, c),
            SExpr::Val(n) => write!(f, "{}", n),
            SExpr::BinOp(e1, e2, SBinOp::Plus) => write!(f, "({} + {})", e1, e2),
            SExpr::BinOp(e1, e2, SBinOp::Minus) => write!(f, "({} - {})", e1, e2),
            SExpr::BinOp(e1, e2, SBinOp::Mult) => write!(f, "({} * {})", e1, e2),
            SExpr::Var(v) => write!(f, "{}", v),
            SExpr::Eval(e) => write!(f, "eval({})", e),
            SExpr::Defer(e) => write!(f, "defer({})", e),
            SExpr::Update(e1, e2) => write!(f, "update({}, {})", e1, e2),
        }
    }
}

impl<VarT: Display + Debug> Display for BExpr<VarT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BExpr::Val(b) => write!(f, "{}", if *b { "true" } else { "false" }),
            BExpr::Eq(e1, e2) => write!(f, "({} == {})", e1, e2),
            BExpr::Le(e1, e2) => write!(f, "({} <= {})", e1, e2),
            BExpr::Not(b) => write!(f, "!{}", b),
            BExpr::And(b1, b2) => write!(f, "({} && {})", b1, b2),
            BExpr::Or(b1, b2) => write!(f, "({} || {})", b1, b2),
        }
    }
}

<<<<<<< Updated upstream


=======
>>>>>>> Stashed changes
pub type InputFileData = BTreeMap<usize, BTreeMap<VarName, ConcreteStreamData>>;
