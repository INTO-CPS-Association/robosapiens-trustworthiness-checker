use crate::{
    core::{
        ConcreteStreamData, ExpressionTyping, IndexedVarName, Specification, StreamExpr,
        StreamSystem, TypeAnnotated, TypeSystem, VarName,
    },
    lola_type_system::{LOLATypeSystem, StreamType},
    MonitoringSemantics, OutputStream, StreamContext,
};
use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
};

pub struct UntypedLOLA;
// pub trait TypedStreamData<T: Type<TS>, TS: TypeSystem>: StreamData<TS> {}

impl ExpressionTyping for UntypedLOLA {
    type TypeSystem = UntypedLOLA;
    type TypedExpr = SExpr<VarName>;

    fn type_of_expr(_: &Self::TypedExpr) -> () {
        ()
    }
}

pub struct UntypedStreams;
impl StreamSystem for UntypedStreams {
    type TypedStream = OutputStream<ConcreteStreamData>;
    type TypeSystem = UntypedLOLA;

    fn type_of_stream(_: &Self::TypedStream) -> () {
        ()
    }

    fn transform_stream(
        transformation: impl crate::core::StreamTransformationFn,
        stream: Self::TypedStream,
    ) -> Self::TypedStream {
        transformation.transform(stream)
    }

    fn to_typed_stream(_: (), stream: OutputStream<ConcreteStreamData>) -> Self::TypedStream {
        stream
    }
}

impl TypeSystem for UntypedLOLA {
    type Type = ();
    type TypedValue = ConcreteStreamData;
    // type TypedValue = ConcreteStreamData;

    fn type_of_value(_: &Self::TypedValue) -> Self::Type {
        ()
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum BExpr<VarT: Debug> {
    Val(bool),
    Eq(Box<SExpr<VarT>>, Box<SExpr<VarT>>),
    Le(Box<SExpr<VarT>>, Box<SExpr<VarT>>),
    Not(Box<BExpr<VarT>>),
    And(Box<BExpr<VarT>>, Box<BExpr<VarT>>),
    Or(Box<BExpr<VarT>>, Box<BExpr<VarT>>),
}

// Stream BinOp
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SBinOp {
    Plus,
    Minus,
    Mult,
}

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

impl StreamExpr<()> for SExpr<VarName> {
    fn var(_: (), var: &VarName) -> Self {
        SExpr::Var(var.clone())
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct LOLASpecification {
    pub input_vars: Vec<VarName>,
    pub output_vars: Vec<VarName>,
    pub exprs: BTreeMap<VarName, SExpr<VarName>>,
    pub type_annotations: BTreeMap<VarName, StreamType>,
}

impl Specification<UntypedLOLA> for LOLASpecification {
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

impl TypeAnnotated<LOLATypeSystem> for LOLASpecification {
    fn type_of_var(&self, var: &VarName) -> Option<StreamType> {
        self.type_annotations.get(var).cloned()
    }
}

impl TypeAnnotated<UntypedLOLA> for LOLASpecification {
    fn type_of_var(&self, _: &VarName) -> Option<()> {
        Some(())
    }
}

// A dummy monitoring semantics for monitors which do not support pluggable
// monitoring semantics
#[derive(Clone)]
pub struct FixedSemantics;

impl<E> MonitoringSemantics<E, OutputStream<ConcreteStreamData>> for FixedSemantics {
    type StreamSystem = UntypedStreams;

    fn to_async_stream(
        _: E,
        _: &dyn StreamContext<UntypedStreams>,
    ) -> OutputStream<ConcreteStreamData> {
        unimplemented!("Dummy monitoring semantics; should not be called")
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

// Trait for indexing a variable producing a new SExpr
pub trait IndexableVar: Debug {
    fn index(&self, i: isize, c: &ConcreteStreamData) -> SExpr<Self>
    where
        Self: Sized;
}

impl IndexableVar for VarName {
    // For unindexed variables, indexing just produces the same expression
    fn index(&self, i: isize, c: &ConcreteStreamData) -> SExpr<VarName> {
        SExpr::Index(Box::new(SExpr::Var(self.clone())), i, c.clone())
    }
}

impl IndexableVar for IndexedVarName {
    // For indexed variables, we can actually attempt to change the index on the underlying variable
    fn index(&self, i: isize, c: &ConcreteStreamData) -> SExpr<IndexedVarName> {
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

pub type InputFileData = BTreeMap<usize, BTreeMap<VarName, ConcreteStreamData>>;
