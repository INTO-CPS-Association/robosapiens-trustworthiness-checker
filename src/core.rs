use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
};

use futures::stream::BoxStream;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConcreteStreamData {
    Int(i64),
    Str(String),
    Bool(bool),
    Unknown,
    Unit,
}
impl StreamData for ConcreteStreamData {}

impl TryFrom<ConcreteStreamData> for i64 {
    type Error = ();

    fn try_from(value: ConcreteStreamData) -> Result<Self, Self::Error> {
        match value {
            ConcreteStreamData::Int(i) => Ok(i),
            _ => Err(()),
        }
    }
}
impl TryFrom<ConcreteStreamData> for String {
    type Error = ();

    fn try_from(value: ConcreteStreamData) -> Result<Self, Self::Error> {
        match value {
            ConcreteStreamData::Str(i) => Ok(i),
            _ => Err(()),
        }
    }
}
impl TryFrom<ConcreteStreamData> for bool {
    type Error = ();

    fn try_from(value: ConcreteStreamData) -> Result<Self, Self::Error> {
        match value {
            ConcreteStreamData::Bool(i) => Ok(i),
            _ => Err(()),
        }
    }
}
impl TryFrom<ConcreteStreamData> for () {
    type Error = ();

    fn try_from(value: ConcreteStreamData) -> Result<Self, Self::Error> {
        match value {
            ConcreteStreamData::Unit => Ok(()),
            _ => Err(()),
        }
    }
}
impl From<i64> for ConcreteStreamData {
    fn from(value: i64) -> Self {
        ConcreteStreamData::Int(value)
    }
}
impl From<String> for ConcreteStreamData {
    fn from(value: String) -> Self {
        ConcreteStreamData::Str(value)
    }
}
impl From<&str> for ConcreteStreamData {
    fn from(value: &str) -> Self {
        ConcreteStreamData::Str(value.to_string())
    }
}
impl From<bool> for ConcreteStreamData {
    fn from(value: bool) -> Self {
        ConcreteStreamData::Bool(value)
    }
}
impl From<()> for ConcreteStreamData {
    fn from(_value: ()) -> Self {
        ConcreteStreamData::Unit
    }
}

impl Display for ConcreteStreamData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConcreteStreamData::Int(i) => write!(f, "{}", i),
            ConcreteStreamData::Str(s) => write!(f, "{}", s),
            ConcreteStreamData::Bool(b) => write!(f, "{}", b),
            ConcreteStreamData::Unknown => write!(f, "unknown"),
            ConcreteStreamData::Unit => write!(f, "()"),
        }
    }
}

pub trait StreamData: Clone + Send + Sync + Debug + 'static {}

// Trait defining the allowed types for expression values
impl StreamData for i64 {}
impl StreamData for String {}
impl StreamData for bool {}
impl StreamData for () {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum StreamType {
    Int,
    Str,
    Bool,
    Unit,
}

// Could also do this with async steams
// trait InputStream = Iterator<Item = StreamData>;

#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord)]
// TODO: Use String instead of Box<str>
pub struct VarName(pub Box<str>);

impl From<&str> for VarName {
    fn from(s: &str) -> Self {
        VarName(s.into())
    }
}

impl From<String> for VarName {
    fn from(s: String) -> Self {
        VarName(s.into_boxed_str())
    }
}

impl Display for VarName {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord)]
// TODO: Use VarName instead of Box<str>
pub struct IndexedVarName(pub Box<str>, pub usize);

pub type OutputStream<T> = BoxStream<'static, T>;

pub trait InputProvider<V> {
    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<V>>;
}

impl<V> InputProvider<V> for BTreeMap<VarName, OutputStream<V>> {
    // We are consuming the input stream from the map when
    // we return it to ensure single ownership and static lifetime
    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<V>> {
        self.remove(var)
    }
}

pub trait StreamContext<Val: StreamData>: Send + 'static {
    fn var(&self, x: &VarName) -> Option<OutputStream<Val>>;

    fn subcontext(&self, history_length: usize) -> Box<dyn StreamContext<Val>>;

    fn advance(&self);
}

pub trait MonitoringSemantics<Expr, Val, CVal = Val>: Clone + Send + 'static {
    // type ExpressionTyping: ExpressionTyping<TypeSystem = <Self::StreamSystem as StreamSystem>::TypeSystem>;

    fn to_async_stream(expr: Expr, ctx: &dyn StreamContext<CVal>) -> OutputStream<Val>;
}

pub trait Specification<Expr> {
    fn input_vars(&self) -> Vec<VarName>;

    fn output_vars(&self) -> Vec<VarName>;

    fn var_expr(&self, var: &VarName) -> Option<Expr>;
}

/*
 * A runtime monitor for a model/specification of type M over streams with
 * values of type V.
 */
pub trait Monitor<M, V> {
    fn new(model: M, input: impl InputProvider<V>) -> Self;

    fn spec(&self) -> &M;

    fn monitor_outputs(&mut self) -> BoxStream<'static, BTreeMap<VarName, V>>;
}
