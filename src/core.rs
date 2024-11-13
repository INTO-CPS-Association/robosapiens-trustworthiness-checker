use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
};

use futures::stream::BoxStream;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Str(String),
    Bool(bool),
    Unknown,
    Unit,
}
impl StreamData for Value {}

impl TryFrom<Value> for i64 {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Int(i) => Ok(i),
            _ => Err(()),
        }
    }
}
impl TryFrom<Value> for String {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Str(i) => Ok(i),
            _ => Err(()),
        }
    }
}
impl TryFrom<Value> for bool {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Bool(i) => Ok(i),
            _ => Err(()),
        }
    }
}
impl TryFrom<Value> for () {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Unit => Ok(()),
            _ => Err(()),
        }
    }
}
impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Int(value)
    }
}
impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::Str(value)
    }
}
impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::Str(value.to_string())
    }
}
impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}
impl From<()> for Value {
    fn from(_value: ()) -> Self {
        Value::Unit
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(i) => write!(f, "{}", i),
            Value::Str(s) => write!(f, "{}", s),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Unknown => write!(f, "unknown"),
            Value::Unit => write!(f, "()"),
        }
    }
}

/* Trait for the values being sent along streams. This could be just Value for
 * untimed heterogeneous streams, more specific types for homogeneous (typed)
 * streams, or time-stamped values for timed streams. This traits allows
 * for the implementation of runtimes to be agnostic of the types of stream
 * values used. */
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
pub struct VarName(pub String);

impl From<&str> for VarName {
    fn from(s: &str) -> Self {
        VarName(s.into())
    }
}

impl From<String> for VarName {
    fn from(s: String) -> Self {
        VarName(s)
    }
}

impl Display for VarName {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct IndexedVarName(pub String, pub usize);

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
