use std::{collections::BTreeMap, fmt::Debug, fmt::Display};

use futures::stream::BoxStream;

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum ConcreteStreamData {
    Int(i64),
    Str(String),
    Bool(bool),
    Unknown,
    Unit,
}

pub trait StreamData: Clone + Send + Sync + Debug + 'static {}

impl StreamData for ConcreteStreamData {}

impl Display for ConcreteStreamData {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ConcreteStreamData::Int(n) => write!(f, "{}", n),
            ConcreteStreamData::Bool(b) => write!(f, "{}", if *b { "true" } else { "false" }),
            ConcreteStreamData::Str(s) => write!(f, "\"{}\"", s),
            ConcreteStreamData::Unknown => write!(f, "unknown"),
            ConcreteStreamData::Unit => write!(f, "unit"),
        }
    }
}

// Could also do this with async steams
// trait InputStream = Iterator<Item = StreamData>;

#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct VarName(pub Box<str>);

impl Display for VarName {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct IndexedVarName(pub Box<str>, pub usize);

pub type OutputStream<T: StreamData> = BoxStream<'static, T>;

pub trait InputProvider<T> {
    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<T>>;
}

impl<T: StreamData> InputProvider<T> for BTreeMap<VarName, OutputStream<T>> {
    // We are consuming the input stream from the map when
    // we return it to ensure single ownership and static lifetime
    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<T>> {
        self.remove(var)
    }
}

pub trait StreamContext<T>: Send + 'static {
    fn var(&self, x: &VarName) -> Option<OutputStream<T>>;

    fn subcontext(&self, history_length: usize) -> Box<dyn StreamContext<T>>;

    fn advance(&self);
}

pub trait StreamExpr {
    fn var(var: &VarName) -> Self;
}

// We do not restrict T to StreamExpr because we want to allow for
// the monitoring semantics to be defined for fragments of the
// stream expression language as well as the top-level stream
// expression language.
// We require copy because we want to be able to
// manage the lifetime of the semantics object
pub trait MonitoringSemantics<T, S: StreamData>: Clone + Send + 'static {
    fn to_async_stream(expr: T, ctx: &dyn StreamContext<S>) -> OutputStream<S>;
}

// A dummy monitoring semantics for monitors which do not support pluggable
// monitoring semantics
#[derive(Clone)]
pub struct FixedSemantics;

impl<T, R: StreamData> MonitoringSemantics<T, R> for FixedSemantics {
    fn to_async_stream(_: T, _: &dyn StreamContext<R>) -> OutputStream<R> {
        unimplemented!("Dummy monitoring semantics; should not be called")
    }
}

pub trait Specification<T: StreamExpr> {
    fn input_vars(&self) -> Vec<VarName>;

    fn output_vars(&self) -> Vec<VarName>;

    fn var_expr(&self, var: &VarName) -> Option<T>;
}

pub trait Monitor<T, S, M, R>
where
    T: StreamExpr,
    S: MonitoringSemantics<T, R>,
    M: Specification<T>,
    R: StreamData,
{
    fn new(model: M, input: impl InputProvider<R>) -> Self;

    fn spec(&self) -> &M;

    fn monitor_outputs(&mut self) -> BoxStream<'static, BTreeMap<VarName, R>>;
}
