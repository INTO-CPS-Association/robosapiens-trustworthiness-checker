use std::{collections::BTreeMap, fmt::Display};

use futures::stream::BoxStream;

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum StreamData {
    Int(i64),
    Str(String),
    Bool(bool),
    Unknown,
    Unit,
}

impl Display for StreamData {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            StreamData::Int(n) => write!(f, "{}", n),
            StreamData::Bool(b) => write!(f, "{}", if *b { "true" } else { "false" }),
            StreamData::Str(s) => write!(f, "\"{}\"", s),
            StreamData::Unknown => write!(f, "unknown"),
            StreamData::Unit => write!(f, "unit"),
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

pub type OutputStream = BoxStream<'static, StreamData>;

pub trait InputProvider {
    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream>;
}

impl InputProvider for BTreeMap<VarName, OutputStream> {
    // We are consuming the input stream from the map when
    // we return it to ensure single ownership and static lifetime
    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream> {
        self.remove(var)
    }
}

pub trait StreamContext: Send + Clone + 'static {
    fn var(&self, x: &VarName) -> Option<OutputStream>;
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
pub trait MonitoringSemantics<T>: Clone + Send + 'static {
    fn to_async_stream(expr: T, ctx: &impl StreamContext) -> OutputStream;
}

// A dummy monitoring semantics for monitors which do not support pluggable
// monitoring semantics
#[derive(Clone)]
pub struct FixedSemantics;

impl<T> MonitoringSemantics<T> for FixedSemantics {
    fn to_async_stream(_: T, _: &impl StreamContext) -> OutputStream {
        unimplemented!("Dummy monitoring semantics; should not be called")
    }
}

pub trait Specification<T: StreamExpr> {
    fn input_vars(&self) -> Vec<VarName>;

    fn output_vars(&self) -> Vec<VarName>;

    fn var_expr(&self, var: &VarName) -> Option<T>;
}

pub trait Monitor<T, S, M>
where
    T: StreamExpr,
    S: MonitoringSemantics<T>,
    M: Specification<T>,
{
    fn new(model: M, input: impl InputProvider) -> Self;

    fn spec(&self) -> &M;

    fn monitor_outputs(&mut self) -> BoxStream<'static, BTreeMap<VarName, StreamData>>;
}
