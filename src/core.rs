use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    rc::Rc,
};

use async_trait::async_trait;
use futures::future::LocalBoxFuture;
use serde::{Deserialize, Serialize};
use smol::LocalExecutor;

use crate::dep_manage::interface::DependencyManager;

// use serde_json::{Deserializer, Sserializer};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Int(i64),
    Str(String),
    Bool(bool),
    List(Vec<Value>),
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
            Value::List(vals) => {
                write!(f, "[")?;
                for val in vals.iter() {
                    write!(f, "{}, ", val)?;
                }
                write!(f, "]")
            }
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

#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord, Serialize, Deserialize, Hash)]
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

pub type OutputStream<T> = futures::stream::LocalBoxStream<'static, T>;

pub trait InputProvider {
    type Val;

    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Self::Val>>;
}

impl<V> InputProvider for BTreeMap<VarName, OutputStream<V>> {
    type Val = V;

    // We are consuming the input stream from the map when
    // we return it to ensure single ownership and static lifetime
    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Self::Val>> {
        self.remove(var)
    }
}

pub trait StreamContext<Val: StreamData>: 'static {
    fn var(&self, x: &VarName) -> Option<OutputStream<Val>>;

    fn subcontext(&self, history_length: usize) -> Box<dyn SyncStreamContext<Val>>;
}

#[async_trait(?Send)]
pub trait SyncStreamContext<Val: StreamData>: StreamContext<Val> + 'static {
    /// Advance the clock used by the context by one step, letting all
    /// streams to progress (blocking)
    async fn advance_clock(&mut self);

    /// Try to advance clock used by the context by one step, letting all
    /// streams to progress (non-blocking, don't care if anything happens)
    async fn lazy_advance_clock(&mut self);

    /// Set the clock to automatically advance, allowing all substreams
    /// to progress freely (limited only by buffering)
    async fn start_auto_clock(&mut self);

    /// Check if the clock is currently started
    fn is_clock_started(&self) -> bool;

    /// Get the current value of the clock (this may not guarantee
    /// that all stream have reached this time)
    fn clock(&self) -> usize;

    // This allows TimedStreamContext to be used as a StreamContext
    // This is necessary due to https://github.com/rust-lang/rust/issues/65991
    fn upcast(&self) -> &dyn StreamContext<Val>;
}

pub trait MonitoringSemantics<Expr, Val, CVal = Val>: Clone + 'static {
    fn to_async_stream(expr: Expr, ctx: &dyn StreamContext<CVal>) -> OutputStream<Val>;
}

pub trait Specification: Sync + Send {
    type Expr;

    fn input_vars(&self) -> Vec<VarName>;

    fn output_vars(&self) -> Vec<VarName>;

    fn var_expr(&self, var: &VarName) -> Option<Self::Expr>;
}

// This could alternatively implement Sink
// The constructor (which is not specified by the trait) should provide any
// configuration details needed by the output handler (e.g. host, port,
// output file name, etc.) whilst provide_streams is called by the runtime to
// finish the setup of the output handler by providing the streams to be output,
// and finally run is called to start the output handler.
#[async_trait(?Send)]
pub trait OutputHandler {
    type Val: StreamData;

    // async fn handle_output(&mut self, var: &VarName, value: V);
    // This should only be called once by the runtime to provide the streams
    fn provide_streams(&mut self, streams: BTreeMap<VarName, OutputStream<Self::Val>>);

    // Essentially this is of type
    // async fn run(&mut self);
    fn run(&mut self) -> LocalBoxFuture<'static, ()>;
    //  -> Pin<Box<dyn Future<Output = ()> + 'static + Send>>;
}

/*
 * A runtime monitor for a model/specification of type M over streams with
 * values of type V.
 *
 * The input provider is provided as an Arc<Mutex<dyn InputProvider<V>>> to allow a dynamic
 * type of input provider to be provided and allows the output
 * to borrow from the input provider without worrying about lifetimes.
 */
#[async_trait(?Send)]
pub trait Monitor<M, V: StreamData> {
    fn new(
        executor: Rc<LocalExecutor<'static>>,
        model: M,
        input: &mut dyn InputProvider<Val = V>,
        output: Box<dyn OutputHandler<Val = V>>,
        dependencies: DependencyManager,
    ) -> Self;

    fn spec(&self) -> &M;

    // Should usually wait on the output provider
    async fn run(mut self);

    // fn monitor_outputs(&mut self) -> BoxStream<'static, BTreeMap<VarName, V>>;
}
