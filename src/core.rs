use std::{
    cell::RefCell,
    collections::BTreeMap,
    fmt::{Debug, Display},
    rc::Rc,
};

use async_trait::async_trait;
use ecow::EcoString;
use ecow::EcoVec;
use futures::future::LocalBoxFuture;
use serde::{Deserialize, Serialize};
use smol::LocalExecutor;

use crate::dep_manage::interface::DependencyManager;

// Global list of all variables in the system. This is used
// to represent individual variables as indices in the runtime
// instead of strings. This makes variables very cheap to clone,
// order, or compare.
//
// Variable names are assumed to act like atoms in programming languages:
// the only permitted operations are equality, cloning, comparison,
// and hashing (the results of the latter two operations is arbitrary
// but consistent for a given program run). Variables are thread local.
// They can also be converted to and from strings. For all of these operations
// they should act indistinguishably from strings: any case in which this
// global state is observable within these constraints is a bug.
//
// This is related to: https://dl.acm.org/doi/10.5555/646066.756689
// and is a pretty standard technique in both programming languages
// implementations and computer algebra systems.
//
// Note that this means that we leak some memory for each unique
// variable encountered in the system: this is hopefully an acceptable
// trade-off given how significant this is for symbolic computations and
// how unwieldy any solution without global sharing is.
thread_local! {
    static VAR_LIST: RefCell<Vec<String>> = RefCell::new(Vec::new());
}

// Anything inside a stream should be clonable in O(1) time in order for the
// runtimes to be efficiently implemented. This is why we use EcoString and
// EcoVec instead of String and Vec. These types are essentially references
// which allow mutation in place if there is only one reference to the data or
// copy-on-write if there is more than one reference.
// Floats are represented as f32 since this is the most common type in
// Ros messages (e.g. LaserScan).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Int(i64),
    Float(f32),
    Str(EcoString),
    Bool(bool),
    List(EcoVec<Value>),
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
impl TryFrom<Value> for f32 {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Float(x) => Ok(x),
            _ => Err(()),
        }
    }
}
impl TryFrom<Value> for String {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Str(i) => Ok(i.to_string()),
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
impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Value::Float(value)
    }
}
impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::Str(value.into())
    }
}
impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::Str(value.into())
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
            Value::Float(fl) => write!(f, "{}", fl),
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
pub trait StreamData: Clone + Debug + 'static {}

// Trait defining the allowed types for expression values
impl StreamData for i64 {}
impl StreamData for f32 {}
impl StreamData for String {}
impl StreamData for bool {}
impl StreamData for () {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum StreamType {
    Int,
    Float,
    Str,
    Bool,
    Unit,
}

// Could also do this with async steams
// trait InputStream = Iterator<Item = StreamData>;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VarName(usize);

impl VarName {
    pub fn new(name: &str) -> Self {
        VAR_LIST.with(|var_list| {
            if let Some(pos) = var_list.borrow().iter().position(|x| x == name) {
                VarName(pos)
            } else {
                let pos = var_list.borrow().len();
                var_list.borrow_mut().push(name.into());
                VarName(pos)
            }
        })
    }

    pub fn name(&self) -> String {
        VAR_LIST.with(|var_list| var_list.borrow()[self.0].clone())
    }
}

impl From<&str> for VarName {
    fn from(s: &str) -> Self {
        VarName::new(s)
    }
}

impl From<String> for VarName {
    fn from(s: String) -> Self {
        VarName::new(&s)
    }
}

impl Display for VarName {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl Debug for VarName {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "VarName::new(\"{}\")", self.name())
    }
}

impl Serialize for VarName {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.name().serialize(serializer)
    }
}

impl<'a> Deserialize<'a> for VarName {
    fn deserialize<D: serde::Deserializer<'a>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        Ok(VarName::new(&name))
    }
}

impl From<&VarName> for String {
    fn from(var_name: &VarName) -> String {
        var_name.name()
    }
}

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

pub trait Specification {
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
    fn provide_streams(&mut self, streams: Vec<OutputStream<Self::Val>>);

    fn var_names(&self) -> Vec<VarName>;

    // Essentially this is of type
    // async fn run(&mut self);
    fn run(&mut self) -> LocalBoxFuture<'static, ()>;
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
