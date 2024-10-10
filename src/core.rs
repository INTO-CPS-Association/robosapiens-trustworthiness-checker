use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    process::Output,
};

use futures::{stream::BoxStream, Stream};

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum ConcreteStreamData {
    Int(i64),
    Str(String),
    Bool(bool),
    Unknown,
    Unit,
}

impl From<&str> for ConcreteStreamData {
    fn from(value: &str) -> Self {
        ConcreteStreamData::Str(value.to_string())
    }
}
impl From<String> for ConcreteStreamData {
    fn from(value: String) -> Self {
        ConcreteStreamData::Str(value)
    }
}

impl From<i64> for ConcreteStreamData {
    fn from(value: i64) -> Self {
        ConcreteStreamData::Int(value)
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

pub trait TypeSystem: Sync + Send + 'static {
    // Type identifying the type of an object within the type system
    // e.g. an enum or an ID type
    // Inner type allows for the type annotation to tag a specific object
    type Type: Clone + Send + Sync + Ord + PartialEq + Eq + Debug + 'static;
    type TypedExpr: StreamExpr<Self::Type>;
    type TypedStream: Stream<Item = Self::TypedValue> + Unpin + Send + 'static;
    type TypedValue: Clone + Send + Sync + Debug + Display + 'static;
    // Idea:
    // - types are in Identifier<()>
    // - typed version of S are in Identifier<S>

    // self param?
    fn type_of_expr(value: &Self::TypedExpr) -> Self::Type;

    fn type_of_value(value: &Self::TypedValue) -> Self::Type;

    fn type_of_stream(value: &Self::TypedStream) -> Self::Type;
    // Should we support subtyping this way?
    // fn subtype(&self, x: &Self::Type, ) -> Self::Type;

    fn transform_stream(
        transformation: impl StreamTransformationFn,
        stream: Self::TypedStream,
    ) -> Self::TypedStream;

    fn to_typed_stream(
        typ: Self::Type,
        stream: OutputStream<Self::TypedValue>,
    ) -> Self::TypedStream;
}

pub trait StreamTransformationFn {
    fn transform<T: 'static>(&self, stream: OutputStream<T>) -> OutputStream<T>;
}

pub trait Value<TS: TypeSystem>: Clone + Debug + PartialEq + Eq {
    fn type_of(&self) -> TS::Type;

    fn to_typed_value(&self) -> TS::TypedValue;
}

// struct TypedValue<TS: TypeSystem, Val> {
//     typ: TS::Identifier<()>,
//     value: Val<Type>,
// }

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

#[derive(Debug, PartialEq, Eq)]
pub enum SemanticError {
    TypeError(String),
    UnknownError(String),
    UndeclaredVariable(String),
}

pub type SemanticErrors = Vec<SemanticError>;
pub type TypeContext<TS: TypeSystem> = BTreeMap<VarName, TS::Type>;

pub type SemanticResult<Expected> = Result<Expected, SemanticErrors>;

pub trait TypeCheckableHelper<TS: TypeSystem> {
    fn type_check_raw(
        &self,
        ctx: &mut TypeContext<TS>,
        errs: &mut SemanticErrors,
    ) -> Result<TS::TypedExpr, ()>;
}

pub trait TypeCheckable<TS: TypeSystem> {
    fn type_check_with_default(&self) -> SemanticResult<TS::TypedExpr> {
        let mut context = TypeContext::<TS>::new();
        self.type_check(&mut context)
    }

    fn type_check(&self, context: &mut TypeContext<TS>) -> SemanticResult<TS::TypedExpr>;
}

impl<TS: TypeSystem, R: TypeCheckableHelper<TS>> TypeCheckable<TS> for R {
    fn type_check(&self, context: &mut TypeContext<TS>) -> SemanticResult<TS::TypedExpr> {
        let mut errors = Vec::new();
        let res = self.type_check_raw(context, &mut errors);
        match res {
            Ok(se) => Ok(se),
            Err(()) => Err(errors),
        }
    }
}

// Could also do this with async steams
// trait InputStream = Iterator<Item = StreamData>;

#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord)]
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
pub struct IndexedVarName(pub Box<str>, pub usize);

pub type OutputStream<T> = BoxStream<'static, T>;

pub trait InputProvider<S> {
    fn input_stream(&mut self, var: &VarName) -> Option<S>;
}

impl<S> InputProvider<OutputStream<S>> for BTreeMap<VarName, OutputStream<S>> {
    // We are consuming the input stream from the map when
    // we return it to ensure single ownership and static lifetime
    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<S>> {
        self.remove(var)
    }
}

pub trait StreamContext<TS: TypeSystem>: Send + 'static {
    fn var(&self, x: &VarName) -> Option<TS::TypedStream>;

    fn subcontext(&self, history_length: usize) -> Box<dyn StreamContext<TS>>;

    fn advance(&self);
}

pub trait StreamExpr<Type> {
    fn var(typ: Type, var: &VarName) -> Self;
}

// We do not restrict T to StreamExpr because we want to allow for
// the monitoring semantics to be defined for fragments of the
// stream expression language as well as the top-level stream
// expression language.
// We require copy because we want to be able to
// manage the lifetime of the semantics object
pub trait MonitoringSemantics<S, TS: TypeSystem>: Clone + Send + 'static {
    fn to_async_stream(expr: S, ctx: &dyn StreamContext<TS>) -> TS::TypedStream;
}

// A dummy monitoring semantics for monitors which do not support pluggable
// monitoring semantics
#[derive(Clone)]
pub struct FixedSemantics;

impl<S, TS: TypeSystem> MonitoringSemantics<S, TS> for FixedSemantics {
    fn to_async_stream(_: S, _: &dyn StreamContext<TS>) -> TS::TypedStream {
        unimplemented!("Dummy monitoring semantics; should not be called")
    }
}

pub trait Specification<TS: TypeSystem> {
    fn input_vars(&self) -> Vec<VarName>;

    fn output_vars(&self) -> Vec<VarName>;

    fn var_expr(&self, var: &VarName) -> Option<TS::TypedExpr>;
}

// Annotations on the types of variables in a specification
pub trait TypeAnnotated<TS: TypeSystem> {
    fn type_of_var(&self, var: &VarName) -> TS::Type;
}

pub trait Monitor<TS, S, M>
where
    TS: TypeSystem,
    S: MonitoringSemantics<TS::TypedExpr, TS>,
    M: Specification<TS> + TypeAnnotated<TS>,
{
    fn new(model: M, input: impl InputProvider<TS::TypedStream>) -> Self;

    fn spec(&self) -> &M;

    fn monitor_outputs(&mut self) -> BoxStream<'static, BTreeMap<VarName, TS::TypedValue>>;
}
