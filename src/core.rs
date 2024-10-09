use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    process::Output,
};

use futures::stream::BoxStream;

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
    type Type;
    type TypedExpr;
    type TypedStream;
    // Idea:
    // - types are in Identifier<()>
    // - typed version of S are in Identifier<S>

    // self param?
    fn type_of_expr(value: &Self::TypedExpr) -> Self::Type;

    fn type_of_stream(value: &Self::TypedStream) -> Self::Type;

    // Should we support subtyping this way?
    // fn subtype(&self, x: &Self::Type, ) -> Self::Type;
}

pub trait Value<TS: TypeSystem>: Clone + Debug + PartialEq + Eq {
    fn type_of(&self) -> TS::Type;
}

// struct TypedValue<TS: TypeSystem, Val> {
//     typ: TS::Identifier<()>,
//     value: Val<Type>,
// }

pub struct Untyped;
// pub trait TypedStreamData<T: Type<TS>, TS: TypeSystem>: StreamData<TS> {}
impl TypeSystem for Untyped {
    type Type = ();
    type TypedExpr = ConcreteStreamData;
    type TypedStream = OutputStream<ConcreteStreamData>;

    fn type_of_expr(_: &Self::TypedExpr) -> Self::Type {
        ()
    }

    fn type_of_stream(_: &Self::TypedStream) -> Self::Type {
        ()
    }
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

impl Display for VarName {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct IndexedVarName(pub Box<str>, pub usize);

pub type OutputStream<T> = BoxStream<'static, T>;

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
