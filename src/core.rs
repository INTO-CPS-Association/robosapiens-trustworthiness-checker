use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
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
    type TypedValue: Clone + Send + Sync + Debug + Display + StreamData + 'static;
    // Idea:
    // - types are in Identifier<()>
    // - typed version of S are in Identifier<S>

    // self param?

    fn type_of_value(value: &Self::TypedValue) -> Self::Type;
}

pub trait ExpressionTyping: Sync + Send + 'static {
    type TypeSystem: TypeSystem;
    type TypedExpr;

    fn type_of_expr(value: &Self::TypedExpr) -> <Self::TypeSystem as TypeSystem>::Type;
}

pub trait StreamTransformationFn {
    fn transform<T: 'static>(&self, stream: OutputStream<T>) -> OutputStream<T>;
}

pub trait Value<TS: TypeSystem>: Clone + Debug + PartialEq + Eq + StreamData {
    fn type_of(&self) -> TS::Type;

    fn to_typed_value(&self) -> TS::TypedValue;

    fn from_typed_value(value: TS::TypedValue) -> Option<Self>;
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
pub type TypeContext<TS> = BTreeMap<VarName, <TS as TypeSystem>::Type>;

pub type SemanticResult<Expected> = Result<Expected, SemanticErrors>;

pub trait TypeCheckableHelper<ET: ExpressionTyping> {
    fn type_check_raw(
        &self,
        ctx: &mut TypeContext<ET::TypeSystem>,
        errs: &mut SemanticErrors,
    ) -> Result<ET::TypedExpr, ()>;
}

pub trait TypeCheckable<ET: ExpressionTyping> {
    fn type_check_with_default(&self) -> SemanticResult<ET::TypedExpr> {
        let mut context = TypeContext::<ET::TypeSystem>::new();
        self.type_check(&mut context)
    }

    fn type_check(
        &self,
        context: &mut TypeContext<ET::TypeSystem>,
    ) -> SemanticResult<ET::TypedExpr>;
}

impl<ET: ExpressionTyping, R: TypeCheckableHelper<ET>> TypeCheckable<ET> for R {
    fn type_check(
        &self,
        context: &mut TypeContext<ET::TypeSystem>,
    ) -> SemanticResult<ET::TypedExpr> {
        let mut errors = Vec::new();
        let res = self.type_check_raw(context, &mut errors);
        match res {
            Ok(se) => Ok(se),
            Err(()) => Err(errors),
        }
    }
}

pub trait TypeCheckableSpecification<
    InputET: ExpressionTyping,
    OutputET: ExpressionTyping,
    OutputSpec: Specification<OutputET>,
>: Specification<InputET>
{
    fn type_check(&self) -> SemanticResult<OutputSpec>;
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

pub trait StreamContext<SS: StreamSystem>: Send + 'static {
    fn var(&self, x: &VarName) -> Option<OutputStream<<SS::TypeSystem as TypeSystem>::TypedValue>>;

    fn subcontext(&self, history_length: usize) -> Box<dyn StreamContext<SS>>;

    fn advance(&self);
}

pub trait StreamExpr<Type> {
    fn var(typ: Type, var: &VarName) -> Self;
}

pub trait StreamSystem: Send + 'static {
    type TypeSystem: TypeSystem;
    type TypedStream: Stream<Item = <Self::TypeSystem as TypeSystem>::TypedValue>
        + Unpin
        + Send
        + 'static;
    // It would be nice to have a type alias for the type of the stream
    // but such type aliases are currently unstable
    // type TypedValueStream = OutputStream<<Self::TypeSystem as TypeSystem>::TypedValue>;

    fn to_typed_stream(
        typ: <Self::TypeSystem as TypeSystem>::Type,
        stream: OutputStream<<Self::TypeSystem as TypeSystem>::TypedValue>,
    ) -> Self::TypedStream;

    fn type_of_stream(value: &Self::TypedStream) -> <Self::TypeSystem as TypeSystem>::Type;

    fn transform_stream(
        transformation: impl StreamTransformationFn,
        stream: Self::TypedStream,
    ) -> Self::TypedStream;
}

// We do not restrict E to StreamExpr or TS::TypedExpr because we want to allow
// for the monitoring semantics to be defined for fragments of the
// stream expression language as well as the top-level stream
// expression language.
// We require copy because we want to be able to
// manage the lifetime of the semantics object
pub trait MonitoringSemantics<Expr, StreamType>: Clone + Send + 'static {
    // type ExpressionTyping: ExpressionTyping<TypeSystem = <Self::StreamSystem as StreamSystem>::TypeSystem>;
    type StreamSystem: StreamSystem;

    fn to_async_stream(expr: Expr, ctx: &dyn StreamContext<Self::StreamSystem>) -> StreamType;
}

pub trait Specification<ET: ExpressionTyping> {
    fn input_vars(&self) -> Vec<VarName>;

    fn output_vars(&self) -> Vec<VarName>;

    fn var_expr(&self, var: &VarName) -> Option<ET::TypedExpr>;
}

// Annotations on the types of variables in a specification
pub trait TypeAnnotated<TS: TypeSystem> {
    fn type_of_var(&self, var: &VarName) -> Option<TS::Type>;
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
