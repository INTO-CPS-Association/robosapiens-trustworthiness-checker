use ecow::{EcoString, EcoVec};

use super::ast::{BoolBinOp, CompBinOp, FloatBinOp, IntBinOp, SBinOp, SExpr, StrBinOp};
use crate::core::{StreamData, StreamType, StreamTypeAscription};
use crate::{DsrvSpecification, Specification};
use crate::{Value, VarName};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display};
use std::ops::Deref;

/// Type constructors used by the type-checker internal representation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum TCTypeConstructor {
    List,
    Map,
}

impl std::fmt::Display for TCTypeConstructor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TCTypeConstructor::List => write!(f, "List"),
            TCTypeConstructor::Map => write!(f, "Map"),
        }
    }
}

/// Type-checker–internal type representation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum TCType {
    Int,
    Float,
    Str,
    Bool,
    Unit,
    /// General application of a type constructor to type arguments. Recursive
    /// data types such as `List<List<Int>>` are represented uniformly here.
    Construct(TCTypeConstructor, Vec<TCType>),
    /// Placeholder used for empty list literals whose element type is not yet known.
    EmptyList,
    /// Placeholder used for empty map literals whose value type is not yet known.
    EmptyMap,
    /// Unknown non-container type used for special values such as Deferred/NoVal.
    Unknown,
}

impl TCType {
    pub fn list(inner: TCType) -> Self {
        TCType::Construct(TCTypeConstructor::List, vec![inner])
    }

    pub fn map(value: TCType) -> Self {
        TCType::Construct(TCTypeConstructor::Map, vec![value])
    }

    pub fn list_element_type(&self) -> Option<&TCType> {
        match self {
            TCType::Construct(TCTypeConstructor::List, args) if args.len() == 1 => args.first(),
            _ => None,
        }
    }

    pub fn map_value_type(&self) -> Option<&TCType> {
        match self {
            TCType::Construct(TCTypeConstructor::Map, args) if args.len() == 1 => args.first(),
            _ => None,
        }
    }

    /// Convert StreamTypes into type checker types
    pub fn from_stream_type(st: &StreamType) -> Self {
        match st {
            StreamType::Int => TCType::Int,
            StreamType::Float => TCType::Float,
            StreamType::Str => TCType::Str,
            StreamType::Bool => TCType::Bool,
            StreamType::Unit => TCType::Unit,
            StreamType::List(inner) => TCType::list(TCType::from_stream_type(inner)),
            StreamType::Map(inner) => TCType::map(TCType::from_stream_type(inner)),
        }
    }

    /// Convert type checker types into stream types
    pub fn to_stream_type(&self) -> Option<StreamType> {
        match self {
            TCType::Int => Some(StreamType::Int),
            TCType::Float => Some(StreamType::Float),
            TCType::Str => Some(StreamType::Str),
            TCType::Bool => Some(StreamType::Bool),
            TCType::Unit => Some(StreamType::Unit),
            TCType::Construct(TCTypeConstructor::List, args) if args.len() == 1 => args[0]
                .to_stream_type()
                .map(|i| StreamType::List(Box::new(i))),
            TCType::Construct(TCTypeConstructor::Map, args) if args.len() == 1 => args[0]
                .to_stream_type()
                .map(|i| StreamType::Map(Box::new(i))),
            TCType::Construct(_, _) | TCType::EmptyList | TCType::EmptyMap | TCType::Unknown => {
                None
            }
        }
    }
}

impl std::fmt::Display for TCType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TCType::Int => write!(f, "Int"),
            TCType::Float => write!(f, "Float"),
            TCType::Str => write!(f, "Str"),
            TCType::Bool => write!(f, "Bool"),
            TCType::Unit => write!(f, "Unit"),
            TCType::Construct(constructor, args) => {
                let args = args
                    .iter()
                    .map(|arg| arg.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{}<{}>", constructor, args)
            }
            TCType::EmptyList => write!(f, "EmptyList"),
            TCType::EmptyMap => write!(f, "EmptyMap"),
            TCType::Unknown => write!(f, "Unknown"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum SemanticError {
    TypeError(String),
    DeferredError(String),
    UndeclaredVariable(String),
    MissingTypeAnnotation(String),
}

pub type SemanticErrors = Vec<SemanticError>;
pub type TypeInfo = BTreeMap<VarName, StreamType>;

pub type SemanticResult<Expected> = Result<Expected, SemanticErrors>;

pub trait TypeCheckableHelper<TypedExpr> {
    fn type_check_raw(
        &self,
        expected: Option<&StreamType>,
        ctx: &mut TypeInfo,
        errs: &mut SemanticErrors,
    ) -> Result<TypedExpr, ()>;
}
impl<TypedExpr, R: TypeCheckableHelper<TypedExpr>> TypeCheckable<TypedExpr> for R {
    fn type_check(&self, context: &mut TypeInfo) -> SemanticResult<TypedExpr> {
        let mut errors = Vec::new();
        let res = self.type_check_raw(None, context, &mut errors);
        match res {
            Ok(se) => Ok(se),
            Err(()) => Err(errors),
        }
    }
}
pub trait TypeCheckable<TypedExpr> {
    fn type_check_with_default(&self) -> SemanticResult<TypedExpr> {
        self.type_check(&mut TypeInfo::new())
    }

    fn type_check(&self, context: &mut TypeInfo) -> SemanticResult<TypedExpr>;
}

#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub enum PartialStreamValue<T> {
    Known(T),
    NoVal,
    Deferred,
}

impl<T: Display> Display for PartialStreamValue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartialStreamValue::Known(val) => write!(f, "{}", val),
            PartialStreamValue::NoVal => write!(f, "no_val"),
            PartialStreamValue::Deferred => write!(f, "⊥"),
        }
    }
}

impl StreamData for PartialStreamValue<bool> {}
impl StreamData for PartialStreamValue<i64> {}
impl StreamData for PartialStreamValue<f64> {}
impl StreamData for PartialStreamValue<String> {}
impl StreamData for PartialStreamValue<()> {}
impl StreamData for PartialStreamValue<EcoVec<Value>> {}
impl StreamData for PartialStreamValue<Value> {}

pub fn extract_type(expr: &SExprTE) -> TCType {
    match expr {
        SExprTE::Bool(_) => TCType::Bool,
        SExprTE::Int(_) => TCType::Int,
        SExprTE::Float(_) => TCType::Float,
        SExprTE::Str(_) => TCType::Str,
        SExprTE::Unit(_) => TCType::Unit,
        SExprTE::List(tl) => tl.list_tc_type(),
        SExprTE::Map(tm) => tm.map_tc_type(),
    }
}

impl Display for SExprTE {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SExprTE::Int(e) => write!(f, "{}", e),
            SExprTE::Float(e) => write!(f, "{}", e),
            SExprTE::Str(e) => write!(f, "{}", e),
            SExprTE::Bool(e) => write!(f, "{}", e),
            SExprTE::Unit(e) => write!(f, "{}", e),
            SExprTE::List(e) => write!(f, "{:?}", e),
            SExprTE::Map(e) => write!(f, "{:?}", e),
        }
    }
}

impl Display for SExprInt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use IntBinOp::*;
        use SExprInt::*;
        match self {
            If(b, e1, e2) => write!(f, "(if {} then {} else {})", b, e1, e2),
            SIndex(s, i) => write!(f, "{}[{}]", s, i),
            Val(v) => write!(f, "{}", v),
            BinOp(e1, e2, Add) => write!(f, "({} + {})", e1, e2),
            BinOp(e1, e2, Sub) => write!(f, "({} - {})", e1, e2),
            BinOp(e1, e2, Mul) => write!(f, "({} * {})", e1, e2),
            BinOp(e1, e2, Div) => write!(f, "({} / {})", e1, e2),
            BinOp(e1, e2, Mod) => write!(f, "({} % {})", e1, e2),
            Var(v) => write!(f, "{}", v),
            Default(e, v) => write!(f, "default({}, {})", e, v),
            Abs(v) => write!(f, "abs({})", v),
            Init(e1, e2) => write!(f, "init({}, {})", e1, e2),
            Defer(e, _, _) => write!(f, "defer({}: Int)", e),
            Dynamic(e, _) => write!(f, "dynamic({}: Int)", e),
            RestrictedDynamic(e, env, _) => {
                let env = env
                    .iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(f, "dynamic({}: Int, {{{}}})", e, env)
            }
            LLen(list) => write!(f, "len({:?})", list),
            LHeadList(list) => write!(f, "head({:?})", list),
            LIndexList(list, idx) => write!(f, "index({:?}, {})", list, idx),
            MGetMap(map, key) => write!(f, "Map.get({:?}, {:?})", map, key),
        }
    }
}

impl Display for SExprFloat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use FloatBinOp::*;
        use SExprFloat::*;
        match self {
            If(b, e1, e2) => write!(f, "(if {} then {} else {})", b, e1, e2),
            SIndex(s, i) => write!(f, "{}[{}]", s, i),
            Val(v) => write!(f, "{}", v),
            BinOp(e1, e2, Add) => write!(f, "({} + {})", e1, e2),
            BinOp(e1, e2, Sub) => write!(f, "({} - {})", e1, e2),
            BinOp(e1, e2, Mul) => write!(f, "({} * {})", e1, e2),
            BinOp(e1, e2, Div) => write!(f, "({} / {})", e1, e2),
            BinOp(e1, e2, Mod) => write!(f, "({} % {})", e1, e2),
            Var(v) => write!(f, "{}", v),
            Default(e, v) => write!(f, "default({}, {})", e, v),
            Sin(v) => write!(f, "sin({})", v),
            Cos(v) => write!(f, "cos({})", v),
            Tan(v) => write!(f, "tan({})", v),
            Abs(v) => write!(f, "abs({})", v),
            Init(e1, e2) => write!(f, "init({}, {})", e1, e2),
            Defer(e, _, _) => write!(f, "defer({}: Float)", e),
            Dynamic(e, _) => write!(f, "dynamic({}: Float)", e),
            RestrictedDynamic(e, env, _) => {
                let env = env
                    .iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(f, "dynamic({}: Float, {{{}}})", e, env)
            }
            LHeadList(list) => write!(f, "head({:?})", list),
            LIndexList(list, idx) => write!(f, "index({:?}, {})", list, idx),
            MGetMap(map, key) => write!(f, "Map.get({:?}, {:?})", map, key),
        }
    }
}

impl Display for SExprStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use SExprStr::*;
        match self {
            If(b, e1, e2) => write!(f, "(if {} then {} else {})", b, e1, e2),
            SIndex(s, i) => write!(f, "{}[{}]", s, i),
            BinOp(e1, e2, StrBinOp::Concat) => write!(f, "({} ++ {})", e1, e2),
            Val(v) => write!(f, "{}", v),
            Var(v) => write!(f, "{}", v),
            Default(e, v) => write!(f, "default({}, {})", e, v),
            Init(e1, e2) => write!(f, "init({}, {})", e1, e2),
            Defer(e, _, _) => write!(f, "defer({}: Str)", e),
            Dynamic(e, _) => write!(f, "dynamic({}: Str)", e),
            RestrictedDynamic(e, env, _) => {
                let env = env
                    .iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(f, "dynamic({}: Str, {{{}}})", e, env)
            }
            LHeadList(list) => write!(f, "head({:?})", list),
            LIndexList(list, idx) => write!(f, "index({:?}, {})", list, idx),
            MGetMap(map, key) => write!(f, "Map.get({:?}, {:?})", map, key),
        }
    }
}

impl Display for SExprUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use SExprUnit::*;
        match self {
            If(b, e1, e2) => write!(f, "(if {} then {} else {})", b, e1, e2),
            SIndex(s, i) => write!(f, "{}[{}]", s, i),
            Val(v) => write!(f, "{:?}", v),
            Var(v) => write!(f, "{}", v),
            Default(e, v) => write!(f, "default({}, {})", e, v),
            Init(e1, e2) => write!(f, "init({}, {})", e1, e2),
            Defer(e, _, _) => write!(f, "defer({}: Unit)", e),
            Dynamic(e, _) => write!(f, "dynamic({}: Unit)", e),
            RestrictedDynamic(e, env, _) => {
                let env = env
                    .iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(f, "dynamic({}: Unit, {{{}}})", e, env)
            }
            LHeadList(list) => write!(f, "head({:?})", list),
            LIndexList(list, idx) => write!(f, "index({:?}, {})", list, idx),
            MGetMap(map, key) => write!(f, "Map.get({:?}, {:?})", map, key),
        }
    }
}

impl Display for SExprBool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use BoolBinOp::*;
        use CompBinOp::*;
        use SExprBool::*;
        match self {
            Val(v) => write!(f, "{}", v),

            Cmp(Eq, e1, e2) => write!(f, "({} == {})", e1, e2),
            Cmp(Le, e1, e2) => write!(f, "({} <= {})", e1, e2),
            Cmp(Lt, e1, e2) => write!(f, "({} < {})", e1, e2),
            Cmp(Ge, e1, e2) => write!(f, "({} >= {})", e1, e2),
            Cmp(Gt, e1, e2) => write!(f, "({} > {})", e1, e2),

            BinOp(e1, e2, Or) => write!(f, "({} || {})", e1, e2),
            BinOp(e1, e2, And) => write!(f, "({} && {})", e1, e2),
            BinOp(e1, e2, Impl) => write!(f, "({} => {})", e1, e2),

            Not(b) => write!(f, "!{}", b),
            If(b, e1, e2) => write!(f, "(if {} then {} else {})", b, e1, e2),
            SIndex(s, i) => write!(f, "{}[{}]", s, i),
            Var(v) => write!(f, "{}", v),
            Default(e, v) => write!(f, "default({}, {})", e, v),
            Init(e1, e2) => write!(f, "init({}, {})", e1, e2),

            IsDefined(sexpr) => write!(f, "is_defined({})", sexpr),
            When(sexpr) => write!(f, "when({})", sexpr),
            LHeadList(list) => write!(f, "head({:?})", list),
            LIndexList(list, idx) => write!(f, "index({:?}, {})", list, idx),
            MGetMap(map, key) => write!(f, "Map.get({:?}, {:?})", map, key),
            MHasKeyMap(map, key) => write!(f, "Map.has_key({:?}, {:?})", map, key),

            Defer(e, _, _) => write!(f, "defer({}: Bool)", e),
            Dynamic(e, _) => write!(f, "dynamic({}: Bool)", e),
            RestrictedDynamic(e, env, _) => {
                let env = env
                    .iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(f, "dynamic({}: Bool, {{{}}})", e, env)
            }
        }
    }
}

impl TryFrom<Value> for PartialStreamValue<i64> {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Int(x) => Ok(PartialStreamValue::Known(x)),
            Value::NoVal => Ok(PartialStreamValue::NoVal),
            Value::Deferred => Ok(PartialStreamValue::Deferred),
            _ => Err(()),
        }
    }
}

// TODO: should the typed semantics actually use EcoString instead of String?
impl TryFrom<Value> for PartialStreamValue<String> {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Str(x) => Ok(PartialStreamValue::Known(x.into())),
            Value::NoVal => Ok(PartialStreamValue::NoVal),
            Value::Deferred => Ok(PartialStreamValue::Deferred),
            _ => Err(()),
        }
    }
}

impl TryFrom<Value> for PartialStreamValue<f64> {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Float(x) => Ok(PartialStreamValue::Known(x)),
            Value::NoVal => Ok(PartialStreamValue::NoVal),
            Value::Deferred => Ok(PartialStreamValue::Deferred),
            _ => Err(()),
        }
    }
}

impl TryFrom<Value> for PartialStreamValue<bool> {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Bool(x) => Ok(PartialStreamValue::Known(x)),
            Value::NoVal => Ok(PartialStreamValue::NoVal),
            Value::Deferred => Ok(PartialStreamValue::Deferred),
            _ => Err(()),
        }
    }
}

impl TryFrom<Value> for PartialStreamValue<()> {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Unit => Ok(PartialStreamValue::Known(())),
            Value::NoVal => Ok(PartialStreamValue::NoVal),
            Value::Deferred => Ok(PartialStreamValue::Deferred),
            _ => Err(()),
        }
    }
}

impl TryFrom<Value> for PartialStreamValue<EcoVec<Value>> {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::List(x) => Ok(PartialStreamValue::Known(x)),
            Value::NoVal => Ok(PartialStreamValue::NoVal),
            Value::Deferred => Ok(PartialStreamValue::Deferred),
            _ => Err(()),
        }
    }
}

impl From<PartialStreamValue<i64>> for Value {
    fn from(value: PartialStreamValue<i64>) -> Self {
        match value {
            PartialStreamValue::Known(v) => Value::Int(v),
            PartialStreamValue::NoVal => Value::NoVal,
            PartialStreamValue::Deferred => Value::Deferred,
        }
    }
}

impl From<PartialStreamValue<f64>> for Value {
    fn from(value: PartialStreamValue<f64>) -> Self {
        match value {
            PartialStreamValue::Known(v) => Value::Float(v),
            PartialStreamValue::NoVal => Value::NoVal,
            PartialStreamValue::Deferred => Value::Deferred,
        }
    }
}
impl From<PartialStreamValue<String>> for Value {
    fn from(value: PartialStreamValue<String>) -> Self {
        match value {
            PartialStreamValue::Known(v) => Value::Str(v.into()),
            PartialStreamValue::NoVal => Value::NoVal,
            PartialStreamValue::Deferred => Value::Deferred,
        }
    }
}
impl From<PartialStreamValue<bool>> for Value {
    fn from(value: PartialStreamValue<bool>) -> Self {
        match value {
            PartialStreamValue::Known(v) => Value::Bool(v),
            PartialStreamValue::NoVal => Value::NoVal,
            PartialStreamValue::Deferred => Value::Deferred,
        }
    }
}
impl From<PartialStreamValue<()>> for Value {
    fn from(value: PartialStreamValue<()>) -> Self {
        match value {
            PartialStreamValue::Known(()) => Value::Unit,
            PartialStreamValue::NoVal => Value::NoVal,
            PartialStreamValue::Deferred => Value::Deferred,
        }
    }
}

impl From<PartialStreamValue<EcoVec<Value>>> for Value {
    fn from(value: PartialStreamValue<EcoVec<Value>>) -> Self {
        match value {
            PartialStreamValue::Known(v) => Value::List(v),
            PartialStreamValue::NoVal => Value::NoVal,
            PartialStreamValue::Deferred => Value::Deferred,
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum SExprBool {
    Val(PartialStreamValue<bool>),

    // Typed comparison: both operands must have the same type (enforced by the type checker).
    Cmp(CompBinOp, Box<SExprTE>, Box<SExprTE>),

    BinOp(Box<Self>, Box<Self>, BoolBinOp),
    Not(Box<Self>),
    If(Box<SExprBool>, Box<Self>, Box<Self>),

    // Stream indexing
    SIndex(
        // Inner SExpr e
        Box<Self>,
        // Index i
        u64,
    ),

    Var(VarName),

    Default(Box<Self>, Box<Self>),

    // Async operators
    Init(Box<Self>, Box<Self>),

    // Boolean-producing unary operators on typed streams
    IsDefined(Box<SExprTE>),
    When(Box<SExprTE>),

    // List element extraction producing Bool
    LHeadList(TypedListExpr),
    LIndexList(TypedListExpr, Box<SExprInt>),

    // Map operations producing Bool
    MGetMap(TypedMapExpr, EcoString),
    MHasKeyMap(TypedMapExpr, EcoString),

    // Deferred and dynamic expressions
    Defer(Box<SExprStr>, TypeInfo, EcoVec<VarName>),
    Dynamic(Box<SExprStr>, TypeInfo),
    RestrictedDynamic(Box<SExprStr>, EcoVec<VarName>, TypeInfo),
}

#[derive(Clone, PartialEq, Debug)]
pub enum SExprInt {
    If(Box<SExprBool>, Box<Self>, Box<Self>),

    // Stream indexing
    SIndex(
        // Inner SExpr e
        Box<Self>,
        // Index i
        u64,
    ),

    // Arithmetic Stream expression
    Val(PartialStreamValue<i64>),

    BinOp(Box<Self>, Box<Self>, IntBinOp),

    Var(VarName),

    Default(Box<Self>, Box<Self>),

    // Math functions
    Abs(Box<Self>),

    // Async operators
    Init(Box<Self>, Box<Self>),

    // Deferred and dynamic expressions
    Defer(Box<SExprStr>, TypeInfo, EcoVec<VarName>),
    Dynamic(Box<SExprStr>, TypeInfo),
    RestrictedDynamic(Box<SExprStr>, EcoVec<VarName>, TypeInfo),

    // List operations producing Int
    LLen(TypedListExpr),
    LHeadList(TypedListExpr),
    LIndexList(TypedListExpr, Box<SExprInt>),

    // Map value extraction producing Int
    MGetMap(TypedMapExpr, EcoString),
}

#[derive(Clone, PartialEq, Debug)]
pub enum SExprFloat {
    If(Box<SExprBool>, Box<Self>, Box<Self>),

    // Stream indexing
    SIndex(
        // Inner SExpr e
        Box<Self>,
        // Index i
        u64,
    ),

    // Arithmetic Stream expression
    Val(PartialStreamValue<f64>),

    BinOp(Box<Self>, Box<Self>, FloatBinOp),

    Var(VarName),

    Default(Box<Self>, Box<Self>),

    // Trigonometric and math functions
    Sin(Box<Self>),
    Cos(Box<Self>),
    Tan(Box<Self>),
    Abs(Box<Self>),

    // Async operators
    Init(Box<Self>, Box<Self>),

    // Deferred and dynamic expressions
    Defer(Box<SExprStr>, TypeInfo, EcoVec<VarName>),
    Dynamic(Box<SExprStr>, TypeInfo),
    RestrictedDynamic(Box<SExprStr>, EcoVec<VarName>, TypeInfo),

    // List element extraction producing Float
    LHeadList(TypedListExpr),
    LIndexList(TypedListExpr, Box<SExprInt>),

    // Map value extraction producing Float
    MGetMap(TypedMapExpr, EcoString),
}

// Stream expressions - now with types
#[derive(Clone, PartialEq, Debug)]
pub enum SExprUnit {
    If(Box<SExprBool>, Box<Self>, Box<Self>),

    // Stream indexing
    SIndex(
        // Inner SExpr e
        Box<Self>,
        // Index i
        u64,
    ),

    // Arithmetic Stream expression
    Val(PartialStreamValue<()>),

    Var(VarName),

    Default(Box<Self>, Box<Self>),

    // Async operators
    Init(Box<Self>, Box<Self>),

    // Deferred and dynamic expressions
    Defer(Box<SExprStr>, TypeInfo, EcoVec<VarName>),
    Dynamic(Box<SExprStr>, TypeInfo),
    RestrictedDynamic(Box<SExprStr>, EcoVec<VarName>, TypeInfo),

    // List element extraction producing Unit
    LHeadList(TypedListExpr),
    LIndexList(TypedListExpr, Box<SExprInt>),

    // Map value extraction producing Unit
    MGetMap(TypedMapExpr, EcoString),
}

#[derive(Clone, PartialEq, Debug)]
pub enum SExprStr {
    If(Box<SExprBool>, Box<Self>, Box<Self>),

    // Stream indexing
    SIndex(
        // Inner SExpr e
        Box<Self>,
        // Index i
        u64,
    ),

    BinOp(Box<Self>, Box<Self>, StrBinOp),

    // Arithmetic Stream expression
    Val(PartialStreamValue<String>),

    Var(VarName),

    Default(Box<Self>, Box<Self>),

    // Async operators
    Init(Box<Self>, Box<Self>),

    // Deferred and dynamic expressions
    Defer(Box<SExprStr>, TypeInfo, EcoVec<VarName>),
    Dynamic(Box<SExprStr>, TypeInfo),
    RestrictedDynamic(Box<SExprStr>, EcoVec<VarName>, TypeInfo),

    // List element extraction producing Str
    LHeadList(TypedListExpr),
    LIndexList(TypedListExpr, Box<SExprInt>),

    // Map value extraction producing Str
    MGetMap(TypedMapExpr, EcoString),
}

#[derive(Clone, PartialEq, Debug)]
pub enum TypedListExprKind {
    If(Box<SExprBool>, Box<TypedListExpr>, Box<TypedListExpr>),
    SIndex(Box<TypedListExpr>, u64),
    Var(VarName),
    Default(Box<TypedListExpr>, Box<TypedListExpr>),
    Init(Box<TypedListExpr>, Box<TypedListExpr>),
    Defer(Box<SExprStr>, TypeInfo),
    Dynamic(Box<SExprStr>, TypeInfo),
    RestrictedDynamic(Box<SExprStr>, EcoVec<VarName>, TypeInfo),
    Literal(Vec<SExprTE>),
    LTail(Box<TypedListExpr>),
    LConcat(Box<TypedListExpr>, Box<TypedListExpr>),
    LAppend(Box<TypedListExpr>, Box<SExprTE>),
    LHeadList(Box<TypedListExpr>),
    LIndexList(Box<TypedListExpr>, Box<SExprInt>),
    MGetMap(Box<TypedMapExpr>, EcoString),
}

#[derive(Clone, PartialEq, Debug)]
pub struct TypedListExpr {
    /// Full parametric type of this list expression, e.g. `List<Int>` or
    /// `List<List<Bool>>`.
    pub typ: TCType,
    pub kind: TypedListExprKind,
}

impl TypedListExpr {
    pub fn list_tc_type(&self) -> TCType {
        self.typ.clone()
    }

    pub fn element_type(&self) -> &TCType {
        self.typ
            .list_element_type()
            .expect("TypedListExpr must carry a List<T> type")
    }

    pub fn typed_default(&self, other: &TypedListExpr) -> TypedListExpr {
        TypedListExpr {
            typ: self.typ.clone(),
            kind: TypedListExprKind::Default(Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    pub fn typed_if(&self, cond: Box<SExprBool>, other: &TypedListExpr) -> TypedListExpr {
        TypedListExpr {
            typ: self.typ.clone(),
            kind: TypedListExprKind::If(cond, Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    pub fn typed_init(&self, other: &TypedListExpr) -> TypedListExpr {
        TypedListExpr {
            typ: self.typ.clone(),
            kind: TypedListExprKind::Init(Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    pub fn typed_tail(&self) -> TypedListExpr {
        TypedListExpr {
            typ: self.typ.clone(),
            kind: TypedListExprKind::LTail(Box::new(self.clone())),
        }
    }

    pub fn typed_sindex(&self, idx: u64) -> TypedListExpr {
        TypedListExpr {
            typ: self.typ.clone(),
            kind: TypedListExprKind::SIndex(Box::new(self.clone()), idx),
        }
    }

    pub fn typed_concat(&self, other: &TypedListExpr) -> TypedListExpr {
        TypedListExpr {
            typ: self.typ.clone(),
            kind: TypedListExprKind::LConcat(Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    pub fn typed_append(&self, elem: Box<SExprTE>) -> TypedListExpr {
        TypedListExpr {
            typ: self.typ.clone(),
            kind: TypedListExprKind::LAppend(Box::new(self.clone()), elem),
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum TypedMapExprKind {
    Var(VarName),
    Literal(BTreeMap<EcoString, SExprTE>),
    Default(Box<TypedMapExpr>, Box<TypedMapExpr>),
    If(Box<SExprBool>, Box<TypedMapExpr>, Box<TypedMapExpr>),
    Init(Box<TypedMapExpr>, Box<TypedMapExpr>),
    SIndex(Box<TypedMapExpr>, u64),
    Defer(Box<SExprStr>, TypeInfo),
    Dynamic(Box<SExprStr>, TypeInfo),
    RestrictedDynamic(Box<SExprStr>, EcoVec<VarName>, TypeInfo),
    MInsert(Box<TypedMapExpr>, EcoString, Box<SExprTE>),
    MRemove(Box<TypedMapExpr>, EcoString),
    MGetMap(Box<TypedMapExpr>, EcoString),
}

#[derive(Clone, PartialEq, Debug)]
pub struct TypedMapExpr {
    /// Full parametric type of this map expression. Keys are strings, and the
    /// single type argument represents the value type, e.g. `Map<Int>`.
    pub typ: TCType,
    pub kind: TypedMapExprKind,
}

impl TypedMapExpr {
    pub fn map_tc_type(&self) -> TCType {
        self.typ.clone()
    }

    pub fn value_type(&self) -> &TCType {
        self.typ
            .map_value_type()
            .expect("TypedMapExpr must carry a Map<T> type")
    }

    pub fn typed_default(&self, other: &TypedMapExpr) -> TypedMapExpr {
        TypedMapExpr {
            typ: self.typ.clone(),
            kind: TypedMapExprKind::Default(Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    pub fn typed_if(&self, cond: Box<SExprBool>, other: &TypedMapExpr) -> TypedMapExpr {
        TypedMapExpr {
            typ: self.typ.clone(),
            kind: TypedMapExprKind::If(cond, Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    pub fn typed_init(&self, other: &TypedMapExpr) -> TypedMapExpr {
        TypedMapExpr {
            typ: self.typ.clone(),
            kind: TypedMapExprKind::Init(Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    pub fn typed_sindex(&self, idx: u64) -> TypedMapExpr {
        TypedMapExpr {
            typ: self.typ.clone(),
            kind: TypedMapExprKind::SIndex(Box::new(self.clone()), idx),
        }
    }

    pub fn typed_insert(&self, key: EcoString, value: Box<SExprTE>) -> TypedMapExpr {
        TypedMapExpr {
            typ: self.typ.clone(),
            kind: TypedMapExprKind::MInsert(Box::new(self.clone()), key, value),
        }
    }

    pub fn typed_remove(&self, key: EcoString) -> TypedMapExpr {
        TypedMapExpr {
            typ: self.typ.clone(),
            kind: TypedMapExprKind::MRemove(Box::new(self.clone()), key),
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct TypedDataList {
    pub data: EcoVec<Value>,
    pub element_type: TCType,
}

fn check_value_type_ref(typ: &TCType, value: &Value) -> Result<(), String> {
    match (typ, value) {
        (TCType::Str, Value::Str(_)) => Ok(()),
        (TCType::Int, Value::Int(_)) => Ok(()),
        (TCType::Bool, Value::Bool(_)) => Ok(()),
        (TCType::Float, Value::Float(_)) => Ok(()),
        (TCType::Unit, Value::Unit) => Ok(()),
        (TCType::EmptyList | TCType::EmptyMap | TCType::Unknown, _) => Ok(()),
        (typ, Value::List(inner_values)) if typ.list_element_type().is_some() => {
            let inner_type = typ.list_element_type().expect("checked above");
            inner_values
                .iter()
                .try_for_each(|val| check_value_type_ref(inner_type, val))
        }
        (typ, Value::Map(values)) if typ.map_value_type().is_some() => {
            let value_type = typ.map_value_type().expect("checked above");
            values
                .values()
                .try_for_each(|val| check_value_type_ref(value_type, val))
        }
        (typ, value) => Err(format!("Type mismatch between {} and {}", typ, value)),
    }
}

pub fn check_value_type(typ: TCType, value: &Value) -> Result<(), String> {
    check_value_type_ref(&typ, value)
}

pub fn check_value_stream_type(typ: &StreamType, value: &Value) -> Result<(), String> {
    let typ = TCType::from_stream_type(typ);
    check_value_type_ref(&typ, value)
}

pub fn extract_value_type(value: Value) -> TCType {
    match value {
        Value::Str(_) => TCType::Str,
        Value::Int(_) => TCType::Int,
        Value::Bool(_) => TCType::Bool,
        Value::Float(_) => TCType::Float,
        Value::Unit => TCType::Unit,
        Value::List(_) => TCType::list(TCType::EmptyList),
        Value::Map(_) => TCType::map(TCType::EmptyMap),
        Value::Deferred => TCType::Unknown,
        Value::NoVal => TCType::Unknown,
    }
}

impl TypedDataList {
    pub fn new(data: impl Into<EcoVec<Value>>, element_type: TCType) -> TypedDataList {
        TypedDataList {
            data: data.into(),
            element_type: element_type,
        }
    }

    pub fn coerce(
        self: TypedDataList,
        other: TypedDataList,
    ) -> Result<(EcoVec<Value>, EcoVec<Value>, TCType), String> {
        if self.element_type != other.element_type {
            Err(format!(
                "Type mismatch: {} != {}",
                self.element_type, other.element_type
            ))
        } else {
            Ok((self.data, other.data, self.element_type))
        }
    }

    pub fn coerce_elem(
        self: TypedDataList,
        elem: Value,
    ) -> Result<(EcoVec<Value>, Value, TCType), String> {
        match check_value_type(self.element_type.clone(), &elem) {
            Ok(()) => Ok((self.data, elem, self.element_type)),
            Err(err) => Err(err),
        }
    }

    pub fn concat(self, other: TypedDataList) -> Result<TypedDataList, String> {
        self.coerce(other).map(|(mut left, right, element_type)| {
            left.extend(right);
            TypedDataList {
                data: left,
                element_type,
            }
        })
    }

    pub fn append(self, elem: Value) -> Result<TypedDataList, String> {
        self.coerce_elem(elem)
            .map(|(mut data, elem, element_type)| {
                data.push(elem);
                TypedDataList { data, element_type }
            })
    }

    pub fn head(self) -> Result<Value, String> {
        self.data
            .first()
            .cloned()
            .ok_or("List is empty".to_string())
    }

    pub fn tail(self) -> Result<TypedDataList, String> {
        self.data
            .split_first()
            .map(|(_, tail)| TypedDataList {
                data: tail.into(),
                element_type: self.element_type,
            })
            .ok_or("List is empty".to_string())
    }

    pub fn len(self) -> usize {
        self.data.len()
    }
}

impl StreamData for TypedDataList {}

// Stream expression typed enum
#[derive(Debug, PartialEq, Clone)]
pub enum SExprTE {
    Int(SExprInt),
    Float(SExprFloat),
    Str(SExprStr),
    Bool(SExprBool),
    Unit(SExprUnit),
    List(TypedListExpr),
    Map(TypedMapExpr),
}

#[derive(Clone, PartialEq, Debug)]
pub struct TypedDsrvSpecification {
    pub input_vars: BTreeSet<VarName>,
    pub output_vars: BTreeSet<VarName>,
    pub aux_info: BTreeSet<VarName>,
    pub exprs: BTreeMap<VarName, SExprTE>,
    pub type_annotations: BTreeMap<VarName, StreamType>,
}

impl std::fmt::Display for TypedDsrvSpecification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for v in self.input_vars.iter() {
            let typ = self.type_annotations.get(v).ok_or(std::fmt::Error)?;
            writeln!(f, "in {}: {}", v, typ)?;
        }
        for v in self.output_vars.iter() {
            let typ = self.type_annotations.get(v).ok_or(std::fmt::Error)?;
            writeln!(f, "out {}: {}", v, typ)?;
        }
        for (v, e) in self.exprs.iter() {
            writeln!(f, "{} = {}", v, e)?;
        }
        Ok(())
    }
}

impl Specification for TypedDsrvSpecification {
    type Expr = SExprTE;

    fn input_vars(&self) -> BTreeSet<VarName> {
        self.input_vars.clone()
    }

    fn output_vars(&self) -> BTreeSet<VarName> {
        self.output_vars.clone()
    }

    fn aux_vars(&self) -> BTreeSet<VarName> {
        self.aux_info.clone()
    }

    fn var_expr(&self, var: &VarName) -> Option<SExprTE> {
        self.exprs.get(var).cloned()
    }

    fn add_input_var(&mut self, var: VarName) {
        // TODO: How to add type info?
        self.input_vars = self
            .input_vars
            .iter()
            .cloned()
            .chain(std::iter::once(var))
            .collect();
    }

    fn type_annotations(&self) -> BTreeMap<VarName, StreamType> {
        self.type_annotations.clone()
    }
}

pub fn type_check(spec: DsrvSpecification) -> SemanticResult<TypedDsrvSpecification> {
    let type_context = spec.type_annotations.clone();
    let mut typed_exprs = BTreeMap::new();
    let mut errors = vec![];
    for (var, expr) in spec.exprs.iter() {
        let mut ctx = type_context.clone();
        let expected = match ctx.get(var).cloned() {
            Some(t) => t,
            None => {
                errors.push(SemanticError::MissingTypeAnnotation(format!(
                    "Variable {:?} is missing a type annotation",
                    var
                )));
                continue;
            }
        };
        let typed_expr = expr.type_check_raw(Some(&expected), &mut ctx, &mut errors);
        // Check consistency of inferred type with the declared type annotation
        if let Ok(ref te) = typed_expr {
            let actual = extract_type(te);
            if actual != TCType::from_stream_type(&expected) {
                errors.push(SemanticError::TypeError(format!(
                    "Variable {:?} has declared type {:?}, but expression has type {:?}",
                    var, expected, actual
                )));
            }
        }
        typed_exprs.insert(var, typed_expr);
    }
    if errors.is_empty() {
        Ok(TypedDsrvSpecification {
            input_vars: spec.input_vars.clone(),
            output_vars: spec.output_vars.clone(),
            aux_info: spec.aux_info.clone().into_iter().collect(),
            exprs: typed_exprs
                .into_iter()
                .map(|(k, v)| (k.clone(), v.unwrap()))
                .collect(),
            type_annotations: spec.type_annotations.clone(),
        })
    } else {
        Err(errors)
    }
}

impl TypeCheckableHelper<SExprTE> for Value {
    fn type_check_raw(
        &self,
        expected: Option<&StreamType>,
        ctx: &mut TypeInfo,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        match self {
            Value::Int(v) => Ok(SExprTE::Int(SExprInt::Val(PartialStreamValue::Known(*v)))),
            Value::Float(v) => Ok(SExprTE::Float(SExprFloat::Val(PartialStreamValue::Known(
                *v,
            )))),
            Value::Str(v) => Ok(SExprTE::Str(SExprStr::Val(PartialStreamValue::Known(
                v.into(),
            )))),
            Value::Bool(v) => Ok(SExprTE::Bool(SExprBool::Val(PartialStreamValue::Known(*v)))),
            Value::List(elems) => {
                if elems.is_empty() {
                    // Use expected type to resolve the element type of an empty list
                    // Prefer the expected type when available; otherwise fall
                    // back to Poly so that type-erasing operations like
                    // is_defined and len can still accept the empty list.
                    let inner = match expected {
                        Some(StreamType::List(inner)) => TCType::from_stream_type(inner.as_ref()),
                        _ => TCType::EmptyList,
                    };
                    Ok(SExprTE::List(make_list_literal(vec![], &inner)))
                } else {
                    // Derive inner expected type from expected (if it is a list type)
                    let inner_expected = match expected {
                        Some(StreamType::List(inner)) => Some(inner.as_ref()),
                        _ => None,
                    };

                    // Type-check first element to determine the element type
                    let first_te = elems[0].type_check_raw(inner_expected, ctx, errs)?;
                    let first_type = extract_type(&first_te);

                    // Type-check remaining elements and check consistency with first element's type
                    let mut typed_elems = vec![first_te];
                    for elem in elems.iter().skip(1) {
                        let elem_te = elem.type_check_raw(inner_expected, ctx, errs)?;
                        let elem_type = extract_type(&elem_te);
                        if elem_type != first_type {
                            errs.push(SemanticError::TypeError(format!(
                                "List element type mismatch: expected {:?} (from first element), got {:?}",
                                first_type, elem_type
                            )));
                            return Err(());
                        }
                        typed_elems.push(elem_te);
                    }

                    Ok(SExprTE::List(make_list_literal(typed_elems, &first_type)))
                }
            }
            Value::Map(entries) => type_check_map_literal(
                entries
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                expected,
                ctx,
                errs,
            ),
            Value::Unit => Ok(SExprTE::Unit(SExprUnit::Val(PartialStreamValue::Known(())))),
            Value::Deferred => {
                errs.push(SemanticError::DeferredError(format!(
                    "Stream expression {:?} not assigned a type before semantic analysis",
                    self
                )));
                Err(())
            }
            // Not sure how the type-checking should deal with a value not provided
            // Something like skipping type-checking for this value but not the next
            Value::NoVal => todo!(),
        }
    }
}

// Type check a binary operation
impl TypeCheckableHelper<SExprTE> for (SBinOp, &SExpr, &SExpr) {
    fn type_check_raw(
        &self,
        _expected: Option<&StreamType>,
        ctx: &mut TypeInfo,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let (op, se1, se2) = self;
        let se1_check = se1.type_check_raw(None, ctx, errs);
        let se2_check = se2.type_check_raw(None, ctx, errs);

        match (op, se1_check, se2_check) {
            // Integer operations
            (SBinOp::NOp(op), Ok(SExprTE::Int(se1)), Ok(SExprTE::Int(se2))) => {
                match op.clone().try_into() {
                    Ok(op) => Ok(SExprTE::Int(SExprInt::BinOp(
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                        op,
                    ))),
                    Err(_) => {
                        errs.push(SemanticError::TypeError(
                            "Numerical operation not valid on integers".into(),
                        ));
                        Err(())
                    }
                }
            }
            // Pure float operations
            (SBinOp::NOp(op), Ok(SExprTE::Float(se1)), Ok(SExprTE::Float(se2))) => {
                match op.clone().try_into() {
                    Ok(op) => Ok(SExprTE::Float(SExprFloat::BinOp(
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                        op,
                    ))),
                    Err(_) => {
                        errs.push(SemanticError::TypeError(
                            "Numerical operation not valid on integers".into(),
                        ));
                        Err(())
                    }
                }
            }

            // Boolean operations
            (SBinOp::BOp(op), Ok(SExprTE::Bool(se1)), Ok(SExprTE::Bool(se2))) => Ok(SExprTE::Bool(
                SExprBool::BinOp(Box::new(se1.clone()), Box::new(se2.clone()), op.clone()),
            )),
            // String operations
            (SBinOp::SOp(op), Ok(SExprTE::Str(se1)), Ok(SExprTE::Str(se2))) => Ok(SExprTE::Str(
                SExprStr::BinOp(Box::new(se1.clone()), Box::new(se2.clone()), op.clone()),
            )),

            // Comparison operations — both operands must have the same type.
            // Eq is valid for all types; ordering (Le/Lt/Ge/Gt) only for Int, Float, Str.
            (SBinOp::COp(op), Ok(ste1), Ok(ste2)) => {
                let ty1 = extract_type(&ste1);
                let ty2 = extract_type(&ste2);
                if ty1 != ty2 {
                    errs.push(SemanticError::TypeError(format!(
                        "Cannot apply comparison {:?} to expressions of different types: {:?} and {:?}",
                        op, ty1, ty2
                    )));
                    return Err(());
                }
                // Ordering comparisons are only valid for Int, Float, Str
                let ordering_ok = matches!(ty1, TCType::Int | TCType::Float | TCType::Str);
                if *op != CompBinOp::Eq && !ordering_ok {
                    errs.push(SemanticError::TypeError(format!(
                        "Cannot apply ordering comparison {:?} to type {:?}",
                        op, ty1
                    )));
                    return Err(());
                }
                Ok(SExprTE::Bool(SExprBool::Cmp(
                    op.clone(),
                    Box::new(ste1),
                    Box::new(ste2),
                )))
            }

            // Any other case where sub-expressions are Ok, but `op` is not supported
            (_, Ok(ste1), Ok(ste2)) => {
                errs.push(SemanticError::TypeError(format!(
                    "Cannot apply binary function {:?} to expressions of type {:?} and {:?}",
                    op, ste1, ste2
                )));
                Err(())
            }
            // If the underlying values already result in an error then simply propagate
            _ => Err(()),
        }
    }
}

// Type check a default operation
impl TypeCheckableHelper<SExprTE> for (&SExpr, &SExpr) {
    fn type_check_raw(
        &self,
        expected: Option<&StreamType>,
        ctx: &mut TypeInfo,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let (se1, se2) = *self;
        let se1_check = se1.type_check_raw(expected, ctx, errs);
        let se2_check = se2.type_check_raw(expected, ctx, errs);

        match (se1_check, se2_check) {
            (Ok(ste1), Ok(ste2)) => {
                // Matching on type-checked expressions. If same then Ok, else error.
                match (ste1, ste2) {
                    (SExprTE::Int(se1), SExprTE::Int(se2)) => Ok(SExprTE::Int(SExprInt::Default(
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Float(se1), SExprTE::Float(se2)) => Ok(SExprTE::Float(
                        SExprFloat::Default(Box::new(se1.clone()), Box::new(se2.clone())),
                    )),
                    (SExprTE::Str(se1), SExprTE::Str(se2)) => Ok(SExprTE::Str(SExprStr::Default(
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Bool(se1), SExprTE::Bool(se2)) => Ok(SExprTE::Bool(
                        SExprBool::Default(Box::new(se1.clone()), Box::new(se2.clone())),
                    )),
                    (SExprTE::Unit(se1), SExprTE::Unit(se2)) => Ok(SExprTE::Unit(
                        SExprUnit::Default(Box::new(se1.clone()), Box::new(se2.clone())),
                    )),
                    (SExprTE::List(tl1), SExprTE::List(tl2)) => {
                        let t1 = tl1.element_type().clone();
                        let t2 = tl2.element_type().clone();
                        match unify_element_types(&t1, &t2) {
                            Some(resolved) => {
                                let tl1 = coerce_empty_list(tl1, &resolved);
                                let tl2 = coerce_empty_list(tl2, &resolved);
                                Ok(SExprTE::List(tl1.typed_default(&tl2)))
                            }
                            None => {
                                errs.push(SemanticError::TypeError(format!(
                                    "Cannot create default-expression with two different list types: {:?} and {:?}",
                                    t1, t2
                                )));
                                Err(())
                            }
                        }
                    }
                    (SExprTE::Map(tm1), SExprTE::Map(tm2)) => {
                        let t1 = tm1.value_type().clone();
                        let t2 = tm2.value_type().clone();
                        match unify_element_types(&t1, &t2) {
                            Some(resolved) => {
                                let tm1 = coerce_empty_map(tm1, &resolved);
                                let tm2 = coerce_empty_map(tm2, &resolved);
                                Ok(SExprTE::Map(tm1.typed_default(&tm2)))
                            }
                            None => {
                                errs.push(SemanticError::TypeError(format!(
                                    "Cannot create default-expression with two different map types: {:?} and {:?}",
                                    t1, t2
                                )));
                                Err(())
                            }
                        }
                    }
                    (stenum1, stenum2) => {
                        errs.push(SemanticError::TypeError(format!(
                            "Cannot create default-expression with two different types: {:?} and {:?}",
                            stenum1, stenum2
                        )));
                        Err(())
                    }
                }
            }
            // If there's already an error in any branch, propagate the error
            _ => Err(()),
        }
    }
}

// Type check an if expression
impl TypeCheckableHelper<SExprTE> for (&SExpr, &SExpr, &SExpr) {
    fn type_check_raw(
        &self,
        expected: Option<&StreamType>,
        ctx: &mut TypeInfo,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let (b, se1, se2) = *self;
        let b_check = b.type_check_raw(None, ctx, errs);
        let se1_check = se1.type_check_raw(expected, ctx, errs);
        let se2_check = se2.type_check_raw(expected, ctx, errs);

        match (b_check, se1_check, se2_check) {
            (Ok(SExprTE::Bool(b)), Ok(ste1), Ok(ste2)) => {
                // Matching on type-checked expressions. If same then Ok, else error.
                match (ste1, ste2) {
                    (SExprTE::Int(se1), SExprTE::Int(se2)) => Ok(SExprTE::Int(SExprInt::If(
                        Box::new(b.clone()),
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Float(se1), SExprTE::Float(se2)) => {
                        Ok(SExprTE::Float(SExprFloat::If(
                            Box::new(b.clone()),
                            Box::new(se1.clone()),
                            Box::new(se2.clone()),
                        )))
                    }
                    (SExprTE::Str(se1), SExprTE::Str(se2)) => Ok(SExprTE::Str(SExprStr::If(
                        Box::new(b.clone()),
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Bool(se1), SExprTE::Bool(se2)) => Ok(SExprTE::Bool(SExprBool::If(
                        Box::new(b.clone()),
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Unit(se1), SExprTE::Unit(se2)) => Ok(SExprTE::Unit(SExprUnit::If(
                        Box::new(b.clone()),
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::List(tl1), SExprTE::List(tl2)) => {
                        let t1 = tl1.element_type().clone();
                        let t2 = tl2.element_type().clone();
                        match unify_element_types(&t1, &t2) {
                            Some(resolved) => {
                                let tl1 = coerce_empty_list(tl1, &resolved);
                                let tl2 = coerce_empty_list(tl2, &resolved);
                                Ok(SExprTE::List(tl1.typed_if(Box::new(b.clone()), &tl2)))
                            }
                            None => {
                                errs.push(SemanticError::TypeError(format!(
                                    "Cannot create if-expression with two different list types: {:?} and {:?}",
                                    t1, t2
                                )));
                                Err(())
                            }
                        }
                    }
                    (SExprTE::Map(tm1), SExprTE::Map(tm2)) => {
                        let t1 = tm1.value_type().clone();
                        let t2 = tm2.value_type().clone();
                        match unify_element_types(&t1, &t2) {
                            Some(resolved) => {
                                let tm1 = coerce_empty_map(tm1, &resolved);
                                let tm2 = coerce_empty_map(tm2, &resolved);
                                Ok(SExprTE::Map(tm1.typed_if(Box::new(b.clone()), &tm2)))
                            }
                            None => {
                                errs.push(SemanticError::TypeError(format!(
                                    "Cannot create if-expression with two different map types: {:?} and {:?}",
                                    t1, t2
                                )));
                                Err(())
                            }
                        }
                    }
                    (stenum1, stenum2) => {
                        errs.push(SemanticError::TypeError(format!(
                            "Cannot create if-expression with two different types: {:?} and {:?}",
                            stenum1, stenum2
                        )));
                        Err(())
                    }
                }
            }
            (Ok(_), Ok(_), Ok(_)) => {
                errs.push(SemanticError::TypeError(
                    "If expression condition must be a boolean".into(),
                ));
                Err(())
            }
            // If there's already an error in any branch, propagate the error
            _ => Err(()),
        }
    }
}

// Type check an index expression
impl TypeCheckableHelper<SExprTE> for (&SExpr, u64) {
    fn type_check_raw(
        &self,
        expected: Option<&StreamType>,
        ctx: &mut TypeInfo,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let (inner, idx) = *self;
        let inner_check = inner.type_check_raw(expected, ctx, errs);

        match inner_check {
            Ok(ste) => match ste {
                SExprTE::Int(se) => Ok(SExprTE::Int(SExprInt::SIndex(Box::new(se.clone()), idx))),
                SExprTE::Float(se) => Ok(SExprTE::Float(SExprFloat::SIndex(
                    Box::new(se.clone()),
                    idx,
                ))),
                SExprTE::Str(se) => Ok(SExprTE::Str(SExprStr::SIndex(Box::new(se.clone()), idx))),
                SExprTE::Bool(se) => {
                    Ok(SExprTE::Bool(SExprBool::SIndex(Box::new(se.clone()), idx)))
                }
                SExprTE::Unit(se) => {
                    Ok(SExprTE::Unit(SExprUnit::SIndex(Box::new(se.clone()), idx)))
                }
                SExprTE::List(tl) => Ok(SExprTE::List(tl.typed_sindex(idx))),
                SExprTE::Map(tm) => Ok(SExprTE::Map(tm.typed_sindex(idx))),
            },
            // If there's already an error just propagate it
            Err(_) => Err(()),
        }
    }
}

// Type check a variable
impl TypeCheckableHelper<SExprTE> for VarName {
    fn type_check_raw(
        &self,
        _expected: Option<&StreamType>,
        ctx: &mut TypeInfo,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let type_opt = ctx.get(self);
        match type_opt {
            Some(t) => match t {
                StreamType::Int => Ok(SExprTE::Int(SExprInt::Var(self.clone()))),
                StreamType::Float => Ok(SExprTE::Float(SExprFloat::Var(self.clone()))),
                StreamType::Str => Ok(SExprTE::Str(SExprStr::Var(self.clone()))),
                StreamType::Bool => Ok(SExprTE::Bool(SExprBool::Var(self.clone()))),
                StreamType::Unit => Ok(SExprTE::Unit(SExprUnit::Var(self.clone()))),
                StreamType::List(inner) => {
                    let typed_list = make_list_var(self.clone(), &TCType::from_stream_type(inner));
                    Ok(SExprTE::List(typed_list))
                }
                StreamType::Map(inner) => {
                    let typed_map = make_map_var(self.clone(), &TCType::from_stream_type(inner));
                    Ok(SExprTE::Map(typed_map))
                }
            },
            None => {
                errs.push(SemanticError::UndeclaredVariable(format!(
                    "Usage of undeclared variable: {:?}",
                    self
                )));
                Err(())
            }
        }
    }
}

fn make_list_var(var: VarName, element_type: &TCType) -> TypedListExpr {
    TypedListExpr {
        typ: TCType::list(element_type.clone()),
        kind: TypedListExprKind::Var(var),
    }
}

fn make_list_literal(elements: Vec<SExprTE>, element_type: &TCType) -> TypedListExpr {
    TypedListExpr {
        typ: TCType::list(element_type.clone()),
        kind: TypedListExprKind::Literal(elements),
    }
}

fn make_map_var(var: VarName, value_type: &TCType) -> TypedMapExpr {
    TypedMapExpr {
        typ: TCType::map(value_type.clone()),
        kind: TypedMapExprKind::Var(var),
    }
}

fn make_map_literal(elements: BTreeMap<EcoString, SExprTE>, value_type: &TCType) -> TypedMapExpr {
    TypedMapExpr {
        typ: TCType::map(value_type.clone()),
        kind: TypedMapExprKind::Literal(elements),
    }
}

fn type_check_map_literal<T>(
    entries: Vec<(EcoString, T)>,
    expected: Option<&StreamType>,
    ctx: &mut TypeInfo,
    errs: &mut SemanticErrors,
) -> Result<SExprTE, ()>
where
    T: TypeCheckableHelper<SExprTE>,
{
    if entries.is_empty() {
        let value_type = match expected {
            Some(StreamType::Map(inner)) => TCType::from_stream_type(inner.as_ref()),
            _ => TCType::EmptyMap,
        };
        return Ok(SExprTE::Map(make_map_literal(BTreeMap::new(), &value_type)));
    }

    let value_expected = match expected {
        Some(StreamType::Map(inner)) => Some(inner.as_ref()),
        _ => None,
    };

    let mut iter = entries.into_iter();
    let (first_key, first_value) = iter.next().expect("non-empty map checked above");
    let first_te = first_value.type_check_raw(value_expected, ctx, errs)?;
    let first_type = extract_type(&first_te);
    let mut typed_entries = BTreeMap::from([(first_key, first_te)]);

    for (key, value) in iter {
        let value_te = value.type_check_raw(value_expected, ctx, errs)?;
        let value_type = extract_type(&value_te);
        if value_type != first_type {
            errs.push(SemanticError::TypeError(format!(
                "Map value type mismatch: expected {:?} (from first value), got {:?}",
                first_type, value_type
            )));
            return Err(());
        }
        typed_entries.insert(key, value_te);
    }

    Ok(SExprTE::Map(make_map_literal(typed_entries, &first_type)))
}

/// Attempt to unify two [`TCType`]s, treating empty-container placeholders as
/// compatible with concrete types.  Returns the resolved (concrete) type on success, or `None`
/// when the two types are incompatible.
fn unify_element_types(t1: &TCType, t2: &TCType) -> Option<TCType> {
    match (t1, t2) {
        _ if t1 == t2 => Some(t1.clone()),
        (TCType::EmptyList, other) | (other, TCType::EmptyList) => Some(other.clone()),
        (TCType::EmptyMap, other) | (other, TCType::EmptyMap) => Some(other.clone()),
        (TCType::Construct(c1, args1), TCType::Construct(c2, args2))
            if c1 == c2 && args1.len() == args2.len() =>
        {
            let args = args1
                .iter()
                .zip(args2.iter())
                .map(|(a, b)| unify_element_types(a, b))
                .collect::<Option<Vec<_>>>()?;
            Some(TCType::Construct(c1.clone(), args))
        }
        _ => None,
    }
}

/// If `list` has an `EmptyList` element type, reconstruct it as a concrete empty
/// list literal of the given `target_element_type`.  Non-`EmptyList` lists are
/// returned unchanged.
fn coerce_empty_list(list: TypedListExpr, target_element_type: &TCType) -> TypedListExpr {
    if list.element_type() == &TCType::EmptyList {
        make_list_literal(vec![], target_element_type)
    } else {
        list
    }
}

fn coerce_empty_map(map: TypedMapExpr, target_value_type: &TCType) -> TypedMapExpr {
    if map.value_type() == &TCType::EmptyMap {
        make_map_literal(BTreeMap::new(), target_value_type)
    } else {
        map
    }
}

fn typed_map_get(
    typed_map: TypedMapExpr,
    key: EcoString,
    errs: &mut SemanticErrors,
) -> Result<SExprTE, ()> {
    match typed_map.value_type().clone() {
        TCType::Int => Ok(SExprTE::Int(SExprInt::MGetMap(typed_map, key))),
        TCType::Float => Ok(SExprTE::Float(SExprFloat::MGetMap(typed_map, key))),
        TCType::Bool => Ok(SExprTE::Bool(SExprBool::MGetMap(typed_map, key))),
        TCType::Str => Ok(SExprTE::Str(SExprStr::MGetMap(typed_map, key))),
        TCType::Unit => Ok(SExprTE::Unit(SExprUnit::MGetMap(typed_map, key))),
        TCType::EmptyMap => {
            errs.push(SemanticError::TypeError(
                "Cannot determine value type for get from an empty map".into(),
            ));
            Err(())
        }
        TCType::EmptyList | TCType::Unknown => {
            errs.push(SemanticError::TypeError(
                "Cannot determine concrete value type for map get".into(),
            ));
            Err(())
        }
        TCType::Construct(TCTypeConstructor::List, args) if args.len() == 1 => {
            Ok(SExprTE::List(TypedListExpr {
                typ: TCType::list(args[0].clone()),
                kind: TypedListExprKind::MGetMap(Box::new(typed_map), key),
            }))
        }
        TCType::Construct(TCTypeConstructor::Map, args) if args.len() == 1 => {
            Ok(SExprTE::Map(TypedMapExpr {
                typ: TCType::map(args[0].clone()),
                kind: TypedMapExprKind::MGetMap(Box::new(typed_map), key),
            }))
        }
        TCType::Construct(constructor, args) => {
            errs.push(SemanticError::TypeError(format!(
                "Unsupported type constructor application for map value type: {:?}<{:?}>",
                constructor, args
            )));
            Err(())
        }
    }
}

impl TypeCheckableHelper<SExprTE> for (SExpr, StreamTypeAscription) {
    fn type_check_raw(
        &self,
        expected: Option<&StreamType>,
        ctx: &mut TypeInfo,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let (expr, ascription) = self;
        // If an explicit ascription is present, prefer it as the expected type for
        // the inner expression; otherwise fall back to the outer expected type.
        let effective_expected = match ascription {
            StreamTypeAscription::Ascribed(ty) => Some(ty),
            StreamTypeAscription::Unascribed => expected,
        };
        let expr_te = expr.type_check_raw(effective_expected, ctx, errs)?;

        match ascription {
            StreamTypeAscription::Unascribed => Ok(expr_te),
            StreamTypeAscription::Ascribed(expected_ty) => {
                let actual_ty = extract_type(&expr_te);
                if actual_ty == TCType::from_stream_type(expected_ty) {
                    Ok(expr_te)
                } else {
                    errs.push(SemanticError::TypeError(format!(
                        "Type mismatch: expected {:?}, got {:?}",
                        expected_ty, actual_ty
                    )));
                    Err(())
                }
            }
        }
    }
}

// Type check an expression
impl TypeCheckableHelper<SExprTE> for SExpr {
    fn type_check_raw(
        &self,
        expected: Option<&StreamType>,
        ctx: &mut TypeInfo,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        match self {
            SExpr::Val(sdata) => sdata.type_check_raw(expected, ctx, errs),
            SExpr::BinOp(se1, se2, op) => {
                (op.clone(), se1.deref(), se2.deref()).type_check_raw(expected, ctx, errs)
            }
            SExpr::If(b, se1, se2) => {
                (b.deref(), se1.deref(), se2.deref()).type_check_raw(expected, ctx, errs)
            }
            SExpr::SIndex(inner, idx) => (inner.deref(), *idx).type_check_raw(expected, ctx, errs),
            SExpr::Var(id) => id.type_check_raw(expected, ctx, errs),
            SExpr::Dynamic(e, type_ascription) => {
                let e_check = e.type_check_raw(None, ctx, errs)?;

                // Ascriptions are required for defers in strictly-typed expressions
                let type_ascription = match type_ascription {
                    StreamTypeAscription::Ascribed(ta) => ta,
                    StreamTypeAscription::Unascribed => {
                        errs.push(SemanticError::TypeError(format!(
                            "Type ascription required for defer"
                        )));
                        return Err(());
                    }
                };

                // Inner stream type must be Str
                let e_str = match e_check {
                    SExprTE::Str(e_str) => e_str,
                    ty => {
                        errs.push(SemanticError::TypeError(format!(
                            "Expected Dynamic to be applied to a Str, got {:?}",
                            ty
                        )));
                        return Err(());
                    }
                };

                // Use the type ascription to determine the output type
                match &type_ascription {
                    StreamType::Int => Ok(SExprTE::Int(SExprInt::Dynamic(
                        Box::new(e_str),
                        ctx.clone(),
                    ))),
                    StreamType::Float => Ok(SExprTE::Float(SExprFloat::Dynamic(
                        Box::new(e_str),
                        ctx.clone(),
                    ))),
                    StreamType::Str => Ok(SExprTE::Str(SExprStr::Dynamic(
                        Box::new(e_str),
                        ctx.clone(),
                    ))),
                    StreamType::Bool => Ok(SExprTE::Bool(SExprBool::Dynamic(
                        Box::new(e_str),
                        ctx.clone(),
                    ))),
                    StreamType::Unit => Ok(SExprTE::Unit(SExprUnit::Dynamic(
                        Box::new(e_str),
                        ctx.clone(),
                    ))),
                    StreamType::List(inner) => Ok(SExprTE::List(TypedListExpr {
                        typ: TCType::list(TCType::from_stream_type(inner)),
                        kind: TypedListExprKind::Dynamic(Box::new(e_str), ctx.clone()),
                    })),
                    StreamType::Map(inner) => Ok(SExprTE::Map(TypedMapExpr {
                        typ: TCType::map(TCType::from_stream_type(inner)),
                        kind: TypedMapExprKind::Dynamic(Box::new(e_str), ctx.clone()),
                    })),
                }
            }
            SExpr::RestrictedDynamic(e, type_ascription, vs) => {
                let e_check = e.type_check_raw(None, ctx, errs)?;

                // Inner stream type must be Str
                let e_str = match e_check {
                    SExprTE::Str(e_str) => e_str,
                    ty => {
                        errs.push(SemanticError::TypeError(format!(
                            "Expected RestrictedDynamic to be applied to a Str, got {:?}",
                            ty
                        )));
                        return Err(());
                    }
                };

                // Verify type ascription if provided - RestrictedDynamic only supports Str output
                let type_ascription = match type_ascription {
                    StreamTypeAscription::Ascribed(ta) => ta,
                    StreamTypeAscription::Unascribed => {
                        errs.push(SemanticError::TypeError(format!(
                            "Type ascription required for dynamic"
                        )));
                        return Err(());
                    }
                };

                // Use the type ascription to determine the output type
                match &type_ascription {
                    StreamType::Int => Ok(SExprTE::Int(SExprInt::RestrictedDynamic(
                        Box::new(e_str),
                        vs.clone(),
                        ctx.clone(),
                    ))),
                    StreamType::Float => Ok(SExprTE::Float(SExprFloat::RestrictedDynamic(
                        Box::new(e_str),
                        vs.clone(),
                        ctx.clone(),
                    ))),
                    StreamType::Str => Ok(SExprTE::Str(SExprStr::RestrictedDynamic(
                        Box::new(e_str),
                        vs.clone(),
                        ctx.clone(),
                    ))),
                    StreamType::Bool => Ok(SExprTE::Bool(SExprBool::RestrictedDynamic(
                        Box::new(e_str),
                        vs.clone(),
                        ctx.clone(),
                    ))),
                    StreamType::Unit => Ok(SExprTE::Unit(SExprUnit::RestrictedDynamic(
                        Box::new(e_str),
                        vs.clone(),
                        ctx.clone(),
                    ))),
                    StreamType::List(inner) => Ok(SExprTE::List(TypedListExpr {
                        typ: TCType::list(TCType::from_stream_type(inner)),
                        kind: TypedListExprKind::RestrictedDynamic(
                            Box::new(e_str),
                            vs.clone(),
                            ctx.clone(),
                        ),
                    })),
                    StreamType::Map(inner) => Ok(SExprTE::Map(TypedMapExpr {
                        typ: TCType::map(TCType::from_stream_type(inner)),
                        kind: TypedMapExprKind::RestrictedDynamic(
                            Box::new(e_str),
                            vs.clone(),
                            ctx.clone(),
                        ),
                    })),
                }
            }
            SExpr::Defer(e, type_ascription, vs) => {
                let e_check = e.type_check_raw(None, ctx, errs)?;

                // Ascriptions are required for defer in strictly-typed expressions
                let type_ascription = match type_ascription {
                    StreamTypeAscription::Ascribed(ta) => ta,
                    StreamTypeAscription::Unascribed => {
                        errs.push(SemanticError::TypeError(format!(
                            "Type ascription required for defer"
                        )));
                        return Err(());
                    }
                };

                // Inner stream type must be Str
                let e_str = match e_check {
                    SExprTE::Str(e_str) => e_str,
                    ty => {
                        errs.push(SemanticError::TypeError(format!(
                            "Expected Defer to be applied to a Str, got {:?}",
                            ty
                        )));
                        return Err(());
                    }
                };

                // Use the type ascription to determine the output type
                match &type_ascription {
                    StreamType::Int => Ok(SExprTE::Int(SExprInt::Defer(
                        Box::new(e_str),
                        ctx.clone(),
                        vs.clone(),
                    ))),
                    StreamType::Float => Ok(SExprTE::Float(SExprFloat::Defer(
                        Box::new(e_str),
                        ctx.clone(),
                        vs.clone(),
                    ))),
                    StreamType::Str => Ok(SExprTE::Str(SExprStr::Defer(
                        Box::new(e_str),
                        ctx.clone(),
                        vs.clone(),
                    ))),
                    StreamType::Bool => Ok(SExprTE::Bool(SExprBool::Defer(
                        Box::new(e_str),
                        ctx.clone(),
                        vs.clone(),
                    ))),
                    StreamType::Unit => Ok(SExprTE::Unit(SExprUnit::Defer(
                        Box::new(e_str),
                        ctx.clone(),
                        vs.clone(),
                    ))),
                    StreamType::List(inner) => Ok(SExprTE::List(TypedListExpr {
                        typ: TCType::list(TCType::from_stream_type(inner)),
                        kind: TypedListExprKind::Defer(Box::new(e_str), ctx.clone()),
                    })),
                    StreamType::Map(inner) => Ok(SExprTE::Map(TypedMapExpr {
                        typ: TCType::map(TCType::from_stream_type(inner)),
                        kind: TypedMapExprKind::Defer(Box::new(e_str), ctx.clone()),
                    })),
                }
            }
            SExpr::Update(_, _) => todo!("Implement support for Update"),
            SExpr::Default(se, d) => (se.deref(), d.deref()).type_check_raw(expected, ctx, errs),
            SExpr::Not(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(Some(&StreamType::Bool), ctx, errs)?;
                match sexpr_check {
                    SExprTE::Bool(se) => Ok(SExprTE::Bool(SExprBool::Not(Box::new(se)))),
                    _ => {
                        errs.push(SemanticError::TypeError(
                            "Not can only be applied to boolean expressions".into(),
                        ));
                        Err(())
                    }
                }
            }
            SExpr::List(exprs) => {
                if exprs.is_empty() {
                    // Use expected type to resolve the element type of an empty list.
                    // Prefer the expected type when available; otherwise fall
                    // back to Poly so that type-erasing operations like
                    // is_defined and len can still accept the empty list.
                    let inner = match expected {
                        Some(StreamType::List(inner)) => TCType::from_stream_type(inner.as_ref()),
                        _ => TCType::EmptyList,
                    };
                    Ok(SExprTE::List(make_list_literal(vec![], &inner)))
                } else {
                    // Derive inner expected type from expected (if it is a list type)
                    let inner_expected = match expected {
                        Some(StreamType::List(inner)) => Some(inner.as_ref()),
                        _ => None,
                    };

                    // Type-check first element to determine element type
                    let first_te = exprs[0].type_check_raw(inner_expected, ctx, errs)?;
                    let first_type = extract_type(&first_te);

                    // Type-check remaining elements and check consistency with first element's type
                    let mut typed_exprs = vec![first_te];
                    for expr in exprs.iter().skip(1) {
                        let elem_te = expr.type_check_raw(inner_expected, ctx, errs)?;
                        let elem_type = extract_type(&elem_te);
                        if elem_type != first_type {
                            errs.push(SemanticError::TypeError(format!(
                                "List element type mismatch: expected {:?} (from first element), got {:?}",
                                first_type, elem_type
                            )));
                            return Err(());
                        }
                        typed_exprs.push(elem_te);
                    }

                    let typed_list = make_list_literal(typed_exprs, &first_type);
                    Ok(SExprTE::List(typed_list))
                }
            }
            SExpr::LIndex(list_expr, idx_expr) => {
                // If we expect element type T, then the list should be List<T>
                let list_expected = expected.map(|t| StreamType::List(Box::new(t.clone())));
                let list_te = list_expr.type_check_raw(list_expected.as_ref(), ctx, errs)?;
                let idx_te = idx_expr.type_check_raw(Some(&StreamType::Int), ctx, errs)?;

                let idx_int = match idx_te {
                    SExprTE::Int(e) => e,
                    other => {
                        errs.push(SemanticError::TypeError(format!(
                            "LIndex index must be Int, got {:?}",
                            other
                        )));
                        return Err(());
                    }
                };

                match list_te {
                    SExprTE::List(typed_list) => {
                        let elem_type = typed_list.element_type().clone();
                        match elem_type {
                            TCType::Int => Ok(SExprTE::Int(SExprInt::LIndexList(
                                typed_list,
                                Box::new(idx_int),
                            ))),
                            TCType::Float => Ok(SExprTE::Float(SExprFloat::LIndexList(
                                typed_list,
                                Box::new(idx_int),
                            ))),
                            TCType::Bool => Ok(SExprTE::Bool(SExprBool::LIndexList(
                                typed_list,
                                Box::new(idx_int),
                            ))),
                            TCType::Str => Ok(SExprTE::Str(SExprStr::LIndexList(
                                typed_list,
                                Box::new(idx_int),
                            ))),
                            TCType::Unit => Ok(SExprTE::Unit(SExprUnit::LIndexList(
                                typed_list,
                                Box::new(idx_int),
                            ))),
                            TCType::EmptyList => {
                                errs.push(SemanticError::TypeError(
                                    "Cannot determine element type for index into an empty list"
                                        .into(),
                                ));
                                Err(())
                            }
                            TCType::EmptyMap | TCType::Unknown => {
                                errs.push(SemanticError::TypeError(format!(
                                    "Cannot index list with unresolved element type {:?}",
                                    elem_type
                                )));
                                Err(())
                            }
                            TCType::Construct(TCTypeConstructor::List, args) if args.len() == 1 => {
                                Ok(SExprTE::List(TypedListExpr {
                                    typ: TCType::list(args[0].clone()),
                                    kind: TypedListExprKind::LIndexList(
                                        Box::new(typed_list),
                                        Box::new(idx_int),
                                    ),
                                }))
                            }
                            TCType::Construct(constructor, args) => {
                                errs.push(SemanticError::TypeError(format!(
                                    "Unsupported type constructor application for list index element type: {:?}<{:?}>",
                                    constructor, args
                                )));
                                Err(())
                            }
                        }
                    }
                    other => {
                        errs.push(SemanticError::TypeError(format!(
                            "LIndex requires a List, got {:?}",
                            other
                        )));
                        Err(())
                    }
                }
            }
            SExpr::LAppend(list_expr, elem_expr) => {
                let list_te = list_expr.type_check_raw(expected, ctx, errs)?;

                match list_te {
                    SExprTE::List(typed_list) => {
                        let elem_type = typed_list.element_type().clone();
                        // When the list is EmptyList (empty), we cannot constrain
                        // the element; let it infer its own type instead.
                        let elem_expected = elem_type.to_stream_type();
                        let elem_te =
                            elem_expr.type_check_raw(elem_expected.as_ref(), ctx, errs)?;
                        let actual_elem_type = extract_type(&elem_te);
                        match unify_element_types(&elem_type, &actual_elem_type) {
                            Some(resolved) => {
                                let typed_list = coerce_empty_list(typed_list, &resolved);
                                Ok(SExprTE::List(typed_list.typed_append(Box::new(elem_te))))
                            }
                            None => {
                                errs.push(SemanticError::TypeError(format!(
                                    "LAppend element type mismatch: list has element type {:?}, but got {:?}",
                                    elem_type, actual_elem_type
                                )));
                                Err(())
                            }
                        }
                    }
                    other => {
                        errs.push(SemanticError::TypeError(format!(
                            "LAppend requires a List as first argument, got {:?}",
                            other
                        )));
                        Err(())
                    }
                }
            }
            SExpr::LConcat(list1_expr, list2_expr) => {
                let list1_te = list1_expr.type_check_raw(expected, ctx, errs)?;
                let list2_te = list2_expr.type_check_raw(expected, ctx, errs)?;

                match (list1_te, list2_te) {
                    (SExprTE::List(tl1), SExprTE::List(tl2)) => {
                        let t1 = tl1.element_type().clone();
                        let t2 = tl2.element_type().clone();
                        match unify_element_types(&t1, &t2) {
                            Some(resolved) => {
                                let tl1 = coerce_empty_list(tl1, &resolved);
                                let tl2 = coerce_empty_list(tl2, &resolved);
                                Ok(SExprTE::List(tl1.typed_concat(&tl2)))
                            }
                            None => {
                                errs.push(SemanticError::TypeError(format!(
                                    "LConcat requires lists of the same type, got {:?} and {:?}",
                                    t1, t2
                                )));
                                Err(())
                            }
                        }
                    }
                    (other1, other2) => {
                        errs.push(SemanticError::TypeError(format!(
                            "LConcat requires two Lists, got {:?} and {:?}",
                            other1, other2
                        )));
                        Err(())
                    }
                }
            }
            SExpr::LHead(list_expr) => {
                // If we expect type T, then the list should be List<T>
                let list_expected = expected.map(|t| StreamType::List(Box::new(t.clone())));
                let list_te = list_expr.type_check_raw(list_expected.as_ref(), ctx, errs)?;
                match list_te {
                    SExprTE::List(typed_list) => {
                        let elem_type = typed_list.element_type().clone();
                        match elem_type {
                            TCType::Int => Ok(SExprTE::Int(SExprInt::LHeadList(typed_list))),
                            TCType::Float => Ok(SExprTE::Float(SExprFloat::LHeadList(typed_list))),
                            TCType::Bool => Ok(SExprTE::Bool(SExprBool::LHeadList(typed_list))),
                            TCType::Str => Ok(SExprTE::Str(SExprStr::LHeadList(typed_list))),
                            TCType::Unit => Ok(SExprTE::Unit(SExprUnit::LHeadList(typed_list))),
                            TCType::EmptyList => {
                                errs.push(SemanticError::TypeError(
                                    "Cannot determine element type for head of an empty list"
                                        .into(),
                                ));
                                Err(())
                            }
                            TCType::EmptyMap | TCType::Unknown => {
                                errs.push(SemanticError::TypeError(format!(
                                    "Cannot take head of list with unresolved element type {:?}",
                                    elem_type
                                )));
                                Err(())
                            }
                            TCType::Construct(TCTypeConstructor::List, args) if args.len() == 1 => {
                                Ok(SExprTE::List(TypedListExpr {
                                    typ: TCType::list(args[0].clone()),
                                    kind: TypedListExprKind::LHeadList(Box::new(typed_list)),
                                }))
                            }
                            TCType::Construct(constructor, args) => {
                                errs.push(SemanticError::TypeError(format!(
                                    "Unsupported type constructor application for list head element type: {:?}<{:?}>",
                                    constructor, args
                                )));
                                Err(())
                            }
                        }
                    }
                    other => {
                        errs.push(SemanticError::TypeError(format!(
                            "LHead requires a List, got {:?}",
                            other
                        )));
                        Err(())
                    }
                }
            }
            SExpr::LTail(list_expr) => {
                // LTail preserves the list type, so propagate expected directly
                let list_te = list_expr.type_check_raw(expected, ctx, errs)?;
                match list_te {
                    SExprTE::List(typed_list) => Ok(SExprTE::List(typed_list.typed_tail())),
                    other => {
                        errs.push(SemanticError::TypeError(format!(
                            "LTail requires a List, got {:?}",
                            other
                        )));
                        Err(())
                    }
                }
            }
            SExpr::LLen(list_expr) => {
                // LLen produces Int; the inner list can be any type
                let list_te = list_expr.type_check_raw(None, ctx, errs)?;
                match list_te {
                    SExprTE::List(typed_list) => Ok(SExprTE::Int(SExprInt::LLen(typed_list))),
                    other => {
                        errs.push(SemanticError::TypeError(format!(
                            "LLen requires a List, got {:?}",
                            other
                        )));
                        Err(())
                    }
                }
            }
            SExpr::IsDefined(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(None, ctx, errs)?;
                Ok(SExprTE::Bool(SExprBool::IsDefined(Box::new(sexpr_check))))
            }
            SExpr::When(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(None, ctx, errs)?;
                Ok(SExprTE::Bool(SExprBool::When(Box::new(sexpr_check))))
            }
            SExpr::Latch(_, _) => todo!("Implement support for typed Latch"),
            SExpr::Init(se1, se2) => {
                let se1_check = se1.type_check_raw(expected, ctx, errs);
                let se2_check = se2.type_check_raw(expected, ctx, errs);
                match (se1_check, se2_check) {
                    (Ok(SExprTE::Int(e1)), Ok(SExprTE::Int(e2))) => {
                        Ok(SExprTE::Int(SExprInt::Init(Box::new(e1), Box::new(e2))))
                    }
                    (Ok(SExprTE::Float(e1)), Ok(SExprTE::Float(e2))) => {
                        Ok(SExprTE::Float(SExprFloat::Init(Box::new(e1), Box::new(e2))))
                    }
                    (Ok(SExprTE::Str(e1)), Ok(SExprTE::Str(e2))) => {
                        Ok(SExprTE::Str(SExprStr::Init(Box::new(e1), Box::new(e2))))
                    }
                    (Ok(SExprTE::Bool(e1)), Ok(SExprTE::Bool(e2))) => {
                        Ok(SExprTE::Bool(SExprBool::Init(Box::new(e1), Box::new(e2))))
                    }
                    (Ok(SExprTE::Unit(e1)), Ok(SExprTE::Unit(e2))) => {
                        Ok(SExprTE::Unit(SExprUnit::Init(Box::new(e1), Box::new(e2))))
                    }
                    (Ok(SExprTE::List(tl1)), Ok(SExprTE::List(tl2))) => {
                        let t1 = tl1.element_type().clone();
                        let t2 = tl2.element_type().clone();
                        match unify_element_types(&t1, &t2) {
                            Some(resolved) => {
                                let tl1 = coerce_empty_list(tl1, &resolved);
                                let tl2 = coerce_empty_list(tl2, &resolved);
                                Ok(SExprTE::List(tl1.typed_init(&tl2)))
                            }
                            None => {
                                errs.push(SemanticError::TypeError(format!(
                                    "Init requires both arguments to have the same type, got List<{:?}> and List<{:?}>",
                                    t1, t2
                                )));
                                Err(())
                            }
                        }
                    }
                    (Ok(SExprTE::Map(tm1)), Ok(SExprTE::Map(tm2))) => {
                        let t1 = tm1.value_type().clone();
                        let t2 = tm2.value_type().clone();
                        match unify_element_types(&t1, &t2) {
                            Some(resolved) => {
                                let tm1 = coerce_empty_map(tm1, &resolved);
                                let tm2 = coerce_empty_map(tm2, &resolved);
                                Ok(SExprTE::Map(tm1.typed_init(&tm2)))
                            }
                            None => {
                                errs.push(SemanticError::TypeError(format!(
                                    "Init requires both arguments to have the same type, got Map<{:?}> and Map<{:?}>",
                                    t1, t2
                                )));
                                Err(())
                            }
                        }
                    }
                    (Ok(ste1), Ok(ste2)) => {
                        errs.push(SemanticError::TypeError(format!(
                            "Init requires both arguments to have the same type, got {:?} and {:?}",
                            ste1, ste2
                        )));
                        Err(())
                    }
                    _ => Err(()),
                }
            }
            SExpr::Sin(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(Some(&StreamType::Float), ctx, errs)?;
                match sexpr_check {
                    SExprTE::Float(se) => Ok(SExprTE::Float(SExprFloat::Sin(Box::new(se)))),
                    other => {
                        errs.push(SemanticError::TypeError(format!(
                            "Sin can only be applied to float expressions, got {:?}",
                            other
                        )));
                        Err(())
                    }
                }
            }
            SExpr::Cos(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(Some(&StreamType::Float), ctx, errs)?;
                match sexpr_check {
                    SExprTE::Float(se) => Ok(SExprTE::Float(SExprFloat::Cos(Box::new(se)))),
                    other => {
                        errs.push(SemanticError::TypeError(format!(
                            "Cos can only be applied to float expressions, got {:?}",
                            other
                        )));
                        Err(())
                    }
                }
            }
            SExpr::Tan(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(Some(&StreamType::Float), ctx, errs)?;
                match sexpr_check {
                    SExprTE::Float(se) => Ok(SExprTE::Float(SExprFloat::Tan(Box::new(se)))),
                    other => {
                        errs.push(SemanticError::TypeError(format!(
                            "Tan can only be applied to float expressions, got {:?}",
                            other
                        )));
                        Err(())
                    }
                }
            }
            SExpr::Abs(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(expected, ctx, errs)?;
                match sexpr_check {
                    SExprTE::Int(se) => Ok(SExprTE::Int(SExprInt::Abs(Box::new(se)))),
                    SExprTE::Float(se) => Ok(SExprTE::Float(SExprFloat::Abs(Box::new(se)))),
                    other => {
                        errs.push(SemanticError::TypeError(format!(
                            "Abs can only be applied to numeric expressions, got {:?}",
                            other
                        )));
                        Err(())
                    }
                }
            }
            SExpr::MonitoredAt(_, _) => todo!("Implement support for typed MonitoredAt"),
            SExpr::Dist(_, _) => todo!("Implement support for typed Dist"),
            SExpr::Map(entries) => type_check_map_literal(
                entries
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                expected,
                ctx,
                errs,
            ),
            SExpr::MGet(map_expr, key) => {
                let map_expected = expected.map(|t| StreamType::Map(Box::new(t.clone())));
                let map_te = map_expr.type_check_raw(map_expected.as_ref(), ctx, errs)?;
                match map_te {
                    SExprTE::Map(typed_map) => typed_map_get(typed_map, key.clone(), errs),
                    other => {
                        errs.push(SemanticError::TypeError(format!(
                            "MGet requires a Map, got {:?}",
                            other
                        )));
                        Err(())
                    }
                }
            }
            SExpr::MInsert(map_expr, key, value_expr) => {
                let map_te = map_expr.type_check_raw(expected, ctx, errs)?;
                match map_te {
                    SExprTE::Map(typed_map) => {
                        let value_type = typed_map.value_type().clone();
                        let value_expected = value_type.to_stream_type();
                        let value_te =
                            value_expr.type_check_raw(value_expected.as_ref(), ctx, errs)?;
                        let actual_value_type = extract_type(&value_te);
                        match unify_element_types(&value_type, &actual_value_type) {
                            Some(resolved) => {
                                let typed_map = coerce_empty_map(typed_map, &resolved);
                                Ok(SExprTE::Map(
                                    typed_map.typed_insert(key.clone(), Box::new(value_te)),
                                ))
                            }
                            None => {
                                errs.push(SemanticError::TypeError(format!(
                                    "MInsert value type mismatch: map has value type {:?}, but got {:?}",
                                    value_type, actual_value_type
                                )));
                                Err(())
                            }
                        }
                    }
                    other => {
                        errs.push(SemanticError::TypeError(format!(
                            "MInsert requires a Map as first argument, got {:?}",
                            other
                        )));
                        Err(())
                    }
                }
            }
            SExpr::MRemove(map_expr, key) => {
                let map_te = map_expr.type_check_raw(expected, ctx, errs)?;
                match map_te {
                    SExprTE::Map(typed_map) => {
                        Ok(SExprTE::Map(typed_map.typed_remove(key.clone())))
                    }
                    other => {
                        errs.push(SemanticError::TypeError(format!(
                            "MRemove requires a Map, got {:?}",
                            other
                        )));
                        Err(())
                    }
                }
            }
            SExpr::MHasKey(map_expr, key) => {
                let map_te = map_expr.type_check_raw(None, ctx, errs)?;
                match map_te {
                    SExprTE::Map(typed_map) => {
                        Ok(SExprTE::Bool(SExprBool::MHasKeyMap(typed_map, key.clone())))
                    }
                    other => {
                        errs.push(SemanticError::TypeError(format!(
                            "MHasKey requires a Map, got {:?}",
                            other
                        )));
                        Err(())
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{iter::zip, mem::discriminant};

    use crate::lang::dsrv::ast::{NumericalBinOp, StrBinOp};

    use super::{SemanticResult, TypeInfo};

    use super::*;
    use ecow::eco_vec;
    use proptest::prelude::*;
    use std::collections::BTreeMap;
    use test_log::test;

    type SExprV = SExpr;
    type SemantResultStr = SemanticResult<SExprTE>;

    trait BinOpExpr<Expr> {
        fn binop_expr(lhs: Expr, rhs: Expr, op: SBinOp) -> Self;
    }

    trait IfExpr<Expr, BoolExpr> {
        fn if_expr(b: BoolExpr, t: Expr, f: Expr) -> Self;
    }

    impl BinOpExpr<Box<SExpr>> for SExpr {
        fn binop_expr(lhs: Box<SExpr>, rhs: Box<SExpr>, op: SBinOp) -> Self {
            SExpr::BinOp(lhs, rhs, op)
        }
    }

    impl BinOpExpr<Box<SExprInt>> for SExprInt {
        fn binop_expr(lhs: Box<SExprInt>, rhs: Box<SExprInt>, op: SBinOp) -> Self {
            match op {
                SBinOp::NOp(op) => SExprInt::BinOp(lhs, rhs, op.try_into().unwrap()),
                _ => panic!("Invalid operation for SExprInt: {:?}", op),
            }
        }
    }

    impl IfExpr<Box<SExpr>, Box<SExpr>> for SExpr {
        fn if_expr(b: Box<SExpr>, t: Box<SExpr>, f: Box<SExpr>) -> Self {
            SExpr::If(b, t, f)
        }
    }

    impl IfExpr<Box<SExprInt>, Box<SExprBool>> for SExprInt {
        fn if_expr(b: Box<SExprBool>, t: Box<SExprInt>, f: Box<SExprInt>) -> Self {
            SExprInt::If(b, t, f)
        }
    }

    fn check_correct_error_type(result: &SemantResultStr, expected: &SemantResultStr) {
        // Checking that error type is correct but not the specific message
        if let (Err(res_errs), Err(exp_errs)) = (&result, &expected) {
            assert_eq!(res_errs.len(), exp_errs.len());
            let mut errs = zip(res_errs, exp_errs);
            assert!(
                errs.all(|(res, exp)| discriminant(res) == discriminant(exp)),
                "Error variants do not match: got {:?}, expected {:?}",
                res_errs,
                exp_errs
            );
        } else {
            // We didn't receive error - make assertion fail with nice output
            let msg = format!(
                "Expected error: {:?}. Received result: {:?}",
                expected, result
            );
            assert!(false, "{}", msg);
        }
    }

    fn check_correct_error_types(results: &Vec<SemantResultStr>, expected: &Vec<SemantResultStr>) {
        assert_eq!(
            results.len(),
            expected.len(),
            "Result and expected vectors must have the same length"
        );

        // Iterate over both vectors and call check_correct_error_type on each pair
        for (result, exp) in results.iter().zip(expected.iter()) {
            check_correct_error_type(result, exp);
        }
    }

    // // Helper function that returns all the sbinop variants at the time of writing these tests
    // // (Not guaranteed to be maintained)
    fn all_sbinop_variants() -> Vec<SBinOp> {
        vec![
            SBinOp::NOp(NumericalBinOp::Add),
            SBinOp::NOp(NumericalBinOp::Sub),
            SBinOp::NOp(NumericalBinOp::Mul),
            SBinOp::NOp(NumericalBinOp::Div),
        ]
    }

    // Function to generate combinations to use in tests, e.g., for binops
    fn generate_combinations<T, Expr, F>(
        variants_a: &[Expr],
        variants_b: &[Expr],
        generate_expr: F,
    ) -> Vec<T>
    where
        // T: AsExpr<Box<Expr>>,
        Expr: Clone,
        F: Fn(Box<Expr>, Box<Expr>) -> T,
    {
        let mut vals = Vec::new();

        for a in variants_a.iter() {
            for b in variants_b.iter() {
                vals.push(generate_expr(Box::new(a.clone()), Box::new(b.clone())));
            }
        }

        vals
    }

    // Example usage for binary operations
    fn generate_binop_combinations<T, Expr>(
        variants_a: &[Expr],
        variants_b: &[Expr],
        sbinops: Vec<SBinOp>,
    ) -> Vec<T>
    where
        T: BinOpExpr<Box<Expr>>,
        Expr: Clone,
    {
        let mut vals = Vec::new();

        for op in sbinops {
            vals.extend(generate_combinations(variants_a, variants_b, |lhs, rhs| {
                T::binop_expr(lhs, rhs, op.clone())
            }));
        }

        vals
    }

    fn generate_concat_combinations(
        variants_a: &[SExprStr],
        variants_b: &[SExprStr],
    ) -> Vec<SExprStr> {
        generate_combinations(variants_a, variants_b, |lhs, rhs| {
            SExprStr::BinOp(Box::new(*lhs), Box::new(*rhs), StrBinOp::Concat)
        })
    }

    // // Example usage for if-expressions
    fn generate_if_combinations<T, Expr, BoolExpr: Clone>(
        variants_a: &[Expr],
        variants_b: &[Expr],
        b_expr: Box<BoolExpr>,
    ) -> Vec<T>
    where
        T: IfExpr<Box<Expr>, Box<BoolExpr>>,
        Expr: Clone,
    {
        generate_combinations(variants_a, variants_b, |lhs, rhs| {
            T::if_expr(b_expr.clone(), lhs, rhs)
        })
    }

    #[test]
    fn test_vals_ok() {
        // Checks that vals returns the expected typed AST after semantic analysis
        let vals: Vec<(SExprV, TCType)> = vec![
            (SExprV::Val(Value::Int(1)), TCType::Int),
            (SExprV::Val(Value::Str("".into())), TCType::Str),
            (SExprV::Val(Value::Bool(true)), TCType::Bool),
            (SExprV::Val(Value::Unit), TCType::Unit),
        ];
        let results = vals
            .iter()
            .map(|(v, _t)| v.type_check(&mut TypeInfo::new()));
        let expected: Vec<SemantResultStr> = vec![
            Ok(SExprTE::Int(SExprInt::Val(PartialStreamValue::Known(1)))),
            Ok(SExprTE::Str(SExprStr::Val(PartialStreamValue::Known(
                "".into(),
            )))),
            Ok(SExprTE::Bool(SExprBool::Val(PartialStreamValue::Known(
                true,
            )))),
            Ok(SExprTE::Unit(SExprUnit::Val(PartialStreamValue::Known(())))),
        ];

        assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_deferred_err() {
        // Checks that if a Val is deferred during semantic analysis it produces a DeferredError
        let val = SExprV::Val(Value::Deferred);
        let result = val.type_check_with_default();
        let expected: SemantResultStr = Err(vec![SemanticError::DeferredError("".into())]);
        check_correct_error_type(&result, &expected);
    }

    #[test]
    fn test_plus_err_ident_types() {
        // Checks that if we add two identical types together that are not addable,
        let vals = [
            SExprV::BinOp(
                Box::new(SExprV::Val(Value::Bool(false))),
                Box::new(SExprV::Val(Value::Bool(false))),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
            SExprV::BinOp(
                Box::new(SExprV::Val(Value::Unit)),
                Box::new(SExprV::Val(Value::Unit)),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
        ];
        let results = vals
            .iter()
            .map(TypeCheckable::type_check_with_default)
            .collect();
        let expected: Vec<SemantResultStr> = vec![
            Err(vec![SemanticError::TypeError("".into())]),
            Err(vec![SemanticError::TypeError("".into())]),
        ];
        check_correct_error_types(&results, &expected);
    }

    #[test]
    fn test_binop_err_diff_types() {
        // Checks that calling a BinOp on two different types results in a TypeError

        // Create a vector of all ConcreteStreamData variants (except Deferred)
        let variants = vec![
            SExprV::Val(Value::Int(0)),
            SExprV::Val(Value::Str("".into())),
            SExprV::Val(Value::Bool(true)),
            SExprV::Val(Value::Unit),
        ];

        // Create a vector of all SBinOp variants
        let sbinops = all_sbinop_variants();

        let vals_tmp = generate_binop_combinations(&variants, &variants, sbinops);
        let vals = vals_tmp.into_iter().filter(|bin_op| {
            match bin_op {
                SExprV::BinOp(left, right, _) => {
                    // Only keep values where left != right
                    left != right
                }
                _ => true, // Keep non-BinOps (unused in this case)
            }
        });

        let results = vals
            .map(|x| TypeCheckable::type_check_with_default(&x))
            .collect::<Vec<_>>();

        // Since all combinations of different types should yield an error,
        // we'll expect each result to be an Err with a type error.
        let expected: Vec<SemantResultStr> = results
            .iter()
            .map(|_| Err(vec![SemanticError::TypeError("".into())]))
            .collect();

        check_correct_error_types(&results, &expected);
    }

    #[test]
    fn test_plus_err_deferred() {
        // Checks that if either value is deferred then Plus does not generate further errors
        let vals = [
            SExprV::BinOp(
                Box::new(SExprV::Val(Value::Int(0))),
                Box::new(SExprV::Val(Value::Deferred)),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
            SExprV::BinOp(
                Box::new(SExprV::Val(Value::Deferred)),
                Box::new(SExprV::Val(Value::Int(0))),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
            SExprV::BinOp(
                Box::new(SExprV::Val(Value::Deferred)),
                Box::new(SExprV::Val(Value::Deferred)),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
        ];
        let results = vals.iter().map(TypeCheckable::type_check_with_default);
        let expected_err_lens = vec![1, 1, 2];

        // For each result, check that we got errors and that we got the correct amount:
        for (res, exp_err_len) in zip(results, expected_err_lens) {
            match res {
                Err(errs) => {
                    assert_eq!(
                        errs.len(),
                        exp_err_len,
                        "Expected {} errors but got {}: {:?}",
                        exp_err_len,
                        errs.len(),
                        errs
                    );
                    // TODO: Check that it is actually DeferredErrors
                }
                Ok(_) => {
                    assert!(
                        false,
                        "Expected an error but got a successful result: {:?}",
                        res
                    );
                }
            }
        }
    }

    #[test]
    fn test_int_binop_ok() {
        // Checks that if we BinOp two Ints together it results in typed AST after semantic analysis
        let int_val = vec![SExprV::Val(Value::Int(0))];
        let sbinops = all_sbinop_variants();
        let vals: Vec<SExpr> = generate_binop_combinations(&int_val, &int_val, sbinops.clone());
        let results = vals.iter().map(TypeCheckable::type_check_with_default);

        let int_t_val = vec![SExprInt::Val(PartialStreamValue::Known(0))];

        // Generate the different combinations and turn them into "Ok" results
        let expected_tmp: Vec<SExprInt> =
            generate_binop_combinations(&int_t_val, &int_t_val, sbinops);
        let expected = expected_tmp.into_iter().map(|v| Ok(SExprTE::Int(v)));
        assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_str_plus_ok() {
        // Checks that if we add two Strings together it results in typed AST after semantic analysis
        let str_val = vec![SExprV::Val(Value::Str("".into()))];
        let sbinops = vec![SBinOp::SOp(StrBinOp::Concat)];
        let vals: Vec<SExpr> = generate_binop_combinations(&str_val, &str_val, sbinops.clone());
        let results = vals.iter().map(TypeCheckable::type_check_with_default);

        let str_t_val = vec![SExprStr::Val(PartialStreamValue::Known("".into()))];

        // Generate the different combinations and turn them into "Ok" results
        let expected_tmp: Vec<SExprStr> = generate_concat_combinations(&str_t_val, &str_t_val);
        let expected = expected_tmp.into_iter().map(|v| Ok(SExprTE::Str(v)));
        assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_if_ok() {
        // Checks that typechecking if-statements with identical types for if- and else- part results in correct typed AST

        // Create a vector of all ConcreteStreamData variants (except Deferred)
        let val_variants = vec![
            SExprV::Val(Value::Int(0)),
            SExprV::Val(Value::Str("".into())),
            SExprV::Val(Value::Bool(true)),
            SExprV::Val(Value::Unit),
        ];

        // Create a vector of all SBinOp variants
        let bexpr = Box::new(SExpr::Val(true.into()));
        let bexpr_checked = Box::new(SExprBool::Val(PartialStreamValue::Known(true)));

        let vals_tmp = generate_if_combinations(&val_variants, &val_variants, bexpr.clone());

        // Only consider cases where true and false cases are equal
        let vals = vals_tmp.into_iter().filter(|bin_op| {
            match bin_op {
                SExprV::If(_, t, f) => t == f,
                _ => true, // Keep non-ifs (unused in this case)
            }
        });
        let results = vals.map(|x| x.type_check_with_default());

        let expected: Vec<SemantResultStr> = vec![
            Ok(SExprTE::Int(SExprInt::If(
                bexpr_checked.clone(),
                Box::new(SExprInt::Val(PartialStreamValue::Known(0))),
                Box::new(SExprInt::Val(PartialStreamValue::Known(0))),
            ))),
            Ok(SExprTE::Str(SExprStr::If(
                bexpr_checked.clone(),
                Box::new(SExprStr::Val(PartialStreamValue::Known("".into()))),
                Box::new(SExprStr::Val(PartialStreamValue::Known("".into()))),
            ))),
            Ok(SExprTE::Bool(SExprBool::If(
                bexpr_checked.clone(),
                Box::new(SExprBool::Val(PartialStreamValue::Known(true))),
                Box::new(SExprBool::Val(PartialStreamValue::Known(true))),
            ))),
            Ok(SExprTE::Unit(SExprUnit::If(
                bexpr_checked.clone(),
                Box::new(SExprUnit::Val(PartialStreamValue::Known(()))),
                Box::new(SExprUnit::Val(PartialStreamValue::Known(()))),
            ))),
        ];

        assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_if_err() {
        // Checks that creating an if-expression with two different types results in a TypeError

        // Create a vector of all ConcreteStreamData variants (except Deferred)
        let variants = vec![
            SExprV::Val(Value::Int(0)),
            SExprV::Val(Value::Str("".into())),
            SExprV::Val(Value::Bool(true)),
            SExprV::Val(Value::Unit),
        ];

        let bexpr = Box::new(SExpr::Val(true.into()));

        let vals_tmp = generate_if_combinations(&variants, &variants, bexpr.clone());
        let vals = vals_tmp.into_iter().filter(|bin_op| {
            match bin_op {
                SExprV::If(_, t, f) => t != f,
                _ => true, // Keep non-BinOps (unused in this case)
            }
        });

        let results = vals
            .map(|x| x.type_check_with_default())
            .collect::<Vec<_>>();

        // Since all combinations of different types should yield an error,
        // we'll expect each result to be an Err with a type error.
        let expected: Vec<SemantResultStr> = results
            .iter()
            .map(|_| Err(vec![SemanticError::TypeError("".into())]))
            .collect();

        check_correct_error_types(&results, &expected);
    }

    #[test]
    fn test_var_ok() {
        // Checks that Vars are correctly typechecked if they exist in the context

        let variant_names = vec!["int", "str", "bool", "unit"];
        let variant_types = vec![
            StreamType::Int,
            StreamType::Str,
            StreamType::Bool,
            StreamType::Unit,
        ];
        let vals = variant_names
            .clone()
            .into_iter()
            .map(|n| SExprV::Var(n.into()));

        // Fake context/environment that simulates type-checking context
        let mut ctx = TypeInfo::new();
        for (n, t) in variant_names.into_iter().zip(variant_types.into_iter()) {
            ctx.insert(n.into(), t);
        }

        let results = vals.into_iter().map(|sexpr| sexpr.type_check(&mut ctx));

        let expected = vec![
            Ok(SExprTE::Int(SExprInt::Var("int".into()))),
            Ok(SExprTE::Str(SExprStr::Var("str".into()))),
            Ok(SExprTE::Bool(SExprBool::Var("bool".into()))),
            Ok(SExprTE::Unit(SExprUnit::Var("unit".into()))),
        ];

        assert!(results.eq(expected));
    }

    #[test]
    fn test_var_err() {
        // Checks that Vars produce UndeclaredVariable errors if they do not exist in the context

        let val = SExprV::Var("undeclared_name".into());
        let result = val.type_check_with_default();
        let expected: SemantResultStr = Err(vec![SemanticError::UndeclaredVariable("".into())]);
        check_correct_error_type(&result, &expected);
    }
    // TODO: Test that any SExpr leaf is a Val. If not it should return a Type-Error

    #[test]
    fn test_dodgy_if() {
        let dodgy_bexpr = SExpr::BinOp(
            Box::new(SExprV::Val(Value::Int(0))),
            Box::new(SExprV::BinOp(
                Box::new(SExprV::Val(Value::Int(3))),
                Box::new(SExprV::Val(Value::Str("Banana".into()))),
                SBinOp::NOp(NumericalBinOp::Add),
            )),
            SBinOp::COp(CompBinOp::Eq),
        );
        let sexpr = SExprV::If(
            Box::new(dodgy_bexpr),
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Val(Value::Int(2))),
        );
        if let Ok(_) = sexpr.type_check_with_default() {
            assert!(false, "Expected type error but got a successful result");
        }
    }

    #[test]
    fn test_defer_nested_int_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Defer(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Int),
                eco_vec!["x".into()],
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        let mut expected_ctx = TypeInfo::new();
        expected_ctx.insert("x".into(), StreamType::Str);
        let expected = Ok(SExprTE::Int(SExprInt::BinOp(
            Box::new(SExprInt::Val(PartialStreamValue::Known(1))),
            Box::new(SExprInt::Defer(
                Box::new(SExprStr::Var("x".into())),
                expected_ctx.clone(),
                eco_vec!["x".into()],
            )),
            IntBinOp::Add,
        )));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_defer_nested_int_ascription_no_eval() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Defer(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Int),
                eco_vec!["x".into()],
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        let mut expected_ctx = TypeInfo::new();
        expected_ctx.insert("x".into(), StreamType::Str);
        let expected = Ok(SExprTE::Int(SExprInt::BinOp(
            Box::new(SExprInt::Val(PartialStreamValue::Known(1))),
            Box::new(SExprInt::Defer(
                Box::new(SExprStr::Var("x".into())),
                expected_ctx.clone(),
                eco_vec!["x".into()],
            )),
            IntBinOp::Add,
        )));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_defer_nested_int_ascription_incorrect_inner_type() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Defer(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Int),
                eco_vec!["x".into()],
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Bool);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_defer_nested_bool_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Bool(true))),
            Box::new(SExprV::Defer(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Bool),
                eco_vec!["x".into()],
            )),
            SBinOp::BOp(BoolBinOp::And),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        let mut expected_ctx = TypeInfo::new();
        expected_ctx.insert("x".into(), StreamType::Str);
        let expected = Ok(SExprTE::Bool(SExprBool::BinOp(
            Box::new(SExprBool::Val(PartialStreamValue::Known(true))),
            Box::new(SExprBool::Defer(
                Box::new(SExprStr::Var("x".into())),
                expected_ctx.clone(),
                eco_vec!["x".into()],
            )),
            BoolBinOp::And,
        )));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_defer_nested_int_bool_false_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Defer(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Bool),
                eco_vec!["x".into()],
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_defer_nested_int_missing_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Defer(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Unascribed,
                eco_vec!["x".into()],
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_dynamic_nested_int_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Dynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Int),
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        let mut expected_ctx = TypeInfo::new();
        expected_ctx.insert("x".into(), StreamType::Str);
        let expected = Ok(SExprTE::Int(SExprInt::BinOp(
            Box::new(SExprInt::Val(PartialStreamValue::Known(1))),
            Box::new(SExprInt::Dynamic(
                Box::new(SExprStr::Var("x".into())),
                expected_ctx.clone(),
            )),
            IntBinOp::Add,
        )));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_dynamic_nested_bool_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Bool(true))),
            Box::new(SExprV::Dynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Bool),
            )),
            SBinOp::BOp(BoolBinOp::And),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        let mut expected_ctx = TypeInfo::new();
        expected_ctx.insert("x".into(), StreamType::Str);
        let expected = Ok(SExprTE::Bool(SExprBool::BinOp(
            Box::new(SExprBool::Val(PartialStreamValue::Known(true))),
            Box::new(SExprBool::Dynamic(
                Box::new(SExprStr::Var("x".into())),
                expected_ctx.clone(),
            )),
            BoolBinOp::And,
        )));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_dynamic_nested_int_bool_false_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Dynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Bool),
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Int);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_dynamic_nested_int_unascribed() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Dynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Unascribed,
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Int);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_restricted_dynamic_nested_str_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Str("hello".into()))),
            Box::new(SExprV::RestrictedDynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Str),
                EcoVec::from(vec!["x".into(), "y".into()]),
            )),
            SBinOp::SOp(StrBinOp::Concat),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        let mut expected_ctx = TypeInfo::new();
        expected_ctx.insert("x".into(), StreamType::Str);
        let expected = Ok(SExprTE::Str(SExprStr::BinOp(
            Box::new(SExprStr::Val(PartialStreamValue::Known("hello".into()))),
            Box::new(SExprStr::RestrictedDynamic(
                Box::new(SExprStr::Var("x".into())),
                EcoVec::from(vec!["x".into(), "y".into()]),
                expected_ctx.clone(),
            )),
            StrBinOp::Concat,
        )));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_restricted_dynamic_nested_int_ascription_error() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::RestrictedDynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Bool),
                EcoVec::from(vec!["x".into(), "y".into()]),
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_restricted_dynamic_nested_str_bool_false_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Str("test".into()))),
            Box::new(SExprV::RestrictedDynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Bool),
                EcoVec::from(vec!["x".into(), "y".into()]),
            )),
            SBinOp::SOp(StrBinOp::Concat),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        // Type ascription mismatch should cause error
        assert!(result.is_err());
    }

    #[test]
    fn test_restricted_dynamic_nested_str_unascribed() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Str("hello".into()))),
            Box::new(SExprV::RestrictedDynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Unascribed,
                EcoVec::from(vec!["x".into(), "y".into()]),
            )),
            SBinOp::SOp(StrBinOp::Concat),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_typed_spec_display_output() {
        let input_vars = BTreeSet::from(["x".into(), "y".into()]);
        let output_vars = BTreeSet::from(["z".into(), "d".into()]);

        let mut dynamic_ctx = BTreeMap::new();
        dynamic_ctx.insert("s".into(), StreamType::Str);

        let exprs = BTreeMap::from([
            (
                "z".into(),
                SExprTE::Int(SExprInt::BinOp(
                    Box::new(SExprInt::Var("x".into())),
                    Box::new(SExprInt::Var("y".into())),
                    IntBinOp::Add,
                )),
            ),
            (
                "d".into(),
                SExprTE::Int(SExprInt::Dynamic(
                    Box::new(SExprStr::Var("s".into())),
                    dynamic_ctx,
                )),
            ),
        ]);

        let type_annotations = BTreeMap::from([
            ("x".into(), StreamType::Int),
            ("y".into(), StreamType::Int),
            ("s".into(), StreamType::Str),
            ("z".into(), StreamType::Int),
            ("d".into(), StreamType::Int),
        ]);

        let spec = TypedDsrvSpecification {
            input_vars,
            output_vars,
            aux_info: BTreeSet::new(),
            exprs,
            type_annotations,
        };

        let rendered = format!("{}", spec);
        let lines: Vec<&str> = rendered.lines().collect();

        assert!(lines.contains(&"in x: Int"));
        assert!(lines.contains(&"in y: Int"));
        assert!(lines.contains(&"out z: Int"));
        assert!(lines.contains(&"out d: Int"));
        assert!(lines.contains(&"z = (x + y)"));
        assert!(lines.contains(&"d = dynamic(s: Int)"));
    }

    fn arb_typed_roundtrip_spec() -> impl Strategy<Value = TypedDsrvSpecification> {
        let fixed_inputs: BTreeSet<VarName> = BTreeSet::from(["a".into(), "b".into(), "s".into()]);

        let all_vars: Vec<VarName> =
            vec!["a".into(), "b".into(), "s".into(), "x".into(), "y".into()];

        (
            crate::lang::dsrv::ast::generation::arb_int_sexpr(all_vars.clone()),
            crate::lang::dsrv::ast::generation::arb_int_sexpr(all_vars.clone()),
            "[a-z]{1,3}",
        )
            .prop_filter_map(
                "generated expressions must typecheck in fixed context",
                move |(expr_x, expr_y, env_name)| {
                    let mut ctx = TypeInfo::new();
                    ctx.insert("a".into(), StreamType::Int);
                    ctx.insert("b".into(), StreamType::Int);
                    ctx.insert("s".into(), StreamType::Str);
                    ctx.insert("x".into(), StreamType::Int);
                    ctx.insert("y".into(), StreamType::Int);

                    let typed_x = expr_x.type_check(&mut ctx).ok()?;
                    let typed_y = expr_y.type_check(&mut ctx).ok()?;

                    let typed_x = match typed_x {
                        SExprTE::Int(e) => SExprTE::Int(e),
                        _ => return None,
                    };
                    let typed_y = match typed_y {
                        SExprTE::Int(e) => SExprTE::Int(e),
                        _ => return None,
                    };

                    let env_var: VarName = env_name.as_str().into();
                    let typed_dyn = SExprTE::Int(SExprInt::RestrictedDynamic(
                        Box::new(SExprStr::Var("s".into())),
                        eco_vec![env_var],
                        BTreeMap::new(),
                    ));

                    Some(TypedDsrvSpecification {
                        input_vars: fixed_inputs.clone(),
                        output_vars: BTreeSet::from(["x".into(), "y".into(), "d".into()]),
                        aux_info: BTreeSet::new(),
                        exprs: BTreeMap::from([
                            ("x".into(), typed_x),
                            ("y".into(), typed_y),
                            ("d".into(), typed_dyn),
                        ]),
                        type_annotations: BTreeMap::from([
                            ("a".into(), StreamType::Int),
                            ("b".into(), StreamType::Int),
                            ("s".into(), StreamType::Str),
                            ("x".into(), StreamType::Int),
                            ("y".into(), StreamType::Int),
                            ("d".into(), StreamType::Int),
                        ]),
                    })
                },
            )
    }

    proptest! {
        #[test]
        fn test_prop_typed_spec_display_parse_roundtrip(spec in arb_typed_roundtrip_spec()) {
            let rendered = format!("{}", spec);
            let mut input = rendered.as_str();
            let parsed = crate::lang::dsrv::parser::dsrv_specification(&mut input)
                .expect("Typed Display output should parse as a DsrvSpecification");

            prop_assert_eq!(parsed.input_vars, spec.input_vars);
            prop_assert_eq!(parsed.output_vars, spec.output_vars);
            prop_assert_eq!(parsed.type_annotations, spec.type_annotations);
            prop_assert_eq!(parsed.exprs.len(), spec.exprs.len());
        }
    }

    // -----------------------------------------------------------------------
    // List type-checking tests
    // -----------------------------------------------------------------------

    // --- Value::List literal type determination ---

    #[test]
    fn test_list_val_flat_int() {
        let val = Value::List(EcoVec::from(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
        ]));
        let expr = SExprV::Val(val);
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        let te = result.unwrap();
        assert_eq!(extract_type(&te), TCType::list(TCType::Int));
    }

    #[test]
    fn test_list_val_flat_bool() {
        let val = Value::List(EcoVec::from(vec![Value::Bool(true), Value::Bool(false)]));
        let expr = SExprV::Val(val);
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        let te = result.unwrap();
        assert_eq!(extract_type(&te), TCType::list(TCType::Bool));
    }

    #[test]
    fn test_list_val_flat_str() {
        let val = Value::List(EcoVec::from(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
        ]));
        let expr = SExprV::Val(val);
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        let te = result.unwrap();
        assert_eq!(extract_type(&te), TCType::list(TCType::Str));
    }

    #[test]
    fn test_list_val_nested_list_of_list_int() {
        // [[1, 2], [3]]
        let val = Value::List(EcoVec::from(vec![
            Value::List(EcoVec::from(vec![Value::Int(1), Value::Int(2)])),
            Value::List(EcoVec::from(vec![Value::Int(3)])),
        ]));
        let expr = SExprV::Val(val);
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        let te = result.unwrap();
        assert_eq!(extract_type(&te), TCType::list(TCType::list(TCType::Int)));
    }

    #[test]
    fn test_list_val_triple_nested() {
        // [[[true]]]
        let val = Value::List(EcoVec::from(vec![Value::List(EcoVec::from(vec![
            Value::List(EcoVec::from(vec![Value::Bool(true)])),
        ]))]));
        let expr = SExprV::Val(val);
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        let te = result.unwrap();
        assert_eq!(
            extract_type(&te),
            TCType::list(TCType::list(TCType::list(TCType::Bool)))
        );
    }

    #[test]
    fn test_list_val_empty_without_expected() {
        // Without expected type, empty list literal succeeds with empty-list element type
        let val = Value::List(EcoVec::new());
        let expr = SExprV::Val(val);
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_ok(),
            "Expected Ok for empty list literal with empty-list element type, got {:?}",
            result
        );
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::EmptyList)
        );
    }

    #[test]
    fn test_list_val_empty_ok_with_expected() {
        // With expected type, empty list literal succeeds
        let val = Value::List(EcoVec::new());
        let expected = StreamType::List(Box::new(StreamType::Int));
        let mut errs = vec![];
        let result = val.type_check_raw(Some(&expected), &mut TypeInfo::new(), &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for empty list with expected type, got {:?}",
            errs
        );
        let te = result.unwrap();
        assert_eq!(extract_type(&te), TCType::list(TCType::Int));
    }

    #[test]
    fn test_list_val_empty_nested_ok_with_expected() {
        // [[]] with expected List<List<Int>> should succeed
        let val = Value::List(EcoVec::from(vec![Value::List(EcoVec::new())]));
        let expected = StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int))));
        let mut errs = vec![];
        let result = val.type_check_raw(Some(&expected), &mut TypeInfo::new(), &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for [[]] with expected List<List<Int>>, got {:?}",
            errs
        );
        let te = result.unwrap();
        assert_eq!(extract_type(&te), TCType::list(TCType::list(TCType::Int)));
    }

    #[test]
    fn test_list_val_nested_inner_empty_without_expected() {
        // [[]] — without expected type, the inner empty list gets empty-list element type.
        // The outer list's first element is List(Poly), so the outer list is
        // List(List(Poly)).
        let val = Value::List(EcoVec::from(vec![Value::List(EcoVec::new())]));
        let expr = SExprV::Val(val);
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_ok(),
            "Expected Ok for [[]] with empty-list element type, got {:?}",
            result
        );
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::list(TCType::EmptyList))
        );
    }

    #[test]
    fn test_list_val_mixed_element_types_error() {
        // [1, "hello"] — first element is Int, second is Str
        let val = Value::List(EcoVec::from(vec![
            Value::Int(1),
            Value::Str("hello".into()),
        ]));
        let expr = SExprV::Val(val);
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err(), "Expected Err for mixed element types");
    }

    #[test]
    fn test_list_val_nested_mixed_depth_error() {
        // [[1, 2], 3] — first element is List<Int>, second is Int
        let val = Value::List(EcoVec::from(vec![
            Value::List(EcoVec::from(vec![Value::Int(1), Value::Int(2)])),
            Value::Int(3),
        ]));
        let expr = SExprV::Val(val);
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err(), "Expected Err for mixed nesting depths");
    }

    #[test]
    fn test_list_val_nested_inconsistent_inner_type_error() {
        // [[1], [true]] — first inner is List<Int>, second is List<Bool>
        let val = Value::List(EcoVec::from(vec![
            Value::List(EcoVec::from(vec![Value::Int(1)])),
            Value::List(EcoVec::from(vec![Value::Bool(true)])),
        ]));
        let expr = SExprV::Val(val);
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_err(),
            "Expected Err for inconsistent nested element types"
        );
    }

    // --- SExpr::List (expression-level list literal) ---

    #[test]
    fn test_sexpr_list_flat_int() {
        let expr = SExpr::List(EcoVec::from(vec![
            SExpr::Val(Value::Int(10)),
            SExpr::Val(Value::Int(20)),
        ]));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_sexpr_list_nested() {
        // List of list-literals: [[1], [2, 3]]
        let expr = SExpr::List(EcoVec::from(vec![
            SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(1))])),
            SExpr::List(EcoVec::from(vec![
                SExpr::Val(Value::Int(2)),
                SExpr::Val(Value::Int(3)),
            ])),
        ]));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::list(TCType::Int))
        );
    }

    #[test]
    fn test_sexpr_list_empty_without_expected() {
        // Without expected type, empty SExpr::List succeeds with empty-list element type
        let expr = SExpr::List(EcoVec::new());
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_ok(),
            "Expected Ok for empty SExpr::List with empty-list element type, got {:?}",
            result
        );
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::EmptyList)
        );
    }

    #[test]
    fn test_sexpr_list_empty_ok_with_expected() {
        // With expected type, empty SExpr::List succeeds
        let expr = SExpr::List(EcoVec::new());
        let expected = StreamType::List(Box::new(StreamType::Int));
        let mut errs = vec![];
        let result = expr.type_check_raw(Some(&expected), &mut TypeInfo::new(), &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for empty SExpr::List with expected type, got {:?}",
            errs
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_sexpr_list_empty_nested_ok_with_expected() {
        // [[]] with expected List<List<Bool>> should succeed
        let expr = SExpr::List(EcoVec::from(vec![SExpr::List(EcoVec::new())]));
        let expected = StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Bool))));
        let mut errs = vec![];
        let result = expr.type_check_raw(Some(&expected), &mut TypeInfo::new(), &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for [[]] with expected List<List<Bool>>, got {:?}",
            errs
        );
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::list(TCType::Bool))
        );
    }

    #[test]
    fn test_sexpr_list_mixed_types_error() {
        let expr = SExpr::List(EcoVec::from(vec![
            SExpr::Val(Value::Int(1)),
            SExpr::Val(Value::Bool(true)),
        ]));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    // --- Empty lists in compound expressions ---

    #[test]
    fn test_default_empty_list_with_typed_list() {
        // default([], xs) where xs : List<Int> → List<Int>
        // The expected type from xs propagates to the empty list via the Default handler.
        let expr = SExpr::Default(
            Box::new(SExpr::List(EcoVec::new())),
            Box::new(SExpr::Var("xs".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Int)));
        let expected = StreamType::List(Box::new(StreamType::Int));
        let mut errs = vec![];
        let result = expr.type_check_raw(Some(&expected), &mut ctx, &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for default([], xs) with expected List<Int>, got {:?}",
            errs
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_if_empty_list_branch_with_expected() {
        // if true then [] else [1, 2] with expected List<Int>
        let expr = SExpr::If(
            Box::new(SExpr::Val(Value::Bool(true))),
            Box::new(SExpr::List(EcoVec::new())),
            Box::new(SExpr::List(EcoVec::from(vec![
                SExpr::Val(Value::Int(1)),
                SExpr::Val(Value::Int(2)),
            ]))),
        );
        let expected = StreamType::List(Box::new(StreamType::Int));
        let mut errs = vec![];
        let result = expr.type_check_raw(Some(&expected), &mut TypeInfo::new(), &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for if(true, [], [1,2]) with expected List<Int>, got {:?}",
            errs
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_init_empty_list_with_expected() {
        // init([], xs) where xs : List<Str> with expected List<Str>
        let expr = SExpr::Init(
            Box::new(SExpr::List(EcoVec::new())),
            Box::new(SExpr::Var("xs".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Str)));
        let expected = StreamType::List(Box::new(StreamType::Str));
        let mut errs = vec![];
        let result = expr.type_check_raw(Some(&expected), &mut ctx, &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for init([], xs) with expected List<Str>, got {:?}",
            errs
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Str));
    }

    #[test]
    fn test_lconcat_empty_list_with_expected() {
        // concat([], [1]) with expected List<Int>
        let expr = SExpr::LConcat(
            Box::new(SExpr::List(EcoVec::new())),
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(1))]))),
        );
        let expected = StreamType::List(Box::new(StreamType::Int));
        let mut errs = vec![];
        let result = expr.type_check_raw(Some(&expected), &mut TypeInfo::new(), &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for concat([], [1]) with expected List<Int>, got {:?}",
            errs
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_lappend_to_empty_list_with_expected() {
        // append([], 42) with expected List<Int>
        let expr = SExpr::LAppend(
            Box::new(SExpr::List(EcoVec::new())),
            Box::new(SExpr::Val(Value::Int(42))),
        );
        let expected = StreamType::List(Box::new(StreamType::Int));
        let mut errs = vec![];
        let result = expr.type_check_raw(Some(&expected), &mut TypeInfo::new(), &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for append([], 42) with expected List<Int>, got {:?}",
            errs
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_top_level_type_check_empty_list_output() {
        // Simulates a full spec where an output variable is assigned []
        // and its declared type is List<Int>.
        let mut exprs = BTreeMap::new();
        let var: VarName = "y".into();
        exprs.insert(var.clone(), SExpr::List(EcoVec::new()));
        let mut type_annotations = BTreeMap::new();
        type_annotations.insert(var.clone(), StreamType::List(Box::new(StreamType::Int)));
        let spec = DsrvSpecification {
            input_vars: BTreeSet::new(),
            output_vars: BTreeSet::from([var.clone()]),
            exprs,
            type_annotations,
            aux_info: vec![],
        };
        let result = type_check(spec);
        assert!(
            result.is_ok(),
            "Expected Ok for spec with y : List<Int> = [], got {:?}",
            result
        );
        let typed_spec = result.unwrap();
        let te = typed_spec.var_expr(&var).unwrap();
        assert_eq!(extract_type(&te), TCType::list(TCType::Int));
    }

    // --- Empty lists in type-erasing operations ---
    // Operations like is_defined and len produce a fixed output type (Bool / Int)
    // that does not constrain their input type.  They pass None as the expected
    // type to their argument, so the empty list falls back to empty-list element type.
    // Because these operations never inspect the element type, the Poly list
    // passes through successfully.

    #[test]
    fn test_is_defined_empty_list_ok() {
        // is_defined([]) — the empty list gets empty-list element type; is_defined
        // does not inspect the element type, so it succeeds.
        let expr = SExpr::IsDefined(Box::new(SExpr::List(EcoVec::new())));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_ok(),
            "Expected Ok for is_defined([]) via Poly, got {:?}",
            result
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::Bool);
    }

    #[test]
    fn test_is_defined_empty_list_ok_with_outer_expected() {
        // With outer expected Bool, same outcome.
        let expr = SExpr::IsDefined(Box::new(SExpr::List(EcoVec::new())));
        let mut errs = vec![];
        let result = expr.type_check_raw(Some(&StreamType::Bool), &mut TypeInfo::new(), &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for is_defined([]) with outer expected Bool, got {:?}",
            errs
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::Bool);
    }

    #[test]
    fn test_llen_empty_list_ok() {
        // len([]) — the empty list gets empty-list element type; len does not
        // inspect the element type, so it succeeds.
        let expr = SExpr::LLen(Box::new(SExpr::List(EcoVec::new())));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_ok(),
            "Expected Ok for len([]) via Poly, got {:?}",
            result
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    #[test]
    fn test_llen_empty_list_ok_with_outer_expected() {
        // With outer expected Int, same outcome.
        let expr = SExpr::LLen(Box::new(SExpr::List(EcoVec::new())));
        let mut errs = vec![];
        let result = expr.type_check_raw(Some(&StreamType::Int), &mut TypeInfo::new(), &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for len([]) with outer expected Int, got {:?}",
            errs
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    #[test]
    fn test_when_empty_list_ok() {
        // when([]) — same as is_defined: does not inspect element type.
        let expr = SExpr::When(Box::new(SExpr::List(EcoVec::new())));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_ok(),
            "Expected Ok for when([]) via Poly, got {:?}",
            result
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::Bool);
    }

    #[test]
    fn test_ltail_empty_list_ok() {
        // tail([]) — preserves the list type including Poly.
        let expr = SExpr::LTail(Box::new(SExpr::List(EcoVec::new())));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_ok(),
            "Expected Ok for tail([]) via Poly, got {:?}",
            result
        );
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::EmptyList)
        );
    }

    #[test]
    fn test_lhead_empty_list_poly_errors() {
        // head([]) — the element type is Poly and head needs a concrete element
        // type, so this is correctly rejected.
        let expr = SExpr::LHead(Box::new(SExpr::List(EcoVec::new())));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_err(),
            "Expected Err for head([]) with empty-list element type"
        );
    }

    #[test]
    fn test_lindex_empty_list_poly_errors() {
        // [][0] — same: index needs a concrete element type.
        let expr = SExpr::LIndex(
            Box::new(SExpr::List(EcoVec::new())),
            Box::new(SExpr::Val(Value::Int(0))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_err(),
            "Expected Err for [][0] with empty-list element type"
        );
    }

    #[test]
    fn test_default_empty_list_with_concrete_no_expected() {
        // default([], xs) where xs : List<Int>, without outer expected type.
        // The empty list gets Poly; unification with Int succeeds.
        let expr = SExpr::Default(
            Box::new(SExpr::List(EcoVec::new())),
            Box::new(SExpr::Var("xs".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Int)));
        let result = expr.type_check(&mut ctx);
        assert!(
            result.is_ok(),
            "Expected Ok for default([], xs) via empty-literal unification, got {:?}",
            result
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_lconcat_empty_list_with_concrete_no_expected() {
        // concat([], [1]) without outer expected type.
        let expr = SExpr::LConcat(
            Box::new(SExpr::List(EcoVec::new())),
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(1))]))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_ok(),
            "Expected Ok for concat([], [1]) via empty-literal unification, got {:?}",
            result
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_lappend_empty_list_no_expected() {
        // append([], 42) without outer expected type.
        let expr = SExpr::LAppend(
            Box::new(SExpr::List(EcoVec::new())),
            Box::new(SExpr::Val(Value::Int(42))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_ok(),
            "Expected Ok for append([], 42) via empty-literal unification, got {:?}",
            result
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    // --- LHead ---

    #[test]
    fn test_lhead_flat_int() {
        // head([1, 2]) : Int
        let expr = SExpr::LHead(Box::new(SExpr::List(EcoVec::from(vec![
            SExpr::Val(Value::Int(1)),
            SExpr::Val(Value::Int(2)),
        ]))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    #[test]
    fn test_lhead_flat_bool() {
        let expr = SExpr::LHead(Box::new(SExpr::List(EcoVec::from(vec![SExpr::Val(
            Value::Bool(true),
        )]))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Bool);
    }

    #[test]
    fn test_lhead_nested_produces_list() {
        // head([[1, 2], [3]]) : List<Int>
        let expr = SExpr::LHead(Box::new(SExpr::List(EcoVec::from(vec![
            SExpr::List(EcoVec::from(vec![
                SExpr::Val(Value::Int(1)),
                SExpr::Val(Value::Int(2)),
            ])),
            SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(3))])),
        ]))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_lhead_triple_nested() {
        // head([[[1]]]) : List<List<Int>>
        let expr = SExpr::LHead(Box::new(SExpr::List(EcoVec::from(vec![SExpr::List(
            EcoVec::from(vec![SExpr::List(EcoVec::from(vec![SExpr::Val(
                Value::Int(1),
            )]))]),
        )]))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::list(TCType::Int))
        );
    }

    #[test]
    fn test_lhead_of_non_list_error() {
        let expr = SExpr::LHead(Box::new(SExpr::Val(Value::Int(42))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_lhead_of_lhead_nested() {
        // head(head([[[ 7 ]]])) : List<Int>  then head again : Int
        // Inner: [[[7]]] is List<List<List<Int>>>
        // head(.) → List<List<Int>>
        // head(.) → List<Int>
        let innermost = SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(7))]));
        let middle = SExpr::List(EcoVec::from(vec![innermost]));
        let outer = SExpr::List(EcoVec::from(vec![middle]));
        let head_once = SExpr::LHead(Box::new(outer));
        let head_twice = SExpr::LHead(Box::new(head_once));
        let result = head_twice.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_lhead_of_lhead_of_lhead_to_scalar() {
        // head(head(head([[[42]]]))) : Int
        let innermost = SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(42))]));
        let middle = SExpr::List(EcoVec::from(vec![innermost]));
        let outer = SExpr::List(EcoVec::from(vec![middle]));
        let h1 = SExpr::LHead(Box::new(outer));
        let h2 = SExpr::LHead(Box::new(h1));
        let h3 = SExpr::LHead(Box::new(h2));
        let result = h3.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    // --- LTail ---

    #[test]
    fn test_ltail_flat() {
        // tail([1, 2, 3]) : List<Int>
        let expr = SExpr::LTail(Box::new(SExpr::List(EcoVec::from(vec![
            SExpr::Val(Value::Int(1)),
            SExpr::Val(Value::Int(2)),
            SExpr::Val(Value::Int(3)),
        ]))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_ltail_nested() {
        // tail([[1], [2], [3]]) : List<List<Int>>
        let expr = SExpr::LTail(Box::new(SExpr::List(EcoVec::from(vec![
            SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(1))])),
            SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(2))])),
            SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(3))])),
        ]))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::list(TCType::Int))
        );
    }

    #[test]
    fn test_ltail_of_non_list_error() {
        let expr = SExpr::LTail(Box::new(SExpr::Val(Value::Bool(true))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    // --- LLen ---

    #[test]
    fn test_llen_flat() {
        // len([1, 2]) : Int
        let expr = SExpr::LLen(Box::new(SExpr::List(EcoVec::from(vec![
            SExpr::Val(Value::Int(1)),
            SExpr::Val(Value::Int(2)),
        ]))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    #[test]
    fn test_llen_nested() {
        // len([[1], [2, 3]]) : Int  (length of outer list = 2)
        let expr = SExpr::LLen(Box::new(SExpr::List(EcoVec::from(vec![
            SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(1))])),
            SExpr::List(EcoVec::from(vec![
                SExpr::Val(Value::Int(2)),
                SExpr::Val(Value::Int(3)),
            ])),
        ]))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    #[test]
    fn test_llen_of_non_list_error() {
        let expr = SExpr::LLen(Box::new(SExpr::Val(Value::Int(1))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    // --- LIndex ---

    #[test]
    fn test_lindex_flat_int() {
        // [10, 20][0] : Int
        let expr = SExpr::LIndex(
            Box::new(SExpr::List(EcoVec::from(vec![
                SExpr::Val(Value::Int(10)),
                SExpr::Val(Value::Int(20)),
            ]))),
            Box::new(SExpr::Val(Value::Int(0))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    #[test]
    fn test_lindex_nested_produces_list() {
        // [[1, 2], [3]][0] : List<Int>
        let expr = SExpr::LIndex(
            Box::new(SExpr::List(EcoVec::from(vec![
                SExpr::List(EcoVec::from(vec![
                    SExpr::Val(Value::Int(1)),
                    SExpr::Val(Value::Int(2)),
                ])),
                SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(3))])),
            ]))),
            Box::new(SExpr::Val(Value::Int(0))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_lindex_non_int_index_error() {
        let expr = SExpr::LIndex(
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(1))]))),
            Box::new(SExpr::Val(Value::Bool(true))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_lindex_non_list_error() {
        let expr = SExpr::LIndex(
            Box::new(SExpr::Val(Value::Int(1))),
            Box::new(SExpr::Val(Value::Int(0))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    // --- LAppend ---

    #[test]
    fn test_lappend_flat() {
        // append([1, 2], 3) : List<Int>
        let expr = SExpr::LAppend(
            Box::new(SExpr::List(EcoVec::from(vec![
                SExpr::Val(Value::Int(1)),
                SExpr::Val(Value::Int(2)),
            ]))),
            Box::new(SExpr::Val(Value::Int(3))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_lappend_nested() {
        // append([[1]], [2, 3]) : List<List<Int>>
        let expr = SExpr::LAppend(
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::List(EcoVec::from(
                vec![SExpr::Val(Value::Int(1))],
            ))]))),
            Box::new(SExpr::List(EcoVec::from(vec![
                SExpr::Val(Value::Int(2)),
                SExpr::Val(Value::Int(3)),
            ]))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::list(TCType::Int))
        );
    }

    #[test]
    fn test_lappend_type_mismatch_error() {
        // append([1, 2], "hello") — element type mismatch
        let expr = SExpr::LAppend(
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(1))]))),
            Box::new(SExpr::Val(Value::Str("hello".into()))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_lappend_nested_type_mismatch_error() {
        // append([[1]], 42) — expects List<Int> element, got Int
        let expr = SExpr::LAppend(
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::List(EcoVec::from(
                vec![SExpr::Val(Value::Int(1))],
            ))]))),
            Box::new(SExpr::Val(Value::Int(42))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_lappend_to_non_list_error() {
        let expr = SExpr::LAppend(
            Box::new(SExpr::Val(Value::Int(1))),
            Box::new(SExpr::Val(Value::Int(2))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    // --- LConcat ---

    #[test]
    fn test_lconcat_flat() {
        // concat([1], [2, 3]) : List<Int>
        let expr = SExpr::LConcat(
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(1))]))),
            Box::new(SExpr::List(EcoVec::from(vec![
                SExpr::Val(Value::Int(2)),
                SExpr::Val(Value::Int(3)),
            ]))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_lconcat_nested() {
        // concat([[1]], [[2]]) : List<List<Int>>
        let expr = SExpr::LConcat(
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::List(EcoVec::from(
                vec![SExpr::Val(Value::Int(1))],
            ))]))),
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::List(EcoVec::from(
                vec![SExpr::Val(Value::Int(2))],
            ))]))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::list(TCType::Int))
        );
    }

    #[test]
    fn test_lconcat_type_mismatch_error() {
        // concat([1], ["a"]) — different element types
        let expr = SExpr::LConcat(
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(1))]))),
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Str(
                "a".into(),
            ))]))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_lconcat_nesting_mismatch_error() {
        // concat([1], [[2]]) — List<Int> vs List<List<Int>>
        let expr = SExpr::LConcat(
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(1))]))),
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::List(EcoVec::from(
                vec![SExpr::Val(Value::Int(2))],
            ))]))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_lconcat_non_list_error() {
        let expr = SExpr::LConcat(
            Box::new(SExpr::Val(Value::Int(1))),
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(2))]))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    // --- Var with list types ---

    #[test]
    fn test_var_list_int() {
        let expr = SExpr::Var("xs".into());
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Int)));
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_var_list_nested() {
        let expr = SExpr::Var("xss".into());
        let mut ctx = TypeInfo::new();
        ctx.insert(
            "xss".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Bool)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::list(TCType::Bool))
        );
    }

    #[test]
    fn test_lhead_of_var_nested() {
        // head(xss) where xss : List<List<Int>> → List<Int>
        let expr = SExpr::LHead(Box::new(SExpr::Var("xss".into())));
        let mut ctx = TypeInfo::new();
        ctx.insert(
            "xss".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_lhead_of_lhead_of_var_to_scalar() {
        // head(head(xss)) where xss : List<List<Int>> → Int
        let expr = SExpr::LHead(Box::new(SExpr::LHead(Box::new(SExpr::Var("xss".into())))));
        let mut ctx = TypeInfo::new();
        ctx.insert(
            "xss".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    #[test]
    fn test_ltail_of_var_nested() {
        // tail(xss) where xss : List<List<Str>> → List<List<Str>>
        let expr = SExpr::LTail(Box::new(SExpr::Var("xss".into())));
        let mut ctx = TypeInfo::new();
        ctx.insert(
            "xss".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Str)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::list(TCType::Str))
        );
    }

    #[test]
    fn test_llen_of_var_nested() {
        // len(xss) where xss : List<List<Int>> → Int
        let expr = SExpr::LLen(Box::new(SExpr::Var("xss".into())));
        let mut ctx = TypeInfo::new();
        ctx.insert(
            "xss".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    #[test]
    fn test_lindex_of_var_nested() {
        // xss[0] where xss : List<List<Float>> → List<Float>
        let expr = SExpr::LIndex(
            Box::new(SExpr::Var("xss".into())),
            Box::new(SExpr::Val(Value::Int(0))),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert(
            "xss".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Float)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Float));
    }

    // --- Float type checking ---

    #[test]
    fn test_default_float_ok() {
        // default(x, y) where x, y : Float
        let expr = SExpr::Default(
            Box::new(SExpr::Var("x".into())),
            Box::new(SExpr::Var("y".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Float);
        ctx.insert("y".into(), StreamType::Float);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Float);
    }

    #[test]
    fn test_if_float_ok() {
        // if true then x else y where x, y : Float
        let expr = SExpr::If(
            Box::new(SExpr::Val(Value::Bool(true))),
            Box::new(SExpr::Var("x".into())),
            Box::new(SExpr::Var("y".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Float);
        ctx.insert("y".into(), StreamType::Float);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Float);
    }

    #[test]
    fn test_default_float_type_mismatch_error() {
        // default(x, y) where x : Float, y : Int → error
        let expr = SExpr::Default(
            Box::new(SExpr::Var("x".into())),
            Box::new(SExpr::Var("y".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Float);
        ctx.insert("y".into(), StreamType::Int);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_if_float_type_mismatch_error() {
        // if true then x else y where x : Float, y : Int → error
        let expr = SExpr::If(
            Box::new(SExpr::Val(Value::Bool(true))),
            Box::new(SExpr::Var("x".into())),
            Box::new(SExpr::Var("y".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Float);
        ctx.insert("y".into(), StreamType::Int);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    // --- List operations with non-scalar types ---

    #[test]
    fn test_default_nested_lists() {
        // default(xs, ys) where xs, ys : List<List<Int>>
        let expr = SExpr::Default(
            Box::new(SExpr::Var("xs".into())),
            Box::new(SExpr::Var("ys".into())),
        );
        let mut ctx = TypeInfo::new();
        let ty = StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int))));
        ctx.insert("xs".into(), ty.clone());
        ctx.insert("ys".into(), ty.clone());
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::from_stream_type(&ty)
        );
    }

    #[test]
    fn test_default_nested_list_type_mismatch_error() {
        // default(xs, ys) where xs : List<Int>, ys : List<Bool>
        let expr = SExpr::Default(
            Box::new(SExpr::Var("xs".into())),
            Box::new(SExpr::Var("ys".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Int)));
        ctx.insert("ys".into(), StreamType::List(Box::new(StreamType::Bool)));
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_default_nested_list_depth_mismatch_error() {
        // default(xs, ys) where xs : List<Int>, ys : List<List<Int>>
        let expr = SExpr::Default(
            Box::new(SExpr::Var("xs".into())),
            Box::new(SExpr::Var("ys".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Int)));
        ctx.insert(
            "ys".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    // --- If on nested lists ---

    #[test]
    fn test_if_nested_lists() {
        // if true then xs else ys where xs, ys : List<List<Int>>
        let expr = SExpr::If(
            Box::new(SExpr::Val(Value::Bool(true))),
            Box::new(SExpr::Var("xs".into())),
            Box::new(SExpr::Var("ys".into())),
        );
        let mut ctx = TypeInfo::new();
        let ty = StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int))));
        ctx.insert("xs".into(), ty.clone());
        ctx.insert("ys".into(), ty.clone());
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::from_stream_type(&ty)
        );
    }

    #[test]
    fn test_if_nested_list_branch_type_mismatch_error() {
        // if true then xs else ys where xs : List<Int>, ys : List<Str>
        let expr = SExpr::If(
            Box::new(SExpr::Val(Value::Bool(true))),
            Box::new(SExpr::Var("xs".into())),
            Box::new(SExpr::Var("ys".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Int)));
        ctx.insert("ys".into(), StreamType::List(Box::new(StreamType::Str)));
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_if_list_vs_scalar_branch_error() {
        // if true then xs else y where xs : List<Int>, y : Int
        let expr = SExpr::If(
            Box::new(SExpr::Val(Value::Bool(true))),
            Box::new(SExpr::Var("xs".into())),
            Box::new(SExpr::Var("y".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Int)));
        ctx.insert("y".into(), StreamType::Int);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    // --- Init on nested lists ---

    #[test]
    fn test_init_nested_lists() {
        // init(xs, ys) where xs, ys : List<List<Bool>>
        let expr = SExpr::Init(
            Box::new(SExpr::Var("xs".into())),
            Box::new(SExpr::Var("ys".into())),
        );
        let mut ctx = TypeInfo::new();
        let ty = StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Bool))));
        ctx.insert("xs".into(), ty.clone());
        ctx.insert("ys".into(), ty.clone());
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::from_stream_type(&ty)
        );
    }

    #[test]
    fn test_init_nested_list_type_mismatch_error() {
        // init(xs, ys) where xs : List<Int>, ys : List<Float>
        let expr = SExpr::Init(
            Box::new(SExpr::Var("xs".into())),
            Box::new(SExpr::Var("ys".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Int)));
        ctx.insert("ys".into(), StreamType::List(Box::new(StreamType::Float)));
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    // --- IsDefined on nested lists ---

    #[test]
    fn test_is_defined_nested_list() {
        // is_defined(xss) where xss : List<List<Int>> → Bool
        let expr = SExpr::IsDefined(Box::new(SExpr::Var("xss".into())));
        let mut ctx = TypeInfo::new();
        ctx.insert(
            "xss".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Bool);
    }

    #[test]
    fn test_is_defined_flat_list() {
        // is_defined(xs) where xs : List<Str> → Bool
        let expr = SExpr::IsDefined(Box::new(SExpr::Var("xs".into())));
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Str)));
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Bool);
    }

    // --- When on nested lists ---

    #[test]
    fn test_when_nested_list() {
        // when(xss) where xss : List<List<Float>> → Bool
        let expr = SExpr::When(Box::new(SExpr::Var("xss".into())));
        let mut ctx = TypeInfo::new();
        ctx.insert(
            "xss".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Float)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Bool);
    }

    #[test]
    fn test_when_flat_list() {
        // when(xs) where xs : List<Int> → Bool
        let expr = SExpr::When(Box::new(SExpr::Var("xs".into())));
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Int)));
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Bool);
    }

    // --- SIndex on nested lists ---

    #[test]
    fn test_sindex_on_nested_list_var() {
        // xs[-1] (stream index) where xs : List<List<Int>> → List<List<Int>>
        let expr = SExpr::SIndex(Box::new(SExpr::Var("xs".into())), 1);
        let mut ctx = TypeInfo::new();
        let ty = StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int))));
        ctx.insert("xs".into(), ty.clone());
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::from_stream_type(&ty)
        );
    }

    // --- Compositions: head of tail, len of head, etc. ---

    #[test]
    fn test_lhead_of_ltail_nested() {
        // head(tail(xss)) where xss : List<List<Int>> → List<Int>
        let expr = SExpr::LHead(Box::new(SExpr::LTail(Box::new(SExpr::Var("xss".into())))));
        let mut ctx = TypeInfo::new();
        ctx.insert(
            "xss".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_llen_of_lhead_nested() {
        // len(head(xss)) where xss : List<List<Int>> → Int
        let expr = SExpr::LLen(Box::new(SExpr::LHead(Box::new(SExpr::Var("xss".into())))));
        let mut ctx = TypeInfo::new();
        ctx.insert(
            "xss".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    #[test]
    fn test_lappend_head_to_tail_nested() {
        // append(tail(xss), head(xss)) where xss : List<List<Int>> → List<List<Int>>
        // tail(xss) : List<List<Int>>, head(xss) : List<Int>
        let xss = || SExpr::Var("xss".into());
        let expr = SExpr::LAppend(
            Box::new(SExpr::LTail(Box::new(xss()))),
            Box::new(SExpr::LHead(Box::new(xss()))),
        );
        let mut ctx = TypeInfo::new();
        let ty = StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int))));
        ctx.insert("xss".into(), ty.clone());
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::from_stream_type(&ty)
        );
    }

    #[test]
    fn test_lconcat_of_ltail() {
        // concat(tail(xs), tail(xs)) where xs : List<Int> → List<Int>
        let xs = || SExpr::Var("xs".into());
        let expr = SExpr::LConcat(
            Box::new(SExpr::LTail(Box::new(xs()))),
            Box::new(SExpr::LTail(Box::new(xs()))),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Int)));
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_basic_types_matching() {
        // These types are explicitly handled in the implementation
        assert!(check_value_type(TCType::Int, &Value::Int(42)).is_ok());
        assert!(check_value_type(TCType::Str, &Value::Str("hello".into())).is_ok());
        assert!(check_value_type(TCType::Bool, &Value::Bool(true)).is_ok());

        // Testing Float and Unit types to match expected functionality
        assert!(check_value_type(TCType::Float, &Value::Float(3.14)).is_ok());
        assert!(check_value_type(TCType::Unit, &Value::Unit).is_ok());
    }

    #[test]
    fn test_type_mismatches() {
        // Int type with non-Int values
        assert!(check_value_type(TCType::Int, &Value::Str("42".into())).is_err());
        assert!(check_value_type(TCType::Int, &Value::Bool(true)).is_err());
        assert!(check_value_type(TCType::Int, &Value::Float(42.0)).is_err());

        // Str type with non-Str values
        assert!(check_value_type(TCType::Str, &Value::Int(42)).is_err());
        assert!(check_value_type(TCType::Str, &Value::Bool(true)).is_err());

        // Bool type with non-Bool values
        assert!(check_value_type(TCType::Bool, &Value::Int(0)).is_err());
        assert!(check_value_type(TCType::Bool, &Value::Str("true".into())).is_err());
    }

    #[test]
    fn test_empty_literal_and_unknown_placeholders_accept_values() {
        // Empty container placeholders accept any value while the literal is still unconstrained.
        assert!(check_value_type(TCType::EmptyList, &Value::Int(42)).is_ok());
        assert!(check_value_type(TCType::EmptyList, &Value::Str("hello".into())).is_ok());
        assert!(check_value_type(TCType::EmptyMap, &Value::Bool(true)).is_ok());
        assert!(check_value_type(TCType::EmptyMap, &Value::Float(3.14)).is_ok());

        let list_val = Value::List(vec![Value::Int(1), Value::Int(2)].into());
        let map_val = Value::Map(BTreeMap::from([("x".into(), Value::Int(1))]));
        assert!(check_value_type(TCType::EmptyList, &list_val).is_ok());
        assert!(check_value_type(TCType::EmptyMap, &map_val).is_ok());

        // Special value placeholder
        assert!(check_value_type(TCType::Unknown, &Value::Deferred).is_ok());
        assert!(check_value_type(TCType::Unknown, &Value::NoVal).is_ok());
    }

    #[test]
    fn test_stream_type_validation_checks_recursive_lists() {
        let list_type = StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int))));
        let valid = Value::List(
            vec![
                Value::List(vec![Value::Int(1), Value::Int(2)].into()),
                Value::List(vec![Value::Int(3)].into()),
            ]
            .into(),
        );
        let invalid = Value::List(
            vec![
                Value::List(vec![Value::Int(1)].into()),
                Value::List(vec![Value::Str("wrong".into())].into()),
            ]
            .into(),
        );

        assert!(check_value_stream_type(&list_type, &valid).is_ok());
        assert!(check_value_stream_type(&list_type, &invalid).is_err());
    }

    #[test]
    fn test_list_type_matching() {
        // Empty list
        let empty_list = Value::List(vec![].into());
        assert!(check_value_type(TCType::list(TCType::Int), &empty_list).is_ok());
        assert!(check_value_type(TCType::list(TCType::Str), &empty_list).is_ok());
        assert!(check_value_type(TCType::list(TCType::Bool), &empty_list).is_ok());

        // Homogeneous lists
        let int_list = Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)].into());
        assert!(check_value_type(TCType::list(TCType::Int), &int_list).is_ok());
        assert!(check_value_type(TCType::list(TCType::Str), &int_list).is_err());

        let str_list = Value::List(
            vec![
                Value::Str("a".into()),
                Value::Str("b".into()),
                Value::Str("c".into()),
            ]
            .into(),
        );
        assert!(check_value_type(TCType::list(TCType::Str), &str_list).is_ok());
        assert!(check_value_type(TCType::list(TCType::Int), &str_list).is_err());

        // Mixed list (should fail for any specific element type)
        let mixed_list =
            Value::List(vec![Value::Int(1), Value::Str("a".into()), Value::Bool(true)].into());
        assert!(check_value_type(TCType::list(TCType::Int), &mixed_list).is_err());
        assert!(check_value_type(TCType::list(TCType::Str), &mixed_list).is_err());
        assert!(check_value_type(TCType::list(TCType::Bool), &mixed_list).is_err());
    }

    #[test]
    fn test_nested_list_type_matching() {
        // List of List of Int
        let nested_int_list = Value::List(
            vec![
                Value::List(vec![Value::Int(1), Value::Int(2)].into()),
                Value::List(vec![Value::Int(3), Value::Int(4)].into()),
            ]
            .into(),
        );

        let nested_int_type = TCType::list(TCType::list(TCType::Int));
        assert!(check_value_type(nested_int_type.clone(), &nested_int_list).is_ok());

        // Nested list with type errors
        let nested_mixed_list = Value::List(
            vec![
                Value::List(vec![Value::Int(1), Value::Int(2)].into()),
                Value::List(vec![Value::Str("a".into())].into()),
            ]
            .into(),
        );
        assert!(check_value_type(nested_int_type, &nested_mixed_list).is_err());
    }

    #[test]
    fn test_deeply_nested_list() {
        // Triple nested list
        let triple_nested_list = Value::List(
            vec![Value::List(
                vec![Value::List(
                    vec![Value::Bool(true), Value::Bool(false)].into(),
                )]
                .into(),
            )]
            .into(),
        );

        let triple_nested_type = TCType::list(TCType::list(TCType::list(TCType::Bool)));
        assert!(check_value_type(triple_nested_type, &triple_nested_list).is_ok());
    }

    #[test]
    fn test_list_depth_mismatches() {
        // A list of integers (depth 1)
        let simple_list = Value::List(vec![Value::Int(1), Value::Int(2)].into());

        // A list of lists of integers (depth 2)
        let nested_list = Value::List(
            vec![
                Value::List(vec![Value::Int(1), Value::Int(2)].into()),
                Value::List(vec![Value::Int(3), Value::Int(4)].into()),
            ]
            .into(),
        );

        // Test: Cannot use a simple list where a nested list is expected
        assert!(check_value_type(TCType::list(TCType::list(TCType::Int)), &simple_list).is_err());

        // Test: Cannot use a nested list where a simple list is expected
        assert!(check_value_type(TCType::list(TCType::Int), &nested_list).is_err());

        // Test: Make sure the error message mentions something about a type mismatch
        let result = check_value_type(TCType::list(TCType::list(TCType::Int)), &simple_list);
        assert!(result.is_err());
        let error_msg = result.unwrap_err();
        assert!(error_msg.contains("Type mismatch"));
    }

    #[test]
    fn test_special_values() {
        // These should fail since they're not the expected concrete types
        assert!(check_value_type(TCType::Int, &Value::Deferred).is_err());
        assert!(check_value_type(TCType::Str, &Value::NoVal).is_err());
        assert!(check_value_type(TCType::list(TCType::Int), &Value::Deferred).is_err());
    }

    #[test]
    fn test_error_messages() {
        // Test that error messages contain useful information
        let result = check_value_type(TCType::Int, &Value::Str("42".into()));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Type mismatch"));

        // Test error propagation in nested lists
        let str_in_int_list = Value::List(
            vec![
                Value::Int(1),
                Value::Str("not an int".into()), // This will cause error
                Value::Int(3),
            ]
            .into(),
        );
        let result = check_value_type(TCType::list(TCType::Int), &str_in_int_list);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Type mismatch"));
    }

    // These tests validate boundary cases
    #[test]
    fn test_boundary_cases() {
        // Non-list value with list type
        assert!(check_value_type(TCType::list(TCType::Int), &Value::Int(42)).is_err());

        // Trying to treat a non-primitive as primitive
        let list_val = Value::List(vec![Value::Int(1), Value::Int(2)].into());
        assert!(check_value_type(TCType::Int, &list_val).is_err());

        // Empty nested list
        let empty_nested = Value::List(vec![Value::List(vec![].into())].into());
        assert!(check_value_type(TCType::list(TCType::list(TCType::Int)), &empty_nested).is_ok());
    }

    #[test]
    fn test_extract_value_type_primitives() {
        // Test primitives
        assert_eq!(extract_value_type(Value::Int(42)), TCType::Int);
        assert_eq!(extract_value_type(Value::Str("hello".into())), TCType::Str);
        assert_eq!(extract_value_type(Value::Bool(true)), TCType::Bool);
        assert_eq!(extract_value_type(Value::Float(3.14)), TCType::Float);
        assert_eq!(extract_value_type(Value::Unit), TCType::Unit);
    }

    #[test]
    fn test_extract_value_type_list() {
        // Test list type
        let list_value = Value::List(vec![Value::Int(1), Value::Int(2)].into());
        match extract_value_type(list_value) {
            TCType::Construct(TCTypeConstructor::List, args) if args.len() == 1 => {
                // Expect EmptyList as default inner type
                assert_eq!(args[0], TCType::EmptyList);
            }
            _ => panic!("Expected List type for list value"),
        }
    }

    #[test]
    fn test_extract_value_type_special_values() {
        assert_eq!(
            extract_value_type(Value::Map(Default::default())),
            TCType::map(TCType::EmptyMap)
        );
        assert_eq!(extract_value_type(Value::Deferred), TCType::Unknown);
        assert_eq!(extract_value_type(Value::NoVal), TCType::Unknown);
    }

    #[test]
    fn test_map_value_type_matching() {
        let map = Value::Map(BTreeMap::from([
            ("x".into(), Value::Int(1)),
            ("y".into(), Value::Int(2)),
        ]));
        assert!(check_value_type(TCType::map(TCType::Int), &map).is_ok());
        assert!(check_value_type(TCType::map(TCType::Str), &map).is_err());

        let nested = Value::Map(BTreeMap::from([(
            "xs".into(),
            Value::List(vec![Value::Bool(true)].into()),
        )]));
        assert!(check_value_type(TCType::map(TCType::list(TCType::Bool)), &nested).is_ok());
    }

    #[test]
    fn test_sexpr_map_literal_and_get() {
        let expr = SExpr::Map(BTreeMap::from([
            ("x".into(), SExpr::Val(Value::Int(1))),
            ("y".into(), SExpr::Val(Value::Int(2))),
        ]));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::map(TCType::Int));

        let get = SExpr::MGet(Box::new(expr), "x".into());
        let result = get.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    #[test]
    fn test_sexpr_map_insert_remove_and_has_key() {
        let empty = SExpr::Map(BTreeMap::new());
        let expected = StreamType::Map(Box::new(StreamType::Bool));
        let mut errs = vec![];
        let inserted = SExpr::MInsert(
            Box::new(empty.clone()),
            "flag".into(),
            Box::new(SExpr::Val(Value::Bool(true))),
        );
        let result = inserted.type_check_raw(Some(&expected), &mut TypeInfo::new(), &mut errs);
        assert!(result.is_ok(), "Expected Ok, got {:?}", errs);
        assert_eq!(extract_type(&result.unwrap()), TCType::map(TCType::Bool));

        let removed = SExpr::MRemove(Box::new(empty.clone()), "flag".into());
        let mut errs = vec![];
        let result = removed.type_check_raw(Some(&expected), &mut TypeInfo::new(), &mut errs);
        assert!(result.is_ok(), "Expected Ok, got {:?}", errs);
        assert_eq!(extract_type(&result.unwrap()), TCType::map(TCType::Bool));

        let has_key = SExpr::MHasKey(Box::new(empty), "flag".into());
        let mut errs = vec![];
        let result = has_key.type_check_raw(Some(&expected), &mut TypeInfo::new(), &mut errs);
        assert!(result.is_ok(), "Expected Ok, got {:?}", errs);
        assert_eq!(extract_type(&result.unwrap()), TCType::Bool);
    }

    #[test]
    fn test_extract_and_check_compatibility() {
        // Extract a type from a value, then check the same value against that type
        let int_value = Value::Int(42);
        let extracted_type = extract_value_type(int_value.clone());
        assert_eq!(extracted_type, TCType::Int);
        assert!(check_value_type(extracted_type, &int_value).is_ok());

        // Same for string
        let str_value = Value::Str("test".into());
        let extracted_type = extract_value_type(str_value.clone());
        assert_eq!(extracted_type, TCType::Str);
        assert!(check_value_type(extracted_type, &str_value).is_ok());

        // Same for list
        let list_value = Value::List(vec![Value::Bool(true), Value::Bool(false)].into());
        let extracted_type = extract_value_type(list_value.clone());
        // List extraction uses EmptyList inner type, so we need to create a matching type for check
        let list_type = TCType::list(TCType::EmptyList);
        assert!(matches!(
            extracted_type,
            TCType::Construct(TCTypeConstructor::List, _)
        ));
        assert!(check_value_type(list_type, &list_value).is_ok());
    }
}
