//! The typed AST for DSRV specifications: the output language of the type
//! checker (see `super::type_checker`) and the input language of the typed
//! semantics.

use contracts::requires;
use ecow::{EcoString, EcoVec};
use itertools::Itertools;

use super::ast::{
    BoolBinOp, CompBinOp, FloatBinOp, IntBinOp, RuntimeScope, SExpr, SpannedExpr, StrBinOp,
};
use crate::core::{PartialStreamValue, StreamType};
use crate::{Specification, Value, VarName};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display};

/// Type environment mapping variable names to their declared stream types.
pub type TypeInfo = BTreeMap<VarName, StreamType>;
/// Type-checker–internal type representation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum TCType {
    Int,
    Float,
    Str,
    Bool,
    Unit,
    Map(Box<TCType>),
    Tuple(EcoVec<TCType>),
    List(Box<TCType>),
    /// Struct specified as a list of field/type pairs and whether extra fields are allowed.
    Struct(EcoVec<(EcoString, TCType)>, bool),
    Function(EcoVec<TCType>, Box<TCType>),
    /// Placeholder used for empty list literals whose element type is not yet known.
    EmptyList,
    /// Placeholder used for empty map literals whose value type is not yet known.
    EmptyMap,
    /// Gradual/dynamic type. Accepted statically and checked when cast to a stricter type.
    Any,
    /// Unknown non-container type used for special values such as Deferred/NoVal.
    Unknown,
}

impl TCType {
    pub fn list(element_type: TCType) -> Self {
        TCType::List(Box::new(element_type))
    }

    pub fn map(value_type: TCType) -> Self {
        TCType::Map(Box::new(value_type))
    }

    pub fn list_element_type(&self) -> Option<&TCType> {
        match self {
            TCType::List(typ) => Some(typ),
            _ => None,
        }
    }

    pub fn map_value_type(&self) -> Option<&TCType> {
        match self {
            TCType::Map(typ) => Some(typ),
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
            StreamType::List(inner) => TCType::List(Box::new(TCType::from_stream_type(inner))),
            StreamType::Tuple(inner) => {
                TCType::Tuple(inner.iter().map(TCType::from_stream_type).collect())
            }
            StreamType::Map(inner) => TCType::Map(Box::new(TCType::from_stream_type(inner))),
            StreamType::Struct(inner, allow_extra) => TCType::Struct(
                inner
                    .iter()
                    .map(|(n, t)| (n.clone(), TCType::from_stream_type(t)))
                    .collect(),
                *allow_extra,
            ),
            StreamType::Function(args, ret) => TCType::Function(
                args.iter().map(TCType::from_stream_type).collect(),
                Box::new(TCType::from_stream_type(ret)),
            ),
            StreamType::Any => TCType::Any,
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
            TCType::List(inner) => inner
                .to_stream_type()
                .map(|x| StreamType::List(Box::new(x))),
            TCType::Tuple(inner) => inner
                .iter()
                .map(TCType::to_stream_type)
                .collect::<Option<EcoVec<_>>>()
                .map(StreamType::Tuple),
            TCType::Map(inner) => inner.to_stream_type().map(|x| StreamType::Map(Box::new(x))),
            TCType::Struct(inner, allow_extra) => inner
                .iter()
                .map(|(k, v)| v.to_stream_type().map(|v| (k.into(), v)))
                .fold_options(
                    StreamType::Struct(EcoVec::new(), *allow_extra),
                    |acc, (k, v)| match acc {
                        StreamType::Struct(mut inner, allow_extra) => {
                            inner.push((k, v));
                            StreamType::Struct(inner, allow_extra)
                        }
                        _ => unreachable!(),
                    },
                ),
            TCType::Function(args, ret) => {
                let args = args
                    .iter()
                    .map(TCType::to_stream_type)
                    .collect::<Option<EcoVec<_>>>()?;
                let ret = ret.to_stream_type()?;
                Some(StreamType::Function(args, Box::new(ret)))
            }
            TCType::Any => Some(StreamType::Any),
            TCType::EmptyList | TCType::EmptyMap | TCType::Unknown => None,
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
            TCType::List(typ) => write!(f, "List<{}>", typ),
            TCType::Tuple(inner) => {
                let len = inner.len();
                let inner = inner.iter().map(|typ| format!("{}", typ)).join(", ");
                if len == 1 {
                    write!(f, "({},)", inner)
                } else {
                    write!(f, "({})", inner)
                }
            }
            TCType::Map(typ) => write!(f, "Map<{}>", typ),
            TCType::Struct(inner, allow_extra) => {
                let mut fields = inner
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, v))
                    .collect::<Vec<_>>();
                if *allow_extra {
                    fields.push("...".into());
                }
                write!(f, "Struct<{}>", fields.join(", "))
            }
            TCType::Function(args, ret) => {
                let args = args.iter().map(|arg| format!("{}", arg)).join(", ");
                write!(f, "({} -> {})", args, ret)
            }
            TCType::Any => write!(f, "Any"),
            TCType::EmptyList => write!(f, "EmptyList"),
            TCType::EmptyMap => write!(f, "EmptyMap"),
            TCType::Unknown => write!(f, "Unknown"),
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum SExprBool {
    Val(PartialStreamValue<bool>),
    Cast(Box<SExprTE>),

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
    Update(Box<Self>, Box<Self>),
    Latch(Box<Self>, Box<Self>),
    Init(Box<Self>, Box<Self>),

    // Boolean-producing unary operators on typed streams
    IsDefined(Box<SExprTE>),
    When(Box<SExprTE>),

    // List element extraction producing Bool
    LHeadList(TypedListExpr),
    LIndexList(TypedListExpr, Box<SExprInt>),

    // Map/struct operations producing Bool
    MGetMap(TypedMapExpr, EcoString),
    SGetStruct(TypedStructExpr, EcoString),
    SGetTuple(TypedTupleExpr, usize),
    MHasKeyMap(TypedMapExpr, EcoString),

    // Deferred and dynamic expressions
    Defer(Box<SExprStr>, TypeInfo, EcoVec<VarName>),
    Dynamic(Box<SExprStr>, TypeInfo),
    RestrictedDynamic(Box<SExprStr>, RuntimeScope, TypeInfo),
}

#[derive(Clone, PartialEq, Debug)]
pub enum SExprInt {
    Cast(Box<SExprTE>),
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

    // Async operators
    Update(Box<Self>, Box<Self>),
    Latch(Box<Self>, Box<Self>),

    // Math functions
    Abs(Box<Self>),

    // Async operators
    Init(Box<Self>, Box<Self>),

    // Deferred and dynamic expressions
    Defer(Box<SExprStr>, TypeInfo, EcoVec<VarName>),
    Dynamic(Box<SExprStr>, TypeInfo),
    RestrictedDynamic(Box<SExprStr>, RuntimeScope, TypeInfo),

    // List operations producing Int
    LLen(TypedListExpr),
    LHeadList(TypedListExpr),
    LIndexList(TypedListExpr, Box<SExprInt>),

    // Map/struct value extraction producing Int
    MGetMap(TypedMapExpr, EcoString),
    SGetStruct(TypedStructExpr, EcoString),
    SGetTuple(TypedTupleExpr, usize),
}

#[derive(Clone, PartialEq, Debug)]
pub enum SExprFloat {
    Cast(Box<SExprTE>),
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

    // Async operators
    Update(Box<Self>, Box<Self>),
    Latch(Box<Self>, Box<Self>),

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
    RestrictedDynamic(Box<SExprStr>, RuntimeScope, TypeInfo),

    // List element extraction producing Float
    LHeadList(TypedListExpr),
    LIndexList(TypedListExpr, Box<SExprInt>),

    // Map/struct value extraction producing Float
    MGetMap(TypedMapExpr, EcoString),
    SGetStruct(TypedStructExpr, EcoString),
    SGetTuple(TypedTupleExpr, usize),
}

// Stream expressions - now with types
#[derive(Clone, PartialEq, Debug)]
pub enum SExprUnit {
    Cast(Box<SExprTE>),
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
    Update(Box<Self>, Box<Self>),
    Latch(Box<Self>, Box<Self>),
    Init(Box<Self>, Box<Self>),

    // Deferred and dynamic expressions
    Defer(Box<SExprStr>, TypeInfo, EcoVec<VarName>),
    Dynamic(Box<SExprStr>, TypeInfo),
    RestrictedDynamic(Box<SExprStr>, RuntimeScope, TypeInfo),

    // List element extraction producing Unit
    LHeadList(TypedListExpr),
    LIndexList(TypedListExpr, Box<SExprInt>),

    // Map/struct value extraction producing Unit
    MGetMap(TypedMapExpr, EcoString),
    SGetStruct(TypedStructExpr, EcoString),
    SGetTuple(TypedTupleExpr, usize),
}

#[derive(Clone, PartialEq, Debug)]
pub enum SExprStr {
    Cast(Box<SExprTE>),
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
    Update(Box<Self>, Box<Self>),
    Latch(Box<Self>, Box<Self>),
    Init(Box<Self>, Box<Self>),

    // Deferred and dynamic expressions
    Defer(Box<SExprStr>, TypeInfo, EcoVec<VarName>),
    Dynamic(Box<SExprStr>, TypeInfo),
    RestrictedDynamic(Box<SExprStr>, RuntimeScope, TypeInfo),

    // List element extraction producing Str
    LHeadList(TypedListExpr),
    LIndexList(TypedListExpr, Box<SExprInt>),

    // Map/struct value extraction producing Str
    MGetMap(TypedMapExpr, EcoString),
    SGetStruct(TypedStructExpr, EcoString),
    SGetTuple(TypedTupleExpr, usize),
}

#[derive(Clone, PartialEq, Debug)]
pub enum TypedListExprKind {
    If(Box<SExprBool>, Box<TypedListExpr>, Box<TypedListExpr>),
    SIndex(Box<TypedListExpr>, u64),
    Var(VarName),
    Default(Box<TypedListExpr>, Box<TypedListExpr>),
    Update(Box<TypedListExpr>, Box<TypedListExpr>),
    Latch(Box<TypedListExpr>, Box<TypedListExpr>),
    Init(Box<TypedListExpr>, Box<TypedListExpr>),
    Defer(Box<SExprStr>, TypeInfo, EcoVec<VarName>),
    Dynamic(Box<SExprStr>, TypeInfo),
    RestrictedDynamic(Box<SExprStr>, RuntimeScope, TypeInfo),
    Literal(Vec<SExprTE>),
    LTail(Box<TypedListExpr>),
    LConcat(Box<TypedListExpr>, Box<TypedListExpr>),
    LAppend(Box<TypedListExpr>, Box<SExprTE>),
    LMap(Box<TypedFunctionExpr>, Box<TypedListExpr>),
    LFilter(Box<TypedFunctionExpr>, Box<TypedListExpr>),
    LHeadList(Box<TypedListExpr>),
    LIndexList(Box<TypedListExpr>, Box<SExprInt>),
    MGetMap(Box<TypedMapExpr>, EcoString),
    SGetStruct(Box<TypedStructExpr>, EcoString),
    SGetTuple(Box<TypedTupleExpr>, usize),
}

#[derive(Clone, PartialEq, Debug)]
pub struct TypedListExpr {
    /// Element type of this list expression, e.g. `Int` for `List<Int>`.
    pub element_type: TCType,
    pub kind: TypedListExprKind,
}

impl TypedListExpr {
    pub fn list_tc_type(&self) -> TCType {
        TCType::List(Box::new(self.element_type.clone()))
    }

    pub fn element_type(&self) -> &TCType {
        &self.element_type
    }

    pub fn typed_default(&self, other: &TypedListExpr) -> TypedListExpr {
        TypedListExpr {
            element_type: self.element_type.clone(),
            kind: TypedListExprKind::Default(Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    pub fn typed_if(&self, cond: Box<SExprBool>, other: &TypedListExpr) -> TypedListExpr {
        TypedListExpr {
            element_type: self.element_type.clone(),
            kind: TypedListExprKind::If(cond, Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    pub fn typed_init(&self, other: &TypedListExpr) -> TypedListExpr {
        TypedListExpr {
            element_type: self.element_type.clone(),
            kind: TypedListExprKind::Init(Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    pub fn typed_update_stream(&self, other: &TypedListExpr) -> TypedListExpr {
        TypedListExpr {
            element_type: self.element_type.clone(),
            kind: TypedListExprKind::Update(Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    pub fn typed_latch(&self, other: &TypedListExpr) -> TypedListExpr {
        TypedListExpr {
            element_type: self.element_type.clone(),
            kind: TypedListExprKind::Latch(Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    pub fn typed_tail(&self) -> TypedListExpr {
        TypedListExpr {
            element_type: self.element_type.clone(),
            kind: TypedListExprKind::LTail(Box::new(self.clone())),
        }
    }

    pub fn typed_sindex(&self, idx: u64) -> TypedListExpr {
        TypedListExpr {
            element_type: self.element_type.clone(),
            kind: TypedListExprKind::SIndex(Box::new(self.clone()), idx),
        }
    }

    pub fn typed_concat(&self, other: &TypedListExpr) -> TypedListExpr {
        TypedListExpr {
            element_type: self.element_type.clone(),
            kind: TypedListExprKind::LConcat(Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    pub fn typed_append(&self, elem: Box<SExprTE>) -> TypedListExpr {
        TypedListExpr {
            element_type: self.element_type.clone(),
            kind: TypedListExprKind::LAppend(Box::new(self.clone()), elem),
        }
    }

    pub fn typed_map(&self, func: Box<TypedFunctionExpr>, element_type: TCType) -> TypedListExpr {
        TypedListExpr {
            element_type,
            kind: TypedListExprKind::LMap(func, Box::new(self.clone())),
        }
    }

    pub fn typed_filter(&self, func: Box<TypedFunctionExpr>) -> TypedListExpr {
        TypedListExpr {
            element_type: self.element_type.clone(),
            kind: TypedListExprKind::LFilter(func, Box::new(self.clone())),
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct TypedFunctionExpr {
    pub params: EcoVec<(VarName, TCType)>,
    pub body: Box<SExprTE>,
    pub return_type: TCType,
    pub recursive_name: Option<VarName>,
}

#[derive(Clone, PartialEq, Debug)]
pub enum TypedTupleExprKind {
    Var(VarName),
    Literal(Vec<SExprTE>),
    Default(Box<TypedTupleExpr>, Box<TypedTupleExpr>),
    If(Box<SExprBool>, Box<TypedTupleExpr>, Box<TypedTupleExpr>),
    Update(Box<TypedTupleExpr>, Box<TypedTupleExpr>),
    Latch(Box<TypedTupleExpr>, Box<TypedTupleExpr>),
    Init(Box<TypedTupleExpr>, Box<TypedTupleExpr>),
    SIndex(Box<TypedTupleExpr>, u64),
    TGetTuple(Box<TypedTupleExpr>, usize),
}

#[derive(Clone, PartialEq, Debug)]
pub struct TypedTupleExpr {
    pub element_types: EcoVec<TCType>,
    pub kind: TypedTupleExprKind,
}

impl TypedTupleExpr {
    pub fn tuple_tc_type(&self) -> TCType {
        TCType::Tuple(self.element_types.clone())
    }

    pub fn to_stream_type(&self) -> Option<StreamType> {
        self.tuple_tc_type().to_stream_type()
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct TypedFoldExpr {
    pub func: Box<TypedFunctionExpr>,
    pub init: Box<SExprTE>,
    pub list: Box<TypedListExpr>,
    pub result_type: TCType,
}

#[derive(Clone, PartialEq, Debug)]
pub struct TypedApplyExpr {
    pub func: Box<TypedFunctionExpr>,
    pub args: EcoVec<SExprTE>,
    pub result_type: TCType,
}

#[derive(Clone, PartialEq, Debug)]
pub enum TypedMapExprKind {
    Var(VarName),
    Literal(BTreeMap<EcoString, SExprTE>),
    Default(Box<TypedMapExpr>, Box<TypedMapExpr>),
    If(Box<SExprBool>, Box<TypedMapExpr>, Box<TypedMapExpr>),
    Update(Box<TypedMapExpr>, Box<TypedMapExpr>),
    Latch(Box<TypedMapExpr>, Box<TypedMapExpr>),
    Init(Box<TypedMapExpr>, Box<TypedMapExpr>),
    SIndex(Box<TypedMapExpr>, u64),
    Defer(Box<SExprStr>, TypeInfo, EcoVec<VarName>),
    Dynamic(Box<SExprStr>, TypeInfo),
    RestrictedDynamic(Box<SExprStr>, RuntimeScope, TypeInfo),
    MInsert(Box<TypedMapExpr>, EcoString, Box<SExprTE>),
    MRemove(Box<TypedMapExpr>, EcoString),
    MGetMap(Box<TypedMapExpr>, EcoString),
    SGetStruct(Box<TypedStructExpr>, EcoString),
    SGetTuple(Box<TypedTupleExpr>, usize),
    LHeadList(Box<TypedListExpr>),
    LIndexList(Box<TypedListExpr>, Box<SExprInt>),
}

#[derive(Clone, PartialEq, Debug)]
pub struct TypedMapExpr {
    /// Value type of this map expression. Keys are strings.
    pub value_type: TCType,
    pub kind: TypedMapExprKind,
}

impl TypedMapExpr {
    pub fn map_tc_type(&self) -> TCType {
        TCType::Map(Box::new(self.value_type.clone()))
    }

    pub fn value_type(&self) -> &TCType {
        &self.value_type
    }

    pub fn typed_default(&self, other: &TypedMapExpr) -> TypedMapExpr {
        TypedMapExpr {
            value_type: self.value_type.clone(),
            kind: TypedMapExprKind::Default(Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    pub fn typed_if(&self, cond: Box<SExprBool>, other: &TypedMapExpr) -> TypedMapExpr {
        TypedMapExpr {
            value_type: self.value_type.clone(),
            kind: TypedMapExprKind::If(cond, Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    pub fn typed_init(&self, other: &TypedMapExpr) -> TypedMapExpr {
        TypedMapExpr {
            value_type: self.value_type.clone(),
            kind: TypedMapExprKind::Init(Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    pub fn typed_update_stream(&self, other: &TypedMapExpr) -> TypedMapExpr {
        TypedMapExpr {
            value_type: self.value_type.clone(),
            kind: TypedMapExprKind::Update(Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    pub fn typed_latch(&self, other: &TypedMapExpr) -> TypedMapExpr {
        TypedMapExpr {
            value_type: self.value_type.clone(),
            kind: TypedMapExprKind::Latch(Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    pub fn typed_sindex(&self, idx: u64) -> TypedMapExpr {
        TypedMapExpr {
            value_type: self.value_type.clone(),
            kind: TypedMapExprKind::SIndex(Box::new(self.clone()), idx),
        }
    }

    pub fn typed_insert(&self, key: EcoString, value: Box<SExprTE>) -> TypedMapExpr {
        TypedMapExpr {
            value_type: self.value_type.clone(),
            kind: TypedMapExprKind::MInsert(Box::new(self.clone()), key, value),
        }
    }

    pub fn typed_remove(&self, key: EcoString) -> TypedMapExpr {
        TypedMapExpr {
            value_type: self.value_type.clone(),
            kind: TypedMapExprKind::MRemove(Box::new(self.clone()), key),
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum TypedStructExprKind {
    Var(VarName),
    Literal(Vec<(EcoString, SExprTE)>),
    Default(Box<TypedStructExpr>, Box<TypedStructExpr>),
    If(Box<SExprBool>, Box<TypedStructExpr>, Box<TypedStructExpr>),
    Update(Box<TypedStructExpr>, Box<TypedStructExpr>),
    Latch(Box<TypedStructExpr>, Box<TypedStructExpr>),
    Init(Box<TypedStructExpr>, Box<TypedStructExpr>),
    SIndex(Box<TypedStructExpr>, u64),
    Defer(Box<SExprStr>, TypeInfo, EcoVec<VarName>),
    Dynamic(Box<SExprStr>, TypeInfo),
    RestrictedDynamic(Box<SExprStr>, RuntimeScope, TypeInfo),
    SUpdate(Box<TypedStructExpr>, EcoString, Box<SExprTE>),
    SGet(Box<TypedStructExpr>, EcoString),
    MGetMap(Box<TypedMapExpr>, EcoString),
    LHeadList(Box<TypedListExpr>),
    LIndexList(Box<TypedListExpr>, Box<SExprInt>),
}

#[derive(Clone, PartialEq, Debug)]
pub struct TypedStructExpr {
    /// Type map for the struct fields
    pub typ_map: EcoVec<(EcoString, TCType)>,
    pub allow_extra_fields: bool,
    pub kind: TypedStructExprKind,
}

impl TypedStructExpr {
    pub fn to_stream_type(&self) -> Option<StreamType> {
        TCType::Struct(self.typ_map.clone(), self.allow_extra_fields).to_stream_type()
    }

    #[requires(self.typ_map == other.typ_map)]
    pub fn typed_default(&self, other: &TypedStructExpr) -> TypedStructExpr {
        TypedStructExpr {
            typ_map: self.typ_map.clone(),
            allow_extra_fields: self.allow_extra_fields,
            kind: TypedStructExprKind::Default(Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    pub fn typed_sindex(&self, index: u64) -> TypedStructExpr {
        TypedStructExpr {
            typ_map: self.typ_map.clone(),
            allow_extra_fields: self.allow_extra_fields,
            kind: TypedStructExprKind::SIndex(Box::new(self.clone()), index),
        }
    }

    #[requires(self.typ_map == other.typ_map)]
    pub fn typed_if(&self, cond: Box<SExprBool>, other: &TypedStructExpr) -> TypedStructExpr {
        TypedStructExpr {
            typ_map: self.typ_map.clone(),
            allow_extra_fields: self.allow_extra_fields,
            kind: TypedStructExprKind::If(cond, Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    #[requires(self.typ_map == other.typ_map)]
    pub fn typed_init(&self, other: &TypedStructExpr) -> TypedStructExpr {
        TypedStructExpr {
            typ_map: self.typ_map.clone(),
            allow_extra_fields: self.allow_extra_fields,
            kind: TypedStructExprKind::Init(Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    #[requires(self.typ_map == other.typ_map)]
    pub fn typed_update_stream(&self, other: &TypedStructExpr) -> TypedStructExpr {
        TypedStructExpr {
            typ_map: self.typ_map.clone(),
            allow_extra_fields: self.allow_extra_fields,
            kind: TypedStructExprKind::Update(Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    #[requires(self.typ_map == other.typ_map)]
    pub fn typed_latch(&self, other: &TypedStructExpr) -> TypedStructExpr {
        TypedStructExpr {
            typ_map: self.typ_map.clone(),
            allow_extra_fields: self.allow_extra_fields,
            kind: TypedStructExprKind::Latch(Box::new(self.clone()), Box::new(other.clone())),
        }
    }

    #[requires(self.typ_map.iter().any(|(k, _)| *k == key))]
    pub fn typed_update(&self, key: EcoString, value: Box<SExprTE>) -> TypedStructExpr {
        TypedStructExpr {
            typ_map: self.typ_map.clone(),
            allow_extra_fields: self.allow_extra_fields,
            kind: TypedStructExprKind::SUpdate(Box::new(self.clone()), key, value),
        }
    }

    #[requires(self.typ_map.iter().any(|(k, _)| *k == key))]
    pub fn typed_get(&self, key: EcoString) -> TypedStructExpr {
        TypedStructExpr {
            typ_map: self.typ_map.clone(),
            allow_extra_fields: self.allow_extra_fields,
            kind: TypedStructExprKind::SGet(Box::new(self.clone()), key),
        }
    }
}

// Stream expression typed enum
#[derive(Debug, PartialEq, Clone)]
pub enum SExprAny {
    Var(VarName),
    Val(Value),
    Expr(SExpr),
}

#[derive(Debug, PartialEq, Clone)]
pub enum SExprTE {
    Int(SExprInt),
    Float(SExprFloat),
    Str(SExprStr),
    Bool(SExprBool),
    Unit(SExprUnit),
    List(TypedListExpr),
    Map(TypedMapExpr),
    Struct(TypedStructExpr),
    Tuple(TypedTupleExpr),
    Function(TypedFunctionExpr),
    Fold(TypedFoldExpr),
    Apply(TypedApplyExpr),
    Any(SExprAny),
}

pub struct SExprTEWithType<'a>(&'a SExprTE);

impl SExprTE {
    pub fn display_with_type(&self) -> SExprTEWithType<'_> {
        SExprTEWithType(self)
    }
}

impl Display for SExprTEWithType<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.0, extract_type(self.0))
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct TypedDsrvSpecification {
    pub input_vars: BTreeSet<VarName>,
    pub output_vars: BTreeSet<VarName>,
    pub aux_vars: BTreeSet<VarName>,
    pub stream_vars: BTreeSet<VarName>,
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
        self.output_vars
            .difference(&self.aux_vars())
            .cloned()
            .collect()
    }

    fn aux_vars(&self) -> BTreeSet<VarName> {
        self.aux_vars.clone()
    }

    fn stream_vars(&self) -> BTreeSet<VarName> {
        self.stream_vars.clone()
    }

    fn var_expr(&self, var: &VarName) -> Option<SExprTE> {
        self.exprs.get(var).cloned()
    }

    fn add_input_var(&mut self, var: VarName) {
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

pub fn extract_type(expr: &SExprTE) -> TCType {
    match expr {
        SExprTE::Bool(_) => TCType::Bool,
        SExprTE::Int(_) => TCType::Int,
        SExprTE::Float(_) => TCType::Float,
        SExprTE::Str(_) => TCType::Str,
        SExprTE::Unit(_) => TCType::Unit,
        SExprTE::List(tl) => tl.list_tc_type(),
        SExprTE::Map(tm) => tm.map_tc_type(),
        SExprTE::Struct(st) => TCType::Struct(st.typ_map.clone(), st.allow_extra_fields),
        SExprTE::Tuple(tuple) => tuple.tuple_tc_type(),
        SExprTE::Function(func) => TCType::Function(
            func.params.iter().map(|(_, typ)| typ.clone()).collect(),
            Box::new(func.return_type.clone()),
        ),
        SExprTE::Fold(fold) => fold.result_type.clone(),
        SExprTE::Apply(apply) => apply.result_type.clone(),
        SExprTE::Any(_) => TCType::Any,
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
            SExprTE::List(e) => write!(f, "{}", e),
            SExprTE::Map(e) => write!(f, "{}", e),
            SExprTE::Struct(e) => write!(f, "{}", e),
            SExprTE::Tuple(e) => write!(f, "{}", e),
            SExprTE::Function(e) => write!(f, "{}", e),
            SExprTE::Fold(e) => write!(f, "{}", e),
            SExprTE::Apply(e) => write!(f, "{}", e),
            SExprTE::Any(e) => write!(f, "{}", e),
        }
    }
}

impl Display for TypedFunctionExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let params = self
            .params
            .iter()
            .map(|(name, typ)| format!("{}: {}", name, typ))
            .join(", ");
        write!(f, "\\{} -> {}", params, self.body)
    }
}

impl Display for TypedFoldExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "List.fold({}, {}, {})", self.func, self.init, self.list)
    }
}

impl Display for TypedApplyExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let args = self.args.iter().map(|arg| format!("{}", arg)).join(", ");
        write!(f, "{}({})", self.func, args)
    }
}

impl Display for SExprAny {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SExprAny::Var(v) => write!(f, "{}", v),
            SExprAny::Val(v) => write!(f, "{}", v),
            SExprAny::Expr(e) => write!(f, "{}", SpannedExpr::from(e.clone())),
        }
    }
}

impl Display for TypedListExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use TypedListExprKind::*;
        match &self.kind {
            If(b, e1, e2) => write!(f, "(if {} then {} else {})", b, e1, e2),
            SIndex(e, i) => write!(f, "{}[{}]", e, i),
            Var(v) => write!(f, "{}", v),
            Default(e, v) => write!(f, "default({}, {})", e, v),
            Update(e1, e2) => write!(f, "update({}, {})", e1, e2),
            Latch(e1, e2) => write!(f, "latch({}, {})", e1, e2),
            Init(e1, e2) => write!(f, "init({}, {})", e1, e2),
            Defer(e, _, _) => write!(f, "defer({}: {})", e, self.list_tc_type()),
            Dynamic(e, _) => write!(f, "dynamic({}: {})", e, self.list_tc_type()),
            RestrictedDynamic(e, env, _) => {
                let env = env.iter().map(|v| format!("{}", v)).join(", ");
                write!(f, "dynamic({}: {}, {{{}}})", e, self.list_tc_type(), env)
            }
            Literal(items) => {
                let items = items.iter().map(|e| format!("{}", e)).join(", ");
                write!(f, "[{}]", items)
            }
            LTail(e) => write!(f, "List.tail({})", e),
            LConcat(e1, e2) => write!(f, "List.concat({}, {})", e1, e2),
            LAppend(e, v) => write!(f, "List.append({}, {})", e, v),
            LMap(func, list) => write!(f, "List.map({}, {})", func, list),
            LFilter(func, list) => write!(f, "List.filter({}, {})", func, list),
            LHeadList(e) => write!(f, "List.head({})", e),
            LIndexList(e, i) => write!(f, "List.get({}, {})", e, i),
            MGetMap(e, key) => write!(f, "Map.get({}, {:?})", e, key),
            SGetStruct(e, key) => write!(f, "{}.{}", e, key),
            SGetTuple(e, idx) => write!(f, "{}.{}", e, idx),
        }
    }
}

impl Display for TypedMapExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use TypedMapExprKind::*;
        match &self.kind {
            Var(v) => write!(f, "{}", v),
            Literal(entries) => {
                let entries = entries
                    .iter()
                    .map(|(k, v)| format!("{:?}: {}", k, v))
                    .join(", ");
                write!(f, "Map({})", entries)
            }
            Default(e, v) => write!(f, "default({}, {})", e, v),
            If(b, e1, e2) => write!(f, "(if {} then {} else {})", b, e1, e2),
            Update(e1, e2) => write!(f, "update({}, {})", e1, e2),
            Latch(e1, e2) => write!(f, "latch({}, {})", e1, e2),
            Init(e1, e2) => write!(f, "init({}, {})", e1, e2),
            SIndex(e, i) => write!(f, "{}[{}]", e, i),
            Defer(e, _, _) => write!(f, "defer({}: {})", e, self.map_tc_type()),
            Dynamic(e, _) => write!(f, "dynamic({}: {})", e, self.map_tc_type()),
            RestrictedDynamic(e, env, _) => {
                let env = env.iter().map(|v| format!("{}", v)).join(", ");
                write!(f, "dynamic({}: {}, {{{}}})", e, self.map_tc_type(), env)
            }
            MInsert(e, key, v) => write!(f, "Map.insert({}, {:?}, {})", e, key, v),
            MRemove(e, key) => write!(f, "Map.remove({}, {:?})", e, key),
            MGetMap(e, key) => write!(f, "Map.get({}, {:?})", e, key),
            SGetStruct(e, key) => write!(f, "{}.{}", e, key),
            SGetTuple(e, idx) => write!(f, "{}.{}", e, idx),
            LHeadList(e) => write!(f, "List.head({})", e),
            LIndexList(e, i) => write!(f, "List.get({}, {})", e, i),
        }
    }
}

impl Display for TypedTupleExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use TypedTupleExprKind::*;
        match &self.kind {
            Var(v) => write!(f, "{}", v),
            Literal(items) => {
                let items = items.iter().map(|e| format!("{}", e)).join(", ");
                write!(f, "Tuple({})", items)
            }
            Default(e, v) => write!(f, "default({}, {})", e, v),
            If(b, e1, e2) => write!(f, "(if {} then {} else {})", b, e1, e2),
            Update(e1, e2) => write!(f, "update({}, {})", e1, e2),
            Latch(e1, e2) => write!(f, "latch({}, {})", e1, e2),
            Init(e1, e2) => write!(f, "init({}, {})", e1, e2),
            SIndex(e, i) => write!(f, "{}[{}]", e, i),
            TGetTuple(e, idx) => write!(f, "{}.{}", e, idx),
        }
    }
}

impl Display for TypedStructExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use TypedStructExprKind::*;
        let typ = TCType::Struct(self.typ_map.clone(), self.allow_extra_fields);
        match &self.kind {
            Var(v) => write!(f, "{}", v),
            Literal(entries) => {
                let entries = entries
                    .iter()
                    .map(|(k, v)| format!("{:?}: {}", k, v))
                    .join(", ");
                write!(f, "Struct({})", entries)
            }
            Default(e, v) => write!(f, "default({}, {})", e, v),
            If(b, e1, e2) => write!(f, "(if {} then {} else {})", b, e1, e2),
            Update(e1, e2) => write!(f, "update({}, {})", e1, e2),
            Latch(e1, e2) => write!(f, "latch({}, {})", e1, e2),
            Init(e1, e2) => write!(f, "init({}, {})", e1, e2),
            SIndex(e, i) => write!(f, "{}[{}]", e, i),
            Defer(e, _, _) => write!(f, "defer({}: {})", e, typ),
            Dynamic(e, _) => write!(f, "dynamic({}: {})", e, typ),
            RestrictedDynamic(e, env, _) => {
                let env = env.iter().map(|v| format!("{}", v)).join(", ");
                write!(f, "dynamic({}: {}, {{{}}})", e, typ, env)
            }
            SUpdate(e, key, v) => write!(f, "Map.insert({}, {:?}, {})", e, key, v),
            SGet(e, key) => write!(f, "{}.{}", e, key),
            MGetMap(e, key) => write!(f, "Map.get({}, {:?})", e, key),
            LHeadList(e) => write!(f, "List.head({})", e),
            LIndexList(e, i) => write!(f, "List.get({}, {})", e, i),
        }
    }
}

impl Display for SExprInt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use IntBinOp::*;
        use SExprInt::*;
        match self {
            Cast(e) => write!(f, "{}", e),
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
            Update(e1, e2) => write!(f, "update({}, {})", e1, e2),
            Latch(e1, e2) => write!(f, "latch({}, {})", e1, e2),
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
            LLen(list) => write!(f, "List.len({})", list),
            LHeadList(list) => write!(f, "List.head({})", list),
            LIndexList(list, idx) => write!(f, "List.get({}, {})", list, idx),
            MGetMap(map, key) => write!(f, "Map.get({}, {:?})", map, key),
            SGetStruct(st, key) => write!(f, "{}.{}", st, key),
            SGetTuple(tuple, idx) => write!(f, "{}.{}", tuple, idx),
        }
    }
}

impl Display for SExprFloat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use FloatBinOp::*;
        use SExprFloat::*;
        match self {
            Cast(e) => write!(f, "{}", e),
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
            Update(e1, e2) => write!(f, "update({}, {})", e1, e2),
            Latch(e1, e2) => write!(f, "latch({}, {})", e1, e2),
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
            LHeadList(list) => write!(f, "List.head({})", list),
            LIndexList(list, idx) => write!(f, "List.get({}, {})", list, idx),
            MGetMap(map, key) => write!(f, "Map.get({}, {:?})", map, key),
            SGetStruct(st, key) => write!(f, "{}.{}", st, key),
            SGetTuple(tuple, idx) => write!(f, "{}.{}", tuple, idx),
        }
    }
}

impl Display for SExprStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use SExprStr::*;
        match self {
            Cast(e) => write!(f, "{}", e),
            If(b, e1, e2) => write!(f, "(if {} then {} else {})", b, e1, e2),
            SIndex(s, i) => write!(f, "{}[{}]", s, i),
            BinOp(e1, e2, StrBinOp::Concat) => write!(f, "({} ++ {})", e1, e2),
            Val(v) => write!(f, "{}", v),
            Var(v) => write!(f, "{}", v),
            Default(e, v) => write!(f, "default({}, {})", e, v),
            Update(e1, e2) => write!(f, "update({}, {})", e1, e2),
            Latch(e1, e2) => write!(f, "latch({}, {})", e1, e2),
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
            LHeadList(list) => write!(f, "List.head({})", list),
            LIndexList(list, idx) => write!(f, "List.get({}, {})", list, idx),
            MGetMap(map, key) => write!(f, "Map.get({}, {:?})", map, key),
            SGetStruct(st, key) => write!(f, "{}.{}", st, key),
            SGetTuple(tuple, idx) => write!(f, "{}.{}", tuple, idx),
        }
    }
}

impl Display for SExprUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use SExprUnit::*;
        match self {
            Cast(e) => write!(f, "{}", e),
            If(b, e1, e2) => write!(f, "(if {} then {} else {})", b, e1, e2),
            SIndex(s, i) => write!(f, "{}[{}]", s, i),
            Val(v) => write!(f, "{:?}", v),
            Var(v) => write!(f, "{}", v),
            Default(e, v) => write!(f, "default({}, {})", e, v),
            Update(e1, e2) => write!(f, "update({}, {})", e1, e2),
            Latch(e1, e2) => write!(f, "latch({}, {})", e1, e2),
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
            LHeadList(list) => write!(f, "List.head({})", list),
            LIndexList(list, idx) => write!(f, "List.get({}, {})", list, idx),
            MGetMap(map, key) => write!(f, "Map.get({}, {:?})", map, key),
            SGetStruct(st, key) => write!(f, "{}.{}", st, key),
            SGetTuple(tuple, idx) => write!(f, "{}.{}", tuple, idx),
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
            Cast(e) => write!(f, "{}", e),

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
            Update(e1, e2) => write!(f, "update({}, {})", e1, e2),
            Latch(e1, e2) => write!(f, "latch({}, {})", e1, e2),
            Init(e1, e2) => write!(f, "init({}, {})", e1, e2),

            IsDefined(sexpr) => write!(f, "is_defined({})", sexpr),
            When(sexpr) => write!(f, "when({})", sexpr),
            LHeadList(list) => write!(f, "List.head({})", list),
            LIndexList(list, idx) => write!(f, "List.get({}, {})", list, idx),
            MGetMap(map, key) => write!(f, "Map.get({}, {:?})", map, key),
            SGetStruct(st, key) => write!(f, "{}.{}", st, key),
            SGetTuple(tuple, idx) => write!(f, "{}.{}", tuple, idx),
            MHasKeyMap(map, key) => write!(f, "Map.has_key({}, {:?})", map, key),

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
