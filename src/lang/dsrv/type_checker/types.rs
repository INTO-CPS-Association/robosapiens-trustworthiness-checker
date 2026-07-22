//! Checker types and the stream type environment retained by checked DSRV syntax.

use ecow::{EcoString, EcoVec};
use itertools::Itertools;

use crate::VarName;
use crate::core::StreamType;
use std::collections::BTreeMap;

/// Global environment mapping stream variables to their declared or inferred types.
///
/// Lambda-local bindings are layered separately while checking an expression.
pub type StreamTypeEnvironment = BTreeMap<VarName, StreamType>;
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
