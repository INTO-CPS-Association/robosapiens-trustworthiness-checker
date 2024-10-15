use std::fmt::Display;

use crate::core::{StreamData, TypeSystem, Value};

pub struct LOLATypeSystem;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LOLATypedValue {
    Int(i64),
    Str(String),
    Bool(bool),
    Unit,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum StreamType {
    Int,
    Str,
    Bool,
    Unit,
}

impl Display for LOLATypedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LOLATypedValue::Int(i) => write!(f, "{}", i),
            LOLATypedValue::Str(s) => write!(f, "{}", s),
            LOLATypedValue::Bool(b) => write!(f, "{}", b),
            LOLATypedValue::Unit => write!(f, "()"),
        }
    }
}

pub struct BoolTypeSystem;
impl TypeSystem for BoolTypeSystem {
    type Type = StreamType;
    type TypedValue = bool;

    fn type_of_value(value: &Self::TypedValue) -> Self::Type {
        StreamType::Bool
    }
}

impl TypeSystem for LOLATypeSystem {
    type Type = StreamType;
    type TypedValue = LOLATypedValue;

    fn type_of_value(value: &Self::TypedValue) -> Self::Type {
        match value {
            LOLATypedValue::Int(_) => StreamType::Int,
            LOLATypedValue::Str(_) => StreamType::Str,
            LOLATypedValue::Bool(_) => StreamType::Bool,
            LOLATypedValue::Unit => StreamType::Unit,
        }
    }
}

// Trait defining the allowed types for expression values
impl StreamData for i64 {}
impl Value<LOLATypeSystem> for i64 {
    fn type_of(&self) -> <LOLATypeSystem as TypeSystem>::Type {
        StreamType::Int
    }

    fn to_typed_value(&self) -> <LOLATypeSystem as TypeSystem>::TypedValue {
        LOLATypedValue::Int(*self)
    }

    fn from_typed_value(value: <LOLATypeSystem as TypeSystem>::TypedValue) -> Option<Self> {
        match value {
            LOLATypedValue::Int(i) => Some(i),
            _ => None,
        }
    }
}

impl StreamData for String {}
impl Value<LOLATypeSystem> for String {
    fn type_of(&self) -> <LOLATypeSystem as TypeSystem>::Type {
        StreamType::Str
    }

    fn to_typed_value(&self) -> <LOLATypeSystem as TypeSystem>::TypedValue {
        LOLATypedValue::Str(self.clone())
    }

    fn from_typed_value(value: <LOLATypeSystem as TypeSystem>::TypedValue) -> Option<Self> {
        match value {
            LOLATypedValue::Str(s) => Some(s),
            _ => None,
        }
    }
}
impl StreamData for bool {}
impl Value<LOLATypeSystem> for bool {
    fn type_of(&self) -> <LOLATypeSystem as TypeSystem>::Type {
        StreamType::Bool
    }

    fn to_typed_value(&self) -> <LOLATypeSystem as TypeSystem>::TypedValue {
        LOLATypedValue::Bool(*self)
    }

    fn from_typed_value(value: <LOLATypeSystem as TypeSystem>::TypedValue) -> Option<Self> {
        match value {
            LOLATypedValue::Bool(b) => Some(b),
            _ => None,
        }
    }
}
impl StreamData for () {}
impl Value<LOLATypeSystem> for () {
    fn type_of(&self) -> <LOLATypeSystem as TypeSystem>::Type {
        StreamType::Int
    }

    fn to_typed_value(&self) -> <LOLATypeSystem as TypeSystem>::TypedValue {
        LOLATypedValue::Unit
    }

    fn from_typed_value(value: <LOLATypeSystem as TypeSystem>::TypedValue) -> Option<Self> {
        match value {
            LOLATypedValue::Unit => Some(()),
            _ => None,
        }
    }
}
