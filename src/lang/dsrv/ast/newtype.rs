//! The two newtype operations as a specification writes them.
//!
//! `Name::new(x)` builds a value of the newtype `Name` from its
//! representation without validating it, and `Name::value(x)` reads the
//! representation back. Only the module that declares `Name` may write
//! either; everywhere else, including text supplied at run time to a
//! `dynamic` or `defer` node, uses the functions that module exports.
//!
//! Both are kept as [`NewtypeOperation`] nodes through checking, which is
//! where a generic newtype's arguments are inferred. Elaboration then lowers
//! them to the run-time `Wrap` and `Unwrap` nodes, which name one concrete
//! instance each, so no runtime sees an operation whose newtype is
//! unsettled.

use std::fmt;

use ecow::EcoString;

use crate::core::{NewtypeSchema, StreamType, brand_stem};
use crate::lang::dsrv::path::TypePath;

/// Which of the two operations a node is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub enum NewtypeOperationKind {
    /// `Name::new(x)`: brand a representation.
    New,
    /// `Name::value(x)`: read a representation back.
    Value,
}

impl NewtypeOperationKind {
    /// The operation as it is spelled after the newtype's name.
    pub fn name(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::Value => "value",
        }
    }

    /// The spelling an operation has, if `tag` is one.
    pub(crate) fn from_name(tag: &str) -> Option<Self> {
        match tag {
            "new" => Some(Self::New),
            "value" => Some(Self::Value),
            _ => None,
        }
    }
}

/// Which newtype an operation's qualifier names.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub enum NewtypeTarget {
    /// One instance: a newtype without parameters, or an alias of an
    /// applied generic one.
    Instance(NewtypeSchema<StreamType>),
    /// A generic newtype, whose arguments checking infers from the type the
    /// operation is expected to have or is given.
    Generic { stem: EcoString, arity: usize },
}

impl NewtypeTarget {
    /// The canonical path of the declaration the target belongs to.
    pub fn stem(&self) -> &str {
        match self {
            Self::Instance(schema) => brand_stem(schema.brand()),
            Self::Generic { stem, .. } => stem,
        }
    }

    /// Whether `schema` is an instance of this target.
    pub fn admits(&self, schema: &NewtypeSchema<impl Clone>) -> bool {
        match self {
            Self::Instance(instance) => instance.brand() == schema.brand(),
            Self::Generic { stem, .. } => schema.stem() == stem.as_str(),
        }
    }
}

/// `Name::new(x)` or `Name::value(x)`, with the newtype its qualifier
/// resolved to.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub struct NewtypeOperation {
    pub kind: NewtypeOperationKind,
    /// The qualifier as written, which is how the node prints.
    pub written: TypePath,
    pub target: NewtypeTarget,
}

impl fmt::Display for NewtypeOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}::{}", self.written, self.kind.name())
    }
}
