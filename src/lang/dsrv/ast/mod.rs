//! DSRV syntax, specifications, and expression APIs.

// Operators used by expression nodes.
mod operators;
// Core nodes and expression handles.
mod expression;
// Type-checking decorates the shared expression representation.
mod checked;

// Specifications and their checked metadata.
mod specification;

// Semantic queries over expressions.
mod analysis;

// Expression formatting.
mod display;

#[cfg(test)]
pub(crate) mod generation;

pub(crate) use checked::TypeAnnotations;
pub use checked::{CheckedExpr, CheckedExprRef};
pub use contiguous_tree::TreeCursorExt;
pub use expression::*;
pub use operators::{BoolBinOp, CompBinOp, FloatBinOp, IntBinOp, NumericalBinOp, SBinOp, StrBinOp};
pub use specification::*;
