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
// Structural rewriting into shared expression forests.
pub(crate) mod rewrite;

// Expression formatting.
mod display;

#[cfg(test)]
pub(crate) mod generation;

pub(crate) use checked::ExprCursor;
pub use checked::{CheckedExpr, CheckedExprRef};
pub use contiguous_tree::TreeCursorExt;
pub use expression::{DynamicExprScope, Expr, ExprId, ExprRef, ExprView, VarOrNodeName};
pub(crate) use expression::{ExprArena, ExprBuilder, ExprFieldRefs, ExprKind, ExprRefs};
pub use operators::{BoolBinOp, CompBinOp, FloatBinOp, IntBinOp, NumericalBinOp, SBinOp, StrBinOp};
pub(crate) use rewrite::{RewriteForestError, rewrite_forest};

pub use specification::{CheckedDsrvSpecification, DsrvSpecification};
