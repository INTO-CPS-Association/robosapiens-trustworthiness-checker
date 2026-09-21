//! Type checking for DSRV specifications.
//!
//! Inferred node types are stored as immutable metadata indexed by expression ID.
//!
//! - `strict`: the strict driver, which requires type annotations on all
//!   variables
//! - `gradual`: the gradual driver, which infers types for unannotated
//!   variables and falls back to `Any`
//! - `types`: the checker type representation and node annotations
//! - `warnings`: the warnings one checking attempt collects
//! - `validation`: AST validation and type extraction for runtime values
//!
//! Every driver returns a
//! [`SemanticAnalysisReport`](crate::lang::dsrv::diagnostics::SemanticAnalysisReport);
//! the diagnostics themselves live in [`diagnostics`](crate::lang::dsrv::diagnostics).

mod checker;
mod gradual;
mod strict;
mod types;
mod validation;
mod warnings;

#[cfg(test)]
mod property_tests;

pub use checker::type_check_expression;
pub use types::*;
pub use validation::*;

pub(crate) use checker::check_expression;
pub(crate) use gradual::{check_validated_gradual, type_check_gradual};
pub(crate) use strict::{check_validated_strict, type_check};
