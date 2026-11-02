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
//!
//! Checking accepts an `Any` where a concrete type is needed, and records
//! that the value must be checked when it runs. The pass whose types are
//! kept (strict checking, or gradual checking's final pass) records, in
//! `RuntimeChecks`, each node of type `Any` it accepted at a concrete type:
//! where a value is checked against an expectation, where a binary operator
//! settles its operands' type, and where a call's argument meets its
//! parameter. One helper in the checker does all recording, and a
//! speculative check that fails drops what it recorded. Elaboration wraps
//! each recorded node in a run-time check, and runtimes stop at the first
//! data value that fails one with a [`RuntimeTypeError`]. Only types whose
//! values can be checked are recorded: scalars, and lists, maps, tuples and
//! structs of them or of `Any`.

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
