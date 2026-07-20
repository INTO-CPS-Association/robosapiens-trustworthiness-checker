//! Type checking for DSRV specifications.
//!
//! Inferred node types are stored as immutable metadata indexed by expression ID.
//!
//! - `strict`: the strict `type_check` driver, which requires type
//!   annotations on all variables
//! - `gradual`: the gradual `type_check_gradual` driver, which infers types
//!   for unannotated variables and falls back to `Any`
//! - `types`: the checker type representation and node annotations
//! - `errors`: structured semantic and type-checking errors
//! - `validation`: AST validation and type extraction for runtime values

mod checker;
mod errors;
mod gradual;
mod strict;
mod types;
mod validation;

#[cfg(test)]
mod property_tests;

pub use checker::type_check_expression;
pub use errors::*;
pub use gradual::*;
pub use strict::*;
pub use types::*;
pub use validation::*;

pub(crate) use checker::check_expression;
