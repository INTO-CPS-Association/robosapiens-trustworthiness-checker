//! Type checking for DSRV specifications.
//!
//! The typed AST produced by the checker lives in `crate::lang::dsrv::typed_ast`
//! and is re-exported here for convenience. This module contains the checking
//! algorithms themselves:
//!
//! - `expr`: expression-level type checking shared by both drivers
//! - `strict`: the strict `type_check` driver, which requires type
//!   annotations on all variables
//! - `gradual`: the gradual `type_check_gradual` driver, which infers types
//!   for unannotated variables and falls back to `Any`
//! - `types`: semantic errors, the type-checking traits, and runtime
//!   validation of values against types

mod expr;
mod gradual;
mod strict;
mod types;

pub use gradual::*;
pub use strict::*;
pub use types::*;

// Re-exported for backwards compatibility: these items previously lived in
// this module.
pub use super::typed_ast::*;
pub use crate::core::PartialStreamValue;
