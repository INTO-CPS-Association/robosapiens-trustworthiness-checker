//! DSRV syntax, parsers, and type checking.

pub mod ast;
pub mod parser;
mod pipeline;
pub mod span;
pub mod type_checker;

#[cfg(test)]
pub(crate) mod test_support;

pub use parser::DsrvParseError;
pub use pipeline::{DsrvPipelineError, TypeCheckMode, TypeCheckOptions};
