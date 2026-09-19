//! DSRV syntax, parsers, and type checking.

pub mod ast;
mod expand;
pub mod parser;
mod pipeline;
pub mod source;
pub mod span;
mod syntax;
pub mod type_checker;

#[cfg(test)]
pub(crate) mod test_support;

pub use parser::DsrvParseError;
pub use pipeline::{DsrvPipelineError, TypeCheckMode, TypeCheckOptions};
#[cfg(test)]
pub(crate) use pipeline::{reset_test_pipeline_counts, test_pipeline_counts};
