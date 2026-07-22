//! DSRV syntax, parsers, and type checking.

pub mod ast;
pub mod parser;
mod pipeline;
pub mod span;
pub mod type_checker;

pub use parser::DsrvParseError;
pub use pipeline::{DsrvPipelineError, TypeCheckMode, TypeCheckOptions};
