//! DSRV syntax, parsers, and type checking.

pub mod ast;
mod elaborate;
mod expand;
pub mod parser;
pub mod patterns;
mod pipeline;
pub(crate) mod runtime_text;
pub mod source;
pub mod span;
mod syntax;
pub mod type_checker;

#[cfg(test)]
mod generic_tests;
#[cfg(test)]
mod match_tests;
#[cfg(test)]
mod tagged_union_tests;
#[cfg(test)]
pub(crate) mod test_support;

pub use elaborate::ElaboratedDsrvSpecification;
pub use expand::language::{
    CoreDsrvSpecification, Dialect, Edition, Feature, LanguageConfig, LanguageError,
    LanguageRequest,
};
pub use parser::{DsrvParseError, check_core_source};
pub use pipeline::{DsrvPipelineError, TypeCheckMode, TypeCheckOptions};
#[cfg(test)]
pub(crate) use pipeline::{reset_test_pipeline_counts, test_pipeline_counts};
