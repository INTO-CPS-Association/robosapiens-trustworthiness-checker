//! DSRV syntax, parsers, and type checking.

pub mod ast;
pub mod diagnostics;
mod elaborate;
pub(crate) mod expand;
pub mod modules;
pub mod parser;
pub mod path;
pub mod patterns;
mod pipeline;
pub(crate) mod runtime_text;
pub mod source;
pub mod span;
mod syntax;
pub mod type_checker;

#[cfg(test)]
mod cast_tests;
#[cfg(test)]
mod constant_tests;
#[cfg(test)]
mod generic_tests;
#[cfg(test)]
mod match_tests;
#[cfg(test)]
mod module_tests;
#[cfg(test)]
mod tagged_union_tests;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod warning_tests;

pub use elaborate::ElaboratedDsrvSpecification;
pub use expand::language::{
    CoreDsrvSpecification, Dialect, Edition, Feature, LanguageConfig, LanguageError,
    LanguageRequest,
};
pub use parser::{DsrvParseError, check_core_source};
pub use pipeline::{TypeCheckMode, TypeCheckOptions};
#[cfg(test)]
pub(crate) use pipeline::{reset_test_pipeline_counts, test_pipeline_counts};
