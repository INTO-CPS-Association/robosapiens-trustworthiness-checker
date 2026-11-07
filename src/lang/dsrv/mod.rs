//! DSRV syntax, parsing, validation and type checking.

pub mod ast;
pub mod catalogue;
pub mod diagnostics;
mod elaborate;
pub(crate) mod expand;
pub mod inspection;
pub mod modules;
pub mod parser;
pub mod path;
pub mod patterns;
mod pipeline;
pub mod program;
pub(crate) mod runtime_expression;
pub mod source;
pub mod source_map;
pub mod span;
mod syntax;
pub mod type_checker;
pub mod validation;

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;

pub use elaborate::ElaboratedDsrvSpecification;
pub use expand::language::{
    CoreDsrvSpecification, Dialect, Feature, IfPolicy, LanguageConfig, LanguageError, MetaFeature,
};
pub use parser::{DsrvParseError, check_core_source};
