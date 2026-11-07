//! Language pipeline from DSRV source to a prepared and checked specification ready for the
//! semantics/runtime.
//!
//! Stages run in this order, stopping at the first failure:
//!
//! | # | Stage | Result and guarantee |
//! |---|---|---|
//! | 1 | Syntax ([`syntax`]) | Parsed declarations and expressions. |
//! | 2 | Expansion ([`expand`]) | [`DsrvSpecification`]: type names resolved, constants folded and `def` calls inlined. |
//! | 3 | Validation ([`validation`]) | [`ValidatedDsrvSpecification`]: declarations, bindings and runtime scopes valid. |
//! | 4 | Type checking ([`type_checker`]) | [`CheckedDsrvSpecification`]: a concrete type for every expression node. |
//! | 5 | Elaboration ([`elaborate`]) | [`ElaboratedDsrvSpecification`]: execution rewrites applied and runtime-expression sites prepared. |
//!
//! Parsing and expansion failures return Err. Once those stages succeed, the returned report
//! contains the validation/type-checking result and any warnings.
//! [`SemanticAnalysisReport`] contains a complete report of checking errors and warnings;
//! this provides the diagnostic information provided via the commandline
//! and the LSP.

use std::str::FromStr;

use super::{
    ast::{
        AstShared, CheckedDsrvSpecification, DsrvSpecification, Expr, ValidatedDsrvSpecification,
    },
    diagnostics::{SemanticAnalysisReport, SemanticResult},
    elaborate::{self, ElaboratedDsrvSpecification},
    expand::{self, language::Dialect},
    modules::ModuleCollector,
    parser::{self, DsrvParseError},
    source::SourceContext,
    source_map::{SourceArchive, SourceId, SourceLabel},
    syntax, type_checker, validation,
};

/// Build an unchecked core specification from source.
pub(super) fn parse_and_expand_specification(
    source: &str,
    label: SourceLabel,
    requested_dialect: Option<Dialect>,
) -> Result<DsrvSpecification, DsrvParseError> {
    let (parsed, archive) = syntax::parse_archived_specification(source, label.clone())?;
    if parser::needs_module_collection(&parsed) {
        let modules = ModuleCollector::with_label(source, label)?.finish()?;
        Ok(expand::expand_program(modules, requested_dialect)?)
    } else {
        Ok(expand::expand_specification(
            parsed,
            requested_dialect,
            AstShared::new(archive),
        )?)
    }
}

impl DsrvSpecification {
    /// Establish declaration, binding and runtime-scope invariants.
    pub fn validate(self) -> SemanticResult<ValidatedDsrvSpecification> {
        validation::validate(self)
    }

    /// Validate and type-check, reporting inferred types and warnings.
    pub fn check(self) -> SemanticAnalysisReport<CheckedDsrvSpecification> {
        #[cfg(test)]
        super::test_support::record_check_entry();
        match self.validate() {
            Ok(validated) => type_checker::type_check(validated),
            Err(errors) => SemanticAnalysisReport::new(Err(errors), Vec::new()),
        }
    }
}

impl ValidatedDsrvSpecification {
    /// Type-check a validated specification and report warnings.
    pub fn check(self) -> SemanticAnalysisReport<CheckedDsrvSpecification> {
        #[cfg(test)]
        super::test_support::record_check_entry();
        type_checker::type_check(self)
    }
}

impl CheckedDsrvSpecification {
    /// Produce the execution tree and prepare its runtime-expression sites.
    pub fn elaborate(self) -> ElaboratedDsrvSpecification {
        elaborate::elaborate_specification(self)
    }
}

// Composed entry points.

impl FromStr for DsrvSpecification {
    type Err = DsrvParseError;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        #[cfg(test)]
        super::test_support::record_parse_entry();
        parser::parse_str(source)
    }
}

impl DsrvSpecification {
    /// Validate, type-check and elaborate, preserving semantic warnings.
    pub fn check_and_elaborate(self) -> SemanticAnalysisReport<ElaboratedDsrvSpecification> {
        self.check()
            .map_checked(CheckedDsrvSpecification::elaborate)
    }
}

impl CheckedDsrvSpecification {
    /// Return a checked specification and warnings from source.
    pub fn parse(source: &str) -> Result<SemanticAnalysisReport<Self>, DsrvParseError> {
        Ok(source.parse::<DsrvSpecification>()?.check())
    }
}

impl ElaboratedDsrvSpecification {
    /// Return an execution-ready specification and warnings from source.
    pub fn parse(source: &str) -> Result<SemanticAnalysisReport<Self>, DsrvParseError> {
        CheckedDsrvSpecification::parse(source)
            .map(|report| report.map_checked(CheckedDsrvSpecification::elaborate))
    }
}

/// Expand expression source using the supplied type and callable contexts.
pub(super) fn parse_and_expand_expression(
    source: &str,
    context: AstShared<SourceContext>,
    callable: AstShared<expand::functions::Callable>,
    located: Option<(&SourceArchive, SourceId)>,
) -> Result<Expr, DsrvParseError> {
    let parsed = syntax::parse_expression(source)?;
    Ok(expand::expand_expression(
        &parsed, &context, &callable, located,
    )?)
}
