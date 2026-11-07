//! Deterministic checking with concrete stream types.
//!
//! Each expression is visited once with the concrete information available at
//! that point. Declared generic parameters are substituted from required results
//! and source-order arguments. Empty shapes and lambda inputs need concrete
//! context; differing contributions need an explicit common type.
//! Checking leaves syntax unchanged and publishes only complete node annotations.
mod checking;
mod diagnostics;
mod instantiation;
mod relations;
mod rules;
mod runtime_checks;
mod specification;
#[cfg(test)]
mod tests;
mod type_contexts;
mod types;
mod typing_support;

use self::{checking::check, diagnostics::WarningCollector, type_contexts::TypeContext};
use super::{
    ast::{AstShared, CheckedDsrvSpecification, CheckedExpr, Expr, ValidatedDsrvSpecification},
    diagnostics::{SemanticAnalysisReport, SemanticResult},
    source_map::SourceArchive,
};
use crate::core::StreamType;
use contiguous_tree::TreeCursorExt;
pub use runtime_checks::check_value_stream_type;
pub use types::*;

/// Infer and check types after the pipeline has validated the specification.
pub(crate) fn type_check(
    spec: ValidatedDsrvSpecification,
) -> SemanticAnalysisReport<CheckedDsrvSpecification> {
    let mut spec = spec.into_specification();
    let mut warnings = WarningCollector::default();
    let sources = AstShared::clone(spec.sources());
    let result = (|| {
        let mut environment = spec.type_annotations().clone();
        let types = specification::check_streams(&spec, &mut environment, &mut warnings)?;
        spec.type_annotations = environment;
        Ok(CheckedDsrvSpecification::new(spec, types))
    })();
    warnings.finish_report(result, Some(&sources))
}
pub(crate) fn check_expression(
    expr: Expr,
    expected: &StreamType,
    environment: &AstShared<StreamTypes>,
    sources: Option<&SourceArchive>,
) -> SemanticAnalysisReport<CheckedExpr> {
    let mut warnings = WarningCollector::default();
    let result = check_expression_with(expr, expected, environment, &mut warnings);
    warnings.finish_report(result, sources)
}
pub(crate) fn check_and_elaborate_expression(
    expr: Expr,
    expected: &StreamType,
    environment: &AstShared<StreamTypes>,
    sources: Option<&SourceArchive>,
) -> SemanticAnalysisReport<CheckedExpr> {
    check_expression(expr, expected, environment, sources).map_checked(CheckedExpr::elaborate)
}
fn check_expression_with(
    expr: Expr,
    expected: &StreamType,
    environment: &AstShared<StreamTypes>,
    warnings: &mut WarningCollector,
) -> SemanticResult<CheckedExpr> {
    super::validation::validate_standalone_expression(
        expr.as_ref(),
        &environment.keys().cloned().collect(),
    )?;
    let mut context = TypeContext::new(environment, warnings);
    check(expr.as_ref(), Some(expected), &mut context).map_err(|error| vec![error])?;
    let mut annotations = expr.annotations_builder();
    for node in expr.as_ref().postorder() {
        annotations
            .insert(node, context.expr_types[&node.id()].clone())
            .expect("checked node belongs to tree");
    }
    let types = annotations
        .finish()
        .expect("successful checking records every node");
    Ok(CheckedExpr::new(expr, types, AstShared::clone(environment)))
}
/// Check an expression against a known concrete type and stream environment.
pub fn type_check_expression(
    expr: &Expr,
    expected: &StreamType,
    environment: &StreamTypes,
) -> SemanticAnalysisReport<CheckedExpr> {
    check_expression(
        expr.clone(),
        expected,
        &AstShared::new(environment.clone()),
        None,
    )
}
