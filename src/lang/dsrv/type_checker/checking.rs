//! One syntax-directed visit with concrete expectations and admission.
use super::diagnostics::{RejectedFlow, annotation_needed, mismatch};
use super::relations::flow;
use super::rules;
use super::type_contexts::TypeContext;
use crate::core::StreamType;
use crate::lang::dsrv::{ast::ExprRef, diagnostics::SemanticError};
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Expected {
    Required,
    Hint,
}
pub(super) fn check(
    expr: ExprRef<'_>,
    expected: Option<&StreamType>,
    context: &mut TypeContext<'_>,
) -> Result<StreamType, SemanticError> {
    check_with_mismatch_error(expr, expected, context, |rejected| mismatch(expr, rejected))
}
pub(super) fn check_with_mismatch_error(
    expr: ExprRef<'_>,
    expected: Option<&StreamType>,
    context: &mut TypeContext<'_>,
    make_error: impl FnOnce(RejectedFlow<'_>) -> SemanticError,
) -> Result<StreamType, SemanticError> {
    let actual = type_and_record(expr, expected, Expected::Required, context)?;
    if let Some(expected) = expected {
        flow(&actual, expected).map_err(|why| {
            make_error(RejectedFlow {
                actual: &actual,
                expected,
                why,
            })
        })?;
    }
    Ok(actual)
}
pub(super) fn infer(
    expr: ExprRef<'_>,
    expected: Option<&StreamType>,
    context: &mut TypeContext<'_>,
) -> Result<StreamType, SemanticError> {
    type_and_record(expr, expected, Expected::Hint, context)
}
pub(super) fn type_part(
    expr: ExprRef<'_>,
    expected: Option<&StreamType>,
    how: Expected,
    context: &mut TypeContext<'_>,
) -> Result<StreamType, SemanticError> {
    match how {
        Expected::Required => check(expr, expected, context),
        Expected::Hint => infer(expr, expected, context),
    }
}
fn type_and_record(
    expr: ExprRef<'_>,
    expected: Option<&StreamType>,
    how: Expected,
    context: &mut TypeContext<'_>,
) -> Result<StreamType, SemanticError> {
    let typ = rules::type_of(expr, expected, how, context)?;
    context.record(expr, &typ);
    Ok(typ)
}
pub(super) fn require(
    actual: &StreamType,
    expected: &StreamType,
    expr: ExprRef<'_>,
) -> Result<(), SemanticError> {
    flow(actual, expected).map_err(|why| {
        mismatch(
            expr,
            RejectedFlow {
                actual,
                expected,
                why,
            },
        )
    })
}
/// An explicit target admits contributions independently; otherwise their concrete types agree.
pub(super) fn merge(
    node: ExprRef<'_>,
    branches: &[StreamType],
    expected: Option<&StreamType>,
) -> Result<StreamType, SemanticError> {
    if let Some(expected) = expected {
        for branch in branches {
            require(branch, expected, node)?;
        }
        return Ok(expected.clone());
    }
    let first = branches.first().ok_or_else(|| {
        annotation_needed(node, "empty collection needs an element type annotation")
    })?;
    if branches
        .iter()
        .all(|typ| flow(typ, first).is_ok() && flow(first, typ).is_ok())
    {
        Ok(first.clone())
    } else {
        Err(annotation_needed(
            node,
            "contributions have different concrete types; supply an explicit common type",
        ))
    }
}
