//! Operand rules, numeric constraints and casts.

use super::super::diagnostics::Site;
use super::super::diagnostics::error_at;
use super::super::type_contexts::TypeContext;

use crate::core::{BinaryOperator, BinaryOperatorKind, StreamType};
use crate::lang::dsrv::ast::{ExprRef, ExprView, SyntaxLiteral};
use crate::lang::dsrv::diagnostics::{
    SemanticError, SemanticWarning, SemanticWarningKind, TypeErrorKind,
};

pub(in crate::lang::dsrv::type_checker) fn is_negative_integer_literal(expr: ExprRef<'_>) -> bool {
    match expr.view() {
        ExprView::Neg(inner) => {
            matches!(inner.view(), ExprView::Val(SyntaxLiteral::Int(value)) if *value > 0)
        }
        // Source literals represent negativity as `Neg`, but programmatically
        // constructed ASTs may contain any negative `SyntaxLiteral::Int` directly.
        ExprView::Val(SyntaxLiteral::Int(value)) => *value < 0,
        _ => false,
    }
}

/// The error of a binary operator that cannot combine operands of types
/// `lhs` and `rhs`.
pub(in crate::lang::dsrv::type_checker) fn operator_error(
    site: Site,
    operator: BinaryOperator,
    lhs: &StreamType,
    rhs: &StreamType,
) -> SemanticError {
    let hint = match (lhs, rhs) {
        (StreamType::Int, StreamType::Float) | (StreamType::Float, StreamType::Int) => {
            "; convert the Int explicitly with `as Float`"
        }
        _ => "",
    };
    error_at(
        site,
        TypeErrorKind::OperatorTypeMismatch,
        format!(
            "{} operator `{}` cannot combine {} and {}{hint}",
            operator.name(),
            operator.symbol(),
            lhs,
            rhs,
        ),
    )
}

/// The type of a binary operator's result, where its operands, already
/// compared, have the concrete types `lhs` and `rhs`.
pub(in crate::lang::dsrv::type_checker) fn resolve_binary(
    site: Site,
    operator: BinaryOperator,
    lhs: &StreamType,
    rhs: &StreamType,
) -> Result<StreamType, SemanticError> {
    let numeric_result = || match (lhs, rhs) {
        (StreamType::Int, StreamType::Int) => Some(StreamType::Int),
        (StreamType::Float, StreamType::Float) => Some(StreamType::Float),
        _ => None,
    };

    Ok(match operator.kind() {
        BinaryOperatorKind::Numeric(_) => {
            numeric_result().ok_or_else(|| operator_error(site, operator, lhs, rhs))?
        }
        BinaryOperatorKind::Boolean(_)
            if matches!(lhs, StreamType::Bool) && matches!(rhs, StreamType::Bool) =>
        {
            StreamType::Bool
        }
        BinaryOperatorKind::String
            if matches!(lhs, StreamType::Str) && matches!(rhs, StreamType::Str) =>
        {
            StreamType::Str
        }
        // Values of any one type can be compared for equality.
        BinaryOperatorKind::Equality(_) => StreamType::Bool,
        BinaryOperatorKind::Ordering(_) if numeric_result().is_some() => StreamType::Bool,
        BinaryOperatorKind::Ordering(_)
            if matches!(lhs, StreamType::Bool) && matches!(rhs, StreamType::Bool) =>
        {
            StreamType::Bool
        }
        BinaryOperatorKind::Ordering(_)
            if matches!(lhs, StreamType::Str) && matches!(rhs, StreamType::Str) =>
        {
            StreamType::Bool
        }
        _ => return Err(operator_error(site, operator, lhs, rhs)),
    })
}

pub(in crate::lang::dsrv::type_checker) fn negative_exponent(site: Site) -> SemanticError {
    error_at(
        site,
        TypeErrorKind::OperatorTypeMismatch,
        "integer exponent must be non-negative".to_owned(),
    )
}

/// Whether `value` is an empty list or map literal and `target` a type of that
/// collection.
pub(in crate::lang::dsrv::type_checker) fn is_empty_collection_of(
    value: ExprRef<'_>,
    target: &StreamType,
) -> bool {
    match (value.view(), target) {
        (ExprView::List(items), StreamType::List(_)) => items.len() == 0,
        (ExprView::Map(fields) | ExprView::ObjectLiteral(fields), StreamType::Map(_)) => {
            fields.iter().next().is_none()
        }
        _ => false,
    }
}

/// Check a cast from `source` to `target` at `site`: warn where it is
/// redundant, and reject one that is not allowed.
pub(in crate::lang::dsrv::type_checker) fn check_cast(
    site: Site,
    source: &StreamType,
    target: &StreamType,
    context: &mut TypeContext<'_>,
) -> Result<(), SemanticError> {
    if source == target {
        context.warnings.record(
            site.id,
            SemanticWarning::new(
                SemanticWarningKind::RedundantCast,
                format!("cast from {target} to {target} is redundant"),
                Some(site.span),
            )
            .located(site.origin),
        );
    }
    let valid = source == target
        || *source == StreamType::Int && *target == StreamType::Float
        || *source == StreamType::Str && matches!(target, StreamType::Expr(_))
        || matches!(
            source,
            StreamType::Int | StreamType::Float | StreamType::Bool | StreamType::Unit
        ) && *target == StreamType::Str;
    if !valid {
        let detail = if *source == StreamType::Float && *target == StreamType::Int {
            "; use trunc, floor, ceil, or round instead"
        } else {
            ""
        };
        return Err(error_at(
            site,
            TypeErrorKind::OperatorTypeMismatch,
            format!("cannot cast {} to {target}{detail}", source),
        ));
    }
    Ok(())
}
