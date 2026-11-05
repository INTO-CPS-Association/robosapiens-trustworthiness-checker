//! Concrete collection elements and field diagnostics.
use super::super::{
    checking::{Expected, check_with_mismatch_error, merge, type_part},
    diagnostics::{error, mismatch},
    relations::Mismatch,
    type_contexts::TypeContext,
};
use crate::core::StreamType;
use crate::lang::dsrv::{
    ast::ExprRef,
    diagnostics::{SemanticError, TypeErrorKind},
};
pub(in crate::lang::dsrv::type_checker) fn type_collection_items<'a>(
    node: ExprRef<'_>,
    items: impl IntoIterator<Item = ExprRef<'a>>,
    expected: Option<&StreamType>,
    how: Expected,
    context: &mut TypeContext<'_>,
) -> Result<StreamType, SemanticError> {
    let mut types = Vec::new();
    for item in items {
        types.push(type_part(item, expected, how, context)?);
    }
    merge(node, &types, expected)
}
pub(in crate::lang::dsrv::type_checker) fn check_field(
    expr: ExprRef<'_>,
    name: &str,
    value: ExprRef<'_>,
    expected: Option<&StreamType>,
    context: &mut TypeContext<'_>,
) -> Result<StreamType, SemanticError> {
    check_with_mismatch_error(value, expected, context, |rejected| match rejected.why {
        Mismatch::Types => error(
            expr,
            TypeErrorKind::StructFieldTypeMismatch,
            format!(
                "field {name} has type {}, expected {}{}",
                rejected.actual,
                rejected.expected,
                rejected.detail()
            ),
        ),
        _ => mismatch(value, rejected),
    })
}
pub(in crate::lang::dsrv::type_checker) fn list_element<'typ>(
    expr: ExprRef<'_>,
    typ: &'typ StreamType,
    kind: TypeErrorKind,
    message: &str,
) -> Result<&'typ StreamType, SemanticError> {
    match typ {
        StreamType::List(element) => Ok(element.as_ref()),
        _ => Err(error(expr, kind, message)),
    }
}
