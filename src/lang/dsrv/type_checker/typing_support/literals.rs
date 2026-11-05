//! Types present in syntax constants, including contextual empty shapes.
use super::super::{
    checking::merge,
    diagnostics::{Site, annotation_needed, error_at},
};
use crate::core::StreamType;
use crate::lang::dsrv::{
    ast::{ExprRef, SyntaxLiteral},
    diagnostics::{SemanticError, TypeErrorKind},
};
pub(in crate::lang::dsrv::type_checker) fn list_element(typ: &StreamType) -> Option<&StreamType> {
    match typ {
        StreamType::List(t) => Some(t),
        _ => None,
    }
}
pub(in crate::lang::dsrv::type_checker) fn map_value(typ: &StreamType) -> Option<&StreamType> {
    match typ {
        StreamType::Map(t) => Some(t),
        _ => None,
    }
}
pub(in crate::lang::dsrv::type_checker) fn value_type(
    node: ExprRef<'_>,
    value: &SyntaxLiteral,
    expected: Option<&StreamType>,
) -> Result<StreamType, SemanticError> {
    Ok(match value {
        SyntaxLiteral::Int(_) => StreamType::Int,
        SyntaxLiteral::Float(_) => StreamType::Float,
        SyntaxLiteral::Str(_) => match expected {
            Some(StreamType::Expr(t)) => StreamType::Expr(t.clone()),
            _ => StreamType::Str,
        },
        SyntaxLiteral::Bool(_) => StreamType::Bool,
        SyntaxLiteral::Unit => StreamType::Unit,
        SyntaxLiteral::List(values) => StreamType::List(Box::new(values_type(
            node,
            values.iter(),
            expected.and_then(list_element),
        )?)),
        SyntaxLiteral::Map(values) => StreamType::Map(Box::new(values_type(
            node,
            values.values(),
            expected.and_then(map_value),
        )?)),
        SyntaxLiteral::Tuple(values) => StreamType::Tuple(
            values
                .iter()
                .enumerate()
                .map(|(i, value)| {
                    value_type(
                        node,
                        value,
                        match expected {
                            Some(StreamType::Tuple(types)) => types.get(i),
                            _ => None,
                        },
                    )
                })
                .collect::<Result<_, _>>()?,
        ),
        SyntaxLiteral::Struct(fields) => StreamType::Struct(
            fields
                .iter()
                .map(|(name, value)| {
                    let expected = match expected {
                        Some(StreamType::Struct(fields, _)) => fields
                            .iter()
                            .find(|(field, _)| field == name)
                            .map(|(_, typ)| typ),
                        _ => None,
                    };
                    Ok((name.clone(), value_type(node, value, expected)?))
                })
                .collect::<Result<_, SemanticError>>()?,
            false,
        ),
        SyntaxLiteral::NoVal => {
            return Err(annotation_needed(
                node,
                "runtime markers are not source literals",
            ));
        }
    })
}
fn values_type<'a>(
    node: ExprRef<'_>,
    values: impl Iterator<Item = &'a SyntaxLiteral>,
    expected: Option<&StreamType>,
) -> Result<StreamType, SemanticError> {
    let types = values
        .map(|value| value_type(node, value, expected))
        .collect::<Result<Vec<_>, _>>()?;
    merge(node, &types, expected)
}
pub(in crate::lang::dsrv::type_checker) fn literal_too_large(
    site: Site,
    literal: i64,
) -> SemanticError {
    error_at(
        site,
        TypeErrorKind::OperatorTypeMismatch,
        format!(
            "integer literal {literal} cannot be represented exactly as a Float; write it as a Float literal"
        ),
    )
}
