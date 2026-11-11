//! Pattern bindings, constructor resolution and exhaustiveness.

use super::super::diagnostics::error;
use super::super::relations::comparable;

use crate::VarName;
use crate::core::{ClosedUnion, StreamType, UnionPayload};
use crate::lang::dsrv::ast::{ExprRef, SyntaxLiteral};
use crate::lang::dsrv::diagnostics::{SemanticError, TypeErrorKind};
use crate::lang::dsrv::path::TypePath;
use crate::lang::dsrv::patterns::{MatchArm, MatchPattern, PatternKind};
use ecow::EcoString;
use std::collections::BTreeSet;

/// What a pattern binds, and their types, when matched against `scrutinee`.
///
/// A pattern that cannot describe a value of that type is reported here,
/// where the shape is known, rather than as a mismatch in the arm's body.
pub(in crate::lang::dsrv::type_checker) fn infer_pattern_bindings(
    pattern: &MatchPattern,
    scrutinee: &StreamType,
    expr: ExprRef<'_>,
) -> Result<Vec<(VarName, StreamType)>, SemanticError> {
    let mut bindings = Vec::new();
    collect_pattern_bindings(pattern, scrutinee, expr, &mut bindings)?;
    Ok(bindings)
}

fn collect_pattern_bindings(
    pattern: &MatchPattern,
    scrutinee: &StreamType,
    expr: ExprRef<'_>,
    bindings: &mut Vec<(VarName, StreamType)>,
) -> Result<(), SemanticError> {
    let mismatch = |wanted: &str| {
        Err(error(
            expr,
            TypeErrorKind::PatternTypeMismatch,
            format!(
                "`{pattern}` matches {wanted}, but the value here is {}",
                scrutinee
            ),
        ))
    };
    match &pattern.kind {
        PatternKind::Wildcard => {}
        PatternKind::Bind(name) => bindings.push((name.clone(), scrutinee.clone())),
        PatternKind::As(name, inner) => {
            bindings.push((name.clone(), scrutinee.clone()));
            collect_pattern_bindings(inner, scrutinee, expr, bindings)?;
        }
        PatternKind::Tag { tag, payload } => {
            let StreamType::Union(schema) = scrutinee else {
                return mismatch("a union");
            };
            let Some((_, alternative)) = schema.alternative(tag) else {
                return Err(error(
                    expr,
                    TypeErrorKind::UnknownUnionTag,
                    format!("`{tag}` is not an alternative of {}", scrutinee),
                ));
            };
            match (alternative.payload(), payload) {
                (UnionPayload::Nullary, None) => {}
                (UnionPayload::Of(declared), Some(payload)) => {
                    collect_pattern_bindings(payload, declared, expr, bindings)?;
                }
                (UnionPayload::Nullary, Some(_)) => {
                    return Err(error(
                        expr,
                        TypeErrorKind::ConstructorPayloadArity,
                        format!("`{tag}` carries no payload"),
                    ));
                }
                (UnionPayload::Of(declared), None) => {
                    return Err(error(
                        expr,
                        TypeErrorKind::ConstructorPayloadArity,
                        format!(
                            "`{tag}` carries {}, which this pattern does not name",
                            declared
                        ),
                    ));
                }
            }
        }
        PatternKind::Tuple(items) => match scrutinee.clone() {
            StreamType::Tuple(types) if types.len() == items.len() => {
                for (item, typ) in items.iter().zip(types.iter()) {
                    collect_pattern_bindings(item, typ, expr, bindings)?;
                }
            }
            _ => return mismatch("a tuple of that width"),
        },
        PatternKind::List(items) => match scrutinee.clone() {
            StreamType::List(element) => {
                for item in items {
                    collect_pattern_bindings(item, &element, expr, bindings)?;
                }
            }
            _ => return mismatch("a list"),
        },
        PatternKind::Struct { fields, rest } => match scrutinee.clone() {
            StreamType::Struct(declared, allow_extra) => {
                // A closed struct has all its fields, and a pattern without
                // `..` matches only a value with exactly the fields it names.
                if !rest && !allow_extra {
                    let omitted: Vec<String> = declared
                        .iter()
                        .filter(|(field, _)| !fields.iter().any(|(named, _)| named == field))
                        .map(|(field, _)| format!("`{field}`"))
                        .collect();
                    if !omitted.is_empty() {
                        return Err(error(
                            expr,
                            TypeErrorKind::PatternTypeMismatch,
                            format!(
                                "`{pattern}` leaves out {} of {}, so it can never \
                                 match; add `..` to ignore the other fields",
                                omitted.join(", "),
                                scrutinee
                            ),
                        ));
                    }
                }
                for (name, pattern) in fields {
                    let Some((_, typ)) = declared.iter().find(|(field, _)| field == name) else {
                        return Err(error(
                            expr,
                            TypeErrorKind::StructUnknownField,
                            format!("{} has no field `{name}`", scrutinee),
                        ));
                    };
                    collect_pattern_bindings(pattern, typ, expr, bindings)?;
                }
            }
            StreamType::Map(value) => {
                for (_, pattern) in fields {
                    collect_pattern_bindings(pattern, &value, expr, bindings)?;
                }
            }
            _ => return mismatch("a struct"),
        },
        PatternKind::Literal(value) => {
            let literal = match value {
                SyntaxLiteral::Int(_) => StreamType::Int,
                SyntaxLiteral::Str(_) => StreamType::Str,
                SyntaxLiteral::Bool(_) => StreamType::Bool,
                _ => StreamType::Unit,
            };
            if !comparable(scrutinee, &literal) {
                return mismatch(&literal.to_string());
            }
        }
        PatternKind::Range { .. } => {
            if !comparable(scrutinee, &StreamType::Int) {
                return mismatch("an Int");
            }
        }
        // Every alternative sees the same value, so each must bind the same
        // names with the same types for the arm to have one meaning.
        PatternKind::Or(alternatives) => {
            let mut first: Option<Vec<(VarName, StreamType)>> = None;
            for alternative in alternatives {
                let mut bound = Vec::new();
                collect_pattern_bindings(alternative, scrutinee, expr, &mut bound)?;
                bound.sort_by(|left, right| left.0.cmp(&right.0));
                match &first {
                    None => first = Some(bound),
                    Some(expected) if bind_alike(expected, &bound).is_some() => {}
                    Some(expected) => {
                        return Err(error(
                            expr,
                            TypeErrorKind::OrPatternBindings,
                            format!(
                                "`{alternative}` binds {}, while an earlier alternative binds {}",
                                show_bindings(&bound),
                                show_bindings(expected)
                            ),
                        ));
                    }
                }
            }
            bindings.extend(first.unwrap_or_default());
        }
    }
    Ok(())
}

/// Make two alternatives of an or-pattern, each with its bindings sorted by
/// name, bind the same names at the same types, or `None` where they cannot.
/// A failure decides nothing, not even for the names that did agree.
fn bind_alike(first: &[(VarName, StreamType)], second: &[(VarName, StreamType)]) -> Option<()> {
    if first.len() != second.len() {
        return None;
    }
    first.iter().zip(second).all(|(a, b)| a == b).then_some(())
}

fn show_bindings(bindings: &[(VarName, StreamType)]) -> String {
    if bindings.is_empty() {
        return "nothing".to_owned();
    }
    bindings
        .iter()
        .map(|(name, typ)| format!("{}: {}", name.name(), typ))
        .collect::<Vec<_>>()
        .join(", ")
}

// Which concrete Boolean values a pattern covers: bit 0 is false, bit 1 true.
fn boolean_coverage(pattern: &MatchPattern) -> u8 {
    match &pattern.kind {
        PatternKind::Wildcard | PatternKind::Bind(_) => 0b11,
        PatternKind::As(_, inner) => boolean_coverage(inner),
        PatternKind::Literal(SyntaxLiteral::Bool(false)) => 0b01,
        PatternKind::Literal(SyntaxLiteral::Bool(true)) => 0b10,
        PatternKind::Or(alternatives) => alternatives.iter().fold(0, |covered, alternative| {
            covered | boolean_coverage(alternative)
        }),
        _ => 0,
    }
}

/// Whether the arms leave a value unmatched.
///
/// A guarded arm does not count: its guard may refuse the value it matched,
/// so something after it still has to catch that value.
pub(in crate::lang::dsrv::type_checker) fn check_exhaustive(
    expr: ExprRef<'_>,
    shape: &[MatchArm],
    scrutinee: &StreamType,
) -> Result<(), SemanticError> {
    let unguarded = shape.iter().filter(|arm| !arm.guarded);
    if unguarded.clone().any(|arm| arm.pattern.is_irrefutable()) {
        return Ok(());
    }
    if *scrutinee == StreamType::Bool {
        let covered = unguarded
            .clone()
            .fold(0, |covered, arm| covered | boolean_coverage(&arm.pattern));
        if covered == 0b11 {
            return Ok(());
        }
        let missing = match covered {
            0b01 => "true",
            0b10 => "false",
            _ => "false, true",
        };
        return Err(error(
            expr,
            TypeErrorKind::MatchNotExhaustive,
            format!("no arm matches {missing}"),
        ));
    }
    let StreamType::Union(schema) = scrutinee else {
        // Other types need a pattern that matches whatever they are given.
        return Err(error(
            expr,
            TypeErrorKind::MatchNotExhaustive,
            format!(
                "a `match` on {} needs an arm that matches every value, such as `_`",
                scrutinee
            ),
        ));
    };
    let covered: BTreeSet<&str> = unguarded
        .flat_map(|arm| arm.pattern.covered_tags())
        .collect();
    let missing: Vec<&str> = schema
        .alternatives()
        .iter()
        .map(|alternative| alternative.tag().as_str())
        .filter(|tag| !covered.contains(tag))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    Err(error(
        expr,
        TypeErrorKind::MatchNotExhaustive,
        format!("no arm matches {}", missing.join(", ")),
    ))
}

/// The union a qualifier names, in the namespace the node was expanded in.
pub(in crate::lang::dsrv::type_checker) fn union_named(
    expr: ExprRef<'_>,
    qualifier: &TypePath,
) -> Result<ClosedUnion<StreamType>, SemanticError> {
    let named = expr
        .source_context()
        .and_then(|context| context.get(qualifier));
    match named.cloned() {
        Some(StreamType::Union(schema)) => Ok(schema),
        Some(other) => Err(error(
            expr,
            TypeErrorKind::ExpectedUnion,
            format!("{qualifier} is {other}, not a union"),
        )),
        None => Err(error(
            expr,
            TypeErrorKind::ExpectedUnion,
            format!("{qualifier} names no type here"),
        )),
    }
}

/// The unions in scope that do have this tag, to say in a message where it
/// could have come from.
pub(in crate::lang::dsrv::type_checker) fn tags_found_in(
    expr: ExprRef<'_>,
    tag: &EcoString,
) -> String {
    let Some(context) = expr.source_context() else {
        return String::new();
    };
    let names: Vec<_> = context
        .aliases()
        .iter()
        .filter(|(_, ty)| match ty {
            StreamType::Union(schema) => schema.alternative(tag).is_some(),
            _ => false,
        })
        .map(|(name, _)| name.to_string())
        .collect();
    match names.len() {
        0 => String::new(),
        _ => format!("; `{tag}` is an alternative of {}", names.join(", ")),
    }
}
