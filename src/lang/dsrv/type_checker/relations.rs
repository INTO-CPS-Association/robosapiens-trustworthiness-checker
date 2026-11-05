//! Structural admission of concrete types. No relation synthesizes a type.

use crate::core::StreamType;
use crate::core::{ClosedUnion, UnionPayload};
use ecow::EcoString;
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Mismatch {
    Types,
    MissingField(EcoString),
    ExtraField(EcoString),
    OpenValue,
    Parameter(Box<Mismatch>),
}
impl Mismatch {
    fn in_parameter(self) -> Self {
        Self::Parameter(Box::new(self))
    }
}
pub(super) fn flow(actual: &StreamType, expected: &StreamType) -> Result<(), Mismatch> {
    if actual == expected {
        return Ok(());
    }
    match (actual, expected) {
        (StreamType::List(a), StreamType::List(b))
        | (StreamType::Map(a), StreamType::Map(b))
        | (StreamType::Expr(a), StreamType::Expr(b)) => flow(a, b),
        (StreamType::Tuple(a), StreamType::Tuple(b)) if a.len() == b.len() => {
            for (a, b) in a.iter().zip(b) {
                flow(a, b)?;
            }
            Ok(())
        }
        (StreamType::Struct(actual, actual_open), StreamType::Struct(expected, open)) => {
            for (name, wanted) in expected {
                let (_, known) = actual
                    .iter()
                    .find(|(field, _)| field == name)
                    .ok_or_else(|| Mismatch::MissingField(name.clone()))?;
                flow(known, wanted)?;
            }
            if !open {
                if let Some((name, _)) = actual
                    .iter()
                    .find(|(name, _)| !expected.iter().any(|(field, _)| field == name))
                {
                    return Err(Mismatch::ExtraField(name.clone()));
                }
                if *actual_open {
                    return Err(Mismatch::OpenValue);
                }
            }
            Ok(())
        }
        (StreamType::Function(params, result), StreamType::Function(passed, wanted))
            if params.len() == passed.len() =>
        {
            for (passed, param) in passed.iter().zip(params) {
                flow(passed, param).map_err(Mismatch::in_parameter)?;
            }
            flow(result, wanted)
        }
        (StreamType::Union(a), StreamType::Union(b)) if unions_line_up(a, b) => {
            for (a, b) in a.alternatives().iter().zip(b.alternatives()) {
                if let (UnionPayload::Of(a), UnionPayload::Of(b)) = (a.payload(), b.payload()) {
                    flow(a, b)?;
                }
            }
            Ok(())
        }
        _ => Err(Mismatch::Types),
    }
}
pub(super) fn comparable(a: &StreamType, b: &StreamType) -> bool {
    flow(a, b).is_ok() || flow(b, a).is_ok()
}
pub(super) fn unions_line_up(a: &ClosedUnion<StreamType>, b: &ClosedUnion<StreamType>) -> bool {
    a.alternatives().len() == b.alternatives().len()
        && a.alternatives().iter().zip(b.alternatives()).all(|(a, b)| {
            a.tag() == b.tag()
                && matches!(a.payload(), UnionPayload::Of(_))
                    == matches!(b.payload(), UnionPayload::Of(_))
        })
}
