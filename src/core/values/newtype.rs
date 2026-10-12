//! Nominal newtypes: their schemas and their run-time values.
//!
//! A newtype is a distinct type over a representation type. Two newtypes
//! over the same representation are never interchangeable, and a newtype is
//! never its representation: a value becomes one only through the explicit
//! wrap operation elaboration lowers `Name::new` to, and leaves one only
//! through the unwrap operation `Name::value` becomes. Neither a union nor a
//! structural alias carries this identity, so a newtype is its own run-time
//! and wire concept rather than a singleton union.
//!
//! Identity is the *brand*: the canonical path of the declaring module, the
//! declaration's name and, for a generic newtype, its canonical resolved
//! arguments, as in `std::time::Duration<app::JournalTime>`. A declaration in
//! the root module has no module prefix. Every check of identity is a brand
//! comparison; the representation is checked separately.
//!
//! Values carry their brand, so identity survives `Any`, a cast and a round
//! trip through the wire encoding, `{"$newtype": "<brand>", "value": …}`.
//! Values that are not newtypes pay nothing: no other value carries a brand
//! and no other operation consults one.

use std::fmt;
use std::rc::Rc;

use ecow::{EcoString, EcoVec};

use super::{StreamType, Value};

/// The reserved JSON member that marks an encoded newtype value.
pub const NEWTYPE_MARKER: &str = "$newtype";
/// The member holding an encoded newtype's representation.
pub const NEWTYPE_VALUE: &str = "value";

/// One instance of a newtype declaration: its brand, the resolved arguments
/// the brand was made from, and the representation its values hold.
///
/// Equality is equality of all three, which within one program is equality
/// of brands: a declaration's arguments and representation follow from its
/// brand.
///
/// The fields sit behind one box so that the `StreamType` and `TCType`
/// variants holding a schema are no wider than their other variants: a
/// schema inline would double the size of both types, and with it every
/// frame of the recursive visitors (the type checker's among them) that
/// ordinary programs run.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub struct NewtypeSchema<T>(Box<NewtypeFields<T>>);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
struct NewtypeFields<T> {
    brand: EcoString,
    arguments: EcoVec<T>,
    repr: T,
}

impl<T: Clone> NewtypeSchema<T> {
    pub fn new(brand: impl Into<EcoString>, arguments: EcoVec<T>, repr: T) -> Self {
        Self(Box::new(NewtypeFields {
            brand: brand.into(),
            arguments,
            repr,
        }))
    }

    /// The canonical identity values of this newtype carry.
    pub fn brand(&self) -> &EcoString {
        &self.0.brand
    }

    /// The declaration's canonical path, without its arguments: the brand
    /// up to its argument list.
    pub fn stem(&self) -> &str {
        brand_stem(&self.0.brand)
    }

    /// The resolved type arguments, in the declaration's parameter order.
    pub fn arguments(&self) -> &[T] {
        &self.0.arguments
    }

    /// The type a value of this newtype holds.
    pub fn repr(&self) -> &T {
        &self.0.repr
    }

    pub fn map<U: Clone>(&self, mut f: impl FnMut(&T) -> U) -> NewtypeSchema<U> {
        NewtypeSchema::new(
            self.0.brand.clone(),
            self.0.arguments.iter().map(&mut f).collect(),
            f(&self.0.repr),
        )
    }

    pub fn try_map<U: Clone, E>(
        &self,
        mut f: impl FnMut(&T) -> Result<U, E>,
    ) -> Result<NewtypeSchema<U>, E> {
        Ok(NewtypeSchema::new(
            self.0.brand.clone(),
            self.0
                .arguments
                .iter()
                .map(&mut f)
                .collect::<Result<_, _>>()?,
            f(&self.0.repr)?,
        ))
    }
}

impl<T> fmt::Display for NewtypeSchema<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.brand)
    }
}

/// The declaration path a brand names: everything before its argument list.
/// A module path and a type name never contain `<`, so the first one begins
/// the arguments.
pub fn brand_stem(brand: &str) -> &str {
    brand.split_once('<').map_or(brand, |(stem, _)| stem)
}

/// The module path that declared the newtype `stem` names, spelled as a
/// brand spells it: empty for the root module.
pub fn stem_module(stem: &str) -> &str {
    stem.rsplit_once("::").map_or("", |(module, _)| module)
}

/// The brand a newtype declaration with canonical path `stem` makes from
/// its resolved `arguments` (N5): the stem alone for a declaration without
/// parameters, and otherwise the stem followed by each argument's
/// [canonical spelling](canonical_type_text), as in
/// `time::Duration<app::JournalTime>`.
///
/// Brands are persisted on the wire, so this spelling is its own contract
/// and deliberately does not reuse any diagnostic `Display`.
pub fn canonical_brand(stem: &str, arguments: &[StreamType]) -> EcoString {
    let mut brand = String::from(stem);
    if !arguments.is_empty() {
        brand.push('<');
        write_canonical_list(&mut brand, arguments);
        brand.push('>');
    }
    brand.into()
}

/// The canonical spelling of a resolved type inside a brand. A newtype is
/// its brand, so an argument that is itself an instance spells as that
/// instance's identity, and a structural alias has already resolved to the
/// structure it names. Struct fields keep their order, which is part of a
/// struct type; union alternatives are already sorted by tag.
pub fn canonical_type_text(ty: &StreamType) -> String {
    let mut out = String::new();
    write_canonical(&mut out, ty);
    out
}

fn write_canonical_list(out: &mut String, types: &[StreamType]) {
    for (index, ty) in types.iter().enumerate() {
        if index != 0 {
            out.push_str(", ");
        }
        write_canonical(out, ty);
    }
}

fn write_canonical(out: &mut String, ty: &StreamType) {
    use super::UnionPayload;
    let wrapped = |out: &mut String, name: &str, inner: &StreamType| {
        out.push_str(name);
        out.push('<');
        write_canonical(out, inner);
        out.push('>');
    };
    match ty {
        StreamType::Int => out.push_str("Int"),
        StreamType::Float => out.push_str("Float"),
        StreamType::Str => out.push_str("Str"),
        StreamType::Bool => out.push_str("Bool"),
        StreamType::Unit => out.push_str("Unit"),
        StreamType::Any => out.push_str("Any"),
        StreamType::List(inner) => wrapped(out, "List", inner),
        StreamType::Map(inner) => wrapped(out, "Map", inner),
        StreamType::Expr(inner) => wrapped(out, "Expr", inner),
        StreamType::Tuple(items) => {
            out.push('(');
            write_canonical_list(out, items);
            if items.len() == 1 {
                out.push(',');
            }
            out.push(')');
        }
        StreamType::Struct(fields, open) => {
            out.push_str("Struct<");
            for (index, (name, field)) in fields.iter().enumerate() {
                if index != 0 {
                    out.push_str(", ");
                }
                out.push_str(name);
                out.push_str(": ");
                write_canonical(out, field);
            }
            if *open {
                if !fields.is_empty() {
                    out.push_str(", ");
                }
                out.push_str("...");
            }
            out.push('>');
        }
        StreamType::Function(arguments, result) => {
            out.push('(');
            write_canonical_list(out, arguments);
            out.push_str(" -> ");
            write_canonical(out, result);
            out.push(')');
        }
        StreamType::Union(schema) => {
            out.push_str("Union<");
            for (index, alternative) in schema.alternatives().iter().enumerate() {
                if index != 0 {
                    out.push_str(", ");
                }
                out.push_str(alternative.tag());
                if let UnionPayload::Of(payload) = alternative.payload() {
                    out.push_str(": ");
                    write_canonical(out, payload);
                }
            }
            out.push('>');
        }
        StreamType::Newtype(schema) => out.push_str(schema.brand()),
    }
}

/// A value of a newtype: its brand and the representation it holds.
#[derive(Debug, Clone, PartialEq)]
pub struct NewtypeValue {
    brand: EcoString,
    value: Value,
}

impl NewtypeValue {
    /// A branded value. This checks nothing: the wrap operation and the
    /// decoders are what decide whether a value may carry a brand.
    pub fn new(brand: impl Into<EcoString>, value: Value) -> Self {
        Self {
            brand: brand.into(),
            value,
        }
    }

    pub fn brand(&self) -> &EcoString {
        &self.brand
    }

    /// The representation this value holds.
    pub fn value(&self) -> &Value {
        &self.value
    }
}

impl From<NewtypeValue> for Value {
    fn from(value: NewtypeValue) -> Self {
        Value::Newtype(Rc::new(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_brands_have_a_fixed_spelling() {
        use crate::core::{ClosedUnion, UnionAlternative, UnionPayload};
        let journal = StreamType::Newtype(NewtypeSchema::new(
            "app::JournalTime",
            EcoVec::new(),
            StreamType::Unit,
        ));
        assert_eq!(canonical_brand("m::Meters", &[]), "m::Meters");
        assert_eq!(
            canonical_brand("time::Instant", std::slice::from_ref(&journal)),
            "time::Instant<app::JournalTime>"
        );
        let structural = [
            StreamType::List(Box::new(journal.clone())),
            StreamType::Tuple(EcoVec::from([StreamType::Int])),
            StreamType::Struct(
                EcoVec::from([
                    ("b".into(), StreamType::Float),
                    ("a".into(), journal.clone()),
                ]),
                true,
            ),
            StreamType::Union(
                ClosedUnion::new([
                    UnionAlternative::new("Some", UnionPayload::Of(StreamType::Str)),
                    UnionAlternative::new("None", UnionPayload::Nullary),
                ])
                .unwrap(),
            ),
            StreamType::Function(
                EcoVec::from([StreamType::Bool]),
                Box::new(StreamType::Map(Box::new(StreamType::Any))),
            ),
        ];
        assert_eq!(
            canonical_brand("p::Wide", &structural),
            "p::Wide<List<app::JournalTime>, (Int,), Struct<b: Float, a: app::JournalTime, ...>, \
             Union<None, Some: Str>, (Bool -> Map<Any>)>"
        );
    }

    #[test]
    fn the_stem_is_the_brand_before_its_arguments() {
        assert_eq!(brand_stem("app::units::Meters"), "app::units::Meters");
        assert_eq!(
            brand_stem("std::time::Duration<app::JournalTime>"),
            "std::time::Duration"
        );
        assert_eq!(brand_stem("Pair<a::X, Map<b::Y>>"), "Pair");
        assert_eq!(stem_module("app::units::Meters"), "app::units");
        assert_eq!(stem_module("Meters"), "");
    }

    use crate::core::values::operations::{ValueOpError, cast, newtype_unwrap, newtype_wrap};
    use crate::core::{
        JsonStreamValue, StreamType, UnionConformanceCause, UnionValue, check_value_conformance,
    };

    const JOURNAL: &str = "time::Duration<app::JournalTime>";

    fn duration(domain: &str) -> NewtypeSchema<StreamType> {
        NewtypeSchema::new(
            format!("time::Duration<app::{domain}>"),
            EcoVec::from([StreamType::Newtype(NewtypeSchema::new(
                format!("app::{domain}"),
                EcoVec::new(),
                StreamType::Unit,
            ))]),
            StreamType::Float,
        )
    }

    fn instant() -> NewtypeSchema<StreamType> {
        NewtypeSchema::new(
            "time::Instant<app::JournalTime>",
            duration("JournalTime").arguments().into(),
            StreamType::Float,
        )
    }

    fn branded(brand: &str, value: f64) -> Value {
        NewtypeValue::new(brand, Value::Float(value)).into()
    }

    // AC-NT-6: its own marker, never a union's `$tag`, and a round trip.
    #[test]
    fn a_newtype_value_has_its_own_wire_marker_and_round_trips() {
        let value = branded(JOURNAL, 0.2);
        let written = value.encode_json().unwrap();
        assert_eq!(
            written,
            r#"{"$newtype":"time::Duration<app::JournalTime>","value":0.2}"#
        );
        assert!(!written.contains("$tag"));
        assert_eq!(Value::decode_json(written.as_bytes()).unwrap(), value);
        let through_json: serde_json::Value = serde_json::from_str(&written).unwrap();
        assert_eq!(Value::try_from(through_json).unwrap(), value);
        // Nested, and with a non-finite representation.
        let nested = Value::List(vec![branded(JOURNAL, f64::NAN), value.clone()].into());
        let written = nested.encode_json().unwrap();
        let Value::List(read) = Value::decode_json(written.as_bytes()).unwrap() else {
            panic!("a list");
        };
        assert!(matches!(&read[0], Value::Newtype(v) if v.brand() == JOURNAL));
        assert_eq!(read[1], value);
    }

    #[test]
    fn the_marker_is_reserved() {
        // A map cannot pose as a newtype value when written.
        let map = Value::Map([(NEWTYPE_MARKER.into(), Value::Str("m::A".into()))].into());
        assert!(map.encode_json().is_err());
        // Malformed markers are refused rather than read as a map.
        for text in [
            r#"{"$newtype":"m::A"}"#,
            r#"{"$newtype":"m::A","value":1,"extra":2}"#,
            r#"{"$newtype":1,"value":1}"#,
            r#"{"$newtype":"m::A","$tag":"B","value":1}"#,
        ] {
            assert!(Value::decode_json(text.as_bytes()).is_err(), "{text}");
        }
        // A union tag is still any string, brand-shaped or not.
        let union: Value = UnionValue::new(JOURNAL, Some(Value::Float(1.0))).into();
        let written = union.encode_json().unwrap();
        assert!(written.contains("$tag") && !written.contains(NEWTYPE_MARKER));
        assert_eq!(Value::decode_json(written.as_bytes()).unwrap(), union);
    }

    // AC-NT-4 at the value level: the four N3 cases of a validating cast.
    #[test]
    fn a_cast_validates_an_identity_and_never_creates_one() {
        let target = StreamType::Newtype(duration("JournalTime"));
        // Duration<JournalTime> through Any and back.
        let value = branded(JOURNAL, 1.0);
        assert_eq!(cast(value.clone(), &StreamType::Any).unwrap(), value);
        assert_eq!(cast(value.clone(), &target).unwrap(), value);
        // Instant<JournalTime> is not a Duration<JournalTime>.
        let wrong_role = branded(instant().brand(), 1.0);
        // Instant<Producer> is not an Instant<JournalTime>.
        let wrong_domain = branded("time::Instant<app::Producer>", 1.0);
        // A bare Float is not made one.
        for (value, target) in [
            (wrong_role, target.clone()),
            (wrong_domain, StreamType::Newtype(instant())),
            (Value::Float(1.0), target.clone()),
        ] {
            assert!(
                matches!(
                    cast(value.clone(), &target),
                    Err(ValueOpError::InvalidCast { .. })
                ),
                "{value} as {target}"
            );
            let error = check_value_conformance(&value, &target).unwrap_err();
            assert!(matches!(
                error.cause,
                UnionConformanceCause::NewtypeBrand { .. }
            ));
        }
        // The brand is right but what it holds is not the representation.
        let malformed: Value = NewtypeValue::new(JOURNAL, Value::Str("far".into())).into();
        assert!(cast(malformed, &target).is_err());
        // Any accepts a newtype, but not markers hidden inside one.
        let hidden: Value = NewtypeValue::new(JOURNAL, Value::NoVal).into();
        assert!(check_value_conformance(&hidden, &StreamType::Any).is_err());
    }

    #[test]
    fn wrap_brands_and_unwrap_checks_the_brand() {
        let schema = duration("JournalTime");
        let value = newtype_wrap(Value::Float(2.0), &schema);
        assert_eq!(value, branded(JOURNAL, 2.0));
        assert_eq!(newtype_unwrap(value, &schema).unwrap(), Value::Float(2.0));
        let producer = newtype_wrap(Value::Float(2.0), &duration("Producer"));
        let error = newtype_unwrap(producer, &schema).unwrap_err();
        assert!(
            error.to_string().contains("time::Duration<app::Producer>"),
            "{error}"
        );
        assert!(newtype_unwrap(Value::Float(2.0), &schema).is_err());
        // Equality is brand and representation.
        assert_ne!(
            branded(JOURNAL, 2.0),
            branded("time::Duration<app::Producer>", 2.0)
        );
        assert_ne!(branded(JOURNAL, 2.0), Value::Float(2.0));
    }

    /// AC-NT-8: the newtype variants do not widen the type representations
    /// every recursive visitor holds in each frame: a schema is one pointer.
    #[test]
    fn a_newtype_type_does_not_widen_the_type_representations() {
        use crate::lang::dsrv::type_checker::TCType;
        assert_eq!(std::mem::size_of::<NewtypeSchema<StreamType>>(), 8);
        assert_eq!(std::mem::size_of::<NewtypeSchema<TCType>>(), 8);
        // A function type, arguments and a boxed result, is the widest
        // variant, and a newtype is narrower.
        let function = std::mem::size_of::<(EcoVec<StreamType>, Box<StreamType>)>();
        assert!(std::mem::size_of::<StreamType>() <= function + 8);
        assert!(std::mem::size_of::<TCType>() <= function + 8);
    }

    #[test]
    fn a_newtype_value_is_one_pointer_wide() {
        // The variant is behind an `Rc`, as a union's is, so adding it does
        // not enlarge every value.
        assert!(std::mem::size_of::<Value>() <= 32);
        let value: Value = NewtypeValue::new("m::A", Value::Float(1.0)).into();
        let copy = value.clone();
        let (Value::Newtype(left), Value::Newtype(right)) = (&value, &copy) else {
            unreachable!()
        };
        assert!(Rc::ptr_eq(left, right), "cloning shares the payload");
    }
}
