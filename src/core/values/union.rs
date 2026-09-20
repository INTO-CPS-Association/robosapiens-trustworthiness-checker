//! Canonical structural union schemas and schema-free runtime values.
use std::{fmt, rc::Rc};

use ecow::{EcoString, EcoVec};

use super::{StreamType, Value};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub enum UnionPayload<T> {
    Nullary,
    Of(T),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub struct UnionAlternative<T> {
    tag: EcoString,
    payload: UnionPayload<T>,
}

impl<T> UnionAlternative<T> {
    pub fn new(tag: impl Into<EcoString>, payload: UnionPayload<T>) -> Self {
        Self {
            tag: tag.into(),
            payload,
        }
    }
    pub fn tag(&self) -> &EcoString {
        &self.tag
    }
    pub fn payload(&self) -> &UnionPayload<T> {
        &self.payload
    }
}

/// Nonempty alternatives, sorted by tag with no duplicates.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub struct ClosedUnion<T> {
    alternatives: EcoVec<UnionAlternative<T>>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UnionSchemaError {
    #[error("a closed union must contain an alternative")]
    Empty,
    #[error("duplicate union tag {0:?}")]
    DuplicateTag(EcoString),
}

impl<T: Clone> ClosedUnion<T> {
    pub fn new(
        alternatives: impl IntoIterator<Item = UnionAlternative<T>>,
    ) -> Result<Self, UnionSchemaError> {
        let mut alternatives: Vec<_> = alternatives.into_iter().collect();
        if alternatives.is_empty() {
            return Err(UnionSchemaError::Empty);
        }
        alternatives.sort_by(|a, b| a.tag.cmp(&b.tag));
        for pair in alternatives.windows(2) {
            if pair[0].tag == pair[1].tag {
                return Err(UnionSchemaError::DuplicateTag(pair[0].tag.clone()));
            }
        }
        Ok(Self {
            alternatives: alternatives.into(),
        })
    }
    pub fn alternatives(&self) -> &[UnionAlternative<T>] {
        &self.alternatives
    }
    pub fn alternative(&self, tag: &str) -> Option<(usize, &UnionAlternative<T>)> {
        self.alternatives
            .binary_search_by(|a| a.tag.as_str().cmp(tag))
            .ok()
            .map(|i| (i, &self.alternatives[i]))
    }
    pub fn try_map<U: Clone, E>(
        &self,
        mut f: impl FnMut(&T) -> Result<U, E>,
    ) -> Result<ClosedUnion<U>, E> {
        let alternatives = self
            .alternatives
            .iter()
            .map(|a| {
                Ok(UnionAlternative::new(
                    a.tag.clone(),
                    match &a.payload {
                        UnionPayload::Nullary => UnionPayload::Nullary,
                        UnionPayload::Of(ty) => UnionPayload::Of(f(ty)?),
                    },
                ))
            })
            .collect::<Result<EcoVec<_>, E>>()?;
        Ok(ClosedUnion { alternatives })
    }
    pub fn map<U: Clone>(&self, mut f: impl FnMut(&T) -> U) -> ClosedUnion<U> {
        self.try_map::<_, std::convert::Infallible>(|ty| Ok(f(ty)))
            .unwrap()
    }
}

impl<T: Clone + fmt::Display> fmt::Display for ClosedUnion<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Union<")?;
        for (i, a) in self.alternatives.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", a.tag)?;
            match &a.payload {
                UnionPayload::Of(ty) => write!(f, ": {ty}")?,
                UnionPayload::Nullary => {}
            }
        }
        write!(f, ">")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnionValue {
    tag: EcoString,
    payload: Option<Value>,
}

impl UnionValue {
    pub fn new(tag: impl Into<EcoString>, payload: Option<Value>) -> Self {
        Self {
            tag: tag.into(),
            payload,
        }
    }
    pub fn tag(&self) -> &EcoString {
        &self.tag
    }
    pub fn payload(&self) -> Option<&Value> {
        self.payload.as_ref()
    }
}

impl From<UnionValue> for Value {
    fn from(value: UnionValue) -> Self {
        Self::Union(Rc::new(value))
    }
}

/// Evidence borrows both the value and the complete expected schema.
#[derive(Debug, Clone, Copy)]
pub struct CheckedUnionRef<'a> {
    value: &'a UnionValue,
    schema: &'a ClosedUnion<StreamType>,
    alternative_index: usize,
}

impl<'a> CheckedUnionRef<'a> {
    pub fn value(self) -> &'a UnionValue {
        self.value
    }
    pub fn schema(self) -> &'a ClosedUnion<StreamType> {
        self.schema
    }
    pub fn alternative_index(self) -> usize {
        self.alternative_index
    }
    pub fn alternative(self) -> &'a UnionAlternative<StreamType> {
        &self.schema.alternatives[self.alternative_index]
    }
    pub fn payload(self) -> Option<&'a Value> {
        self.value.payload()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnionPathSegment {
    Payload,
    Index(usize),
    Field(EcoString),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UnionConformanceCause {
    #[error("unknown union tag {0:?}")]
    UnknownTag(EcoString),
    #[error("union payload presence differs from the schema")]
    PayloadPresence,
    #[error("value representation does not conform to {0}")]
    TypeMismatch(StreamType),
    #[error("nested stream marker is not a concrete value")]
    Marker,
    #[error("missing field {0:?}")]
    MissingField(EcoString),
    #[error("unexpected field {0:?}")]
    ExtraField(EcoString),
    #[error("core conformance cannot establish executable type {0}")]
    UnsupportedSchema(StreamType),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{cause} at {path:?} (union tag {actual_tag:?}, expected {expected_schema:?})")]
pub struct UnionConformanceError {
    pub path: Vec<UnionPathSegment>,
    pub cause: UnionConformanceCause,
    /// The complete outer union being checked, not an inferred singleton.
    pub expected_schema: Option<ClosedUnion<StreamType>>,
    pub actual_tag: Option<EcoString>,
}

impl UnionConformanceError {
    fn new(cause: UnionConformanceCause) -> Self {
        Self {
            path: Vec::new(),
            cause,
            expected_schema: None,
            actual_tag: None,
        }
    }
    fn at(mut self, segment: UnionPathSegment) -> Self {
        self.path.insert(0, segment);
        self
    }
}

pub fn check_union<'a>(
    value: &'a UnionValue,
    expected: &'a ClosedUnion<StreamType>,
) -> Result<CheckedUnionRef<'a>, UnionConformanceError> {
    check_union_inner(value, expected).map_err(|mut error| {
        error.expected_schema = Some(expected.clone());
        error.actual_tag = Some(value.tag.clone());
        error
    })
}

fn check_union_inner<'a>(
    value: &'a UnionValue,
    expected: &'a ClosedUnion<StreamType>,
) -> Result<CheckedUnionRef<'a>, UnionConformanceError> {
    let (alternative_index, alternative) = expected.alternative(value.tag()).ok_or_else(|| {
        UnionConformanceError::new(UnionConformanceCause::UnknownTag(value.tag.clone()))
    })?;
    match (alternative.payload(), value.payload()) {
        (UnionPayload::Nullary, None) => {}
        (UnionPayload::Of(ty), Some(value)) => {
            check_value_conformance(value, ty).map_err(|e| e.at(UnionPathSegment::Payload))?
        }
        _ => {
            return Err(
                UnionConformanceError::new(UnionConformanceCause::PayloadPresence)
                    .at(UnionPathSegment::Payload),
            );
        }
    }
    Ok(CheckedUnionRef {
        value,
        schema: expected,
        alternative_index,
    })
}

/// Representation-preserving check: no numeric coercions and no singleton schema inference.
/// Even `Any` rejects markers recursively inside concrete containers.
///
/// Explicit `Function` and `Expr` schemas require checker-aware evidence unavailable
/// in core and return `UnsupportedSchema`; their outer runtime representation alone
/// cannot prove a function signature or the type of executable source.
pub fn check_value_conformance(
    value: &Value,
    expected: &StreamType,
) -> Result<(), UnionConformanceError> {
    use StreamType as T;
    use UnionConformanceCause as C;
    use UnionPathSegment as P;
    if matches!(value, Value::Deferred | Value::NoVal) {
        return Err(UnionConformanceError::new(C::Marker));
    }
    match (value, expected) {
        (_, T::Function(_, _) | T::Expr(_)) => Err(UnionConformanceError::new(
            C::UnsupportedSchema(expected.clone()),
        )),
        (Value::Int(_), T::Int)
        | (Value::Float(_), T::Float)
        | (Value::Str(_), T::Str)
        | (Value::Bool(_), T::Bool)
        | (Value::Unit, T::Unit) => Ok(()),
        (Value::Union(value), T::Union(schema)) => check_union(value, schema).map(|_| ()),
        (Value::List(values), T::List(inner)) => {
            for (i, value) in values.iter().enumerate() {
                check_value_conformance(value, inner).map_err(|e| e.at(P::Index(i)))?;
            }
            Ok(())
        }
        (Value::Tuple(values), T::Tuple(types)) if values.len() == types.len() => {
            for (i, (value, ty)) in values.iter().zip(types).enumerate() {
                check_value_conformance(value, ty).map_err(|e| e.at(P::Index(i)))?;
            }
            Ok(())
        }
        (Value::Map(values), T::Map(inner)) => {
            for (key, value) in values {
                check_value_conformance(value, inner).map_err(|e| e.at(P::Field(key.clone())))?;
            }
            Ok(())
        }
        (Value::Map(values), T::Struct(fields, extra)) => {
            for (key, ty) in fields {
                let value = values.get(key).ok_or_else(|| {
                    UnionConformanceError::new(C::MissingField(key.clone()))
                        .at(P::Field(key.clone()))
                })?;
                check_value_conformance(value, ty).map_err(|e| e.at(P::Field(key.clone())))?;
            }
            for (key, value) in values {
                if !fields.iter().any(|(name, _)| name == key) {
                    if !extra {
                        return Err(UnionConformanceError::new(C::ExtraField(key.clone()))
                            .at(P::Field(key.clone())));
                    }
                    check_value_conformance(value, &T::Any)
                        .map_err(|e| e.at(P::Field(key.clone())))?;
                }
            }
            Ok(())
        }
        (_, T::Any) => {
            match value {
                Value::List(values) | Value::Tuple(values) => {
                    for (i, value) in values.iter().enumerate() {
                        check_value_conformance(value, &T::Any).map_err(|e| e.at(P::Index(i)))?;
                    }
                }
                Value::Map(values) => {
                    for (key, value) in values {
                        check_value_conformance(value, &T::Any)
                            .map_err(|e| e.at(P::Field(key.clone())))?;
                    }
                }
                Value::Union(value) => {
                    if let Some(payload) = value.payload() {
                        check_value_conformance(payload, &T::Any).map_err(|e| e.at(P::Payload))?;
                    }
                }
                _ => {}
            }
            Ok(())
        }
        _ => Err(UnionConformanceError::new(C::TypeMismatch(
            expected.clone(),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::JsonStreamValue;
    use crate::lang::dsrv::type_checker::TCType;
    use std::collections::BTreeMap;

    fn schema() -> ClosedUnion<StreamType> {
        ClosedUnion::new([
            UnionAlternative::new("Stopped", UnionPayload::Nullary),
            UnionAlternative::new(
                "Moving",
                UnionPayload::Of(StreamType::List(Box::new(StreamType::Int))),
            ),
        ])
        .unwrap()
    }

    #[test]
    fn union_schema_canonical_and_transcodes() {
        let schema = schema();
        assert_eq!(schema.alternatives()[0].tag(), "Moving");
        assert_eq!(
            ClosedUnion::new(schema.alternatives().iter().rev().cloned()).unwrap(),
            schema
        );
        assert!(matches!(
            ClosedUnion::new([
                schema.alternatives()[0].clone(),
                schema.alternatives()[0].clone()
            ]),
            Err(UnionSchemaError::DuplicateTag(_))
        ));
        let ty = StreamType::Union(schema);
        assert_eq!(ty.to_string(), "Union<Moving: List<Int>, Stopped>");
        assert_eq!(TCType::from_stream_type(&ty).to_stream_type(), Some(ty));
    }

    #[test]
    fn union_borrowed_conformance_retains_complete_schema_and_path() {
        let schema = schema();
        let value = UnionValue::new("Stopped", None);
        let checked = check_union(&value, &schema).unwrap();
        assert!(std::ptr::eq(checked.value(), &value));
        assert!(std::ptr::eq(checked.schema(), &schema));
        let value = UnionValue::new("Moving", Some(Value::List(vec![Value::Float(1.0)].into())));
        let error = check_union(&value, &schema).unwrap_err();
        assert_eq!(
            error.path,
            [UnionPathSegment::Payload, UnionPathSegment::Index(0)]
        );
        assert_eq!(error.expected_schema.as_ref(), Some(&schema));
        assert_eq!(error.actual_tag.as_deref(), Some("Moving"));
        assert!(check_union(&UnionValue::new("Stopped", Some(Value::Unit)), &schema).is_err());
        let nested_marker: Value = UnionValue::new("Anything", Some(Value::NoVal)).into();
        assert_eq!(
            check_value_conformance(&nested_marker, &StreamType::Any)
                .unwrap_err()
                .cause,
            UnionConformanceCause::Marker
        );
        let any_payload = ClosedUnion::new([UnionAlternative::new(
            "Anything",
            UnionPayload::Of(StreamType::Any),
        )])
        .unwrap();
        assert!(
            check_union(
                &UnionValue::new("Anything", Some(Value::NoVal)),
                &any_payload
            )
            .is_err()
        );
        assert!(
            check_union(
                &UnionValue::new("Anything", Some(Value::Str("valid".into()))),
                &any_payload
            )
            .is_ok()
        );
        assert!(check_value_conformance(&Value::NoVal, &StreamType::Int).is_err());
    }

    #[test]
    fn union_equality_does_not_shortcut_shared_nan() {
        let nan: Value =
            UnionValue::new("N", Some(Value::List(vec![Value::Float(f64::NAN)].into()))).into();
        assert_ne!(nan, nan.clone());
        let value: Value = UnionValue::new("N", Some(Value::Unit)).into();
        assert_eq!(value, value.clone());
        assert_eq!(value, Value::from(UnionValue::new("N", Some(Value::Unit))));
        assert_ne!(value, Value::from(UnionValue::new("N", None)));
        assert_eq!(Value::Int(1).encode_stdout().unwrap(), "1");
        assert_eq!(value.encode_stdout().unwrap(), "N(())");
    }

    #[test]
    fn union_codec_preserves_nullary_null_and_arbitrary_string_tags() {
        for raw in [
            r#"{"$tag":"Stopped"}"#,
            r#"{"$tag":"Ack","payload":null}"#,
            r#"{"$tag":"⊥","payload":3}"#,
        ] {
            let value: Value = serde_json::from_str(raw).unwrap();
            assert_eq!(value.encode_json().unwrap(), raw);
            assert_eq!(Value::decode_json(raw.as_bytes()).unwrap(), value);
            assert_eq!(
                Value::try_from(serde_json::from_str::<serde_json::Value>(raw).unwrap()).unwrap(),
                value
            );
        }
    }

    #[test]
    fn union_codec_rejects_reserved_shape_errors_and_nested_collisions() {
        for raw in [
            r#"{"$tag":"X","$tag":"X"}"#,
            r#"{"payload":null,"payload":1,"$tag":"X"}"#,
            r#"{"$tag":null}"#,
            r#"{"$tag":"X","extra":1}"#,
            r#"{"$tag":"X","payload":{"$tag":"Y","payload":1,"payload":2}}"#,
        ] {
            assert!(serde_json::from_str::<Value>(raw).is_err(), "{raw}");
            assert!(Value::decode_json(raw.as_bytes()).is_err(), "{raw}");
        }
        let collision = Value::Map(BTreeMap::from([("$tag".into(), Value::Str("X".into()))]));
        assert!(collision.encode_json().is_err());
        let nested: Value = UnionValue::new("Nested", Some(collision)).into();
        assert!(nested.encode_json().is_err());
        assert!(matches!(
            Value::decode_json(br#"{"payload":1,"payload":2}"#).unwrap(),
            Value::Map(_)
        ));
    }

    #[test]
    fn union_codec_nonfinite_payload_uses_json5() {
        let value: Value = UnionValue::new(
            "Nested",
            Some(Value::List(vec![Value::Float(f64::INFINITY)].into())),
        )
        .into();
        let encoded = value.encode_json().unwrap();
        assert!(encoded.contains("Infinity"));
        assert_eq!(Value::decode_json(encoded.as_bytes()).unwrap(), value);
    }

    /// The schema-free codec accepts the wire shape
    /// before the complete schema is known. Membership, arity, and embedded
    /// marker failures are reported by the separate conformance boundary.
    #[test]
    fn codec_and_conformance_keep_unknown_missing_and_marker_stages_distinct() {
        let schema = schema();
        let cases = [
            (
                r#"{"$tag":"Unknown","payload":[]}"#,
                UnionConformanceCause::UnknownTag("Unknown".into()),
            ),
            (
                r#"{"$tag":"Moving"}"#,
                UnionConformanceCause::PayloadPresence,
            ),
            (
                r#"{"$tag":"Moving","payload":["⊥"]}"#,
                UnionConformanceCause::Marker,
            ),
        ];
        for (raw, cause) in cases {
            let value = Value::decode_json(raw.as_bytes()).unwrap();
            let Value::Union(value) = value else {
                panic!("wire union shape was lost for {raw}");
            };
            let error = check_union(&value, &schema).unwrap_err();
            assert_eq!(error.cause, cause, "{raw}");
            assert_eq!(error.actual_tag.as_deref(), Some(value.tag().as_str()));
            assert_eq!(error.expected_schema.as_ref(), Some(&schema));
        }
        assert!(Value::decode_json(br#"{"$tag":"Moving","payload":[],"extra":1}"#).is_err());
    }

    /// Any relaxes the outer type shape, not the validity of a concrete marker
    /// nested inside a container or union payload.
    #[test]
    fn any_accepts_unknown_concrete_shapes_but_rejects_embedded_markers() {
        let concrete = Value::Map(BTreeMap::from([(
            "nested".into(),
            Value::List(vec![Value::Int(1)].into()),
        )]));
        assert!(check_value_conformance(&concrete, &StreamType::Any).is_ok());

        let marker = Value::Map(BTreeMap::from([(
            "nested".into(),
            Value::List(vec![Value::Deferred].into()),
        )]));
        let error = check_value_conformance(&marker, &StreamType::Any).unwrap_err();
        assert_eq!(error.cause, UnionConformanceCause::Marker);
        assert_eq!(
            error.path,
            [
                UnionPathSegment::Field("nested".into()),
                UnionPathSegment::Index(0)
            ]
        );

        let union: Value = UnionValue::new("Unregistered", Some(marker)).into();
        let error = check_value_conformance(&union, &StreamType::Any).unwrap_err();
        assert_eq!(error.cause, UnionConformanceCause::Marker);
        assert_eq!(
            error.path,
            [
                UnionPathSegment::Payload,
                UnionPathSegment::Field("nested".into()),
                UnionPathSegment::Index(0)
            ]
        );
    }

    /// Conformance walks nested struct fields and reports the deepest expected
    /// path instead of inferring a singleton runtime schema.
    #[test]
    fn conformance_reports_nested_struct_field_shape_and_extra_fields() {
        let schema = ClosedUnion::new([UnionAlternative::new(
            "Record",
            UnionPayload::Of(StreamType::Struct(
                vec![("count".into(), StreamType::Int)].into(),
                false,
            )),
        )])
        .unwrap();
        let missing = UnionValue::new("Record", Some(Value::Map(BTreeMap::new())));
        let error = check_union(&missing, &schema).unwrap_err();
        assert_eq!(
            error.cause,
            UnionConformanceCause::MissingField("count".into())
        );
        assert_eq!(
            error.path,
            [
                UnionPathSegment::Payload,
                UnionPathSegment::Field("count".into())
            ]
        );

        let extra = UnionValue::new(
            "Record",
            Some(Value::Map(BTreeMap::from([
                ("count".into(), Value::Int(1)),
                ("unexpected".into(), Value::Bool(true)),
            ]))),
        );
        let error = check_union(&extra, &schema).unwrap_err();
        assert_eq!(
            error.cause,
            UnionConformanceCause::ExtraField("unexpected".into())
        );
        assert_eq!(
            error.path,
            [
                UnionPathSegment::Payload,
                UnionPathSegment::Field("unexpected".into())
            ]
        );
    }
}
