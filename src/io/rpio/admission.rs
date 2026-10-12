//! Admission against the full declared observation type.
//!
//! Classic wire decoding and exact-version resolution precede this boundary.
//! This module only reifies representation conveniences (integer-written Float
//! fields and absent optional struct fields), then checks every declared field.
//! It neither looks up history nor makes an application-level decision.

use anyhow::{Context, Result, bail};

use crate::core::{
    ClosedUnion, NewtypeValue, StreamType, UnionPayload, UnionValue, Value, check_value_conformance,
};

pub(crate) fn observation(value: Value, declared: &Option<StreamType>) -> Result<Value> {
    let Some(declared) = declared else {
        check_value_conformance(&value, &StreamType::Any)
            .context("RPIO observation contains a non-concrete value")?;
        return Ok(value);
    };
    let value = reify(value, declared, "observation")?;
    check_value_conformance(&value, declared)
        .context("RPIO observation does not conform to its complete declared type")?;
    Ok(value)
}

fn optional(schema: &ClosedUnion<StreamType>) -> bool {
    matches!(
        (schema.alternatives(), schema.alternative("None"), schema.alternative("Some")),
        ([_, _], Some((_, none)), Some((_, some)))
            if matches!(none.payload(), UnionPayload::Nullary)
                && matches!(some.payload(), UnionPayload::Of(_))
    )
}

fn reify(value: Value, expected: &StreamType, at: &str) -> Result<Value> {
    Ok(match (value, expected) {
        (Value::Int(number), StreamType::Float) => Value::Float(number as f64),
        (Value::Map(mut values), StreamType::Struct(fields, extra)) => {
            for (name, ty) in fields {
                let path = format!("{at}.{name}");
                let field = match values.remove(name) {
                    Some(field) => field,
                    None if matches!(ty, StreamType::Union(schema) if optional(schema)) => {
                        UnionValue::new("None", None).into()
                    }
                    None => bail!("RPIO {path}: missing declared field of type {ty}"),
                };
                values.insert(name.clone(), reify(field, ty, &path)?);
            }
            if !extra {
                for (name, _) in &values {
                    if !fields.iter().any(|(declared, _)| declared == name) {
                        bail!("RPIO {at}.{name}: field is not declared in closed struct");
                    }
                }
            }
            Value::Map(values)
        }
        (Value::List(values), StreamType::List(inner)) => Value::List(
            values
                .into_iter()
                .enumerate()
                .map(|(index, value)| reify(value, inner, &format!("{at}[{index}]")))
                .collect::<Result<_>>()?,
        ),
        (Value::Tuple(values), StreamType::Tuple(types)) if values.len() == types.len() => {
            Value::Tuple(
                values
                    .into_iter()
                    .zip(types)
                    .enumerate()
                    .map(|(index, (value, ty))| reify(value, ty, &format!("{at}[{index}]")))
                    .collect::<Result<_>>()?,
            )
        }
        (Value::Map(values), StreamType::Map(inner)) => Value::Map(
            values
                .into_iter()
                .map(|(name, value)| {
                    let value = reify(value, inner, &format!("{at}.{name}"))?;
                    Ok((name, value))
                })
                .collect::<Result<_>>()?,
        ),
        (Value::Union(value), StreamType::Union(schema)) => {
            let Some((_, arm)) = schema.alternative(value.tag()) else {
                bail!(
                    "RPIO {at}: tag `{}` is not declared in {schema}",
                    value.tag()
                );
            };
            let payload = match (value.payload(), arm.payload()) {
                (None, UnionPayload::Nullary) => None,
                (Some(payload), UnionPayload::Of(ty)) => {
                    Some(reify(payload.clone(), ty, &format!("{at}.payload"))?)
                }
                _ => bail!(
                    "RPIO {at}: tag `{}` has the wrong payload presence",
                    value.tag()
                ),
            };
            UnionValue::new(value.tag().clone(), payload).into()
        }
        (Value::Newtype(value), StreamType::Newtype(schema)) => {
            if value.brand() != schema.brand() {
                bail!(
                    "RPIO {at}: newtype brand `{}` differs from declared `{}`",
                    value.brand(),
                    schema.brand()
                );
            }
            NewtypeValue::new(
                value.brand().clone(),
                reify(value.value().clone(), schema.repr(), &format!("{at}.value"))?,
            )
            .into()
        }
        (value, ty) => {
            check_value_conformance(&value, ty)
                .with_context(|| format!("RPIO {at}: expected {ty}"))?;
            value
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{NewtypeSchema, UnionAlternative};
    use ecow::EcoVec;
    use serde_json::json;

    fn structure(fields: Vec<(&str, StreamType)>, extra: bool) -> StreamType {
        StreamType::Struct(
            fields
                .into_iter()
                .map(|(name, ty)| (name.into(), ty))
                .collect(),
            extra,
        )
    }

    fn union(arms: Vec<(&str, UnionPayload<StreamType>)>) -> StreamType {
        StreamType::Union(
            ClosedUnion::new(
                arms.into_iter()
                    .map(|(name, payload)| UnionAlternative::new(name, payload)),
            )
            .unwrap(),
        )
    }

    fn option(inner: StreamType) -> StreamType {
        union(vec![
            ("None", UnionPayload::Nullary),
            ("Some", UnionPayload::Of(inner)),
        ])
    }

    fn instant() -> StreamType {
        StreamType::Newtype(NewtypeSchema::new(
            "std::time::Instant<JournalClock>",
            EcoVec::new(),
            StreamType::Float,
        ))
    }

    fn observation_type(record: StreamType) -> Option<StreamType> {
        Some(structure(
            vec![
                (
                    "record",
                    option(structure(
                        vec![("time", instant()), ("value", record)],
                        false,
                    )),
                ),
                ("clock", option(structure(vec![("time", instant())], true))),
            ],
            false,
        ))
    }

    fn record(value: serde_json::Value) -> Value {
        serde_json::from_value(json!({
            "record": {"$tag":"Some", "payload": {
                "time":{"$newtype":"std::time::Instant<JournalClock>", "value":1},
                "value":value}}, "clock":{"$tag":"None"}
        }))
        .unwrap()
    }

    fn knowledge_type() -> Option<StreamType> {
        let domain = structure(
            vec![
                ("mean", StreamType::Float),
                ("unused", StreamType::Bool),
                ("note", option(StreamType::Str)),
            ],
            true,
        );
        observation_type(union(vec![(
            "Knowledge",
            UnionPayload::Of(structure(
                vec![(
                    "version",
                    structure(
                        vec![("value", union(vec![("Sensor", UnionPayload::Of(domain))]))],
                        true,
                    ),
                )],
                true,
            )),
        )]))
    }

    fn knowledge(fields: serde_json::Value) -> Value {
        record(json!({"$tag":"Knowledge", "payload":{"version":{
            "value":{"$tag":"Sensor", "payload":fields}}}}))
    }

    #[test]
    fn every_declared_domain_field_is_checked_even_if_unused_by_a_monitor() {
        let bad = knowledge(json!({"mean":2, "unused":"true"}));
        let error = observation(bad, &knowledge_type()).unwrap_err();
        assert!(format!("{error:#}").contains("version.value.payload.unused"));
        assert!(format!("{error:#}").contains("Bool"));
    }

    #[test]
    fn missing_optional_fields_and_integer_written_floats_have_declared_values() {
        let got = observation(
            knowledge(json!({"mean":2, "unused":true})),
            &knowledge_type(),
        )
        .unwrap();
        let encoded = serde_json::to_value(got).unwrap();
        let fields =
            &encoded["record"]["payload"]["value"]["payload"]["version"]["value"]["payload"];
        assert_eq!(fields["note"], json!({"$tag":"None"}));
        assert_eq!(fields["mean"], json!(2.0));
        assert_eq!(encoded["record"]["payload"]["time"]["value"], json!(1.0));
    }

    #[test]
    fn tagged_options_and_open_extras_are_preserved_but_malformed_options_are_refused() {
        let valid = knowledge(json!({"mean":2, "unused":true,
            "note":{"$tag":"Some", "payload":"caption"}, "extra":{"calibration":3}}));
        let encoded = serde_json::to_value(observation(valid, &knowledge_type()).unwrap()).unwrap();
        let fields =
            &encoded["record"]["payload"]["value"]["payload"]["version"]["value"]["payload"];
        assert_eq!(fields["note"], json!({"$tag":"Some", "payload":"caption"}));
        assert_eq!(fields["extra"], json!({"calibration":3}));
        for malformed in [
            json!(null),
            json!({"$tag":"Missing"}),
            json!({"$tag":"None", "payload":null}),
        ] {
            let error = observation(
                knowledge(json!({"mean":2,"unused":true,"note":malformed})),
                &knowledge_type(),
            )
            .unwrap_err();
            assert!(format!("{error:#}").contains("note"));
        }
    }

    #[test]
    fn wrong_newtype_brands_and_unbranded_clock_values_are_refused() {
        let value = knowledge(json!({"mean":2, "unused":true}));
        let encoded = serde_json::to_value(value).unwrap();
        for time in [
            json!({"$newtype":"std::time::Instant<OtherClock>","value":1}),
            json!(1),
        ] {
            let mut bad = encoded.clone();
            bad["record"]["payload"]["time"] = time;
            let error =
                observation(serde_json::from_value(bad).unwrap(), &knowledge_type()).unwrap_err();
            assert!(format!("{error:#}").contains("record.payload.time"));
        }
    }

    #[test]
    fn undeclared_knowledge_tags_and_wrong_primitives_are_refused() {
        let bad = record(json!({"$tag":"Knowledge", "payload":{"version":{
            "value":{"$tag":"Other", "payload":{"mean":2,"unused":true}}}}}));
        assert!(
            format!("{:#}", observation(bad, &knowledge_type()).unwrap_err()).contains("Other")
        );
        let bad = knowledge(json!({"mean":"2", "unused":true}));
        assert!(format!("{:#}", observation(bad, &knowledge_type()).unwrap_err()).contains("mean"));
    }

    #[test]
    fn triggers_and_emitted_message_fields_are_checked_by_the_same_full_type() {
        let message = structure(vec![("rate", StreamType::Float)], true);
        let detail = union(vec![("Changed", UnionPayload::Nullary)]);
        let emitted = structure(
            vec![("detail", detail), ("message", message.clone())],
            false,
        );
        let trigger = union(vec![("Received", UnionPayload::Of(message))]);
        let step = union(vec![
            (
                "Start",
                UnionPayload::Of(structure(vec![("trigger", trigger)], true)),
            ),
            (
                "Event",
                UnionPayload::Of(structure(vec![("payload", emitted)], true)),
            ),
        ]);
        let declared = observation_type(union(vec![(
            "Phase",
            UnionPayload::Of(structure(vec![("step", step)], true)),
        )]));
        let event = |rate| {
            record(json!({"$tag":"Phase", "payload":{"step":{"$tag":"Event",
            "payload":{"payload":{"detail":{"$tag":"Changed"},"message":{"rate":rate}}}}}}))
        };
        assert!(observation(event(json!(3)), &declared).is_ok());
        assert!(
            format!(
                "{:#}",
                observation(event(json!("3")), &declared).unwrap_err()
            )
            .contains("message.rate")
        );
        let wrong_detail = record(json!({"$tag":"Phase", "payload":{"step":{"$tag":"Event",
            "payload":{"payload":{"detail":{"$tag":"Other"},"message":{"rate":3}}}}}}));
        assert!(
            format!("{:#}", observation(wrong_detail, &declared).unwrap_err()).contains("detail")
        );
        let start = record(json!({"$tag":"Phase", "payload":{"step":{"$tag":"Start",
            "payload":{"trigger":{"$tag":"Received","payload":{"rate":false}}}}}}));
        assert!(
            format!("{:#}", observation(start, &declared).unwrap_err())
                .contains("trigger.payload.rate")
        );
    }
}
