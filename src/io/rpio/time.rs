//! The times an RPIO observation journal carries.
//!
//! A journal record carries its admission time as `arrival.journal_time`,
//! and the Redis journal entry that holds it repeats that time beside the
//! label of the clock that stamped it, `journal_clock`. Journal admission is
//! the timestamp authority: a producer's own clock never stands in for it.
//!
//! Both decoders are stateless `TryFrom<&Value>` conversions onto a target
//! that names its schema and its domain. Whether a label is the one a source
//! is bound to is decided by the source that owns the label check.

use std::marker::PhantomData;

use ecow::EcoString;

use crate::core::Value;
use crate::core::time::{TimeDomain, TimeInputError, seconds};

/// The journal admission time of a record, if there is a record.
///
/// Reads a `Record` (`Phase`, `Knowledge` or `Incomplete`, each with its
/// `arrival`), or an `Option` of one. `None`, like [`Value::NoVal`], is the
/// absence of a record and so of a time; a record admitted at zero reads
/// `Some(0.0)`.
#[derive(Debug)]
pub struct JournalArrival<D: TimeDomain<Instant = f64>>(pub Option<D::Instant>);

impl<D: TimeDomain<Instant = f64>> TryFrom<&Value> for JournalArrival<D> {
    type Error = TimeInputError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let record = match value {
            Value::NoVal => return Ok(Self(None)),
            Value::Union(record) => record,
            other => return Err(TimeInputError::wrong_type("record", other, "a Record")),
        };
        match (record.tag().as_str(), record.payload()) {
            ("None", None) => Ok(Self(None)),
            ("Some", Some(record)) => Self::try_from(record),
            ("Phase" | "Knowledge" | "Incomplete", Some(Value::Map(fields))) => {
                let arrival = fields.get("arrival").ok_or(TimeInputError::MissingField {
                    what: "a journal record",
                    field: "arrival",
                })?;
                let Value::Map(arrival) = arrival else {
                    return Err(TimeInputError::wrong_type("arrival", arrival, "a struct"));
                };
                let time = arrival
                    .get("journal_time")
                    .ok_or(TimeInputError::MissingField {
                        what: "a record's arrival",
                        field: "journal_time",
                    })?;
                Ok(Self(Some(seconds("journal_time", time)?)))
            }
            _ => Err(TimeInputError::wrong_type("record", value, "a Record")),
        }
    }
}

/// The metadata of one journal entry: the admission time and the label of
/// the clock that stamped it, as `{journal_time, journal_clock}`.
#[derive(Debug)]
pub struct JournalEnvelope<D: TimeDomain<Instant = f64>> {
    pub time: D::Instant,
    pub clock: EcoString,
    domain: PhantomData<D>,
}

impl<D: TimeDomain<Instant = f64>> TryFrom<&Value> for JournalEnvelope<D> {
    type Error = TimeInputError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        const WHAT: &str = "a journal entry";
        let Value::Map(fields) = value else {
            return Err(TimeInputError::wrong_type(
                "journal entry",
                value,
                "a struct",
            ));
        };
        let field = |name: &'static str| {
            fields.get(name).ok_or(TimeInputError::MissingField {
                what: WHAT,
                field: name,
            })
        };
        let time = seconds("journal_time", field("journal_time")?)?;
        let clock = match field("journal_clock")? {
            Value::Str(clock) => clock.clone(),
            other => {
                return Err(TimeInputError::wrong_type(
                    "journal_clock",
                    other,
                    "a string",
                ));
            }
        };
        Ok(Self {
            time,
            clock,
            domain: PhantomData,
        })
    }
}

/// Checks that a source's journal clock labels are the one it is bound to.
///
/// A label is compared on the first entry and again whenever it differs
/// from the previous entry's, so a source that changes clock mid-stream is
/// refused at the entry that changed it.
#[derive(Debug)]
pub(crate) struct LabelCheck {
    expected: EcoString,
    last: Option<EcoString>,
}

impl LabelCheck {
    pub(crate) fn new(expected: impl Into<EcoString>) -> Self {
        Self {
            expected: expected.into(),
            last: None,
        }
    }

    pub(crate) fn check(&mut self, label: &EcoString) -> Result<(), TimeInputError> {
        if self.last.as_ref() == Some(label) {
            return Ok(());
        }
        if *label != self.expected {
            return Err(TimeInputError::Mismatch {
                what: "journal_clock label",
                expected: self.expected.to_string(),
                found: label.to_string(),
            });
        }
        self.last = Some(label.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::core::ValueMap;

    use super::*;
    use crate::core::UnionValue;
    use crate::core::time::SecondsDomain;

    type Arrival = JournalArrival<SecondsDomain>;
    type Envelope = JournalEnvelope<SecondsDomain>;

    fn record(tag: &str, journal_time: Value) -> Value {
        UnionValue::new(
            tag,
            Some(Value::Map(ValueMap::from([(
                "arrival".into(),
                Value::Map(ValueMap::from([("journal_time".into(), journal_time)])),
            )]))),
        )
        .into()
    }

    fn option(value: Option<Value>) -> Value {
        UnionValue::new(if value.is_some() { "Some" } else { "None" }, value).into()
    }

    #[test]
    fn an_arrival_at_zero_is_present_and_no_record_is_absent() {
        for tag in ["Phase", "Knowledge", "Incomplete"] {
            assert_eq!(
                Arrival::try_from(&record(tag, Value::Float(0.0)))
                    .unwrap()
                    .0,
                Some(0.0)
            );
        }
        assert_eq!(Arrival::try_from(&Value::NoVal).unwrap().0, None);
        assert_eq!(Arrival::try_from(&option(None)).unwrap().0, None);
        assert_eq!(
            Arrival::try_from(&option(Some(record("Phase", Value::Float(0.0)))))
                .unwrap()
                .0,
            Some(0.0)
        );
    }

    #[test]
    fn arrival_decoding_names_what_is_wrong() {
        let missing: Value = UnionValue::new("Phase", Some(Value::Map(ValueMap::new()))).into();
        assert_eq!(
            Arrival::try_from(&missing).unwrap_err(),
            TimeInputError::MissingField {
                what: "a journal record",
                field: "arrival"
            }
        );
        assert!(matches!(
            Arrival::try_from(&record("Phase", Value::Str("late".into()))),
            Err(TimeInputError::WrongType {
                field: "journal_time",
                ..
            })
        ));
        assert!(matches!(
            Arrival::try_from(&record("Phase", Value::Float(f64::NAN))),
            Err(TimeInputError::NonFinite {
                field: "journal_time",
                ..
            })
        ));
        assert!(matches!(
            Arrival::try_from(&record("Unknown", Value::Float(1.0))),
            Err(TimeInputError::WrongType {
                field: "record",
                ..
            })
        ));
    }

    #[test]
    fn records_are_decoded_in_order_even_when_time_goes_backwards() {
        let times: Vec<_> = [3.0, 1.0, 2.0]
            .into_iter()
            .map(|time| {
                Arrival::try_from(&record("Phase", Value::Float(time)))
                    .unwrap()
                    .0
                    .unwrap()
            })
            .collect();
        assert_eq!(times, [3.0, 1.0, 2.0]);
    }

    fn envelope(fields: &[(&str, Value)]) -> Value {
        Value::Map(
            fields
                .iter()
                .map(|(name, value)| ((*name).into(), value.clone()))
                .collect(),
        )
    }

    #[test]
    fn an_envelope_carries_its_time_and_clock_label() {
        let read = Envelope::try_from(&envelope(&[
            ("journal_time", Value::Float(0.0)),
            ("journal_clock", Value::Str("redis.server.time".into())),
        ]))
        .unwrap();
        assert_eq!(read.time, 0.0);
        assert_eq!(read.clock, "redis.server.time");
        assert_eq!(
            Envelope::try_from(&envelope(&[("journal_time", Value::Float(1.0))])).unwrap_err(),
            TimeInputError::MissingField {
                what: "a journal entry",
                field: "journal_clock"
            }
        );
        assert!(matches!(
            Envelope::try_from(&envelope(&[
                ("journal_time", Value::Str("1.0x".into())),
                ("journal_clock", Value::Str("c".into())),
            ])),
            Err(TimeInputError::WrongType {
                field: "journal_time",
                ..
            })
        ));
        assert!(matches!(
            Envelope::try_from(&envelope(&[
                ("journal_time", Value::Float(f64::INFINITY)),
                ("journal_clock", Value::Str("c".into())),
            ])),
            Err(TimeInputError::NonFinite { .. })
        ));
    }

    #[test]
    fn a_label_is_checked_first_and_whenever_it_changes() {
        let mut check = LabelCheck::new("redis.server.time");
        let bound = EcoString::from("redis.server.time");
        check.check(&bound).unwrap();
        check.check(&bound).unwrap();
        let error = check.check(&EcoString::from("time.monotonic")).unwrap_err();
        assert_eq!(
            error.to_string(),
            "the journal_clock label is `time.monotonic`, but this source is bound to \
             `redis.server.time`"
        );
        let mut check = LabelCheck::new("redis.server.time");
        assert!(check.check(&EcoString::from("time.monotonic")).is_err());
    }
}
