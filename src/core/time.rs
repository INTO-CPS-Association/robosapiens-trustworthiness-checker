//! Time domains, clock samples, and the times inputs carry.
//!
//! A *time domain* is one clock's notion of time: the representation of its
//! instants and the label its producers stamp. It is the Rust side of the
//! DSRV domain parameter `D` in `std::time::Instant<D>`, so a value of one
//! domain cannot be compared with a value of another by construction: every
//! type here is parameterised by its domain.
//!
//! Time is read from input in two ways, and neither validates order:
//!
//! - a native typed value that carries its own time implements
//!   [`TimedInput`], answering whether a value has a time at all;
//! - a dynamic [`Value`] is decoded by `TryFrom<&Value>` onto a small target
//!   type that names both the schema and the domain, such as [`ClockSample`]
//!   here and the RPIO journal's arrival and envelope times beside their
//!   decoders. Decoding is stateless: whether a decoded label is the one a
//!   source is bound to is the source's check, not the decoder's.
//!
//! A time that goes backwards is decoded like any other. Whether it is a
//! fault is a property's question, so no decoder hides it.
//!
//! An absent time and a time of zero are different answers: absence is
//! `None` (or [`Value::NoVal`]), and zero is `Some(0.0)`.

use std::fmt::{self, Debug};

use ecow::{EcoString, EcoVec};

use super::{NewtypeSchema, NewtypeValue, StreamType, Value, canonical_brand, canonical_type_text};

/// The canonical path of the DSRV instant newtype. An instant in domain `D`
/// carries the brand this stem makes with `D`, as in
/// `std::time::Instant<app::JournalTime>`.
pub const INSTANT_STEM: &str = "std::time::Instant";

/// One clock's notion of time.
///
/// `Instant` is deliberately neither `Ord` nor `Copy`: a domain need not be
/// totally ordered, and an instant need not be a number. The label is
/// runtime data, so one Rust type can stand for every domain a
/// specification declares.
pub trait TimeDomain: 'static {
    type Instant: Clone + PartialEq + Debug;

    /// The clock label producers in this domain stamp, as in `journal_clock`.
    fn label(&self) -> &str;
}

/// A DSRV-visible domain: instants are `Float` seconds, wrapped in the brand
/// of `std::time::Instant<D>` for the domain marker `D` the specification
/// declares.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondsDomain {
    marker: EcoString,
    label: EcoString,
    instant_brand: EcoString,
}

impl SecondsDomain {
    /// The domain whose DSRV marker type has canonical spelling `marker`
    /// (`JournalTime` for a root-module `newtype JournalTime = Unit`) and whose
    /// producers stamp `label`.
    pub fn new(marker: impl Into<EcoString>, label: impl Into<EcoString>) -> Self {
        let marker = marker.into();
        // A newtype argument spells as its brand, whatever it holds, so a
        // stand-in schema over `Unit` reproduces the brand the specification's
        // own `Instant<D>` makes.
        let argument = StreamType::Newtype(NewtypeSchema::new(
            marker.clone(),
            EcoVec::new(),
            StreamType::Unit,
        ));
        let instant_brand = canonical_brand(INSTANT_STEM, std::slice::from_ref(&argument));
        Self {
            marker,
            label: label.into(),
            instant_brand,
        }
    }

    /// The canonical spelling of the domain marker `D`.
    pub fn marker(&self) -> &str {
        &self.marker
    }

    /// The brand instants in this domain carry.
    pub fn instant_brand(&self) -> &str {
        &self.instant_brand
    }

    /// `seconds` as a DSRV `std::time::Instant<D>` value.
    pub fn instant(&self, seconds: f64) -> Value {
        NewtypeValue::new(self.instant_brand.clone(), Value::Float(seconds)).into()
    }

    /// The domain marker a declared instant type names: `Some(D)` for
    /// `std::time::Instant<D>`, and `None` for a type that names no
    /// newtype (such as `Any`), which therefore says nothing about the
    /// domain. Another newtype in an instant's place is refused, because
    /// the instants delivered there would not be of that type.
    pub fn marker_of_instant(declared: &StreamType) -> Result<Option<EcoString>, TimeInputError> {
        let StreamType::Newtype(schema) = declared else {
            return Ok(None);
        };
        match schema.arguments() {
            [domain] if schema.stem() == INSTANT_STEM => {
                Ok(Some(canonical_type_text(domain).into()))
            }
            _ => Err(TimeInputError::Mismatch {
                what: "declared instant type",
                expected: format!("{INSTANT_STEM}<D>"),
                found: schema.brand().to_string(),
            }),
        }
    }
}

impl TimeDomain for SecondsDomain {
    type Instant = f64;

    fn label(&self) -> &str {
        &self.label
    }
}

/// The identity of a clock that samples time, such as the host clock.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClockId(EcoString);

impl ClockId {
    /// The clock of the host the checker runs on.
    pub const HOST: &str = "host";

    pub fn new(id: impl Into<EcoString>) -> Self {
        Self(id.into())
    }

    pub fn host() -> Self {
        Self::new(Self::HOST)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ClockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// How a clock sample came to be taken.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SampleOrigin {
    /// Read from a running clock by a periodic timer.
    Periodic,
    /// Supplied by a finite replay, which stands in for the samples a run took.
    Replay,
}

impl SampleOrigin {
    /// The variant's name, which is how DSRV sees it: `origin` is a `Str`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Periodic => "Periodic",
            Self::Replay => "Replay",
        }
    }

    fn parse(name: &str) -> Option<Self> {
        match name {
            "Periodic" => Some(Self::Periodic),
            "Replay" => Some(Self::Replay),
            _ => None,
        }
    }
}

impl fmt::Display for SampleOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One reading of a clock in domain `D`. A sample reports the time a clock
/// showed when it was read; it certifies nothing about records stamped
/// earlier.
#[derive(Clone, Debug, PartialEq)]
pub struct ClockSample<D: TimeDomain> {
    pub clock: ClockId,
    pub time: D::Instant,
    pub origin: SampleOrigin,
}

impl ClockSample<SecondsDomain> {
    /// The sample as the DSRV value `std::time::ClockSample<D>`.
    pub fn to_value(&self, domain: &SecondsDomain) -> Value {
        Value::Map(
            [
                ("clock".into(), Value::Str(self.clock.0.clone())),
                ("time".into(), domain.instant(self.time)),
                ("origin".into(), Value::Str(self.origin.as_str().into())),
            ]
            .into(),
        )
    }
}

/// Decodes the DSRV shape `Struct<clock: Str, time: Instant<D>, origin: Str>`.
/// The time may also be a bare `Float`; which brand it carries is the
/// source's check, since a decoder has no domain to compare it with.
impl<D: TimeDomain<Instant = f64>> TryFrom<&Value> for ClockSample<D> {
    type Error = TimeInputError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        const WHAT: &str = "a clock sample";
        let Value::Map(fields) = value else {
            return Err(TimeInputError::wrong_type(WHAT, value, "a struct"));
        };
        let field = |name: &'static str| {
            fields.get(name).ok_or(TimeInputError::MissingField {
                what: WHAT,
                field: name,
            })
        };
        let clock = match field("clock")? {
            Value::Str(clock) => ClockId::new(clock.clone()),
            other => return Err(TimeInputError::wrong_type("clock", other, "a string")),
        };
        let time = seconds("time", field("time")?)?;
        let origin = match field("origin")? {
            Value::Str(origin) => {
                SampleOrigin::parse(origin).ok_or_else(|| TimeInputError::WrongType {
                    field: "origin",
                    found: format!("`{origin}`"),
                    expected: "`Periodic` or `Replay`",
                })?
            }
            other => return Err(TimeInputError::wrong_type("origin", other, "a string")),
        };
        Ok(Self {
            clock,
            time,
            origin,
        })
    }
}

/// A finite number of seconds from a `Float`, an `Int`, or an instant
/// newtype holding one.
pub fn seconds(field: &'static str, value: &Value) -> Result<f64, TimeInputError> {
    let seconds = match value {
        Value::Float(seconds) => *seconds,
        Value::Int(seconds) => *seconds as f64,
        Value::Newtype(instant) => return seconds(field, instant.value()),
        other => return Err(TimeInputError::wrong_type(field, other, "a number")),
    };
    if seconds.is_finite() {
        Ok(seconds)
    } else {
        Err(TimeInputError::NonFinite {
            field,
            value: seconds,
        })
    }
}

/// A native typed value that may carry its own time.
///
/// `None` means the value has no time, as a missing sample has none; it is
/// never a stand-in for zero.
pub trait TimedInput: super::StreamData {
    type Domain: TimeDomain;

    fn input_time(&self) -> Option<&<Self::Domain as TimeDomain>::Instant>;
}

/// Why a time could not be read from an input.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum TimeInputError {
    #[error("{what} has no `{field}`")]
    MissingField {
        what: &'static str,
        field: &'static str,
    },
    #[error("`{field}` is {found}, expected {expected}")]
    WrongType {
        field: &'static str,
        found: String,
        expected: &'static str,
    },
    #[error("`{field}` is {value}, and a time must be finite")]
    NonFinite { field: &'static str, value: f64 },
    #[error("the {what} is `{found}`, but this source is bound to `{expected}`")]
    Mismatch {
        what: &'static str,
        expected: String,
        found: String,
    },
}

impl TimeInputError {
    pub fn wrong_type(field: &'static str, found: &Value, expected: &'static str) -> Self {
        Self::WrongType {
            field,
            found: format!("{found:?}"),
            expected,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn journal() -> SecondsDomain {
        SecondsDomain::new("JournalTime", "fixture.journal")
    }

    #[test]
    fn an_instant_carries_the_brand_the_specification_makes() {
        let domain = SecondsDomain::new("app::JournalTime", "redis.server.time");
        assert_eq!(
            domain.instant_brand(),
            "std::time::Instant<app::JournalTime>"
        );
        assert_eq!(domain.label(), "redis.server.time");
        let Value::Newtype(instant) = domain.instant(1.5) else {
            panic!("an instant is a newtype value")
        };
        assert_eq!(instant.brand(), "std::time::Instant<app::JournalTime>");
        assert_eq!(instant.value(), &Value::Float(1.5));
    }

    #[test]
    fn a_declared_instant_names_its_domain_and_nothing_else_does() {
        let marker = |brand: &str| {
            StreamType::Newtype(NewtypeSchema::new(brand, EcoVec::new(), StreamType::Unit))
        };
        let instant = |stem: &str, domain: &str| {
            let argument = marker(domain);
            StreamType::Newtype(NewtypeSchema::new(
                canonical_brand(stem, std::slice::from_ref(&argument)),
                EcoVec::from([argument]),
                StreamType::Float,
            ))
        };
        assert_eq!(
            SecondsDomain::marker_of_instant(&instant(INSTANT_STEM, "app::JournalTime")).unwrap(),
            Some("app::JournalTime".into())
        );
        assert_eq!(
            SecondsDomain::marker_of_instant(&StreamType::Any).unwrap(),
            None
        );
        let error = SecondsDomain::marker_of_instant(&instant("timing::Instant", "JournalTime"))
            .unwrap_err();
        assert!(
            error.to_string().contains("timing::Instant<JournalTime>"),
            "{error}"
        );
        assert!(SecondsDomain::marker_of_instant(&marker("JournalTime")).is_err());
    }

    #[test]
    fn a_clock_sample_round_trips_through_its_dsrv_shape() {
        let domain = journal();
        let sample = ClockSample::<SecondsDomain> {
            clock: ClockId::host(),
            time: 12.0,
            origin: SampleOrigin::Replay,
        };
        let value = sample.to_value(&domain);
        assert_eq!(
            ClockSample::<SecondsDomain>::try_from(&value).unwrap(),
            sample
        );
    }

    #[test]
    fn a_clock_sample_at_zero_is_present() {
        let domain = journal();
        let sample = ClockSample::<SecondsDomain> {
            clock: ClockId::host(),
            time: 0.0,
            origin: SampleOrigin::Periodic,
        };
        let decoded = ClockSample::<SecondsDomain>::try_from(&sample.to_value(&domain)).unwrap();
        assert_eq!(decoded.time, 0.0);
        assert!(matches!(
            ClockSample::<SecondsDomain>::try_from(&Value::NoVal),
            Err(TimeInputError::WrongType { .. })
        ));
    }

    #[test]
    fn clock_sample_decoding_names_what_is_wrong() {
        let sample = |time: Value, origin: &str| {
            Value::Map(
                [
                    ("clock".into(), Value::Str("host".into())),
                    ("time".into(), time),
                    ("origin".into(), Value::Str(origin.into())),
                ]
                .into(),
            )
        };
        let decode = |value: &Value| ClockSample::<SecondsDomain>::try_from(value).unwrap_err();
        assert_eq!(
            decode(&Value::Map(
                [("clock".into(), Value::Str("host".into()))].into()
            )),
            TimeInputError::MissingField {
                what: "a clock sample",
                field: "time"
            }
        );
        assert!(matches!(
            decode(&sample(Value::Str("soon".into()), "Replay")),
            TimeInputError::WrongType { field: "time", .. }
        ));
        assert!(matches!(
            decode(&sample(Value::Float(f64::INFINITY), "Replay")),
            TimeInputError::NonFinite { field: "time", .. }
        ));
        assert!(matches!(
            decode(&sample(Value::Float(1.0), "Guessed")),
            TimeInputError::WrongType {
                field: "origin",
                ..
            }
        ));
    }

    #[test]
    fn a_backwards_time_is_decoded_like_any_other() {
        let domain = journal();
        let samples = [5.0, 3.0].map(|time| {
            ClockSample::<SecondsDomain>::try_from(
                &ClockSample::<SecondsDomain> {
                    clock: ClockId::host(),
                    time,
                    origin: SampleOrigin::Replay,
                }
                .to_value(&domain),
            )
            .unwrap()
            .time
        });
        assert_eq!(samples, [5.0, 3.0]);
    }
}
