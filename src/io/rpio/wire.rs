//! Reading one record of an observation journal.
//!
//! The decoder checks structure and nothing else: that a record is the shape
//! the journal declares, that the names it uses correspond, and that an
//! emission it names was published. Sequencing and grouping are not wire
//! rules — which cycle a record belongs to, whether roles ran in order, and
//! whether an episode is open are the MAPLE-K library's derivation from the
//! edges, or a profile's properties.
//!
//! What a record means is declared in DSRV, in `rpio::journal`; this module
//! produces exactly those values, with one exception it does not hide: a
//! record names Knowledge versions rather than carrying their values, so the
//! wire writes a bare `Version` where the contract declares a `Resolved`,
//! and the decoder lifts it to a `Resolved` holding only its `ref`. The
//! Knowledge resolver supplies the value. Until it has, the record is not
//! yet a well-typed `Record`.

use std::collections::BTreeMap;

use ecow::{EcoString, EcoVec};

use crate::core::{UnionValue, Value, ValueMap};

use super::json::{JsonError, JsonValue};

/// The five phases of a MAPLE-K loop, as the journal tags them.
const ROLES: [&str; 5] = ["Monitor", "Analysis", "Plan", "Legitimate", "Execute"];

/// Which outcome each role ends with. Every role may also end `Aborted`.
const ROLE_OUTCOME: [(&str, &str); 5] = [
    ("Monitor", "Monitored"),
    ("Analysis", "Analysed"),
    ("Plan", "Planned"),
    ("Legitimate", "Legitimised"),
    ("Execute", "Executed"),
];

const ABORTED: &str = "Aborted";

/// The value schema this decoder reads. A journal of another version is
/// refused rather than read for whatever happens to match.
pub const SCHEMA_VERSION: u32 = 2;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum WireError {
    #[error("malformed journal record: {0}")]
    Json(#[from] JsonError),
    #[error("{what} is missing from {record}")]
    Missing {
        what: &'static str,
        record: &'static str,
    },
    #[error("{what} is {found}, expected {expected}")]
    Shape {
        what: &'static str,
        found: String,
        expected: &'static str,
    },
    #[error("{what} is empty, and an identity names something")]
    EmptyIdentity { what: &'static str },
    #[error("`{found}` is not a role; the roles are {}", ROLES.join(", "))]
    UnknownRole { found: EcoString },
    #[error("a {role} activation cannot end `{found}`")]
    OutcomeNotOfRole { role: EcoString, found: EcoString },
    #[error("the event is named `{name}` but its detail is tagged `{tag}`")]
    EventNameMismatch { name: EcoString, tag: EcoString },
    #[error("the activation was woken by `{emitted}` but its trigger is tagged `{trigger}`")]
    TriggerMismatch {
        trigger: EcoString,
        emitted: EcoString,
    },
    #[error("this journal is version {found}, and only version {expected} is read")]
    SchemaVersion { found: String, expected: u32 },
    #[error("`{tag}` carries no payload, but one was written")]
    NullaryWithPayload { tag: EcoString },
    #[error("the Knowledge value is tagged `{tag}` but its key is `{key}`")]
    KnowledgeKeyMismatch { tag: EcoString, key: EcoString },
    #[error(
        "activation `{activation}` published emission {found}, expected {expected}: an emission \
         after a gap cannot be named"
    )]
    OrdinalGap {
        activation: EcoString,
        found: i64,
        expected: i64,
    },
    #[error("emission {ordinal} of activation `{activation}` was never published")]
    UnpublishedEmission { activation: EcoString, ordinal: i64 },
    #[error(
        "activation `{activation}` is {found} here and was {first} before; one activation is \
         one run of one callback"
    )]
    ActivationChanged {
        activation: EcoString,
        first: String,
        found: String,
    },
    #[error("activation `{publisher}` cannot publish an emission of activation `{named}`")]
    EmissionNotOwned {
        publisher: EcoString,
        named: EcoString,
    },
}

/// Reads a journal's records, in the order the producer wrote them.
///
/// The decoder remembers the emissions each activation published, because an
/// activation names the emission that woke it and that emission was
/// published before it. A record naming an emission that has not been seen
/// is rejected rather than held: the journal is read in order.
///
/// Emissions are remembered per run, because an activation's identity is
/// unique within a run and may be reused by the next, so one journal holding
/// two runs must not let a record of one name an emission of the other.
#[derive(Debug, Default)]
pub struct WireDecoder {
    /// The event each of an activation's emissions was named by, in order,
    /// so an ordinal is both the gap check and the way back to the event.
    published: BTreeMap<(EcoString, EcoString), Vec<EcoString>>,
    /// The role and loop each activation was first seen with. An activation
    /// is one run of one phase callback, so its own records agree about
    /// which callback it was.
    activations: BTreeMap<(EcoString, EcoString), (EcoString, Value)>,
}

impl WireDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// One record of the phase stream, as the DSRV value it stands for.
    pub fn phase_record(&mut self, record: &JsonValue) -> Result<Value, WireError> {
        check_schema_version(record)?;
        let (tag, payload) = union_of(record, "the record")?;
        match tag.as_str() {
            "Phase" => self.phase(payload),
            "Incomplete" => {
                let activation_json = member(payload, "activation", "Incomplete")?;
                let activation = activation_of(activation_json)?;
                self.check_activation_identity(&activation)?;
                // An activation that did not finish still names what woke it.
                self.check_caused_by(activation_json)?;
                Ok(record_value(
                    "Incomplete",
                    [
                        ("activation", activation),
                        ("arrival", arrival_of(payload)?),
                    ],
                ))
            }
            _ => Err(WireError::Shape {
                what: "the record",
                found: tag.to_string(),
                expected: "`Phase` or `Incomplete`",
            }),
        }
    }

    /// One record of the Knowledge stream: a version and the value written.
    pub fn knowledge_record(&self, record: &JsonValue) -> Result<Value, WireError> {
        check_schema_version(record)?;
        let (tag, payload) = union_of(record, "the record")?;
        if tag != "Knowledge" {
            return Err(WireError::Shape {
                what: "the record",
                found: tag.to_string(),
                expected: "`Knowledge`",
            });
        }
        let version = member(payload, "version", "Knowledge")?;
        let reference = version_of(member(version, "ref", "the version")?)?;
        let value = member(version, "value", "the version")?.to_value()?;
        // The Knowledge union's tag is the key the value was written under,
        // so a value filed under another key is a malformed record.
        let Value::Union(union) = &value else {
            return Err(WireError::Shape {
                what: "a Knowledge value",
                found: value.to_string(),
                expected: "a value tagged by its Knowledge key",
            });
        };
        let key = reference_key(&reference);
        if union.tag() != &key {
            return Err(WireError::KnowledgeKeyMismatch {
                tag: union.tag().clone(),
                key,
            });
        }
        Ok(record_value(
            "Knowledge",
            [
                (
                    "version",
                    Value::Map(ValueMap::from([
                        ("ref".into(), reference),
                        ("value".into(), value),
                        (
                            "written_by".into(),
                            writer_of(member(version, "written_by", "the version")?)?,
                        ),
                    ])),
                ),
                ("arrival", arrival_of(payload)?),
            ],
        ))
    }

    fn phase(&mut self, payload: &JsonValue) -> Result<Value, WireError> {
        let activation_json = member(payload, "activation", "Phase")?;
        let activation = activation_of(activation_json)?;
        self.check_activation_identity(&activation)?;
        let woke_it = self.check_caused_by(activation_json)?;
        let step = self.step_of(
            member(payload, "step", "Phase")?,
            activation_json,
            woke_it.as_ref(),
        )?;
        Ok(record_value(
            "Phase",
            [
                ("activation", activation),
                ("step", step),
                ("knowledge", knowledge_refs_of(payload)?),
                ("arrival", arrival_of(payload)?),
            ],
        ))
    }

    fn step_of(
        &mut self,
        step: &JsonValue,
        activation: &JsonValue,
        woke_it: Option<&EcoString>,
    ) -> Result<Value, WireError> {
        let run_id = identity(activation, "run_id", "the activation")?;
        let activation_id = identity(activation, "activation_id", "the activation")?;
        let role = string_member(activation, "role", "the activation")?;
        let (tag, payload) = union_of(step, "the step")?;
        let fields: EcoVec<(EcoString, Value)> = match tag.as_str() {
            "Start" => {
                // The trigger is tagged by the event that woke the
                // activation, so it must name the same event the emission
                // did. An activation the loop did not wake — a message from
                // outside, or a callback called directly — names no
                // emission, and there is nothing to compare it with.
                let trigger = member(payload, "trigger", "Start")?.to_value()?;
                let trigger_tag = union_tag(&trigger, "a trigger")?;
                if let Some(emitted) = woke_it
                    && &trigger_tag != emitted
                {
                    return Err(WireError::TriggerMismatch {
                        trigger: trigger_tag,
                        emitted: emitted.clone(),
                    });
                }
                eco_fields([("trigger", trigger)])
            }
            "Event" => {
                let name = identity(payload, "name", "Event")?;
                // The payload is the application's event struct; the event it
                // stands for is its `detail`.
                let event = member(payload, "payload", "Event")?.to_value()?;
                let detail = match &event {
                    Value::Map(members) => members.get("detail"),
                    _ => None,
                };
                let Some(detail) = detail else {
                    return Err(WireError::Shape {
                        what: "an event payload",
                        found: event.to_string(),
                        expected: "a struct holding a `detail` tagged by the event name",
                    });
                };
                let detail_tag = union_tag(detail, "an event detail")?;
                // The name is the tag written the way a reader writes names.
                if snake_case(&detail_tag) != name {
                    return Err(WireError::EventNameMismatch {
                        name,
                        tag: detail_tag,
                    });
                }
                let emission = emission_of(member(payload, "emission", "Event")?)?;
                self.publish(&run_id, &activation_id, &emission, &detail_tag)?;
                eco_fields([
                    ("name", Value::Str(name)),
                    ("payload", event.clone()),
                    ("emission", emission_value(&emission)),
                ])
            }
            "Read" | "Write" => {
                // New producers retain the accessed key even for an absent read.
                // Historical v2 records omitted it; their resolved reference still
                // identifies any present acquisition.
                let key = if optional_member(payload, "key").is_some() {
                    identity(payload, "key", "a Knowledge access")?
                } else {
                    EcoString::new()
                };
                eco_fields([("key", Value::Str(key))])
            }
            "ServiceStarted" => eco_fields([
                (
                    "service",
                    Value::Str(identity(payload, "service", "ServiceStarted")?),
                ),
                (
                    "request_id",
                    Value::Str(identity(payload, "request_id", "ServiceStarted")?),
                ),
            ]),
            "ServiceCompleted" => {
                let outcome = member(payload, "outcome", "ServiceCompleted")?;
                let (outcome_tag, outcome_payload) = union_of(outcome, "a service outcome")?;
                if outcome_tag != "Delivered" && outcome_tag != "NotDelivered" {
                    return Err(WireError::Shape {
                        what: "a service outcome",
                        found: outcome_tag.to_string(),
                        expected: "`Delivered` or `NotDelivered`",
                    });
                }
                require_nullary(&outcome_tag, outcome_payload)?;
                eco_fields([
                    (
                        "service",
                        Value::Str(identity(payload, "service", "ServiceCompleted")?),
                    ),
                    (
                        "request_id",
                        Value::Str(identity(payload, "request_id", "ServiceCompleted")?),
                    ),
                    ("outcome", nullary(outcome_tag)),
                ])
            }
            "End" => {
                let outcome = member(payload, "outcome", "End")?;
                eco_fields([("outcome", outcome_of(outcome, &role)?)])
            }
            _ => {
                return Err(WireError::Shape {
                    what: "the step",
                    found: tag.to_string(),
                    expected: "one of Start, Read, Write, Event, ServiceStarted, \
                               ServiceCompleted or End",
                });
            }
        };
        Ok(tagged(tag, Value::Map(fields.iter().cloned().collect())))
    }

    /// An activation is one run of one phase callback, so every record of it
    /// names the same role and the same loop. A record that says otherwise
    /// is describing a different activation under a name already taken.
    fn check_activation_identity(&mut self, activation: &Value) -> Result<(), WireError> {
        let Value::Map(members) = activation else {
            return Ok(());
        };
        let identity = |name: &str| match members.get(name) {
            Some(Value::Str(text)) => text.clone(),
            _ => EcoString::new(),
        };
        let role = match members.get("role") {
            Some(Value::Union(role)) => role.tag().clone(),
            _ => EcoString::new(),
        };
        let loop_path = members.get("loop").cloned().unwrap_or(Value::Unit);
        let key = (identity("run_id"), identity("activation_id"));
        match self.activations.get(&key) {
            None => {
                self.activations.insert(key, (role, loop_path));
                Ok(())
            }
            Some((first_role, first_loop)) if first_role == &role && first_loop == &loop_path => {
                Ok(())
            }
            Some((first_role, first_loop)) => Err(WireError::ActivationChanged {
                activation: key.1,
                first: format!("{first_role} of {first_loop}"),
                found: format!("{role} of {loop_path}"),
            }),
        }
    }

    /// The emission that woke this activation, if the loop woke it at all,
    /// checked against what has been published.
    fn check_caused_by(&self, activation: &JsonValue) -> Result<Option<EcoString>, WireError> {
        let Some(emission) = caused_by_of(activation)? else {
            return Ok(None);
        };
        let run_id = identity(activation, "run_id", "the activation")?;
        self.published_event(&run_id, &emission).map(Some)
    }

    /// Record an emission an activation published, in order.
    /// Record an emission an activation published, in order.
    fn publish(
        &mut self,
        run_id: &EcoString,
        activation_id: &EcoString,
        emission: &Emission,
        event: &EcoString,
    ) -> Result<(), WireError> {
        if &emission.activation != activation_id {
            return Err(WireError::EmissionNotOwned {
                publisher: activation_id.clone(),
                named: emission.activation.clone(),
            });
        }
        let published = self
            .published
            .entry((run_id.clone(), activation_id.clone()))
            .or_default();
        let expected = published.len() as i64 + 1;
        if emission.ordinal != expected {
            return Err(WireError::OrdinalGap {
                activation: activation_id.clone(),
                found: emission.ordinal,
                expected,
            });
        }
        published.push(event.clone());
        Ok(())
    }

    /// The event an emission was named by. Asking for it is also the check
    /// that it was published at all.
    fn published_event(
        &self,
        run_id: &EcoString,
        emission: &Emission,
    ) -> Result<EcoString, WireError> {
        usize::try_from(emission.ordinal)
            .ok()
            .filter(|ordinal| *ordinal >= 1)
            .and_then(|ordinal| {
                self.published
                    .get(&(run_id.clone(), emission.activation.clone()))?
                    .get(ordinal - 1)
                    .cloned()
            })
            .ok_or_else(|| WireError::UnpublishedEmission {
                activation: emission.activation.clone(),
                ordinal: emission.ordinal,
            })
    }
}

/// An emission's identity: which activation published it, and which of that
/// activation's emissions it is.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Emission {
    activation: EcoString,
    ordinal: i64,
}

fn emission_of(json: &JsonValue) -> Result<Emission, WireError> {
    let ordinal = integer_member(json, "ordinal", "an emission")?;
    if ordinal < 1 {
        return Err(WireError::Shape {
            what: "an emission ordinal",
            found: ordinal.to_string(),
            expected: "a positive number, counting from the activation's first",
        });
    }
    Ok(Emission {
        activation: identity(json, "activation_id", "an emission")?,
        ordinal,
    })
}

fn emission_value(emission: &Emission) -> Value {
    Value::Map(ValueMap::from([
        (
            "activation_id".into(),
            Value::Str(emission.activation.clone()),
        ),
        ("ordinal".into(), Value::Int(emission.ordinal)),
    ]))
}

fn caused_by_of(activation: &JsonValue) -> Result<Option<Emission>, WireError> {
    let caused_by = member(activation, "caused_by", "the activation")?;
    // Absent is written as the `None` of an Option, never as a bare null.
    let (tag, payload) = union_of(caused_by, "`caused_by`")?;
    match tag.as_str() {
        "None" => {
            require_nullary(&tag, payload)?;
            Ok(None)
        }
        "Some" => Ok(Some(emission_of(payload)?)),
        _ => Err(WireError::Shape {
            what: "`caused_by`",
            found: tag.to_string(),
            expected: "`Some` or `None`",
        }),
    }
}

fn activation_of(activation: &JsonValue) -> Result<Value, WireError> {
    let role = string_member(activation, "role", "the activation")?;
    if !ROLES.contains(&role.as_str()) {
        return Err(WireError::UnknownRole { found: role });
    }
    let loop_path = member(activation, "loop", "the activation")?;
    let JsonValue::Array(segments) = loop_path else {
        return Err(WireError::Shape {
            what: "a loop identity",
            found: loop_path.canonical_text(),
            expected: "a path, such as [\"robot1\", \"navigation\"]",
        });
    };
    if segments.is_empty() {
        return Err(WireError::EmptyIdentity {
            what: "a loop identity",
        });
    }
    let mut path = EcoVec::new();
    for segment in segments {
        let JsonValue::String(segment) = segment else {
            return Err(WireError::Shape {
                what: "a loop identity segment",
                found: segment.canonical_text(),
                expected: "a name",
            });
        };
        if segment.is_empty() {
            return Err(WireError::EmptyIdentity {
                what: "a loop identity segment",
            });
        }
        path.push(Value::Str(segment.clone()));
    }
    let caused_by = match caused_by_of(activation)? {
        Some(emission) => tagged("Some".into(), emission_value(&emission)),
        None => nullary("None".into()),
    };
    Ok(Value::Map(ValueMap::from([
        (
            "run_id".into(),
            Value::Str(identity(activation, "run_id", "the activation")?),
        ),
        ("loop".into(), Value::List(path)),
        (
            "producer".into(),
            Value::Str(if optional_member(activation, "producer").is_some() {
                identity(activation, "producer", "the activation")?
            } else {
                role.clone()
            }),
        ),
        ("role".into(), nullary(role)),
        (
            "activation_id".into(),
            Value::Str(identity(activation, "activation_id", "the activation")?),
        ),
        ("caused_by".into(), caused_by),
    ])))
}

/// An End outcome, checked against the role that ended and against the shape
/// `journal.dsrv` declares for it.
///
/// Every field an outcome declares is a Knowledge version, so what the
/// record names is lifted into a `Resolved` the Knowledge resolver
/// completes. Nothing is copied through unchecked: an arm that does not
/// belong to the union, a missing field, or a nullary arm carrying a payload
/// is a malformed record.
fn outcome_of(outcome: &JsonValue, role: &EcoString) -> Result<Value, WireError> {
    let (tag, payload) = union_of(outcome, "an outcome")?;
    let of_role = ROLE_OUTCOME
        .iter()
        .find(|(name, _)| name == role)
        .map(|(_, outcome)| *outcome);
    if tag != ABORTED && Some(tag.as_str()) != of_role {
        return Err(WireError::OutcomeNotOfRole {
            role: role.clone(),
            found: tag,
        });
    }
    let value = match tag.as_str() {
        "Monitored" => resolved_fields(payload, &["observation"], "Monitored")?,
        "Analysed" => nested(
            payload,
            "Analysed",
            &[("Nominal", &[]), ("Anomaly", &["evidence"])],
        )?,
        "Planned" => nested(
            payload,
            "Planned",
            &[("Generated", &["plan"]), ("NoPlan", &[])],
        )?,
        "Legitimised" => nested(
            payload,
            "Legitimised",
            &[
                ("Accepted", &["plan", "legitimate_result"]),
                ("Rejected", &["plan", "legitimate_result"]),
            ],
        )?,
        "Executed" => {
            let (arm, fields) = union_of(payload, "an `Executed` outcome")?;
            match arm.as_str() {
                "Applied" => tagged(
                    arm,
                    resolved_fields(fields, &["plan", "effect"], "Applied")?,
                ),
                // The reason a plan was not applied is not a version but a
                // reason, which may name one.
                "NotApplied" => {
                    let mut members = BTreeMap::from([(
                        EcoString::from("plan"),
                        unresolved(version_of(member(fields, "plan", "NotApplied")?)?),
                    )]);
                    members.insert(
                        "reason".into(),
                        abort_reason(member(fields, "reason", "NotApplied")?)?,
                    );
                    tagged(arm, Value::Map(members.into()))
                }
                _ => {
                    return Err(WireError::Shape {
                        what: "an `Executed` outcome",
                        found: arm.to_string(),
                        expected: "`Applied` or `NotApplied`",
                    });
                }
            }
        }
        ABORTED => Value::Map(ValueMap::from([(
            "reason".into(),
            abort_reason(member(payload, "reason", "Aborted")?)?,
        )])),
        _ => unreachable!("the outcome was checked against its role"),
    };
    Ok(tagged(tag, value))
}

/// An arm of an outcome that is itself a union, such as `Analysed`.
fn nested(
    payload: &JsonValue,
    what: &'static str,
    arms: &[(&'static str, &[&str])],
) -> Result<Value, WireError> {
    let (arm, fields) = union_of(payload, what)?;
    let Some((name, declared)) = arms.iter().find(|(name, _)| *name == arm) else {
        return Err(WireError::Shape {
            what,
            found: arm.to_string(),
            expected: "an arm of this outcome",
        });
    };
    if declared.is_empty() {
        require_nullary(&arm, fields)?;
        return Ok(nullary(arm));
    }
    Ok(tagged(arm, resolved_fields(fields, declared, name)?))
}

/// A struct of fields each naming a Knowledge version.
fn resolved_fields(
    fields: &JsonValue,
    declared: &[&str],
    what: &'static str,
) -> Result<Value, WireError> {
    let mut members = BTreeMap::new();
    for name in declared {
        let version = optional_member(fields, name).ok_or(WireError::Missing {
            what: "a named version",
            record: what,
        })?;
        members.insert(EcoString::from(*name), unresolved(version_of(version)?));
    }
    Ok(Value::Map(members.into()))
}

/// Why an activation gave up. `Application` names a version; the rest say so
/// on their own.
fn abort_reason(reason: &JsonValue) -> Result<Value, WireError> {
    let (tag, payload) = union_of(reason, "an abort reason")?;
    Ok(match tag.as_str() {
        "BudgetExhausted" | "Timeout" => {
            require_nullary(&tag, payload)?;
            nullary(tag)
        }
        "Failed" => match payload {
            JsonValue::String(text) => tagged(tag, Value::Str(text.clone())),
            other => {
                return Err(WireError::Shape {
                    what: "`Failed`",
                    found: other.canonical_text(),
                    expected: "the text of the failure",
                });
            }
        },
        "Application" => tagged(tag, unresolved(version_of(payload)?)),
        _ => {
            return Err(WireError::Shape {
                what: "an abort reason",
                found: tag.to_string(),
                expected: "`BudgetExhausted`, `Timeout`, `Failed` or `Application`",
            });
        }
    })
}

/// Who performed a write. A write that is not a phase activation is named
/// rather than left absent, because a node seeding defaults at startup and
/// something outside the loop reaching into the store are different facts,
/// and a profile may want to tell them apart.
fn writer_of(writer: &JsonValue) -> Result<Value, WireError> {
    let (tag, payload) = union_of(writer, "a writer")?;
    let members = match tag.as_str() {
        "Activation" => [
            (
                "run_id",
                identity(payload, "run_id", "an activation writer")?,
            ),
            (
                "activation_id",
                identity(payload, "activation_id", "an activation writer")?,
            ),
        ]
        .as_slice()
        .to_vec(),
        "Node" => vec![
            ("run_id", identity(payload, "run_id", "a node writer")?),
            ("name", identity(payload, "name", "a node writer")?),
        ],
        // A writer outside the loop belongs to no run of it, so it names
        // only itself; `arrival` already places the write in time.
        "External" => vec![("name", identity(payload, "name", "an external writer")?)],
        _ => {
            return Err(WireError::Shape {
                what: "a writer",
                found: tag.to_string(),
                expected: "`Activation`, `Node` or `External`",
            });
        }
    };
    Ok(tagged(
        tag,
        Value::Map(
            members
                .into_iter()
                .map(|(name, value)| (EcoString::from(name), Value::Str(value)))
                .collect(),
        ),
    ))
}

/// A version this record names, as the `ref` of a `Resolved` the Knowledge
/// resolver completes.
fn version_of(version: &JsonValue) -> Result<Value, WireError> {
    let sequence = integer_member(version, "global_write_sequence", "a version")?;
    Ok(Value::Map(ValueMap::from([
        (
            "store_epoch".into(),
            Value::Str(identity(version, "store_epoch", "a version")?),
        ),
        ("global_write_sequence".into(), Value::Int(sequence)),
        (
            "key".into(),
            Value::Str(identity(version, "key", "a version")?),
        ),
    ])))
}

fn reference_key(reference: &Value) -> EcoString {
    match reference {
        Value::Map(members) => match members.get("key") {
            Some(Value::Str(key)) => key.clone(),
            _ => EcoString::new(),
        },
        _ => EcoString::new(),
    }
}

/// The versions a phase record names, as `Nothing`, `One` or `Several`.
fn knowledge_refs_of(payload: &JsonValue) -> Result<Value, WireError> {
    let knowledge = member(payload, "knowledge", "Phase")?;
    let (tag, refs) = union_of(knowledge, "the Knowledge references")?;
    match tag.as_str() {
        "Nothing" => {
            require_nullary(&tag, refs)?;
            Ok(nullary(tag))
        }
        "One" => Ok(tagged("One".into(), unresolved(version_of(refs)?))),
        "Several" => {
            let JsonValue::Array(items) = refs else {
                return Err(WireError::Shape {
                    what: "`Several`",
                    found: refs.canonical_text(),
                    expected: "a list of versions",
                });
            };
            let items = items
                .iter()
                .map(|item| Ok(unresolved(version_of(item)?)))
                .collect::<Result<EcoVec<_>, WireError>>()?;
            Ok(tagged("Several".into(), Value::List(items)))
        }
        _ => Err(WireError::Shape {
            what: "the Knowledge references",
            found: tag.to_string(),
            expected: "`Nothing`, `One` or `Several`",
        }),
    }
}

/// A `Resolved` whose value the Knowledge resolver has yet to supply.
fn unresolved(reference: Value) -> Value {
    Value::Map(ValueMap::from([("ref".into(), reference)]))
}

fn arrival_of(payload: &JsonValue) -> Result<Value, WireError> {
    let arrival = member(payload, "arrival", "the record")?;
    let journal_time = member(arrival, "journal_time", "the arrival")?;
    let JsonValue::Number(time) = journal_time else {
        return Err(WireError::Shape {
            what: "a journal time",
            found: journal_time.canonical_text(),
            expected: "a number",
        });
    };
    Ok(Value::Map(ValueMap::from([(
        "journal_time".into(),
        Value::Float(time.as_f64()),
    )])))
}

fn record_value<const N: usize>(tag: &str, fields: [(&str, Value); N]) -> Value {
    tagged(
        tag.into(),
        Value::Map(
            fields
                .into_iter()
                .map(|(name, value)| (EcoString::from(name), value))
                .collect(),
        ),
    )
}

/// A tag that carries nothing, which is a value in its own right.
fn nullary(tag: EcoString) -> Value {
    UnionValue::new(tag, None).into()
}

/// A tag the contract declares nullary carries nothing, so a payload written
/// beside it is a record built to another shape.
fn require_nullary(tag: &EcoString, payload: &JsonValue) -> Result<(), WireError> {
    match payload {
        JsonValue::Null => Ok(()),
        _ => Err(WireError::NullaryWithPayload { tag: tag.clone() }),
    }
}

/// A tag written the way a name is written: `ObservationRecorded` is the
/// event `observation_recorded` names. Tags are CamelCase on the wire and an
/// event keeps its snake_case name for tooling, so the two correspond rather
/// than match.
fn snake_case(tag: &str) -> EcoString {
    let mut name = String::with_capacity(tag.len() + 4);
    for (index, character) in tag.char_indices() {
        if character.is_ascii_uppercase() {
            if index != 0 {
                name.push('_');
            }
            name.push(character.to_ascii_lowercase());
        } else {
            name.push(character);
        }
    }
    name.into()
}

/// Only this version of the value schema is read. A journal of another
/// version is refused rather than read for whatever happens to match.
fn check_schema_version(record: &JsonValue) -> Result<(), WireError> {
    let Some(version) = optional_member(record, "schema_version") else {
        return Err(WireError::Missing {
            what: "schema_version",
            record: "the record",
        });
    };
    match version {
        JsonValue::Number(number) if number.as_u64() == Some(u64::from(SCHEMA_VERSION)) => Ok(()),
        other => Err(WireError::SchemaVersion {
            found: other.canonical_text(),
            expected: SCHEMA_VERSION,
        }),
    }
}

fn tagged(tag: EcoString, payload: Value) -> Value {
    UnionValue::new(tag, Some(payload)).into()
}

fn eco_fields<const N: usize>(fields: [(&str, Value); N]) -> EcoVec<(EcoString, Value)> {
    fields
        .into_iter()
        .map(|(name, value)| (EcoString::from(name), value))
        .collect()
}

fn union_of<'a>(
    json: &'a JsonValue,
    what: &'static str,
) -> Result<(EcoString, &'a JsonValue), WireError> {
    let JsonValue::Object(members) = json else {
        return Err(WireError::Shape {
            what,
            found: json.canonical_text(),
            expected: "a tagged value",
        });
    };
    let Some(JsonValue::String(tag)) = members.get("$tag") else {
        return Err(WireError::Shape {
            what,
            found: json.canonical_text(),
            expected: "a tagged value",
        });
    };
    const EMPTY: &JsonValue = &JsonValue::Null;
    Ok((tag.clone(), members.get("payload").unwrap_or(EMPTY)))
}

fn union_tag(value: &Value, what: &'static str) -> Result<EcoString, WireError> {
    match value {
        Value::Union(union) => Ok(union.tag().clone()),
        other => Err(WireError::Shape {
            what,
            found: other.to_string(),
            expected: "a tagged value",
        }),
    }
}

fn member<'a>(
    json: &'a JsonValue,
    name: &'static str,
    record: &'static str,
) -> Result<&'a JsonValue, WireError> {
    optional_member(json, name).ok_or(WireError::Missing { what: name, record })
}

fn optional_member<'a>(json: &'a JsonValue, name: &str) -> Option<&'a JsonValue> {
    match json {
        JsonValue::Object(members) => members.get(name),
        _ => None,
    }
}

fn string_member(
    json: &JsonValue,
    name: &'static str,
    record: &'static str,
) -> Result<EcoString, WireError> {
    match member(json, name, record)? {
        JsonValue::String(text) => Ok(text.clone()),
        other => Err(WireError::Shape {
            what: name,
            found: other.canonical_text(),
            expected: "a string",
        }),
    }
}

/// A string that names something, which the empty string does not.
fn identity(
    json: &JsonValue,
    name: &'static str,
    record: &'static str,
) -> Result<EcoString, WireError> {
    let text = string_member(json, name, record)?;
    if text.is_empty() {
        return Err(WireError::EmptyIdentity { what: name });
    }
    Ok(text)
}

fn integer_member(
    json: &JsonValue,
    name: &'static str,
    record: &'static str,
) -> Result<i64, WireError> {
    match member(json, name, record)? {
        JsonValue::Number(number) => number.as_i64().ok_or(WireError::Shape {
            what: name,
            found: number.as_str().to_owned(),
            expected: "a whole number",
        }),
        other => Err(WireError::Shape {
            what: name,
            found: other.canonical_text(),
            expected: "a whole number",
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_phase(record: &str) -> Result<Value, WireError> {
        WireDecoder::new().phase_record(&JsonValue::parse(record).expect("valid JSON"))
    }

    /// A journal read in order, so an emission is published before it is named.
    fn decode_all(records: &[&str]) -> Result<Vec<Value>, WireError> {
        let mut decoder = WireDecoder::new();
        records
            .iter()
            .map(|record| decoder.phase_record(&JsonValue::parse(record).expect("valid JSON")))
            .collect()
    }

    fn activation(role: &str, id: &str, caused_by: &str) -> String {
        format!(
            r#""activation":{{"run_id":"r1","loop":["robot1"],"role":"{role}",
               "activation_id":"{id}","caused_by":{caused_by}}}"#
        )
    }

    fn phase(role: &str, id: &str, caused_by: &str, step: &str) -> String {
        format!(
            r#"{{"schema_version":2,"$tag":"Phase","payload":{{{},"step":{step},
               "knowledge":{{"$tag":"Nothing"}},
               "arrival":{{"journal_time":1.5}}}}}}"#,
            activation(role, id, caused_by)
        )
    }

    /// An event as the producer writes one: the payload is the application's
    /// event struct, and its `detail` carries the tag the name corresponds to.
    fn event_step(name: &str, tag: &str, ordinal: i64, activation: &str) -> String {
        format!(
            r#"{{"$tag":"Event","payload":{{"name":"{name}",
               "payload":{{"detail":{{"$tag":"{tag}"}}}},
               "emission":{{"activation_id":"{activation}","ordinal":{ordinal}}}}}}}"#
        )
    }

    const NONE: &str = r#"{"$tag":"None"}"#;

    // R18.1: a record is read as the value `rpio::journal` declares, with the
    // activation, the step and the arrival in place.
    #[test]
    fn a_phase_record_is_read_as_its_journal_value() {
        let record = phase(
            "Monitor",
            "a1",
            NONE,
            r#"{"$tag":"Start","payload":{"trigger":{"$tag":"SensorDataReceived"}}}"#,
        );
        let Value::Union(union) = decode_phase(&record).expect("a well-formed record") else {
            panic!("a record is a tagged value");
        };
        assert_eq!(union.tag(), "Phase");
        let Some(Value::Map(fields)) = union.payload() else {
            panic!("a Phase carries its fields");
        };
        assert_eq!(
            fields.keys().map(EcoString::as_str).collect::<Vec<_>>(),
            ["activation", "arrival", "knowledge", "step"]
        );
        let Some(Value::Map(activation)) = fields.get("activation") else {
            panic!("an activation is a struct");
        };
        assert_eq!(
            activation.get("role"),
            Some(&Value::from(UnionValue::new("Monitor", None)))
        );
        assert_eq!(
            activation.get("loop"),
            Some(&Value::List(EcoVec::from([Value::Str("robot1".into())])))
        );
        // A record that names no Knowledge names `Nothing`, not nothing.
        assert_eq!(
            fields.get("knowledge"),
            Some(&Value::from(UnionValue::new("Nothing", None)))
        );
    }

    // R18.2: an identity names something, so the empty string is refused
    // wherever one is expected, and no specification has to test for `""`.
    #[test]
    fn an_empty_identity_is_refused() {
        for (role, id) in [("Monitor", ""), ("Monitor", "a1")] {
            let record = phase(
                role,
                id,
                NONE,
                r#"{"$tag":"Start","payload":{"trigger":{"$tag":"Tick"}}}"#,
            );
            let decoded = decode_phase(&record);
            if id.is_empty() {
                assert!(
                    matches!(decoded, Err(WireError::EmptyIdentity { .. })),
                    "{decoded:?}"
                );
            } else {
                assert!(decoded.is_ok(), "{decoded:?}");
            }
        }
        // A run identity, a loop segment and an emission are identities too.
        let empty_run = phase("Monitor", "a1", NONE, r#"{"$tag":"Read"}"#)
            .replace(r#""run_id":"r1""#, r#""run_id":"""#);
        assert!(matches!(
            decode_phase(&empty_run),
            Err(WireError::EmptyIdentity { .. })
        ));
        let empty_loop = phase("Monitor", "a1", NONE, r#"{"$tag":"Read"}"#)
            .replace(r#""loop":["robot1"]"#, r#""loop":[""]"#);
        assert!(matches!(
            decode_phase(&empty_loop),
            Err(WireError::EmptyIdentity { .. })
        ));
    }

    // R18.3: an event is named once: the name and the payload's tag are the
    // same thing, and a record where they differ is malformed.
    #[test]
    fn an_event_name_matches_its_payload_tag() {
        let matching = phase(
            "Monitor",
            "a1",
            NONE,
            &event_step("observation_recorded", "ObservationRecorded", 1, "a1"),
        );
        assert!(
            decode_phase(&matching).is_ok(),
            "{:?}",
            decode_phase(&matching)
        );

        let mismatched = matching.replace(
            r#""detail":{"$tag":"ObservationRecorded"}"#,
            r#""detail":{"$tag":"DetectionCompleted"}"#,
        );
        assert!(matches!(
            decode_phase(&mismatched),
            Err(WireError::EventNameMismatch { .. })
        ));
    }

    // R18.4: a Knowledge value is filed under the key its tag names.
    #[test]
    fn a_knowledge_value_is_tagged_by_its_key() {
        let decoder = WireDecoder::new();
        let record = r#"{"schema_version":2,"$tag":"Knowledge","payload":{"version":{
            "ref":{"store_epoch":"e1","global_write_sequence":7,"key":"LegitResult"},
            "value":{"$tag":"LegitResult","payload":{"approved":true}},
            "written_by":{"$tag":"Activation","payload":{"run_id":"r1","activation_id":"a4"}}},
            "arrival":{"journal_time":2.0}}}"#;
        let decoded = decoder
            .knowledge_record(&JsonValue::parse(record).expect("valid JSON"))
            .expect("a well-formed Knowledge record");
        let Value::Union(union) = decoded else {
            panic!("a record is a tagged value");
        };
        assert_eq!(union.tag(), "Knowledge");

        let mistagged = record.replace(r#""$tag":"LegitResult""#, r#""$tag":"CandidateModel""#);
        assert!(matches!(
            decoder.knowledge_record(&JsonValue::parse(&mistagged).expect("valid JSON")),
            Err(WireError::KnowledgeKeyMismatch { .. })
        ));
    }

    // R18.5: each role ends with its own outcome, and any role may abort.
    #[test]
    fn an_outcome_belongs_to_the_role_that_ended() {
        let end = |role: &str, outcome: &str| {
            phase(
                role,
                "a1",
                NONE,
                &format!(r#"{{"$tag":"End","payload":{{"outcome":{outcome}}}}}"#),
            )
        };
        let observation = r#"{"$tag":"Monitored","payload":{"observation":
            {"store_epoch":"e1","global_write_sequence":1,"key":"FrameRef"}}}"#;
        assert!(decode_phase(&end("Monitor", observation)).is_ok());
        assert!(matches!(
            decode_phase(&end("Analysis", observation)),
            Err(WireError::OutcomeNotOfRole { .. })
        ));
        // Aborting is every role's.
        let aborted = r#"{"$tag":"Aborted","payload":{"reason":{"$tag":"Timeout"}}}"#;
        for role in ROLES {
            assert!(decode_phase(&end(role, aborted)).is_ok(), "{role}");
        }
    }

    // R18.6: an outcome names Knowledge rather than carrying it, so what it
    // names is left for the Knowledge resolver to supply.
    #[test]
    fn an_outcome_names_versions_it_does_not_carry() {
        let record = phase(
            "Plan",
            "a1",
            NONE,
            r#"{"$tag":"End","payload":{"outcome":{"$tag":"Planned","payload":
               {"$tag":"Generated","payload":{"plan":
               {"store_epoch":"e1","global_write_sequence":4,"key":"CandidateModel"}}}}}}"#,
        );
        let decoded = decode_phase(&record).expect("a well-formed record");
        let printed = decoded.to_string();
        // The version is in place, and no value stands beside it yet.
        assert!(printed.contains("CandidateModel"), "{printed}");
        assert!(printed.contains("\"ref\""), "{printed}");
        assert!(!printed.contains("\"value\""), "{printed}");
    }

    // R18.7: an activation names the emission that woke it, and that
    // emission was published by the activation it names.
    #[test]
    fn a_named_emission_was_published_by_the_activation_it_names() {
        let published = phase(
            "Monitor",
            "a1",
            NONE,
            &event_step("observation_recorded", "ObservationRecorded", 1, "a1"),
        );
        let woken = phase(
            "Analysis",
            "a2",
            r#"{"$tag":"Some","payload":{"activation_id":"a1","ordinal":1}}"#,
            r#"{"$tag":"Start","payload":{"trigger":{"$tag":"ObservationRecorded"}}}"#,
        );
        assert!(decode_all(&[&published, &woken]).is_ok());

        // Naming an emission nobody published is malformed.
        assert!(matches!(
            decode_all(&[&woken]),
            Err(WireError::UnpublishedEmission { .. })
        ));

        // An activation cannot publish another activation's emission.
        let borrowed = published.replace(
            r#""emission":{"activation_id":"a1""#,
            r#""emission":{"activation_id":"a2""#,
        );
        assert!(matches!(
            decode_all(&[&borrowed]),
            Err(WireError::EmissionNotOwned { .. })
        ));
    }

    // R18.8: an activation's emissions are counted from its first, so a gap
    // is a record that was lost rather than one that is merely late.
    #[test]
    fn emissions_are_counted_without_gaps() {
        let emission = |ordinal: i64| {
            phase(
                "Monitor",
                "a1",
                NONE,
                &event_step("tick", "Tick", ordinal, "a1"),
            )
        };
        assert!(decode_all(&[&emission(1), &emission(2)]).is_ok());
        assert!(matches!(
            decode_all(&[&emission(1), &emission(3)]),
            Err(WireError::OrdinalGap { .. })
        ));
        assert!(matches!(
            decode_all(&[&emission(0)]),
            Err(WireError::Shape { .. })
        ));
    }

    // R18.9: sequencing is not a wire rule. Roles out of order, several
    // loops and concurrent activations are all readable; whether they are
    // allowed is the library's derivation or a profile's property.
    #[test]
    fn sequencing_and_grouping_are_not_wire_rules() {
        let start = |role: &str, id: &str| {
            phase(
                role,
                id,
                NONE,
                r#"{"$tag":"Start","payload":{"trigger":{"$tag":"Tick"}}}"#,
            )
        };
        // Execute before Monitor, and two activations of the same role open
        // at once: structurally these are ordinary records.
        assert!(
            decode_all(&[
                &start("Execute", "a1"),
                &start("Monitor", "a2"),
                &start("Monitor", "a3"),
            ])
            .is_ok()
        );
    }

    // R18.10: a record that is not a record of this journal is refused,
    // rather than read as something close to it.
    #[test]
    fn a_record_of_another_shape_is_refused() {
        for record in [
            r#"{"$tag":"Elsewhere","payload":{}}"#,
            r#"{"nothing":"tagged"}"#,
            r#"[1,2,3]"#,
        ] {
            assert!(decode_phase(record).is_err(), "{record}");
        }
        let unknown_role = phase(
            "Supervisor",
            "a1",
            NONE,
            r#"{"$tag":"Start","payload":{"trigger":{"$tag":"Tick"}}}"#,
        );
        assert!(matches!(
            decode_phase(&unknown_role),
            Err(WireError::UnknownRole { .. })
        ));
    }

    // R18.11: an outcome is checked all the way down, not at its outermost
    // tag. Its arms, its fields and the versions they name are all declared,
    // so a record missing any of them is malformed rather than copied
    // through for a property to trip over.
    #[test]
    fn an_outcome_is_checked_below_its_first_tag() {
        let end = |role: &str, outcome: &str| {
            phase(
                role,
                "a1",
                NONE,
                &format!(r#"{{"$tag":"End","payload":{{"outcome":{outcome}}}}}"#),
            )
        };
        let version = r#"{"store_epoch":"e1","global_write_sequence":1,"key":"FrameRef"}"#;
        for (role, outcome) in [
            // A field the outcome declares is missing.
            ("Monitor", r#"{"$tag":"Monitored","payload":{}}"#),
            // An arm that belongs to no outcome.
            (
                "Analysis",
                r#"{"$tag":"Analysed","payload":{"$tag":"Sideways"}}"#,
            ),
            // An arm declared nullary, carrying a payload.
            (
                "Plan",
                r#"{"$tag":"Planned","payload":{"$tag":"NoPlan","payload":{"plan":1}}}"#,
            ),
            // An abort reason that is not one.
            (
                "Execute",
                r#"{"$tag":"Aborted","payload":{"reason":{"$tag":"Nonsense"}}}"#,
            ),
        ] {
            let record = end(role, outcome);
            assert!(decode_phase(&record).is_err(), "{outcome}");
        }
        // The same outcomes, written as declared, are read.
        let applied = format!(
            r#"{{"$tag":"Executed","payload":{{"$tag":"Applied","payload":
               {{"plan":{version},"effect":{version}}}}}}}"#
        );
        assert!(decode_phase(&end("Execute", &applied)).is_ok());
        assert!(
            decode_phase(&end(
                "Analysis",
                r#"{"$tag":"Analysed","payload":{"$tag":"Nominal"}}"#
            ))
            .is_ok()
        );
    }

    // R18.12: an activation's identity is unique within a run, so one
    // journal holding two runs must not let a record of one name an
    // emission of the other.
    #[test]
    fn emissions_belong_to_the_run_that_published_them() {
        let first_run = phase("Monitor", "a1", NONE, &event_step("tick", "Tick", 1, "a1"));
        // The same activation identity, reused by the next run.
        let second_run = first_run.replace(r#""run_id":"r1""#, r#""run_id":"r2""#);
        // Counting starts again per run rather than continuing.
        assert!(decode_all(&[&first_run, &second_run]).is_ok());

        // An edge cannot reach across runs to an emission of another.
        let woken_elsewhere = phase(
            "Analysis",
            "a2",
            r#"{"$tag":"Some","payload":{"activation_id":"a1","ordinal":1}}"#,
            r#"{"$tag":"Start","payload":{"trigger":{"$tag":"Tick"}}}"#,
        )
        .replace(r#""run_id":"r1""#, r#""run_id":"r3""#);
        assert!(matches!(
            decode_all(&[&first_run, &woken_elsewhere]),
            Err(WireError::UnpublishedEmission { .. })
        ));
    }

    // R18.13: an activation's trigger names the event that woke it, so it
    // names the same event the emission did.
    #[test]
    fn a_trigger_names_the_event_that_woke_the_activation() {
        let published = phase(
            "Monitor",
            "a1",
            NONE,
            &event_step("observation_recorded", "ObservationRecorded", 1, "a1"),
        );
        let start = |trigger: &str| {
            phase(
                "Analysis",
                "a2",
                r#"{"$tag":"Some","payload":{"activation_id":"a1","ordinal":1}}"#,
                &format!(r#"{{"$tag":"Start","payload":{{"trigger":{{"$tag":"{trigger}"}}}}}}"#),
            )
        };
        assert!(decode_all(&[&published, &start("ObservationRecorded")]).is_ok());
        assert!(matches!(
            decode_all(&[&published, &start("DetectionCompleted")]),
            Err(WireError::TriggerMismatch { .. })
        ));
        // An activation the loop did not wake names no emission, and there
        // is nothing to compare its trigger with.
        let from_outside = phase(
            "Monitor",
            "a3",
            NONE,
            r#"{"$tag":"Start","payload":{"trigger":{"$tag":"SensorDataReceived"}}}"#,
        );
        assert!(decode_phase(&from_outside).is_ok());
    }

    // R18.14: an optional field is written `Some` or `None`; leaving it out
    // is a record of another shape, not a shorthand for absence.
    #[test]
    fn a_field_the_contract_declares_is_written() {
        let complete = phase(
            "Monitor",
            "a1",
            NONE,
            r#"{"$tag":"Start","payload":{"trigger":{"$tag":"Tick"}}}"#,
        );
        assert!(decode_phase(&complete).is_ok());
        for missing in [
            r#","caused_by":{"$tag":"None"}"#,
            r#""knowledge":{"$tag":"Nothing"},"#,
        ] {
            let without = complete.replace(missing, "");
            assert!(
                matches!(decode_phase(&without), Err(WireError::Missing { .. })),
                "{without}"
            );
        }
    }

    // R18.15: only this version of the schema is read. A journal of another
    // is refused rather than read for whatever happens to match.
    #[test]
    fn only_this_schema_version_is_read() {
        let record = phase("Monitor", "a1", NONE, r#"{"$tag":"Read"}"#);
        assert!(decode_phase(&record).is_ok());
        let older = record.replace(r#""schema_version":2"#, r#""schema_version":1"#);
        assert!(matches!(
            decode_phase(&older),
            Err(WireError::SchemaVersion { .. })
        ));
        let absent = record.replace(r#""schema_version":2,"#, "");
        assert!(matches!(
            decode_phase(&absent),
            Err(WireError::Missing { .. })
        ));
    }

    // R18.16: an activation is one run of one phase callback, so its own
    // records agree about which callback it was. A journal that reuses an
    // identity for something else is describing two activations under one
    // name.
    #[test]
    fn an_activation_keeps_the_role_and_loop_it_started_with() {
        let read = |role: &str| phase(role, "a1", NONE, r#"{"$tag":"Read"}"#);
        assert!(decode_all(&[&read("Monitor"), &read("Monitor")]).is_ok());
        assert!(matches!(
            decode_all(&[&read("Monitor"), &read("Execute")]),
            Err(WireError::ActivationChanged { .. })
        ));
        // The loop it belongs to is part of the same identity.
        let elsewhere = read("Monitor").replace(r#""loop":["robot1"]"#, r#""loop":["robot2"]"#);
        assert!(matches!(
            decode_all(&[&read("Monitor"), &elsewhere]),
            Err(WireError::ActivationChanged { .. })
        ));
        // Another run may reuse the identity for a different callback.
        let next_run = read("Execute").replace(r#""run_id":"r1""#, r#""run_id":"r2""#);
        assert!(decode_all(&[&read("Monitor"), &next_run]).is_ok());
    }

    // R18.17: a write that is not a phase activation is named rather than
    // left absent. A node seeding defaults at startup and something outside
    // the loop reaching into the store are different facts, and a property
    // that asks whether a decision rested on Knowledge from outside the loop
    // needs to tell them apart.
    #[test]
    fn a_write_names_who_performed_it() {
        let knowledge = |writer: &str| {
            format!(
                r#"{{"schema_version":2,"$tag":"Knowledge","payload":{{"version":{{
                   "ref":{{"store_epoch":"e1","global_write_sequence":7,"key":"FrameRef"}},
                   "value":{{"$tag":"FrameRef","payload":{{}}}},
                   "written_by":{writer}}},
                   "arrival":{{"journal_time":2.0}}}}}}"#
            )
        };
        let decoder = WireDecoder::new();
        let decode = |writer: &str| {
            decoder.knowledge_record(&JsonValue::parse(&knowledge(writer)).expect("valid JSON"))
        };
        for (writer, tag) in [
            (
                r#"{"$tag":"Activation","payload":{"run_id":"r1","activation_id":"a1"}}"#,
                "Activation",
            ),
            (
                r#"{"$tag":"Node","payload":{"run_id":"r1","name":"Monitor"}}"#,
                "Node",
            ),
            (
                r#"{"$tag":"External","payload":{"name":"ur-pendant"}}"#,
                "External",
            ),
        ] {
            let record = decode(writer).unwrap_or_else(|error| panic!("{writer}: {error}"));
            assert!(record.to_string().contains(tag), "{writer}: {record}");
        }

        // An activation writer is run-qualified, because an activation
        // identity is unique only within a run.
        assert!(matches!(
            decode(r#"{"$tag":"Activation","payload":{"activation_id":"a1"}}"#),
            Err(WireError::Missing { .. })
        ));
        // An identity names something here as everywhere else.
        assert!(matches!(
            decode(r#"{"$tag":"External","payload":{"name":""}}"#),
            Err(WireError::EmptyIdentity { .. })
        ));
        // A writer of another kind is a record built to another contract.
        assert!(matches!(
            decode(r#"{"$tag":"Somebody","payload":{"name":"x"}}"#),
            Err(WireError::Shape { .. })
        ));
        // A version that says nothing about who wrote it is malformed: the
        // writer is stated, never inferred.
        let without = knowledge(r#"{"$tag":"External","payload":{"name":"x"}}"#).replace(
            r#""written_by":{"$tag":"External","payload":{"name":"x"}}"#,
            r#""spare":0"#,
        );
        assert!(matches!(
            decoder.knowledge_record(&JsonValue::parse(&without).expect("valid JSON")),
            Err(WireError::Missing { .. })
        ));
    }
}
