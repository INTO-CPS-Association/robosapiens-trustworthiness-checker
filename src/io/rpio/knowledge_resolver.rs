//! Joining each Knowledge version a record names with the value written.
//!
//! A record names Knowledge rather than carrying it: the wire writes a bare
//! version where the contract declares a `Resolved`, and the decoder lifts it
//! into a `Resolved` holding only its `ref`. A record is therefore not yet the
//! `Record` that `rpio::journal` declares. This module supplies the value
//! written under each version named, and only then does every `Resolved<K>`
//! carry both halves.

use std::collections::BTreeMap;

use ecow::{EcoString, EcoVec};

use super::JsonValue;
use crate::core::{UnionValue, Value, ValueMap};

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ResolverError {
    #[error(
        "write {global_write_sequence} of `{key}` in store epoch `{store_epoch}` is named by a \
         record, and the journal never wrote it"
    )]
    UnknownVersion {
        store_epoch: EcoString,
        global_write_sequence: i64,
        key: EcoString,
    },
    #[error("a version is {found}, expected a store epoch, a global write sequence and a key")]
    MalformedVersion { found: String },
    #[error("a Knowledge record is {found}, expected a version and the value written under it")]
    NotAKnowledgeRecord { found: String },
    #[error(
        "immutable write {global_write_sequence} of `{key}` in store epoch `{store_epoch}` is defined with another value or writer"
    )]
    ConflictingDefinition {
        store_epoch: EcoString,
        global_write_sequence: i64,
        key: EcoString,
    },
    #[error(
        "global write {global_write_sequence} in store epoch `{store_epoch}` names both `{first_key}` and `{key}`"
    )]
    SequenceCollision {
        store_epoch: EcoString,
        global_write_sequence: i64,
        first_key: EcoString,
        key: EcoString,
    },
}

/// The versions the journal has written, and the value each one holds.
///
/// A record names a version; the resolver completes it with the value written
/// under exactly that version. Another version of the same key is never put in
/// its place, because a record says what the activation read, not what the
/// store holds afterwards.
///
/// Callback and Knowledge records travel on separate streams. File replay
/// validates the complete immutable history before resolving callbacks; live
/// input can hold a callback until its exact definition arrives. The resolver
/// reports a missing version to its caller, which owns that delivery policy.
///
/// The resolver decides nothing else. Which cycle a record belongs to, whether
/// the roles ran in order, and whether an episode is open are the MAPLE-K
/// library's derivation or a profile's properties.
#[derive(Debug, Default)]
pub struct KnowledgeResolver {
    /// What each version holds, and who wrote it. Both are supplied
    /// together, because both are facts the Knowledge record states and
    /// neither can be recovered from a naming position alone.
    versions: BTreeMap<Version, Written>,
    sequences: BTreeMap<(EcoString, i64), EcoString>,
}

impl KnowledgeResolver {
    pub fn new() -> Self {
        Self::default()
    }

    /// Remember an already-converted Knowledge definition.
    /// Native source admission uses `remember_json` to preserve raw value identity.
    ///
    /// A version is its store epoch, its global write sequence and its key
    /// together, so the same key at another sequence, or the same sequence in
    /// another store epoch, is a different version. A version the journal
    /// repeats identically is idempotent. Another value or writer for that
    /// immutable reference, or another key at the same global write, is an
    /// integrity error; the first definition remains unchanged.
    pub fn remember(&mut self, record: &Value) -> Result<(), ResolverError> {
        self.remember_with(record, false, None)
    }

    /// Remember a decoded source definition without losing integer precision.
    ///
    /// Runtime values round integers outside `i64` to floats. The compact hash
    /// of the original canonical value distinguishes those definitions while
    /// allowing whitespace, key-order and arrival-time differences in retries.
    pub(crate) fn remember_json(
        &mut self,
        record: &Value,
        original: &JsonValue,
    ) -> Result<(), ResolverError> {
        let value = (|| {
            let JsonValue::Object(fields) = original else {
                return None;
            };
            let JsonValue::Object(payload) = fields.get("payload")? else {
                return None;
            };
            let JsonValue::Object(version) = payload.get("version")? else {
                return None;
            };
            version.get("value")
        })()
        .ok_or_else(|| ResolverError::NotAKnowledgeRecord {
            found: record.to_string(),
        })?;
        self.remember_with(record, false, Some(value.digest_bytes()))
    }

    /// A value-only ablation deliberately aliases equal values across writes.
    /// The raw journal must already have passed immutable-history validation.
    /// Only writer differences introduced by that aliasing may be coalesced.
    pub(crate) fn remember_coalesced(&mut self, record: &Value) -> Result<(), ResolverError> {
        self.remember_with(record, true, None)
    }

    fn remember_with(
        &mut self,
        record: &Value,
        coalesced: bool,
        value_digest: Option<[u8; 32]>,
    ) -> Result<(), ResolverError> {
        let mistaken = || ResolverError::NotAKnowledgeRecord {
            found: record.to_string(),
        };
        let Value::Union(record) = record else {
            return Err(mistaken());
        };
        if record.tag() != "Knowledge" {
            return Err(mistaken());
        }
        let Some(Value::Map(fields)) = record.payload() else {
            return Err(mistaken());
        };
        let Some(Value::Map(version)) = fields.get("version") else {
            return Err(mistaken());
        };
        let (Some(reference), Some(value), Some(written_by)) = (
            version.get("ref"),
            version.get("value"),
            version.get("written_by"),
        ) else {
            return Err(mistaken());
        };
        let version = version_of(reference)?;
        let sequence = (version.store_epoch.clone(), version.global_write_sequence);
        if let Some(key) = self.sequences.get(&sequence)
            && key != &version.key
        {
            return Err(ResolverError::SequenceCollision {
                store_epoch: version.store_epoch,
                global_write_sequence: version.global_write_sequence,
                first_key: key.clone(),
                key: version.key,
            });
        }
        let written = Written {
            value: value.clone(),
            written_by: written_by.clone(),
            value_digest,
        };
        if let Some(previous) = self.versions.get(&version) {
            if previous.value != written.value
                || previous.value_digest != written.value_digest
                || (!coalesced && previous.written_by != written.written_by)
            {
                return Err(ResolverError::ConflictingDefinition {
                    store_epoch: version.store_epoch,
                    global_write_sequence: version.global_write_sequence,
                    key: version.key,
                });
            }
            if !coalesced {
                return Ok(());
            }
        }
        self.sequences.insert(sequence, version.key.clone());
        self.versions.insert(version, written);
        Ok(())
    }

    /// A phase record with every version it names completed in place.
    pub fn resolve(&self, record: &Value) -> Result<Value, ResolverError> {
        self.completed(record)
    }

    fn completed(&self, value: &Value) -> Result<Value, ResolverError> {
        Ok(match value {
            Value::Map(members) => match named_version(members) {
                Some(reference) => {
                    let version = version_of(reference)?;
                    let Some(written) = self.versions.get(&version) else {
                        return Err(ResolverError::UnknownVersion {
                            store_epoch: version.store_epoch,
                            global_write_sequence: version.global_write_sequence,
                            key: version.key,
                        });
                    };
                    Value::Map(ValueMap::from([
                        ("ref".into(), reference.clone()),
                        ("value".into(), written.value.clone()),
                        ("written_by".into(), written.written_by.clone()),
                    ]))
                }
                None => Value::Map(
                    members
                        .iter()
                        .map(|(name, member)| Ok((name.clone(), self.completed(member)?)))
                        .collect::<Result<_, ResolverError>>()?,
                ),
            },
            Value::List(items) => Value::List(self.each_completed(items)?),
            Value::Tuple(items) => Value::Tuple(self.each_completed(items)?),
            Value::Union(union) => {
                let payload = union
                    .payload()
                    .map(|payload| self.completed(payload))
                    .transpose()?;
                UnionValue::new(union.tag().clone(), payload).into()
            }
            other => other.clone(),
        })
    }

    fn each_completed(&self, items: &EcoVec<Value>) -> Result<EcoVec<Value>, ResolverError> {
        items.iter().map(|item| self.completed(item)).collect()
    }
}

/// What a Knowledge record said about one version: the value written under
/// it, and who wrote it.
#[derive(Clone, Debug, PartialEq)]
struct Written {
    value: Value,
    written_by: Value,
    /// Source values retain a 32-byte fingerprint, never a duplicate JSON tree.
    /// Typed-only callers and already-validated value-only views use `None`.
    value_digest: Option<[u8; 32]>,
}

/// A version's identity: the store epoch that wrote it, which write of that
/// epoch it was, and the Knowledge key it was written under.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Version {
    store_epoch: EcoString,
    global_write_sequence: i64,
    key: EcoString,
}

fn version_of(reference: &Value) -> Result<Version, ResolverError> {
    let malformed = || ResolverError::MalformedVersion {
        found: reference.to_string(),
    };
    let Value::Map(members) = reference else {
        return Err(malformed());
    };
    let (Some(Value::Str(store_epoch)), Some(Value::Int(sequence)), Some(Value::Str(key))) = (
        members.get("store_epoch"),
        members.get("global_write_sequence"),
        members.get("key"),
    ) else {
        return Err(malformed());
    };
    Ok(Version {
        store_epoch: store_epoch.clone(),
        global_write_sequence: *sequence,
        key: key.clone(),
    })
}

/// The version an unresolved `Resolved` names: a lone `ref`, as the wire
/// decoder leaves it. A position that already carries its value is complete
/// and is walked no further.
fn named_version(members: &ValueMap) -> Option<&Value> {
    match (members.len(), members.get("ref")) {
        (1, Some(reference)) => Some(reference),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::io::rpio::json::JsonValue;
    use crate::io::rpio::wire::WireDecoder;

    const READ: &str = r#"{"$tag":"Read"}"#;

    /// A Knowledge record as the journal writes it: a version, its value, and
    /// who wrote it.
    fn knowledge(epoch: &str, sequence: i64, key: &str, payload: &str) -> Value {
        written_knowledge(epoch, sequence, key, payload, "a1")
    }

    fn written_knowledge(
        epoch: &str,
        sequence: i64,
        key: &str,
        payload: &str,
        activation: &str,
    ) -> Value {
        let record = raw_written_knowledge(epoch, sequence, key, payload, activation);
        WireDecoder::new()
            .knowledge_record(&record)
            .expect("a well-formed Knowledge record")
    }

    fn raw_written_knowledge(
        epoch: &str,
        sequence: i64,
        key: &str,
        payload: &str,
        activation: &str,
    ) -> JsonValue {
        let record = format!(
            r#"{{"schema_version":2,"$tag":"Knowledge","payload":{{"version":{{
               "ref":{{"store_epoch":"{epoch}","global_write_sequence":{sequence},"key":"{key}"}},
               "value":{{"$tag":"{key}","payload":{payload}}},
               "written_by":{{"$tag":"Activation","payload":{{"run_id":"r1",
                 "activation_id":"{activation}"}}}}}},
               "arrival":{{"journal_time":1.0}}}}}}"#
        );
        JsonValue::parse(&record).expect("valid JSON")
    }

    fn reference(epoch: &str, sequence: i64, key: &str) -> String {
        format!(r#"{{"store_epoch":"{epoch}","global_write_sequence":{sequence},"key":"{key}"}}"#)
    }

    #[test]
    fn identical_definition_retries_are_idempotent() {
        let mut resolver = KnowledgeResolver::new();
        let record = knowledge("e1", 1, "FrameRef", r#"{"frame":3}"#);
        resolver.remember(&record).unwrap();
        resolver.remember(&record).unwrap();
        assert_eq!(resolver.resolve(&record).unwrap(), record);
        assert_eq!(resolver.versions.len(), 1);
    }

    #[test]
    fn contradictory_value_or_writer_cannot_replace_an_immutable_definition() {
        let original = knowledge("e1", 1, "FrameRef", r#"{"frame":3}"#);
        for contradictory in [
            knowledge("e1", 1, "FrameRef", r#"{"frame":4}"#),
            written_knowledge("e1", 1, "FrameRef", r#"{"frame":3}"#, "a2"),
        ] {
            let mut resolver = KnowledgeResolver::new();
            resolver.remember(&original).unwrap();
            let read = phase("Monitor", READ, &one(&reference("e1", 1, "FrameRef")));
            let before = resolver.resolve(&read).unwrap();
            assert!(matches!(
                resolver.remember(&contradictory),
                Err(ResolverError::ConflictingDefinition { .. })
            ));
            assert_eq!(resolver.resolve(&read).unwrap(), before);
        }
    }

    #[test]
    fn distinct_wide_integers_cannot_retry_the_same_immutable_definition() {
        let original = raw_written_knowledge(
            "e1",
            1,
            "FrameRef",
            r#"{"frame":18446744073709551616}"#,
            "a1",
        );
        let changed = raw_written_knowledge(
            "e1",
            1,
            "FrameRef",
            r#"{"frame":18446744073709551617}"#,
            "a1",
        );
        let decoded = WireDecoder::new().knowledge_record(&original).unwrap();
        let changed_decoded = WireDecoder::new().knowledge_record(&changed).unwrap();
        // Both round to the same runtime Float; source identity must not.
        assert_eq!(decoded, changed_decoded);
        let mut resolver = KnowledgeResolver::new();
        resolver.remember_json(&decoded, &original).unwrap();
        assert!(matches!(
            resolver.remember_json(&changed_decoded, &changed),
            Err(ResolverError::ConflictingDefinition { .. })
        ));
        assert_eq!(resolver.versions.len(), 1);
        assert_eq!(
            resolver.versions.values().next().unwrap().value_digest,
            Some(original_value(&original).digest_bytes())
        );
    }

    fn original_value(record: &JsonValue) -> &JsonValue {
        let JsonValue::Object(fields) = record else {
            panic!("record")
        };
        let JsonValue::Object(payload) = &fields["payload"] else {
            panic!("payload")
        };
        let JsonValue::Object(version) = &payload["version"] else {
            panic!("version")
        };
        &version["value"]
    }

    #[test]
    fn raw_definition_retries_ignore_presentation_and_arrival_but_preserve_writer() {
        let original = raw_written_knowledge(
            "e1",
            1,
            "FrameRef",
            r#"{"frame":18446744073709551616,"label":"x"}"#,
            "a1",
        );
        let retry = raw_written_knowledge(
            "e1",
            1,
            "FrameRef",
            r#"{ "label": "x", "frame": 18446744073709551616 }"#,
            "a1",
        );
        let retry = JsonValue::parse(
            &retry
                .canonical_text()
                .replace("\"journal_time\":1.0", "\"journal_time\":2.0"),
        )
        .unwrap();
        assert_ne!(original, retry, "retry should have a different arrival");
        let mut resolver = KnowledgeResolver::new();
        resolver
            .remember_json(
                &WireDecoder::new().knowledge_record(&original).unwrap(),
                &original,
            )
            .unwrap();
        resolver
            .remember_json(
                &WireDecoder::new().knowledge_record(&retry).unwrap(),
                &retry,
            )
            .unwrap();
        assert_eq!(resolver.versions.len(), 1);
        let before = resolver.versions.clone();
        let changed_writer = raw_written_knowledge(
            "e1",
            1,
            "FrameRef",
            r#"{"frame":18446744073709551616,"label":"x"}"#,
            "a2",
        );
        assert!(matches!(
            resolver.remember_json(
                &WireDecoder::new()
                    .knowledge_record(&changed_writer)
                    .unwrap(),
                &changed_writer,
            ),
            Err(ResolverError::ConflictingDefinition { .. })
        ));
        assert_eq!(resolver.versions, before);
    }

    #[test]
    fn a_global_write_cannot_define_two_keys_but_epochs_are_independent() {
        let mut resolver = KnowledgeResolver::new();
        resolver
            .remember(&knowledge("e1", 1, "FrameRef", "{}"))
            .unwrap();
        assert!(matches!(
            resolver.remember(&knowledge("e1", 1, "ActiveModel", "{}")),
            Err(ResolverError::SequenceCollision { .. })
        ));
        resolver
            .remember(&knowledge("e2", 1, "ActiveModel", "{}"))
            .unwrap();
        assert_eq!(resolver.versions.len(), 2);
    }

    #[test]
    fn a_value_only_view_may_alias_writers_but_never_different_values() {
        let mut resolver = KnowledgeResolver::new();
        resolver
            .remember_coalesced(&knowledge("e1", 1, "FrameRef", "{}"))
            .unwrap();
        resolver
            .remember_coalesced(&written_knowledge("e1", 1, "FrameRef", "{}", "a2"))
            .unwrap();
        assert!(matches!(
            resolver.remember_coalesced(&knowledge("e1", 1, "FrameRef", r#"{"frame":4}"#)),
            Err(ResolverError::ConflictingDefinition { .. })
        ));
    }

    fn one(reference: &str) -> String {
        format!(r#"{{"$tag":"One","payload":{reference}}}"#)
    }

    /// A phase record, naming versions through its step and its `knowledge`.
    fn phase(role: &str, step: &str, knowledge: &str) -> Value {
        let record = format!(
            r#"{{"schema_version":2,"$tag":"Phase","payload":{{
               "activation":{{"run_id":"r1","loop":["robot1"],"role":"{role}",
                 "activation_id":"a1","caused_by":{{"$tag":"None"}}}},
               "step":{step},"knowledge":{knowledge},
               "arrival":{{"journal_time":2.0}}}}}}"#
        );
        WireDecoder::new()
            .phase_record(&JsonValue::parse(&record).expect("valid JSON"))
            .expect("a well-formed phase record")
    }

    /// The version a record names through `knowledge`.
    fn named(record: &Value) -> BTreeMap<EcoString, Value> {
        let Value::Union(record) = record else {
            panic!("a record is a tagged value");
        };
        let Some(Value::Map(fields)) = record.payload() else {
            panic!("a Phase carries its fields");
        };
        let Some(Value::Union(knowledge)) = fields.get("knowledge") else {
            panic!("the Knowledge references are tagged");
        };
        let Some(Value::Map(version)) = knowledge.payload() else {
            panic!("`One` carries a version");
        };
        version.clone().into_btree_map()
    }

    fn model_id(version: &BTreeMap<EcoString, Value>) -> i64 {
        let Some(Value::Union(value)) = version.get("value") else {
            panic!("a resolved version carries the value written");
        };
        let Some(Value::Map(fields)) = value.payload() else {
            panic!("a CandidateModel is a struct");
        };
        let Some(Value::Int(model_id)) = fields.get("model_id") else {
            panic!("a CandidateModel has a model identity");
        };
        *model_id
    }

    /// How many positions still hold a name and no value.
    fn outstanding(value: &Value) -> usize {
        match value {
            Value::Map(members) if named_version(members).is_some() => 1,
            Value::Map(members) => members.values().map(outstanding).sum(),
            Value::List(items) | Value::Tuple(items) => items.iter().map(outstanding).sum(),
            Value::Union(union) => union.payload().map_or(0, outstanding),
            _ => 0,
        }
    }

    // R19.1: a record names a version, and the resolver completes it with the
    // value written under it, leaving the name beside the value.
    #[test]
    fn a_named_version_is_completed_with_the_value_written() {
        let mut resolver = KnowledgeResolver::new();
        resolver
            .remember(&knowledge("e1", 4, "CandidateModel", r#"{"model_id":7}"#))
            .expect("a well-formed Knowledge record");

        let record = phase("Plan", READ, &one(&reference("e1", 4, "CandidateModel")));
        assert_eq!(outstanding(&record), 1);
        let resolved = resolver
            .resolve(&record)
            .expect("a version the journal wrote");
        assert_eq!(outstanding(&resolved), 0);

        let version = named(&resolved);
        // Resolution supplies both halves the Knowledge record stated: the
        // value written, and who wrote it.
        assert_eq!(
            version.keys().map(EcoString::as_str).collect::<Vec<_>>(),
            ["ref", "value", "written_by"]
        );
        assert_eq!(model_id(&version), 7);
        // The name stays, so a profile can still see which version was read.
        let before = named(&record);
        assert_eq!(version.get("ref"), before.get("ref"));
    }

    // R19.2: a version is the whole reference — store epoch, write sequence
    // and key — so another version of the same key is never put in its place.
    #[test]
    fn only_the_version_named_is_supplied() {
        let mut resolver = KnowledgeResolver::new();
        for (sequence, model) in [(4, 7), (9, 8)] {
            resolver
                .remember(&knowledge(
                    "e1",
                    sequence,
                    "CandidateModel",
                    &format!(r#"{{"model_id":{model}}}"#),
                ))
                .expect("a well-formed Knowledge record");
        }

        for (sequence, model) in [(4, 7), (9, 8)] {
            let record = phase(
                "Plan",
                READ,
                &one(&reference("e1", sequence, "CandidateModel")),
            );
            let resolved = resolver
                .resolve(&record)
                .expect("a version the journal wrote");
            assert_eq!(model_id(&named(&resolved)), model);
        }

        // A write of that key the journal never made, and the same write in
        // another store epoch, are both versions the journal never showed.
        for missing in [
            reference("e1", 5, "CandidateModel"),
            reference("e2", 4, "CandidateModel"),
        ] {
            let record = phase("Plan", READ, &one(&missing));
            assert!(matches!(
                resolver.resolve(&record),
                Err(ResolverError::UnknownVersion { .. })
            ));
        }
    }

    // R19.3: a reference to a version the journal never showed is refused and
    // the error names that version, because the journal is read in order and
    // a later write cannot answer for it.
    #[test]
    fn a_version_the_journal_never_wrote_is_refused() {
        let resolver = KnowledgeResolver::new();
        let record = phase("Plan", READ, &one(&reference("e1", 4, "CandidateModel")));
        let refused = resolver
            .resolve(&record)
            .expect_err("a version nothing wrote");
        assert_eq!(
            refused,
            ResolverError::UnknownVersion {
                store_epoch: "e1".into(),
                global_write_sequence: 4,
                key: "CandidateModel".into(),
            }
        );
        let reported = refused.to_string();
        for part in ["e1", "4", "CandidateModel"] {
            assert!(reported.contains(part), "{reported}");
        }
    }

    // R19.4: every version a record names is completed, however deeply an
    // outcome nests it and however many the record names.
    #[test]
    fn every_version_a_record_names_is_completed() {
        let mut resolver = KnowledgeResolver::new();
        resolver
            .remember(&knowledge("e1", 4, "CandidateModel", r#"{"model_id":7}"#))
            .expect("a well-formed Knowledge record");
        resolver
            .remember(&knowledge("e1", 5, "ApplyAck", r#"{"applied":true}"#))
            .expect("a well-formed Knowledge record");

        let plan = reference("e1", 4, "CandidateModel");
        let effect = reference("e1", 5, "ApplyAck");
        let end = format!(
            r#"{{"$tag":"End","payload":{{"outcome":{{"$tag":"Executed","payload":
               {{"$tag":"Applied","payload":{{"plan":{plan},"effect":{effect}}}}}}}}}}}"#
        );
        let several = format!(r#"{{"$tag":"Several","payload":[{plan},{effect}]}}"#);

        let record = phase("Execute", &end, &several);
        assert_eq!(outstanding(&record), 4);
        let resolved = resolver
            .resolve(&record)
            .expect("versions the journal wrote");
        assert_eq!(outstanding(&resolved), 0);
        let printed = resolved.to_string();
        for written in ["model_id", "applied"] {
            assert!(printed.contains(written), "{printed}");
        }
    }

    // R19.5: the resolver supplies values and decides nothing else: it reads
    // no role, step or order, and a record naming nothing comes back as it
    // was.
    #[test]
    fn resolution_decides_nothing_about_order_or_grouping() {
        let mut resolver = KnowledgeResolver::new();
        resolver
            .remember(&knowledge("e1", 1, "FrameRef", r#"{"frame":3}"#))
            .expect("a well-formed Knowledge record");

        // A version one activation wrote, named by an Execute record, with no
        // record of the activation that wrote it read yet.
        let named_by_execute = phase("Execute", READ, &one(&reference("e1", 1, "FrameRef")));
        let resolved = resolver
            .resolve(&named_by_execute)
            .expect("a version the journal wrote");
        assert_eq!(outstanding(&resolved), 0);

        let names_nothing = phase("Monitor", READ, r#"{"$tag":"Nothing"}"#);
        assert_eq!(
            resolver
                .resolve(&names_nothing)
                .expect("a record naming nothing"),
            names_nothing
        );
    }

    // R19.6: the writer travels with the value. A record that names a
    // version gets back who wrote it without holding a table of its own,
    // which is the whole reason the writer is on the Knowledge record.
    #[test]
    fn the_writer_travels_with_the_value() {
        let mut resolver = KnowledgeResolver::new();
        resolver
            .remember(&written_knowledge("e1", 1, "EntropyHistory", "{}", "a2"))
            .expect("a well-formed Knowledge record");
        resolver
            .remember(&written_knowledge("e1", 2, "RunningAvgEntropy", "{}", "a2"))
            .expect("a well-formed Knowledge record");

        let versions = format!(
            r#"{{"$tag":"Several","payload":[{},{}]}}"#,
            reference("e1", 1, "EntropyHistory"),
            reference("e1", 2, "RunningAvgEntropy")
        );
        let naming = phase("Analysis", r#"{"$tag":"Read"}"#, &versions);
        let resolved = resolver
            .resolve(&naming)
            .expect("both versions are written");

        // Two values one activation wrote name the same writer, which is
        // what a profile compares instead of reconstructing a cycle.
        let writers = writers_in(&resolved);
        assert_eq!(writers.len(), 2);
        assert_eq!(writers[0], writers[1]);
        assert!(writers[0].to_string().contains("a2"), "{}", writers[0]);
    }

    /// Every `written_by` a resolved record carries, in order.
    fn writers_in(value: &Value) -> Vec<Value> {
        let mut found = Vec::new();
        fn walk(value: &Value, found: &mut Vec<Value>) {
            match value {
                Value::Map(members) => {
                    if let Some(writer) = members.get("written_by") {
                        found.push(writer.clone());
                    }
                    members.values().for_each(|value| walk(value, found));
                }
                Value::List(items) | Value::Tuple(items) => {
                    items.iter().for_each(|item| walk(item, found))
                }
                Value::Union(union) => {
                    if let Some(payload) = union.payload() {
                        walk(payload, found);
                    }
                }
                _ => {}
            }
        }
        walk(value, &mut found);
        found
    }
}
