//! Reading a journal with observation capabilities removed, and with unrecorded versions.
//!
//! An ablation asks which of a journal's capabilities a property depends on. A
//! view takes capabilities out before phase records are decoded, after the
//! original Knowledge history has passed immutable-definition checks, so a
//! specification runs unchanged on what remains: a property whose evidence is
//! gone simply cannot be violated in that view.
//!
//! | Capability  | Removed                                                         |
//! |-------------|-----------------------------------------------------------------|
//! | `writers`   | every version's writer becomes the [`UNATTRIBUTED`] node        |
//! | `identity`  | a version's sequence number becomes a digest of key and value   |
//! | `causality` | an activation no longer names the emission that woke it         |
//! | `reads`     | Read steps                                                      |
//! | `steps`     | Read and Write steps                                            |
//!
//! Without identity two versions holding equal values are the same version, as
//! they are to a monitor that compares values. Write order is still the order
//! of the Knowledge records.
//!
//! Separately, a journal can name a version it never wrote, as when a Knowledge
//! record is lost. The default is to refuse the journal. [`Unrecorded::Incomplete`]
//! instead reads each phase record that names such a version as an `Incomplete`
//! record of its activation, so the rest of the journal can still be checked.

use std::collections::{BTreeMap, BTreeSet};

use ecow::EcoString;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::JsonValue;

/// One observation capability a view can remove.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JournalCapability {
    Writers,
    Identity,
    Causality,
    Reads,
    Steps,
}

/// What to do with a phase record that names a version the journal never wrote.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Unrecorded {
    /// The journal is malformed and is not read.
    #[default]
    Refuse,
    /// The record is read as an `Incomplete` record of its activation.
    Incomplete,
}

impl Unrecorded {
    pub fn is_default(&self) -> bool {
        *self == Self::Refuse
    }
}

/// How a file source reads its journal: the capabilities removed, and what an
/// unrecorded version does. The default reads the journal as written.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JournalReading {
    pub remove: BTreeSet<JournalCapability>,
    pub unrecorded: Unrecorded,
}

/// The writer every version has once writers are removed.
pub const UNATTRIBUTED: &str = "projection:unattributed";

/// The records with the capabilities in `remove` taken away. Identity goes
/// first, so that the digests are over the values as written.
pub(crate) fn project<T>(
    records: Vec<(T, JsonValue)>,
    remove: &BTreeSet<JournalCapability>,
) -> Vec<(T, JsonValue)> {
    if remove.is_empty() {
        return records;
    }
    let mut records = records;
    if remove.contains(&JournalCapability::Identity) {
        let digests = value_digests(&records);
        for (_, record) in &mut records {
            rewrite_references(record, &digests);
        }
    }
    let drop_reads =
        remove.contains(&JournalCapability::Reads) || remove.contains(&JournalCapability::Steps);
    let drop_writes = remove.contains(&JournalCapability::Steps);
    records
        .into_iter()
        .filter_map(|(location, mut record)| {
            match tag(&record) {
                Some("Knowledge") if remove.contains(&JournalCapability::Writers) => {
                    if let Some(version) = member_mut(&mut record, &["payload", "version"]) {
                        set(version, "written_by", unattributed());
                    }
                }
                Some("Phase") => {
                    let step = member(&record, &["payload", "step"]).and_then(tag_of_value);
                    if (drop_reads && step == Some("Read"))
                        || (drop_writes && step == Some("Write"))
                    {
                        return None;
                    }
                    if remove.contains(&JournalCapability::Causality) {
                        if let Some(activation) =
                            member_mut(&mut record, &["payload", "activation"])
                        {
                            set(
                                activation,
                                "caused_by",
                                JsonValue::object([("$tag", "None".into())]),
                            );
                        }
                    }
                }
                _ => {}
            }
            Some((location, record))
        })
        .collect()
}

/// The `Incomplete` record standing for a phase record whose activation and
/// arrival it keeps.
pub(crate) fn incomplete_of(record: &JsonValue) -> Option<JsonValue> {
    let JsonValue::Object(fields) = record else {
        return None;
    };
    let activation = member(record, &["payload", "activation"])?.clone();
    let arrival = member(record, &["payload", "arrival"])?.clone();
    let mut incomplete: BTreeMap<EcoString, JsonValue> = BTreeMap::new();
    if let Some(version) = fields.get("schema_version") {
        incomplete.insert("schema_version".into(), version.clone());
    }
    incomplete.insert("$tag".into(), "Incomplete".into());
    incomplete.insert(
        "payload".into(),
        JsonValue::object([("activation", activation), ("arrival", arrival)]),
    );
    Some(JsonValue::Object(incomplete))
}

/// A version's store epoch, key and sequence number (as canonical text).
type VersionKey = (EcoString, EcoString, String);

/// Each written version's digest of its key and value, keyed by the version.
fn value_digests<T>(records: &[(T, JsonValue)]) -> BTreeMap<VersionKey, i64> {
    let mut digests = BTreeMap::new();
    for (_, record) in records {
        if tag(record) != Some("Knowledge") {
            continue;
        }
        let Some(version) = member(record, &["payload", "version"]) else {
            continue;
        };
        let (Some(reference), Some(value)) =
            (member(version, &["ref"]), member(version, &["value"]))
        else {
            continue;
        };
        if let Some(key) = version_key(reference) {
            let digest = digest_of(&key.1, value);
            digests.insert(key, digest);
        }
    }
    digests
}

/// A 48-bit digest of a key and the value written under it: equal values of a
/// key share it, and anything else almost surely does not.
fn digest_of(key: &str, value: &JsonValue) -> i64 {
    let text = JsonValue::Array(vec![JsonValue::string(key), value.clone()]).canonical_text();
    let hash = Sha256::digest(text.as_bytes());
    hash[..6]
        .iter()
        .fold(0_i64, |digest, byte| (digest << 8) | i64::from(*byte))
}

/// Replace the sequence number of every version reference, at any depth, by
/// its digest. A reference to a version the journal never wrote keeps its
/// number, so that it stays as unresolvable as it was.
fn rewrite_references(value: &mut JsonValue, digests: &BTreeMap<VersionKey, i64>) {
    match value {
        JsonValue::Object(fields) => {
            if let Some(key) = version_key_of(fields) {
                if let Some(digest) = digests.get(&key) {
                    fields.insert("global_write_sequence".into(), JsonValue::from(*digest));
                }
                return;
            }
            for child in fields.values_mut() {
                rewrite_references(child, digests);
            }
        }
        JsonValue::Array(items) => {
            for item in items {
                rewrite_references(item, digests);
            }
        }
        _ => {}
    }
}

/// A version reference's identity: its store epoch, key and sequence number.
fn version_key(value: &JsonValue) -> Option<VersionKey> {
    match value {
        JsonValue::Object(fields) => version_key_of(fields),
        _ => None,
    }
}

fn version_key_of(fields: &BTreeMap<EcoString, JsonValue>) -> Option<VersionKey> {
    let JsonValue::String(epoch) = fields.get("store_epoch")? else {
        return None;
    };
    let JsonValue::String(key) = fields.get("key")? else {
        return None;
    };
    let sequence = fields.get("global_write_sequence")?.canonical_text();
    Some((epoch.clone(), key.clone(), sequence))
}

fn unattributed() -> JsonValue {
    JsonValue::object([
        ("$tag", JsonValue::string("Node")),
        (
            "payload",
            JsonValue::object([
                ("name", JsonValue::string(UNATTRIBUTED)),
                ("run_id", JsonValue::string("projection")),
            ]),
        ),
    ])
}

fn tag(record: &JsonValue) -> Option<&str> {
    tag_of_value(record)
}

fn tag_of_value(value: &JsonValue) -> Option<&str> {
    match value {
        JsonValue::Object(fields) => match fields.get("$tag") {
            Some(JsonValue::String(tag)) => Some(tag.as_str()),
            _ => None,
        },
        _ => None,
    }
}

fn member<'a>(value: &'a JsonValue, path: &[&str]) -> Option<&'a JsonValue> {
    path.iter().try_fold(value, |value, name| match value {
        JsonValue::Object(fields) => fields.get(*name),
        _ => None,
    })
}

fn member_mut<'a>(value: &'a mut JsonValue, path: &[&str]) -> Option<&'a mut JsonValue> {
    path.iter().try_fold(value, |value, name| match value {
        JsonValue::Object(fields) => fields.get_mut(*name),
        _ => None,
    })
}

fn set(value: &mut JsonValue, name: &str, member: JsonValue) {
    if let JsonValue::Object(fields) = value {
        fields.insert(name.into(), member);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn without_causality_an_activation_names_no_waker() {
        let record = JsonValue::parse(
            r#"{"$tag":"Phase","payload":{"activation":{"activation_id":"b","caused_by":{"$tag":"Some","payload":{"activation_id":"a","ordinal":1}}},"step":{"$tag":"Start"}}}"#,
        )
        .unwrap();
        let remove = BTreeSet::from([JournalCapability::Causality]);
        let projected = project(vec![((), record)], &remove);
        assert_eq!(
            member(&projected[0].1, &["payload", "activation", "caused_by"]),
            Some(&JsonValue::object([("$tag", "None".into())]))
        );
    }

    #[test]
    fn capabilities_and_policies_are_named_in_lower_case() {
        let remove: BTreeSet<JournalCapability> =
            serde_json::from_str(r#"["writers","identity","causality","reads","steps"]"#).unwrap();
        assert_eq!(remove.len(), 5);
        let policy: Unrecorded = serde_json::from_str(r#""incomplete""#).unwrap();
        assert_eq!(policy, Unrecorded::Incomplete);
    }
}
