//! Finite replay of an RPIO observation journal from JSON Lines files.
//!
//! Each nonblank line is one journal record, Knowledge or phase, in the order
//! the producer wrote it. Every record becomes one tick of the observation
//! variable, completed with the Knowledge values it names.

use anyhow::Context;
use ecow::EcoString;
use sha2::{Digest, Sha256};

use crate::core::{InputBatch, Value, VarName};

use super::journal_view::{self, JournalReading, Unrecorded};
use super::{JsonValue, KnowledgeResolver, ResolverError, WireDecoder};

/// One decoded and resolved journal record, and where it was read: the
/// file and line, as `path:line`.
#[derive(Debug)]
pub(crate) struct FileRecord {
    pub value: Value,
    pub cursor: EcoString,
}

#[cfg(test)]
fn read_files(
    files: &[String],
    variable: VarName,
    reading: &JournalReading,
) -> anyhow::Result<Vec<InputBatch<Value>>> {
    read_files_checked(files, variable, reading, &[])
}

pub(crate) fn read_files_checked(
    files: &[String],
    variable: VarName,
    reading: &JournalReading,
    checks: &[crate::io::config::RpioCaptureCheck],
) -> anyhow::Result<Vec<InputBatch<Value>>> {
    Ok(read_records_checked(files, reading, checks)?
        .into_iter()
        .map(|record| InputBatch::update(variable.clone(), record.value))
        .collect())
}

/// Validate the frozen capture before projecting a reduced observation view.
/// A missing final record or a changed internal record is a source error,
/// rather than a shorter replay that appears to complete successfully.
pub(crate) fn read_records_checked(
    files: &[String],
    reading: &JournalReading,
    checks: &[crate::io::config::RpioCaptureCheck],
) -> anyhow::Result<Vec<FileRecord>> {
    anyhow::ensure!(
        checks.is_empty() || checks.len() == files.len(),
        "RPIO capture_checks must name every configured file in order"
    );
    let mut parsed = Vec::new();
    for (file_index, path) in files.iter().enumerate() {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("RPIO observation file `{path}` could not be read"))?;
        if let Some(check) = checks.get(file_index) {
            let digest = format!("{:x}", Sha256::digest(contents.as_bytes()));
            anyhow::ensure!(
                digest.eq_ignore_ascii_case(&check.sha256),
                "RPIO observation file `{path}` SHA-256 differs from its frozen capture"
            );
            let records = contents
                .lines()
                .filter(|line| !line.trim().is_empty())
                .count();
            anyhow::ensure!(
                records == check.records,
                "RPIO observation file `{path}` capture extent differs: expected {} records, received {records}",
                check.records
            );
            anyhow::ensure!(
                contents.is_empty() || contents.ends_with('\n'),
                "RPIO observation file `{path}` has no final line terminator"
            );
        }
        for (index, line) in contents.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let record = JsonValue::parse(line)
                .with_context(|| format!("RPIO observation file `{path}` line {}", index + 1))?;
            if let Some(check) = checks.get(file_index) {
                if let JsonValue::Object(fields) = &record
                    && let Some(JsonValue::Object(payload)) = fields.get("payload")
                    && let Some(JsonValue::Object(arrival)) = payload.get("arrival")
                    && let Some(clock) = arrival.get("journal_clock")
                {
                    anyhow::ensure!(
                        matches!(clock, JsonValue::String(label) if label == &check.journal_clock),
                        "RPIO observation file `{path}` line {} clock {clock:?} differs from capture clock `{}`",
                        index + 1,
                        check.journal_clock
                    );
                }
            }
            parsed.push(((path.as_str(), index + 1), record));
        }
    }
    // Validate immutable definitions in the original capture before a view
    // deliberately aliases identities or erases writers. Such a view must
    // not repair contradictory source history.
    if !reading.remove.is_empty() {
        let mut raw_history = KnowledgeResolver::new();
        for ((path, line_number), record) in &parsed {
            if is_knowledge(record) {
                let value = WireDecoder::new()
                    .knowledge_record(record)
                    .with_context(|| {
                        format!("RPIO observation file `{path}` line {line_number}")
                    })?;
                raw_history.remember_json(&value, record).with_context(|| {
                    format!("RPIO observation file `{path}` line {line_number}")
                })?;
            }
        }
    }
    // A view removes capabilities before the phase records are decoded, so the records
    // that remain are read exactly as a journal written without them would be.
    let records: Vec<_> = journal_view::project(parsed, &reading.remove)
        .into_iter()
        .map(|((path, line_number), record)| (path, line_number, record))
        .collect();

    let mut resolver = KnowledgeResolver::new();
    // A finite replay is a materialised two-stream journal. Pre-indexing its
    // immutable Knowledge stream permits exact joins without reordering or
    // suppressing any record in the emitted sequence.
    for (path, line_number, record) in &records {
        if is_knowledge(record) {
            let value = WireDecoder::new()
                .knowledge_record(record)
                .with_context(|| format!("RPIO observation file `{path}` line {line_number}"))?;
            let remembered = if reading.remove.contains(&super::JournalCapability::Identity) {
                resolver.remember_coalesced(&value)
            } else {
                resolver.remember_json(&value, record)
            };
            remembered
                .with_context(|| format!("RPIO observation file `{path}` line {line_number}"))?;
        }
    }

    let mut decoder = WireDecoder::new();
    let mut resolved = Vec::with_capacity(records.len());
    for (path, line_number, record) in records {
        let at = || format!("RPIO observation file `{path}` line {line_number}");
        let value = if is_knowledge(&record) {
            decoder.knowledge_record(&record)
        } else {
            decoder.phase_record(&record)
        }
        .with_context(at)?;
        let value = match resolver.resolve(&value) {
            Ok(value) => value,
            // A phase record naming a version the journal never wrote can be
            // kept as an incomplete record of its activation.
            Err(ResolverError::UnknownVersion { .. })
                if reading.unrecorded == Unrecorded::Incomplete && !is_knowledge(&record) =>
            {
                let incomplete = journal_view::incomplete_of(&record)
                    .with_context(|| format!("{}: no activation to keep", at()))?;
                let value = decoder.phase_record(&incomplete).with_context(at)?;
                resolver.resolve(&value).with_context(at)?
            }
            Err(error) => return Err(error).with_context(at),
        };
        resolved.push(FileRecord {
            value,
            cursor: format!("{path}:{line_number}").into(),
        });
    }
    Ok(resolved)
}

/// Whether a record belongs to the Knowledge stream. Both streams share one
/// file, so the record's own tag chooses the decoder's entry point.
fn is_knowledge(record: &JsonValue) -> bool {
    matches!(
        record,
        JsonValue::Object(fields)
            if matches!(fields.get("$tag"), Some(JsonValue::String(tag)) if tag == "Knowledge")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn journal_file_is_decoded_resolved_and_kept_in_source_order() {
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/rpio/nominal.jsonl");
        let batches = read_files(
            &[path.to_string_lossy().into_owned()],
            VarName::new("observation"),
            &JournalReading::default(),
        )
        .unwrap();
        let source_lines = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count();
        assert_eq!(batches.len(), source_lines);
        let tags: Vec<_> = batches
            .iter()
            .map(|batch| {
                let Value::Union(record) = &batch.updates().next().unwrap().value else {
                    panic!("an observation record must be a Record union")
                };
                record.tag().to_string()
            })
            .collect();
        assert_eq!(&tags[..3], &["Knowledge", "Knowledge", "Phase"]);
        fn all_named_versions_are_resolved(value: &Value) -> bool {
            match value {
                Value::Map(fields) => {
                    (!fields.contains_key("ref")
                        || (fields.contains_key("value") && fields.contains_key("written_by")))
                        && fields.values().all(all_named_versions_are_resolved)
                }
                Value::List(items) | Value::Tuple(items) => {
                    items.iter().all(all_named_versions_are_resolved)
                }
                Value::Union(item) => item.payload().is_none_or(all_named_versions_are_resolved),
                _ => true,
            }
        }
        assert!(
            batches.iter().all(|batch| all_named_versions_are_resolved(
                &batch.updates().next().unwrap().value
            ))
        );
    }

    const MONITOR: &str = r#"{"run_id":"r","loop":["l"],"role":"Monitor","activation_id":"a1","caused_by":{"$tag":"None"}}"#;

    fn frame(sequence: i64, brightness: f64, journal_time: f64) -> String {
        format!(
            r#"{{"schema_version":2,"$tag":"Knowledge","payload":{{"version":{{"ref":{{"store_epoch":"E","global_write_sequence":{sequence},"key":"FrameRef"}},"value":{{"$tag":"FrameRef","payload":{{"brightness":{brightness}}}}},"written_by":{{"$tag":"External","payload":{{"name":"test"}}}}}},"arrival":{{"journal_time":{journal_time}}}}}}}"#
        )
    }

    fn read_of(sequence: i64, journal_time: f64) -> String {
        format!(
            r#"{{"schema_version":2,"$tag":"Phase","payload":{{"activation":{MONITOR},"step":{{"$tag":"Read"}},"knowledge":{{"$tag":"One","payload":{{"store_epoch":"E","global_write_sequence":{sequence},"key":"FrameRef"}}}},"arrival":{{"journal_time":{journal_time}}}}}}}"#
        )
    }

    fn journal(lines: &[String]) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "tc-rpio-observation-{}-{}.jsonl",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::write(&path, lines.join("\n") + "\n").unwrap();
        path
    }

    fn capture_check(bytes: &str, records: usize) -> crate::io::config::RpioCaptureCheck {
        crate::io::config::RpioCaptureCheck {
            records,
            sha256: format!("{:x}", Sha256::digest(bytes.as_bytes())),
            journal_clock: "fixture.journal".into(),
        }
    }

    #[test]
    fn frozen_capture_is_checked_before_ablation_removes_records() {
        let path = journal(&[frame(1, 0.25, 1.0), read_of(1, 2.0)]);
        let bytes = std::fs::read_to_string(&path).unwrap();
        let reading = JournalReading {
            remove: [super::super::JournalCapability::Reads].into(),
            ..Default::default()
        };
        let result = read_records_checked(
            &[path.display().to_string()],
            &reading,
            &[capture_check(&bytes, 2)],
        );
        std::fs::remove_file(path).unwrap();
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn a_complete_json_line_prefix_cannot_pass_as_the_frozen_capture() {
        let lines = [frame(1, 0.25, 1.0), read_of(1, 2.0)];
        let path = journal(&lines);
        let frozen = capture_check(&std::fs::read_to_string(&path).unwrap(), 2);
        std::fs::write(&path, format!("{}\n", lines[0])).unwrap();
        let result = read_records_checked(
            &[path.display().to_string()],
            &JournalReading::default(),
            &[frozen],
        );
        std::fs::remove_file(path).unwrap();
        assert!(result.unwrap_err().to_string().contains("SHA-256"));
    }

    #[test]
    fn extent_and_line_termination_are_checked_even_when_hash_matches() {
        let path = journal(&[frame(1, 0.25, 1.0)]);
        let bytes = std::fs::read_to_string(&path).unwrap();
        let count_error = read_records_checked(
            &[path.display().to_string()],
            &JournalReading::default(),
            &[capture_check(&bytes, 2)],
        );
        let unterminated = bytes.trim_end_matches('\n');
        std::fs::write(&path, unterminated).unwrap();
        let terminator_error = read_records_checked(
            &[path.display().to_string()],
            &JournalReading::default(),
            &[capture_check(unterminated, 1)],
        );
        std::fs::remove_file(path).unwrap();
        assert!(
            count_error
                .unwrap_err()
                .to_string()
                .contains("extent differs")
        );
        assert!(
            terminator_error
                .unwrap_err()
                .to_string()
                .contains("line terminator")
        );
    }

    #[test]
    fn a_record_clock_must_agree_with_its_frozen_capture() {
        let line = frame(1, 0.25, 1.0).replace(
            "\"journal_time\":1",
            "\"journal_clock\":\"other.clock\",\"journal_time\":1",
        );
        let path = journal(&[line]);
        let bytes = std::fs::read_to_string(&path).unwrap();
        let result = read_records_checked(
            &[path.display().to_string()],
            &JournalReading::default(),
            &[capture_check(&bytes, 1)],
        );
        std::fs::remove_file(path).unwrap();
        assert!(result.unwrap_err().to_string().contains("other.clock"));
    }

    #[test]
    fn an_ablated_view_cannot_repair_contradictory_raw_history() {
        let path = journal(&[frame(1, 0.25, 1.0), frame(1, 0.75, 2.0)]);
        let reading = JournalReading {
            remove: [
                super::super::JournalCapability::Identity,
                super::super::JournalCapability::Writers,
            ]
            .into(),
            ..Default::default()
        };
        let result = read_records_checked(&[path.display().to_string()], &reading, &[]);
        std::fs::remove_file(path).unwrap();
        let error = format!("{:#}", result.unwrap_err());
        assert!(error.contains("immutable write"), "{error}");
    }

    #[test]
    fn wide_integer_conflicts_are_rejected_before_identity_ablation() {
        let original = frame(1, 0.25, 1.0)
            .replace("\"brightness\":0.25", "\"brightness\":18446744073709551616");
        let changed = original.replace("18446744073709551616", "18446744073709551617");
        let lines = [original, changed];
        for remove in [vec![], vec![super::super::JournalCapability::Identity]] {
            let path = journal(&lines);
            let result = read_records_checked(
                &[path.display().to_string()],
                &JournalReading {
                    remove: remove.into_iter().collect(),
                    ..Default::default()
                },
                &[],
            );
            std::fs::remove_file(path).unwrap();
            let error = format!("{:#}", result.unwrap_err());
            assert!(error.contains("immutable write"), "{error}");
        }
    }

    fn tag_of(batch: &InputBatch<Value>) -> String {
        let Value::Union(record) = &batch.updates().next().unwrap().value else {
            panic!("an observation record must be a Record union")
        };
        record.tag().to_string()
    }

    #[test]
    fn exact_reference_is_resolved_and_knowledge_is_its_own_tick() {
        // Two versions of one key: the Read names the first, so it must be
        // completed with that version's value rather than the latest.
        let path = journal(&[frame(1, 0.25, 1.0), frame(2, 0.75, 2.0), read_of(1, 3.0)]);
        let batches = read_files(
            &[path.display().to_string()],
            VarName::new("observation"),
            &JournalReading::default(),
        );
        let _ = std::fs::remove_file(&path);
        let batches = batches.unwrap();
        assert!(
            batches
                .iter()
                .all(|batch| (batch.tick_count(), batch.update_count()) == (1, 1))
        );
        let tags: Vec<_> = batches.iter().map(tag_of).collect();
        assert_eq!(tags, ["Knowledge", "Knowledge", "Phase"]);

        let Value::Union(phase) = &batches[2].updates().next().unwrap().value else {
            unreachable!()
        };
        let Some(Value::Map(payload)) = phase.payload() else {
            panic!("a Phase record must carry an object")
        };
        let Value::Union(knowledge) = &payload["knowledge"] else {
            panic!("a Phase record's knowledge must be a union")
        };
        assert_eq!(knowledge.tag(), "One");
        let Some(Value::Map(resolved)) = knowledge.payload() else {
            panic!("a named version must be completed as an object")
        };
        let Value::Union(value) = &resolved["value"] else {
            panic!("a Knowledge value must be tagged by its key")
        };
        let Some(Value::Map(frame)) = value.payload() else {
            panic!("a FrameRef must carry an object")
        };
        assert_eq!(frame["brightness"], Value::Float(0.25));
    }

    #[test]
    fn a_version_the_file_never_wrote_is_refused() {
        let path = journal(&[frame(1, 0.25, 1.0), read_of(2, 2.0)]);
        let error = read_files(
            &[path.display().to_string()],
            VarName::new("observation"),
            &JournalReading::default(),
        )
        .unwrap_err();
        let _ = std::fs::remove_file(&path);
        let message = format!("{error:#}");
        assert!(message.contains("never wrote"), "{message}");
        assert!(message.contains("line 2"), "{message}");
    }

    #[test]
    fn a_record_without_the_schema_version_is_refused_at_its_line() {
        let unversioned = read_of(1, 2.0).replace(r#""schema_version":2,"#, "");
        let path = journal(&[frame(1, 0.25, 1.0), unversioned]);
        let error = read_files(
            &[path.display().to_string()],
            VarName::new("observation"),
            &JournalReading::default(),
        )
        .unwrap_err();
        let _ = std::fs::remove_file(&path);
        let message = format!("{error:#}");
        assert!(message.contains("schema_version"), "{message}");
        assert!(message.contains("line 2"), "{message}");
    }

    #[test]
    fn file_errors_name_path_and_line() {
        let path = std::env::temp_dir().join(format!(
            "tc-rpio-observation-{}-{}.jsonl",
            std::process::id(),
            line!()
        ));
        std::fs::write(&path, "\n{}\n").unwrap();
        let error = read_files(
            &[path.display().to_string()],
            VarName::new("observation"),
            &JournalReading::default(),
        )
        .unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains(&path.display().to_string()));
        assert!(message.contains("line 2"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn real_producer_fixture_projects_one_tick_per_nonblank_line() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/journal/native-v2/clean-local.jsonl");
        let text = std::fs::read_to_string(&path).unwrap();
        let expected = text.lines().filter(|line| !line.trim().is_empty()).count();
        let batches = read_files(
            &[path.display().to_string()],
            VarName::new("observation"),
            &JournalReading::default(),
        )
        .unwrap();
        assert_eq!(batches.len(), expected);
        assert!(batches.iter().all(|batch| batch.tick_count() == 1));
        assert!(batches.iter().all(|batch| batch.update_count() == 1));
    }

    fn write_of(sequence: i64, journal_time: f64) -> String {
        read_of(sequence, journal_time).replace(r#""$tag":"Read""#, r#""$tag":"Write""#)
    }

    fn read_with(
        lines: &[String],
        reading: JournalReading,
    ) -> anyhow::Result<Vec<InputBatch<Value>>> {
        let path = journal(lines);
        let batches = read_files(
            &[path.display().to_string()],
            VarName::new("observation"),
            &reading,
        );
        let _ = std::fs::remove_file(&path);
        batches
    }

    fn removing(capabilities: &[super::super::JournalCapability]) -> JournalReading {
        JournalReading {
            remove: capabilities.iter().copied().collect(),
            ..JournalReading::default()
        }
    }

    fn field<'a>(value: &'a Value, path: &[&str]) -> &'a Value {
        path.iter().fold(value, |value, name| match value {
            Value::Union(union) if *name == "payload" => union.payload().expect("a payload"),
            Value::Map(fields) => &fields[*name],
            other => panic!("no `{name}` in {other}"),
        })
    }

    fn record(batch: &InputBatch<Value>) -> &Value {
        &batch.updates().next().unwrap().value
    }

    #[test]
    fn without_identity_equal_values_are_one_version_and_different_values_are_not() {
        use super::super::JournalCapability::Identity;
        let lines = [
            frame(1, 0.25, 1.0),
            frame(2, 0.25, 2.0),
            frame(3, 0.75, 3.0),
            read_of(1, 4.0),
        ];
        let batches = read_with(&lines, removing(&[Identity])).unwrap();
        let sequence =
            |batch: &InputBatch<Value>, path: &[&str]| field(record(batch), path).clone();
        let written = ["payload", "version", "ref", "global_write_sequence"];
        let (first, second, third) = (
            sequence(&batches[0], &written),
            sequence(&batches[1], &written),
            sequence(&batches[2], &written),
        );
        assert_eq!(first, second, "equal values share a digest");
        assert_ne!(first, third, "different values do not");
        let read = sequence(
            &batches[3],
            &[
                "payload",
                "knowledge",
                "payload",
                "ref",
                "global_write_sequence",
            ],
        );
        assert_eq!(read, first, "a read names its version by the digest too");
    }

    #[test]
    fn without_writers_every_version_is_unattributed() {
        use super::super::JournalCapability::Writers;
        let batches = read_with(
            &[frame(1, 0.25, 1.0), read_of(1, 2.0)],
            removing(&[Writers]),
        )
        .unwrap();
        for (batch, path) in [
            (&batches[0], &["payload", "version", "written_by"][..]),
            (
                &batches[1],
                &["payload", "knowledge", "payload", "written_by"][..],
            ),
        ] {
            let Value::Union(writer) = field(record(batch), path) else {
                panic!("a writer is a union")
            };
            assert_eq!(writer.tag(), "Node");
            assert_eq!(
                field(record(batch), &[path, &["payload", "name"]].concat()),
                &Value::Str(super::super::journal_view::UNATTRIBUTED.into())
            );
        }
    }

    #[test]
    fn removing_reads_keeps_writes_and_removing_steps_drops_both() {
        use super::super::JournalCapability::{Reads, Steps};
        let lines = [frame(1, 0.25, 1.0), write_of(1, 2.0), read_of(1, 3.0)];
        let tags = |reading| {
            read_with(&lines, reading)
                .unwrap()
                .iter()
                .map(tag_of)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            tags(JournalReading::default()),
            ["Knowledge", "Phase", "Phase"]
        );
        assert_eq!(tags(removing(&[Reads])), ["Knowledge", "Phase"]);
        assert_eq!(tags(removing(&[Steps])), ["Knowledge"]);
    }

    #[test]
    fn an_unrecorded_version_is_refused_or_read_as_incomplete() {
        let lines = [frame(1, 0.25, 1.0), read_of(2, 2.0)];
        let message = format!(
            "{:#}",
            read_with(&lines, JournalReading::default()).unwrap_err()
        );
        assert!(message.contains("never wrote"), "{message}");
        let incomplete = JournalReading {
            unrecorded: super::super::Unrecorded::Incomplete,
            ..JournalReading::default()
        };
        let batches = read_with(&lines, incomplete).unwrap();
        assert_eq!(
            batches.iter().map(tag_of).collect::<Vec<_>>(),
            ["Knowledge", "Incomplete"]
        );
        let Value::Map(activation) = field(record(&batches[1]), &["payload", "activation"]) else {
            panic!("an incomplete record keeps its activation")
        };
        assert_eq!(activation["activation_id"], Value::Str("a1".into()));
    }
}
