//! Archived inline journals retain their hash, extent, clock and shape checks.
//! They are logical/coordinator fixtures, explicitly refused as native input.
//! Current native classic packets are decoded by WireDecoder, joined by exact
//! immutable references, and admitted through the real typed file source.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde_json::Value as Json;
use sha2::{Digest, Sha256};
use tc_testutils::journal::app::{Trial, lab, shape};
use trustworthiness_checker::core::Value;
use trustworthiness_checker::io::rpio::{
    JournalArrival, JsonValue, KnowledgeResolver, WireDecoder,
};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn fixtures() -> PathBuf {
    root().join("tests/fixtures/journal")
}

const TRIALS: [&str; 9] = [
    "nominal",
    "nominal@redis",
    "adapt",
    "retries-then-accept",
    "budget-abort",
    "no-plan",
    "not-applied",
    "unsolicited-execute",
    "acquisitions-at-limit",
];

fn sha256(path: &Path) -> String {
    Sha256::digest(std::fs::read(path).unwrap())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// One journal: its records and where they must start, in which run and
/// with which clock.
struct Capture {
    name: String,
    lines: Vec<String>,
    after: u64,
    epoch: String,
    clock: String,
}

fn read_lines(path: PathBuf) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect()
}

fn captures() -> Vec<Capture> {
    let mut out: Vec<Capture> = TRIALS
        .iter()
        .map(|name| {
            let trial = Trial::load(&fixtures().join("app"), name);
            Capture {
                name: name.to_string(),
                after: trial.after(),
                epoch: trial.epoch().into(),
                clock: trial.clock_label().into(),
                lines: trial.lines,
            }
        })
        .collect();
    let fragment: Json = serde_json::from_slice(
        &std::fs::read(fixtures().join("fragment/long-effect.fragment.json")).unwrap(),
    )
    .unwrap();
    out.push(Capture {
        name: "long-effect fragment".into(),
        lines: read_lines(fixtures().join("fragment/long-effect.jsonl")),
        after: 0,
        epoch: fragment["epoch"].as_str().unwrap().into(),
        clock: fragment["journal_clock"].as_str().unwrap().into(),
    });
    for (stem, trace) in lab::all() {
        out.push(Capture {
            name: format!("lab {stem}"),
            lines: trace.lines(),
            after: 0,
            epoch: lab::EPOCH.into(),
            clock: lab::CLOCK.into(),
        });
    }
    out.push(Capture {
        name: "producer nominal".into(),
        lines: read_lines(fixtures().join("producer/nominal.jsonl")),
        after: 0,
        epoch: "nominal-epoch".into(),
        clock: "python:time.time_ns".into(),
    });
    out
}

/// The converted captures are the ones each trial's metadata names: their
/// bytes hash to what it records.
#[test]
fn captures_match_their_recorded_hashes() {
    for name in TRIALS {
        let dir = fixtures().join("app");
        let meta: Json =
            serde_json::from_slice(&std::fs::read(dir.join(format!("{name}.trial.json"))).unwrap())
                .unwrap();
        assert_eq!(meta["journal_schema_version"], 4, "{name}");
        assert_eq!(
            read_lines(dir.join(meta["capture_file"].as_str().unwrap())).len() as u64,
            meta["record_count"].as_u64().unwrap(),
            "{name}: capture extent"
        );
        assert_eq!(
            sha256(&dir.join(meta["capture_file"].as_str().unwrap())),
            meta["capture_sha256"].as_str().unwrap(),
            "{name}"
        );
    }
    let fragment: Json = serde_json::from_slice(
        &std::fs::read(fixtures().join("fragment/long-effect.fragment.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        sha256(&fixtures().join("fragment/long-effect.jsonl")),
        fragment["fragment_sha256"].as_str().unwrap()
    );
}

/// Every archived inline JSON record retains its position, epoch and clock.
/// None is silently accepted as a native classic packet.
#[test]
fn archived_inline_fixtures_preserve_positions_clocks_and_are_not_native_packets() {
    for capture in captures() {
        let mut kinds = BTreeMap::<String, usize>::new();
        for (index, line) in capture.lines.iter().enumerate() {
            let record: Json = serde_json::from_str(line).unwrap();
            assert_eq!(record["schema_version"], 4, "{}", capture.name);
            assert_eq!(
                record["position"]["seq"].as_u64().unwrap(),
                capture.after + index as u64 + 1,
                "{}",
                capture.name
            );
            assert_eq!(record["position"]["epoch"], capture.epoch);
            assert_eq!(record["arrival"]["journal_clock"], capture.clock);
            *kinds
                .entry(record["$tag"].as_str().unwrap().into())
                .or_default() += 1;
            let json = JsonValue::parse(line).unwrap();
            let mut decoder = WireDecoder::new();
            let result = if record["$tag"] == "Knowledge" {
                decoder.knowledge_record(&json)
            } else {
                decoder.phase_record(&json)
            };
            assert!(
                result.is_err(),
                "archived {}:{} must not become native input",
                capture.name,
                index + 1
            );
        }
        assert!(kinds.contains_key("Phase"), "{}: {kinds:?}", capture.name);
    }
}

/// Archived inline fixtures retain their historical shape: Knowledge values are plain
/// objects (no tagged union, no tagged Option), only Event records carry
/// evidence slots, and a woken Start names an earlier emission.
#[test]
fn archived_inline_records_keep_their_original_shape() {
    fn tagged_option(value: &Json) -> bool {
        match value {
            Json::Object(members) => {
                members
                    .get("$tag")
                    .is_some_and(|tag| tag == "Some" || tag == "None")
                    || members.values().any(tagged_option)
            }
            Json::Array(items) => items.iter().any(tagged_option),
            _ => false,
        }
    }
    for capture in captures() {
        let records: Vec<Json> = capture
            .lines
            .iter()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        for record in &records {
            assert_eq!(record["schema_version"], 4);
            for tuple in shape::tuples(record) {
                assert!(tuple["value"].is_object(), "{}: {tuple}", capture.name);
                assert!(!tagged_option(&tuple["value"]), "{}: {tuple}", capture.name);
            }
            if record["$tag"] == "Phase" && shape::step(record) != Some("Event") {
                assert_eq!(
                    record["payload"]["evidence"],
                    serde_json::json!({}),
                    "{}",
                    capture.name
                );
            }
            if let Some(cause) = shape::cause(record) {
                assert!(
                    records.iter().any(|r| shape::emission(r) == Some(cause)
                        && shape::position_seq(r) < shape::position_seq(record)),
                    "{}: {record}",
                    capture.name
                );
            }
        }
    }
}

fn native_directory() -> PathBuf {
    fixtures().join("native-v2")
}

fn native_manifest() -> Json {
    serde_json::from_slice(&std::fs::read(native_directory().join("manifest.json")).unwrap())
        .unwrap()
}

fn native_captures() -> Vec<Json> {
    let manifest = native_manifest();
    manifest["captures"]
        .as_array()
        .unwrap()
        .iter()
        .chain(manifest["handwritten"].as_array().unwrap())
        .cloned()
        .collect()
}

fn native_path(capture: &Json) -> PathBuf {
    native_directory().join(capture["file"].as_str().unwrap())
}

fn value_of(value: &Json) -> Value {
    JsonValue::parse(&value.to_string())
        .unwrap()
        .to_value()
        .unwrap()
}

fn ref_key(value: &Value) -> (String, i64, String) {
    let Value::Map(fields) = value else {
        panic!("a reference is a struct: {value}")
    };
    let (Value::Str(epoch), Value::Int(write), Value::Str(key)) = (
        &fields["store_epoch"],
        &fields["global_write_sequence"],
        &fields["key"],
    ) else {
        panic!("an exact reference has epoch/write/key: {value}")
    };
    (epoch.to_string(), *write, key.to_string())
}

fn assert_resolved(
    value: &Value,
    history: &BTreeMap<(String, i64, String), (Value, Value)>,
) -> usize {
    match value {
        Value::Map(fields) => {
            if let (Some(reference), Some(value), Some(writer)) = (
                fields.get("ref"),
                fields.get("value"),
                fields.get("written_by"),
            ) {
                let expected = &history[&ref_key(reference)];
                assert_eq!(value, &expected.0, "exact reference keeps its value");
                assert_eq!(writer, &expected.1, "exact reference keeps its writer");
                1
            } else {
                fields.values().map(|v| assert_resolved(v, history)).sum()
            }
        }
        Value::Union(union) => union.payload().map_or(0, |v| assert_resolved(v, history)),
        Value::List(items) | Value::Tuple(items) => {
            items.iter().map(|v| assert_resolved(v, history)).sum()
        }
        _ => 0,
    }
}

#[test]
fn native_fixture_manifest_preserves_capture_hash_extent_clock_and_lineage() {
    let manifest = native_manifest();
    assert_eq!(manifest["schema_version"], 2);
    for capture in native_captures() {
        let path = native_path(&capture);
        assert_eq!(
            sha256(&path),
            capture["sha256"].as_str().unwrap(),
            "{}",
            capture["name"]
        );
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(
            read_lines(path).len() as u64,
            capture["records"].as_u64().unwrap()
        );
        assert!(!capture["journal_clock"].as_str().unwrap().is_empty());
        assert!(!capture["lineage"].as_str().unwrap().is_empty());
    }
    for capture in manifest["captures"].as_array().unwrap() {
        assert_eq!(
            capture["producer_heads"]["producer"]
                .as_str()
                .unwrap()
                .len(),
            40
        );
        assert_eq!(
            capture["producer_heads"]["study"].as_str().unwrap().len(),
            40
        );
    }
}

#[test]
fn native_classic_packets_decode_and_resolve_exact_values_and_writers() {
    let mut source_positions_are_not_write_sequences = false;
    for capture in native_captures() {
        let lines = read_lines(native_path(&capture));
        let records: Vec<Json> = lines
            .iter()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        let mut resolver = KnowledgeResolver::new();
        let mut history = BTreeMap::new();
        // Materialised two-stream replay indexes history first: historical
        // handwritten writes can precede their Knowledge definition in this
        // file, and must still resolve without changing source positions.
        for (index, (line, raw)) in lines.iter().zip(&records).enumerate() {
            if raw["$tag"] != "Knowledge" {
                continue;
            }
            let decoded = WireDecoder::new()
                .knowledge_record(&JsonValue::parse(line).unwrap())
                .unwrap();
            resolver.remember(&decoded).unwrap();
            let version = &raw["payload"]["version"];
            let reference = value_of(&version["ref"]);
            let key = ref_key(&reference);
            if let Some(epoch) = capture["store_epoch"].as_str() {
                assert_eq!(key.0, epoch);
            }
            source_positions_are_not_write_sequences |= key.1 != (index + 1) as i64;
            let expected = (
                value_of(&version["value"]),
                value_of(&version["written_by"]),
            );
            if let Some(old) = history.insert(key, expected.clone()) {
                assert_eq!(old, expected);
            }
        }
        let mut decoder = WireDecoder::new();
        let mut phases = 0;
        let mut resolved_count = 0;
        for (position, (line, raw)) in lines.iter().zip(&records).enumerate() {
            assert_eq!(raw["schema_version"], 2);
            assert!(
                raw.get("position").is_none(),
                "file position is external metadata"
            );
            let json = JsonValue::parse(line).unwrap();
            let decoded = if raw["$tag"] == "Knowledge" {
                decoder.knowledge_record(&json)
            } else {
                phases += 1;
                decoder.phase_record(&json)
            }
            .unwrap_or_else(|error| panic!("{}:{}: {error}", capture["name"], position + 1));
            let resolved = resolver.resolve(&decoded).unwrap();
            resolved_count += assert_resolved(&resolved, &history);
            let arrival =
                JournalArrival::<trustworthiness_checker::core::time::SecondsDomain>::try_from(
                    &resolved,
                )
                .unwrap();
            assert_eq!(
                arrival.0,
                Some(raw["payload"]["arrival"]["journal_time"].as_f64().unwrap())
            );
        }
        assert!(phases > 0);
        if !history.is_empty() {
            assert!(resolved_count > history.len(), "Phase joins are exercised");
        }
    }
    assert!(source_positions_are_not_write_sequences);
}

#[test]
fn native_wire_keeps_knowledge_once_and_phase_references_separate() {
    for capture in native_captures() {
        for line in read_lines(native_path(&capture)) {
            let raw: Json = serde_json::from_str(&line).unwrap();
            let payload = &raw["payload"];
            if raw["$tag"] == "Knowledge" {
                let version = &payload["version"];
                assert_eq!(version["value"]["$tag"], version["ref"]["key"]);
                assert!(version["written_by"]["$tag"].is_string());
            } else if raw["$tag"] == "Phase" {
                assert!(
                    payload.get("evidence").is_none(),
                    "no inline v4 evidence slots"
                );
                let references = &payload["knowledge"];
                let refs: Vec<&Json> = match references["$tag"].as_str().unwrap() {
                    "Nothing" => vec![],
                    "One" => vec![&references["payload"]],
                    "Several" => references["payload"].as_array().unwrap().iter().collect(),
                    tag => panic!("unknown classic reference tag {tag}"),
                };
                for reference in refs {
                    assert!(reference["key"].is_string());
                    assert!(reference["store_epoch"].is_string());
                    assert!(reference["global_write_sequence"].as_i64().unwrap() > 0);
                    assert!(reference.get("value").is_none());
                    assert!(reference.get("written_by").is_none());
                }
            }
        }
    }
}

const NATIVE_ROOT: &str = r#"use experimental::{modules, generics, tagged_unions, functions, pattern_matching, newtypes}
use rpio::journal::timing::{Observation}
use std::option::{self, Option}
newtype JournalTime = Unit
in journal: Observation<Any, Any, Any, JournalTime>
out seen: Bool = std::option::is_some(journal.record)
"#;

fn native_run(
    bytes: &str,
    declaration: &str,
    check: Json,
    clock: &str,
) -> (std::process::Output, Vec<Json>) {
    use tc_testutils::journal::app::Scratch;
    let scratch = Scratch::new("classic-decode");
    let journal = scratch.write("capture.jsonl", bytes);
    let spec = scratch.write("root.dsrv", declaration);
    let provenance = scratch.file("provenance.jsonl");
    let config = scratch.write(
        "input.json",
        serde_json::json!({"sources":{"journal":{
            "kind":"rpio-observation-clock-file", "variable":"journal", "journal_clock":clock,
            "files":[journal], "capture_checks":[check], "clock_values":[], "provenance":provenance
        }}})
        .to_string(),
    );
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
        .arg(spec)
        .args([
            "--input-config",
            config.to_str().unwrap(),
            "--output-stdout",
            "--runtime",
            "dataflow",
            "--semantics",
            "gradual-typed-untimed",
            "--execution-policy",
            "synchronous",
        ])
        .env("RUST_LOG", "error")
        .current_dir(root())
        .output()
        .unwrap();
    let origins = std::fs::read_to_string(provenance)
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    (output, origins)
}

fn capture_check(bytes: &str, clock: &str) -> Json {
    serde_json::json!({"records":bytes.lines().count(),"sha256":format!("{:x}",Sha256::digest(bytes.as_bytes())),
        "journal_clock":clock})
}

fn refused(output: &std::process::Output, reason: &str) {
    assert!(!output.status.success(), "source must refuse {reason}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("panicked"), "{stderr}");
    assert!(stderr.contains(reason), "expected {reason}: {stderr}");
    assert!(
        output.stdout.is_empty(),
        "finite source validation must precede any evaluated output"
    );
}

#[test]
fn native_source_preserves_every_capture_position_and_admission_time() {
    for capture in native_captures() {
        let bytes = std::fs::read_to_string(native_path(&capture)).unwrap();
        let clock = capture["journal_clock"].as_str().unwrap();
        let check = serde_json::json!({"records":capture["records"],"sha256":capture["sha256"],
            "journal_clock":clock});
        let (output, provenance) = native_run(&bytes, NATIVE_ROOT, check, clock);
        assert!(
            output.status.success(),
            "{}: {}",
            capture["name"],
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            provenance.len() as u64,
            capture["records"].as_u64().unwrap()
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert_eq!(stdout.lines().count(), provenance.len());
        for (position, ((line, origin), shown)) in bytes
            .lines()
            .zip(&provenance)
            .zip(stdout.lines())
            .enumerate()
        {
            assert_eq!(shown, format!("seen[{position}] = true"));
            assert_eq!(origin["tick"], position);
            let origins = origin["origins"].as_array().unwrap();
            assert_eq!(origins.len(), 1);
            assert_eq!(origins[0]["member"], "journal");
            assert!(
                origins[0]["cursor"]
                    .as_str()
                    .unwrap()
                    .ends_with(&format!(":{}", position + 1))
            );
            let raw: Json = serde_json::from_str(line).unwrap();
            assert_eq!(
                origins[0]["time"],
                raw["payload"]["arrival"]["journal_time"]
            );
        }
    }
}

#[test]
fn native_source_rejects_changed_hash_extent_missing_terminator_and_clock_binding() {
    let capture = &native_captures()[0];
    let bytes = std::fs::read_to_string(native_path(capture)).unwrap();
    let clock = capture["journal_clock"].as_str().unwrap();
    let valid = capture_check(&bytes, clock);
    let mut hash = valid.clone();
    hash["sha256"] = Json::String("0".repeat(64));
    refused(&native_run(&bytes, NATIVE_ROOT, hash, clock).0, "SHA-256");
    let mut count = valid.clone();
    count["records"] = serde_json::json!(bytes.lines().count() + 1);
    refused(
        &native_run(&bytes, NATIVE_ROOT, count, clock).0,
        "capture extent",
    );
    let without_lf = bytes.trim_end_matches('\n');
    refused(
        &native_run(
            without_lf,
            NATIVE_ROOT,
            capture_check(without_lf, clock),
            clock,
        )
        .0,
        "final line terminator",
    );
    refused(
        &native_run(&bytes, NATIVE_ROOT, valid, "another.clock").0,
        "capture clock",
    );
}

fn reading(payload: Json) -> Json {
    serde_json::json!({"schema_version":2,"$tag":"Knowledge","payload":{
        "version":{"ref":{"store_epoch":"typed-source","global_write_sequence":1,"key":"Reading"},
        "value":{"$tag":"Reading","payload":payload},
        "written_by":{"$tag":"Node","payload":{"run_id":"typed-source","name":"Seed"}}},
        "arrival":{"journal_time":1}}})
}

#[test]
fn native_source_checks_unused_domain_fields_and_normalizes_declared_options_and_floats() {
    let source = NATIVE_ROOT.replace("Observation<Any, Any, Any, JournalTime>",
        "Observation<Union<Reading: Struct<value: Float, unused: Bool, optional: Option<Int>>>, Any, Any, JournalTime>");
    let valid = format!(
        "{}\n",
        reading(serde_json::json!({"value":7,"unused":true}))
    );
    let (output, provenance) = native_run(
        &valid,
        &source,
        capture_check(&valid, "typed.fixture"),
        "typed.fixture",
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(provenance.len(), 1);
    for payload in [
        serde_json::json!({"value":7,"unused":"bad"}),
        serde_json::json!({"value":7,"unused":true,"optional":null}),
        serde_json::json!({"value":"bad","unused":true}),
    ] {
        let bytes = format!("{}\n", reading(payload));
        refused(
            &native_run(
                &bytes,
                &source,
                capture_check(&bytes, "typed.fixture"),
                "typed.fixture",
            )
            .0,
            "RPIO observation",
        );
    }
}

#[test]
fn native_source_rejects_unknown_references_and_conflicting_immutable_values_or_writers() {
    let capture = native_captures()
        .into_iter()
        .find(|c| c["name"] == "clean@local")
        .unwrap();
    let bytes = std::fs::read_to_string(native_path(&capture)).unwrap();
    let mut records: Vec<Json> = bytes
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let phase = records
        .iter_mut()
        .find(|r| r["$tag"] == "Phase" && r["payload"]["knowledge"]["$tag"] == "One")
        .unwrap();
    phase["payload"]["knowledge"]["payload"]["global_write_sequence"] = serde_json::json!(999999);
    let changed = records.iter().map(|r| format!("{r}\n")).collect::<String>();
    let clock = capture["journal_clock"].as_str().unwrap();
    refused(
        &native_run(&changed, NATIVE_ROOT, capture_check(&changed, clock), clock).0,
        "never wrote it",
    );
    let original = reading(serde_json::json!({"value":7,"unused":true}));
    for writer in [false, true] {
        let mut duplicate = original.clone();
        if writer {
            duplicate["payload"]["version"]["written_by"]["payload"]["name"] =
                serde_json::json!("Other");
        } else {
            duplicate["payload"]["version"]["value"]["payload"]["value"] = serde_json::json!(8);
        }
        let bytes = format!("{original}\n{duplicate}\n");
        refused(
            &native_run(
                &bytes,
                NATIVE_ROOT,
                capture_check(&bytes, "typed.fixture"),
                "typed.fixture",
            )
            .0,
            "another value or writer",
        );
    }
}
