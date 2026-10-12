//! Running specifications over the ScrewDetection captures.
//!
//! A [`Trial`] is one capture from `tests/fixtures/journal/app`: its records
//! and its independently sampled clock. [`Journal`] is an editable copy of its records for SHARD-style
//! mutations (edit one record as JSON, delete or truncate a line). [`Checker`]
//! runs the checker binary on a specification with an explicit native
//! `rpio-observation-clock-file` projection built from the trial, and [`Run`] reads the result back by output
//! name and by journal position, using the provenance file the source writes.
//!
//! Nothing here interprets checker output: tests state their expectations
//! from the trial's scenario and the specification text.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value as Json, json};

/// One verified capture and its metadata.
#[derive(Clone, Debug)]
pub struct Trial {
    pub name: String,
    pub dir: PathBuf,
    pub meta: Json,
    /// The capture's records, one JSON text per line, without LF.
    pub lines: Vec<String>,
}

impl Trial {
    /// Load `<dir>/<name>.trial.json` and the capture it names.
    pub fn load(dir: &Path, name: &str) -> Self {
        let meta: Json = serde_json::from_slice(
            &std::fs::read(dir.join(format!("{name}.trial.json")))
                .unwrap_or_else(|error| panic!("{name}.trial.json: {error}")),
        )
        .unwrap();
        let capture = dir.join(meta["capture_file"].as_str().unwrap());
        let lines = std::fs::read_to_string(&capture)
            .map(|text| text.lines().map(str::to_owned).collect())
            .unwrap_or_default();
        Self {
            name: name.into(),
            dir: dir.into(),
            meta,
            lines,
        }
    }

    pub fn epoch(&self) -> &str {
        self.meta["observation_epoch"].as_str().unwrap()
    }
    pub fn clock_label(&self) -> &str {
        self.meta["journal_clock"].as_str().unwrap()
    }
    pub fn after(&self) -> u64 {
        self.meta["start_after_seq"].as_u64().unwrap()
    }
    pub fn through(&self) -> u64 {
        self.meta["through_seq"].as_u64().unwrap()
    }
    pub fn clock_file(&self) -> PathBuf {
        self.dir.join(self.meta["clock_file"].as_str().unwrap())
    }
    /// The independently sampled clock, as `(time, through)`.
    pub fn clock_samples(&self) -> Vec<(f64, u64)> {
        std::fs::read_to_string(self.clock_file())
            .unwrap()
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                let sample: Json = serde_json::from_str(line).unwrap();
                (
                    sample["time"].as_f64().unwrap(),
                    sample["through"].as_u64().unwrap(),
                )
            })
            .collect()
    }

    /// The record at position `seq`, parsed.
    pub fn record(&self, seq: u64) -> Json {
        serde_json::from_str(&self.lines[(seq - self.after() - 1) as usize]).unwrap()
    }

    /// Positions of the records `select` accepts, in order.
    pub fn positions(&self, select: impl Fn(&Json) -> bool) -> Vec<u64> {
        (self.after() + 1..=self.through())
            .filter(|seq| select(&self.record(*seq)))
            .collect()
    }

    /// An editable archived capture read from bytes, retaining malformed input.
    pub fn journal_from_bytes(&self, bytes: &[u8]) -> Journal {
        let final_lf = bytes.ends_with(b"\n");
        let mut lines: Vec<Vec<u8>> = bytes.split(|b| *b == b'\n').map(|l| l.to_vec()).collect();
        if final_lf {
            lines.pop();
        }
        Journal {
            after: self.after(),
            lines,
            final_lf,
        }
    }

    /// An editable copy of the records.
    pub fn journal(&self) -> Journal {
        Journal {
            after: self.after(),
            lines: self
                .lines
                .iter()
                .map(|line| line.clone().into_bytes())
                .collect(),
            final_lf: true,
        }
    }
}

/// What a record is, read from its JSON: its tag, step tag and names.
pub mod shape {
    use super::Json;

    /// The record's own position in its run.
    pub fn position_seq(record: &Json) -> u64 {
        record["position"]["seq"].as_u64().unwrap()
    }
    pub fn step(record: &Json) -> Option<&str> {
        record["payload"]["step"]["$tag"].as_str()
    }
    pub fn producer(record: &Json) -> Option<&str> {
        record["payload"]["activation"]["producer"].as_str()
    }
    pub fn start_seq(record: &Json) -> Option<i64> {
        record["payload"]["activation"]["start_seq"].as_i64()
    }
    /// The topic an Event publishes on.
    pub fn event_name(record: &Json) -> Option<&str> {
        (step(record) == Some("Event"))
            .then(|| record["payload"]["step"]["payload"]["topic"].as_str())
            .flatten()
    }
    pub fn is_start_of(record: &Json, producer_name: &str) -> bool {
        step(record) == Some("Start") && producer(record) == Some(producer_name)
    }
    pub fn is_end_of(record: &Json, producer_name: &str) -> bool {
        step(record) == Some("End") && producer(record) == Some(producer_name)
    }
    /// `(start_seq, sequence_no)` of the emission that woke a Start.
    pub fn cause(record: &Json) -> Option<(i64, i64)> {
        let cause = &record["payload"]["activation"]["caused_by"];
        (!cause.is_null()).then(|| {
            (
                cause["activation"]["start_seq"].as_i64().unwrap(),
                cause["sequence_no"].as_i64().unwrap(),
            )
        })
    }
    /// Whether `value` is a Knowledge tuple `{ref, value, written_by}`.
    pub fn is_tuple(value: &Json) -> bool {
        value.get("ref").is_some()
            && value.get("value").is_some()
            && value.get("written_by").is_some()
    }
    /// Every Knowledge tuple a record carries, wherever it is: a Knowledge
    /// record's version, a Read's or Write's value, an Event's evidence and
    /// an End outcome's fields.
    pub fn tuples(record: &Json) -> Vec<&Json> {
        fn walk<'a>(value: &'a Json, out: &mut Vec<&'a Json>) {
            if is_tuple(value) {
                out.push(value);
                return;
            }
            match value {
                Json::Object(members) => members.values().for_each(|m| walk(m, out)),
                Json::Array(items) => items.iter().for_each(|i| walk(i, out)),
                _ => {}
            }
        }
        let mut out = Vec::new();
        walk(record, &mut out);
        out
    }
    /// Apply `edit` to every Knowledge tuple a record carries.
    pub fn tuples_mut(record: &mut Json, edit: &mut dyn FnMut(&mut Json)) {
        if is_tuple(record) {
            edit(record);
            return;
        }
        match record {
            Json::Object(members) => members.values_mut().for_each(|m| tuples_mut(m, edit)),
            Json::Array(items) => items.iter_mut().for_each(|i| tuples_mut(i, edit)),
            _ => {}
        }
    }
    /// The tuple a record commits: a Write's value or a Knowledge version.
    pub fn written(record: &Json) -> Option<&Json> {
        match (record["$tag"].as_str(), step(record)) {
            (Some("Knowledge"), _) => Some(&record["payload"]["version"]),
            (_, Some("Write")) => Some(&record["payload"]["step"]["payload"]["value"]),
            _ => None,
        }
    }
    /// The tuple an Event cites under `slot`.
    pub fn cited<'a>(record: &'a Json, slot: &str) -> &'a Json {
        &record["payload"]["evidence"][slot]
    }
    /// `(start_seq, sequence_no)` an Event publishes.
    pub fn emission(record: &Json) -> Option<(i64, i64)> {
        (step(record) == Some("Event")).then(|| {
            (
                start_seq(record).unwrap(),
                record["payload"]["step"]["payload"]["sequence_no"]
                    .as_i64()
                    .unwrap(),
            )
        })
    }
}

/// Editable capture lines for SHARD-style mutations. Positions name the
/// original records, so edits compose.
#[derive(Clone, Debug)]
pub struct Journal {
    after: u64,
    lines: Vec<Vec<u8>>,
    final_lf: bool,
}

impl Journal {
    fn index(&self, seq: u64) -> usize {
        (seq - self.after - 1) as usize
    }
    /// The record at position `seq`, parsed, as edited so far.
    pub fn record(&self, seq: u64) -> Json {
        serde_json::from_slice(&self.lines[self.index(seq)]).unwrap()
    }
    /// Edit the record at `seq` as JSON.
    pub fn edit(mut self, seq: u64, edit: impl FnOnce(&mut Json)) -> Self {
        let index = self.index(seq);
        let mut record: Json = serde_json::from_slice(&self.lines[index]).unwrap();
        edit(&mut record);
        self.lines[index] = serde_json::to_vec(&record).unwrap();
        self
    }
    /// Replace the bytes of the record at `seq`.
    pub fn replace(mut self, seq: u64, bytes: impl Into<Vec<u8>>) -> Self {
        let index = self.index(seq);
        self.lines[index] = bytes.into();
        self
    }
    /// Delete the record at `seq`; later records keep their positions.
    pub fn delete(mut self, seq: u64) -> Self {
        let index = self.index(seq);
        self.lines.remove(index);
        self
    }
    /// Drop the final LF and `cut` bytes of the last record.
    pub fn truncate_last(mut self, cut: usize) -> Self {
        let last = self.lines.last_mut().unwrap();
        let keep = last.len().saturating_sub(cut);
        last.truncate(keep);
        self.final_lf = false;
        self
    }
    pub fn bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for (index, line) in self.lines.iter().enumerate() {
            out.extend_from_slice(line);
            if self.final_lf || index + 1 < self.lines.len() {
                out.push(b'\n');
            }
        }
        out
    }
}

/// Native reference-journal projection of a retained inline-evidence capture.
/// `positions` maps physical native lines back to archived facts; inserted
/// immutable Knowledge definitions have no archived record position.
#[derive(Clone, Debug)]
pub struct NativeCapture {
    pub bytes: Vec<u8>,
    pub positions: Vec<Option<u64>>,
    pub archive_records: usize,
}

impl NativeCapture {
    pub fn from_archive(journal: &Journal, schema: &Json, epoch: &str) -> Self {
        fn tag(name: &str, payload: Option<Json>) -> Json {
            let mut v = json!({"$tag": name});
            if let Some(payload) = payload {
                v["payload"] = payload;
            }
            v
        }
        fn fields(value: &Json, declaration: &Json) -> Json {
            let mut out = value.as_object().cloned().unwrap_or_default();
            for (name, ty) in declaration.as_object().unwrap() {
                if ty.as_str().unwrap().starts_with("Option<") {
                    let value = out.remove(name).unwrap_or(Json::Null);
                    let wrapped = if value.get("$tag").is_some() {
                        value
                    } else if value.is_null() {
                        tag("None", None)
                    } else {
                        tag("Some", Some(value))
                    };
                    out.insert(name.clone(), wrapped);
                }
            }
            Json::Object(out)
        }
        fn token(value: &Json) -> Json {
            Json::String(
                value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string()),
            )
        }
        fn emission(value: &Json) -> Json {
            json!({"activation_id": token(&value["activation"]["start_seq"]),
                "ordinal": value["sequence_no"]})
        }
        fn activation(value: &Json, epoch: &str) -> Json {
            let cause = if value["caused_by"].is_null() {
                tag("None", None)
            } else {
                tag("Some", Some(emission(&value["caused_by"])))
            };
            json!({"run_id": value.get("epoch").unwrap_or(&json!(epoch)),
                "activation_id": token(&value["start_seq"]), "producer": value["producer"],
                "role": value["role"], "loop": value["loop"], "caused_by": cause})
        }
        fn resolved(value: &Json, schema: &Json, epoch: &str) -> Json {
            let key = value["ref"]["key"].as_str().unwrap();
            let writer = &value["written_by"];
            let writer = match writer["$tag"].as_str() {
                Some("Activation") => tag(
                    "Activation",
                    Some(json!({
                    "run_id": writer["payload"].get("epoch").unwrap_or(&json!(epoch)),
                    "activation_id": token(&writer["payload"]["start_seq"])})),
                ),
                Some("Node") => tag(
                    "Node",
                    Some(json!({"run_id": epoch,
                    "name": writer["payload"]["name"]})),
                ),
                _ => writer.clone(),
            };
            json!({"ref": value["ref"], "written_by": writer,
                "value": tag(key, Some(fields(&value["value"], &schema["knowledge"][key])))})
        }
        fn refs(value: &Json) -> Json {
            if shape::is_tuple(value) {
                return value["ref"].clone();
            }
            match value {
                Json::Object(v) => {
                    Json::Object(v.iter().map(|(k, v)| (k.clone(), refs(v))).collect())
                }
                Json::Array(v) => Json::Array(v.iter().map(refs).collect()),
                _ => value.clone(),
            }
        }
        fn event_tag(topic: &str) -> String {
            topic
                .split('_')
                .map(|s| {
                    let mut chars = s.chars();
                    chars
                        .next()
                        .map(|c| c.to_uppercase().collect::<String>() + chars.as_str())
                        .unwrap_or_default()
                })
                .collect()
        }
        fn emitted(topic: &str, message: &Json, schema: &Json) -> Json {
            let name = event_tag(topic);
            let detail = if topic == "adaptation_aborted" {
                tag(
                    &name,
                    Some(json!({"reason": message["reason"].as_str().unwrap_or("")})),
                )
            } else {
                tag(&name, None)
            };
            json!({"detail": detail, "message": fields(message, &schema["message"])})
        }
        let mut out = Self {
            bytes: Vec::new(),
            positions: Vec::new(),
            archive_records: journal.lines.len(),
        };
        let mut admitted = BTreeMap::<String, Json>::new();
        let append = |out: &mut Self, record: Json, position: Option<u64>| {
            out.bytes.extend(serde_json::to_vec(&record).unwrap());
            out.bytes.push(b'\n');
            out.positions.push(position);
        };
        for line in &journal.lines {
            let Ok(record) = serde_json::from_slice::<Json>(line) else {
                // Keep malformed/deep input as a source error rather than making
                // the fixture adapter itself the component under test.
                out.bytes.extend(line);
                out.bytes.push(b'\n');
                out.positions.push(None);
                continue;
            };
            let position = record["position"]["seq"].as_u64();
            let arrival = record["arrival"].clone();
            let own_knowledge = record["$tag"] == "Knowledge";
            let tuples = shape::tuples(&record);
            for tuple in tuples {
                let tuple = resolved(tuple, schema, epoch);
                let identity = serde_json::to_string(&tuple["ref"]).unwrap();
                // Repeated identical references are idempotent. Contradictory
                // copies become contradictory immutable definitions, which the
                // real source must reject; never silently erase a mutation.
                if admitted.get(&identity) != Some(&tuple) || own_knowledge {
                    append(
                        &mut out,
                        json!({"schema_version":2,"$tag":"Knowledge",
                        "payload":{"version":tuple,"arrival":arrival}}),
                        if own_knowledge { position } else { None },
                    );
                    admitted.insert(identity, tuple);
                }
            }
            if own_knowledge {
                continue;
            }
            let p = &record["payload"];
            let act = activation(&p["activation"], epoch);
            let mut step = p["step"].clone();
            let mut knowledge = Vec::new();
            match shape::step(&record) {
                Some("Start") => {
                    let t = &p["step"]["payload"];
                    let topic = t["topic"].as_str().unwrap_or("unrecognized");
                    let message = &t["message"];
                    let trigger = if topic == "sensor_data_received" {
                        tag(
                            "SensorDataReceived",
                            Some(fields(message, &schema["trigger"])),
                        )
                    } else if [
                        "observation_recorded",
                        "detection_completed",
                        "cycle_completed",
                        "anomaly_detected",
                        "plan_generated",
                        "plan_validated",
                        "plan_rejected",
                        "plan_executed",
                        "adaptation_aborted",
                    ]
                    .contains(&topic)
                    {
                        tag(&event_tag(topic), Some(emitted(topic, message, schema)))
                    } else {
                        tag("Unrecognized", Some(json!(topic)))
                    };
                    step = tag("Start", Some(json!({"trigger":trigger})));
                }
                Some("Read") => {
                    let k = &p["step"]["payload"]["value"];
                    if shape::is_tuple(k) {
                        knowledge.push(k["ref"].clone());
                    } else if k["$tag"] == "Some" {
                        knowledge.push(k["payload"]["ref"].clone());
                    }
                    step = tag("Read", Some(json!({"key":p["step"]["payload"]["key"]})));
                }
                Some("Write") => {
                    knowledge.push(p["step"]["payload"]["value"]["ref"].clone());
                    step = tag("Write", Some(json!({"key":p["step"]["payload"]["key"]})));
                }
                Some("Event") => {
                    let e = &p["step"]["payload"];
                    let topic = e["topic"].as_str().unwrap();
                    knowledge = p["evidence"]
                        .as_object()
                        .unwrap()
                        .values()
                        .map(|t| t["ref"].clone())
                        .collect();
                    step = tag(
                        "Event",
                        Some(
                            json!({"name":topic,"payload":emitted(topic,&e["message"],schema),
                        "emission":{"activation_id":act["activation_id"],"ordinal":e["sequence_no"]}}),
                        ),
                    );
                }
                Some("ServiceCompleted") => {
                    let outcome = &p["step"]["payload"]["outcome"];
                    if let Some(name) = outcome.as_str() {
                        step["payload"]["outcome"] = tag(name, None);
                    }
                }
                Some("End") => {
                    knowledge = shape::tuples(&p["step"]["payload"]["outcome"])
                        .iter()
                        .map(|t| t["ref"].clone())
                        .collect();
                    step = tag(
                        "End",
                        Some(json!({"outcome":refs(&p["step"]["payload"]["outcome"])})),
                    );
                }
                _ => {}
            }
            let k = if knowledge.is_empty() {
                tag("Nothing", None)
            } else if knowledge.len() == 1 {
                tag("One", Some(knowledge.remove(0)))
            } else {
                tag("Several", Some(json!(knowledge)))
            };
            let record_tag = record["$tag"].as_str().unwrap();
            let mut payload = json!({"activation":act,"arrival":arrival});
            if record_tag == "Phase" {
                payload["step"] = step;
                payload["knowledge"] = k;
            }
            append(
                &mut out,
                json!({"schema_version":2,"$tag":record_tag,"payload":payload}),
                position,
            );
        }
        if !journal.final_lf {
            out.bytes.pop();
        }
        out
    }
    /// Number of physical records covered by an archived fact boundary.
    pub fn through(&self, seq: u64) -> usize {
        self.positions
            .iter()
            .rposition(|p| p.is_some_and(|p| p <= seq))
            .map_or(0, |i| i + 1)
    }
    pub fn map_run(&self, run: &mut Run) {
        for row in &mut run.provenance {
            if let Some(origins) = row["origins"].as_array_mut() {
                for origin in origins {
                    if origin["member"] == "journal" {
                        let line = origin["cursor"]
                            .as_str()
                            .unwrap()
                            .rsplit_once(':')
                            .unwrap()
                            .1
                            .parse::<usize>()
                            .unwrap();
                        origin["archive_position"] = self
                            .positions
                            .get(line - 1)
                            .copied()
                            .flatten()
                            .map_or(Json::Null, |p| json!(p));
                    }
                }
            }
        }
    }
}

/// A fresh directory under the system temporary directory, removed on drop.
pub struct Scratch(PathBuf);

impl Scratch {
    pub fn new(tag: &str) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "tc-{tag}-{}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    pub fn path(&self) -> &Path {
        &self.0
    }
    pub fn file(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
    pub fn write(&self, name: &str, bytes: impl AsRef<[u8]>) -> PathBuf {
        let path = self.file(name);
        std::fs::write(&path, bytes).unwrap();
        path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The checker binary and where it runs.
#[derive(Clone, Debug)]
pub struct Checker {
    pub binary: PathBuf,
    pub cwd: PathBuf,
}

/// What one checker run produced.
#[derive(Clone, Debug)]
pub struct Run {
    pub success: bool,
    pub stderr: String,
    /// Output values by name, by row.
    pub outputs: BTreeMap<String, BTreeMap<usize, String>>,
    /// Each tick's provenance line, by row.
    pub provenance: Vec<Json>,
    /// The journal file's `after`: provenance cursors name 1-based lines.
    pub after: u64,
}

impl Checker {
    /// Run `spec` with `config` on `runtime`, printing outputs to stdout.
    pub fn run(&self, spec: &Path, config: &Json, runtime: &str, after: u64) -> Run {
        let scratch = Scratch::new("run");
        let config_path = scratch.write("input.json", serde_json::to_vec(config).unwrap());
        let provenance = config["sources"]["journal"]["provenance"]
            .as_str()
            .map(PathBuf::from);
        let output = Command::new(&self.binary)
            .arg(spec)
            .args(["--input-config", config_path.to_str().unwrap()])
            .args(["--output-stdout", "--runtime", runtime])
            .env("RUST_LOG", "error")
            .current_dir(&self.cwd)
            .output()
            .expect("the checker runs");
        let mut outputs = BTreeMap::<String, BTreeMap<usize, String>>::new();
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            let Some((key, value)) = line.split_once(" = ") else {
                continue;
            };
            let Some((name, row)) = key.split_once('[') else {
                continue;
            };
            let Ok(row) = row.trim_end_matches(']').parse() else {
                continue;
            };
            outputs
                .entry(name.to_owned())
                .or_default()
                .insert(row, value.to_owned());
        }
        let provenance = provenance
            .and_then(|path| std::fs::read_to_string(path).ok())
            .map(|text| {
                text.lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect()
            })
            .unwrap_or_default();
        Run {
            success: output.status.success(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            outputs,
            provenance,
            after,
        }
    }
}

impl Run {
    /// Rows printed for an output.
    pub fn rows(&self, name: &str) -> usize {
        self.outputs.get(name).map_or(0, BTreeMap::len)
    }

    /// The value of `name` on `row`.
    pub fn at(&self, name: &str, row: usize) -> &str {
        self.outputs
            .get(name)
            .and_then(|rows| rows.get(&row))
            .unwrap_or_else(|| panic!("no {name}[{row}]"))
    }

    /// Whether `name` printed `true` on `row`.
    pub fn flag(&self, name: &str, row: usize) -> bool {
        self.at(name, row) == "true"
    }

    /// Rows on which `name` printed `true`.
    pub fn true_rows(&self, name: &str) -> Vec<usize> {
        self.outputs
            .get(name)
            .map(|rows| {
                rows.iter()
                    .filter(|(_, value)| *value == "true")
                    .map(|(row, _)| *row)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The journal position a row's record came from, if it carried one.
    pub fn position(&self, row: usize) -> Option<u64> {
        let origins = self.provenance.get(row)?["origins"].as_array()?;
        origins
            .iter()
            .find(|o| o["member"] == "journal")
            .and_then(|origin| {
                if let Some(position) = origin.get("archive_position") {
                    return position.as_u64();
                }
                let cursor = origin["cursor"].as_str().unwrap();
                let (_, tail) = cursor.rsplit_once(':').unwrap();
                match tail.split_once('-') {
                    // A Redis entry ID `<seq>-1`.
                    Some((seq, _)) => Some(seq.parse().unwrap()),
                    None => Some(self.after + tail.parse::<u64>().unwrap()),
                }
            })
    }

    /// The row carrying the record at `seq`.
    pub fn row_of(&self, seq: u64) -> usize {
        (0..self.provenance.len())
            .find(|row| self.position(*row) == Some(seq))
            .unwrap_or_else(|| panic!("no row carries position {seq}"))
    }

    /// Positions of the records on rows where `name` printed `true`.
    pub fn true_positions(&self, name: &str) -> Vec<u64> {
        self.true_rows(name)
            .into_iter()
            .filter_map(|row| self.position(row))
            .collect()
    }

    /// Every output, row by row, for comparing whole traces.
    pub fn trace(&self) -> &BTreeMap<String, BTreeMap<usize, String>> {
        &self.outputs
    }
}

/// Small journals in the ScrewDetection vocabulary for the library and
/// runtime tests. The record on line `k` is at position `k`, so the tests'
/// row indices are positions. `tests/fixtures/journal/lab` holds their files;
/// a drift test keeps them equal to these recipes.
pub mod lab {
    use serde_json::{Value as Json, json};

    use crate::journal::{ActivationId, Emission, Resolved, Role, Trace, TraceBuilder, outcome};

    pub const EPOCH: &str = "lab-run";
    pub const CLOCK: &str = "fixture.journal";

    fn builder() -> TraceBuilder {
        TraceBuilder::new(EPOCH).clock_label(CLOCK)
    }

    fn at(t: &mut TraceBuilder, seconds: f64) {
        t.at_us((seconds * 1e6).round() as i64);
    }

    pub fn frame_ref() -> Json {
        json!({"frame_path": "frames/cycle-1.jpg", "timestamp": "2026-10-09T00:00:00Z",
               "detection_type": "screw", "tcp_pose": null, "brightness": 120.0})
    }
    pub fn detections() -> Json {
        json!({"items": [], "screw_entropy": 0.9, "adaptive_cycle": true, "model_id": 1,
               "detection_type": "screw", "frame_path": "frames/cycle-1.jpg"})
    }
    pub fn entropy_history() -> Json {
        json!({"values": [0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9], "model_id": 1})
    }
    pub fn running_average() -> Json {
        json!({"value": 0.9})
    }
    pub fn active_model() -> Json {
        json!({"model_id": 1})
    }
    pub fn candidate_model() -> Json {
        json!({"model_id": 2})
    }
    pub fn in_planning() -> Json {
        json!({"in_planning": false})
    }
    pub fn replanning_counter() -> Json {
        json!({"count": 0})
    }
    pub fn legit_result() -> Json {
        json!({"is_legit": true, "candidate_model_id": 2, "candidate_avg_entropy": 0.1,
               "current_avg_entropy": 0.9, "evidence_count": 10, "evidence_complete": true,
               "evidence_matches_window": true, "evidence_unique": true})
    }
    pub fn action_command() -> Json {
        json!({"command": "swap_model", "model_id": 2, "effect_model_id": 2,
               "effect_acknowledged": true})
    }

    fn start(
        t: &mut TraceBuilder,
        role: Role,
        topic: &str,
        message: Json,
        cause: Option<&Emission>,
    ) -> ActivationId {
        t.start(role.as_str(), role, "event", topic, message, cause)
    }
    fn sensor_request() -> Json {
        json!({"detection_type": "screw"})
    }

    /// One adaptation: a Monitor, Analysis, Plan, Legitimate and Execute
    /// callback in turn, each woken by its predecessor, with Knowledge written
    /// outside any callback between them (which a producer does only before
    /// its loop starts). The anomaly cites an `EntropyHistory` its Analysis
    /// never read, so the coordinator reports it as not acquired: the journal
    /// exercises timing and Knowledge views, not a clean run.
    pub fn outside_writes() -> Trace {
        let mut t = builder();
        at(&mut t, 0.0);
        t.knowledge("Detections", detections(), "Analysis");
        at(&mut t, 0.5);
        t.knowledge("EntropyHistory", entropy_history(), "Analysis");
        at(&mut t, 1.0);
        let monitor = start(
            &mut t,
            Role::Monitor,
            "sensor_data_received",
            sensor_request(),
            None,
        );
        at(&mut t, 1.1);
        t.read(&monitor, "EntropyHistory");
        at(&mut t, 1.2);
        let frame = t.write(&monitor, "FrameRef", frame_ref());
        at(&mut t, 1.25);
        t.knowledge("ActiveModel", active_model(), "Execute");
        at(&mut t, 1.3);
        let observed = t.event(
            &monitor,
            "observation_recorded",
            json!({}),
            &[("frame", &frame)],
        );
        at(&mut t, 1.4);
        t.end(&monitor, outcome::monitored(&frame));

        at(&mut t, 2.0);
        let analysis = start(
            &mut t,
            Role::Analysis,
            "observation_recorded",
            json!({}),
            Some(&observed),
        );
        at(&mut t, 2.1);
        t.read(&analysis, "FrameRef");
        at(&mut t, 2.2);
        let average = t.write(&analysis, "RunningAvgEntropy", running_average());
        at(&mut t, 2.25);
        t.knowledge("InPlanning", in_planning(), "Analysis");
        at(&mut t, 2.3);
        let history: Resolved = t.latest("EntropyHistory").unwrap().clone();
        let anomaly = t.event(
            &analysis,
            "anomaly_detected",
            json!({}),
            &[("average", &average), ("history", &history)],
        );
        at(&mut t, 2.4);
        t.end(&analysis, outcome::anomaly(&average));

        at(&mut t, 3.0);
        let plan = start(
            &mut t,
            Role::Plan,
            "anomaly_detected",
            json!({}),
            Some(&anomaly),
        );
        at(&mut t, 3.1);
        let candidate = t.write(&plan, "CandidateModel", candidate_model());
        at(&mut t, 3.15);
        t.knowledge("ReplanningCounter", replanning_counter(), "Plan");
        at(&mut t, 3.2);
        let proposed = t.event(&plan, "plan_generated", json!({}), &[("plan", &candidate)]);
        at(&mut t, 3.3);
        t.end(&plan, outcome::generated(&candidate));

        at(&mut t, 4.0);
        let legitimate = start(
            &mut t,
            Role::Legitimate,
            "plan_generated",
            json!({}),
            Some(&proposed),
        );
        at(&mut t, 4.1);
        let result = t.write(&legitimate, "LegitResult", legit_result());
        at(&mut t, 4.15);
        t.knowledge("Detections", detections(), "Analysis");
        at(&mut t, 4.2);
        let accepted = t.event(
            &legitimate,
            "plan_validated",
            json!({}),
            &[("plan", &candidate), ("result", &result)],
        );
        at(&mut t, 4.3);
        t.end(&legitimate, outcome::legitimised(true, &candidate, &result));

        at(&mut t, 5.0);
        let execute = start(
            &mut t,
            Role::Execute,
            "plan_validated",
            json!({}),
            Some(&accepted),
        );
        at(&mut t, 5.1);
        t.service_started(&execute, "swap_model", "req-a");
        at(&mut t, 5.2);
        t.service_completed(&execute, "swap_model", "req-a", true);
        at(&mut t, 5.3);
        let effect = t.write(&execute, "ActionCommand", action_command());
        at(&mut t, 5.35);
        t.knowledge("InPlanning", in_planning(), "Execute");
        at(&mut t, 5.4);
        t.end(&execute, outcome::applied(&candidate, &effect));
        t.finish()
    }

    fn execute_start(t: &mut TraceBuilder) -> ActivationId {
        start(t, Role::Execute, "plan_validated", json!({}), None)
    }

    /// `service-rearm`: a swap request abandoned by its callback's failed End,
    /// a second callback's request, the first request's late completion, the
    /// second's completion, and a third callback.
    pub fn service_rearm() -> Trace {
        let mut t = builder();
        at(&mut t, 0.0);
        let first = execute_start(&mut t);
        at(&mut t, 1.0);
        t.service_started(&first, "swap_model", "req-a");
        at(&mut t, 3.0);
        t.end(
            &first,
            outcome::aborted(outcome::failed("owner callback failed")),
        );
        at(&mut t, 12.0);
        let second = execute_start(&mut t);
        at(&mut t, 13.0);
        t.service_started(&second, "swap_model", "req-b");
        at(&mut t, 14.0);
        t.service_completed(&first, "swap_model", "req-a", true);
        at(&mut t, 15.0);
        t.service_completed(&second, "swap_model", "req-b", true);
        at(&mut t, 16.0);
        execute_start(&mut t);
        t.finish()
    }

    /// `service-pending`: one swap request that never completes.
    pub fn service_pending() -> Trace {
        let mut t = builder();
        at(&mut t, 10.0);
        let execute = execute_start(&mut t);
        at(&mut t, 11.0);
        t.service_started(&execute, "swap_model", "req-a");
        t.finish()
    }

    /// `handoff-terminals`: a proposal whose Legitimate starts before the
    /// Plan ends (an overlap), a proposal withdrawn by its own End, and an
    /// unrelated Start.
    pub fn handoff_terminals() -> Trace {
        let mut t = builder();
        at(&mut t, 0.0);
        let first = start(&mut t, Role::Plan, "anomaly_detected", json!({}), None);
        at(&mut t, 2.0);
        let proposed = t.event(&first, "plan_generated", json!({}), &[]);
        at(&mut t, 3.0);
        start(
            &mut t,
            Role::Legitimate,
            "plan_generated",
            json!({}),
            Some(&proposed),
        );
        at(&mut t, 4.0);
        t.end(&first, outcome::no_plan());
        at(&mut t, 5.0);
        let second = start(&mut t, Role::Plan, "anomaly_detected", json!({}), None);
        at(&mut t, 6.0);
        t.event(&second, "plan_generated", json!({}), &[]);
        at(&mut t, 7.0);
        t.end(&second, outcome::no_plan());
        at(&mut t, 8.0);
        start(
            &mut t,
            Role::Monitor,
            "sensor_data_received",
            sensor_request(),
            None,
        );
        t.finish()
    }

    /// Every lab journal by file stem.
    pub fn all() -> Vec<(&'static str, Trace)> {
        vec![
            ("outside-writes", outside_writes()),
            ("service-rearm", service_rearm()),
            ("service-pending", service_pending()),
            ("handoff-terminals", handoff_terminals()),
        ]
    }
}
