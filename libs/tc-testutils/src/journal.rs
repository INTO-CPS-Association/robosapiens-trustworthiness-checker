//! Test-side writer of RPIO observation journal records (schema version 4).
//!
//! This module writes the records the simplified journal's wire format (W1)
//! defines. It deliberately does not depend on the checker crate: it is an
//! independent restatement of the wire format, so a test that decodes its
//! output with the checker compares two implementations rather than one with
//! itself.
//!
//! - [`TraceBuilder`] allocates contiguous positions, Start identities,
//!   per-activation emission numbers and write versions, so a trace is
//!   consistent by construction. Faults are injected afterwards by editing a
//!   record's JSON ([`Trace::records`] or [`app::Journal`]).
//! - [`outcome`] writes the fixed End outcome vocabulary.
//! - [`app`]: the converted ScrewDetection captures, the lab journals, SHARD-
//!   style edits and a checker runner that reads outputs back by position.
//! - [`strategies`]: proptest strategies and reference models for ordered
//!   delivery and `std::timing::deadline`.

use std::collections::HashMap;

use serde_json::{Value as Json, json};

pub mod app;
pub mod strategies;

/// The only accepted wire schema version.
pub const SCHEMA_VERSION: i64 = 4;
/// The clock label the live Redis binding names.
pub const REDIS_CLOCK: &str = "redis.server.time";

/// The five roles, spelled as the journal spells them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Role {
    Monitor,
    Analysis,
    Plan,
    Legitimate,
    Execute,
}

impl Role {
    pub const ALL: [Role; 5] = [
        Role::Monitor,
        Role::Analysis,
        Role::Plan,
        Role::Legitimate,
        Role::Execute,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Monitor => "Monitor",
            Role::Analysis => "Analysis",
            Role::Plan => "Plan",
            Role::Legitimate => "Legitimate",
            Role::Execute => "Execute",
        }
    }
}

/// `(epoch, start_seq)`: an activation's identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ActivationId {
    pub epoch: String,
    pub start_seq: i64,
}

impl ActivationId {
    pub fn new(epoch: &str, start_seq: i64) -> Self {
        Self {
            epoch: epoch.into(),
            start_seq,
        }
    }
    pub fn to_json(&self) -> Json {
        json!({"epoch": self.epoch, "start_seq": self.start_seq})
    }
}

/// One publication: its activation and its number there, from 1.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Emission {
    pub activation: ActivationId,
    pub sequence_no: i64,
}

impl Emission {
    pub fn to_json(&self) -> Json {
        json!({"activation": self.activation.to_json(), "sequence_no": self.sequence_no})
    }
}

/// Who wrote a Knowledge version.
#[derive(Clone, Debug, PartialEq)]
pub enum Writer {
    Activation(ActivationId),
    Node(String),
}

impl Writer {
    pub fn to_json(&self) -> Json {
        match self {
            Writer::Activation(id) => json!({"$tag": "Activation", "payload": id.to_json()}),
            Writer::Node(name) => json!({"$tag": "Node", "payload": {"name": name}}),
        }
    }
}

/// A Knowledge version: `{ref, value, written_by}`, its value plain JSON.
#[derive(Clone, Debug, PartialEq)]
pub struct Resolved {
    pub store_epoch: String,
    pub global_write_sequence: i64,
    pub key: String,
    pub value: Json,
    pub written_by: Writer,
}

impl Resolved {
    pub fn to_json(&self) -> Json {
        json!({
            "ref": {
                "store_epoch": self.store_epoch,
                "global_write_sequence": self.global_write_sequence,
                "key": self.key,
            },
            "value": self.value,
            "written_by": self.written_by.to_json(),
        })
    }
}

/// End outcomes, in the journal's fixed vocabulary.
pub mod outcome {
    use super::{Json, Resolved, json};

    fn tagged(tag: &str, payload: Json) -> Json {
        json!({"$tag": tag, "payload": payload})
    }
    fn nullary(tag: &str) -> Json {
        json!({"$tag": tag})
    }
    pub fn monitored(observation: &Resolved) -> Json {
        tagged("Monitored", json!({"observation": observation.to_json()}))
    }
    pub fn nominal() -> Json {
        tagged("Analysed", nullary("Nominal"))
    }
    pub fn anomaly(evidence: &Resolved) -> Json {
        tagged(
            "Analysed",
            tagged("Anomaly", json!({"evidence": evidence.to_json()})),
        )
    }
    pub fn generated(plan: &Resolved) -> Json {
        tagged(
            "Planned",
            tagged("Generated", json!({"plan": plan.to_json()})),
        )
    }
    pub fn no_plan() -> Json {
        tagged("Planned", nullary("NoPlan"))
    }
    pub fn legitimised(accepted: bool, plan: &Resolved, result: &Resolved) -> Json {
        tagged(
            "Legitimised",
            tagged(
                if accepted { "Accepted" } else { "Rejected" },
                json!({"plan": plan.to_json(), "legitimate_result": result.to_json()}),
            ),
        )
    }
    pub fn applied(plan: &Resolved, effect: &Resolved) -> Json {
        tagged(
            "Executed",
            tagged(
                "Applied",
                json!({"plan": plan.to_json(), "effect": effect.to_json()}),
            ),
        )
    }
    pub fn not_applied(plan: &Resolved, reason: Json) -> Json {
        tagged(
            "Executed",
            tagged(
                "NotApplied",
                json!({"plan": plan.to_json(), "reason": reason}),
            ),
        )
    }
    pub fn aborted(reason: Json) -> Json {
        tagged("Aborted", json!({"reason": reason}))
    }
    pub fn budget_exhausted() -> Json {
        nullary("BudgetExhausted")
    }
    pub fn failed(text: &str) -> Json {
        tagged("Failed", json!(text))
    }
}

/// A scheduled clock sample `{time, through, with_record?}`.
#[derive(Clone, Debug, PartialEq)]
pub struct ClockEntry {
    pub time_us: i64,
    pub through: i64,
    pub with_record: bool,
}

impl ClockEntry {
    pub fn to_json(&self) -> Json {
        let mut entry = json!({"time": seconds(self.time_us), "through": self.through});
        if self.with_record {
            entry["with_record"] = json!(true);
        }
        entry
    }
}

/// Seconds from microseconds.
pub fn seconds(us: i64) -> f64 {
    us as f64 / 1e6
}

#[derive(Clone, Debug)]
struct Open {
    activation: Json,
    emitted: i64,
}

/// Builds a consistent journal of one run, one record at a time.
#[derive(Clone, Debug)]
pub struct TraceBuilder {
    epoch: String,
    clock: String,
    after: i64,
    next: i64,
    now_us: i64,
    step_us: i64,
    loop_path: Vec<String>,
    open: HashMap<i64, Open>,
    latest: HashMap<String, Resolved>,
    records: Vec<Json>,
    samples: Vec<ClockEntry>,
}

impl TraceBuilder {
    /// A run of `epoch`, clock `python:time.time_ns`, starting at position 1
    /// and time 1000 s, advancing 1 ms per record unless told otherwise.
    pub fn new(epoch: &str) -> Self {
        Self {
            epoch: epoch.into(),
            clock: "python:time.time_ns".into(),
            after: 0,
            next: 1,
            now_us: 1_000_000_000,
            step_us: 1_000,
            loop_path: vec!["screw_detection".into()],
            open: HashMap::new(),
            latest: HashMap::new(),
            records: Vec::new(),
            samples: Vec::new(),
        }
    }
    /// Start after position `after` (a resumed or partial journal).
    pub fn after(mut self, after: i64) -> Self {
        self.after = after;
        self.next = after + 1;
        self
    }
    pub fn clock_label(mut self, clock: &str) -> Self {
        self.clock = clock.into();
        self
    }
    pub fn loop_path(mut self, path: &[&str]) -> Self {
        self.loop_path = path.iter().map(|s| s.to_string()).collect();
        self
    }
    pub fn epoch(&self) -> &str {
        &self.epoch
    }
    pub fn next_seq(&self) -> i64 {
        self.next
    }
    pub fn last_seq(&self) -> i64 {
        self.next - 1
    }
    pub fn now_us(&self) -> i64 {
        self.now_us
    }
    /// Set the next record's time; later records advance by the step.
    pub fn at_us(&mut self, us: i64) -> &mut Self {
        self.now_us = us;
        self
    }
    pub fn at_ms(&mut self, ms: i64) -> &mut Self {
        self.at_us(ms * 1_000)
    }
    pub fn advance_ms(&mut self, ms: i64) -> &mut Self {
        self.now_us += ms * 1_000;
        self
    }
    pub fn set_step_us(&mut self, step: i64) -> &mut Self {
        self.step_us = step;
        self
    }
    /// The latest version of `key` written so far.
    pub fn latest(&self, key: &str) -> Option<&Resolved> {
        self.latest.get(key)
    }
    /// A clock sample read at `time_us` covering records through `through`.
    pub fn clock(&mut self, time_us: i64, through: i64) -> &mut Self {
        self.samples.push(ClockEntry {
            time_us,
            through,
            with_record: false,
        });
        self
    }
    /// A clock sample now, covering every record so far.
    pub fn clock_now(&mut self) -> &mut Self {
        let (now, through) = (self.now_us, self.last_seq());
        self.clock(now, through)
    }

    fn push(&mut self, tag: &str, payload: Json) -> i64 {
        let seq = self.next;
        self.records.push(json!({
            "schema_version": SCHEMA_VERSION,
            "position": {"epoch": self.epoch, "seq": seq},
            "arrival": {"journal_time": seconds(self.now_us), "journal_clock": self.clock},
            "$tag": tag,
            "payload": payload,
        }));
        // A position past `i64::MAX` wraps: tests of overflow write it.
        self.next = self.next.wrapping_add(1);
        self.now_us += self.step_us;
        seq
    }

    fn phase(&mut self, id: &ActivationId, step: &str, payload: Json, evidence: Json) -> i64 {
        let activation = self.open[&id.start_seq].activation.clone();
        self.push(
            "Phase",
            json!({
                "activation": activation,
                "step": {"$tag": step, "payload": payload},
                "evidence": evidence,
            }),
        )
    }

    fn version(&mut self, key: &str, value: Json, written_by: Writer) -> Resolved {
        let resolved = Resolved {
            store_epoch: self.epoch.clone(),
            global_write_sequence: self.next,
            key: key.into(),
            value,
            written_by,
        };
        self.latest.insert(key.into(), resolved.clone());
        resolved
    }

    /// A write outside any activation, by node `node`.
    pub fn knowledge(&mut self, key: &str, value: Json, node: &str) -> Resolved {
        let resolved = self.version(key, value, Writer::Node(node.into()));
        self.push("Knowledge", json!({"version": resolved.to_json()}));
        resolved
    }

    /// A Start of `producer` in `role`, on `kind` (`event` or `message`)
    /// `topic`, receiving `message`, woken by `caused_by`.
    pub fn start(
        &mut self,
        producer: &str,
        role: Role,
        kind: &str,
        topic: &str,
        message: Json,
        caused_by: Option<&Emission>,
    ) -> ActivationId {
        let id = ActivationId::new(&self.epoch, self.next);
        let activation = json!({
            "epoch": self.epoch,
            "start_seq": id.start_seq,
            "producer": producer,
            "role": role.as_str(),
            "loop": self.loop_path,
            "caused_by": caused_by.map_or(Json::Null, Emission::to_json),
        });
        self.open.insert(
            id.start_seq,
            Open {
                activation,
                emitted: 0,
            },
        );
        self.phase(
            &id,
            "Start",
            json!({"kind": kind, "topic": topic, "message": message}),
            json!({}),
        );
        id
    }

    /// A Read of the latest version of `key`, or of nothing.
    pub fn read(&mut self, id: &ActivationId, key: &str) -> Option<Resolved> {
        let found = self.latest.get(key).cloned();
        self.read_resolved(id, key, found.clone());
        found
    }

    /// A Read that found exactly `resolved`.
    pub fn read_resolved(&mut self, id: &ActivationId, key: &str, resolved: Option<Resolved>) {
        self.phase(
            id,
            "Read",
            json!({"key": key, "value": resolved.as_ref().map_or(Json::Null, Resolved::to_json)}),
            json!({}),
        );
    }

    /// A Write of `value` to `key`, committed here by `id`.
    pub fn write(&mut self, id: &ActivationId, key: &str, value: Json) -> Resolved {
        let resolved = self.version(key, value, Writer::Activation(id.clone()));
        self.phase(
            id,
            "Write",
            json!({"key": key, "value": resolved.to_json()}),
            json!({}),
        );
        resolved
    }

    /// An Event publishing `message` on event `topic`, citing `evidence`.
    pub fn event(
        &mut self,
        id: &ActivationId,
        topic: &str,
        message: Json,
        evidence: &[(&str, &Resolved)],
    ) -> Emission {
        let open = self
            .open
            .get_mut(&id.start_seq)
            .expect("an open activation");
        open.emitted += 1;
        let sequence_no = open.emitted;
        let slots: serde_json::Map<String, Json> = evidence
            .iter()
            .map(|(name, resolved)| (name.to_string(), resolved.to_json()))
            .collect();
        self.phase(
            id,
            "Event",
            json!({"kind": "event", "topic": topic, "sequence_no": sequence_no, "message": message}),
            Json::Object(slots),
        );
        Emission {
            activation: id.clone(),
            sequence_no,
        }
    }

    pub fn service_started(&mut self, id: &ActivationId, service: &str, request_id: &str) -> i64 {
        self.phase(
            id,
            "ServiceStarted",
            json!({"service": service, "request_id": request_id}),
            json!({}),
        )
    }

    pub fn service_completed(
        &mut self,
        id: &ActivationId,
        service: &str,
        request_id: &str,
        delivered: bool,
    ) -> i64 {
        self.phase(
            id,
            "ServiceCompleted",
            json!({
                "service": service,
                "request_id": request_id,
                "outcome": if delivered { "Delivered" } else { "NotDelivered" },
            }),
            json!({}),
        )
    }

    /// The End of `id` with `outcome` (see [`outcome`]).
    pub fn end(&mut self, id: &ActivationId, outcome: Json) -> i64 {
        self.phase(id, "End", json!({"outcome": outcome}), json!({}))
    }

    pub fn finish(self) -> Trace {
        Trace {
            epoch: self.epoch,
            clock: self.clock,
            after: self.after,
            records: self.records,
            samples: self.samples,
        }
    }
}

/// A finished journal: its records in order and its scheduled clock samples.
#[derive(Clone, Debug)]
pub struct Trace {
    pub epoch: String,
    pub clock: String,
    pub after: i64,
    pub records: Vec<Json>,
    pub samples: Vec<ClockEntry>,
}

impl Trace {
    /// The last position.
    pub fn through(&self) -> i64 {
        self.after + self.records.len() as i64
    }
    /// Each record as one line of compact JSON.
    pub fn lines(&self) -> Vec<String> {
        self.records.iter().map(Json::to_string).collect()
    }
    /// JSON Lines, every record followed by LF.
    pub fn jsonl(&self) -> String {
        self.lines().into_iter().map(|line| line + "\n").collect()
    }
    /// The record at position `seq`.
    pub fn record(&self, seq: i64) -> &Json {
        &self.records[(seq - self.after - 1) as usize]
    }
    /// The clock samples as a schedule file.
    pub fn schedule_jsonl(&self) -> String {
        self.samples
            .iter()
            .map(|sample| sample.to_json().to_string() + "\n")
            .collect()
    }
}
