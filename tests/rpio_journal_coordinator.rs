//! The journal coordinator (`rpio::journal::state::track`) over the
//! ScrewDetection captures, through the checker CLI.
//!
//! A small root runs the coordinator with its generic resolved observation
//! view and prints its report each tick. Expected values come from
//! the captures themselves through an independent model written here
//! (callback nesting, pending publications and the distinct tuples each
//! callback acquired), from the scenario descriptions, and from
//! single-record mutations whose finding the coordinator names.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use serde_json::{Value as Json, json};
use tc_testutils::journal::app::{Journal, Run, Scratch, Trial, shape};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn trial(name: &str) -> Trial {
    Trial::load(&root().join("tests/fixtures/journal/app"), name)
}

const ROOT: &str = r#"use experimental::{modules, functions, generics, local_streams, tagged_unions, pattern_matching, newtypes}
type Knowledge = Any
type Message = Any
type Trigger = Any
use rpio::journal::view::timing::{Observation}
use rpio::journal::state::{self, JournalState, Started, Stepped, Finding}
use std::option::{self, Option}
newtype JournalTime = Unit

def start(s: Started<Knowledge, Message, Trigger, Int>) -> Int = 0
def step(s: Stepped<Knowledge, Message, Trigger, Int>) -> Int = s.context + 1
def consumed(topic: Str) -> Bool = topic == "observation_recorded" || topic == "anomaly_detected"
  || topic == "plan_rejected" || topic == "plan_generated" || topic == "plan_validated"

in journal: Observation<Knowledge, Message, Trigger, JournalTime>
aux j: JournalState<Knowledge, Message, Trigger, Int> = rpio::journal::state::track::<start, step, consumed>(journal)
out invalid: Bool = rpio::journal::state::invalid(j.report)
out first: Int = match(j.report.diagnostic) { Some(d) -> d.position.seq, None -> -1 }
out active: Int = j.report.active_activations
out pending: Int = j.report.pending_publications
out finding: Option<Finding> = j.finding
out unsolicited: Bool = j.unsolicited
out raw: Bool = std::option::is_some(j.record)
out acquired: Int = match(j.entry) { Some(e) -> List.len(e.acquired), None -> -1 }
"#;

/// Normalize the historical inline capture into the typed logical view.
/// These are coordinator fixtures, not classic wire packets: their deliberately
/// contradictory inline evidence must reach the coordinator unchanged. Native
/// decoding, history resolution and admission have separate integration tests.
fn logical_record(mut record: Json) -> Json {
    let position = record["position"].take();
    let arrival = record["arrival"].take();
    record.as_object_mut().unwrap().remove("schema_version");
    record.as_object_mut().unwrap().remove("position");
    record.as_object_mut().unwrap().remove("arrival");
    record["payload"]["position"] = position;
    record["payload"]["arrival"] = arrival;
    shape::tuples_mut(&mut record, &mut |tuple| {
        let epoch = tuple["ref"]["store_epoch"].clone();
        let writer = &mut tuple["written_by"];
        match writer["$tag"].as_str().unwrap() {
            "Activation" => {
                let activation = &mut writer["payload"];
                activation["run_id"] = activation["epoch"].take();
                activation["activation_id"] = json!(activation["start_seq"].take().to_string());
                activation.as_object_mut().unwrap().remove("epoch");
                activation.as_object_mut().unwrap().remove("start_seq");
            }
            "Node" => writer["payload"]["run_id"] = epoch,
            _ => {}
        }
    });
    if record["$tag"] == "Phase" {
        let payload = &mut record["payload"];
        let activation = &mut payload["activation"];
        activation["start_seq"] = json!(activation["start_seq"].to_string());
        activation["role"] = json!({"$tag": activation["role"]});
        let mut cause = activation["caused_by"].take();
        activation["caused_by"] = if cause.is_null() {
            json!({"$tag":"None"})
        } else {
            cause["activation"]["start_seq"] = json!(cause["activation"]["start_seq"].to_string());
            json!({"$tag":"Some", "payload":cause})
        };
        let evidence = payload["evidence"].take();
        payload["evidence"] = Json::Array(
            evidence
                .as_object()
                .unwrap()
                .iter()
                .map(|(name, value)| json!({"name":name,"value":value}))
                .collect(),
        );
        let step = &mut payload["step"];
        match step["$tag"].as_str().unwrap() {
            "Start" | "Event" => {
                step["payload"]["kind"] = json!({"$tag": match step["payload"]["kind"].as_str().unwrap() {
                    "event" => "Event", "message" => "Message", other => panic!("unknown kind {other}")
                }});
            }
            "Read" => {
                let value = step["payload"]["value"].take();
                step["payload"]["value"] = if value.is_null() {
                    json!({"$tag":"None"})
                } else {
                    json!({"$tag":"Some","payload":value})
                };
            }
            "ServiceCompleted" => {
                step["payload"]["outcome"] = json!({"$tag":step["payload"]["outcome"]});
            }
            _ => {}
        }
    }
    record
}

fn instant(time: Json) -> Json {
    json!({"$newtype":"std::time::Instant<JournalTime>","value":time})
}

fn record_tick(record: Json) -> Json {
    json!({"record":{"$tag":"Some","payload":{
        "time":instant(record["payload"]["arrival"]["journal_time"].clone()),"value":record}},
        "clock":{"$tag":"None"}})
}

fn clock_tick(time: f64, label: &str) -> Json {
    json!({"record":{"$tag":"None"},"clock":{"$tag":"Some","payload":{
        "clock":label,"time":instant(json!(time)),"origin":"Replay"}}})
}

/// The row map is fixture bookkeeping, deliberately independent of native
/// source provenance. It preserves the old model's position-based assertions.
fn input(trial: &Trial, journal: &Journal) -> (String, Vec<Json>) {
    let mut text = String::new();
    let mut rows = Vec::new();
    let mut next = trial.after() + 1;
    let mut emit = |tick: Json, position: Option<u64>| {
        text.push_str(&format!("{}: journal = {}\n", rows.len(), tick));
        rows.push(
            json!({"origins":position.map(|seq|vec![json!({"member":"journal",
            "cursor":format!("fixture:{}",seq-trial.after())})]).unwrap_or_default()}),
        );
    };
    for (time, through) in trial.clock_samples() {
        while next <= through {
            emit(
                record_tick(logical_record(journal.record(next))),
                Some(next),
            );
            next += 1;
        }
        emit(clock_tick(time, trial.clock_label()), None);
    }
    while next <= trial.through() {
        emit(
            record_tick(logical_record(journal.record(next))),
            Some(next),
        );
        next += 1;
    }
    (text, rows)
}

fn outputs(stdout: &[u8]) -> BTreeMap<String, BTreeMap<usize, String>> {
    let mut outputs = BTreeMap::<String, BTreeMap<usize, String>>::new();
    for line in String::from_utf8_lossy(stdout).lines() {
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
    outputs
}

/// Run the unchanged coordinator model against a typed logical observation.
fn coordinate(trial: &Trial, journal: &Journal, runtime: &str) -> Run {
    coordinate_with_spec(trial, journal, runtime, ROOT)
}

fn coordinate_with_spec(trial: &Trial, journal: &Journal, runtime: &str, source: &str) -> Run {
    let scratch = Scratch::new("coordinator-view");
    let spec = scratch.write("root.dsrv", source);
    let (text, provenance) = input(trial, journal);
    let file = scratch.write("observations.input", text);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
        .arg(&spec)
        .args(["--input-file", file.to_str().unwrap()])
        .args(["--output-stdout", "--runtime", runtime])
        .env("RUST_LOG", "error")
        .current_dir(root())
        .output()
        .unwrap();
    let run = Run {
        success: output.status.success(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        outputs: outputs(&output.stdout),
        provenance,
        after: trial.after(),
    };
    assert!(run.success, "{} on {runtime}: {}", trial.name, run.stderr);
    assert_eq!(
        run.rows("raw"),
        run.provenance.len(),
        "every fixture tick evaluated"
    );
    run
}

fn finding(run: &Run, seq: u64) -> String {
    run.at("finding", run.row_of(seq)).to_owned()
}

/// The topics a ScrewDetection callback is registered for.
const CONSUMED: [&str; 5] = [
    "observation_recorded",
    "anomaly_detected",
    "plan_rejected",
    "plan_generated",
    "plan_validated",
];

/// What the coordinator must hold after each position, computed from the
/// capture alone: open callbacks (Start to End), pending publications (an
/// Event on a consumed topic until the first Start it wakes), and,
/// on each record of a callback, the distinct tuples that callback read
/// (found) or wrote so far.
#[derive(Debug, Default)]
struct Model {
    active: BTreeMap<u64, usize>,
    pending: BTreeMap<u64, usize>,
    acquired: BTreeMap<u64, usize>,
}

fn model(trial: &Trial, journal: &Journal) -> Model {
    let mut model = Model::default();
    let mut open = BTreeSet::new();
    let mut pending = BTreeSet::new();
    let mut held = BTreeMap::<i64, BTreeSet<String>>::new();
    for seq in trial.after() + 1..=trial.through() {
        let record = journal.record(seq);
        let owner = shape::start_seq(&record);
        match shape::step(&record) {
            Some("Start") => {
                open.insert(owner.unwrap());
                if let Some(cause) = shape::cause(&record) {
                    pending.remove(&cause);
                }
            }
            Some("Event") if CONSUMED.contains(&shape::event_name(&record).unwrap()) => {
                pending.insert(shape::emission(&record).unwrap());
            }
            Some("Read" | "Write") => {
                let tuple = &record["payload"]["step"]["payload"]["value"];
                if !tuple.is_null() {
                    held.entry(owner.unwrap())
                        .or_default()
                        .insert(tuple.to_string());
                }
            }
            _ => {}
        }
        if let Some(owner) = owner {
            model
                .acquired
                .insert(seq, held.get(&owner).map_or(0, BTreeSet::len));
        }
        if shape::step(&record) == Some("End") {
            open.remove(&owner.unwrap());
        }
        model.active.insert(seq, open.len());
        model.pending.insert(seq, pending.len());
    }
    model
}

const CLEAN: [&str; 8] = [
    "nominal",
    "nominal@redis",
    "adapt",
    "retries-then-accept",
    "budget-abort",
    "no-plan",
    "unsolicited-execute",
    "acquisitions-at-limit",
];

/// The nested trials state nothing contradictory about one another: no
/// finding on any tick, and the open callbacks, pending publications and
/// acquired tuples are the model's on every record. Callbacks return to
/// zero, and publications on topics nobody consumes are never kept.
#[test]
fn actual_trials_raise_no_journal_finding_and_match_the_model() {
    for name in CLEAN {
        let trial = trial(name);
        let journal = trial.journal();
        let model = model(&trial, &journal);
        let run = coordinate(&trial, &journal, "dataflow");
        assert!(run.true_rows("invalid").is_empty(), "{name}");
        for seq in trial.after() + 1..=trial.through() {
            let row = run.row_of(seq);
            assert_eq!(run.at("finding", row), "None", "{name} {seq}");
            assert_eq!(
                run.at("active", row),
                model.active[&seq].to_string(),
                "{name} {seq}"
            );
            assert_eq!(
                run.at("pending", row),
                model.pending[&seq].to_string(),
                "{name} {seq}"
            );
            if let Some(acquired) = model.acquired.get(&seq) {
                assert_eq!(
                    run.at("acquired", row),
                    acquired.to_string(),
                    "{name} {seq}"
                );
            }
        }
        assert_eq!(model.active[&trial.through()], 0, "{name}");
        // Clock-only ticks retire nothing.
        assert_eq!(
            run.at("pending", run.provenance.len() - 1),
            model.pending[&trial.through()].to_string(),
            "{name}"
        );
        if ["nominal", "adapt"].contains(&name) {
            let semi = coordinate(&trial, &journal, "semi-sync");
            assert_eq!(semi.trace(), run.trace(), "{name}: the runtimes agree");
        }
    }
}

/// The duplicate delivery in `not-applied` cites an acceptance its first
/// Start already consumed: the second Execute's Start is a finding there,
/// the journal is invalid from it on, and every record stays visible.
#[test]
fn a_consumed_publication_cited_again_is_a_finding() {
    let trial = trial("not-applied");
    let executes = trial.positions(|r| shape::is_start_of(r, "Execute"));
    assert_eq!(executes.len(), 2);
    let (first, second) = (executes[0], executes[1]);
    assert_eq!(
        shape::cause(&trial.record(first)),
        shape::cause(&trial.record(second)),
        "the scenario delivers one acceptance twice"
    );
    let run = coordinate(&trial, &trial.journal(), "dataflow");
    assert_eq!(finding(&run, second), "Some(CauseNotPending)");
    assert_eq!(run.true_positions("invalid").first(), Some(&second));
    assert!(run.flag("invalid", run.provenance.len() - 1));
    assert_eq!(
        run.at("first", run.provenance.len() - 1),
        second.to_string()
    );
    let model = model(&trial, &trial.journal());
    for seq in second..=trial.through() {
        let row = run.row_of(seq);
        assert!(run.flag("raw", row));
        assert_eq!(run.at("active", row), model.active[&seq].to_string());
    }
}

/// A Start woken by a publication arrives on that publication's topic; on
/// another topic it is a finding. A Start of an analysing or adapting role
/// that nothing woke is a fact, not a finding.
#[test]
fn a_consumer_on_another_topic_is_a_finding_and_an_external_start_a_fact() {
    let trial = trial("nominal");
    let analyses = trial.positions(|r| shape::is_start_of(r, "Analysis"));
    let journal = trial.journal().edit(analyses[1], |record| {
        record["payload"]["step"]["payload"]["topic"] = json!("elsewhere");
    });
    let run = coordinate(&trial, &journal, "dataflow");
    assert_eq!(finding(&run, analyses[0]), "None");
    assert_eq!(finding(&run, analyses[1]), "Some(TopicMismatch)");
    assert_eq!(run.true_positions("invalid").first(), Some(&analyses[1]));

    let trial = self::trial("unsolicited-execute");
    let execute = trial.positions(|r| shape::is_start_of(r, "Execute"))[0];
    assert_eq!(shape::cause(&trial.record(execute)), None);
    let run = coordinate(&trial, &trial.journal(), "dataflow");
    assert_eq!(run.true_positions("unsolicited"), [execute]);
    assert!(run.true_rows("invalid").is_empty());
}

/// Evidence an Event or End cites is compared with what its callback
/// acquired, tuple for tuple. Each case is in a different callback of one
/// journal, so each finding appears on its own record.
#[test]
fn cited_evidence_is_compared_exactly_with_acquired_evidence() {
    let trial = trial("nominal");
    let analyses = trial.positions(|r| shape::is_start_of(r, "Analysis"));
    let in_analysis = |k: usize, select: &dyn Fn(&Json) -> bool| {
        trial
            .positions(|r| shape::start_seq(r) == Some(analyses[k] as i64) && select(r))
            .remove(0)
    };
    let detection =
        |k: usize| in_analysis(k, &|r| shape::event_name(r) == Some("detection_completed"));
    let frame_read = |k: usize| {
        in_analysis(k, &|r| {
            shape::step(r) == Some("Read") && r["payload"]["step"]["payload"]["key"] == "FrameRef"
        })
    };
    let brightness = |tuple: &mut Json| tuple["value"]["brightness"] = json!(3.5);
    let monitor_ends = trial.positions(|r| shape::is_end_of(r, "Monitor"));
    let journal = trial
        .journal()
        // Same version, another value.
        .edit(detection(0), |r| {
            brightness(&mut r["payload"]["evidence"]["frame"])
        })
        // A version the callback never acquired.
        .edit(detection(1), |r| {
            r["payload"]["evidence"]["frame"]["ref"]["global_write_sequence"] = json!(5)
        })
        // Same version and value, another writer.
        .edit(detection(2), |r| {
            r["payload"]["evidence"]["frame"]["written_by"] =
                json!({"$tag": "Node", "payload": {"name": "Monitor"}})
        })
        // The Read and the Event agree with each other, not with the Write.
        .edit(frame_read(3), |r| {
            brightness(&mut r["payload"]["step"]["payload"]["value"])
        })
        .edit(detection(3), |r| {
            brightness(&mut r["payload"]["evidence"]["frame"])
        })
        // An End cites a value its callback did not write.
        .edit(monitor_ends[5], |r| {
            brightness(&mut r["payload"]["step"]["payload"]["outcome"]["payload"]["observation"])
        });
    let run = coordinate(&trial, &journal, "dataflow");
    assert_eq!(finding(&run, detection(0)), "Some(ConflictingEvidence)");
    assert_eq!(finding(&run, detection(1)), "Some(NotAcquired)");
    assert_eq!(finding(&run, detection(2)), "Some(ConflictingEvidence)");
    assert_eq!(finding(&run, frame_read(3)), "None");
    assert_eq!(finding(&run, detection(3)), "None");
    assert_eq!(finding(&run, monitor_ends[5]), "Some(ConflictingEvidence)");
    assert_eq!(run.true_positions("invalid").first(), Some(&detection(0)));
}

/// Lifecycle and emission findings are raised on the record that shows
/// them: a record of an activation that never started, a record restating
/// its activation otherwise, an outcome its role cannot end with, and an
/// emission numbered out of order.
#[test]
fn lifecycle_and_emission_findings_are_raised_on_the_record_that_shows_them() {
    let trial = trial("nominal");
    let analyses = trial.positions(|r| shape::is_start_of(r, "Analysis"));
    let of = |k: usize, select: &dyn Fn(&Json) -> bool| {
        trial
            .positions(|r| shape::start_seq(r) == Some(analyses[k] as i64) && select(r))
            .remove(0)
    };
    let read = |k: usize| of(k, &|r| shape::step(r) == Some("Read"));
    let end = |k: usize| of(k, &|r| shape::step(r) == Some("End"));
    let cycle = of(3, &|r| shape::event_name(r) == Some("cycle_completed"));
    let journal = trial
        .journal()
        .edit(read(0), |r| {
            r["payload"]["activation"]["start_seq"] = json!(999)
        })
        .edit(read(1), |r| {
            r["payload"]["activation"]["producer"] = json!("Other")
        })
        .edit(end(2), |r| {
            r["payload"]["step"]["payload"]["outcome"] =
                json!({"$tag": "Planned", "payload": {"$tag": "NoPlan"}})
        })
        .edit(cycle, |r| {
            r["payload"]["step"]["payload"]["sequence_no"] = json!(7)
        });
    let run = coordinate(&trial, &journal, "dataflow");
    assert_eq!(finding(&run, read(0)), "Some(UnknownActivation)");
    assert_eq!(finding(&run, read(1)), "Some(ActivationChanged)");
    assert_eq!(finding(&run, end(2)), "Some(OutcomeNotOfRole)");
    assert_eq!(finding(&run, cycle), "Some(EmissionOutOfOrder)");
    assert_eq!(
        run.at("first", run.provenance.len() - 1),
        read(0).to_string()
    );
}

/// Only a publication on a topic the application says is consumed is kept:
/// an observation stays pending until the Analysis it wakes starts, and a
/// cycle completion is never kept, so a Start that claims to be woken by one
/// names no pending publication.
#[test]
fn only_publications_on_consumed_topics_are_kept_until_their_consumer() {
    let trial = trial("nominal");
    let observation = trial.positions(|r| shape::event_name(r) == Some("observation_recorded"))[0];
    let analysis = trial.positions(|r| shape::is_start_of(r, "Analysis"))[0];
    let cycle = trial.positions(|r| shape::event_name(r) == Some("cycle_completed"))[0];
    let run = coordinate(&trial, &trial.journal(), "dataflow");
    let pending = |seq: u64| run.at("pending", run.row_of(seq)).parse::<i64>().unwrap();
    assert_eq!(pending(observation) - pending(observation - 1), 1, "kept");
    assert_eq!(pending(analysis) - pending(analysis - 1), -1, "consumed");
    assert_eq!(pending(cycle), pending(cycle - 1), "never kept");

    // The second Analysis claims to be woken by the first cycle completion.
    let (owner, ordinal) = shape::emission(&trial.record(cycle)).unwrap();
    let second = trial.positions(|r| shape::is_start_of(r, "Analysis"))[1];
    let journal = trial
        .positions(|r| shape::start_seq(r) == Some(second as i64))
        .into_iter()
        .fold(trial.journal(), |journal, seq| {
            journal.edit(seq, |record| {
                record["payload"]["activation"]["caused_by"] = json!({
                    "activation": {"epoch": trial.epoch(), "start_seq": owner},
                    "sequence_no": ordinal,
                });
            })
        });
    let run = coordinate(&trial, &journal, "dataflow");
    assert_eq!(finding(&run, second), "Some(CauseNotPending)");
}

/// Regression: the semi-sync runtime once never finished a tick of the
/// coordinator, whose diagnostic read its own previous value inside a
/// match's scrutinee. Every tick is evaluated, in time.
#[test]
fn the_coordinator_runs_to_completion_on_semi_sync() {
    let scratch = Scratch::new("semi-sync");
    let spec = scratch.write("root.dsrv", ROOT);
    let capture =
        std::fs::read_to_string(root().join("tests/fixtures/journal/lab/outside-writes.jsonl"))
            .unwrap();
    let input = capture
        .lines()
        .enumerate()
        .map(|(row, line)| {
            format!(
                "{row}: journal = {}\n",
                record_tick(logical_record(serde_json::from_str(line).unwrap()))
            )
        })
        .collect::<String>();
    let file = scratch.write("observations.input", input);
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
        .arg(&spec)
        .args(["--input-file", file.to_str().unwrap()])
        .args(["--output-stdout", "--runtime", "semi-sync"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let started = std::time::Instant::now();
    while child.try_wait().unwrap().is_none() {
        if started.elapsed() > std::time::Duration::from_secs(60) {
            child.kill().unwrap();
            panic!("the coordinator did not finish on semi-sync within 60 s");
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        stdout.lines().filter(|l| l.starts_with("invalid[")).count(),
        30
    );
}

/// Typed adapter unit exercised through the CLI generic input-file boundary.
/// This does not test native wire decoding or source admission; their focused
/// suites do that. Exercise the generic bridge separately from historical inline
/// fixtures. Tokens, Knowledge write sequence and admission position are three
/// independent identities. No application-specific adapter or wire stub is used.
#[test]
fn typed_classic_adapter_preserves_identity_history_and_incomplete_evidence() {
    let scratch = Scratch::new("native-coordinator-view");
    let native_root = ROOT.replace(
        "in journal: Observation<Knowledge, Message, Trigger, JournalTime>",
        r#"use rpio::journal::{self}
use rpio::journal::timing
use rpio::journal::view::{Record, Received, Slot}
use rpio::journal::view::adapt

def receive(trigger: Str) -> Received<Any> = {kind: Event, topic: trigger, message: trigger}
def message(payload: Int) -> Any = payload
def cite(values: List<journal::Resolved<Knowledge>>) -> List<Slot<Knowledge>> =
  List.map(\value -> {name: value.ref.key, value}, values)

in native: timing::Observation<Knowledge, Int, Str, JournalTime>
in position: Int
aux journal: Observation<Knowledge, Message, Trigger, JournalTime> =
  rpio::journal::view::adapt::observation(
    native, {epoch: "admission-only", seq: position}, "redis/server-7", \trigger -> receive(trigger), \payload -> message(payload), \values -> cite(values))
out token: Str = match(j.record) {
  Some(Phase({activation, ..})) -> activation.start_seq,
  Some(Incomplete({activation, ..})) -> activation.start_seq,
  _ -> "",
}
out label: Str = match(j.record) {
  Some(r) -> rpio::journal::view::arrival(r).journal_clock,
  _ -> "",
}
out incomplete_native: Any = match(j.record) {
  Some(Incomplete({native, ..})) -> native, _ -> "",
}
out first_token: Str = match(j.report.diagnostic) {
  Some({activation: Some(id), ..}) -> id.start_seq, _ -> "",
}
out latest_sequence: Int = if List.len(j.latest) == 0 then -1
  else List.head(j.latest).value.ref.global_write_sequence
out latest_context: Int = if List.len(j.latest) == 0 then -1
  else std::option::unwrap_or(List.head(j.latest).context, -1)
"#,
    );
    let spec = scratch.write("root.dsrv", native_root);
    let a = "58b0d153-7a18-44a1-a220-4cc23e54c422";
    let b = "95b28a94-2373-4a89-b05c-e21fd7b277fd";
    let activation = |token: &str, role: &str, cause: Json| {
        json!({
        "run_id":"callback-run", "activation_id":token,"role":{"$tag":role},
        "producer":role,"loop":["generic"],"caused_by":cause})
    };
    let owner_a = activation(a, "Monitor", json!({"$tag":"None"}));
    let owner_b = activation(
        b,
        "Analysis",
        json!({"$tag":"Some","payload":{
        "activation_id":a,"ordinal":1}}),
    );
    let initial = json!({"ref":{"store_epoch":"knowledge-store","global_write_sequence":7000,"key":"counter"},
        "value":8,"written_by":{"$tag":"External","payload":{"name":"sensor"}}});
    let commit = json!({"ref":{"store_epoch":"knowledge-store","global_write_sequence":7001,"key":"counter"},
        "value":9,"written_by":{"$tag":"Activation","payload":{"run_id":"callback-run","activation_id":a}}});
    let nothing = json!({"$tag":"Nothing"});
    let one = |value: &Json| json!({"$tag":"One","payload":value});
    let phase = |owner: &Json, step: Json, knowledge: Json| {
        json!({"$tag":"Phase","payload":{
        "activation":owner,"step":step,"knowledge":knowledge,"arrival":{"journal_time":10.0}}})
    };
    let knowledge = |value: &Json| {
        json!({"$tag":"Knowledge","payload":{
        "version":value,"arrival":{"journal_time":10.0}}})
    };
    let records = [
        knowledge(&initial),
        phase(
            &owner_a,
            json!({"$tag":"Start","payload":{"trigger":"sensor"}}),
            nothing.clone(),
        ),
        phase(
            &owner_a,
            json!({"$tag":"Read","payload":{"key":"counter"}}),
            one(&initial),
        ),
        knowledge(&commit),
        phase(
            &owner_a,
            json!({"$tag":"Write","payload":{"key":"counter"}}),
            one(&commit),
        ),
        phase(
            &owner_a,
            json!({"$tag":"Event","payload":{"name":"observation_recorded","payload":42,
            "emission":{"activation_id":a,"ordinal":1}}}),
            one(&commit),
        ),
        phase(
            &owner_a,
            json!({"$tag":"End","payload":{"outcome":{"$tag":"Aborted","payload":{
            "reason":{"$tag":"BudgetExhausted"}}}}}),
            nothing.clone(),
        ),
        phase(
            &owner_b,
            json!({"$tag":"Start","payload":{"trigger":"observation_recorded"}}),
            nothing.clone(),
        ),
        json!({"$tag":"Incomplete","payload":{"activation":owner_b,"arrival":{"journal_time":10.0}}}),
        phase(
            &owner_b,
            json!({"$tag":"Read","payload":{"key":"counter"}}),
            json!({"$tag":"Several","payload":[initial,commit]}),
        ),
        phase(
            &owner_b,
            json!({"$tag":"End","payload":{"outcome":{"$tag":"Analysed","payload":{"$tag":"Nominal"}}}}),
            nothing,
        ),
    ];
    let mut input = String::new();
    for (row, record) in records.iter().enumerate() {
        let observation = json!({"record":{"$tag":"Some","payload":{"time":instant(json!(10.0)),"value":record}},
            "clock":{"$tag":"None"}});
        input.push_str(&format!(
            "{row}: native = {observation}\n{row}: position = {}\n",
            row + 1
        ));
    }
    input.push_str(&format!(
        "11: native = {}\n11: position = 12\n",
        clock_tick(20.0, "redis/server-7")
    ));
    let file = scratch.write("observations.input", input);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
        .arg(&spec)
        .args(["--input-file", file.to_str().unwrap(), "--output-stdout"])
        .env("RUST_LOG", "error")
        .current_dir(root())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows = outputs(&output.stdout);
    let at = |name: &str, row: usize| rows.get(name).unwrap().get(&row).unwrap().as_str();
    assert_eq!(at("token", 1), format!("\"{a}\""));
    assert_eq!(at("token", 7), format!("\"{b}\""));
    assert_eq!(at("label", 0), "\"redis/server-7\"");
    assert_eq!(at("label", 8), "\"redis/server-7\"");
    assert_eq!(
        at("latest_sequence", 3),
        "7000",
        "history notice is visible but does not invent a contextual Phase Write"
    );
    assert_eq!(at("latest_sequence", 4), "7001");
    assert_eq!(
        at("latest_context", 4),
        "2",
        "Write retains the owning callback context after Read and Write"
    );
    assert_eq!(at("acquired", 4), "2");
    assert_eq!(at("pending", 5), "1");
    assert_eq!(
        at("pending", 7),
        "0",
        "the exact UUID publication wakes its consumer"
    );
    for row in 0..records.len() {
        assert_eq!(at("raw", row), "true");
    }
    for row in 0..8 {
        assert_eq!(at("finding", row), "None");
    }
    assert_eq!(at("finding", 8), "Some(IncompleteEvidence)");
    assert_eq!(
        at("finding", 9),
        "Some(IncompleteEvidence)",
        "multiple Read values are not silently discarded"
    );
    assert_eq!(
        at("first", 11),
        "9",
        "diagnostics use admission position, never write sequence or UUID"
    );
    assert_eq!(at("invalid", 11), "true");
    assert_eq!(at("first_token", 11), format!("\"{b}\""));
    assert!(at("incomplete_native", 9).contains("7000"));
    assert!(at("incomplete_native", 9).contains("7001"));
    assert!(at("incomplete_native", 9).contains(b));
    assert_eq!(
        at("raw", 11),
        "false",
        "clock-only ticks replay no raw record"
    );
    assert_eq!(at("active", 10), "0");
}

/// The shared adapter also composes with the actual classic file source:
/// schema decoding, immutable history resolution and strict declared-type
/// admission happen before the coordinator receives a record.
#[test]
fn classic_source_admits_then_coordinates_the_resolved_observation() {
    use sha2::{Digest, Sha256};
    let scratch = Scratch::new("source-coordinator-view");
    let source_root = ROOT.replace("type Knowledge = Any", "type Knowledge = Union<Counter: Int>").replace(
        "in journal: Observation<Knowledge, Message, Trigger, JournalTime>",
        r#"use rpio::journal::{self}
use rpio::journal::timing
use rpio::journal::view::{Received, Slot}
use rpio::journal::view::adapt

type NativeTrigger = Union<SensorDataReceived, ObservationRecorded>
type NativeMessage = Struct<detail: Union<ObservationRecorded>, ...>
def receive(trigger: NativeTrigger) -> Received<Any> = {
  kind: Event, topic: if matches(trigger, ObservationRecorded) then "observation_recorded" else "sensor",
  message: trigger,
}
def message(payload: NativeMessage) -> Any = payload
def cite(values: List<journal::Resolved<Knowledge>>) -> List<Slot<Knowledge>> =
  List.map(\value -> {name: value.ref.key, value}, values)
in native: timing::Observation<Knowledge, NativeMessage, NativeTrigger, JournalTime>
aux position: Int = default(position[1], 0) + 1
aux journal: Observation<Knowledge, Message, Trigger, JournalTime> =
  rpio::journal::view::adapt::observation(
    native, {epoch: "local-admission", seq: position}, "fixture.classic", \trigger -> receive(trigger), \payload -> message(payload), \values -> cite(values))
out committed_value: Int = if List.len(j.latest) == 0 then -1
  else match(List.head(j.latest).value.value) { Counter(n) -> n }
out committed_position: Int = if List.len(j.latest) == 0 then -1
  else List.head(j.latest).position.seq
"#,
    );
    let spec = scratch.write("root.dsrv", source_root);
    let a = "58b0d153-7a18-44a1-a220-4cc23e54c422";
    let b = "95b28a94-2373-4a89-b05c-e21fd7b277fd";
    let activation = |token: &str, role: &str, cause: Json| {
        json!({"run_id":"callback-run",
        "activation_id":token,"role":role,"producer":role,"loop":["generic"],"caused_by":cause})
    };
    let owner_a = activation(a, "Monitor", json!({"$tag":"None"}));
    let owner_b = activation(
        b,
        "Analysis",
        json!({"$tag":"Some","payload":{"activation_id":a,"ordinal":1}}),
    );
    let initial_ref =
        json!({"store_epoch":"knowledge-store","global_write_sequence":7000,"key":"Counter"});
    let commit_ref =
        json!({"store_epoch":"knowledge-store","global_write_sequence":7001,"key":"Counter"});
    let nothing = json!({"$tag":"Nothing"});
    let one = |reference: &Json| json!({"$tag":"One","payload":reference});
    let phase = |owner: &Json, step: Json, knowledge: Json| {
        json!({"schema_version":2,"$tag":"Phase","payload":{
        "activation":owner,"step":step,"knowledge":knowledge,"arrival":{"journal_time":10.0}}})
    };
    let mut records = vec![
        json!({"schema_version":2,"$tag":"Knowledge","payload":{"version":{"ref":initial_ref,
            "value":{"$tag":"Counter","payload":8},"written_by":{"$tag":"External","payload":{"name":"sensor"}}},
            "arrival":{"journal_time":10.0}}}),
        phase(
            &owner_a,
            json!({"$tag":"Start","payload":{"trigger":{"$tag":"SensorDataReceived"}}}),
            nothing.clone(),
        ),
        phase(&owner_a, json!({"$tag":"Read"}), one(&initial_ref)),
        json!({"schema_version":2,"$tag":"Knowledge","payload":{"version":{"ref":commit_ref,
            "value":{"$tag":"Counter","payload":9},"written_by":{"$tag":"Activation","payload":{
                "run_id":"callback-run","activation_id":a}}},"arrival":{"journal_time":10.0}}}),
        phase(&owner_a, json!({"$tag":"Write"}), one(&commit_ref)),
        phase(
            &owner_a,
            json!({"$tag":"Event","payload":{"name":"observation_recorded",
            "payload":{"detail":{"$tag":"ObservationRecorded"}},"emission":{"activation_id":a,"ordinal":1}}}),
            one(&commit_ref),
        ),
        phase(
            &owner_a,
            json!({"$tag":"End","payload":{"outcome":{"$tag":"Monitored","payload":{"observation":commit_ref}}}}),
            one(&commit_ref),
        ),
        phase(
            &owner_b,
            json!({"$tag":"Start","payload":{"trigger":{"$tag":"ObservationRecorded"}}}),
            nothing.clone(),
        ),
        phase(
            &owner_b,
            json!({"$tag":"End","payload":{"outcome":{"$tag":"Analysed","payload":{"$tag":"Nominal"}}}}),
            nothing,
        ),
    ];
    // An old identical definition received after a newer commit is still a
    // raw receipt, but cannot regress the version or restart availability.
    records.push(records[0].clone());
    records.insert(1, records[0].clone());
    let run = |records: &[Json], case: &str| {
        let text = records
            .iter()
            .map(|record| format!("{record}\n"))
            .collect::<String>();
        let file = scratch.write(&format!("{case}.jsonl"), text.as_bytes());
        let config = json!({"sources":{"native":{
            "kind":"rpio-observation-clock-file","variable":"native","journal_clock":"fixture.classic",
            "files":[file],"capture_checks":[{"records":records.len(),"journal_clock":"fixture.classic",
                "sha256":format!("{:x}",Sha256::digest(text.as_bytes()))}]}}});
        let config = scratch.write(
            &format!("{case}.json"),
            serde_json::to_vec(&config).unwrap(),
        );
        std::process::Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
            .arg(&spec)
            .args([
                "--input-config",
                config.to_str().unwrap(),
                "--output-stdout",
            ])
            .env("RUST_LOG", "error")
            .current_dir(root())
            .output()
            .unwrap()
    };
    let output = run(&records, "valid");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows = outputs(&output.stdout);
    assert_eq!(rows["raw"].len(), records.len());
    assert!(rows["invalid"].values().all(|value| value == "false"));
    assert_eq!(
        rows["committed_position"][&1], "1",
        "idempotent receipt keeps the original commit position"
    );
    assert_eq!(rows["acquired"][&5], "2");
    assert_eq!(rows["pending"][&6], "1");
    assert_eq!(rows["pending"][&8], "0");
    assert_eq!(rows["active"][&9], "0");
    assert_eq!(rows["committed_value"][&9], "9");
    assert_eq!(rows["raw"][&10], "true");
    assert_eq!(rows["committed_value"][&10], "9");
    assert_eq!(rows["committed_position"][&10], "6");
    let mut invalid = records;
    invalid[0]["payload"]["version"]["value"]["payload"] = json!("wrong integer type");
    let output = run(&invalid, "invalid");
    assert!(
        !output.status.success(),
        "native source must reject a well-shaped wire value of the wrong declared type"
    );
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        error.contains("Counter") || error.contains("conform") || error.contains("Int"),
        "{error}"
    );
}

/// Shared publication helpers distinguish an actual publication from its
/// callback's successful End. Preserve the later cycle/adaptation API without
/// introducing any application keys into the shared modules.
#[test]
fn shared_publication_helpers_keep_end_outcomes_separate_from_publications() {
    let source = format!(
        "{ROOT}{}",
        r#"
use rpio::journal::view::{Received, Posted, Outcome}
use rpio::journal::view::maplek::{self}
use rpio::journal::view::maplek::events::{Milestone}

def request(r: Received<Trigger>) -> Bool = r.topic == "sensor_data_received"
def classify(p: Posted<Message>) -> Option<Milestone> = match(p.topic) {
  "observation_recorded" -> Some(Milestone::Observation),
  "detection_completed" -> Some(Milestone::Analysis),
  "anomaly_detected" -> Some(Milestone::Anomaly),
  "plan_generated" -> Some(Milestone::Proposal),
  "plan_rejected" -> Some(Milestone::Rejection),
  "plan_validated" -> Some(Milestone::Acceptance),
  "plan_executed" -> Some(Milestone::Execution),
  "adaptation_aborted" -> Some(Milestone::Abort),
  _ -> None,
}
def required(o: Outcome<Knowledge>) -> Option<Milestone> = match(o) {
  Monitored(_) -> Some(Milestone::Observation), _ -> None,
}
def closes(o: Outcome<Knowledge>) -> Bool = matches(o, Planned(NoPlan) | Executed(NotApplied(_)) | Aborted(_))
aux publications = rpio::journal::view::maplek::observe::<request,classify,required,closes>(
  journal,j,{acceptance_plan:"plan",execution_plan:"plan",execution_effect:"effect"})
aux cycle = rpio::journal::view::maplek::loop(journal)
out observed: Bool = publications.publications.sensing.observed
out analysed: Bool = publications.publications.sensing.analysed
out missing: Bool = std::option::is_some(publications.publications.sensing.missing_publication)
out callback_ended: Bool = std::option::is_some(publications.callbacks.ended)
out protocol: Bool = cycle.protocol_followed
"#
    );
    let trial = trial("nominal");
    let run = coordinate_with_spec(&trial, &trial.journal(), "dataflow", &source);
    let publications =
        trial.positions(|record| shape::event_name(record) == Some("observation_recorded"));
    assert_eq!(run.true_positions("observed"), publications);
    let analyses =
        trial.positions(|record| shape::event_name(record) == Some("detection_completed"));
    assert_eq!(run.true_positions("analysed"), analyses);
    assert!(run.true_rows("missing").is_empty());
    assert_eq!(
        run.true_positions("callback_ended"),
        trial.positions(|record| shape::step(record) == Some("End"))
    );
    // The outcome-cycle helper deliberately describes one sequential loop.
    // These captures nest Analysis inside Monitor: nominal Analysis closes
    // the cycle before its caller Monitor returns. Preserve the precise
    // out-of-protocol facts rather than pretending this helper owns nesting.
    let outside: Vec<_> = run.outputs["protocol"]
        .iter()
        .filter(|(_, value)| value.as_str() == "false")
        .map(|(row, _)| run.position(*row).unwrap())
        .collect();
    assert_eq!(
        outside,
        trial.positions(|record| shape::is_end_of(record, "Monitor"))
    );
    let missing = publications[0];
    let journal = trial.journal().edit(missing, |record| {
        record["payload"]["step"]["payload"]["topic"] = json!("unselected_completion");
    });
    let ended = trial.positions(|record| shape::is_end_of(record, "Monitor"))[0];
    let run = coordinate_with_spec(&trial, &journal, "dataflow", &source);
    assert_eq!(run.true_positions("missing"), [ended]);
    assert!(run.flag("callback_ended", run.row_of(ended)));
    assert!(
        !run.flag("observed", run.row_of(ended)),
        "a successful End supplies no missing publication"
    );
}
