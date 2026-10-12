use crate::dsrv_fixtures::WithoutWarnings;
use crate::lang::dsrv::{TypeCheckOptions, parser::parse_str};
use crate::{Value, dataflow::DataflowMonitor};

#[test]
fn std_time_helpers_and_std_timing_local_streams_check_strictly() {
    let source = r#"
use experimental::{modules, functions, generics, local_streams, tagged_unions, pattern_matching, newtypes, constants}
use std::option
use std::option::{Option, Some, None}
use std::time::{self, Instant, Duration, TimeInterval}
use std::timing
newtype JournalTime = Unit
const limit: Duration<JournalTime> = std::time::seconds(2.0)
in active: Bool
in seconds: Float
aux now: Instant<JournalTime> = std::time::instant(seconds)
aux start: Instant<JournalTime> = std::time::instant(1.0)
aux current: Option<Instant<JournalTime>> = Some(now)
aux event: Option<Instant<JournalTime>> = None
aux tracked = std::timing::while(active, current, event)
out elapsed: Duration<JournalTime> = std::time::duration(std::time::between(start, now))
out waited: Duration<JournalTime> = std::time::elapsed(start, now)
out late: Bool = std::time::exceeds(tracked.span, limit)
out short: Bool = std::time::shorter_than(std::option::some(std::time::between(start, now)), limit)
out backwards: Bool = std::time::is_backwards(std::time::between(now, start))
out since: Option<Instant<JournalTime>> = tracked.since
out span: Option<TimeInterval<JournalTime>> = std::time::since(std::option::some(start), current)
aux key: Option<Int> = if active then Some(1) else None
aux bounded = std::timing::bounded_response(key, key, None, current, event, limit)
out overdue: List<Int> = bounded.overdue
out obligations: Int = List.len(bounded.pending)
out conflicting: Bool = bounded.conflicting
"#;
    parse_str(source)
        .unwrap()
        .check(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
}

#[test]
fn embedded_rpio_journal_timing_and_maplek_modules_check_strictly() {
    let source = r#"
use experimental::{modules, functions, generics, local_streams, tagged_unions, pattern_matching, newtypes}
use rpio::journal
use rpio::journal::timing
use rpio::journal::knowledge
use rpio::maplek
use std::option::{self, Option}
use std::time::{Instant}

newtype JournalTime = Unit
type K = Union<Sample: Struct<value: Int>>
type E = Struct<detail: Union<Pulse>>
type M = Union<External>
in observation: timing::Observation<K, E, M, JournalTime>
def sample(k: K) -> Option<Int> = match(k) { Sample(s) -> Some(s.value), _ -> None }
aux activation = rpio::journal::timing::activation_timing::<journal::Role::Monitor>(observation)
aux service = rpio::journal::timing::service_timing::<"apply">(observation)
aux read = rpio::journal::knowledge::last_access::<journal::Role::Plan, knowledge::Access::Read, sample>(observation)
aux loop = rpio::maplek::loop(observation)
aux tick = rpio::journal::timing::tick(observation)
aux now: Option<Instant<JournalTime>> = rpio::journal::timing::now(observation)
out pending: Bool = activation.pending
out protocol_followed: Bool = loop.protocol_followed
out phase: Bool = tick.is_phase_record
out has_now: Bool = std::option::is_some(now)
out overlapping: Bool = activation.overlapping_start || service.overlapping_start
out abandoned: Bool = matches(service.ended, Some(Abandoned))
out read_version: Option<journal::Version> = match(read) { Some(r) -> Some(r.ref), None -> None }
out read_value: Option<Int> = match(read) { Some(r) -> Some(r.value), None -> None }
"#;
    parse_str(source)
        .unwrap()
        .check(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
}

#[test]
fn while_reports_the_old_interval_on_the_clearing_tick_and_comparisons_are_strict() {
    let source = r#"
use experimental::{modules, functions, generics, local_streams, tagged_unions, pattern_matching, newtypes, constants}
use std::option::{Option, Some, None}
use std::time::{Instant, Duration}
use std::timing
newtype JournalTime = Unit
const limit: Duration<JournalTime> = std::time::seconds(2.0)
in active: Bool
in now: Float
aux at: Option<Instant<JournalTime>> = Some(std::time::instant(now))
aux event: Option<Instant<JournalTime>> = None
aux tracked = std::timing::while(active, at, event)
out has_span: Bool = std::option::is_some(tracked.span)
out pending: Bool = std::option::is_some(tracked.since)
out late: Bool = std::time::exceeds(tracked.span, limit)
out short: Bool = std::time::shorter_than(tracked.span, limit)
"#;
    let checked = parse_str(source)
        .unwrap()
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_checked(checked).unwrap();
    let inputs = monitor.input_vars().to_vec();
    let outputs = monitor.output_vars().to_vec();
    let mut observed = Vec::new();
    for (active, now) in [(true, 0.0), (true, 1.0), (false, 2.0), (false, 3.0)] {
        let values = inputs
            .iter()
            .map(|name| match name.name().as_str() {
                "active" => Value::Bool(active),
                "now" => Value::Float(now),
                other => panic!("unexpected input {other}"),
            })
            .collect::<Vec<_>>();
        let mut row = vec![Value::NoVal; outputs.len()];
        monitor.evaluate(&values, &mut row).unwrap();
        observed.push(
            outputs
                .iter()
                .map(|name| name.name())
                .zip(row)
                .collect::<std::collections::BTreeMap<_, _>>(),
        );
    }
    let column = |name: &str| {
        observed
            .iter()
            .map(|row| row[name].clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        column("has_span"),
        [false, true, true, false].map(Value::Bool)
    );
    assert_eq!(
        column("pending"),
        [true, true, false, false].map(Value::Bool)
    );
    assert_eq!(
        column("late"),
        [false, false, false, false].map(Value::Bool)
    );
    assert_eq!(
        column("short"),
        [false, true, false, false].map(Value::Bool)
    );
}

fn timing_rows(
    source: &str,
    ticks: &[Vec<(&str, Value)>],
) -> Vec<std::collections::BTreeMap<String, Value>> {
    let checked = parse_str(source)
        .unwrap()
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_checked(checked).unwrap();
    let inputs = monitor.input_vars().to_vec();
    let outputs = monitor.output_vars().to_vec();
    ticks
        .iter()
        .map(|tick| {
            let values = inputs
                .iter()
                .map(|name| {
                    tick.iter()
                        .find(|(key, _)| *key == name.name())
                        .unwrap()
                        .1
                        .clone()
                })
                .collect::<Vec<_>>();
            let mut row = vec![Value::NoVal; outputs.len()];
            monitor.evaluate(&values, &mut row).unwrap();
            outputs
                .iter()
                .map(|name| name.name())
                .zip(row.into_iter().map(|value| match value {
                    Value::Newtype(v) => v.value().clone(),
                    value => value,
                }))
                .collect()
        })
        .collect()
}

#[test]
fn response_time_pairs_overlapping_keys_and_retains_the_first_opener() {
    let source = r#"
use experimental::{modules, functions, generics, local_streams, tagged_unions, pattern_matching, newtypes}
use std::option::{Option, Some, None}
use std::time::{Instant, Duration}
use std::timing
newtype TestTime = Unit
in start: Int
in finish: Int
in timed: Bool
in seconds: Float
aux opening: Option<Int> = if start < 0 then None else Some(start)
aux closing: Option<Int> = if finish < 0 then None else Some(finish)
aux at: Option<Instant<TestTime>> = if timed then Some(std::time::instant(seconds)) else None
aux tracked = std::timing::response_time(opening, closing, at)
out count: Int = List.len(tracked.pending)
out conflict: Bool = tracked.conflicting
out unmatched: Bool = tracked.unmatched
out key: Int = match(tracked.completed) { Some(c) -> c.key, _ -> -1 }
out duration: Duration<TestTime> = match(tracked.completed) { Some(c) -> std::time::duration(c.span), _ -> std::time::seconds(-1.0) }
"#;
    let ticks = [
        (1, -1, true, 0.0),
        (2, -1, true, 1.0),
        (1, -1, true, 2.0),
        (-1, 1, false, 99.0),
        (-1, -1, true, 100.0),
        (-1, 2, true, 4.0),
        (1, 1, true, 5.0),
        (-1, 1, true, 7.0),
        (-1, 1, true, 8.0),
    ];
    let rows = timing_rows(
        source,
        &ticks.map(|(start, finish, timed, seconds)| {
            vec![
                ("start", Value::Int(start)),
                ("finish", Value::Int(finish)),
                ("timed", Value::Bool(timed)),
                ("seconds", Value::Float(seconds)),
            ]
        }),
    );
    for (row, count) in rows.iter().zip([1, 2, 2, 2, 2, 1, 1, 0, 0]) {
        assert_eq!(row["count"], Value::Int(count));
    }
    assert_eq!(rows[2]["conflict"], Value::Bool(true));
    for row in &rows[..5] {
        assert_eq!(row["key"], Value::Int(-1));
    }
    assert_eq!(rows[3]["unmatched"], Value::Bool(false));
    for (index, key, duration) in [(5, 2, 3.0), (6, 1, 5.0), (7, 1, 2.0)] {
        assert_eq!(rows[index]["key"], Value::Int(key));
        assert_eq!(rows[index]["duration"], Value::Float(duration));
    }
    assert_eq!(rows[6]["conflict"], Value::Bool(false));
    assert_eq!(rows[8]["unmatched"], Value::Bool(true));
}

// The payload is deliberately unrelated to ScrewDetection: an integer-coded
// laboratory signal demonstrates that classification belongs to the caller.
const EVENT_PROTOCOL_SOURCE: &str = r#"
use experimental::{modules, functions, generics, local_streams, tagged_unions, pattern_matching, newtypes}
use std::option::{Option, Some, None}
use std::time::{Duration}
use rpio::journal::view::{self, Role, Outcome, CommunicationKind}
use rpio::journal::view::timing
use rpio::journal::view::maplek::events
newtype TestTime = Unit
type K = Union<Reading: Int>
type E = Struct<signal: Int>
type M = Union<Measure>
in kind: Int
in run: Str
in owner: Str
in parent: Str
in ordinal: Int
in cause: Int
in seconds: Float
in clock: Float
aux role: Role = if kind == 1 || kind == 2 then Role::Analysis else if kind == 3 || kind == 8 || kind == 9 then Role::Plan else if kind == 4 || kind == 5 then Role::Legitimate else Role::Execute
aux activation: view::Activation = { epoch: run, start_seq: owner, producer: "lab", role: role, loop: ["lab"],
  caused_by: Some({ activation: { epoch: run, start_seq: parent }, sequence_no: cause }) }
aux step: view::Step<K,E,M> = if kind == 8 then End({ outcome: Planned(NoPlan) }) else if kind == 9 then End({ outcome: Aborted({ reason: Failed("timeout") }) }) else Event({ kind: CommunicationKind::Event, topic: "signal", sequence_no: ordinal, message: { signal: kind } })
aux record: view::Record<K,E,M> = Phase({ position: { epoch: run, seq: 1 }, arrival: { journal_time: seconds, journal_clock: "lab" },
  activation: activation, step: step, evidence: List.filter(\s -> false, [{ name: "", value: { ref: { store_epoch: run, global_write_sequence: 1, key: "Reading" }, value: K::Reading(0), written_by: view::Writer::Node({ run_id:"fixture-run", name: "lab" }) } }]) })
aux observation: timing::Observation<K,E,M,TestTime> = {
 record: if kind == 0 then None else Some({ time: std::time::instant(seconds), value: record }),
 clock: if clock < 0.0 then None else Some({ clock: "lab", time: std::time::instant(clock), origin: "Replay" }) }
def classify(p: view::Posted<E>) -> Option<events::Milestone> = match(p.message.signal) {
  1 -> Some(events::Milestone::Anomaly), 3 -> Some(events::Milestone::Proposal),
  4 -> Some(events::Milestone::Rejection), 5 -> Some(events::Milestone::Acceptance),
  6 -> Some(events::Milestone::Execution), _ -> None }
def required(o: Outcome<K>) -> Option<events::Milestone> = match(o) { Planned(_) -> Some(events::Milestone::Proposal), _ -> None }
def closes(o: Outcome<K>) -> Bool = matches(o, Aborted(_))
aux progress = rpio::journal::view::maplek::events::adaptation::<classify,required,closes>(observation)
out stage: Int = match(progress.state) { Idle -> 0, AwaitingPlan(_) -> 1, AwaitingValidation(_) -> 2, AwaitingExecution(_) -> 3 }
out protocol: Bool = progress.protocol_followed
out missing: Bool = std::option::is_some(progress.missing_publication)
out executed: Bool = progress.executed
out plan: Duration<TestTime> = match(progress.timing.plan) { Some(s) -> std::time::duration(s), _ -> std::time::seconds(-1.0) }
out episode: Duration<TestTime> = match(progress.timing.episode) { Some(s) -> std::time::duration(s), _ -> std::time::seconds(-1.0) }
"#;

/// One journal tick of the protocol fixture. Activations are named by the
/// letters the cases use; each letter stands for its own Start sequence.
fn event_tick(
    kind: i64,
    run: &str,
    owner: &str,
    parent: &str,
    ordinal: i64,
    cause: i64,
    seconds: f64,
    clock: f64,
) -> Vec<(&'static str, Value)> {
    let start_seq = |name: &str| match name {
        "" => 0,
        "a" => 1,
        "p" => 2,
        "l" => 3,
        "p2" => 4,
        "l2" => 5,
        "e" => 6,
        "m" => 7,
        "m2" => 8,
        other => panic!("no activation {other}"),
    };
    vec![
        ("kind", Value::Int(kind)),
        ("run", Value::Str(run.into())),
        ("owner", Value::Str(start_seq(owner).to_string().into())),
        ("parent", Value::Str(start_seq(parent).to_string().into())),
        ("ordinal", Value::Int(ordinal)),
        ("cause", Value::Int(cause)),
        ("seconds", Value::Float(seconds)),
        ("clock", Value::Float(clock)),
    ]
}

#[test]
fn adaptation_matches_full_causes_and_rejection_preserves_episode_time() {
    let rows = timing_rows(
        EVENT_PROTOCOL_SOURCE,
        &[
            event_tick(1, "r", "a", "", 2, 0, 0.0, -1.0),
            event_tick(3, "r", "p", "a", 1, 1, 1.0, -1.0), // wrong ordinal
            event_tick(3, "other", "p", "a", 1, 2, 2.0, -1.0), // wrong run
            event_tick(1, "r", "a", "", 2, 0, 3.0, -1.0),  // repeated anomaly
            event_tick(0, "r", "", "", 0, 0, 0.0, 4.0),
            event_tick(3, "r", "p", "a", 7, 2, 5.0, -1.0),
            event_tick(3, "r", "p", "a", 7, 2, 6.0, -1.0), // repeated proposal
            event_tick(4, "r", "l", "p", 8, 7, 7.0, -1.0),
            event_tick(3, "r", "p2", "l", 9, 8, 8.0, -1.0),
            event_tick(5, "r", "l2", "p2", 10, 9, 9.0, -1.0),
            event_tick(6, "r", "e", "l2", 11, 10, 10.0, 100.0),
            event_tick(0, "r", "", "", 0, 0, 0.0, 101.0),
        ],
    );
    for (row, stage) in rows.iter().zip([1, 1, 1, 1, 1, 2, 2, 1, 2, 3, 0, 0]) {
        assert_eq!(row["stage"], Value::Int(stage));
    }
    for index in [1, 2, 3, 6] {
        assert_eq!(rows[index]["protocol"], Value::Bool(false));
    }
    assert_eq!(rows[4]["plan"], Value::Float(4.0));
    assert_eq!(rows[7]["episode"], Value::Float(7.0));
    assert_eq!(rows[10]["episode"], Value::Float(10.0));
    assert_eq!(rows[10]["executed"], Value::Bool(true));
    assert_eq!(rows[11]["episode"], Value::Float(-1.0));
}

#[test]
fn adaptation_success_without_publication_remains_pending_until_selected_failure() {
    let rows = timing_rows(
        EVENT_PROTOCOL_SOURCE,
        &[
            event_tick(1, "r", "a", "", 2, 0, 0.0, -1.0),
            event_tick(8, "r", "p", "a", 0, 2, 2.0, -1.0),
            event_tick(9, "other", "p", "a", 0, 2, 3.0, -1.0),
            event_tick(9, "r", "p", "a", 0, 1, 4.0, -1.0),
            event_tick(9, "r", "p", "a", 0, 2, 5.0, 100.0),
        ],
    );
    for (row, stage) in rows.iter().zip([1, 1, 1, 1, 0]) {
        assert_eq!(row["stage"], Value::Int(stage));
    }
    assert_eq!(rows[1]["missing"], Value::Bool(true));
    assert_eq!(rows[4]["episode"], Value::Float(5.0));
    assert_eq!(rows[4]["executed"], Value::Bool(false));
}

#[test]
fn sensing_requires_selected_request_and_exact_observation_cause() {
    let source = EVENT_PROTOCOL_SOURCE
        .replace("if kind == 1 || kind == 2", "if kind == 10 || kind == 11 then Role::Monitor else if kind == 12")
        .replace("if kind == 8 then", "if kind == 10 then Start({ kind: CommunicationKind::Event, topic: \"signal\", message: M::Measure }) else if kind == 8 then")
        .replace("1 -> Some(events::Milestone::Anomaly)", "11 -> Some(events::Milestone::Observation), 12 -> Some(events::Milestone::Analysis), 1 -> Some(events::Milestone::Anomaly)");
    let source = source.split("aux progress =").next().unwrap().to_owned()
        + r#"
def request(r: view::Received<M>) -> Bool = matches(r.message, Measure)
aux progress = rpio::journal::view::maplek::events::sensing::<request,classify,required>(observation)
out stage: Int = match(progress.state) { Idle -> 0, AwaitingObservation(_) -> 1, AwaitingAnalysis(_) -> 2 }
out observed: Bool = progress.observed
out analysed: Bool = progress.analysed
out overlap: Bool = progress.overlapping_request
out capture: Duration<TestTime> = match(progress.timing.observation) { Some(s) -> std::time::duration(s), _ -> std::time::seconds(-1.0) }
out detection: Duration<TestTime> = match(progress.timing.analysis) { Some(s) -> std::time::duration(s), _ -> std::time::seconds(-1.0) }
"#;
    let rows = timing_rows(
        &source,
        &[
            event_tick(10, "r", "m", "", 0, 0, 0.0, -1.0),
            event_tick(10, "r", "m2", "", 0, 0, 1.0, -1.0),
            event_tick(11, "other", "m", "", 3, 0, 2.0, -1.0),
            event_tick(11, "r", "m", "", 3, 0, 3.0, -1.0),
            event_tick(12, "r", "a", "m", 1, 2, 4.0, -1.0),
            event_tick(12, "other", "a", "m", 1, 3, 5.0, -1.0),
            event_tick(0, "r", "", "", 0, 0, 0.0, 6.0),
            event_tick(12, "r", "a", "m", 1, 3, 7.0, 100.0),
        ],
    );
    for (row, stage) in rows.iter().zip([1, 1, 1, 2, 2, 2, 2, 0]) {
        assert_eq!(row["stage"], Value::Int(stage));
    }
    assert_eq!(rows[1]["overlap"], Value::Bool(true));
    assert_eq!(rows[3]["observed"], Value::Bool(true));
    assert_eq!(rows[3]["capture"], Value::Float(3.0));
    assert_eq!(rows[6]["detection"], Value::Float(3.0));
    assert_eq!(rows[7]["detection"], Value::Float(4.0));
    assert_eq!(rows[7]["analysed"], Value::Bool(true));
}

// ---------------------------------------------------------------------------
// `std::timing::deadline`, the response deadline helper the paper roots use.
// ---------------------------------------------------------------------------

/// `deadline` over plain inputs: a negative key, time or budget is absent.
const DEADLINE_SOURCE: &str = r#"
use experimental::{modules, functions, generics, local_streams, tagged_unions, pattern_matching, newtypes}
use std::option::{Option, Some, None}
use std::time::{Instant, Duration}
use std::timing
newtype TestTime = Unit
in open: Int
in close: Int
in cancel: Int
in now: Float
in at: Float
in limit: Float
def key(k: Int) -> Option<Int> = if k < 0 then None else Some(k)
def instant(t: Float) -> Option<Instant<TestTime>> = if t < 0.0 then None else Some(std::time::instant(t))
aux budget: Option<Duration<TestTime>> = if limit < 0.0 then None else Some(std::time::seconds(limit))
aux d = std::timing::deadline(key(open), key(close), key(cancel), budget, { now: instant(now), at: instant(at) })
out overdue: Bool = d.overdue
out completed_start: Instant<TestTime> = match(d.completed) { Some(s) -> s.start, _ -> std::time::instant(-1.0) }
out completed_end: Instant<TestTime> = match(d.completed) { Some(s) -> s.end, _ -> std::time::instant(-1.0) }
out pending: Int = d.pending
"#;

/// One `deadline` call: keys and times are absent when negative.
#[derive(Clone, Copy)]
struct Call {
    open: i64,
    close: i64,
    cancel: i64,
    now: f64,
    at: f64,
}

/// A record tick at `t` (so `now` and `at` agree) with the given keys.
fn record(open: i64, close: i64, t: f64) -> Call {
    Call {
        open,
        close,
        cancel: -1,
        now: t,
        at: t,
    }
}

/// A clock-only tick: `now` advances, nothing opens or closes.
fn clock(now: f64) -> Call {
    Call {
        now,
        at: -1.0,
        ..record(-1, -1, -1.0)
    }
}

#[derive(Debug, PartialEq)]
struct Watched {
    overdue: bool,
    completed: Option<(f64, f64)>,
    pending: i64,
}

/// The checked `deadline` program, compiled once; each run gets a fresh monitor.
fn deadline_program() -> crate::lang::dsrv::ElaboratedDsrvSpecification {
    parse_str(DEADLINE_SOURCE)
        .unwrap()
        .check_and_elaborate(TypeCheckOptions::STRICT)
        .without_warnings()
        .unwrap()
}

fn run_deadline(
    program: &crate::lang::dsrv::ElaboratedDsrvSpecification,
    limit: Option<f64>,
    calls: &[Call],
) -> Vec<Watched> {
    let mut monitor = DataflowMonitor::compile_checked(program.clone()).unwrap();
    let inputs = monitor.input_vars().to_vec();
    let outputs = monitor.output_vars().to_vec();
    calls
        .iter()
        .map(|call| {
            let value = |name: &str| match name {
                "open" => Value::Int(call.open),
                "close" => Value::Int(call.close),
                "cancel" => Value::Int(call.cancel),
                "now" => Value::Float(call.now),
                "at" => Value::Float(call.at),
                "limit" => Value::Float(limit.unwrap_or(-1.0)),
                other => panic!("no input {other}"),
            };
            let row: Vec<Value> = inputs
                .iter()
                .map(|name| value(name.name().as_ref()))
                .collect();
            let mut out = vec![Value::NoVal; outputs.len()];
            monitor.evaluate(&row, &mut out).unwrap();
            let get = |name: &str| {
                let index = outputs
                    .iter()
                    .position(|o| o.name().as_ref() as &str == name)
                    .unwrap();
                match &out[index] {
                    Value::Newtype(v) => v.value().clone(),
                    value => value.clone(),
                }
            };
            let float = |name: &str| match get(name) {
                Value::Float(f) => f,
                other => panic!("{name}: {other:?}"),
            };
            Watched {
                overdue: get("overdue") == Value::Bool(true),
                completed: (float("completed_end") >= 0.0)
                    .then(|| (float("completed_start"), float("completed_end"))),
                pending: match get("pending") {
                    Value::Int(n) => n,
                    other => panic!("pending: {other:?}"),
                },
            }
        })
        .collect()
}

/// A wait is overdue once: when its closing response took strictly longer
/// than the budget, or when "now" first passes the budget while it is open.
/// A response exactly at the budget is on time, and a backwards span is never
/// a breach.
#[test]
fn deadline_reports_a_late_closure_and_a_silent_breach_once() {
    let program = deadline_program();
    let rows = run_deadline(
        &program,
        Some(5.0),
        &[
            record(1, -1, 0.0),  // 0: wait 1 from 0
            record(2, -1, 1.0),  // 1: wait 2 from 1
            record(-1, 1, 5.0),  // 2: closes 1 after exactly 5 s
            clock(6.5),          // 3: wait 2 has been open 5.5 s
            clock(8.0),          // 4: already reported
            record(-1, 2, 9.0),  // 5: closes 2; it was reported while open
            record(3, -1, 10.0), // 6
            record(-1, 3, 15.5), // 7: a late closing response
            record(4, -1, 20.0), // 8
            Call {
                at: 18.0,
                ..record(-1, 4, 20.0)
            }, // 9: closes before it began
        ],
    );
    let overdue: Vec<bool> = rows.iter().map(|row| row.overdue).collect();
    assert_eq!(
        overdue,
        [
            false, false, false, true, false, false, false, true, false, false
        ]
    );
    assert_eq!(rows[2].completed, Some((0.0, 5.0)));
    assert_eq!(rows[5].completed, Some((1.0, 9.0)));
    assert_eq!(rows[7].completed, Some((10.0, 15.5)));
    assert_eq!(rows[9].completed, Some((20.0, 18.0)));
    let pending: Vec<i64> = rows.iter().map(|row| row.pending).collect();
    assert_eq!(pending, [1, 2, 1, 1, 1, 0, 1, 0, 1, 0]);
}

/// A duplicate open keeps the original wait; close-then-open on one tick
/// closes the old wait and opens a new one; a cancel withdraws without
/// judgement; with no budget, the interval is kept for a later verdict; any
/// number of waits may be open at once.
#[test]
fn deadline_orders_close_cancel_and_open_within_a_tick() {
    let program = deadline_program();
    let rows = run_deadline(
        &program,
        Some(5.0),
        &[
            record(1, -1, 0.0),
            record(1, -1, 3.0), // duplicate: keeps since 0
            record(-1, 1, 4.0), // closes the original
            record(1, -1, 10.0),
            record(1, 1, 12.0),  // closes since 10, opens since 12
            record(-1, 1, 18.0), // late: 6 s
            Call {
                cancel: 1,
                ..record(1, -1, 20.0)
            }, // cancel (nothing) then open
            Call {
                cancel: 1,
                ..record(-1, -1, 40.0)
            }, // withdrawn, not judged
        ],
    );
    assert_eq!(rows[1].pending, 1);
    assert_eq!(rows[2].completed, Some((0.0, 4.0)));
    assert_eq!(rows[4].completed, Some((10.0, 12.0)));
    assert_eq!(rows[4].pending, 1);
    assert_eq!(rows[5].completed, Some((12.0, 18.0)));
    assert!(rows[5].overdue);
    assert_eq!(rows[6].pending, 1);
    assert_eq!(rows[7].pending, 0);
    assert_eq!(rows[7].completed, None);
    assert!(!rows[7].overdue);

    let open_ended = run_deadline(
        &program,
        None,
        &[record(1, -1, 0.0), clock(1000.0), record(-1, 1, 2000.0)],
    );
    assert!(open_ended.iter().all(|row| !row.overdue));
    assert_eq!(open_ended[2].completed, Some((0.0, 2000.0)));

    let many: Vec<Call> = (0..100).map(|k| record(k, -1, k as f64 * 0.01)).collect();
    let rows = run_deadline(&program, Some(5.0), &many);
    assert_eq!(rows.last().unwrap().pending, 100);
}

mod deadline_model {
    use super::*;
    use proptest::prelude::*;
    use tc_testutils::journal::strategies::deadline::{self, DeadlineTick};

    fn calls(ticks: &[DeadlineTick]) -> Vec<Call> {
        let key = |k: Option<u8>| k.map_or(-1, i64::from);
        let time = |t: Option<i64>| t.map_or(-1.0, |us| us as f64 / 1e6);
        ticks
            .iter()
            .map(|tick| Call {
                open: key(tick.open),
                close: key(tick.close),
                cancel: key(tick.cancel),
                now: time(tick.now_us),
                at: time(tick.at_us),
            })
            .collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

        /// `deadline` agrees tick by tick with an independent restatement.
        #[test]
        fn deadline_matches_its_reference_model(
            budget in deadline::budget(),
            ticks in deadline::ticks(deadline::Bounds::default()),
        ) {
            let program = deadline_program();
            let limit = budget.map(|us| us as f64 / 1e6);
            let observed = run_deadline(&program, limit, &calls(&ticks));
            let expected = deadline::run(budget, &ticks);
            for (index, (observed, expected)) in observed.iter().zip(&expected).enumerate() {
                let completed = expected.completed.map(|(start, end)| (start as f64 / 1e6, end as f64 / 1e6));
                prop_assert_eq!(
                    (observed.overdue, observed.completed, observed.pending as usize),
                    (expected.overdue, completed, expected.pending),
                    "tick {}: {:?}", index, ticks[index]
                );
            }
        }
    }
}
