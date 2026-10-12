//! Library patterns that skip work on quiet ticks agree with their eager
//! formulations when inputs are absent or deferred.
//!
//! `std::timing::deadline` and `std::timing::bounded_response` rebuild their
//! pending lists only on a tick where some entry first becomes overdue, and
//! `std::stream::recent` returns a history already within capacity unchanged.
//! Each skip is a `match` arm that is not selected, so its expressions are not
//! evaluated on that tick. These tests run each pattern beside the eager
//! formulation it replaced, copied below with its private helpers renamed, on
//! the dataflow and semi-sync runtimes under both checked semantics. Inputs
//! are absent on some ticks and `defer` holds others Deferred, including ticks
//! that abort because an absent value reaches a list callback. Both programs
//! must print the same rows, succeed or fail together, and report the same
//! failure.

use std::path::{Path, PathBuf};
use std::process::Command;

const RUNTIMES: [&str; 2] = ["dataflow", "semi-sync"];
const SEMANTICS: [&str; 2] = ["typed-untimed", "gradual-typed-untimed"];

const DEADLINE_HEADER: &str = r#"
use experimental::{modules, functions, generics, local_streams, tagged_unions, pattern_matching, newtypes}
use std::option::{Option, Some, None}
use std::time::{Instant, Duration, TimeInterval}
use std::timing::{Wait, Clock, Deadline}
newtype TestTime = Unit
in open: Int
in close: Int
in cancel: Int
in now: Float
in at: Float
in limit_source: Str
aux limit: Float = defer(limit_source: Float)
def key_of(k: Int) -> Option<Int> = if k < 0 then None else Some(k)
def instant(t: Float) -> Option<Instant<TestTime>> = if t < 0.0 then None else Some(std::time::instant(t))
aux budget: Option<Duration<TestTime>> = if limit < 0.0 then None else Some(std::time::seconds(limit))
aux clock: Clock<TestTime> = { now: instant(now), at: instant(at) }
out overdue: Bool = d.overdue
out completed: Option<TimeInterval<TestTime>> = d.completed
out pending: Int = d.pending
"#;

const DEADLINE_HELPER: &str = r#"
aux d: Deadline<TestTime> = std::timing::deadline(key_of(open), key_of(close), key_of(cancel), budget, clock)
"#;

// `std::timing::deadline` without its skip of quiet ticks.
const DEADLINE_EAGER: &str = r#"
def eager_wait_for<K, D>(waits: List<Wait<K, D>>, key: Option<K>) -> Option<Wait<K, D>> = match(key) {
  Some(k) -> match(List.filter(\w: Wait<K, D> -> w.key == k, waits)) { [] -> None, found -> Some(List.head(found)) },
  _ -> None,
}
def eager_waits_without<K, D>(waits: List<Wait<K, D>>, key: Option<K>) -> List<Wait<K, D>> = match(key) {
  Some(k) -> List.filter(\w: Wait<K, D> -> w.key != k, waits),
  _ -> waits,
}
def eager_over_limit<D>(span: Option<TimeInterval<D>>, limit: Option<Duration<D>>) -> Bool = match(limit) {
  Some(budget) -> std::time::exceeds(span, budget), _ -> false,
}
def eager_deadline<K, D>(open: Option<K>, close: Option<K>, cancel: Option<K>,
  budget: Option<Duration<D>>, clock: Clock<D>) -> Deadline<D> = {
    overdue: late_close || List.len(late_open) > 0,
    completed: completion,
    pending: List.len(pending)
  }
where {
  before: List<Wait<K, D>> = default(pending[1], [])
  timed: Bool = std::option::is_some(clock.at)
  response: Option<Wait<K, D>> = if timed then eager_wait_for(before, close) else None
  answered: List<Wait<K, D>> = if timed then eager_waits_without(before, close) else before
  remaining: List<Wait<K, D>> = if timed then eager_waits_without(answered, cancel) else answered
  repeated: Option<Wait<K, D>> = eager_wait_for(remaining, open)
  created: List<Wait<K, D>> = match(Tuple(open, clock.at)) {
    (Some(key), Some(time)) if std::option::is_none(repeated) -> [{key, since: time, reported: false}],
    _ -> [],
  }
  kept: List<Wait<K, D>> = List.concat(remaining, created)
  completion: Option<TimeInterval<D>> = match(Tuple(response, clock.at)) {
    (Some(w), Some(time)) -> Some(std::time::between(w.since, time)), _ -> None,
  }
  late_close: Bool = std::option::is_some_and(response, \w: Wait<K, D> -> !w.reported)
    && eager_over_limit(completion, budget)
  late_open: List<Wait<K, D>> = List.filter(\w: Wait<K, D> ->
    (w.reported || eager_over_limit(std::time::since(Some(w.since), clock.now), budget)) != w.reported, kept)
  pending: List<Wait<K, D>> = List.map(\w: Wait<K, D> ->
    {key: w.key, since: w.since, reported: w.reported || eager_over_limit(std::time::since(Some(w.since), clock.now), budget)}, kept)
}
aux d: Deadline<TestTime> = eager_deadline(key_of(open), key_of(close), key_of(cancel), budget, clock)
"#;

const BOUNDED_HEADER: &str = r#"
use experimental::{modules, functions, generics, local_streams, tagged_unions, pattern_matching, newtypes}
use std::option::{self, Option}
use std::time::{self, Instant, Duration}
use std::timing::{self, Obligation, Closure, Closing, BoundedResponseTiming}
newtype JournalTime = Unit
in trigger: Int
in respond: Int
in withdraw: Int
in clock: Float
in at: Float
in limit_source: Str
aux limit: Float = defer(limit_source: Float)
def key_of(k: Int) -> Option<Int> = if k == 0 then None else Some(k)
def instant(t: Float) -> Option<Instant<JournalTime>> = if t < 0.0 then None else Some(std::time::instant(t))
out overdue: List<Int> = response.overdue
out pending: List<Obligation<Int, JournalTime>> = response.pending
out closed: Option<Closure<Int, JournalTime>> = response.closed
out conflicting: Bool = response.conflicting
out unmatched: Bool = response.unmatched
"#;

const BOUNDED_HELPER: &str = r#"
aux response: BoundedResponseTiming<Int, JournalTime> = std::timing::bounded_response(
  key_of(trigger), key_of(respond), key_of(withdraw), instant(clock), instant(at), std::time::seconds(limit))
"#;

// `std::timing::bounded_response` before its open list skipped quiet ticks.
const BOUNDED_EAGER: &str = r#"
def eager_bounded_response<K, D>(triggered: Option<K>, responded: Option<K>, withdrawn: Option<K>,
                               now: Option<Instant<D>>, event: Option<Instant<D>>,
                               limit: Duration<D>) -> BoundedResponseTiming<K, D> = {
    pending: open,
    overdue: List.concat(late, breached),
    closed: closed,
    conflicting: std::option::is_some(repeated),
    unmatched: timed && ((std::option::is_some(responded) && std::option::is_none(response))
      || (std::option::is_some(withdrawn) && std::option::is_none(withdrawal)))
  }
where {
  before: List<Obligation<K, D>> = default(open[1], [])
  at: Option<Instant<D>> = std::option::or_else(event, now)
  timed: Bool = std::option::is_some(at)
  response: Option<Obligation<K, D>> = if timed then std::timing::obligation(before, responded) else None
  answered: List<Obligation<K, D>> = if timed then std::timing::without(before, responded) else before
  withdrawal: Option<Obligation<K, D>> = if timed then std::timing::obligation(answered, withdrawn) else None
  remaining: List<Obligation<K, D>> = if timed then std::timing::without(answered, withdrawn) else answered
  repeated: Option<Obligation<K, D>> = if timed then std::timing::obligation(remaining, triggered) else None
  created: List<Obligation<K, D>> = match(Tuple(triggered, at, repeated)) {
    (Some(k), Some(t), None) -> [{ key: k, since: t, reported: false }],
    _ -> [],
  }
  kept: List<Obligation<K, D>> = List.concat(remaining, created)
  closed: Option<Closure<K, D>> = match(Tuple(response, withdrawal, at)) {
    (Some(o), _, Some(t)) -> Some({ key: o.key, span: std::time::between(o.since, t), by: Closing::Responded }),
    (None, Some(o), Some(t)) -> Some({ key: o.key, span: std::time::between(o.since, t), by: Closing::Withdrawn }),
    _ -> None,
  }
  late: List<K> = match(Tuple(response, closed)) {
    (Some(o), Some(c)) if !o.reported && std::time::exceeds(std::option::some(c.span), limit) -> [o.key],
    _ -> [],
  }
  breached: List<K> = List.map(\o: Obligation<K, D> -> o.key,
    List.filter(\o: Obligation<K, D> -> std::timing::breaches(o, now, limit), kept))
  open: List<Obligation<K, D>> = List.map(\o: Obligation<K, D> ->
    if std::timing::breaches(o, now, limit) then { key: o.key, since: o.since, reported: true } else o, kept)
}
aux response: BoundedResponseTiming<Int, JournalTime> = eager_bounded_response(
  key_of(trigger), key_of(respond), key_of(withdraw), instant(clock), instant(at), std::time::seconds(limit))
"#;

const RECENT_HEADER: &str = r#"
use experimental::{modules, functions, generics, local_streams, tagged_unions, pattern_matching}
use std::option::{Option}
in x: Int
in hit: Bool
in reset: Bool
in x_source: Str
aux deferred_x: Int = defer(x_source: Int)
aux sample: Option<Int> = if hit then Some(deferred_x) else None
out before2: List<Int> = r2.before
out after2: List<Int> = r2.after
out after3: List<Int> = r3.after
"#;

const RECENT_HELPER: &str = r#"
use std::stream
aux r2 = std::stream::recent::<2>(sample, reset)
aux r3 = std::stream::recent::<3>(sample, reset)
"#;

// `std::stream::recent` before `take` returned short histories unchanged.
const RECENT_EAGER: &str = r#"
def eager_take<T>(entries: List<T>, capacity: Int) -> List<T> =
  List.fold(\kept, entry ->
    if List.len(kept) < capacity then List.append(kept, entry) else kept,
    List.filter(\entry -> false, entries), entries)
def eager_recent<T>(sample: Option<T>, reset: Bool, capacity: Int) -> Struct<before: List<T>, after: List<T>> = { before, after }
where {
  before: List<T> = default(after[1], [])
  inserted: List<T> = match(sample) { Some(value) -> List.concat([value], before), None -> before }
  after: List<T> = if reset then [] else eager_take(inserted, capacity)
}
aux r2 = eager_recent(sample, reset, 2)
aux r3 = eager_recent(sample, reset, 3)
"#;

/// Traces for `deadline`, with a budget of 2 s.
const DEADLINE_TRACES: &[(&str, &str)] = &[
    (
        // Waits age over clock-only ticks that omit every other input; one
        // breaches, closes late without a second report, and the next
        // breaches later.
        "quiet-ticks",
        r#"0: open = 1
   close = -1
   cancel = -1
   now = 0.0
   at = 0.0
   limit_source = "2.0"
1: now = 0.5
2: open = 2
   close = -1
   cancel = -1
   now = 1.0
   at = 1.0
3: now = 2.5
4: now = 2.6
5: open = -1
   close = 1
   cancel = -1
   now = 3.0
   at = 3.0
6: now = 3.5
7: open = 3
   close = -1
   cancel = -1
   now = 4.0
   at = 4.0
8: now = 9.0
9: open = 4
   close = -1
   cancel = -1
   now = 10.0
   at = 10.0
10: now = 13.0
"#,
    ),
    (
        // The limit stays Deferred until tick 2; nothing is pending before it.
        "deferred-limit",
        r#"0: open = -1
   close = -1
   cancel = -1
   now = 0.0
   at = -1.0
1: now = 1.0
2: open = 1
   close = -1
   cancel = -1
   now = 2.0
   at = 2.0
   limit_source = "2.0"
3: now = 3.0
4: now = 5.0
5: close = 1
   open = -1
   now = 6.0
   at = 6.0
"#,
    ),
    (
        // A wait opens while "now" is still absent, so the overdue test
        // receives no value and the run aborts.
        "absent-now-with-a-pending-wait",
        r#"0: open = 1
   close = -1
   cancel = -1
   at = 0.0
   limit_source = "2.0"
1: now = 3.0
"#,
    ),
    (
        // The limit is Deferred while a wait is pending, so the run aborts.
        "deferred-limit-with-a-pending-wait",
        r#"0: open = 1
   close = -1
   cancel = -1
   now = 0.0
   at = 0.0
1: now = 3.0
"#,
    ),
];

/// Traces for `bounded_response`, with a limit of 1 s.
const BOUNDED_TRACES: &[(&str, &str)] = &[
    (
        "quiet-ticks",
        r#"0: trigger = 1
   respond = 0
   withdraw = 0
   clock = 0.0
   at = 0.0
   limit_source = "1.0"
1: clock = 0.5
2: trigger = 2
   respond = 0
   withdraw = 0
   clock = 0.8
   at = 0.8
3: clock = 1.5
4: clock = 1.6
5: trigger = 0
   respond = 1
   withdraw = 0
   clock = 2.0
   at = 2.0
6: clock = 2.5
7: trigger = 3
   respond = 0
   withdraw = 2
   clock = 3.0
   at = 3.0
8: clock = 5.0
"#,
    ),
    (
        "deferred-limit",
        r#"0: trigger = 0
   respond = 0
   withdraw = 0
   clock = 0.0
   at = -1.0
1: clock = 1.0
   limit_source = "1.0"
2: trigger = 1
   respond = 0
   withdraw = 0
   clock = 2.0
   at = 2.0
3: clock = 4.0
"#,
    ),
    (
        "absent-clock-with-a-pending-obligation",
        r#"0: trigger = 1
   respond = 0
   withdraw = 0
   at = 0.0
   limit_source = "1.0"
1: clock = 3.0
"#,
    ),
];

/// Traces for `recent` at capacities 2 and 3.
const RECENT_TRACES: &[(&str, &str)] = &[
    (
        "absent-and-deferred-samples",
        r#"0: x = 1
   hit = false
   reset = true
1: hit = true
   reset = false
2: x = 2
   x_source = "x"
3: x = 3
   hit = true
4: reset = false
5: x = 5
   hit = true
   reset = false
6: x = 6
7: hit = false
   reset = true
8: x = 8
   hit = true
   reset = false
"#,
    ),
    (
        "deferred-sample",
        r#"0: x = 1
   hit = true
   reset = true
1: x = 2
   hit = true
   reset = false
2: x = 3
   x_source = "x"
3: x = 4
   hit = true
"#,
    ),
];

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "tc-library-eager-parity-{name}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn write(&self, name: &str, text: &str) -> PathBuf {
        let path = self.0.join(name);
        std::fs::write(&path, text).unwrap();
        path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// What one run showed: whether it succeeded, its rows, and its failure.
#[derive(Debug, PartialEq)]
struct Outcome {
    success: bool,
    stdout: String,
    failure: String,
}

fn run(spec: &Path, trace: &Path, runtime: &str, semantics: &str) -> Outcome {
    let result = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
        .arg(spec)
        .arg("--input-file")
        .arg(trace)
        .args([
            "--output-stdout",
            "--runtime",
            runtime,
            "--semantics",
            semantics,
        ])
        .env("RUST_LOG", "error")
        .output()
        .unwrap();
    // A panic names its thread by number; only the message identifies it.
    let failure = String::from_utf8_lossy(&result.stderr)
        .lines()
        .filter(|line| !line.starts_with("thread ") && !line.starts_with("note: "))
        .collect::<Vec<_>>()
        .join("\n");
    Outcome {
        success: result.status.success(),
        stdout: String::from_utf8_lossy(&result.stdout).into_owned(),
        failure,
    }
}

/// Run the library pattern and its eager formulation over every trace and
/// require the same outcome. Returns how many runs succeeded and failed, so a
/// caller can check that both kinds were exercised.
fn assert_parity(
    name: &str,
    header: &str,
    helper: &str,
    eager: &str,
    traces: &[(&str, &str)],
) -> (usize, usize) {
    let scratch = Scratch::new(name);
    let helper = scratch.write("helper.dsrv", &format!("{header}{helper}"));
    let eager = scratch.write("eager.dsrv", &format!("{header}{eager}"));
    let (mut succeeded, mut failed) = (0, 0);
    for (trace_name, text) in traces {
        let trace = scratch.write(&format!("{trace_name}.trace"), text);
        for runtime in RUNTIMES {
            for semantics in SEMANTICS {
                let library = run(&helper, &trace, runtime, semantics);
                let reference = run(&eager, &trace, runtime, semantics);
                assert_eq!(
                    library, reference,
                    "{name}/{trace_name} on {runtime}/{semantics}"
                );
                if library.success {
                    assert!(
                        !library.stdout.is_empty(),
                        "{name}/{trace_name} on {runtime}/{semantics} printed nothing"
                    );
                    succeeded += 1;
                } else {
                    failed += 1;
                }
            }
        }
    }
    (succeeded, failed)
}

#[test]
fn deadline_matches_its_eager_formulation_with_absent_and_deferred_inputs() {
    let (succeeded, failed) = assert_parity(
        "deadline",
        DEADLINE_HEADER,
        DEADLINE_HELPER,
        DEADLINE_EAGER,
        DEADLINE_TRACES,
    );
    assert!(
        succeeded > 0 && failed > 0,
        "{succeeded} succeeded, {failed} failed"
    );
}

#[test]
fn bounded_response_matches_its_eager_formulation_with_absent_and_deferred_inputs() {
    let (succeeded, failed) = assert_parity(
        "bounded",
        BOUNDED_HEADER,
        BOUNDED_HELPER,
        BOUNDED_EAGER,
        BOUNDED_TRACES,
    );
    assert!(
        succeeded > 0 && failed > 0,
        "{succeeded} succeeded, {failed} failed"
    );
}

#[test]
fn recent_matches_its_eager_formulation_with_absent_and_deferred_samples() {
    let (succeeded, _) = assert_parity(
        "recent",
        RECENT_HEADER,
        RECENT_HELPER,
        RECENT_EAGER,
        RECENT_TRACES,
    );
    assert!(succeeded > 0);
}
