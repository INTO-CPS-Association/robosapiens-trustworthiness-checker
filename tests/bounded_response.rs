//! `std::timing::bounded_response` through the CLI under both runtimes.
//!
//! `tests/fixtures/bounded_response.dsrv` calls the stream function over
//! integer keys with a limit of 1 s. Each scenario is a table of ticks; a
//! key of 0 and a negative instant stand for absence, so a tick with only a
//! `clock` is a clock-only tick that carries no record.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;

const ROOT: &str = env!("CARGO_MANIFEST_DIR");
const MODEL: &str = "tests/fixtures/bounded_response.dsrv";
const RUNTIMES: [&str; 2] = ["dataflow", "semi-sync"];
const SEMANTICS: [&str; 2] = ["typed-untimed", "gradual-typed-untimed"];

/// One tick: the keys triggered, responded to and withdrawn, "now", and the
/// instant of the record the tick carries.
#[derive(Clone, Copy, Default)]
struct Tick {
    trigger: i64,
    respond: i64,
    withdraw: i64,
    clock: f64,
    at: f64,
}

/// A record at `at`, which also moves "now" there.
fn record(at: f64) -> Tick {
    Tick {
        clock: at,
        at,
        ..Tick::default()
    }
}

fn trigger(key: i64, at: f64) -> Tick {
    Tick {
        trigger: key,
        ..record(at)
    }
}

fn respond(key: i64, at: f64) -> Tick {
    Tick {
        respond: key,
        ..record(at)
    }
}

fn withdraw(key: i64, at: f64) -> Tick {
    Tick {
        withdraw: key,
        ..record(at)
    }
}

/// A clock-only tick: "now" moves to `clock` and no record is observed.
fn clock(clock: f64) -> Tick {
    Tick {
        clock,
        at: -1.0,
        ..Tick::default()
    }
}

type Rows = BTreeMap<String, Vec<String>>;

fn temp_path(suffix: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "tc-bounded-response-{}-{}{suffix}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ))
}

fn replay(ticks: &[Tick], runtime: &str, semantics: &str) -> Rows {
    let input = temp_path(".input");
    let mut text = String::new();
    for (k, tick) in ticks.iter().enumerate() {
        text.push_str(&format!(
            "{k}: trigger = {}\n{k}: respond = {}\n{k}: withdraw = {}\n{k}: clock = {:?}\n{k}: at = {:?}\n",
            tick.trigger, tick.respond, tick.withdraw, tick.clock, tick.at
        ));
    }
    std::fs::write(&input, text).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
        .args([
            MODEL,
            "--input-file",
            &input.display().to_string(),
            "--output-stdout",
            "--runtime",
            runtime,
            "--semantics",
            semantics,
        ])
        .env("RUST_LOG", "error")
        .current_dir(ROOT)
        .output()
        .expect("the checker runs");
    let _ = std::fs::remove_file(&input);
    assert!(
        output.status.success(),
        "{runtime}/{semantics}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut rows = Rows::new();
    for line in String::from_utf8(output.stdout).unwrap().lines() {
        let (key, value) = line.split_once(" = ").expect("an output assignment");
        let (name, row) = key.split_once('[').expect("a row-indexed output");
        let row: usize = row.trim_end_matches(']').parse().unwrap();
        let column = rows.entry(name.to_owned()).or_default();
        assert_eq!(column.len(), row, "{runtime}/{semantics}: rows in order");
        column.push(value.to_owned());
    }
    rows
}

/// Replay under both runtimes, which must agree on every row, once per
/// semantics, returning the dataflow rows.
fn replay_both(ticks: &[Tick]) -> Rows {
    let mut result = None;
    for semantics in SEMANTICS {
        let [dataflow, semi_sync] = RUNTIMES.map(|runtime| replay(ticks, runtime, semantics));
        assert_eq!(dataflow, semi_sync, "{semantics}: the runtimes agree");
        if let Some(previous) = &result {
            assert_eq!(previous, &dataflow, "{semantics}: the semantics agree");
        }
        result = Some(dataflow);
    }
    result.unwrap()
}

fn column<'a>(rows: &'a Rows, name: &str) -> Vec<&'a str> {
    rows[name].iter().map(String::as_str).collect()
}

#[test]
fn a_late_response_is_reported_once_at_the_closing_tick() {
    let rows = replay_both(&[
        trigger(1, 0.0),
        respond(1, 0.5),
        trigger(2, 1.0),
        respond(2, 2.5),
        clock(5.0),
    ]);
    assert_eq!(column(&rows, "overdue"), ["[]", "[]", "[]", "[2]", "[]"]);
    assert_eq!(
        column(&rows, "closed"),
        ["None", "Some(1)", "None", "Some(2)", "None"]
    );
    assert_eq!(column(&rows, "pending"), ["[1]", "[]", "[2]", "[]", "[]"]);
    assert_eq!(column(&rows, "withdrawn"), ["false"; 5]);
}

/// Silence: only clock-only ticks follow the trigger, and the first whose
/// age strictly exceeds the limit reports the obligation. Neither a later
/// clock tick nor the late response reports it again.
#[test]
fn a_clock_only_tick_reports_silence_once() {
    let rows = replay_both(&[
        trigger(1, 0.0),
        clock(0.5),
        clock(1.0),
        clock(1.5),
        clock(2.0),
        respond(1, 3.0),
    ]);
    assert_eq!(
        column(&rows, "overdue"),
        ["[]", "[]", "[]", "[1]", "[]", "[]"]
    );
    assert_eq!(
        column(&rows, "pending"),
        ["[1]", "[1]", "[1]", "[1]", "[1]", "[]"]
    );
    assert_eq!(column(&rows, "closed")[5], "Some(1)");
}

#[test]
fn overlapping_obligations_are_judged_independently() {
    let rows = replay_both(&[
        trigger(1, 0.0),
        trigger(2, 0.5),
        respond(2, 0.9),
        clock(1.2),
        respond(1, 1.3),
        trigger(3, 2.0),
        trigger(4, 2.5),
        clock(3.2),
        clock(3.6),
    ]);
    assert_eq!(
        column(&rows, "overdue"),
        ["[]", "[]", "[]", "[1]", "[]", "[]", "[]", "[3]", "[4]"]
    );
    assert_eq!(
        column(&rows, "pending"),
        [
            "[1]", "[1, 2]", "[1]", "[1]", "[]", "[3]", "[3, 4]", "[3, 4]", "[3, 4]"
        ]
    );
    assert_eq!(column(&rows, "conflicting"), ["false"; 9]);
    assert_eq!(column(&rows, "unmatched"), ["false"; 9]);
}

/// A re-trigger keeps the original instant, so the obligation is overdue
/// 1 s after the first trigger. A response to a key never triggered is
/// unmatched. A withdrawal closes without judgement, even late, and a second
/// one is unmatched. A tick with no instant changes nothing.
#[test]
fn conflicts_unmatched_closings_and_withdrawals() {
    let rows = replay_both(&[
        trigger(1, 0.0),
        trigger(1, 0.6),
        clock(1.2),
        respond(3, 1.3),
        withdraw(1, 1.4),
        trigger(4, 2.0),
        withdraw(4, 3.5),
        withdraw(4, 3.6),
        Tick {
            trigger: 5,
            clock: -1.0,
            at: -1.0,
            ..Tick::default()
        },
        clock(9.0),
    ]);
    assert_eq!(
        column(&rows, "conflicting"),
        [
            "false", "true", "false", "false", "false", "false", "false", "false", "false", "false"
        ]
    );
    assert_eq!(
        column(&rows, "unmatched"),
        [
            "false", "false", "false", "true", "false", "false", "false", "true", "false", "false"
        ]
    );
    assert_eq!(
        column(&rows, "overdue"),
        ["[]", "[]", "[1]", "[]", "[]", "[]", "[]", "[]", "[]", "[]"]
    );
    assert_eq!(
        column(&rows, "closed"),
        [
            "None", "None", "None", "None", "Some(1)", "None", "Some(4)", "None", "None", "None"
        ]
    );
    assert_eq!(
        column(&rows, "withdrawn"),
        [
            "false", "false", "false", "false", "true", "false", "true", "false", "false", "false"
        ]
    );
    assert_eq!(
        column(&rows, "pending"),
        [
            "[1]", "[1]", "[1]", "[1]", "[]", "[4]", "[]", "[]", "[]", "[]"
        ]
    );
}

/// AC-TS-3d: a span or age that runs backwards is not a breach. A response
/// recorded before its trigger closes the obligation without a finding, and
/// an obligation triggered ahead of "now" is judged once "now" passes it.
#[test]
fn a_backwards_span_or_age_is_not_a_breach() {
    let rows = replay_both(&[
        trigger(1, 5.0),
        Tick {
            respond: 1,
            clock: 5.0,
            at: 3.0,
            ..Tick::default()
        },
        Tick {
            trigger: 2,
            clock: 5.0,
            at: 8.0,
            ..Tick::default()
        },
        clock(6.5),
        clock(9.5),
    ]);
    assert_eq!(column(&rows, "overdue"), ["[]", "[]", "[]", "[]", "[2]"]);
    assert_eq!(column(&rows, "closed")[1], "Some(1)");
    assert_eq!(column(&rows, "pending"), ["[1]", "[]", "[2]", "[2]", "[2]"]);
}

/// Within a tick, a response is applied before a trigger. A response and a
/// trigger with a pending key close it and open a fresh obligation; with a
/// key that is not pending, the response is unmatched and the trigger opens
/// one.
#[test]
fn a_response_is_applied_before_a_trigger_on_the_same_tick() {
    let rows = replay_both(&[
        trigger(1, 0.0),
        Tick {
            trigger: 1,
            respond: 1,
            ..record(0.5)
        },
        clock(1.2),
        clock(1.6),
        Tick {
            trigger: 2,
            respond: 2,
            ..record(1.7)
        },
    ]);
    assert_eq!(
        column(&rows, "closed"),
        ["None", "Some(1)", "None", "None", "None"]
    );
    assert_eq!(column(&rows, "conflicting"), ["false"; 5]);
    assert_eq!(
        column(&rows, "unmatched"),
        ["false", "false", "false", "false", "true"]
    );
    assert_eq!(column(&rows, "overdue"), ["[]", "[]", "[]", "[1]", "[]"]);
    assert_eq!(
        column(&rows, "pending"),
        ["[1]", "[1]", "[1]", "[1]", "[1, 2]"]
    );
}
