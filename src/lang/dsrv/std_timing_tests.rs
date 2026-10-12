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
