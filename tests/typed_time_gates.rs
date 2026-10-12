//! The typed timing libraries' acceptance gates, through the CLI under both
//! runtimes: library-owned presence (AC-TS-5, G1), the zero-hold "now"
//! (TS-3d) and forward-only interval predicates (AC-TS-3d).
//!
//! `tests/fixtures/rpio_presence_gate.dsrv` reads the shipped RPIO
//! libraries over the ordered journal source. Its outputs are either retained
//! state, which a tick without a record must leave as it was, or what the
//! tick observed. Each scenario replays the frozen native journal
//! `fixtures/rpio/nominal.jsonl` with clock samples scheduled between its
//! records; the record at row `k` of a journal-only prefix is on physical
//! line `k + 1`. Its Knowledge records occupy the lines and instants where the
//! archived `tests/fixtures/journal/lab/outside-writes.jsonl` wrote outside
//! callbacks, so every phase record keeps its row and instant.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;

const ROOT: &str = env!("CARGO_MANIFEST_DIR");
const MODEL: &str = "tests/fixtures/rpio_presence_gate.dsrv";
const JOURNAL: &str = "fixtures/rpio/nominal.jsonl";
/// The frozen capture: its record count, bytes and clock label.
const RECORDS: usize = 30;
const SHA256: &str = "759bacae7ec999729d5e83e72f942b5e4cc795c12d242f9e1080f9e93fc669d7";
const JOURNAL_CLOCK: &str = "fixture.journal";

/// Outputs that hold state across ticks. `previous_phase_time` is not one:
/// it names the phase record before the current tick, which on a tick after
/// a phase record is that record.
const STATE: [&str; 7] = [
    "cycle",
    "episode",
    "run",
    "monitor_window",
    "window_ref",
    "plan_pending",
    "records",
];

const RUNTIMES: [&str; 2] = ["dataflow", "semi-sync"];
const SEMANTICS: [&str; 2] = ["typed-untimed", "gradual-typed-untimed"];

type Rows = BTreeMap<String, Vec<String>>;

fn temp_path(suffix: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "tc-presence-gate-{}-{}{suffix}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ))
}

/// A step that takes the next tick of the named members.
#[derive(Clone, Copy)]
enum Step {
    J,
    C,
    JC,
}

/// The replay clock samples a step schedule stands for: a clock step after
/// `k` records is a sample through `k`; a combined step joins record `k + 1`.
fn samples(clock_values: &[f64], schedule: &[(usize, Step)]) -> (String, usize) {
    let mut records = 0;
    let mut clocks = clock_values.iter();
    let mut samples = Vec::new();
    for step in schedule
        .iter()
        .flat_map(|(count, step)| std::iter::repeat_n(*step, *count))
    {
        match step {
            Step::J => records += 1,
            Step::C => samples.push(format!(
                "{{time: {:?}, through: {records}}}",
                clocks.next().unwrap()
            )),
            Step::JC => {
                records += 1;
                samples.push(format!(
                    "{{time: {:?}, through: {records}, with_record: true}}",
                    clocks.next().unwrap()
                ));
            }
        }
    }
    (samples.join(", "), records)
}

/// Replay `journal` with `clock_values` scheduled by `schedule`, returning
/// each output's value on every row.
fn replay(
    journal: &str,
    clock_values: &[f64],
    schedule: &[(usize, Step)],
    runtime: &str,
    semantics: &str,
) -> Rows {
    let config = temp_path(".json5");
    let (samples, through) = samples(clock_values, schedule);
    assert_eq!(
        through, RECORDS,
        "a schedule replays the whole frozen capture"
    );
    std::fs::write(
        &config,
        format!(
            r#"{{ sources: {{ journal: {{
                kind: "rpio-observation-clock-file",
                variable: "native_observation",
                journal_clock: "{JOURNAL_CLOCK}",
                files: [{}],
                capture_checks: [{{records: {RECORDS}, sha256: "{SHA256}", journal_clock: "{JOURNAL_CLOCK}"}}],
                clock_samples: [{samples}],
            }} }} }}"#,
            serde_json::to_string(journal).unwrap(),
        ),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
        .args([
            MODEL,
            "--input-config",
            &config.display().to_string(),
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
    let _ = std::fs::remove_file(&config);
    assert!(
        output.status.success(),
        "{runtime}/{semantics}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut columns = BTreeMap::<String, BTreeMap<usize, String>>::new();
    for line in String::from_utf8(output.stdout).unwrap().lines() {
        let (key, value) = line.split_once(" = ").expect("an output assignment");
        let (name, row) = key.split_once('[').expect("a row-indexed output");
        let row = row.trim_end_matches(']').parse().unwrap();
        assert!(
            columns
                .entry(name.to_owned())
                .or_default()
                .insert(row, value.to_owned())
                .is_none(),
            "one value per output per row"
        );
    }
    let ticks = schedule.iter().map(|(count, _)| count).sum::<usize>();
    columns
        .into_iter()
        .map(|(name, values)| {
            assert_eq!(
                values.keys().copied().collect::<Vec<_>>(),
                (0..ticks).collect::<Vec<_>>(),
                "{name}: one row per step"
            );
            (name, values.into_values().collect())
        })
        .collect()
}

/// Replay under both runtimes, which must agree on every row, once per
/// checked semantics.
fn replay_both(journal: &str, clock_values: &[f64], schedule: &[(usize, Step)]) -> Vec<Rows> {
    SEMANTICS
        .iter()
        .map(|semantics| {
            let [dataflow, semi_sync] =
                RUNTIMES.map(|runtime| replay(journal, clock_values, schedule, runtime, semantics));
            assert_eq!(dataflow, semi_sync, "{semantics}: the runtimes agree");
            dataflow
        })
        .collect()
}

fn instant(time: f64) -> String {
    format!("std::time::Instant<JournalTime>::new({time:?})")
}

fn interval(start: f64, end: f64) -> String {
    format!(
        "Some(Map(\"end\": {}, \"start\": {}))",
        instant(end),
        instant(start)
    )
}

/// The retained Knowledge evidence on `tick` is present: the Monitor's Read
/// of the entropy window and the newest window both name write 11, so holding
/// them across clock-only ticks is not satisfied by an absent value.
fn assert_knowledge_state_present(rows: &Rows, tick: usize) {
    for name in ["monitor_window", "window_ref"] {
        assert!(
            rows[name][tick].starts_with("Some(")
                && rows[name][tick].contains("\"global_write_sequence\": 11"),
            "{name} on tick {tick}: {}",
            rows[name][tick]
        );
    }
}

/// Clock-only ticks `ticks`, after the record on tick `after`, change no
/// retained state and no transition, while `now` and the ages advance.
fn assert_clock_ticks_hold_state(rows: &Rows, after: usize, ticks: &[usize], clocks: &[f64]) {
    assert_knowledge_state_present(rows, after);
    for name in STATE {
        for &tick in ticks {
            assert_eq!(
                rows[name][tick], rows[name][after],
                "{name} on clock-only tick {tick}"
            );
        }
    }
    for (&tick, &time) in ticks.iter().zip(clocks) {
        assert_eq!(rows["record"][tick], "None", "tick {tick}");
        assert!(rows["clock"][tick].starts_with("Some("), "tick {tick}");
        assert_eq!(rows["now"][tick], format!("Some({})", instant(time)));
        assert_eq!(rows["cycle_step"][tick], "Stayed", "tick {tick}");
        assert_eq!(rows["episode_step"][tick], "Stayed", "tick {tick}");
        assert_eq!(rows["plan_ended"][tick], "false", "tick {tick}");
    }
}

/// Criterion 1: after an Event, three clock-only ticks leave the retained
/// state as it was, and the pending Plan activation ages.
#[test]
fn clock_only_ticks_after_an_event_hold_state_while_ages_advance() {
    // Records 0-17 (17 is Plan a3's Event at 3.2), three samples, the rest.
    let clocks = [3.22, 3.24, 3.26];
    for rows in replay_both(
        JOURNAL,
        &clocks,
        &[(18, Step::J), (3, Step::C), (12, Step::J)],
    ) {
        assert!(
            rows["record"][17].contains("step\": Event("),
            "{}",
            rows["record"][17]
        );
        assert_clock_ticks_hold_state(&rows, 17, &[18, 19, 20], &clocks);
        assert_eq!(rows["plan_pending"][17], "true");
        for (tick, time) in [(17, 3.2), (18, 3.22), (19, 3.24), (20, 3.26)] {
            assert_eq!(rows["plan_span"][tick], interval(3.0, time), "tick {tick}");
            assert_eq!(rows["role_span"][tick], interval(3.0, time), "tick {tick}");
        }
    }
}

/// Criterion 2: after an End, the same holds, and no terminal interval is
/// fabricated: the ended activation has no age and nothing ends again.
#[test]
fn clock_only_ticks_after_an_end_fabricate_no_terminal_interval() {
    // Records 0-18 (18 is Plan a3's End at 3.3), three samples, the rest.
    let clocks = [3.32, 3.34, 3.36];
    for rows in replay_both(
        JOURNAL,
        &clocks,
        &[(19, Step::J), (3, Step::C), (11, Step::J)],
    ) {
        assert!(
            rows["record"][18].contains("step\": End("),
            "{}",
            rows["record"][18]
        );
        assert_eq!(rows["plan_ended"][18], "true");
        assert_eq!(rows["plan_span"][18], interval(3.0, 3.3));
        assert_eq!(rows["plan_pending"][18], "false");
        assert_clock_ticks_hold_state(&rows, 18, &[19, 20, 21], &clocks);
        for (tick, time) in [(19, 3.32), (20, 3.34), (21, 3.36)] {
            assert_eq!(rows["plan_span"][tick], "None", "tick {tick}");
            // The plan awaits legitimation from its End at 3.3.
            assert_eq!(rows["validation_span"][tick], interval(3.3, time));
        }
    }
}

// Criterion 3 of the version 2 gates (an omitted record is never observed
// while the samples around it keep their identity) is obsolete: in v3 an
// omitted position is a gap, an input error before any later record or
// clock. `src/io/rpio/source.rs` (`a_gap_stops_input_before_later_records_and_clocks`)
// and `tests/screw_detection_roots.rs` (SHARD deletion) cover it.

/// Criterion 4: a combined `[J, C]` step is one tick carrying both, and the
/// record is processed exactly once.
#[test]
fn a_combined_step_is_one_tick_and_processes_its_record_once() {
    // Record 29 is Execute a5's End at 5.4, which closes the episode.
    let split = replay_both(JOURNAL, &[5.45], &[(30, Step::J), (1, Step::C)]);
    let combined = replay_both(JOURNAL, &[5.45], &[(29, Step::J), (1, Step::JC)]);
    for (split, combined) in split.iter().zip(&combined) {
        assert_eq!(combined["record"][29], split["record"][29]);
        assert_eq!(combined["clock"][29], split["clock"][30]);
        assert_eq!(combined["records"][29], "30");
        for rows in [split, combined] {
            let closed = rows["episode_step"]
                .iter()
                .filter(|step| step.starts_with("Closed("))
                .count();
            assert_eq!(closed, 1, "the episode closes once");
        }
        assert_knowledge_state_present(combined, 29);
        assert_knowledge_state_present(split, 30);
        for name in STATE {
            assert_eq!(combined[name][29], split[name][30], "{name}");
        }
        assert_eq!(combined["now"][29], format!("Some({})", instant(5.45)));
    }
}

/// TS-3d: a record instant below "now" is admitted with its own time. The
/// interval it closes ends there, while an age measured on the same tick
/// runs to "now", which never regresses.
#[test]
fn a_record_below_now_keeps_its_own_instant_and_now_holds() {
    // Records 0-17, a sample at 3.5, then Plan a3's End at 3.3 (record 18).
    for rows in replay_both(
        JOURNAL,
        &[3.5],
        &[(18, Step::J), (1, Step::C), (12, Step::J)],
    ) {
        assert_eq!(rows["now"][18], format!("Some({})", instant(3.5)));
        assert!(
            rows["record"][19].contains("step\": End("),
            "{}",
            rows["record"][19]
        );
        assert_eq!(rows["now"][19], format!("Some({})", instant(3.5)));
        assert_eq!(rows["plan_ended"][19], "true");
        assert_eq!(rows["plan_span"][19], interval(3.0, 3.3));
        // The Plan role is still the cycle's: its age runs to "now".
        assert_eq!(rows["cycle_step"][19], "Stayed");
        assert_eq!(rows["role_span"][19], interval(3.0, 3.5));
        assert_eq!(
            rows["previous_phase_time"][20],
            format!("Some({})", instant(3.3))
        );
        // A Knowledge record's admission time advances "now" like any
        // record's (contract clarifications 5).
        assert!(
            rows["record"][22].contains("Knowledge"),
            "{}",
            rows["record"][22]
        );
        assert_eq!(rows["now"][22], format!("Some({})", instant(4.15)));
    }
}

/// AC-TS-3d: `between` is total and `duration` signed; a backwards interval
/// satisfies neither `exceeds` nor `shorter_than`, `is_backwards` reports
/// it, and nothing fails.
#[test]
fn a_backwards_interval_satisfies_neither_predicate_and_nothing_fails() {
    for semantics in SEMANTICS {
        let [dataflow, semi_sync] = RUNTIMES.map(|runtime| {
            let output = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
                .args([
                    "tests/fixtures/timing_intervals.dsrv",
                    "--input-file",
                    "tests/fixtures/timing_intervals.input",
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
            assert!(
                output.status.success(),
                "{runtime}/{semantics}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let mut lines = String::from_utf8(output.stdout)
                .unwrap()
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            lines.sort_unstable();
            lines
        });
        assert_eq!(dataflow, semi_sync, "{semantics}: the runtimes agree");
        let duration = |seconds: f64| format!("std::time::Duration<JournalTime>::new({seconds:?})");
        // [start, end]: forward and long, forward and short, backwards by
        // 2 s, backwards by 0.5 s, and empty; the limit is 1 s.
        for (tick, backwards, exceeds, shorter, length) in [
            (0, false, true, false, 2.0),
            (1, false, false, true, 0.5),
            (2, true, false, false, -2.0),
            (3, true, false, false, -0.5),
            (4, false, false, true, 0.0),
        ] {
            for expected in [
                format!("backwards[{tick}] = {backwards}"),
                format!("exceeds[{tick}] = {exceeds}"),
                format!("shorter[{tick}] = {shorter}"),
                format!("duration[{tick}] = {}", duration(length)),
            ] {
                assert!(dataflow.contains(&expected), "{semantics}: {expected}");
            }
        }
    }
}
