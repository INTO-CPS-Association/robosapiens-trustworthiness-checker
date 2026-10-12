//! Stream function instances on the runtimes that run a file.
//!
//! Dataflow is the reference runtime for stream function instances; semi-sync admits
//! them only because it produces the same results. Each case runs the checker
//! on a program of several files, under every DSRV semantics, and compares
//! every output of every tick. A runtime that does not admit instances must
//! refuse the program by name before evaluating it.

use std::path::{Path, PathBuf};
use std::process::Command;

const SEMANTICS: &[&str] = &["untimed", "typed-untimed", "gradual-typed-untimed"];

const COUNTING: &str = r#"use experimental::{functions, modules, local_streams}

// How many ticks `hit` has held on, this one included.
def count(hit: Bool) -> Int = count
where {
  before: Int = default(count[1], 0)
  count: Int = if hit then before + 1 else before
}
"#;

const TIMING: &str = r#"use experimental::{modules, functions, generics, local_streams, tagged_unions}
use std::option::*
use counting

def stats(value: Int) -> Struct<count: Int, total: Int, high: Int> = { count: count, total: total, high: high }
where {
  count: Int = default(count[1], 0) + 1
  total: Int = default(total[1], 0) + value
  before: Int = default(high[1], value)
  high: Int = if value > before then value else before
}

// Two counters, each an instance of its own.
def pair(a: Bool, b: Bool) -> Struct<a: Int, b: Int> = { a: left, b: right }
where {
  left: Int = counting::count(a)
  right: Int = counting::count(b)
}

def previous<T>(value: T) -> Option<T> = default(seen[1], None)
where {
  seen: Option<T> = Some(value)
}

def accepted<project: (Int -> Bool)>(value: Int) -> Int = count
where {
  hit: Bool = project(value)
  count: Int = default(count[1], 0) + (if hit then 1 else 0)
}
"#;

const ROOT: &str = r#"use experimental::{modules, functions, generics, local_streams, tagged_unions, pattern_matching}
mod counting
mod timing
use std::option::*
use timing

def positive(n: Int) -> Bool = n > 0

in x: Int
in hit: Bool
aux stats = timing::stats(x)
aux pair = timing::pair(hit, !hit)
aux previous = timing::previous(x)
out count: Int = stats.count
out total: Int = stats.total
out high: Int = stats.high
out hits: Int = pair.a
out misses: Int = pair.b
out positives: Int = timing::accepted::<positive>(x)
out before: Int = match(previous) { Some(v) -> v, None -> -1 }
"#;

const INPUT: &str = "\
0: x = 3
   hit = true
1: x = -1
   hit = false
2: x = 7
   hit = true
3: x = 7
   hit = true
";

/// A directory holding the program's files, removed when dropped.
struct Program(PathBuf);

impl Program {
    fn write(name: &str) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "tc-stream-function-runtimes-{name}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).expect("a scratch directory");
        for (file, text) in [
            ("root.dsrv", ROOT),
            ("counting.dsrv", COUNTING),
            ("timing.dsrv", TIMING),
            ("root.input", INPUT),
        ] {
            std::fs::write(directory.join(file), text).expect("a scratch file");
        }
        Self(directory)
    }

    fn path(&self, file: &str) -> String {
        self.0.join(file).to_string_lossy().into_owned()
    }
}

impl Drop for Program {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Run the checker, returning whether it succeeded, its sorted output lines
/// and its diagnostics.
fn check(program: &Program, runtime: &str, semantics: &str) -> (bool, Vec<String>, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
        .args([
            &program.path("root.dsrv"),
            "--input-file",
            &program.path("root.input"),
            "--output-stdout",
            "--runtime",
            runtime,
            "--semantics",
            semantics,
        ])
        .env("RUST_LOG", "error")
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .output()
        .expect("the checker runs");
    let mut lines: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_owned)
        .collect();
    lines.sort();
    (
        output.status.success(),
        lines,
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn semi_sync_agrees_with_dataflow_on_every_output_of_every_tick() {
    let program = Program::write("parity");
    for semantics in SEMANTICS {
        let (ran, dataflow, errors) = check(&program, "dataflow", semantics);
        assert!(ran, "dataflow/{semantics}: {errors}");
        for expected in [
            // Two uses of `Count`, nested in one `Pair`, keep apart.
            "hits[3] = 3",
            "misses[3] = 1",
            // The struct result, one field at a time.
            "count[3] = 4",
            "total[3] = 16",
            "high[1] = 3",
            "high[3] = 7",
            // A fixed function, and an inferred type parameter.
            "positives[3] = 3",
            "before[0] = -1",
            "before[2] = -1",
            "before[3] = 7",
        ] {
            assert!(
                dataflow.iter().any(|line| line == expected),
                "dataflow/{semantics} lacks {expected}: {dataflow:?}"
            );
        }
        let (ran, semi_sync, errors) = check(&program, "semi-sync", semantics);
        assert!(ran, "semi-sync/{semantics}: {errors}");
        assert_eq!(semi_sync, dataflow, "semi-sync/{semantics}");
    }
}

#[test]
fn async_refuses_an_instance_before_it_evaluates() {
    let program = Program::write("async");
    for semantics in SEMANTICS {
        let (ran, lines, errors) = check(&program, "async", semantics);
        assert!(!ran, "async/{semantics} ran: {lines:?}");
        assert!(lines.is_empty(), "async/{semantics} evaluated: {lines:?}");
        assert!(
            errors.contains("a stream function instance at Span")
                && errors.contains("cannot run on the async runtime")
                && errors.contains("does not support stream function instances"),
            "async/{semantics}: {errors}"
        );
    }
}

fn check_maplek_fixture(runtime: &str, semantics: &str) -> (bool, Vec<String>, String) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let output = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
        .args([
            "tests/fixtures/maplek_stream_function_slice.dsrv",
            "--input-file",
            "tests/fixtures/maplek_stream_function_slice.input",
            "--output-stdout",
            "--runtime",
            runtime,
            "--semantics",
            semantics,
        ])
        .env("RUST_LOG", "error")
        .current_dir(root)
        .output()
        .expect("the checker runs");
    let mut lines: Vec<_> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_owned)
        .collect();
    lines.sort();
    (
        output.status.success(),
        lines,
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn maplek_slice_proves_nested_history_fixed_arguments_and_instance_isolation() {
    let mut expected = vec![
        "independent_accepted[0] = true",
        "independent_accepted[1] = false",
        "independent_accepted[2] = true",
        "independent_accepted[3] = true",
        "independent_count[0] = 1",
        "independent_count[1] = 1",
        "independent_count[2] = 2",
        "independent_count[3] = 3",
        "independent_total[0] = 3",
        "independent_total[1] = 3",
        "independent_total[2] = 9",
        "independent_total[3] = 9",
        "raw_accepted[0] = true",
        "raw_accepted[1] = false",
        "raw_accepted[2] = true",
        "raw_accepted[3] = true",
        "raw_count[0] = 1",
        "raw_count[1] = 1",
        "raw_count[2] = 2",
        "raw_count[3] = 3",
        "raw_total[0] = 2",
        "raw_total[1] = 2",
        "raw_total[2] = 6",
        "raw_total[3] = 6",
        "shifted_accepted[0] = true",
        "shifted_accepted[1] = true",
        "shifted_accepted[2] = true",
        "shifted_accepted[3] = true",
        "shifted_count[0] = 1",
        "shifted_count[1] = 2",
        "shifted_count[2] = 3",
        "shifted_count[3] = 4",
        "shifted_total[0] = 4",
        "shifted_total[1] = 4",
        "shifted_total[2] = 10",
        "shifted_total[3] = 12",
    ];
    expected.sort_unstable();
    for semantics in ["typed-untimed", "gradual-typed-untimed"] {
        let (ran, dataflow, errors) = check_maplek_fixture("dataflow", semantics);
        assert!(ran, "dataflow/{semantics}: {errors}");
        assert_eq!(dataflow, expected, "dataflow/{semantics}");
        let (ran, semi_sync, errors) = check_maplek_fixture("semi-sync", semantics);
        assert!(ran, "semi-sync/{semantics}: {errors}");
        assert_eq!(semi_sync, expected, "semi-sync/{semantics}");
    }
    let (ran, output, errors) = check_maplek_fixture("async", "typed-untimed");
    assert!(!ran, "async unexpectedly ran: {output:?}");
    assert!(
        output.is_empty(),
        "async emitted before refusal: {output:?}"
    );
    assert!(
        errors.contains("does not support stream function instances"),
        "{errors}"
    );
}

#[test]
fn journal_file_drives_the_shipped_maplek_loop_with_runtime_parity() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for semantics in ["typed-untimed", "gradual-typed-untimed"] {
        let output = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
            .args([
                "tests/fixtures/rpio_loop.dsrv",
                "--input-config",
                "tests/fixtures/rpio_loop_inputs.json5",
                "--output-stdout",
                "--runtime",
                "dataflow",
                "--semantics",
                semantics,
            ])
            .env("RUST_LOG", "error")
            .current_dir(root)
            .output()
            .expect("the checker runs");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{semantics}: {stderr}");
        assert!(stdout.contains("cycle_active[2] = true"), "{stdout}");
        assert!(stdout.contains("episode_moved[13] = true"), "{stdout}");
        assert!(stdout.contains("episode_closed[29] = true"), "{stdout}");
        assert!(stdout.contains("cycle_active[29] = false"), "{stdout}");
        assert!(stdout.contains("plan_pending[14] = true"), "{stdout}");
        assert!(stdout.contains("plan_ended[18] = true"), "{stdout}");
        assert!(stdout.contains("plan_has_span[18] = true"), "{stdout}");
        assert!(stdout.contains("plan_has_span[19] = false"), "{stdout}");
        assert!(
            !stdout.contains("plan_overlapping_start[18] = true"),
            "{stdout}"
        );
        assert!(stdout.contains("swap_pending[25] = true"), "{stdout}");
        assert!(stdout.contains("swap_completed[26] = true"), "{stdout}");
        assert!(stdout.contains("swap_has_span[26] = true"), "{stdout}");
        assert!(stdout.contains("swap_has_span[27] = false"), "{stdout}");
        assert!(stdout.contains("handoff_pending[17] = true"), "{stdout}");
        assert!(stdout.contains("handoff_has_span[18] = false"), "{stdout}");
        assert!(stdout.contains("handoff_gap[19] = true"), "{stdout}");
        assert!(stdout.contains("handoff_has_span[19] = true"), "{stdout}");
        assert!(!stdout.contains("handoff_overlap[19] = true"), "{stdout}");
        assert!(stdout.contains("protocol_followed[29] = true"), "{stdout}");
        assert_eq!(
            stdout
                .lines()
                .filter(|line| line.starts_with("cycle_active["))
                .count(),
            30,
            "one complete result per source record"
        );

        let semi_sync = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
            .args([
                "tests/fixtures/rpio_loop.dsrv",
                "--input-config",
                "tests/fixtures/rpio_loop_inputs.json5",
                "--output-stdout",
                "--runtime",
                "semi-sync",
                "--semantics",
                semantics,
            ])
            .env("RUST_LOG", "error")
            .current_dir(root)
            .output()
            .expect("the checker runs");
        let semi_stderr = String::from_utf8_lossy(&semi_sync.stderr);
        assert!(semi_sync.status.success(), "{semantics}: {semi_stderr}");
        let mut dataflow_lines = stdout.lines().collect::<Vec<_>>();
        let semi_stdout = String::from_utf8_lossy(&semi_sync.stdout);
        let mut semi_lines = semi_stdout.lines().collect::<Vec<_>>();
        dataflow_lines.sort_unstable();
        semi_lines.sort_unstable();
        assert_eq!(semi_lines, dataflow_lines, "semi-sync/{semantics}");
    }
}

#[test]
fn semi_sync_matches_dataflow_for_lexical_match_calls() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for semantics in ["typed-untimed", "gradual-typed-untimed"] {
        let run = |runtime: &str| {
            let output = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
                .args([
                    "tests/fixtures/lexical_match_calls.dsrv",
                    "--input-file",
                    "tests/fixtures/lexical_match_calls.input",
                    "--output-stdout",
                    "--runtime",
                    runtime,
                    "--semantics",
                    semantics,
                ])
                .env("RUST_LOG", "error")
                .current_dir(root)
                .output()
                .expect("the checker runs");
            let error = String::from_utf8_lossy(&output.stderr);
            assert!(output.status.success(), "{runtime}/{semantics}: {error}");
            let mut lines = String::from_utf8(output.stdout)
                .expect("UTF-8 output")
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            lines.sort_unstable();
            lines
        };
        assert_eq!(run("semi-sync"), run("dataflow"), "{semantics}");
    }
}

#[test]
fn service_timing_rearms_and_ignores_a_late_old_completion() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for semantics in ["typed-untimed", "gradual-typed-untimed"] {
        let output = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
            .args([
                "tests/fixtures/rpio_service_rearm.dsrv",
                "--input-config",
                "tests/fixtures/rpio_service_rearm_inputs.json5",
                "--output-stdout",
                "--runtime",
                "dataflow",
                "--semantics",
                semantics,
            ])
            .env("RUST_LOG", "error")
            .current_dir(root)
            .output()
            .expect("the checker runs");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{semantics}: {stderr}");

        assert!(stdout.contains("pending[1] = true"), "{stdout}");
        assert!(stdout.contains("abandoned[2] = true"), "{stdout}");
        assert!(stdout.contains("pending[2] = false"), "{stdout}");
        assert!(stdout.contains("has_span[2] = true"), "{stdout}");
        assert!(stdout.contains("pending[4] = true"), "{stdout}");
        assert!(stdout.contains("pending[5] = true"), "{stdout}");
        assert!(stdout.contains("completed[5] = false"), "{stdout}");
        assert!(stdout.contains("completed[6] = true"), "{stdout}");
        assert!(stdout.contains("pending[6] = false"), "{stdout}");
        assert!(stdout.contains("has_span[6] = true"), "{stdout}");
        assert!(stdout.contains("has_span[7] = false"), "{stdout}");
    }
}

#[test]
fn clock_only_ticks_advance_pending_service_without_replaying_records() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for semantics in ["typed-untimed", "gradual-typed-untimed"] {
        let run = |runtime: &str| {
            let output = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
                .args([
                    "tests/fixtures/rpio_service_clock.dsrv",
                    "--input-config",
                    "tests/fixtures/rpio_service_clock_inputs.json5",
                    "--output-stdout",
                    "--runtime",
                    runtime,
                    "--semantics",
                    semantics,
                ])
                .env("RUST_LOG", "error")
                .current_dir(root)
                .output()
                .expect("the checker runs");
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(output.status.success(), "{runtime}/{semantics}: {stderr}");
            String::from_utf8(output.stdout).expect("UTF-8 output")
        };

        let dataflow = run("dataflow");
        for tick in [0, 1] {
            assert!(
                dataflow.contains(&format!("has_record[{tick}] = true")),
                "{dataflow}"
            );
            assert!(
                dataflow.contains(&format!("has_clock[{tick}] = false")),
                "{dataflow}"
            );
        }
        for tick in [2, 3] {
            assert!(
                dataflow.contains(&format!("has_record[{tick}] = false")),
                "{dataflow}"
            );
            assert!(
                dataflow.contains(&format!("has_clock[{tick}] = true")),
                "{dataflow}"
            );
            assert!(
                dataflow.contains(&format!("pending[{tick}] = true")),
                "{dataflow}"
            );
            assert!(
                dataflow.contains(&format!("has_span[{tick}] = true")),
                "{dataflow}"
            );
        }
        assert!(dataflow.contains("overdue[2] = false"), "{dataflow}");
        assert!(dataflow.contains("overdue[3] = true"), "{dataflow}");
        assert_eq!(
            dataflow.lines().count(),
            20,
            "exactly five outputs on four ticks"
        );

        let mut dataflow_lines = dataflow.lines().collect::<Vec<_>>();
        let semi_sync = run("semi-sync");
        let mut semi_lines = semi_sync.lines().collect::<Vec<_>>();
        dataflow_lines.sort_unstable();
        semi_lines.sort_unstable();
        assert_eq!(semi_lines, dataflow_lines, "{semantics}");
    }
}

#[test]
fn combined_terminal_and_clock_tick_closes_before_assessing_age() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for semantics in ["typed-untimed", "gradual-typed-untimed"] {
        for runtime in ["dataflow", "semi-sync"] {
            let output = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
                .args([
                    "tests/fixtures/rpio_combined_terminal_clock.dsrv",
                    "--input-file",
                    "tests/fixtures/rpio_combined_terminal_clock.input",
                    "--output-stdout",
                    "--runtime",
                    runtime,
                    "--semantics",
                    semantics,
                ])
                .env("RUST_LOG", "error")
                .current_dir(root)
                .output()
                .expect("the checker runs");
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(output.status.success(), "{runtime}/{semantics}: {stderr}");
            assert!(stdout.contains("has_record[2] = true"), "{stdout}");
            assert!(stdout.contains("has_clock[2] = true"), "{stdout}");
            assert!(stdout.contains("completed[2] = true"), "{stdout}");
            assert!(stdout.contains("pending[2] = false"), "{stdout}");
            // The terminal record closes the interval at its own instant,
            // 12, not at the step's clock sample, 14.
            assert!(
                stdout.contains("span_end[2] = Some(std::time::Instant<JournalTime>::new(12.0))"),
                "{stdout}"
            );
            assert!(stdout.contains("overdue[2] = false"), "{stdout}");
        }
    }
}

#[test]
fn handoff_reports_overlap_and_explicit_withdrawal_without_reopening() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for semantics in ["typed-untimed", "gradual-typed-untimed"] {
        let run = |runtime: &str| {
            let output = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
                .args([
                    "tests/fixtures/rpio_handoff_terminals.dsrv",
                    "--input-config",
                    "tests/fixtures/rpio_handoff_terminals_inputs.json5",
                    "--output-stdout",
                    "--runtime",
                    runtime,
                    "--semantics",
                    semantics,
                ])
                .env("RUST_LOG", "error")
                .current_dir(root)
                .output()
                .expect("the checker runs");
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(output.status.success(), "{runtime}/{semantics}: {stderr}");
            String::from_utf8(output.stdout).expect("UTF-8 output")
        };
        let dataflow = run("dataflow");
        assert!(dataflow.contains("pending[1] = true"), "{dataflow}");
        assert!(dataflow.contains("overlap[2] = true"), "{dataflow}");
        assert!(dataflow.contains("pending[2] = false"), "{dataflow}");
        assert!(dataflow.contains("pending[3] = false"), "{dataflow}");
        assert!(dataflow.contains("pending[5] = true"), "{dataflow}");
        assert!(
            dataflow.contains("withdrawn_terminal[6] = true"),
            "{dataflow}"
        );
        assert!(dataflow.contains("pending[6] = false"), "{dataflow}");
        assert!(dataflow.contains("has_span[7] = false"), "{dataflow}");

        let mut dataflow_lines = dataflow.lines().collect::<Vec<_>>();
        let semi_sync = run("semi-sync");
        let mut semi_lines = semi_sync.lines().collect::<Vec<_>>();
        dataflow_lines.sort_unstable();
        semi_lines.sort_unstable();
        assert_eq!(semi_lines, dataflow_lines, "{semantics}");
    }
}
