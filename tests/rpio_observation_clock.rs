//! The RPIO journal replay source with its clock samples, end to end
//! through the CLI.
//!
//! The source's provenance file names the origins of each logical input
//! tick, and a finding names the stdout row it was printed on. An evaluator
//! joins the two, so the tick a provenance line names must be the row its
//! outputs print on, in both runtimes.

use std::collections::BTreeMap;
use std::process::Command;

use serde_json::Value as Json;

const ROOT: &str = env!("CARGO_MANIFEST_DIR");

fn temp_path(suffix: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "tc-observation-clock-{}-{}{suffix}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ))
}

/// Run the fixture model with provenance, returning the stdout rows by
/// output name and the provenance lines in order.
fn run(runtime: &str, semantics: &str) -> (BTreeMap<String, BTreeMap<usize, String>>, Vec<Json>) {
    let provenance = temp_path(".provenance.jsonl");
    let config = temp_path(".json5");
    std::fs::write(
        &config,
        format!(
            r#"{{ sources: {{ journal: {{
                kind: "rpio-observation-clock-file",
                variable: "observation",
                domain: "JournalTime",
                journal_clock: "fixture.journal",
                files: ["fixtures/rpio/service-rearm.jsonl"],
                capture_checks: [{{records:8,sha256:"468f8250dcbe61efbda9b3fbfff5435631508958eca5e6933fc060f4d5802fb8",journal_clock:"fixture.journal"}}],
                clock_samples: [
                    {{time: 2.0, through: 2}},
                    {{time: 20.0, through: 4, with_record: true}},
                    {{time: 21.0, through: 5}},
                ],
                provenance: {},
            }} }} }}"#,
            serde_json::to_string(&provenance.display().to_string()).unwrap()
        ),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
        .args([
            "tests/fixtures/rpio_observation_clock.dsrv",
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
    let lines = std::fs::read_to_string(&provenance).unwrap_or_default();
    let _ = std::fs::remove_file(&provenance);
    assert!(
        output.status.success(),
        "{runtime}/{semantics}: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut rows = BTreeMap::<String, BTreeMap<usize, String>>::new();
    for line in String::from_utf8(output.stdout).unwrap().lines() {
        let (key, value) = line.split_once(" = ").expect("an output assignment");
        let (name, row) = key.split_once('[').expect("a row-indexed output");
        let row = row.trim_end_matches(']').parse().unwrap();
        assert!(
            rows.entry(name.to_owned())
                .or_default()
                .insert(row, value.to_owned())
                .is_none(),
            "one value per output per row"
        );
    }
    let provenance = lines
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    (rows, provenance)
}

fn instant(time: f64) -> String {
    format!("std::time::Instant<JournalTime>::new({time:?})")
}

/// AC-TS-6: input logical tick k is output row k. Every provenance line's
/// origins appear, with their instants, on the row of the same number.
#[test]
fn provenance_tick_is_the_stdout_row_in_both_runtimes() {
    for semantics in ["typed-untimed", "gradual-typed-untimed"] {
        let mut outputs = Vec::new();
        for runtime in ["dataflow", "semi-sync"] {
            let (rows, provenance) = run(runtime, semantics);
            assert_eq!(provenance.len(), 10, "{runtime}: one line per step");
            for (k, line) in provenance.iter().enumerate() {
                assert_eq!(line["tick"], k, "{runtime}: ticks are numbered from zero");
                let origins = line["origins"].as_array().unwrap();
                let has = |member: &str| origins.iter().any(|origin| origin["member"] == member);
                assert_eq!(
                    rows["recorded"][&k],
                    has("journal").to_string(),
                    "{runtime}/{semantics} row {k}"
                );
                assert_eq!(
                    rows["clocked"][&k],
                    has("clock").to_string(),
                    "{runtime}/{semantics} row {k}"
                );
                for origin in origins {
                    let time = origin["time"].as_f64().unwrap();
                    if origin["member"] == "clock" {
                        assert_eq!(
                            origin["clock"], "fixture.journal",
                            "replay clock retains its captured identity"
                        );
                    }
                    assert!(
                        rows["instants"][&k].contains(&instant(time)),
                        "{runtime}/{semantics} row {k} lacks {origin}: {}",
                        rows["instants"][&k]
                    );
                }
            }
            for column in rows.values() {
                assert_eq!(
                    column.keys().copied().collect::<Vec<_>>(),
                    (0..provenance.len()).collect::<Vec<_>>(),
                    "{runtime}/{semantics}: one row per tick"
                );
            }
            outputs.push(rows);
        }
        assert_eq!(outputs[0], outputs[1], "{semantics}: runtimes agree");
    }
}

/// A sample covering the final record follows it, after every record; a
/// replay without a provenance path writes nothing else.
#[test]
fn a_sample_through_the_final_record_follows_it() {
    let config = temp_path(".json5");
    std::fs::write(
        &config,
        r#"{ sources: { journal: {
            kind: "rpio-observation-clock-file",
            variable: "observation",
            domain: "JournalTime",
            journal_clock: "fixture.journal",
            files: ["fixtures/rpio/service-pending.jsonl"],
            capture_checks: [{records:2,sha256:"61e979d9419a12f52419c334b94c8cc3b1cb2abf480d906efea4fffb57ea6460",journal_clock:"fixture.journal"}],
            clock_samples: [{time: 12.0, through: 2}],
        } } }"#,
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
        .args([
            "tests/fixtures/rpio_observation_clock.dsrv",
            "--input-config",
            &config.display().to_string(),
            "--output-stdout",
        ])
        .env("RUST_LOG", "error")
        .current_dir(ROOT)
        .output()
        .expect("the checker runs");
    let _ = std::fs::remove_file(&config);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    for expected in [
        "recorded[0] = true",
        "clocked[0] = false",
        "recorded[1] = true",
        "recorded[2] = false",
        "clocked[2] = true",
    ] {
        assert!(stdout.contains(expected), "{expected}: {stdout}");
    }
    assert_eq!(stdout.lines().count(), 9);
}

/// A schedule that does not fit the declared interval stops the checker
/// before it evaluates anything, and says which sample is wrong.
#[test]
fn a_schedule_error_is_reported_at_startup() {
    let config = temp_path(".json5");
    std::fs::write(
        &config,
        r#"{ sources: { journal: {
            kind: "rpio-observation-clock-file",
            variable: "observation",
            domain: "JournalTime",
            journal_clock: "fixture.journal",
            files: ["fixtures/rpio/service-pending.jsonl"],
            capture_checks: [{records:2,sha256:"61e979d9419a12f52419c334b94c8cc3b1cb2abf480d906efea4fffb57ea6460",journal_clock:"fixture.journal"}],
            clock_samples: [{time: 5.0, through: 1}, {time: 6.0, through: 3}],
        } } }"#,
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
        .args([
            "tests/fixtures/rpio_observation_clock.dsrv",
            "--input-config",
            &config.display().to_string(),
            "--output-stdout",
        ])
        .env("RUST_LOG", "error")
        .current_dir(ROOT)
        .output()
        .expect("the checker runs");
    let _ = std::fs::remove_file(&config);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(
            "clock sample 2 covers records through 3, outside the replay interval after 0 through 2"
        ),
        "{stderr}"
    );
}

// ---------------------------------------------------------------------------
// Clock schedule files (review 30): a file schedule is checked whole before
// any tick in one bounded pass, then streamed, so its length costs no replay
// memory; each line is bounded.
// ---------------------------------------------------------------------------

/// The actual nominal capture, for schedules longer than any test journal.
fn nominal() -> tc_testutils::journal::app::Trial {
    // The measured classic producer capture, not a typed logical fixture.
    let dir = std::path::Path::new(ROOT).join("fixtures/rpio");
    let bytes = std::fs::read(dir.join("nominal.jsonl")).unwrap();
    let lines: Vec<_> = String::from_utf8(bytes.clone())
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    use sha2::{Digest, Sha256};
    tc_testutils::journal::app::Trial {
        name: "classic nominal".into(),
        dir,
        meta: serde_json::json!({"capture_file":"nominal.jsonl","journal_clock":"fixture.journal",
            "observation_epoch":"fixture.capture","start_after_seq":0,"through_seq":lines.len(),
            "record_count":lines.len(),"capture_sha256":format!("{:x}",Sha256::digest(&bytes))}),
        lines,
    }
}

/// The replay of `trial` with the clock sample file `schedule`.
fn scheduled_config(trial: &tc_testutils::journal::app::Trial, schedule: &std::path::Path) -> Json {
    serde_json::json!({"sources": {"journal": {
        "kind": "rpio-observation-clock-file",
        "variable": "observation",
        "domain": "JournalTime",
        "journal_clock": trial.clock_label(),
        "files": [trial.dir.join(trial.meta["capture_file"].as_str().unwrap()).display().to_string()],
        "capture_checks": [{"records":trial.lines.len(),"sha256":trial.meta["capture_sha256"],
            "journal_clock":trial.clock_label()}],
        "clock_samples": schedule.display().to_string(),
    }}})
}

/// `count` samples spread evenly, in order, over every position of the
/// interval from `after` through `through`, at increasing times.
fn spread_schedule(count: u64, after: u64, through: u64) -> Vec<(f64, u64)> {
    (0..count)
        .map(|index| {
            (
                1_000.0 + index as f64 * 0.001,
                after + index * (through - after + 1) / count,
            )
        })
        .collect()
}

fn schedule_lines(samples: &[(f64, u64)]) -> String {
    samples
        .iter()
        .map(|(time, through)| format!("{{\"time\":{time:?},\"through\":{through}}}\n"))
        .collect()
}

/// One input tick: the record's position and the clock sample's time.
type ReplayTick = (Option<u64>, Option<f64>);

fn replay_tick(
    batch: &trustworthiness_checker::InputBatch<trustworthiness_checker::Value>,
    admitted: &mut u64,
) -> ReplayTick {
    use trustworthiness_checker::Value;
    let some = |value: &Value| -> Option<Value> {
        match value {
            Value::Union(option) if option.tag() == "Some" => option.payload().cloned(),
            _ => None,
        }
    };
    let field = |value: &Value, name: &str| -> Value {
        let Value::Map(fields) = value else {
            panic!("expected a struct, got {value}")
        };
        fields[name].clone()
    };
    let observation = &batch.updates().next().expect("one update").value;
    // The classic record has no processing-position field. Count admitted
    // record ticks independently; Knowledge's write sequence is unrelated.
    let record = some(&field(observation, "record")).map(|_| {
        *admitted += 1;
        *admitted
    });
    let clock = some(&field(observation, "clock")).map(|sample| match field(&sample, "time") {
        Value::Newtype(inner) => match inner.value() {
            Value::Float(time) => *time,
            other => panic!("an instant: {other}"),
        },
        other => panic!("an instant: {other}"),
    });
    (record, clock)
}

/// The ticks a replay of positions `after + 1 ..= through` with `schedule`
/// delivers: each record, then each sample covering records through it, in
/// schedule order; samples through `after` come before the first record.
fn expected_ticks(after: u64, through: u64, schedule: &[(f64, u64)]) -> Vec<ReplayTick> {
    let mut ticks = Vec::new();
    let mut samples = schedule.iter().peekable();
    for seq in after..=through {
        if seq > after {
            ticks.push((Some(seq), None));
        }
        while let Some((time, _)) = samples.next_if(|(_, covered)| *covered == seq) {
            ticks.push((None, Some(*time)));
        }
    }
    assert!(samples.next().is_none());
    ticks
}

/// A schedule of 200,000 samples over the actual nominal capture replays in
/// order through the configured input pipeline: every record and every
/// sample, each sample after the records it covers.
#[macro_rules_attribute::apply(trustworthiness_checker::async_test)]
async fn a_long_clock_schedule_file_replays_every_sample_in_order(
    executor: std::rc::Rc<smol::LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    use futures::StreamExt;
    use trustworthiness_checker::io::{InputConfigFile, InputPipeline, InputSources};

    let trial = nominal();
    let schedule = spread_schedule(200_000, trial.after(), trial.through());
    let path = temp_path(".clock.jsonl");
    std::fs::write(&path, schedule_lines(&schedule))?;
    let provenance_path = temp_path(".provenance.jsonl");
    let mut config = scheduled_config(&trial, &path);
    config["sources"]["journal"]["provenance"] = serde_json::json!(provenance_path);
    let config: InputConfigFile = serde_json::from_value(config)?;
    let sources =
        InputSources::from_config(config, executor.clone(), None, None, Default::default())?;
    let mut input = InputPipeline::from_sources(sources)
        .with_executor(executor)
        .build(std::collections::BTreeSet::from([
            trustworthiness_checker::VarName::new("observation"),
        ]))
        .await?;
    let mut ticks = Vec::new();
    let mut admitted = trial.after();
    while let Some(batch) = input.next().await {
        ticks.push(replay_tick(&batch?, &mut admitted));
    }
    let _ = std::fs::remove_file(&path);
    let expected = expected_ticks(trial.after(), trial.through(), &schedule);
    assert_eq!(ticks.len(), expected.len());
    assert_eq!(ticks, expected);
    // Independently verify actual physical capture cursors. Counting record
    // ticks must not conceal a duplicated, missing or reordered capture row.
    let origins = std::fs::read_to_string(&provenance_path)?;
    let positions: Vec<u64> = origins
        .lines()
        .flat_map(|line| {
            let tick: Json = serde_json::from_str(line).unwrap();
            tick["origins"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|origin| origin["member"] == "journal")
                .map(|origin| {
                    origin["cursor"]
                        .as_str()
                        .unwrap()
                        .rsplit_once(':')
                        .unwrap()
                        .1
                        .parse()
                        .unwrap()
                })
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(positions, (1..=trial.through()).collect::<Vec<_>>());
    assert_eq!(origins.lines().count(), ticks.len());
    let _ = std::fs::remove_file(provenance_path);
    Ok(())
}

/// Run the replay of `trial` with clock sample file contents `schedule`,
/// returning the checker's exit status, stdout and stderr.
fn run_with_schedule(
    trial: &tc_testutils::journal::app::Trial,
    schedule: &[u8],
) -> (bool, String, String) {
    let path = temp_path(".clock.jsonl");
    std::fs::write(&path, schedule).unwrap();
    let config = temp_path(".json");
    std::fs::write(
        &config,
        serde_json::to_vec(&scheduled_config(trial, &path)).unwrap(),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
        .args([
            "tests/fixtures/rpio_observation_clock.dsrv",
            "--input-config",
            &config.display().to_string(),
            "--output-stdout",
        ])
        .env("RUST_LOG", "error")
        .current_dir(ROOT)
        .output()
        .expect("the checker runs");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&config);
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// A schedule file is refused whole at startup, before any output, by the
/// line that breaks it, however late that line comes: a sample out of order
/// after a long valid prefix, and a malformed line. A long line is read like
/// any other.
#[test]
fn a_late_bad_clock_schedule_line_is_refused_at_startup_by_its_line() {
    let trial = nominal();
    let (after, through) = (trial.after(), trial.through());
    let valid = spread_schedule(100_000, after, through);
    let prefix = schedule_lines(&valid);
    // The last valid sample covers `through`; one covering less follows it.
    let backwards = format!("{prefix}{{\"time\":5000.0,\"through\":{}}}\n", through - 1);
    let malformed = format!("{prefix}{{\"time\":5000.0,\"through\":\n");
    let padded = |bytes: usize| {
        let sample = format!("{{\"time\":5000.0,\"through\":{through}}}");
        format!("{sample}{}", " ".repeat(bytes - sample.len()))
    };
    let line = valid.len() + 1;
    for (what, schedule, message) in [
        (
            "a sample out of order",
            backwards,
            format!(
                "line {line} covers records through {}, before the previous sample's {through}",
                through - 1
            ),
        ),
        ("a malformed line", malformed, format!("line {line}")),
    ] {
        let (success, stdout, stderr) = run_with_schedule(&trial, schedule.as_bytes());
        assert!(!success, "{what}");
        assert!(stdout.is_empty(), "{what}: nothing is evaluated: {stdout}");
        assert!(stderr.contains(&message), "{what}: {stderr}");
    }

    // A valid JSON sample beyond the documented 1 MiB line bound is
    // rejected during whole-file validation, even after a large valid prefix.
    let oversized = format!("{prefix}{}\n", padded(1_048_577));
    let (success, stdout, stderr) = run_with_schedule(&trial, oversized.as_bytes());
    assert!(!success);
    assert!(stdout.is_empty(), "oversized late line evaluates nothing");
    assert!(stderr.contains(&format!("line {line}")), "{stderr}");
    assert!(stderr.contains("exceeds 1048576 bytes"), "{stderr}");

    // A long line, with or without its final LF, is read.
    for ending in ["\n", ""] {
        let schedule = format!("{}{ending}", padded(100_000));
        let (success, stdout, stderr) = run_with_schedule(&trial, schedule.as_bytes());
        assert!(success, "{stderr}");
        assert_eq!(
            stdout.lines().filter(|l| l.starts_with("clocked[")).count() as u64,
            through - after + 1,
            "every record and the one sample"
        );
    }
}

/// Review M2 / clarification 14: live samples are the Redis server's `TIME`,
/// whose identity is `redis.server.time`. A live source that samples while
/// bound to another journal clock is refused when its configuration is
/// checked, and the CLI stops before reading anything; without sampling, or
/// with the Redis clock, the binding is accepted.
#[test]
fn live_sampling_requires_the_redis_server_clock() {
    use trustworthiness_checker::io::InputConfigFile;

    let live = |clock: &str, period: Option<u64>| -> Json {
        let mut source = serde_json::json!({
            "kind": "rpio-observation-clock",
            "port": 1,
            "variable": "observation",
            "domain": "JournalTime",
            "journal_clock": clock,
            "phase_stream": "trial:phase",
            "knowledge_stream": "trial:knowledge",
        });
        if let Some(period) = period {
            source["clock_period_ms"] = serde_json::json!(period);
        }
        serde_json::json!({"sources": {"journal": source}})
    };
    let check = |config: Json| {
        serde_json::from_value::<InputConfigFile>(config)
            .unwrap()
            .validate()
    };
    assert!(check(live("redis.server.time", Some(50))).is_ok());
    assert!(check(live("python:time.time_ns", None)).is_ok());
    let error = check(live("python:time.time_ns", Some(50))).unwrap_err();
    assert!(
        format!("{error:#}").contains("samples the Redis server clock (`redis.server.time`)"),
        "{error:#}"
    );

    let config = temp_path(".json");
    std::fs::write(
        &config,
        serde_json::to_vec(&live("python:time.time_ns", Some(50))).unwrap(),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
        .args([
            "tests/fixtures/rpio_observation_clock.dsrv",
            "--input-config",
            &config.display().to_string(),
            "--output-stdout",
        ])
        .env("RUST_LOG", "error")
        .current_dir(ROOT)
        .output()
        .expect("the checker runs");
    let _ = std::fs::remove_file(&config);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("but its journal_clock is `python:time.time_ns`"),
        "{stderr}"
    );
}
