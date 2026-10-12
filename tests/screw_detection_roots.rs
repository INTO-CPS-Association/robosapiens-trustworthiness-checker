//! The ScrewDetection paper roots on the actual captures, through the CLI.
//!
//! Each root in `examples/screw-detection` reads one ordered journal input.
//! The tests replay each verified capture under the epoch and clock it was
//! captured with, and state what each root must report from the scenario's
//! description and the root's own text: correct trials raise nothing, fault
//! trials raise their family on the records that show the fault, PV-A and
//! PV-B print the same trace, and SHARD-style mutations of a capture are
//! either input errors or visible findings, never a crash or silence.

use std::path::PathBuf;

use serde_json::{Value as Json, json};
use tc_testutils::journal::app::{Checker, Journal, NativeCapture, Run, Scratch, Trial, shape};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn trial(name: &str) -> Trial {
    Trial::load(&root().join("tests/fixtures/journal/app"), name)
}

fn spec(name: &str) -> PathBuf {
    root().join(format!("examples/screw-detection/{name}.dsrv"))
}

/// Run `root_name` over `journal` under `trial`'s epoch and clock.
fn run_root(root_name: &str, trial: &Trial, journal: &Journal, runtime: &str) -> Run {
    let scratch = Scratch::new("roots");
    let capture = native_capture(trial, journal);
    let file = scratch.write("journal.jsonl", &capture.bytes);
    let config = native_config(
        trial,
        &file,
        &scratch.file("provenance.jsonl"),
        &capture,
        true,
    );
    stage_modules(trial, scratch.path());
    let mut run = Checker {
        binary: env!("CARGO_BIN_EXE_trustworthiness_checker").into(),
        cwd: root(),
    }
    .run(
        &scratch.file(&format!("{root_name}.dsrv")),
        &config,
        runtime,
        trial.after(),
    );
    capture.map_run(&mut run);
    run
}

fn native_capture(trial: &Trial, journal: &Journal) -> NativeCapture {
    let schema: Json = serde_json::from_slice(
        &std::fs::read(root().join("examples/screw-detection/observation_schema.json")).unwrap(),
    )
    .unwrap();
    NativeCapture::from_archive(journal, &schema, trial.epoch())
}

fn stage_modules(trial: &Trial, dir: &std::path::Path) {
    for file in std::fs::read_dir(root().join("examples/screw-detection")).unwrap() {
        let file = file.unwrap().path();
        if file.extension().is_some_and(|ext| ext == "dsrv") {
            std::fs::copy(&file, dir.join(file.file_name().unwrap())).unwrap();
        }
    }
    std::fs::write(
        dir.join("observation_clock.dsrv"),
        format!(
            "use experimental::{{modules, functions}}\ndef label() -> Str = {}\n",
            serde_json::to_string(trial.clock_label()).unwrap()
        ),
    )
    .unwrap();
}

fn native_config(
    trial: &Trial,
    file: &std::path::Path,
    provenance: &std::path::Path,
    capture: &NativeCapture,
    clocks: bool,
) -> Json {
    // Structural losses are checked against the frozen complete native capture;
    // coherent value mutations are frozen as a new input artifact. The hashes
    // prove capture extent, independently of Knowledge write-sequence numbers.
    let structurally_short = capture.archive_records != trial.lines.len();
    let baseline = if structurally_short {
        native_capture(trial, &trial.journal())
    } else {
        capture.clone()
    };
    let mut config = json!({"sources":{"journal":{
        "kind":"rpio-observation-clock-file", "variable":"journal_input",
        "files":[file.display().to_string()], "journal_clock":trial.clock_label(),
        "capture_checks":[{"records":baseline.positions.len(),"sha256":sha256_hex(&baseline.bytes),"journal_clock":trial.clock_label()}],
        "provenance":provenance.display().to_string()
    }}});
    if clocks {
        config["sources"]["journal"]["clock_samples"] = json!(
            trial
                .clock_samples()
                .into_iter()
                .map(|(time, through)| json!({"time":time,"through":capture.through(through)}))
                .collect::<Vec<_>>()
        );
    }
    config
}

/// Producer-written native capture, independently of archival conversion.
fn run_native_fixture(root_name: &str, entry: &Json) -> Run {
    let scratch = Scratch::new("native-root");
    stage_modules(&trial("nominal"), scratch.path());
    std::fs::write(
        scratch.file("observation_clock.dsrv"),
        format!(
            "use experimental::{{modules, functions}}\ndef label() -> Str = {}\n",
            serde_json::to_string(&entry["journal_clock"]).unwrap()
        ),
    )
    .unwrap();
    let file = root()
        .join("tests/fixtures/journal/native-v2")
        .join(entry["file"].as_str().unwrap());
    let config = json!({"sources":{"journal":{
        "kind":"rpio-observation-clock-file","variable":"journal_input",
        "journal_clock":entry["journal_clock"],"files":[file],
        "capture_checks":[{"records":entry["records"],"sha256":entry["sha256"],"journal_clock":entry["journal_clock"]}],
        "provenance":scratch.file("provenance.jsonl")
    }}});
    Checker {
        binary: env!("CARGO_BIN_EXE_trustworthiness_checker").into(),
        cwd: root(),
    }
    .run(
        &scratch.file(&format!("{root_name}.dsrv")),
        &config,
        "dataflow",
        0,
    )
}

fn run_ok(root_name: &str, trial: &Trial) -> Run {
    let run = run_root(root_name, trial, &trial.journal(), "dataflow");
    assert!(run.success, "{root_name} on {}: {}", trial.name, run.stderr);
    run
}

/// Run one root over several trials in parallel.
fn run_all(root_name: &str, names: &[&str]) -> Vec<(Trial, Run)> {
    std::thread::scope(|scope| {
        let handles: Vec<_> = names
            .iter()
            .map(|name| {
                scope.spawn(move || {
                    let trial = trial(name);
                    let run = run_ok(root_name, &trial);
                    (trial, run)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

fn never(run: &Run, outputs: &[&str], context: &str) {
    for output in outputs {
        assert!(
            run.true_rows(output).is_empty(),
            "{context}: {output} at positions {:?} (rows {:?})",
            run.true_positions(output),
            run.true_rows(output)
        );
    }
}

fn last(run: &Run, output: &str) -> String {
    run.at(output, run.provenance.len() - 1).to_owned()
}

/// Positions of Event records publishing on topic `name`.
fn events(trial: &Trial, name: &str) -> Vec<u64> {
    trial.positions(|r| shape::event_name(r) == Some(name))
}

/// How many publications on consumed topics (`evidence::consumed`) no Start
/// consumes: they stay pending for the run. Publications on other topics are
/// never kept.
fn unconsumed(trial: &Trial) -> usize {
    const CONSUMED: [&str; 5] = [
        "observation_recorded",
        "anomaly_detected",
        "plan_rejected",
        "plan_generated",
        "plan_validated",
    ];
    let causes: std::collections::BTreeSet<(i64, i64)> = trial
        .positions(|r| shape::step(r) == Some("Start"))
        .into_iter()
        .filter_map(|seq| shape::cause(&trial.record(seq)))
        .collect();
    trial
        .positions(|r| shape::event_name(r).is_some_and(|topic| CONSUMED.contains(&topic)))
        .into_iter()
        .filter(|seq| !causes.contains(&shape::emission(&trial.record(*seq)).unwrap()))
        .count()
}

const CORRECT: [&str; 6] = [
    "nominal",
    "nominal@redis",
    "adapt",
    "retries-then-accept",
    "no-plan",
    "acquisitions-at-limit",
];

/// Correct trials, with and without adaptation, retries within the budget
/// and a plan-less episode, raise no Sahar finding of any severity, leave no
/// response open, and never make the journal invalid or the evaluation
/// incomplete.
#[test]
fn sahar_finds_nothing_on_correct_trials() {
    // Actual current-producer bytes also exercise source admission and the
    // shared adapter. In particular, immutable average definitions must not
    // create context-free duplicate window findings before their Phase Writes.
    let manifest: Json = serde_json::from_slice(
        &std::fs::read(root().join("tests/fixtures/journal/native-v2/manifest.json")).unwrap(),
    )
    .unwrap();
    let entry = manifest["captures"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["file"] == "latency-nominal.jsonl")
        .unwrap();
    let mut publications = Vec::new();
    for (name, output_count) in [
        ("sahar", 23),
        ("strengthened", 35),
        ("pv-a", 8),
        ("pv-b", 8),
    ] {
        let run = run_native_fixture(name, entry);
        assert!(run.success, "native {name}: {}", run.stderr);
        let records = entry["records"].as_u64().unwrap() as usize;
        assert_eq!(
            run.provenance.len(),
            records,
            "one provenance row per native record"
        );
        assert_eq!(
            run.outputs.len(),
            output_count,
            "declared output set for {name}"
        );
        assert!(
            run.outputs.values().all(|rows| rows.len() == records),
            "full output coverage for {name}"
        );
        assert_eq!(
            (0..records)
                .map(|row| run.position(row))
                .collect::<Vec<_>>(),
            (1..=records as u64).map(Some).collect::<Vec<_>>(),
            "physical native cursors"
        );
        if name.starts_with("pv") {
            never(
                &run,
                &[
                    "journal_invalid",
                    "protocol_violated",
                    "executed_plan_mismatch",
                    "required_publication_missing",
                ],
                name,
            );
            publications.push(run.outputs);
        } else {
            never(
                &run,
                &[
                    "violation",
                    "warning",
                    "startup_incomplete",
                    "journal_invalid",
                    "ever_violation",
                ],
                name,
            );
            if name == "strengthened" {
                never(&run, &["observation_warning", "ever_warning"], name);
            }
        }
    }
    assert_eq!(
        publications[0], publications[1],
        "native publication views agree exactly"
    );
    for (trial, run) in run_all("sahar", &CORRECT) {
        never(
            &run,
            &[
                "violation",
                "warning",
                "startup_incomplete",
                "journal_invalid",
                "ever_violation",
            ],
            &trial.name,
        );
        assert_eq!(last(&run, "pending_checks"), "0", "{}", trial.name);
        assert_eq!(
            run.provenance.len() as u64,
            native_capture(&trial, &trial.journal()).positions.len() as u64
                + trial.clock_samples().len() as u64,
            "{}: one row per record and sample",
            trial.name
        );
    }
}

/// P11 counts rejections within an episode and resets at each anomaly. The
/// budget-abort trial has three rejections per episode and then an abort,
/// so no rejection exceeds the budget of three. (Defect F1: the counter's
/// previous value sits inside a lazy `if` branch.)
#[test]
fn sahar_counts_rejections_per_episode_so_a_budget_abort_is_not_a_retry_violation() {
    for (trial, run) in run_all("sahar", &["budget-abort", "unsolicited-execute"]) {
        let anomalies = events(&trial, "anomaly_detected");
        let rejections = events(&trial, "plan_rejected");
        assert_eq!(anomalies.len(), 3);
        for window in anomalies
            .windows(2)
            .map(|w| (w[0], w[1]))
            .chain([(anomalies[2], trial.through())])
        {
            let within = rejections
                .iter()
                .filter(|r| (window.0..window.1).contains(r))
                .count();
            assert_eq!(within, 3, "{}: three rejections per episode", trial.name);
        }
        never(
            &run,
            &["retry_budget", "journal_invalid", "startup_incomplete"],
            &trial.name,
        );
        if trial.name == "budget-abort" {
            never(&run, &["violation"], &trial.name);
        }
    }
}

/// An Execute started by a plan_validated from outside the loop, after the
/// budget abort, is a protocol finding of that callback, not a journal fault:
/// an external trigger names no publication.
#[test]
fn sahar_reports_an_unsolicited_execution_as_a_protocol_finding() {
    let trial = trial("unsolicited-execute");
    let start = trial.positions(|r| shape::is_start_of(r, "Execute"))[0];
    assert_eq!(shape::cause(&trial.record(start)), None);
    let end = trial.positions(|r| shape::is_end_of(r, "Execute"))[0];
    let run = run_ok("sahar", &trial);
    let found = run.true_positions("protocol_bad");
    assert!(!found.is_empty());
    assert!(
        found.iter().all(|seq| (start..=end).contains(seq)),
        "protocol_bad at {found:?}, outside the Execute callback {start}..={end}"
    );
    never(
        &run,
        &["journal_invalid", "startup_incomplete"],
        "unsolicited-execute",
    );
}

/// The duplicate delivery in `not-applied` cites an acceptance already
/// consumed: every root that reads the coordinator reports the journal
/// invalid from that Start on, and the strengthened root also reports the
/// NotApplied execution as a bad effect.
#[test]
fn a_duplicate_delivery_makes_the_journal_invalid_from_the_duplicate_start() {
    let trial = trial("not-applied");
    let duplicate = trial.positions(|r| shape::is_start_of(r, "Execute"))[1];
    let not_applied = trial.positions(|r| {
        shape::is_end_of(r, "Execute")
            && r["payload"]["step"]["payload"]["outcome"]["payload"]["$tag"] == "NotApplied"
    });
    assert_eq!(not_applied.len(), 1);
    for (root_name, output) in [
        ("sahar", "journal_invalid"),
        ("strengthened", "journal_invalid"),
        ("pv-a", "journal_invalid"),
        ("pv-b", "journal_invalid"),
    ] {
        let run = run_ok(root_name, &trial);
        assert_eq!(
            run.true_positions(output).first(),
            Some(&duplicate),
            "{root_name}"
        );
        assert!(
            run.flag(output, run.provenance.len() - 1),
            "{root_name}: sticky"
        );
        if !root_name.starts_with("pv") {
            never(&run, &["startup_incomplete"], root_name);
        }
        if root_name == "strengthened" {
            assert_eq!(run.true_positions("bad_effect"), not_applied);
        }
    }
}

/// After a swap, Sahar's P14 assessment stays open until the first full
/// window. Only two cycles follow the swap in `adapt`, so it stays open to
/// the end of the trial. (Defect F1, as for P11.)
#[test]
fn assessment_after_a_swap_stays_open_until_a_full_window() {
    let trial = trial("adapt");
    let executed = events(&trial, "plan_executed");
    assert_eq!(executed.len(), 1);
    let cycles_after = events(&trial, "cycle_completed")
        .into_iter()
        .filter(|seq| *seq > executed[0])
        .count();
    assert!(
        cycles_after < 10,
        "{cycles_after} cycles cannot fill a window"
    );
    let run = run_ok("sahar", &trial);
    let from = run.row_of(executed[0]);
    let open: Vec<usize> = (from..run.provenance.len())
        .filter(|row| !run.flag("assessing", *row))
        .collect();
    assert!(
        open.is_empty(),
        "assessing closed on rows {open:?} after the swap at row {from}"
    );
    never(&run, &["ineffective", "effect_inconclusive"], "adapt");
}

/// The strengthened root raises nothing on correct trials without retries;
/// after the swap the recovery stays pending, because fewer than ten fresh
/// samples of the new model follow it.
#[test]
fn strengthened_finds_nothing_on_correct_trials_and_waits_for_recovery() {
    for (trial, run) in run_all(
        "strengthened",
        &["nominal", "adapt", "acquisitions-at-limit"],
    ) {
        never(
            &run,
            &[
                "violation",
                "warning",
                "startup_incomplete",
                "journal_invalid",
                "ever_violation",
            ],
            &trial.name,
        );
        assert_eq!(last(&run, "pending_checks"), "0", "{}", trial.name);
        match events(&trial, "plan_executed").first() {
            Some(executed) => {
                let from = run.row_of(*executed);
                assert!(
                    run.true_rows("recovery_pending").first() == Some(&from),
                    "{}",
                    trial.name
                );
                assert!(run.flag("recovery_pending", run.provenance.len() - 1));
            }
            None => never(&run, &["recovery_pending"], &trial.name),
        }
    }
}

/// TW-I05: evidence starvation needs `n` (= 10, `strengthened.dsrv`)
/// consecutive first adaptive samples without an entropy; a sample with one
/// resets the streak. Each Detections tuple is edited in every record that
/// carries it, so the capture stays coherent and only the domain value
/// changes. A streak broken once in eleven missing samples never starves; ten
/// in a row starve on the tenth, and a later present sample ends it. (A
/// streak whose previous value is read inside a lazy branch counts the
/// eleven; defect F1.)
#[test]
fn evidence_starvation_needs_n_consecutive_samples_without_entropy() {
    const N: usize = 10;
    let trial = trial("nominal");
    let cycles = events(&trial, "cycle_completed");
    assert_eq!(cycles.len(), 12);
    // The Detections tuple each cycle's publication cites, and every record
    // carrying the same tuple.
    let cited = |cycle: u64| shape::cited(&trial.record(cycle), "result")["ref"].clone();
    let detections = |cycle: u64| -> Vec<u64> {
        let reference = cited(cycle);
        trial.positions(|r| shape::tuples(r).iter().any(|t| t["ref"] == reference))
    };
    for cycle in &cycles {
        let sample = &shape::cited(&trial.record(*cycle), "result")["value"].clone();
        assert_eq!(sample["adaptive_cycle"], true);
        assert!(!sample["screw_entropy"].is_null());
        assert_eq!(detections(*cycle).len(), 3, "Write, detection and cycle");
    }
    let without_entropy = |missing: &[usize]| {
        missing.iter().fold(trial.journal(), |journal, index| {
            let reference = cited(cycles[*index]);
            detections(cycles[*index])
                .into_iter()
                .fold(journal, |journal, seq| {
                    journal.edit(seq, |record| {
                        shape::tuples_mut(record, &mut |tuple| {
                            if tuple["ref"] == reference {
                                tuple["value"]["screw_entropy"] = Json::Null;
                            }
                        })
                    })
                })
        })
    };
    // The cycles on which the streak of missing samples reaches N.
    let starved = |missing: &[usize]| -> Vec<u64> {
        let mut streak = 0;
        cycles
            .iter()
            .enumerate()
            .filter_map(|(index, seq)| {
                streak = if missing.contains(&index) {
                    streak + 1
                } else {
                    0
                };
                (streak >= N).then_some(*seq)
            })
            .collect()
    };
    let broken: Vec<usize> = (0..12).filter(|index| *index != 5).collect();
    let ten: Vec<usize> = (0..10).collect();
    let all: Vec<usize> = (0..12).collect();
    let cases = [broken, ten, all];
    let runs: Vec<Run> = std::thread::scope(|scope| {
        let handles: Vec<_> = cases
            .iter()
            .map(|missing| {
                let journal = without_entropy(missing);
                let trial = &trial;
                scope.spawn(move || run_root("strengthened", trial, &journal, "dataflow"))
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    for (missing, run) in cases.iter().zip(&runs) {
        let context = format!("missing samples {missing:?}");
        assert!(run.success, "{context}: {}", run.stderr);
        let expected: Vec<u64> = missing.iter().map(|index| cycles[*index]).collect();
        assert_eq!(run.true_positions("missing_entropy"), expected, "{context}");
        assert_eq!(
            run.true_positions("evidence_starvation"),
            starved(missing),
            "{context}"
        );
        never(run, &["journal_invalid", "startup_incomplete"], &context);
    }
    assert!(starved(&cases[0]).is_empty());
    assert_eq!(starved(&cases[1]), [cycles[9]]);
    assert_eq!(starved(&cases[2]), cycles[9..]);
}

/// The strengthened root judges what the actual application did: an
/// immediate re-proposal of the model just rejected, in the same episode and
/// window, is a repeated proposal; every abort with its allowed reason and
/// counter is a bounded abort (a safety finding of the stricter root); the
/// counters themselves are within budget, and the protocol is followed.
#[test]
fn strengthened_judges_repeated_proposals_and_bounded_aborts() {
    for (trial, run) in run_all(
        "strengthened",
        &["retries-then-accept", "budget-abort", "no-plan"],
    ) {
        let name = trial.name.as_str();
        // A proposal repeats the rejection before it when it names the same
        // model and no EntropyHistory write intervened.
        let mut expected = Vec::new();
        let mut rejected: Option<(Json, u64)> = None;
        for seq in trial.after() + 1..=trial.through() {
            let record = trial.record(seq);
            let model = |r: &Json| shape::cited(r, "plan")["value"]["model_id"].clone();
            match shape::event_name(&record) {
                Some("plan_rejected") => rejected = Some((model(&record), seq)),
                Some("plan_generated") => {
                    if rejected.as_ref().is_some_and(|(m, _)| *m == model(&record)) {
                        expected.push(seq);
                    }
                }
                Some("anomaly_detected" | "adaptation_aborted" | "plan_validated") => {
                    rejected = None
                }
                _ => {}
            }
            if shape::step(&record) == Some("Write")
                && record["payload"]["step"]["payload"]["key"] == "EntropyHistory"
            {
                rejected = None;
            }
        }
        assert_eq!(run.true_positions("repeated_proposal"), expected, "{name}");
        assert_eq!(
            run.true_positions("bounded_abort"),
            events(&trial, "adaptation_aborted"),
            "{name}"
        );
        // Each rejection cites the count of rejections so far, and a budget
        // abort cites one more than the budget.
        for seq in events(&trial, "plan_rejected")
            .into_iter()
            .chain(events(&trial, "adaptation_aborted"))
        {
            let count = shape::cited(&trial.record(seq), "counter")["value"]["count"].clone();
            assert!(
                count.as_i64().is_some_and(|c| (0..=4).contains(&c)),
                "{name} {seq}: {count}"
            );
        }
        never(
            &run,
            &[
                "retry_budget",
                "protocol_bad",
                "journal_invalid",
                "startup_incomplete",
                "bad_gate",
            ],
            name,
        );
    }
}

/// PV-A composes the publication view in one call; PV-B builds it from the
/// separate components. On every recording, and on mutated ones, the two
/// print identical traces.
#[test]
fn pv_a_and_pv_b_print_identical_traces_on_every_recording() {
    let names = [
        "nominal",
        "nominal@redis",
        "adapt",
        "retries-then-accept",
        "budget-abort",
        "no-plan",
        "not-applied",
        "unsolicited-execute",
        "acquisitions-at-limit",
    ];
    let a = run_all("pv-a", &names);
    let b = run_all("pv-b", &names);
    for ((trial, a), (_, b)) in a.iter().zip(&b) {
        assert_eq!(a.trace(), b.trace(), "{}", trial.name);
        never(a, &["required_publication_missing"], &trial.name);
        let ends = trial.positions(|r| shape::step(r) == Some("End"));
        assert_eq!(
            a.true_positions("callback_ended"),
            ends,
            "{}: one per End",
            trial.name
        );
        assert_eq!(last(a, "active_activations"), "0", "{}", trial.name);
        // Publications on consumed topics that nobody consumed stay pending.
        assert_eq!(
            last(a, "pending_publications"),
            unconsumed(trial).to_string(),
            "{}",
            trial.name
        );
        match trial.name.as_str() {
            "not-applied" => assert!(!a.true_rows("journal_invalid").is_empty()),
            "unsolicited-execute" => {
                let start = trial.positions(|r| shape::is_start_of(r, "Execute"))[0];
                assert!(
                    a.true_positions("protocol_violated")
                        .iter()
                        .all(|seq| *seq >= start)
                );
                assert!(!a.true_rows("protocol_violated").is_empty());
            }
            _ => never(
                a,
                &[
                    "journal_invalid",
                    "protocol_violated",
                    "executed_plan_mismatch",
                ],
                &trial.name,
            ),
        }
    }

    // Execute uses another plan version than the one accepted: both plans
    // stay visible, so the mismatch is reported, identically.
    let trial = trial("adapt");
    let execute = trial.positions(|r| shape::is_start_of(r, "Execute"))[0] as i64;
    let mine = trial.positions(|r| shape::start_seq(r) == Some(execute));
    let plan_records: Vec<u64> = mine
        .into_iter()
        .filter(|seq| {
            shape::tuples(&trial.record(*seq))
                .iter()
                .any(|t| t["ref"]["key"] == "CandidateModel")
        })
        .collect();
    assert!(
        plan_records.len() >= 2,
        "a Read and the Event cite the plan"
    );
    let plan_write = trial
        .positions(|r| shape::written(r).is_some_and(|t| t["ref"]["key"] == "CandidateModel"))[0];
    let mut old_plan = shape::written(&trial.record(plan_write)).unwrap().clone();
    let used_versions: std::collections::BTreeSet<u64> = (trial.after() + 1..=trial.through())
        .flat_map(|seq| {
            shape::tuples(&trial.record(seq))
                .into_iter()
                .map(|t| t["ref"]["global_write_sequence"].as_u64().unwrap())
                .collect::<Vec<_>>()
        })
        .collect();
    let accepted_version = old_plan["ref"]["global_write_sequence"].as_u64().unwrap();
    let old_version = (1..accepted_version)
        .find(|v| !used_versions.contains(v))
        .unwrap();
    old_plan["ref"]["global_write_sequence"] = json!(old_version);
    old_plan["written_by"] = json!({"$tag":"External","payload":{"name":"retained-prior-plan"}});
    // The archive has no startup CandidateModel. This counterfactual retained
    // version uses the same valid plan value with a distinct, unused immutable
    // reference and writer; the source adapter admits its definition explicitly.
    let journal = plan_records.iter().fold(trial.journal(), |journal, seq| {
        journal.edit(*seq, |r| {
            shape::tuples_mut(r, &mut |t| {
                if t["ref"]["key"] == "CandidateModel" {
                    *t = old_plan.clone();
                }
            })
        })
    });
    let executed = events(&trial, "plan_executed");
    let a = run_root("pv-a", &trial, &journal, "dataflow");
    let b = run_root("pv-b", &trial, &journal, "dataflow");
    assert!(a.success && b.success, "{}{}", a.stderr, b.stderr);
    assert_eq!(a.trace(), b.trace());
    assert_eq!(a.true_positions("executed_plan_mismatch"), executed);
    never(
        &a,
        &["journal_invalid"],
        "an executed old plan is not a journal fault",
    );

    // A removed record is an input error before any later output, in both.
    let gone = executed[0];
    for root_name in ["pv-a", "pv-b"] {
        let run = run_root(root_name, &trial, &trial.journal().delete(gone), "dataflow");
        assert!(!run.success, "{root_name}");
        assert!(
            run.stderr
                .contains("SHA-256 differs from its frozen capture"),
            "{root_name}: {}",
            run.stderr
        );
        assert!(
            (0..run.provenance.len()).all(|row| run.position(row).is_none_or(|seq| seq < gone))
        );
    }
}

/// SHARD-style mutations of a correct capture: frozen extent checks reject
/// lost or truncated records before evaluation; contradictory evidence becomes
/// conflicting immutable history. Invalid domain fields or excessive nesting
/// are source errors, never a crash or a falsely completed trial.
#[test]
fn shard_mutations_are_input_errors_or_visible_findings() {
    let trial = trial("nominal");
    let middle = 100;
    let deleted = run_root("sahar", &trial, &trial.journal().delete(middle), "dataflow");
    assert!(!deleted.success);
    assert!(
        deleted
            .stderr
            .contains("SHA-256 differs from its frozen capture"),
        "{}",
        deleted.stderr
    );
    assert!(
        deleted.provenance.is_empty(),
        "capture completeness is checked before output"
    );

    let truncated = run_root(
        "sahar",
        &trial,
        &trial.journal().truncate_last(10),
        "dataflow",
    );
    assert!(!truncated.success);
    assert!(
        truncated.stderr.contains("has no final line terminator"),
        "{}",
        truncated.stderr
    );
    assert!(
        (0..truncated.provenance.len()).all(|row| truncated.position(row) != Some(trial.through()))
    );

    let short = run_root(
        "sahar",
        &trial,
        &trial.journal().delete(trial.through()),
        "dataflow",
    );
    assert!(!short.success);
    assert!(
        short
            .stderr
            .contains("SHA-256 differs from its frozen capture"),
        "{}",
        short.stderr
    );

    // One Event's copy of its evidence no longer matches what was acquired.
    let detection = events(&trial, "detection_completed")[2];
    let journal = trial.journal().edit(detection, |r| {
        r["payload"]["evidence"]["frame"]["value"]["brightness"] = json!(3.5)
    });
    let edited = run_root("sahar", &trial, &journal, "dataflow");
    assert!(
        !edited.success,
        "an immutable definition may not be contradicted"
    );
    assert!(
        edited
            .stderr
            .contains("defined with another value or writer"),
        "{}",
        edited.stderr
    );
    assert!(
        edited.outputs.is_empty(),
        "conflicting history is rejected before evaluation"
    );

    // A FrameRef whose brightness is not a float is not a FrameRef.
    let write = trial.positions(|r| shape::step(r) == Some("Write"))[0];
    let invalid_ref = trial.record(write)["payload"]["step"]["payload"]["value"]["ref"].clone();
    let journal = (trial.after() + 1..=trial.through()).fold(trial.journal(), |journal, seq| {
        journal.edit(seq, |r| {
            shape::tuples_mut(r, &mut |t| {
                if t["ref"] == invalid_ref {
                    t["value"]["brightness"] = json!(true);
                }
            })
        })
    });

    let typed = run_root("sahar", &trial, &journal, "dataflow");
    assert!(!typed.success);
    assert!(typed.stderr.contains("brightness"), "{}", typed.stderr);
    assert!(!typed.stderr.contains("panicked"), "{}", typed.stderr);
    assert!(
        (0..typed.provenance.len()).all(|row| typed.position(row).is_none_or(|seq| seq < write)),
        "no record at or after invalid typed write is evaluated"
    );

    // A bracket bomb far deeper than any stack.
    let mut bomb = br#"{"schema_version":4,"position":"#.to_vec();
    bomb.extend(std::iter::repeat_n(b'[', 200_000));
    let bombed = run_root(
        "sahar",
        &trial,
        &trial.journal().replace(middle, bomb),
        "dataflow",
    );
    assert!(!bombed.success);
    assert!(
        bombed.stderr.contains("malformed JSON") && bombed.stderr.contains("EOF while parsing"),
        "{}",
        bombed.stderr
    );
    assert!(!bombed.stderr.contains("overflow"), "{}", bombed.stderr);
}

/// The paper roots use the full prototype language, whose `if` is lazy; the
/// semi-sync runtime does not run lazy branches and refuses the roots by
/// name before reading any input.
#[test]
fn semi_sync_refuses_the_lazy_paper_roots_by_name() {
    let trial = trial("nominal");
    for root_name in ["sahar", "strengthened", "pv-a", "pv-b"] {
        let run = run_root(root_name, &trial, &trial.journal(), "semi-sync");
        assert!(!run.success, "{root_name}");
        assert!(run.stderr.contains("lazy"), "{root_name}: {}", run.stderr);
        assert!(run.provenance.is_empty(), "{root_name}");
    }
}

/// Every root and its generated declarations check, strictly and gradually,
/// as files with their modules; the declarations beside them, and the test
/// fixtures' copy, are exactly what the generator makes from the
/// application's observation schema.
#[test]
fn the_roots_check_with_their_generated_declarations() {
    let dir = root().join("examples/screw-detection");
    for output in [
        dir.join("declarations.dsrv"),
        root().join("tests/fixtures/rpio_knowledge.dsrv"),
    ] {
        let status = std::process::Command::new(env!("CARGO_BIN_EXE_generate_rpio_declarations"))
            .arg(dir.join("observation_schema.json"))
            .arg("--output")
            .arg(&output)
            .arg("--check")
            .output()
            .expect("the generator runs");
        assert!(
            status.status.success(),
            "{}: {}",
            output.display(),
            String::from_utf8_lossy(&status.stderr)
        );
    }
    for root_name in ["sahar", "strengthened", "pv-a", "pv-b"] {
        for mode in ["strict", "gradual"] {
            let output = std::process::Command::new(env!("CARGO_BIN_EXE_tc-expand"))
                .args(["--check-mode", mode])
                .arg(spec(root_name))
                .output()
                .expect("tc-expand runs");
            assert!(
                output.status.success(),
                "{root_name} {mode}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Coherent entropy edits, a fragment of the long run, and the source
// boundary's type check.
// ---------------------------------------------------------------------------

/// Set every entropy a record carries in tuples written after position
/// `after`, coherently: each sample of an `EntropyHistory`, a
/// `RunningAvgEntropy` value that is present, and the `screw_entropy` of
/// `Detections` whose model is `model` (any model when `None`). Applied to
/// every record, the edit is the same on every copy of a tuple, so records
/// stay coherent with the records they cite and that cite them.
fn set_entropies(record: &mut Json, value: f64, model: Option<i64>, after: u64) {
    shape::tuples_mut(record, &mut |tuple| {
        if tuple["ref"]["global_write_sequence"].as_u64() <= Some(after) {
            return;
        }
        let key = tuple["ref"]["key"].as_str().map(str::to_owned);
        let fields = &mut tuple["value"];
        match key.as_deref() {
            Some("EntropyHistory") => {
                for sample in fields["values"].as_array_mut().unwrap() {
                    *sample = json!(value);
                }
            }
            Some("RunningAvgEntropy") if !fields["value"].is_null() => {
                fields["value"] = json!(value);
            }
            Some("Detections")
                if model.is_none_or(|m| fields["model_id"] == m)
                    && !fields["screw_entropy"].is_null() =>
            {
                fields["screw_entropy"] = json!(value);
            }
            _ => {}
        }
    })
}

/// What a CLI run printed and how it ended.
struct CliRun {
    code: Option<i32>,
    rows: std::collections::BTreeMap<String, std::collections::BTreeMap<usize, String>>,
    stderr: String,
}

impl CliRun {
    fn true_rows(&self, name: &str) -> Vec<usize> {
        self.rows[name]
            .iter()
            .filter(|(_, value)| *value == "true")
            .map(|(row, _)| *row)
            .collect()
    }
}

fn run_cli(spec: &std::path::Path, config: &Json, args: &[&str]) -> CliRun {
    let scratch = Scratch::new("cli");
    let mut input_config = config.clone();
    for key in [
        "_test_archive_positions",
        "_test_archive_after",
        "_test_spec_dir",
    ] {
        input_config.as_object_mut().unwrap().remove(key);
    }
    if let Some(positions) = config["_test_archive_positions"].as_array() {
        let schedule = input_config["sources"]["journal"]
            .get("clock_samples")
            .cloned();
        if let Some(schedule) = schedule {
            let mut samples: Vec<Json> = if let Some(path) = schedule.as_str() {
                std::fs::read_to_string(path)
                    .unwrap()
                    .lines()
                    .filter(|l| !l.trim().is_empty())
                    .map(|l| serde_json::from_str(l).unwrap())
                    .collect()
            } else {
                schedule.as_array().unwrap().clone()
            };
            for sample in &mut samples {
                let through = sample["through"].as_u64().unwrap();
                sample["through"] = json!(
                    positions
                        .iter()
                        .rposition(|p| p.as_u64().is_some_and(|p| p <= through))
                        .map_or(0, |i| i + 1)
                );
            }
            input_config["sources"]["journal"]["clock_samples"] = json!(samples);
        }
    }
    let staged_spec = config["_test_spec_dir"]
        .as_str()
        .map(|dir| PathBuf::from(dir).join(spec.file_name().unwrap()));
    let config_path = scratch.write("input.json", serde_json::to_vec(&input_config).unwrap());
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
        .arg(staged_spec.as_deref().unwrap_or(spec))
        .args([
            "--input-config",
            config_path.to_str().unwrap(),
            "--output-stdout",
        ])
        .args(args)
        .env("RUST_LOG", "error")
        .current_dir(root())
        .output()
        .expect("the checker runs");
    let mut rows =
        std::collections::BTreeMap::<String, std::collections::BTreeMap<usize, String>>::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let Some((key, value)) = line.split_once(" = ") else {
            continue;
        };
        let Some((name, row)) = key.split_once('[') else {
            continue;
        };
        if let Ok(mut row) = row.trim_end_matches(']').parse::<usize>() {
            if let Some(positions) = config["_test_archive_positions"].as_array() {
                if row < positions.len() {
                    let Some(position) = positions[row].as_u64() else {
                        continue;
                    };
                    row = (position - config["_test_archive_after"].as_u64().unwrap() - 1) as usize;
                } else {
                    row = positions.iter().filter(|p| p.is_u64()).count() + row - positions.len();
                }
            }
            rows.entry(name.to_owned())
                .or_default()
                .insert(row, value.to_owned());
        }
    }
    CliRun {
        code: output.status.code(),
        rows,
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

/// The replay of `journal` over `trial`'s extent, with no clock samples: row
/// `k` is the record at position `after + 1 + k`.
fn record_only_config(trial: &Trial, file: &std::path::Path) -> Json {
    let bytes = std::fs::read(file).unwrap();
    let journal = trial.journal_from_bytes(&bytes);
    let capture = native_capture(trial, &journal);
    std::fs::write(file, &capture.bytes).unwrap();
    // Each concurrent case owns its module directory; copying into a shared
    // directory could let another checker read a temporarily truncated file.
    let modules = file.with_extension("modules");
    std::fs::create_dir_all(&modules).unwrap();
    stage_modules(trial, &modules);
    let mut config = native_config(
        trial,
        file,
        &file.with_extension("provenance.jsonl"),
        &capture,
        false,
    );
    config["_test_archive_positions"] = json!(capture.positions);
    config["_test_archive_after"] = json!(trial.after());
    config["_test_spec_dir"] = json!(modules.display().to_string());
    config
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The long-run fragment (`tests/fixtures/journal/fragment`): records 1..764
/// of a long capture, an honest fragment and not a verified trial, replayed
/// over its own explicit extent.
fn long_effect_fragment() -> Trial {
    let dir = root().join("tests/fixtures/journal/fragment");
    let fragment: Json =
        serde_json::from_slice(&std::fs::read(dir.join("long-effect.fragment.json")).unwrap())
            .unwrap();
    assert_eq!(fragment["kind"], "actual-capture-prefix-fragment");
    assert_eq!(fragment["verified_capture"], false);
    let capture = std::fs::read(dir.join("long-effect.jsonl")).unwrap();
    assert_eq!(sha256_hex(&capture), fragment["fragment_sha256"]);
    Trial {
        name: "long-effect fragment".into(),
        dir: dir.clone(),
        meta: json!({
            "capture_file": "long-effect.jsonl",
            "observation_epoch": fragment["epoch"],
            "journal_clock": fragment["journal_clock"],
            "start_after_seq": fragment["fragment_extent"]["after"],
            "through_seq": fragment["fragment_extent"]["through"],
        }),
        lines: String::from_utf8(capture)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect(),
    }
}

/// S4 (review 33) on the long-run fragment. Everything expected is derived
/// from the records here, and cross-checked against the exporter's landmarks:
///
/// - the first Execute callback that ends `Applied` publishes
///   `plan_executed` and deploys the model its `ActiveModel` Write names;
/// - a fresh deployed sample is a `cycle_completed` whose cited Detections
///   was written after that publication by the deployed model. The outer,
///   pre-swap Analysis completes after the nested Execute, but its sample
///   is old and does not count;
/// - Sahar's assessment closes at the first window (average Write) whose
///   history is full; the strengthened recovery closes at the tenth fresh
///   sample.
///
/// On the actual values (entropy 0.15) both close with no finding. A
/// coherent degraded edit of every fresh sample, window and average (0.5,
/// above the 0.35 threshold) closes them at the same records, and raises
/// Sahar `ineffective` at the full window and strengthened
/// `unresolved_degradation` at the tenth fresh sample, each exactly there.
#[test]
fn s4_recovery_closes_at_the_tenth_fresh_sample_and_judges_its_window() {
    const N: usize = 10;
    let trial = long_effect_fragment();
    let fragment: Json = serde_json::from_slice(
        &std::fs::read(trial.dir.join("long-effect.fragment.json")).unwrap(),
    )
    .unwrap();
    let key = |r: &Json| r["payload"]["step"]["payload"]["key"].clone();

    let applied_end = trial.positions(|r| {
        shape::is_end_of(r, "Execute")
            && r["payload"]["step"]["payload"]["outcome"]["payload"]["$tag"] == "Applied"
    })[0];
    let execute = shape::start_seq(&trial.record(applied_end)).unwrap();
    let executed = trial.positions(|r| {
        shape::start_seq(r) == Some(execute) && shape::event_name(r) == Some("plan_executed")
    })[0];
    let deployed = trial.record(
        trial.positions(|r| {
            shape::start_seq(r) == Some(execute)
                && shape::step(r) == Some("Write")
                && key(r) == "ActiveModel"
        })[0],
    )["payload"]["step"]["payload"]["value"]["value"]["model_id"]
        .as_i64()
        .unwrap();
    let command_version =
        trial.record(executed)["payload"]["evidence"]["command"]["ref"]["global_write_sequence"]
            .as_u64()
            .unwrap();
    let cycles = events(&trial, "cycle_completed");
    let sample = |seq: u64| trial.record(seq)["payload"]["evidence"]["result"].clone();
    let fresh: Vec<u64> = cycles
        .iter()
        .copied()
        .filter(|seq| {
            let cited = sample(*seq);
            cited["ref"]["global_write_sequence"].as_u64().unwrap() > command_version
                && cited["value"]["model_id"] == deployed
        })
        .collect();
    let old_after_swap: Vec<u64> = cycles
        .iter()
        .copied()
        .filter(|seq| *seq > executed && !fresh.contains(seq))
        .collect();
    assert!(fresh.len() >= N);
    let tenth = fresh[N - 1];
    // Sahar's first full window after the execution: the average Write that
    // follows a history Write of at least N samples.
    let full_window = trial.positions(|r| {
        shape::step(r) == Some("Write")
            && key(r) == "EntropyHistory"
            && r["payload"]["step"]["payload"]["value"]["value"]["values"]
                .as_array()
                .unwrap()
                .len()
                >= N
    });
    let full_window = trial.positions(|r| {
        shape::step(r) == Some("Write")
            && key(r) == "RunningAvgEntropy"
            && shape::position_seq(r) > *full_window.iter().find(|s| **s > executed).unwrap()
    })[0];

    // Cross-check with the exporter's landmarks.
    assert_eq!(applied_end, fragment["first_applied_end_seq"]);
    assert_eq!(deployed, fragment["deployed_model_id"]);
    let listed = |name: &str| -> Vec<u64> {
        fragment[name]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["event_seq"].as_u64().unwrap())
            .collect()
    };
    assert_eq!(fresh[..N], listed("fresh_deployed_cycle_samples")[..]);
    assert_eq!(old_after_swap, listed("excluded_old_outer_cycle_events"));
    assert_eq!(tenth, fragment["tenth_fresh_cycle_event_seq"]);
    assert!(executed < old_after_swap[0] && old_after_swap[0] < fresh[0]);
    assert!(
        full_window < tenth,
        "the window fills before its sample completes"
    );

    let scratch = Scratch::new("s4");
    let degraded = (trial.after() + 1..=trial.through()).fold(trial.journal(), |journal, seq| {
        journal.edit(seq, |r| {
            set_entropies(r, 0.5, Some(deployed), command_version)
        })
    });
    let runs: Vec<(bool, CliRun, CliRun)> = [false, true]
        .into_iter()
        .map(|degrade| {
            let journal = if degrade {
                degraded.clone()
            } else {
                trial.journal()
            };
            let file = scratch.write(&format!("journal-{degrade}.jsonl"), journal.bytes());
            let config = record_only_config(&trial, &file);
            let sahar = run_cli(&spec("sahar"), &config, &["--runtime", "dataflow"]);
            let strengthened = run_cli(&spec("strengthened"), &config, &["--runtime", "dataflow"]);
            (degrade, sahar, strengthened)
        })
        .collect();
    let row = |seq: u64| (seq - trial.after() - 1) as usize;
    let span = |from: u64, until: u64| -> Vec<usize> { (row(from)..row(until)).collect() };
    for (degrade, sahar, strengthened) in &runs {
        let context = if *degrade { "degraded" } else { "actual" };
        for run in [sahar, strengthened] {
            assert_eq!(run.code, Some(0), "{context}: {}", run.stderr);
            assert_eq!(
                run.rows["journal_invalid"].len() as u64,
                trial.through() - trial.after()
            );
            assert!(run.true_rows("journal_invalid").is_empty(), "{context}");
            assert!(run.true_rows("startup_incomplete").is_empty(), "{context}");
        }
        assert_eq!(
            sahar.true_rows("assessing"),
            span(executed, full_window),
            "{context}: Sahar assesses from the execution until its first full window"
        );
        assert_eq!(
            strengthened.true_rows("recovery_pending"),
            span(executed, tenth),
            "{context}: recovery is pending until the tenth fresh sample"
        );
        let (ineffective, unresolved) = if *degrade {
            (vec![row(full_window)], vec![row(tenth)])
        } else {
            (vec![], vec![])
        };
        assert_eq!(sahar.true_rows("ineffective"), ineffective, "{context}");
        assert!(
            sahar.true_rows("effect_inconclusive").is_empty(),
            "{context}"
        );
        assert_eq!(
            strengthened.true_rows("unresolved_degradation"),
            unresolved,
            "{context}"
        );
    }
}

/// Review 31: on a clock-only tick the loop state is the coordinator's
/// latest written state, so a degraded state persists through silence.
/// Every entropy of `nominal` is raised coherently to 0.5 (above the 0.35
/// threshold), so the state is degraded, and not planning, from the first
/// record at which the latest history holds N samples. No anomaly is ever
/// published. The records span milliseconds; clock samples follow every 3 s
/// after that record. Sahar's `anomaly_missing` (degradation for more than
/// 10 s) is false on every record and on the samples at 3, 6 and 9 s, and
/// true on those at 12 and 15 s. With the actual values it never fires.
#[test]
fn a_degraded_state_persists_through_clock_only_silence() {
    const N: usize = 10;
    let trial = trial("nominal");
    let value_of = |r: &Json| -> Option<(String, Json)> {
        let tuple = match r["$tag"].as_str() {
            Some("Knowledge") => &r["payload"]["version"],
            _ if shape::step(r) == Some("Write") => &r["payload"]["step"]["payload"]["value"],
            _ => return None,
        };
        Some((
            tuple["ref"]["key"].as_str()?.to_owned(),
            tuple["value"].clone(),
        ))
    };
    // The first record after which the latest history is full, the latest
    // average is present (0.5 once edited) and the loop is not planning.
    let mut latest = std::collections::BTreeMap::<String, Json>::new();
    let mut degraded_from = None;
    for seq in trial.after() + 1..=trial.through() {
        if let Some((key, value)) = value_of(&trial.record(seq)) {
            latest.insert(key, value);
        }
        let full = latest
            .get("EntropyHistory")
            .is_some_and(|h| h["values"].as_array().unwrap().len() >= N);
        let average = latest
            .get("RunningAvgEntropy")
            .is_some_and(|a| !a["value"].is_null());
        let planning = latest
            .get("InPlanning")
            .is_some_and(|p| p["in_planning"] == true);
        if full && average && !planning {
            degraded_from = Some(seq);
            break;
        }
    }
    let degraded_from = degraded_from.unwrap();
    let start = trial.record(degraded_from)["arrival"]["journal_time"]
        .as_f64()
        .unwrap();
    let last = trial.record(trial.through())["arrival"]["journal_time"]
        .as_f64()
        .unwrap();
    assert!(
        last - start < 1.0,
        "the records alone span far less than 10 s"
    );
    assert!(events(&trial, "anomaly_detected").is_empty());

    let scratch = Scratch::new("clock-only");
    let offsets = [3.0, 6.0, 9.0, 12.0, 15.0];
    let schedule: String = offsets
        .iter()
        .map(|offset| {
            format!(
                "{{\"time\":{:?},\"through\":{}}}\n",
                start + offset,
                trial.through()
            )
        })
        .collect();
    let schedule = scratch.write("clock.jsonl", schedule);
    let records = (trial.through() - trial.after()) as usize;
    for degrade in [false, true] {
        let journal = if degrade {
            (trial.after() + 1..=trial.through()).fold(trial.journal(), |journal, seq| {
                journal.edit(seq, |r| set_entropies(r, 0.5, None, 0))
            })
        } else {
            trial.journal()
        };
        let file = scratch.write(&format!("journal-{degrade}.jsonl"), journal.bytes());
        let mut config = record_only_config(&trial, &file);
        config["sources"]["journal"]["clock_samples"] = json!(schedule.display().to_string());
        let run = run_cli(&spec("sahar"), &config, &["--runtime", "dataflow"]);
        assert_eq!(run.code, Some(0), "{}", run.stderr);
        assert_eq!(run.rows["anomaly_missing"].len(), records + offsets.len());
        assert!(run.true_rows("journal_invalid").is_empty());
        let expected: Vec<usize> = if degrade {
            offsets
                .iter()
                .enumerate()
                .filter(|(_, offset)| **offset > 10.0)
                .map(|(index, _)| records + index)
                .collect()
        } else {
            vec![]
        };
        assert_eq!(
            run.true_rows("anomaly_missing"),
            expected,
            "degraded {degrade}: from position {degraded_from}"
        );
    }
}

/// A capture whose FrameRef `brightness` is a string where the root's
/// declarations (generated from the observation schema) declare a float:
/// every copy of every FrameRef tuple is edited. Returns the edited capture
/// and the first position carrying a FrameRef brightness.
fn string_brightness(trial: &Trial, scratch: &Scratch) -> (std::path::PathBuf, u64) {
    let mut first = None;
    let journal = (trial.after() + 1..=trial.through()).fold(trial.journal(), |journal, seq| {
        let mut record = journal.record(seq);
        let mut changed = false;
        shape::tuples_mut(&mut record, &mut |tuple| {
            if tuple["ref"]["key"] == "FrameRef" && !tuple["value"]["brightness"].is_null() {
                let text = tuple["value"]["brightness"].to_string();
                tuple["value"]["brightness"] = json!(text);
                changed = true;
            }
        });
        if changed {
            first.get_or_insert(seq);
        }
        journal.edit(seq, |r| *r = record)
    });
    (
        scratch.write("journal.jsonl", journal.bytes()),
        first.expect("the capture carries a FrameRef brightness"),
    )
}

/// Application values are read against the root's declared types. A
/// capture whose FrameRef `brightness` is a string, where the declarations
/// say Float, is an input error at the first record carrying one: exit 1, no
/// panic, every earlier record evaluated and none after, and an error naming
/// the record's position and the field.
#[test]
fn a_value_that_does_not_fit_its_declared_type_is_an_input_error() {
    let trial = trial("adapt");
    let scratch = Scratch::new("typed");
    let (file, first) = string_brightness(&trial, &scratch);
    let run = run_cli(
        &spec("sahar"),
        &record_only_config(&trial, &file),
        &["--runtime", "dataflow", "--execution-policy", "synchronous"],
    );
    assert_eq!(run.code, Some(1), "{}", run.stderr);
    assert!(!run.stderr.contains("panicked"), "{}", run.stderr);
    assert!(run.stderr.contains("brightness"), "{}", run.stderr);
    assert!(run.stderr.contains("Float"), "{}", run.stderr);
    assert_eq!(
        run.rows["journal_invalid"].len() as u64,
        first - trial.after() - 1,
        "all archived facts before invalid value, and none after"
    );
}

/// One Analysis callback completing after an execution: its cycle
/// publication and the positions of its Detections, history and average
/// Writes.
#[derive(Clone, Copy, Debug)]
struct Cycle {
    event: u64,
    detection: u64,
    history: u64,
    average: u64,
}

/// What the recovery boundaries are judged from, derived from a journal's
/// records: the execution's `plan_executed`, the deployed model, and every
/// cycle whose detection was written after the execution, in order.
struct Recovery {
    executed: u64,
    deployed: i64,
    cycles: Vec<Cycle>,
}

fn recovery_of(trial: &Trial, journal: &Journal) -> Recovery {
    let record = |seq: u64| journal.record(seq);
    let seqs = trial.after() + 1..=trial.through();
    let key = |r: &Json| r["payload"]["step"]["payload"]["key"].clone();
    let applied_end = seqs
        .clone()
        .find(|seq| {
            let r = record(*seq);
            shape::is_end_of(&r, "Execute")
                && r["payload"]["step"]["payload"]["outcome"]["payload"]["$tag"] == "Applied"
        })
        .unwrap();
    let execute = shape::start_seq(&record(applied_end)).unwrap();
    let in_callback = |owner: i64, select: &dyn Fn(&Json) -> bool| -> Vec<u64> {
        seqs.clone()
            .filter(|seq| {
                let r = record(*seq);
                shape::start_seq(&r) == Some(owner) && select(&r)
            })
            .collect()
    };
    let executed = in_callback(execute, &|r| shape::event_name(r) == Some("plan_executed"))[0];
    let command_version =
        record(executed)["payload"]["evidence"]["command"]["ref"]["global_write_sequence"]
            .as_u64()
            .unwrap();
    let deployed = record(
        in_callback(execute, &|r| {
            shape::step(r) == Some("Write") && key(r) == "ActiveModel"
        })[0],
    )["payload"]["step"]["payload"]["value"]["value"]["model_id"]
        .as_i64()
        .unwrap();
    let cycles = seqs
        .clone()
        .filter(|seq| shape::event_name(&record(*seq)) == Some("cycle_completed"))
        .filter_map(|event| {
            let r = record(event);
            let detection = r["payload"]["evidence"]["result"]["ref"]["global_write_sequence"]
                .as_u64()
                .unwrap();
            (detection > command_version).then(|| {
                let owner = shape::start_seq(&r).unwrap();
                let write = |name: &str| {
                    in_callback(owner, &|w| {
                        shape::step(w) == Some("Write") && key(w) == name
                    })[0]
                };
                Cycle {
                    event,
                    detection,
                    history: write("EntropyHistory"),
                    average: write("RunningAvgEntropy"),
                }
            })
        })
        .collect();
    Recovery {
        executed,
        deployed,
        cycles,
    }
}

/// The recovery outcomes the authority gives a journal, computed from its
/// records alone.
///
/// Strengthened TW-C02: a fresh sample is the first completion of an
/// adaptive cycle whose detection was written after the execution, by the
/// deployed model, with an entropy in [0, 1]. At the tenth fresh sample the
/// recovery is ready if its window is exactly ten valid entropies of the
/// deployed model and the average is their mean; it then closes, and is
/// unresolved if the average exceeds 0.35. Otherwise it stays pending.
///
/// Sahar P14: the assessment closes at the first window (average Write)
/// after the execution whose history holds at least ten samples, and the
/// effect is ineffective if that average exceeds 0.35. It asks nothing of
/// the samples' model, domain or mean.
#[derive(Debug, PartialEq)]
struct Outcome {
    /// Positions where the assessment is open, from the execution.
    sahar_assessing: (u64, Option<u64>),
    sahar_ineffective: Vec<u64>,
    recovery_pending: (u64, Option<u64>),
    unresolved: Vec<u64>,
}

fn expected_outcome(recovery: &Recovery, journal: &Journal) -> Outcome {
    const N: usize = 10;
    const THRESHOLD: f64 = 0.35;
    let value =
        |seq: u64| journal.record(seq)["payload"]["step"]["payload"]["value"]["value"].clone();
    let average = |cycle: &Cycle| value(cycle.average)["value"].as_f64();
    let history = |cycle: &Cycle| -> (Vec<f64>, Json) {
        let h = value(cycle.history);
        (
            h["values"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_f64().unwrap())
                .collect(),
            h["model_id"].clone(),
        )
    };
    let valid = |e: f64| (0.0..=1.0).contains(&e);
    // Sahar.
    let full = recovery
        .cycles
        .iter()
        .find(|c| history(c).0.len() >= N)
        .map(|c| c.average);
    let sahar_ineffective = recovery
        .cycles
        .iter()
        .find(|c| Some(c.average) == full)
        .filter(|c| average(c).is_some_and(|a| a > THRESHOLD))
        .map(|c| vec![c.average])
        .unwrap_or_default();
    // Strengthened.
    let fresh: Vec<&Cycle> = recovery
        .cycles
        .iter()
        .filter(|c| {
            let sample = shape::cited(&journal.record(c.event), "result")["value"].clone();
            sample["adaptive_cycle"] == true
                && sample["model_id"] == recovery.deployed
                && sample["screw_entropy"].as_f64().is_some_and(valid)
        })
        .collect();
    let ready = fresh.get(N - 1).copied().filter(|c| {
        let (values, model) = history(c);
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        values.len() == N
            && model == json!(recovery.deployed)
            && values.iter().all(|v| valid(*v))
            && average(c).is_some_and(|a| (a - mean).abs() <= 1e-6)
    });
    Outcome {
        sahar_assessing: (recovery.executed, full),
        sahar_ineffective,
        recovery_pending: (recovery.executed, ready.map(|c| c.event)),
        unresolved: ready
            .filter(|c| average(c).is_some_and(|a| a > THRESHOLD))
            .map(|c| vec![c.event])
            .unwrap_or_default(),
    }
}

/// Rewrite the entropy evidence of every record: `post` gives each
/// post-execution cycle `k` its model and entropy, its history the entropies
/// of cycles `0..=k` and its average `average(k, mean)`; `before` rewrites
/// the tuples written up to the execution. Each tuple is rewritten the same
/// way on every record that carries it.
fn rewrite_entropies(
    trial: &Trial,
    recovery: &Recovery,
    post: &dyn Fn(usize) -> (i64, f64),
    average: &dyn Fn(usize, f64) -> Option<f64>,
    before: &dyn Fn(&str, &mut Json),
) -> Journal {
    let index =
        |seq: u64, pick: fn(&Cycle) -> u64| recovery.cycles.iter().position(|c| pick(c) == seq);
    (trial.after() + 1..=trial.through()).fold(trial.journal(), |journal, seq| {
        journal.edit(seq, |record| {
            shape::tuples_mut(record, &mut |tuple| {
                let written = tuple["ref"]["global_write_sequence"].as_u64().unwrap();
                let key = tuple["ref"]["key"].as_str().unwrap().to_owned();
                let fields = &mut tuple["value"];
                if written <= recovery.executed {
                    before(&key, fields);
                    return;
                }
                if let Some(k) = index(written, |c| c.detection) {
                    let (model, entropy) = post(k);
                    fields["model_id"] = json!(model);
                    fields["screw_entropy"] = json!(entropy);
                } else if let Some(k) = index(written, |c| c.history) {
                    fields["values"] = json!((0..=k).map(|i| post(i).1).collect::<Vec<_>>());
                } else if let Some(k) = index(written, |c| c.average) {
                    let mean = (0..=k).map(|i| post(i).1).sum::<f64>() / (k + 1) as f64;
                    fields["value"] = match average(k, mean) {
                        Some(a) => json!(a),
                        None => Json::Null,
                    };
                }
            })
        })
    })
}

/// The four recovery boundaries the case study retired for want of
/// hand-edited evidence (review 33 addenda; case-study scenarios §9), on the
/// long-run fragment with coherent edits that keep every raw record and
/// original position. Expected outcomes come from `expected_outcome`.
///
/// - (a) No acceptance-time baseline is needed: the verdict judges the
///   deployed window against the threshold alone. The pre-swap evidence is
///   rewritten to an absent average and a much better (0.05) or much worse
///   (0.95) history, without changing either verdict.
/// - (b) A sample from another model than the deployed one is not fresh:
///   with one of ten degraded samples by model 3, no tenth fresh sample
///   completes in the fragment, so the recovery stays pending with no
///   verdict. Sahar, which asks nothing of the model, still finds the
///   effect ineffective.
/// - (c) An out-of-domain sample (entropy 1.5) is not fresh: the recovery
///   stays pending, and the raw values are visible as `bad_uncertainty` on
///   every Write carrying them.
/// - (d) A mean that disagrees with its window: averages of 0.5 over
///   windows of 0.15 never close the recovery and are each a `bad_window`.
///   Sahar closes on its first full window and judges the written average.
#[test]
fn recovery_boundaries_follow_fresh_valid_coherent_deployed_samples() {
    let trial = long_effect_fragment();
    let recovery = recovery_of(&trial, &trial.journal());
    assert_eq!(recovery.cycles.len(), 10, "{:?}", recovery.cycles);
    let deployed = recovery.deployed;
    let worse_baseline = |key: &str, payload: &mut Json| match key {
        "EntropyHistory" => {
            for v in payload["values"].as_array_mut().unwrap() {
                *v = json!(0.95);
            }
        }
        "RunningAvgEntropy" => payload["value"] = Json::Null,
        "Detections" if !payload["screw_entropy"].is_null() => {
            payload["screw_entropy"] = json!(0.95)
        }
        _ => {}
    };
    let better_baseline = |key: &str, payload: &mut Json| match key {
        "EntropyHistory" => {
            for v in payload["values"].as_array_mut().unwrap() {
                *v = json!(0.05);
            }
        }
        "RunningAvgEntropy" => payload["value"] = Json::Null,
        "Detections" if !payload["screw_entropy"].is_null() => {
            payload["screw_entropy"] = json!(0.05)
        }
        _ => {}
    };
    let unchanged = |_: &str, _: &mut Json| {};
    let mean = |_: usize, mean: f64| Some(mean);
    let cases: Vec<(&str, Journal)> = vec![
        (
            "(a) degraded deployment over a better, absent-average baseline",
            rewrite_entropies(
                &trial,
                &recovery,
                &|_| (deployed, 0.5),
                &mean,
                &better_baseline,
            ),
        ),
        (
            "(a) recovered deployment over a worse, absent-average baseline",
            rewrite_entropies(
                &trial,
                &recovery,
                &|_| (deployed, 0.15),
                &mean,
                &worse_baseline,
            ),
        ),
        (
            "(b) one degraded sample from another model",
            rewrite_entropies(
                &trial,
                &recovery,
                &|k| (if k == 4 { 3 } else { deployed }, 0.5),
                &mean,
                &unchanged,
            ),
        ),
        (
            "(c) one out-of-domain sample",
            rewrite_entropies(
                &trial,
                &recovery,
                &|k| (deployed, if k == 4 { 1.5 } else { 0.15 }),
                &mean,
                &unchanged,
            ),
        ),
        (
            "(d) averages disagreeing with their windows",
            rewrite_entropies(
                &trial,
                &recovery,
                &|_| (deployed, 0.15),
                &|_, _| Some(0.5),
                &unchanged,
            ),
        ),
    ];
    let scratch = Scratch::new("recovery-boundaries");
    let row = |seq: u64| (seq - trial.after() - 1) as usize;
    let span = |(from, until): (u64, Option<u64>)| -> Vec<usize> {
        (row(from)..until.map_or(row(trial.through()) + 1, row)).collect()
    };
    let runs: Vec<(CliRun, CliRun)> = std::thread::scope(|scope| {
        let handles: Vec<_> = cases
            .iter()
            .enumerate()
            .map(|(index, (_, journal))| {
                let file = scratch.write(&format!("case-{index}.jsonl"), journal.bytes());
                let trial = &trial;
                scope.spawn(move || {
                    let config = record_only_config(trial, &file);
                    (
                        run_cli(&spec("sahar"), &config, &["--runtime", "dataflow"]),
                        run_cli(&spec("strengthened"), &config, &["--runtime", "dataflow"]),
                    )
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut outcomes = Vec::new();
    for ((what, journal), (sahar, strengthened)) in cases.iter().zip(&runs) {
        // The edits keep every record of every cycle in place.
        let edited = recovery_of(&trial, journal);
        assert_eq!(
            edited.cycles.iter().map(|c| c.event).collect::<Vec<_>>(),
            recovery.cycles.iter().map(|c| c.event).collect::<Vec<_>>()
        );
        let expected = expected_outcome(&edited, journal);
        for run in [sahar, strengthened] {
            assert_eq!(run.code, Some(0), "{what}: {}", run.stderr);
            assert!(run.true_rows("journal_invalid").is_empty(), "{what}");
            assert!(run.true_rows("startup_incomplete").is_empty(), "{what}");
        }
        assert_eq!(
            sahar.true_rows("assessing"),
            span(expected.sahar_assessing),
            "{what}"
        );
        assert_eq!(
            sahar.true_rows("ineffective"),
            expected
                .sahar_ineffective
                .iter()
                .map(|s| row(*s))
                .collect::<Vec<_>>(),
            "{what}"
        );
        assert_eq!(
            strengthened.true_rows("recovery_pending"),
            span(expected.recovery_pending),
            "{what}"
        );
        assert_eq!(
            strengthened.true_rows("unresolved_degradation"),
            expected
                .unresolved
                .iter()
                .map(|s| row(*s))
                .collect::<Vec<_>>(),
            "{what}"
        );
        outcomes.push(expected);
    }
    // The oracle's own reading of each boundary.
    let tenth = recovery.cycles[9];
    assert_eq!(outcomes[0].unresolved, [tenth.event]);
    assert_eq!(outcomes[0].sahar_ineffective, [tenth.average]);
    assert!(outcomes[1].unresolved.is_empty() && outcomes[1].sahar_ineffective.is_empty());
    assert_eq!(outcomes[1].recovery_pending.1, Some(tenth.event));
    for boundary in &outcomes[2..] {
        assert_eq!(
            boundary.recovery_pending.1, None,
            "no verdict: {boundary:?}"
        );
        assert!(boundary.unresolved.is_empty());
        assert_eq!(boundary.sahar_assessing.1, Some(tenth.average));
    }
    assert_eq!(outcomes[2].sahar_ineffective, [tenth.average]);
    assert!(
        outcomes[3].sahar_ineffective.is_empty(),
        "the mean 0.285 is below 0.35"
    );
    assert_eq!(outcomes[4].sahar_ineffective, [tenth.average]);

    // Raw occurrences stay visible: the out-of-domain value on every Write
    // carrying it, and each disagreeing average.
    let (_, strengthened) = &runs[3];
    let out_of_domain: Vec<usize> = std::iter::once(recovery.cycles[4].detection)
        .chain(recovery.cycles[4..].iter().map(|c| c.history))
        .map(row)
        .collect();
    assert_eq!(strengthened.true_rows("bad_uncertainty"), out_of_domain);
    let (_, strengthened) = &runs[4];
    assert_eq!(
        strengthened.true_rows("bad_window"),
        recovery
            .cycles
            .iter()
            .map(|c| row(c.average))
            .collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------
// The application's own protocol tables in `evidence.dsrv`.
// ---------------------------------------------------------------------------

/// A root beside copies of the example's modules that prints the Tick's
/// status and protocol facts.
const TICK_PROBE: &str = r#"use experimental::high_level_dsrv
mod declarations
mod schema
mod evidence
mod v2_adapter
mod observation_clock
use declarations::*
use schema::*
use evidence::{self, Tick}
use v2_adapter
use rpio::journal::view::timing::{Observation}
in journal_input: v2_adapter::Input
aux journal: Observation<Knowledge, Message, Trigger, JournalTime> = v2_adapter::adapt(journal_input)
aux tick: Tick<JournalTime> = evidence::tick(journal)
out journal_invalid: Bool = tick.status.journal_invalid
out missing_evidence: Bool = tick.protocol.missing_evidence
out missing_publication: Bool = tick.protocol.missing_publication
out backwards_clock: Bool = tick.status.backwards_clock
"#;

fn run_probe(trial: &Trial, journal: &Journal, spec_name: Option<&str>) -> Run {
    let scratch = Scratch::new("tick-probe");
    stage_modules(trial, scratch.path());
    let probe = match spec_name {
        Some(name) => scratch.file(&format!("{name}.dsrv")),
        None => scratch.write("probe.dsrv", TICK_PROBE),
    };
    let capture = native_capture(trial, journal);
    let file = scratch.write("journal.jsonl", &capture.bytes);
    let config = native_config(
        trial,
        &file,
        &scratch.file("provenance.jsonl"),
        &capture,
        false,
    );
    let mut run = Checker {
        binary: env!("CARGO_BIN_EXE_trustworthiness_checker").into(),
        cwd: root(),
    }
    .run(&probe, &config, "dataflow", trial.after());
    capture.map_run(&mut run);
    assert!(run.success, "{}", run.stderr);
    run
}

/// The evidence and publication tables are checks of the application,
/// judged on the record that shows a fault:
///
/// - an Event without a slot its role and topic require, and an End whose
///   outcome names a version of another key than its field requires, are
///   missing evidence, which makes the journal invalid from then on (in the
///   roots' Tick and in PV-A alike);
/// - an Analysis that ends with an anomaly it never published is a missing
///   publication;
/// - a phase record admitted before the previous one is a backwards clock.
#[test]
fn the_application_tables_report_on_the_record_that_shows_them() {
    let trial = trial("adapt");
    let observation = events(&trial, "observation_recorded")[1];
    let anomaly_end = trial.positions(|r| {
        shape::is_end_of(r, "Analysis")
            && r["payload"]["step"]["payload"]["outcome"]["payload"]["$tag"] == "Anomaly"
    })[0];
    let analysis = shape::start_seq(&trial.record(anomaly_end)).unwrap();
    // A Detections version the same Analysis wrote: acquired, wrong key.
    let detections = trial.positions(|r| {
        shape::start_seq(r) == Some(analysis)
            && shape::written(r).is_some_and(|w| w["ref"]["key"] == "Detections")
    })[0];
    let detections = shape::written(&trial.record(detections)).unwrap().clone();
    let journal = trial
        .journal()
        .edit(observation, |r| {
            r["payload"]["evidence"]
                .as_object_mut()
                .unwrap()
                .remove("frame");
        })
        .edit(anomaly_end, |r| {
            r["payload"]["step"]["payload"]["outcome"]["payload"]["payload"]["evidence"] =
                detections.clone()
        });
    let run = run_probe(&trial, &journal, None);
    assert_eq!(
        run.true_positions("missing_evidence"),
        [observation, anomaly_end]
    );
    assert_eq!(
        run.true_positions("journal_invalid").first(),
        Some(&observation)
    );
    assert!(
        run.flag("journal_invalid", run.provenance.len() - 1),
        "sticky"
    );
    assert!(run.true_rows("missing_publication").is_empty());
    let pv = run_probe(&trial, &journal, Some("pv-a"));
    assert_eq!(
        pv.true_positions("journal_invalid").first(),
        Some(&observation)
    );

    // A nominal Analysis now ends with an anomaly it never published, citing
    // the average it wrote.
    let trial = self::trial("nominal");
    let end = trial.positions(|r| shape::is_end_of(r, "Analysis"))[3];
    let analysis = shape::start_seq(&trial.record(end)).unwrap();
    let average = trial.positions(|r| {
        shape::start_seq(r) == Some(analysis)
            && shape::written(r).is_some_and(|w| w["ref"]["key"] == "RunningAvgEntropy")
    })[0];
    let average = shape::written(&trial.record(average)).unwrap().clone();
    let later = trial.positions(|r| shape::is_start_of(r, "Monitor"))[6];
    let earlier = trial.record(later - 1)["arrival"]["journal_time"]
        .as_f64()
        .unwrap();
    let journal = trial
        .journal()
        .edit(end, |r| {
            r["payload"]["step"]["payload"]["outcome"] =
                json!({"$tag": "Analysed", "payload": {"$tag": "Anomaly", "payload": {"evidence": average}}})
        })
        .edit(later, |r| r["arrival"]["journal_time"] = json!(earlier - 1.0));
    let run = run_probe(&trial, &journal, None);
    assert_eq!(run.true_positions("missing_publication"), [end]);
    assert!(run.true_rows("missing_evidence").is_empty());
    assert!(run.true_rows("journal_invalid").is_empty());
    assert_eq!(run.true_positions("backwards_clock"), [later]);
}
