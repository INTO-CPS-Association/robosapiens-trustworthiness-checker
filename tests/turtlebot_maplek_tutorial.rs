//! Executable evidence for the TurtleBot MAPLE-K tutorial.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

fn replay(
    spec: &str,
    scenario: &str,
    runtime: &str,
    semantics: &str,
) -> BTreeMap<String, Vec<String>> {
    let suffix = if spec == "lifecycle" {
        "-lifecycle"
    } else {
        ""
    };
    let output = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
        .args([
            &format!("examples/turtlebot-maplek/{spec}.dsrv"),
            "--input-config",
            &format!("examples/turtlebot-maplek/{scenario}{suffix}-input.json5"),
            "--output-stdout",
            "--runtime",
            runtime,
            "--semantics",
            semantics,
        ])
        .env("RUST_LOG", "error")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("tutorial replay starts");
    assert!(
        output.status.success(),
        "{spec}/{scenario}/{runtime}/{semantics}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut columns = BTreeMap::<String, BTreeMap<usize, String>>::new();
    for line in String::from_utf8(output.stdout).unwrap().lines() {
        let (key, value) = line.split_once(" = ").expect("output assignment");
        let (name, tick) = key.split_once('[').expect("tick-indexed output");
        let tick = tick.trim_end_matches(']').parse().unwrap();
        assert!(
            columns
                .entry(name.to_owned())
                .or_default()
                .insert(tick, value.to_owned())
                .is_none(),
            "one value per stream per tick"
        );
    }
    let records = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("examples/turtlebot-maplek/native/{scenario}.jsonl")),
    )
    .unwrap()
    .lines()
    .count();
    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
            "examples/turtlebot-maplek/native/{scenario}-manifest.json"
        )))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(manifest["records"].as_u64().unwrap() as usize, records);
    // Native commit insertion changes physical extent. The mapping must still
    // identify each archived semantic fact with its corresponding native row.
    let archived = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("examples/turtlebot-maplek/{scenario}.jsonl")),
    )
    .unwrap();
    let native = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("examples/turtlebot-maplek/native/{scenario}.jsonl")),
    )
    .unwrap();
    assert_eq!(
        manifest["archived_sha256"].as_str().unwrap(),
        format!("{:x}", Sha256::digest(archived.as_bytes()))
    );
    let native_rows: Vec<serde_json::Value> = native
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let mapping = manifest["semantic_record_ticks"].as_array().unwrap();
    assert_eq!(mapping.len(), archived.lines().count());
    for (line, tick) in archived.lines().zip(mapping) {
        let old: serde_json::Value = serde_json::from_str(line).unwrap();
        let new = &native_rows[tick.as_u64().unwrap() as usize];
        assert_eq!(old["$tag"], new["$tag"]);
        if old["$tag"] == "Phase" {
            assert_eq!(
                old["payload"]["activation"]["role"],
                new["payload"]["activation"]["role"]
            );
            assert_eq!(
                old["payload"]["step"]["$tag"],
                new["payload"]["step"]["$tag"]
            );
        }
    }
    let ticks = records + usize::from(spec == "lifecycle" && scenario == "pending-service") * 2;
    assert!(!columns.is_empty());
    columns
        .into_iter()
        .map(|(name, values)| {
            assert_eq!(
                values.keys().copied().collect::<Vec<_>>(),
                (0..ticks).collect::<Vec<_>>()
            );
            (name, values.into_values().collect())
        })
        .collect()
}

#[test]
fn loop_properties_distinguish_success_counterexamples_and_unfinished_work() {
    for semantics in ["typed-untimed", "gradual-typed-untimed"] {
        for scenario in ["accepted", "replanned", "nominal"] {
            let observed = replay("properties", scenario, "dataflow", semantics);
            assert!(
                observed["adaptation_requirements_held"]
                    .iter()
                    .all(|v| v == "true")
            );
            assert_eq!(
                observed["plans_applied"].last().unwrap(),
                if scenario == "nominal" { "0" } else { "1" }
            );
            assert_eq!(observed["adaptation_in_progress"].last().unwrap(), "false");
            assert_eq!(
                observed,
                replay("properties", scenario, "semi-sync", semantics)
            );
        }

        for (scenario, property) in [
            ("false-anomaly", "anomaly_has_occlusion"),
            ("external-plan", "plan_written_by_planner"),
            ("missing-read", "plan_read_current_mask_values"),
            ("stale", "plan_read_newest_masks"),
            ("same-value-update", "plan_read_newest_masks"),
            ("unsafe", "accepted_plan_within_limits"),
            ("ineffective", "accepted_plan_within_limits"),
            ("unvalidated", "execution_authorised"),
            ("wrong-effect", "applied_config_matches_plan"),
            ("retry-exhausted", "rejections_within_limit"),
            ("no-plan", "episode_closes_applied"),
            ("not-delivered", "episode_closes_applied"),
            ("withdrawn", "episode_closes_applied"),
            ("overlap", "protocol_followed"),
        ] {
            let observed = replay("properties", scenario, "dataflow", semantics);
            assert!(
                observed[property].iter().any(|v| v == "false"),
                "{scenario}: {property}"
            );
            assert_eq!(
                observed["adaptation_requirements_held"].last().unwrap(),
                "false"
            );
            if scenario == "same-value-update" {
                assert!(
                    observed["plan_read_current_mask_values"]
                        .iter()
                        .all(|v| v == "true")
                );
            }
            assert_eq!(
                observed,
                replay("properties", scenario, "semi-sync", semantics)
            );
        }
        let slow = replay("properties", "slow", "dataflow", semantics);
        assert!(slow["planning_on_time"].iter().any(|v| v == "false"));
        assert_eq!(
            slow["adaptation_requirements_held"].last().unwrap(),
            "false"
        );
        assert_eq!(slow, replay("properties", "slow", "semi-sync", semantics));

        let unfinished = replay("properties", "pending-service", "dataflow", semantics);
        assert_eq!(
            unfinished["adaptation_requirements_held"].last().unwrap(),
            "true"
        );
        assert_eq!(unfinished["adaptation_in_progress"].last().unwrap(), "true");
        assert_eq!(unfinished["plans_applied"].last().unwrap(), "0");
        assert_eq!(
            unfinished,
            replay("properties", "pending-service", "semi-sync", semantics)
        );
    }
}

/// Native physical record index, including preceding Knowledge commit rows.
/// This deliberately does not reuse archived semantic row or write sequence.
fn tick_of(scenario: &str, role: &str, step: &str) -> usize {
    std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("examples/turtlebot-maplek/native/{scenario}.jsonl")),
    )
    .unwrap()
    .lines()
    .position(|line| {
        let record: serde_json::Value = serde_json::from_str(line).unwrap();
        record["payload"]["activation"]["role"] == role && record["payload"]["step"]["$tag"] == step
    })
    .unwrap_or_else(|| panic!("{scenario} has no {role} {step}"))
}

#[test]
fn lifecycle_properties_distinguish_completion_failure_overlap_and_silence() {
    for semantics in ["typed-untimed", "gradual-typed-untimed"] {
        let accepted = replay("lifecycle", "accepted", "dataflow", semantics);
        let plan_end = tick_of("accepted", "Plan", "End");
        let legitimate_start = tick_of("accepted", "Legitimate", "Start");
        let delivered = tick_of("accepted", "Execute", "ServiceCompleted");
        assert_eq!(accepted["plan_callback_ended"][plan_end], "true");
        assert_eq!(accepted["handoff_resolved"][legitimate_start], "true");
        assert_eq!(accepted["handoff_sequential"][legitimate_start], "true");
        assert_eq!(accepted["apply_spin_ended"][delivered], "true");
        assert_eq!(accepted["apply_spin_delivered"][delivered], "true");
        assert!(accepted["apply_spin_on_time"].iter().all(|v| v == "true"));
        assert_eq!(
            accepted,
            replay("lifecycle", "accepted", "semi-sync", semantics)
        );

        for (scenario, property) in [
            ("not-delivered", "apply_spin_delivered"),
            ("owner-ended", "apply_spin_delivered"),
            ("service-conflict", "apply_spin_single_request"),
            ("overlap", "handoff_sequential"),
            ("withdrawn", "handoff_sequential"),
        ] {
            let observed = replay("lifecycle", scenario, "dataflow", semantics);
            assert!(
                observed[property].iter().any(|v| v == "false"),
                "{scenario}: {property}"
            );
            if scenario == "withdrawn" {
                assert!(observed["handoff_withdrawn"].iter().any(|v| v == "true"));
            }
            if matches!(scenario, "owner-ended" | "service-conflict") {
                let adaptation = replay("properties", scenario, "dataflow", semantics);
                assert_eq!(
                    adaptation["adaptation_requirements_held"].last().unwrap(),
                    "true"
                );
                assert_eq!(adaptation["plans_applied"].last().unwrap(), "1");
                assert_eq!(
                    adaptation["adaptation_in_progress"].last().unwrap(),
                    "false"
                );
            }
            assert_eq!(
                observed,
                replay("lifecycle", scenario, "semi-sync", semantics)
            );
        }
        let pending = replay("lifecycle", "pending-service", "dataflow", semantics);
        // The last two ticks are the clock samples after the final record.
        let clocks = pending["apply_spin_pending"].len() - 2;
        assert_eq!(&pending["apply_spin_pending"][clocks..], ["true", "true"]);
        assert_eq!(&pending["apply_spin_on_time"][clocks..], ["false", "false"]);
        assert!(pending["apply_spin_ended"].iter().all(|v| v == "false"));
        assert_eq!(
            pending,
            replay("lifecycle", "pending-service", "semi-sync", semantics)
        );
    }
}
