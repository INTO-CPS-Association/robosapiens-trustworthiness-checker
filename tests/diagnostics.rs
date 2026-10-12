//! Diagnostics observe dataflow execution without changing it.
//!
//! Every test compares an observed monitor with an unobserved twin, or inspects the report a
//! recorder produced for a known trace. Run with `cargo test --features diagnostics --test
//! diagnostics`; add `jit` for the native-tier cases.
#![cfg(feature = "diagnostics")]

use std::process::Command;

use serde_json::Value as Json;
use trustworthiness_checker::dataflow::{
    ContextTransferPolicy, DataflowEvaluationError, DataflowMonitor, DataflowProgram,
};
use trustworthiness_checker::diagnostics::{DiagnosticsConfig, SnapshotSkipped, TickRange};
use trustworthiness_checker::dsrv_fixtures::elaborated;
use trustworthiness_checker::lang::dsrv::program::load_program_file;
use trustworthiness_checker::{ElaboratedDsrvSpecification, TypeCheckOptions, Value};

const DEMO: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/diagnostics/profile_demo.dsrv"
);
const DEMO_INPUT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/diagnostics/profile_demo.input"
);

fn demo_specification() -> ElaboratedDsrvSpecification {
    let program = smol::block_on(load_program_file(DEMO, Default::default())).unwrap();
    program
        .specification
        .check_and_elaborate(TypeCheckOptions::GRADUAL)
        .into_parts()
        .0
        .unwrap()
}

/// The demo trace as monitor input rows in `program`'s input order.
fn demo_rows(program: &DataflowProgram) -> Vec<Vec<Value>> {
    let text = std::fs::read_to_string(DEMO_INPUT).unwrap();
    let mut rows: Vec<Vec<Value>> = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        let assignment = match line.split_once(':') {
            Some((tick, rest)) if tick.chars().all(|c| c.is_ascii_digit()) => {
                rows.push(vec![Value::NoVal; program.input_vars().len()]);
                rest
            }
            _ => line,
        };
        let (name, value) = assignment.split_once('=').unwrap();
        let index = program
            .input_vars()
            .iter()
            .position(|var| var.name() == name.trim())
            .unwrap();
        let value = value.trim();
        rows.last_mut().unwrap()[index] = if let Some(text) = value.strip_prefix('"') {
            Value::Str(text.trim_end_matches('"').into())
        } else if let Some(items) = value.strip_prefix('[') {
            Value::List(
                items
                    .trim_end_matches(']')
                    .split(',')
                    .filter(|item| !item.trim().is_empty())
                    .map(|item| Value::Int(item.trim().parse().unwrap()))
                    .collect(),
            )
        } else {
            Value::Int(value.parse().unwrap())
        };
    }
    rows
}

type Outcome = Vec<Result<Vec<Value>, String>>;

/// Evaluate every row, recording each output row or error. Stops after the first error, which a
/// monitor treats as terminal, but evaluates one more row to capture the terminal error.
fn run(monitor: &mut DataflowMonitor, rows: &[Vec<Value>]) -> Outcome {
    let mut outcome = Vec::new();
    for row in rows {
        let mut output = vec![Value::NoVal; monitor.output_vars().len()];
        match monitor.evaluate(row, &mut output) {
            Ok(()) => outcome.push(Ok(output)),
            Err(error) => {
                outcome.push(Err(error.to_string()));
                let again = monitor.evaluate(row, &mut output).unwrap_err();
                outcome.push(Err(again.to_string()));
                break;
            }
        }
    }
    outcome
}

fn config() -> DiagnosticsConfig {
    let mut config = DiagnosticsConfig::default();
    config.memory.every_ticks = Some(10);
    config.trace.streams = vec!["level".into(), "recent".into()];
    config
}

fn report(monitor: &DataflowMonitor) -> Json {
    monitor.diagnostics_report().unwrap().to_json()
}

fn observed_and_plain(
    program: &DataflowProgram,
    configure: impl Fn(&mut DataflowMonitor),
) -> (DataflowMonitor, DataflowMonitor) {
    let mut observed = DataflowMonitor::from_program(program.clone());
    configure(&mut observed);
    observed.enable_diagnostics(&config());
    let mut plain = DataflowMonitor::from_program(program.clone());
    configure(&mut plain);
    (observed, plain)
}

#[test]
fn outputs_are_unchanged_with_and_without_quickening() {
    let program = DataflowProgram::compile_checked(demo_specification()).unwrap();
    let rows = demo_rows(&program);
    for quickening in [true, false] {
        let (mut observed, mut plain) =
            observed_and_plain(&program, |monitor| monitor.set_quickening(quickening));
        assert_eq!(
            run(&mut observed, &rows),
            run(&mut plain, &rows),
            "{quickening}"
        );
        assert_eq!(observed.revision(), plain.revision());
        let report = report(&observed);
        assert_eq!(report["ticks"]["succeeded"], rows.len() as u64);
        assert_eq!(report["ticks"]["failed"], 0);
    }
}

#[test]
fn a_failed_tick_keeps_its_error_and_commit_boundary() {
    let source = "in x: Int\nin source: Str\nout y: Int\nout total: Int\n\
                  y = dynamic(source: Int, {x})\ntotal = default(total[1], 0) + x";
    let program = DataflowProgram::compile_checked(elaborated(source)).unwrap();
    let rows = [
        vec![Value::Int(1), Value::Str("x + 1".into())],
        vec![Value::Int(2), Value::NoVal],
        vec![Value::Int(3), Value::Str("x +".into())],
        vec![Value::Int(4), Value::NoVal],
    ];
    let (mut observed, mut plain) = observed_and_plain(&program, |_| {});
    let outcome = run(&mut observed, &rows);
    assert_eq!(outcome, run(&mut plain, &rows));
    assert!(
        matches!(outcome.last(), Some(Err(message)) if message == &DataflowEvaluationError::MonitorFailed.to_string())
    );

    let report = report(&observed);
    assert_eq!(report["ticks"]["succeeded"], 2);
    assert_eq!(
        report["ticks"]["failed"], 1,
        "the terminal rejection is not a tick"
    );
    assert_eq!(report["events"]["tick_failed"], 1);
    assert_eq!(report["dynamic"]["by_outcome"]["activated"]["count"], 1);
    assert_eq!(report["dynamic"]["by_outcome"]["error"]["count"], 1);
    // Commit ran only for the two successful ticks.
    assert_eq!(report["ticks"]["commit"]["count"], 2);
}

#[test]
fn steps_attribute_time_to_streams_and_inlined_definitions() {
    let program = DataflowProgram::compile_checked(demo_specification()).unwrap();
    let rows = demo_rows(&program);
    let (mut observed, _) = observed_and_plain(&program, |monitor| monitor.set_quickening(false));
    run(&mut observed, &rows);
    let report = report(&observed);

    let plans = report["plans"].as_array().unwrap();
    assert!(!plans.is_empty());
    for plan in plans {
        for step in plan["steps"].as_array().unwrap() {
            assert_eq!(step["kind"], "graph");
            assert_eq!(
                step["paths"]
                    .as_object()
                    .unwrap()
                    .keys()
                    .collect::<Vec<_>>(),
                ["canonical"]
            );
        }
    }
    let streams = report["streams"].as_array().unwrap();
    let level = streams
        .iter()
        .find(|stream| stream["stream"] == "level")
        .unwrap();
    assert_eq!(level["total"]["count"], rows.len() as u64);
    assert!(
        level["site"]["file"]
            .as_str()
            .unwrap()
            .ends_with("profile_demo.dsrv")
    );
    // Lines and columns come from an index of line starts: `level = clamp(...)` is line 19.
    assert_eq!(level["site"]["line"], 19);
    assert_eq!(level["site"]["column"], 9);

    // `clamp` was written in demo_lib.dsrv and inlined at its call in `level`.
    let sites = report["definition_sites"].as_array().unwrap();
    let clamp = sites
        .iter()
        .find(|site| {
            site["definition"]["snippet"]
                .as_str()
                .unwrap()
                .starts_with("if x < lo")
        })
        .expect("clamp's body is a definition site");
    assert!(
        clamp["definition"]["file"]
            .as_str()
            .unwrap()
            .ends_with("demo_lib.dsrv")
    );
    assert_eq!(clamp["call_sites"][0]["stream"], "level");
    assert_eq!(
        clamp["call_sites"][0]["call_site"]["snippet"],
        "clamp(score, 0, 100)"
    );

    // The model map lists nodes of nested branches and functions with their sites.
    let definition = &report["definitions"][0];
    let level = definition["streams"]
        .as_array()
        .unwrap()
        .iter()
        .find(|stream| stream["name"] == "level")
        .unwrap();
    let nodes = level["nodes"].as_array().unwrap();
    assert!(
        nodes
            .iter()
            .any(|node| node["path"].as_str().unwrap().contains("/else/"))
    );
    assert!(nodes.iter().all(|node| node["call_site"].is_object()));
}

#[test]
fn source_sites_do_not_change_semantic_identity() {
    let compact =
        DataflowProgram::compile_checked(elaborated("in x: Int\nout y: Int\ny = x + 1")).unwrap();
    let spaced = DataflowProgram::compile_checked(elaborated(
        "in x: Int\n\n// moved\nout y: Int\ny =     x   +   1",
    ))
    .unwrap();
    assert_eq!(compact.definition_key(), spaced.definition_key());
    let site = |program: &DataflowProgram| {
        let mut monitor = DataflowMonitor::from_program(program.clone());
        monitor.enable_diagnostics(&DiagnosticsConfig::default());
        let report = report(&monitor);
        assert_eq!(
            report["definitions"][0]["definition_key"],
            program.definition_key().to_string()
        );
        report["definitions"][0]["streams"][0]["site"]["start"].clone()
    };
    assert_ne!(
        site(&compact),
        site(&spaced),
        "the sites differ while the identity does not"
    );
}

#[test]
fn memory_snapshots_follow_growing_lists_and_charge_shared_maps_once() {
    let program = DataflowProgram::compile_checked(demo_specification()).unwrap();
    let rows = demo_rows(&program);
    let (mut observed, _) = observed_and_plain(&program, |_| {});
    run(&mut observed, &rows);
    let report = report(&observed);
    let snapshots = report["memory"]["snapshots"].as_array().unwrap();
    assert_eq!(snapshots.len(), 4, "one every ten of forty ticks");
    let recent_state = |snapshot: &Json| -> u64 {
        snapshot["components"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|component| component["stream"] == "recent")
            .map(|component| component["max_list_len"].as_u64().unwrap())
            .max()
            .unwrap()
    };
    assert_eq!(recent_state(&snapshots[0]), 10);
    assert_eq!(recent_state(&snapshots[3]), 40);
    let map_entries = snapshots[0]["total"]["map_entries"].clone();
    for snapshot in snapshots {
        assert_eq!(snapshot["budget_exhausted"], false);
        // The summary map grows by key, not by tick: its persistent entries stay constant.
        assert_eq!(snapshot["total"]["map_entries"], map_entries);
        // The published summary map is charged where it is first reached and only reported as
        // shared by the delay state and history holding further handles to the same root.
        for path in ["streams/summary/state", "history/summary (depth 1)"] {
            let component = snapshot["components"]
                .as_array()
                .unwrap()
                .iter()
                .find(|component| component["path"] == path)
                .unwrap();
            assert_eq!(component["map_logical_bytes"], 0, "{path}");
            assert!(component["shared_bytes"].as_u64().unwrap() > 0, "{path}");
        }
    }

    let explicit = observed.record_memory_snapshot("explicit").unwrap();
    assert_eq!(explicit.label, "explicit");
    assert_eq!(explicit.tick, rows.len() as u64);
}

#[test]
fn bounded_capture_labels_what_it_drops() {
    let program = DataflowProgram::compile_checked(demo_specification()).unwrap();
    let rows = demo_rows(&program);
    let mut config = DiagnosticsConfig::default();
    config.trace.capacity = 5;
    let mut monitor = DataflowMonitor::from_program(program.clone());
    monitor.enable_diagnostics(&config);
    run(&mut monitor, &rows);
    let report = report(&monitor);
    assert_eq!(report["timeline"]["retained"], 5);
    assert!(report["timeline"]["dropped"].as_u64().unwrap() > 0);
    // Aggregates are complete even though the timeline is not.
    assert_eq!(report["ticks"]["succeeded"], rows.len() as u64);

    let mut config = DiagnosticsConfig::default();
    config.trace.ticks = TickRange::NONE;
    let mut monitor = DataflowMonitor::from_program(program);
    monitor.enable_diagnostics(&config);
    run(&mut monitor, &rows);
    let timeline = monitor.diagnostics_timeline().unwrap();
    assert!(
        timeline
            .events()
            .iter()
            .all(|event| event.category != "tick" && event.category != "step")
    );
    assert!(
        timeline
            .events()
            .iter()
            .any(|event| event.name == "dynamic_activation")
    );
}

#[test]
fn selected_values_respect_no_val_and_the_tick_range() {
    let program =
        DataflowProgram::compile_checked(elaborated("in x: Int\nout y: Int\ny = x * 2")).unwrap();
    let mut config = DiagnosticsConfig::default();
    config.trace.streams = vec!["y".into()];
    config.trace.ticks = TickRange::new(0, Some(2));
    let mut monitor = DataflowMonitor::from_program(program);
    monitor.enable_diagnostics(&config);
    let rows = [vec![Value::NoVal], vec![Value::Int(3)], vec![Value::Int(4)]];
    run(&mut monitor, &rows);
    let values = monitor
        .diagnostics_timeline()
        .unwrap()
        .events()
        .iter()
        .filter(|event| event.category == "values")
        .map(|event| event.args.as_ref().unwrap().clone())
        .collect::<Vec<_>>();
    assert_eq!(values.len(), 2);
    assert_eq!(values[0]["tick"], 0);
    assert_eq!(values[0]["values"]["y"], "no_val");
    assert_eq!(values[1]["values"]["y"], "6");
}

#[test]
fn reconfiguration_and_reset_start_new_definition_versions() {
    let first =
        DataflowProgram::compile_checked(elaborated("in x: Int\nout y: Int\ny = x + 1")).unwrap();
    let second =
        DataflowProgram::compile_checked(elaborated("in x: Int\nout y: Int\ny = x * 10")).unwrap();
    let mut monitor = DataflowMonitor::from_program(first);
    monitor.enable_diagnostics(&DiagnosticsConfig::default());
    let mut output = vec![Value::NoVal];
    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    monitor
        .reconfigure(second, ContextTransferPolicy::MatchingStreamState)
        .unwrap();
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(20)]);
    monitor.reset();
    monitor.evaluate(&[Value::Int(3)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(4)]);

    let report = report(&monitor);
    let versions = report["definitions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|definition| definition["version"].as_u64().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(versions, [0, 1, 2]);
    assert_eq!(
        report["definitions"][0]["definition_key"],
        report["definitions"][2]["definition_key"]
    );
    assert_ne!(
        report["definitions"][0]["definition_key"],
        report["definitions"][1]["definition_key"]
    );
    let plan_versions = report["plans"]
        .as_array()
        .unwrap()
        .iter()
        .map(|plan| plan["definition_version"].as_u64().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        plan_versions,
        [0, 1, 2],
        "reused plan ids never merge across versions"
    );
    assert_eq!(report["ticks"]["succeeded"], 3);
    assert_eq!(report["events"]["reconfigured"], 1);
    assert_eq!(report["events"]["reset"], 1);
}

#[test]
fn fused_regions_are_aggregates_with_members() {
    let program = DataflowProgram::compile_checked(elaborated(
        "in x: Int\nin y: Int\nout a: Int\nout b: Int\nout c: Bool\n\
         a = x + y\nb = a * 2\nc = b > 10",
    ))
    .unwrap();
    let rows = (0..20)
        .map(|index| vec![Value::Int(index), Value::Int(1)])
        .collect::<Vec<_>>();
    let (mut observed, mut plain) = observed_and_plain(&program, |_| {});
    assert_eq!(run(&mut observed, &rows), run(&mut plain, &rows));
    let report = report(&observed);
    let steps = report["plans"][0]["steps"].as_array().unwrap();
    let region = steps
        .iter()
        .find(|step| step["kind"] == "scalar_region")
        .expect("the scalar streams fuse into one region");
    assert_eq!(region["members"], serde_json::json!(["a", "b", "c"]));
    assert!(
        region["paths"]["region_quickened"]["count"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        report["streams"].as_array().unwrap().is_empty(),
        "no per-stream time is inferred for fused members"
    );
    assert_eq!(
        report["regions"][0]["members"],
        serde_json::json!(["a", "b", "c"])
    );
}

#[cfg(feature = "jit")]
#[test]
fn whole_schedule_native_is_one_aggregate_and_selection_is_unchanged() {
    use trustworthiness_checker::dataflow::JitConfig;

    let program = DataflowProgram::compile_checked(elaborated(
        "in x: Int\nout total: Int\nout doubled: Int\n\
         total = default(total[1], 0) + x\ndoubled = total * 2",
    ))
    .unwrap();
    let rows = (0..50)
        .map(|index| vec![Value::Int(index)])
        .collect::<Vec<_>>();
    for config in [JitConfig::eager(), JitConfig::after_events(10)] {
        let mut observed = DataflowMonitor::from_program_with_jit(program.clone(), config);
        observed.enable_diagnostics(&self::config());
        let mut plain = DataflowMonitor::from_program_with_jit(program.clone(), config);
        assert_eq!(run(&mut observed, &rows), run(&mut plain, &rows));
        assert_eq!(
            observed.jit_report(),
            plain.jit_report(),
            "tier selection is unchanged"
        );

        let report = report(&observed);
        let whole = report["plans"]
            .as_array()
            .unwrap()
            .iter()
            .find_map(|plan| plan.get("whole_schedule_native"))
            .expect("the whole schedule ran natively");
        assert_eq!(whole["members"], serde_json::json!(["total", "doubled"]));
        assert!(whole["paths"]["whole_native"]["count"].as_u64().unwrap() > 0);
        assert_eq!(report["tiers"]["jit"]["plan"], "WholeSchedule");
        let snapshot = report["memory"]["snapshots"]
            .as_array()
            .unwrap()
            .last()
            .unwrap();
        assert!(
            snapshot["components"]
                .as_array()
                .unwrap()
                .iter()
                .any(|component| component["opaque"] == true
                    && component["path"] == "execution/native_artifacts"),
            "native state is reported opaque rather than materialized"
        );
    }
}

/// Run the demo through the CLI with extra arguments, returning stdout, `summary.json` and
/// `trace.json`.
fn run_demo_cli(extra: &[&str]) -> (String, Json, Json) {
    let directory = std::env::temp_dir().join(format!(
        "tc-diagnostics-cli-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let output = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
        .args([
            DEMO,
            "--input-file",
            DEMO_INPUT,
            "--output-stdout",
            "--diagnostics-dir",
        ])
        .arg(&directory)
        .args(extra)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let read = |name: &str| -> Json {
        serde_json::from_slice(&std::fs::read(directory.join(name)).unwrap()).unwrap()
    };
    let (summary, trace) = (read("summary.json"), read("trace.json"));
    let _ = std::fs::remove_dir_all(directory);
    (String::from_utf8(output.stdout).unwrap(), summary, trace)
}

#[test]
fn the_cli_writes_a_summary_and_a_perfetto_trace() {
    let (stdout, summary, trace) = run_demo_cli(&[
        "--diagnostics-trace-streams",
        "level",
        "--diagnostics-label",
        "cli-test",
    ]);
    assert!(
        stdout.contains("total[39] = 2423"),
        "monitor output is unchanged"
    );

    assert_eq!(summary["schema"], "tc-diagnostics/1");
    assert_eq!(summary["run"]["label"], "cli-test");
    assert!(summary["run"]["debug_assertions"].is_boolean());
    assert!(
        !summary["run"]["features"]
            .as_array()
            .unwrap()
            .contains(&"debug_assertions".into()),
        "debug_assertions is a build setting, not a feature"
    );
    assert!(summary["error"].is_null());
    assert_eq!(summary["dataflow"]["ticks"]["succeeded"], 40);
    let phases = summary["pipeline"]["phases"].as_array().unwrap();
    let phase = |name: &str| phases.iter().find(|phase| phase["phase"] == name).unwrap();
    assert_eq!(phase("monitor_evaluation")["count"], 40);
    assert_eq!(phase("monitor_evaluation")["kind"], "synchronous_work");
    assert_eq!(phase("input_wait")["kind"], "elapsed_wait");
    assert_eq!(phase("compile")["count"], 1);
    assert_eq!(
        summary["pipeline"]["counters"]["counts"]["logical_ticks"],
        40
    );
    assert_eq!(
        summary["dataflow"]["memory"]["snapshots"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()["label"],
        "final"
    );

    let events = trace["traceEvents"].as_array().unwrap();
    for track in ["pipeline", "monitor", "monitor steps"] {
        assert!(
            events
                .iter()
                .any(|event| event["name"] == "thread_name" && event["args"]["name"] == track),
            "{track}"
        );
    }
    assert!(
        events
            .iter()
            .any(|event| event["ph"] == "X" && event["cat"] == "step")
    );
}

/// The trace events of `category` named `name`, in file order.
fn trace_events<'a>(trace: &'a Json, category: &str, name: &str) -> Vec<&'a Json> {
    trace["traceEvents"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["cat"] == category && event["name"] == name)
        .collect()
}

#[test]
fn the_first_admitted_tick_is_tick_zero_on_every_track() {
    // A range of one tick must contain every span of that tick, on the pipeline, monitor and step
    // tracks alike, whichever tick it is.
    for tick in [0u64, 1] {
        let range = format!("{tick}..{}", tick + 1);
        let (_, _, trace) = run_demo_cli(&["--diagnostics-trace-ticks", &range]);
        let tick_of = |event: &Json| event["args"]["tick"].as_u64().unwrap();

        for name in ["row_preparation", "monitor_evaluation"] {
            let spans = trace_events(&trace, "pipeline", name);
            assert_eq!(spans.len(), 1, "{name} in {range}");
            assert_eq!(tick_of(spans[0]), tick, "{name} in {range}");
        }
        let packing = trace_events(&trace, "pipeline", "output_packing");
        assert!(!packing.is_empty(), "output_packing in {range}");
        assert!(packing.iter().all(|span| tick_of(span) == tick));

        let monitor = trace_events(&trace, "tick", "tick");
        assert_eq!(monitor.len(), 1, "monitor tick in {range}");
        assert_eq!(tick_of(monitor[0]), tick);

        let steps = trace["traceEvents"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["cat"] == "step")
            .collect::<Vec<_>>();
        assert!(!steps.is_empty(), "steps in {range}");
        assert!(steps.iter().all(|step| tick_of(step) == tick), "{range}");
    }
}

#[test]
fn collection_counters_report_buffer_replacements_and_scans() {
    let program = DataflowProgram::compile_checked(demo_specification()).unwrap();
    let rows = demo_rows(&program);
    let (mut observed, _) = observed_and_plain(&program, |_| {});
    run(&mut observed, &rows);
    let collections = &report(&observed)["collections"];
    // `recent` appends once per tick to a list its own delay state still holds, so every append
    // after the first replaces the previous buffer: the first append has an empty source, the
    // others replace buffers of 1 + 2 + … + 39 entries.
    assert_eq!(collections["list_appends"], 40);
    assert_eq!(collections["list_append_buffer_replacements"], 39);
    assert_eq!(
        collections["list_append_replaced_entries"],
        (0..40).sum::<u64>()
    );
    assert_eq!(collections["max_list_len"], 40);
    assert_eq!(collections["list_concats"], 0);
    // One fold over the samples per tick; the entry count bounds the callbacks from above.
    assert_eq!(collections["list_scans"], 40);
    let samples = program
        .input_vars()
        .iter()
        .position(|var| var.name() == "samples")
        .unwrap();
    let scanned = rows
        .iter()
        .map(|row| match &row[samples] {
            Value::List(items) => items.len() as u64,
            _ => 0,
        })
        .sum::<u64>();
    assert_eq!(collections["list_scan_entries"], scanned);
    // Two map inserts per tick build `summary`; handles are never counted as copies.
    assert_eq!(collections["map_updates"], 80);
    assert_eq!(collections["max_map_len"], 3);

    // An unobserved monitor on the same thread counts nothing into the observed one.
    let mut plain = DataflowMonitor::from_program(program.clone());
    run(&mut plain, &rows);
    assert_eq!(report(&observed)["collections"]["list_appends"], 40);
}

/// Names and executor paths of the step spans of one traced tick, in schedule order.
fn step_spans(monitor: &DataflowMonitor, tick: u64) -> Vec<(String, String)> {
    monitor
        .diagnostics_timeline()
        .unwrap()
        .events()
        .iter()
        .filter(|event| event.category == "step")
        .filter(|event| event.args.as_ref().unwrap()["tick"] == tick)
        .map(|event| {
            (
                event.name.to_string(),
                event.args.as_ref().unwrap()["path"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            )
        })
        .collect()
}

#[test]
fn a_failing_last_graph_step_is_recorded_as_failed_with_its_location() {
    // `a` and `b` evaluate on every tick. `m` calls a native function supplied as an input, which
    // returns a real `Err` (not a panic) when its argument is 8: on the third tick, b = 8.
    use trustworthiness_checker::core::values::RuntimeFunction;
    let source = "use experimental::functions\nin x: Int\nin f: (Int -> Int)\n\
                  out a: Int\nout b: Int\nout m: Int\na = x + 1\nb = a * 2\nm = f(b)";
    let program = DataflowProgram::compile_checked(elaborated(source)).unwrap();
    let function = Value::Function(RuntimeFunction::native_value(
        "refuse_eight",
        |args: ecow::EcoVec<Value>| match args.first() {
            Some(Value::Int(8)) => Err(anyhow::anyhow!("the function refuses 8")),
            Some(Value::Int(n)) => Ok(Value::Int(n + 100)),
            other => Err(anyhow::anyhow!("unexpected argument {other:?}")),
        },
    ));
    let position = |name: &str| {
        program
            .input_vars()
            .iter()
            .position(|var| var.name() == name)
            .unwrap()
    };
    let (x, f) = (position("x"), position("f"));
    let rows = (1..=3)
        .map(|value| {
            let mut row = vec![Value::NoVal; program.input_vars().len()];
            row[x] = Value::Int(value);
            row[f] = function.clone();
            row
        })
        .collect::<Vec<_>>();
    let (mut observed, mut plain) = observed_and_plain(&program, |monitor| {
        monitor.set_quickening(false);
    });
    let outcome = run(&mut observed, &rows);
    assert_eq!(outcome, run(&mut plain, &rows), "the error is unchanged");
    assert!(matches!(outcome[2], Err(_)), "{outcome:?}");

    let report = report(&observed);
    assert_eq!(report["ticks"]["succeeded"], 2);
    assert_eq!(report["ticks"]["failed"], 1);
    assert_eq!(report["events"]["step_failed"], 1);

    let steps = report["plans"][0]["steps"].as_array().unwrap();
    // `a` and `b` fuse into one scalar region; it succeeds on every tick and is never `failed`.
    let region = steps
        .iter()
        .find(|step| step["kind"] == "scalar_region")
        .expect("the scalar streams fuse into a region");
    assert_eq!(region["members"], serde_json::json!(["a", "b"]));
    assert_eq!(region["total"]["count"], 3, "the region ran on every tick");
    assert!(region["paths"].get("failed").is_none());
    let failing = steps.iter().find(|step| step["stream"] == "m").unwrap();
    assert_eq!(
        failing["total"]["count"], 3,
        "the failed attempt is counted"
    );
    assert_eq!(failing["paths"]["canonical"]["count"], 2);
    assert_eq!(
        failing["paths"]["failed"]["count"], 1,
        "the failure is its own outcome, not an executor path"
    );

    // The third tick's spans show the earlier step succeeding and the last one failing.
    let spans = step_spans(&observed, 2);
    assert_eq!(spans.len(), 2, "{spans:?}");
    assert!(spans[0].0.starts_with("region[a, b]") && spans[0].1 != "failed");
    assert_eq!(spans[1], ("m".to_owned(), "failed".to_owned()));
    let failure = observed
        .diagnostics_timeline()
        .unwrap()
        .events()
        .iter()
        .find(|event| event.name == "step_failed")
        .unwrap()
        .args
        .clone()
        .unwrap();
    assert_eq!(failure["tick"], 2);
    assert_eq!(failure["step"], "m");
    assert_eq!(failure["phase"], "main");
    assert_eq!(failure["definition_version"], 0);
    assert!(failure["plan"].is_u64() && failure["generation"].is_u64());
    assert!(failure["elapsed_ns"].is_u64());
}

fn retention_program() -> DataflowProgram {
    DataflowProgram::compile_checked(elaborated("in x: Int\nout y: Int\ny = x + 1")).unwrap()
}

fn retention_monitor(every_ticks: Option<u64>, max_snapshots: usize) -> DataflowMonitor {
    let mut config = DiagnosticsConfig::default();
    config.memory.every_ticks = every_ticks;
    config.memory.max_snapshots = max_snapshots;
    let mut monitor = DataflowMonitor::from_program(retention_program());
    monitor.enable_diagnostics(&config);
    monitor
}

fn tick(monitor: &mut DataflowMonitor, x: i64) {
    let mut output = vec![Value::NoVal];
    monitor.evaluate(&[Value::Int(x)], &mut output).unwrap();
}

/// Kinds and tick counts of the retained snapshots.
fn retained(monitor: &DataflowMonitor) -> Vec<(String, u64)> {
    monitor
        .diagnostics_report()
        .unwrap()
        .memory_snapshots()
        .iter()
        .map(|snapshot| {
            (
                serde_json::to_value(snapshot.kind)
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_owned(),
                snapshot.tick,
            )
        })
        .collect()
}

#[test]
fn snapshot_retention_stops_periodic_traversal_and_lets_mandatory_ones_replace_periodic() {
    let mut monitor = retention_monitor(Some(1), 3);
    for x in 0..10 {
        tick(&mut monitor, x);
    }
    let memory = report(&monitor)["memory"].clone();
    assert_eq!(memory["snapshots"].as_array().unwrap().len(), 3);
    assert_eq!(memory["periodic_skipped"], 7);
    assert_eq!(
        memory["snapshots_built"], 3,
        "a periodic snapshot that cannot be kept is not traversed"
    );

    // A final snapshot replaces the most recent periodic one; the earliest stay as a baseline.
    let final_snapshot = monitor.record_final_memory_snapshot().unwrap();
    assert_eq!(final_snapshot.label, "final");
    assert_eq!(
        retained(&monitor),
        [
            ("periodic".to_owned(), 1),
            ("periodic".to_owned(), 2),
            ("final".to_owned(), 10)
        ]
    );
    // So do explicit ones, until no periodic snapshot is left to replace.
    monitor.record_memory_snapshot("first explicit").unwrap();
    monitor.record_memory_snapshot("second explicit").unwrap();
    let labels = |monitor: &DataflowMonitor| -> Vec<String> {
        monitor
            .diagnostics_report()
            .unwrap()
            .memory_snapshots()
            .iter()
            .map(|snapshot| snapshot.label.clone())
            .collect()
    };
    assert_eq!(
        labels(&monitor),
        ["final", "first explicit", "second explicit"]
    );
    let memory = report(&monitor)["memory"].clone();
    assert_eq!(memory["periodic_replaced"], 3);
    assert_eq!(memory["snapshots_built"], 6);

    // Every retained snapshot is mandatory now: the request is refused honestly. It never
    // returns an earlier snapshot, leaves the store alone and is not traversed.
    assert_eq!(
        monitor.record_memory_snapshot("late").unwrap_err(),
        SnapshotSkipped::RetentionFull
    );
    assert_eq!(
        monitor.record_final_memory_snapshot().unwrap_err(),
        SnapshotSkipped::RetentionFull
    );
    assert_eq!(
        labels(&monitor),
        ["final", "first explicit", "second explicit"]
    );
    let memory = report(&monitor)["memory"].clone();
    assert_eq!(
        memory["mandatory_skipped"],
        serde_json::json!(["late", "final"])
    );
    assert_eq!(memory["mandatory_skipped_count"], 2);
    assert_eq!(memory["snapshots_built"], 6);

    // Periodic snapshots remain skipped without traversal.
    tick(&mut monitor, 10);
    let memory = report(&monitor)["memory"].clone();
    assert_eq!(memory["periodic_skipped"], 8);
    assert_eq!(memory["snapshots_built"], 6);
}

#[test]
fn a_zero_cap_refuses_every_snapshot_without_traversal() {
    let mut monitor = retention_monitor(Some(1), 0);
    tick(&mut monitor, 0);
    assert_eq!(
        monitor.record_memory_snapshot("x").unwrap_err(),
        SnapshotSkipped::RetentionFull
    );
    let memory = report(&monitor)["memory"].clone();
    assert_eq!(memory["snapshots"].as_array().unwrap().len(), 0);
    assert_eq!(memory["snapshots_built"], 0);
}

#[test]
fn periodic_snapshots_follow_successful_ticks_only() {
    let source = "in x: Int\nin source: Str\nout m: Int\nm = dynamic(source: Int, {x})";
    let program = DataflowProgram::compile_checked(elaborated(source)).unwrap();
    let mut config = DiagnosticsConfig::default();
    config.memory.every_ticks = Some(2);
    let mut monitor = DataflowMonitor::from_program(program);
    monitor.enable_diagnostics(&config);
    let mut output = vec![Value::NoVal];
    let row = |x: i64, source: &str| [Value::Int(x), Value::Str(source.into())];
    for x in [1, 2] {
        monitor.evaluate(&row(x, "x"), &mut output).unwrap();
    }
    assert_eq!(retained(&monitor), [("periodic".to_owned(), 2)]);
    // The third tick fails with two ticks already succeeded: a multiple of the cadence, but not
    // a successful tick, so no snapshot is taken for it.
    assert!(monitor.evaluate(&row(9, "x +"), &mut output).is_err());
    assert_eq!(retained(&monitor), [("periodic".to_owned(), 2)]);
    assert_eq!(report(&monitor)["memory"]["snapshots_built"], 1);
}

#[test]
fn dependency_memory_comes_from_a_snapshot_of_the_running_definition() {
    let first =
        DataflowProgram::compile_checked(elaborated("in x: Int\nout y: Int\ny = x + 1")).unwrap();
    let second =
        DataflowProgram::compile_checked(elaborated("in x: Int\nout y: Int\ny = x * 10")).unwrap();
    let mut monitor = DataflowMonitor::from_program(first);
    monitor.enable_diagnostics(&DiagnosticsConfig::default());
    let mut output = vec![Value::NoVal];
    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    monitor.record_memory_snapshot("v0").unwrap();
    let before = report(&monitor);
    assert_eq!(before["dependencies"]["definition_version"], 0);
    assert_eq!(before["dependencies"]["memory_snapshot"], "v0");
    assert!(before["dependencies"]["outputs"][0]["local"]["bytes"].is_u64());

    // The reconfiguration takes a snapshot of version 0 first, then installs version 1, which has
    // a stream of the same name. The old snapshot must not be attributed to the new stream.
    monitor
        .reconfigure(second, ContextTransferPolicy::MatchingStreamState)
        .unwrap();
    let after = report(&monitor);
    let snapshots = after["memory"]["snapshots"].as_array().unwrap();
    assert!(
        snapshots
            .iter()
            .all(|snapshot| snapshot["definition_version"] == 0),
        "every snapshot so far describes version 0"
    );
    let dependencies = &after["dependencies"];
    assert_eq!(dependencies["definition_version"], 1);
    assert!(dependencies["memory_snapshot"].is_null());
    assert!(dependencies["outputs"][0]["local"]["bytes"].is_null());
    assert!(dependencies["outputs"][0]["local"]["bytes_with_shared_roots"].is_null());

    // A snapshot of version 1 makes the figures available again.
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    monitor.record_memory_snapshot("v1").unwrap();
    let dependencies = &report(&monitor)["dependencies"];
    assert_eq!(dependencies["memory_snapshot"], "v1");
    assert!(dependencies["outputs"][0]["local"]["bytes"].is_u64());
}

#[test]
fn an_output_is_not_part_of_its_own_upstream_totals() {
    let program = DataflowProgram::compile_checked(elaborated(
        "in x: Int\nout a: Int\nout b: Int\na = x + 1\nb = a * 2",
    ))
    .unwrap();
    let mut monitor = DataflowMonitor::from_program(program);
    monitor.set_quickening(false);
    monitor.enable_diagnostics(&DiagnosticsConfig::default());
    let mut output = vec![Value::NoVal; 2];
    for x in 0..5 {
        monitor.evaluate(&[Value::Int(x)], &mut output).unwrap();
    }
    monitor.record_memory_snapshot("now").unwrap();
    let report = report(&monitor);
    let outputs = report["dependencies"]["outputs"].as_array().unwrap();
    let b = outputs.iter().find(|entry| entry["output"] == "b").unwrap();
    // `a` is reached by both outputs, so it is shared upstream of `b`; `b` itself is local only.
    assert_eq!(b["shared_streams"][0]["stream"], "a");
    assert_eq!(b["unique_streams"], serde_json::json!([]));
    // `a` and `b` fuse into one scalar region whose time belongs to no single stream, so neither
    // has a per-stream step time; the region's time is reported as an aggregate instead.
    assert_eq!(b["local"]["step_ns"], 0);
    assert_eq!(b["shared_upstream"]["step_ns"], 0);
    assert!(b["shared_region_ns"].as_u64().unwrap() > 0);
    let a = outputs.iter().find(|entry| entry["output"] == "a").unwrap();
    assert_eq!(b["shared_upstream"]["bytes"], a["local"]["bytes"]);
}

#[test]
fn shared_roots_are_a_partial_figure_kept_per_stream_and_out_of_upstream_totals() {
    let program = DataflowProgram::compile_checked(demo_specification()).unwrap();
    let rows = demo_rows(&program);
    let (mut observed, _) = observed_and_plain(&program, |_| {});
    run(&mut observed, &rows);
    observed.record_memory_snapshot("end").unwrap();
    let report = report(&observed);
    let snapshot = report["memory"]["snapshots"]
        .as_array()
        .unwrap()
        .last()
        .unwrap();
    let mut shared_components = 0;
    for component in snapshot["components"].as_array().unwrap() {
        let charged = component["total_bytes"].as_u64().unwrap();
        let shared = component["shared_bytes"].as_u64().unwrap();
        assert_eq!(
            component["bytes_with_shared_roots"].as_u64().unwrap(),
            charged + shared,
            "{}",
            component["path"]
        );
        shared_components += usize::from(shared > 0 && component["unique_bytes"] == 0);
    }
    assert!(
        shared_components > 0,
        "a later owner holding only shared roots is charged nothing for them"
    );

    // The partial figure exists for one stream only; every upstream aggregate leaves it null while
    // still summing the charged bytes.
    let outputs = report["dependencies"]["outputs"].as_array().unwrap();
    let recent = outputs
        .iter()
        .find(|entry| entry["output"] == "recent")
        .unwrap();
    let local = &recent["local"];
    assert!(local["bytes_with_shared_roots"].as_u64().unwrap() >= local["bytes"].as_u64().unwrap());
    for output in outputs {
        for upstream in ["unique_upstream", "shared_upstream"] {
            assert!(
                output[upstream]["bytes_with_shared_roots"].is_null(),
                "{} {upstream}",
                output["output"]
            );
            assert!(output[upstream]["bytes"].is_u64());
        }
    }
    assert!(
        report["dependencies"]["note"]
            .as_str()
            .unwrap()
            .contains("null in every upstream aggregate")
    );
}

#[test]
fn unresolved_map_identities_flag_the_stream_and_the_dependency_cost() {
    // Forty distinct one-entry maps: more same-sized roots than the identity window compares, so
    // the later ones are charged without a complete identity check.
    let source = "in ms: List<Map<Int>>\nout z: Int\naux y: List<Map<Int>>\ny = List.concat(ms, [])\nz = List.len(y)";
    let program = DataflowProgram::compile_checked(elaborated(source)).unwrap();
    let maps = Value::List(
        (0..40)
            .map(|index| {
                Value::Map(trustworthiness_checker::ValueMap::from_iter([(
                    ecow::EcoString::from("k"),
                    Value::Int(index),
                )]))
            })
            .collect(),
    );
    let mut monitor = DataflowMonitor::from_program(program);
    monitor.set_quickening(false);
    monitor.enable_diagnostics(&DiagnosticsConfig::default());
    let mut output = vec![Value::NoVal];
    monitor.evaluate(&[maps], &mut output).unwrap();
    assert_eq!(output, [Value::Int(40)]);
    let snapshot = monitor.record_memory_snapshot("end").unwrap();
    assert!(snapshot.total.payload.unresolved_map_identities > 0);

    let report = report(&monitor);
    let components = report["memory"]["snapshots"][0]["components"]
        .as_array()
        .unwrap();
    assert!(components.iter().any(|component| component["stream"] == "y"
        && component["unresolved_map_identities"].as_u64().unwrap() > 0));
    let z = report["dependencies"]["outputs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["output"] == "z")
        .unwrap();
    // `y` is unique upstream of `z`, so its uncertainty reaches the aggregate; `z` itself holds
    // no maps.
    assert_eq!(z["unique_streams"], serde_json::json!(["y"]));
    assert_eq!(z["unique_upstream"]["map_identity_uncertain"], true);
    assert!(z["local"].get("map_identity_uncertain").is_none());
    assert!(
        z["unique_upstream"]["bytes_with_shared_roots"].is_null(),
        "the partial figure is never aggregated"
    );
}

#[test]
fn function_capture_lists_are_counted_but_not_traversed() {
    // The demo's `weighted_sum` folds with a closure capturing `k`: its capture list is shared and
    // mutable, so the snapshot counts its slots but reports it opaque instead of traversing it.
    let program = DataflowProgram::compile_checked(demo_specification()).unwrap();
    let rows = demo_rows(&program);
    let (mut observed, _) = observed_and_plain(&program, |_| {});
    run(&mut observed, &rows);
    observed.record_memory_snapshot("end").unwrap();
    let report = report(&observed);
    let snapshot = report["memory"]["snapshots"]
        .as_array()
        .unwrap()
        .last()
        .unwrap();
    let callables = snapshot["components"]
        .as_array()
        .unwrap()
        .iter()
        .find(|component| component["path"] == "streams/score/callables")
        .expect("the closure's owner is listed");
    assert_eq!(callables["opaque"], true);
    assert!(callables["opaque_values"].as_u64().unwrap() > 0);
    assert!(
        callables["note"]
            .as_str()
            .unwrap()
            .contains("not traversed"),
        "the note says what was not inspected"
    );
    assert_eq!(snapshot["budget_exhausted"], false);
}

#[test]
fn dynamic_activations_are_keyed_by_version_stream_and_node() {
    let source = "in x: Int\nin source: Str\nout y: Int\ny = dynamic(source: Int, {x})";
    let program = DataflowProgram::compile_checked(elaborated(source)).unwrap();
    let mut monitor = DataflowMonitor::from_program(program);
    monitor.enable_diagnostics(&DiagnosticsConfig::default());
    let mut output = vec![Value::NoVal];
    let rows = [
        vec![Value::Int(1), Value::Str("x + 1".into())],
        vec![Value::Int(2), Value::NoVal],
    ];
    for row in &rows {
        monitor.evaluate(row, &mut output).unwrap();
    }
    // A reset installs another definition version whose expression index and node are the same.
    monitor.reset();
    for row in &rows {
        monitor.evaluate(row, &mut output).unwrap();
    }

    let report = report(&monitor);
    let expressions = report["dynamic"]["by_expression"].as_array().unwrap();
    assert_eq!(
        expressions.len(),
        2,
        "one entry per version: {expressions:?}"
    );
    assert_eq!(expressions[0]["definition_version"], 0);
    assert_eq!(expressions[1]["definition_version"], 1);
    for entry in expressions {
        assert_eq!(entry["stream"], "y");
        assert_eq!(entry["node"], expressions[0]["node"]);
        assert_eq!(entry["expression"], expressions[0]["expression"]);
        assert_eq!(entry["activations"]["count"], 1);
    }

    // Every identity-bearing event names its version and plan.
    let events = monitor.diagnostics_timeline().unwrap().events();
    let mut seen = std::collections::BTreeSet::new();
    for event in events {
        if !matches!(
            event.name.as_ref(),
            "tick" | "dynamic_activation" | "plan_switch"
        ) && event.category != "step"
        {
            continue;
        }
        let args = event.args.as_ref().unwrap();
        assert!(args["definition_version"].is_u64(), "{} {args}", event.name);
        assert!(args["tick"].is_u64(), "{} {args}", event.name);
        if event.name == "dynamic_activation" {
            assert_eq!(args["stream"], "y");
            assert!(args["node"].is_u64() && args["expression"].is_u64());
        }
        if event.category == "step" || event.name == "dynamic_activation" {
            assert!(args["plan"].is_u64(), "{} {args}", event.name);
            assert!(args["generation"].is_u64(), "{} {args}", event.name);
        }
        seen.insert((
            event.name.to_string(),
            args["definition_version"].as_u64().unwrap(),
        ));
    }
    for version in [0, 1] {
        for name in ["tick", "dynamic_activation"] {
            assert!(
                seen.contains(&(name.to_owned(), version)),
                "{name} {version}"
            );
        }
    }
}

#[test]
fn model_map_capture_stops_at_its_node_budget() {
    let program = DataflowProgram::compile_checked(demo_specification()).unwrap();
    let mut config = DiagnosticsConfig::default();
    config.limits.max_model_nodes = 5;
    let mut monitor = DataflowMonitor::from_program(program);
    monitor.enable_diagnostics(&config);
    let report = report(&monitor);
    let streams = report["definitions"][0]["streams"].as_array().unwrap();
    let listed = streams
        .iter()
        .map(|stream| stream["nodes"].as_array().unwrap().len())
        .sum::<usize>();
    assert_eq!(listed, 5, "the budget bounds the visited and listed nodes");
    assert!(
        streams
            .iter()
            .any(|stream| stream["nodes_truncated"] == true)
    );
    let last = streams.last().unwrap();
    assert_eq!(
        last["node_count"], 0,
        "no traversal once the budget is spent"
    );
    assert_eq!(last["nodes"], serde_json::json!([]));
    assert_eq!(last["nodes_truncated"], true);
    // The definition-wide count is what the bounded capture visited, not a separate scan.
    assert_eq!(report["definitions"][0]["node_count"], 5);
    assert_eq!(report["definitions"][0]["node_count_is_lower_bound"], true);
}

#[test]
fn a_long_expression_chain_locates_every_node() {
    // Left-deep chains nest deeply: attribution must still reach every node exactly once. The
    // parser and checker recurse over such a chain, so it is built on a thread with a large stack.
    const TERMS: usize = 200;
    let report = std::thread::scope(|scope| {
        std::thread::Builder::new()
            .stack_size(256 << 20)
            .spawn_scoped(scope, || {
                let terms = vec!["x"; TERMS].join(" + ");
                let source = format!("in x: Int\nout y: Int\ny = {terms}");
                let program = DataflowProgram::compile_checked(elaborated(&source)).unwrap();
                let mut monitor = DataflowMonitor::from_program(program);
                monitor.enable_diagnostics(&DiagnosticsConfig::default());
                report(&monitor)
            })
            .unwrap()
            .join()
            .unwrap()
    });
    let stream = &report["definitions"][0]["streams"][0];
    assert_eq!(stream["unlocated_nodes"], 0);
    assert!(stream["node_count"].as_u64().unwrap() >= (TERMS - 1) as u64);
    assert!(
        stream["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|node| node["call_site"].is_object())
    );
}

/// Listed nodes of the first stream of `source` captured under a node budget.
fn listed_nodes(source: &str, budget: usize) -> (usize, Json) {
    let program = DataflowProgram::compile_checked(elaborated(source)).unwrap();
    let mut config = DiagnosticsConfig::default();
    config.limits.max_model_nodes = budget;
    let mut monitor = DataflowMonitor::from_program(program);
    monitor.enable_diagnostics(&config);
    let definition = report(&monitor)["definitions"][0].clone();
    let listed = definition["streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|stream| stream["nodes"].as_array().unwrap().len())
        .sum();
    (listed, definition)
}

#[test]
fn node_budgets_bound_every_cut_through_branches_and_match_arms() {
    let source = "use experimental::pattern_matching\nin x: Int\nout y: Int\nout w: Int\n\
                  y = match (x) { 1 -> x + 1, 2 -> (x * 2) + (x - 3), n if n > 3 -> n + 5, _ -> x - 5 }\n\
                  w = if x > 0 then (x + 1) * 2 else x - 1";
    let (total, full) = listed_nodes(source, 1 << 20);
    assert!(
        total > 10,
        "the fixture has several arms and branches: {total}"
    );
    assert_eq!(full["node_count"], total);
    assert_eq!(full["node_count_is_lower_bound"], false);
    for budget in 0..=total + 2 {
        let (listed, definition) = listed_nodes(source, budget);
        assert_eq!(listed, budget.min(total), "budget {budget}");
        assert_eq!(
            definition["node_count"],
            budget.min(total),
            "budget {budget}"
        );
        // Conservative: reaching the budget flags the count even if nothing was left.
        assert_eq!(
            definition["node_count_is_lower_bound"],
            budget <= total,
            "budget {budget}"
        );
    }
}

#[test]
fn source_positions_stop_at_the_total_scan_budget_but_keep_file_span_and_snippet() {
    let program = DataflowProgram::compile_checked(demo_specification()).unwrap();
    let first_site = |scan: usize| {
        let mut config = DiagnosticsConfig::default();
        config.limits.max_source_scan_bytes = scan;
        let mut monitor = DataflowMonitor::from_program(program.clone());
        monitor.enable_diagnostics(&config);
        let report = report(&monitor);
        let level = report["definitions"][0]["streams"]
            .as_array()
            .unwrap()
            .iter()
            .find(|stream| stream["name"] == "level")
            .unwrap()
            .clone();
        level["site"].clone()
    };
    let exhausted = first_site(0);
    assert!(
        exhausted["file"]
            .as_str()
            .unwrap()
            .ends_with("profile_demo.dsrv")
    );
    assert_eq!(exhausted["snippet"], "clamp(score, 0, 100)");
    assert!(exhausted["start"].is_u64() && exhausted["end"].is_u64());
    assert!(exhausted.get("line").map_or(true, Json::is_null));
    assert!(exhausted.get("column").map_or(true, Json::is_null));
    assert_eq!(exhausted["position_truncated"], true);

    let ample = first_site(16 << 20);
    assert_eq!(ample["line"], 19);
    assert_eq!(ample["column"], 9);
    assert!(ample.get("position_truncated").is_none());
}

#[test]
fn a_directly_driven_run_starts_at_the_configured_epoch() {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use trustworthiness_checker::diagnostics::DiagnosticsHub;
    use trustworthiness_checker::runtime::diagnostics::RunArtifacts;

    let hub = DiagnosticsHub::new(DiagnosticsConfig::default());
    std::thread::sleep(Duration::from_millis(40));
    let mut monitor = DataflowMonitor::from_program(retention_program());
    monitor.enable_diagnostics(hub.config());
    tick(&mut monitor, 1);

    let artifacts = RunArtifacts::for_monitor(&hub, &monitor, None);
    let run = &artifacts.report.run;
    assert!(
        run.elapsed_ns >= 40_000_000,
        "elapsed time counts from the epoch: {}",
        run.elapsed_ns
    );
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    assert!(
        run.started_unix_ms + 35 <= now_ms,
        "the start is the epoch, not the export time: {} vs {now_ms}",
        run.started_unix_ms
    );
}
