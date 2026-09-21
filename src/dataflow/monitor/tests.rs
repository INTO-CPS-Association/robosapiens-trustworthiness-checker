#[cfg(test)]
pub(in crate::dataflow) mod test_support {
    use super::*;

    pub(in crate::dataflow) fn execution(monitor: &DataflowMonitor) -> &MonitorExecution {
        &monitor.execution
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn jit_artifact_count(monitor: &DataflowMonitor) -> usize {
        monitor.execution.jit_artifact_count()
    }

    pub(in crate::dataflow) fn hot_path_counts(monitor: &DataflowMonitor) -> (usize, usize) {
        (
            monitor.expression_scan_count,
            monitor.scheduler.update_schedule_call_count(),
        )
    }

    pub(in crate::dataflow) fn last_reconfiguration_dependencies_changed(
        monitor: &DataflowMonitor,
    ) -> bool {
        monitor.last_reconfiguration_dependencies_changed
    }

    pub(in crate::dataflow) fn reset_hot_path_counts(monitor: &mut DataflowMonitor) {
        monitor.expression_scan_count = 0;
        monitor.scheduler.reset_update_schedule_call_count();
    }
}

use super::*;
use crate::DsrvSpecification;
use crate::core::Semantics;
use crate::dataflow::execution::evaluator_state::{reset_state_clone_count, state_clone_count};
use crate::dataflow::stream_id::StreamId;
use crate::dataflow::{
    ContextTransferReport, DataflowEvaluationError, ReconfigurationMapping, StreamMapping,
    StreamStateTransferOutcome,
};
#[cfg(feature = "jit")]
use crate::dataflow::{JitConfig, JitPlan};
use crate::dsrv_fixtures::WithoutWarnings;
use crate::dsrv_fixtures::elaborated;
use crate::lang::dsrv::ast::{Expr, SyntaxLiteral};
use std::collections::{BTreeMap, BTreeSet};

fn input_row(monitor: &DataflowMonitor, values: &[(&str, Value)]) -> Vec<Value> {
    monitor
        .input_vars()
        .iter()
        .map(|variable| {
            values
                .iter()
                .find_map(|(name, value)| (variable == &VarName::new(name)).then(|| value.clone()))
                .unwrap()
        })
        .collect()
}

fn report_count(report: &ContextTransferReport, outcome: StreamStateTransferOutcome) -> usize {
    report
        .streams
        .iter()
        .filter(|entry| entry.outcome == outcome)
        .count()
}

fn history_id_for(monitor: &DataflowMonitor, variable: &str) -> HistoryId {
    let slot = monitor
        .program
        .environment_layout()
        .slot(&VarName::new(variable))
        .unwrap();
    monitor.history_bindings[slot.index()].unwrap()
}

fn history_depth_for(monitor: &DataflowMonitor, variable: &str) -> Option<usize> {
    let slot = monitor
        .program
        .environment_layout()
        .slot(&VarName::new(variable))?;
    monitor.history_bindings[slot.index()]
        .map(|history_id| monitor.history_store[history_id].required_depth())
}

#[test]
fn checked_expression_typed_source_uses_string_runtime_values() {
    let specification = elaborated(
        "in x: Int\nin property: Expr<Int>\nout result: Int\n\
                         result = dynamic(property)",
    );
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut output = [Value::NoVal];

    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[
                    ("x", Value::Int(41)),
                    ("property", Value::Str("x + 1".into())),
                ],
            ),
            &mut output,
        )
        .unwrap();

    assert_eq!(output, [Value::Int(42)]);
}

#[test]
fn matching_root_reconfiguration_initializes_changed_stream_state() {
    let old_spec = "in x: Int\nout z: Int\nz = x";
    let new_spec = "in x: Int\nout z: Int\nz = x + 1";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&old_spec), Semantics::Untimed).unwrap();
    let candidate =
        DataflowMonitor::compile_with_semantics(elaborated(&new_spec), Semantics::Untimed).unwrap();

    let report = monitor
        .reconfigure(
            candidate.program,
            ContextTransferPolicy::MatchingStreamState,
        )
        .unwrap();

    assert!(report.monitor_changed);
    assert_eq!(
        report.context_transfer.streams.as_slice()[0].outcome,
        StreamStateTransferOutcome::Initialized
    );
    assert_eq!(monitor.revision(), MonitorRevision(1));
    let mut output = [Value::NoVal];
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(3)]);
}

#[test]
fn exact_root_reconfiguration_preserves_active_state_under_matching_transfer() {
    let specification = "in x: Int\nout first: Int\nout second: Int\nfirst = x[1]\nsecond = x[2]";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    monitor.set_quickening(false);
    assert_eq!(monitor.revision(), MonitorRevision::INITIAL);
    assert!(!monitor.quickening_enabled());

    let mut output = [Value::NoVal, Value::NoVal];
    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(1), Value::Deferred]);

    let mut candidate =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    candidate.set_quickening(true);
    let report = monitor
        .reconfigure(
            candidate.program,
            ContextTransferPolicy::MatchingStreamState,
        )
        .unwrap();

    assert!(!report.monitor_changed);
    assert!(!report.interface_changed);
    assert_eq!(report.monitor_revision, MonitorRevision(1));
    assert_eq!(report.interface_revision, InterfaceRevision::INITIAL);
    assert_eq!(report.context_transfer.streams.as_slice().len(), 2);
    assert!(
        report
            .context_transfer
            .streams
            .as_slice()
            .iter()
            .all(|entry| entry.outcome == StreamStateTransferOutcome::Transferred)
    );
    assert_eq!(monitor.revision(), MonitorRevision(1));
    assert_eq!(monitor.interface_revision(), InterfaceRevision::INITIAL);
    assert!(!monitor.quickening_enabled());

    monitor.evaluate(&[Value::Int(3)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(2), Value::Int(1)]);
}

#[test]
fn root_exact_transfer_moves_state_without_cloning_evaluator_state() {
    let specification = "in x: Int\nout z: Int\nz = x[2]";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal];
    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    let source_history_id = history_id_for(&monitor, "x");
    let source_history_slots = monitor.history_store[source_history_id].slots_ptr();

    let candidate =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    reset_state_clone_count();
    let report = monitor
        .reconfigure(
            candidate.program,
            ContextTransferPolicy::MatchingStreamState,
        )
        .unwrap();

    assert_eq!(state_clone_count(), 0);
    assert_eq!(
        report_count(
            &report.context_transfer,
            StreamStateTransferOutcome::Transferred
        ),
        1
    );
    let target_history_id = history_id_for(&monitor, "x");
    assert_eq!(
        monitor.history_store[target_history_id].slots_ptr(),
        source_history_slots
    );
    monitor.evaluate(&[Value::Int(3)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(1)]);
}

#[test]
fn root_replacement_reuses_named_history_immediately() {
    let old_spec = "in x: Int\nout a: Int\na = x[3]";
    let new_spec = "in x: Int\nout v: Bool\nv = x[2] > 0";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&old_spec), Semantics::Untimed).unwrap();
    #[cfg(feature = "jit")]
    monitor.enable_jit(JitConfig::eager());
    let mut output = [Value::NoVal];
    for value in [1, 2, 3] {
        monitor.evaluate(&[Value::Int(value)], &mut output).unwrap();
    }

    let candidate =
        DataflowMonitor::compile_with_semantics(elaborated(&new_spec), Semantics::Untimed).unwrap();
    monitor
        .reconfigure(
            candidate.program,
            ContextTransferPolicy::MatchingStreamState,
        )
        .unwrap();

    monitor.evaluate(&[Value::Int(4)], &mut output).unwrap();
    assert_eq!(output, [Value::Bool(true)]);
}

#[test]
fn root_replacement_reports_partial_named_history_until_target_depth_is_available() {
    let old_spec = "in x: Int\nout v: Int\nv = x[2]";
    let new_spec = "in x: Int\nout v: Int\nv = x[3]";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&old_spec), Semantics::Untimed).unwrap();
    let mut output = [Value::NoVal];
    for value in [1, 2] {
        monitor.evaluate(&[Value::Int(value)], &mut output).unwrap();
    }

    let candidate =
        DataflowMonitor::compile_with_semantics(elaborated(&new_spec), Semantics::Untimed).unwrap();
    monitor
        .reconfigure(
            candidate.program,
            ContextTransferPolicy::MatchingStreamState,
        )
        .unwrap();

    monitor.evaluate(&[Value::Int(3)], &mut output).unwrap();
    assert_eq!(output, [Value::Deferred]);
    monitor.evaluate(&[Value::Int(4)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(1)]);
}

#[test]
fn direct_external_delays_share_one_history_and_do_not_allocate_private_rings() {
    let specification = "in x: Int\nout a: Int\nout b: Int\na = x[1]\nb = x[2]";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    assert_eq!(monitor.history_store.len(), 1);
    assert_eq!(monitor.execution.delay_ring_lengths(), [0, 0]);

    let mut output = [Value::NoVal, Value::NoVal];
    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    assert_eq!(output, [Value::Deferred, Value::Deferred]);
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(1), Value::Deferred]);
    monitor.evaluate(&[Value::Int(3)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(2), Value::Int(1)]);
    assert_eq!(monitor.execution.delay_ring_lengths(), [0, 0]);
}

#[test]
fn matching_root_transfer_initializes_changed_delay_state() {
    let old_spec = "in x: Int\nout z: Int\nz = x[1] + 1";
    let new_spec = "in x: Int\nout z: Int\nz = x[1] + 2";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&old_spec), Semantics::Untimed).unwrap();
    let mut output = [Value::NoVal];
    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    assert_eq!(output, [Value::Deferred]);
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(2)]);

    let candidate =
        DataflowMonitor::compile_with_semantics(elaborated(&new_spec), Semantics::Untimed).unwrap();
    let report = monitor
        .reconfigure(
            candidate.program,
            ContextTransferPolicy::MatchingStreamState,
        )
        .unwrap();

    assert_eq!(
        report_count(
            &report.context_transfer,
            StreamStateTransferOutcome::Initialized
        ),
        1
    );
    monitor.evaluate(&[Value::Int(3)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(4)]);
}

#[test]
fn matching_transfer_initializes_changed_stream_retained_values() {
    let old_spec = "in x: Int\nin source: Str\nout base: Int\nout z: Int\nbase = x[1] + x + 1\nz = dynamic(source: Int)";
    let new_spec = "in x: Int\nin source: Str\nout base: Int\nout z: Int\nbase = x[1] + x + 2\nz = dynamic(source: Int)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&old_spec), Semantics::Untimed).unwrap();
    let mut output = [Value::NoVal, Value::NoVal];

    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[("x", Value::Int(1)), ("source", Value::Str("base".into()))],
            ),
            &mut output,
        )
        .unwrap();
    monitor
        .evaluate(
            &input_row(&monitor, &[("x", Value::Int(2)), ("source", Value::NoVal)]),
            &mut output,
        )
        .unwrap();

    let base_slot = monitor
        .program
        .environment_layout()
        .slot(&VarName::new("base"))
        .unwrap();
    assert_eq!(
        monitor.retained_environment_values.as_ref().unwrap()[base_slot.index()],
        Value::Int(4)
    );

    let candidate =
        DataflowMonitor::compile_with_semantics(elaborated(&new_spec), Semantics::Untimed).unwrap();
    let report = monitor
        .reconfigure(
            candidate.program,
            ContextTransferPolicy::MatchingStreamState,
        )
        .unwrap();
    assert_eq!(
        report_count(
            &report.context_transfer,
            StreamStateTransferOutcome::Initialized
        ),
        1
    );
    assert_eq!(
        monitor.retained_environment_values.as_ref().unwrap()[base_slot.index()],
        Value::NoVal
    );
}

#[test]
fn matching_root_transfer_initializes_changed_delay_history() {
    let old_spec = "in x: Int\nout z: Int\nz = x[1]";
    let new_spec = "in x: Int\nout z: Int\nz = x[1] + 1";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&old_spec), Semantics::Untimed).unwrap();
    let mut output = [Value::NoVal];
    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();

    let candidate =
        DataflowMonitor::compile_with_semantics(elaborated(&new_spec), Semantics::Untimed).unwrap();
    let report = monitor
        .reconfigure(
            candidate.program,
            ContextTransferPolicy::MatchingStreamState,
        )
        .unwrap();

    assert_eq!(
        report_count(
            &report.context_transfer,
            StreamStateTransferOutcome::Initialized
        ),
        1
    );
    assert_eq!(monitor.revision(), MonitorRevision(1));
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(2)]);
}

#[test]
fn active_dynamics_share_one_history_within_a_stream() {
    let specification = "in x: Int\nin first: Str\nin second: Str\nout z: Int\nz = dynamic(first: Int) + dynamic(second: Int)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal];
    let input = input_row(
        &monitor,
        &[
            ("x", Value::Int(10)),
            ("first", Value::Str("x[1]".into())),
            ("second", Value::Str("x[3]".into())),
        ],
    );

    monitor.evaluate(&input, &mut output).unwrap();

    assert_eq!(monitor.history_store.len(), 1);
    assert_eq!(history_depth_for(&monitor, "x"), Some(3));
}

#[test]
fn active_dynamics_share_one_history_across_streams() {
    let specification = "in x: Int\nin first: Str\nin second: Str\nout a: Int\nout b: Int\na = dynamic(first: Int)\nb = dynamic(second: Int)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal, Value::NoVal];
    let input = input_row(
        &monitor,
        &[
            ("x", Value::Int(10)),
            ("first", Value::Str("x[1]".into())),
            ("second", Value::Str("x[3]".into())),
        ],
    );

    monitor.evaluate(&input, &mut output).unwrap();

    assert_eq!(monitor.history_store.len(), 1);
    assert_eq!(history_depth_for(&monitor, "x"), Some(3));
}

#[test]
fn active_defers_share_one_history() {
    let specification = "in x: Int\nin first: Str\nin second: Str\nout a: Int\nout b: Int\na = defer(first: Int)\nb = defer(second: Int)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal, Value::NoVal];
    let input = input_row(
        &monitor,
        &[
            ("x", Value::Int(10)),
            ("first", Value::Str("x[1]".into())),
            ("second", Value::Str("x[3]".into())),
        ],
    );

    monitor.evaluate(&input, &mut output).unwrap();

    assert_eq!(monitor.history_store.len(), 1);
    assert_eq!(history_depth_for(&monitor, "x"), Some(3));
}

#[test]
fn replacing_one_consumer_keeps_the_other_consumer_depth() {
    let specification = "in x: Int\nin first: Str\nin second: Str\nout z: Int\nz = dynamic(first: Int) + dynamic(second: Int)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal];

    let first = input_row(
        &monitor,
        &[
            ("x", Value::Int(1)),
            ("first", Value::Str("x[3]".into())),
            ("second", Value::Str("x[2]".into())),
        ],
    );
    monitor.evaluate(&first, &mut output).unwrap();
    assert_eq!(history_depth_for(&monitor, "x"), Some(3));

    let replacement = input_row(
        &monitor,
        &[
            ("x", Value::Int(2)),
            ("first", Value::Str("x[1]".into())),
            ("second", Value::NoVal),
        ],
    );
    monitor.evaluate(&replacement, &mut output).unwrap();

    assert_eq!(history_depth_for(&monitor, "x"), Some(2));
}

#[test]
fn dynamic_activation_of_unretained_history_starts_cold() {
    let specification = "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal];

    let before_activation = input_row(&monitor, &[("x", Value::Int(1)), ("source", Value::NoVal)]);
    monitor.evaluate(&before_activation, &mut output).unwrap();
    assert_eq!(history_depth_for(&monitor, "x"), None);

    for (x, expected) in [
        (2, Value::Deferred),
        (3, Value::Deferred),
        (4, Value::Int(2)),
    ] {
        let input = input_row(
            &monitor,
            &[("x", Value::Int(x)), ("source", Value::Str("x[2]".into()))],
        );
        monitor.evaluate(&input, &mut output).unwrap();
        assert_eq!(output, [expected]);
    }
    assert_eq!(history_depth_for(&monitor, "x"), Some(2));
}

#[test]
fn exact_root_transfer_moves_shared_history_and_each_active_body() {
    let specification = "in x: Int\nin first: Str\nin second: Str\nout a: Int\nout b: Int\na = dynamic(first: Int)\nb = dynamic(second: Int)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal, Value::NoVal];

    let activation = input_row(
        &monitor,
        &[
            ("x", Value::Int(1)),
            ("first", Value::Str("default(x[1], 100)".into())),
            ("second", Value::Str("default(x[1], 200)".into())),
        ],
    );
    monitor.evaluate(&activation, &mut output).unwrap();
    let sparse = input_row(
        &monitor,
        &[
            ("x", Value::NoVal),
            ("first", Value::NoVal),
            ("second", Value::NoVal),
        ],
    );
    monitor.evaluate(&sparse, &mut output).unwrap();
    assert_eq!(output, [Value::Int(1), Value::Int(1)]);

    let candidate =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    monitor
        .reconfigure(
            candidate.program,
            ContextTransferPolicy::MatchingStreamState,
        )
        .unwrap();

    let after_transfer = input_row(
        &monitor,
        &[
            ("x", Value::NoVal),
            ("first", Value::NoVal),
            ("second", Value::NoVal),
        ],
    );
    monitor.evaluate(&after_transfer, &mut output).unwrap();

    assert_eq!(output, [Value::Int(1), Value::Int(1)]);
    assert_eq!(history_depth_for(&monitor, "x"), Some(1));
}

#[test]
fn definition_keys_follow_normalized_semantics_not_source_formatting() {
    let compact = DataflowMonitor::compile_with_semantics(
        elaborated("in x: Int\nout z: Int\nz = x + 1"),
        Semantics::Untimed,
    )
    .unwrap();
    let formatted = DataflowMonitor::compile_with_semantics(
        elaborated("in x: Int\nout z: Int\nz = ( x + 1 )"),
        Semantics::Untimed,
    )
    .unwrap();
    let changed = DataflowMonitor::compile_with_semantics(
        elaborated("in x: Int\nout z: Int\nz = x + 2"),
        Semantics::Untimed,
    )
    .unwrap();
    assert_eq!(compact.definition_key(), formatted.definition_key());
    assert_ne!(compact.definition_key(), changed.definition_key());
}

#[test]
fn compiled_program_is_separate_from_monitor_state() {
    let specification = elaborated("in x: Int\nout z: Int\nz = x + 1");
    let program = DataflowProgram::compile_checked(specification).unwrap();
    let definition_key = program.definition_key().clone();

    assert_eq!(program.input_vars(), &[VarName::new("x")]);
    assert_eq!(program.output_vars(), &[VarName::new("z")]);
    assert_eq!(program.stream_vars(), &[VarName::new("z")]);
    assert_eq!(program.environment_size(), 2);

    let monitor = DataflowMonitor::new(program);
    assert_eq!(monitor.program().definition_key(), &definition_key);
    assert!(monitor.retained_environment_values.is_none());
}

#[test]
fn static_monitor_does_not_allocate_a_retained_environment() {
    let specification = elaborated("in x: Int\nout z: Int\nz = x + 1");
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();

    assert!(monitor.retained_environment_values.is_none());
}

#[test]
fn matching_root_transfer_initializes_changed_stream_state_after_execution() {
    let old_spec = "in x: Int\nout z: Int\nz = x";
    let new_spec = "in x: Int\nout z: Int\nz = x + 1";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&old_spec), Semantics::Untimed).unwrap();
    let mut output = [Value::NoVal];
    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    let replacement =
        DataflowMonitor::compile_with_semantics(elaborated(&new_spec), Semantics::Untimed).unwrap();

    let report = monitor
        .reconfigure(
            replacement.program,
            ContextTransferPolicy::MatchingStreamState,
        )
        .unwrap();
    assert!(report.monitor_changed);
    assert_eq!(
        report.context_transfer.streams.as_slice()[0].outcome,
        StreamStateTransferOutcome::Initialized
    );
    assert_eq!(monitor.revision(), MonitorRevision(1));
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(3)]);
}

#[test]
fn none_root_transfer_initializes_stream_state() {
    let specification = "in x: Int\nout z: Int\nz = x[1]";
    let mut source =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    source.set_quickening(false);
    let mut output = [Value::NoVal];
    source.evaluate(&[Value::Int(1)], &mut output).unwrap();

    let candidate =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let report = source
        .reconfigure(candidate.program, ContextTransferPolicy::None)
        .unwrap();
    assert!(!report.monitor_changed);
    assert!(!report.interface_changed);
    assert_eq!(report.monitor_revision, MonitorRevision(1));
    assert_eq!(report.interface_revision, InterfaceRevision::INITIAL);
    assert_eq!(report.context_transfer.streams.as_slice().len(), 1);
    assert_eq!(
        report.context_transfer.streams.as_slice()[0].outcome,
        StreamStateTransferOutcome::Initialized
    );
    assert_eq!(source.revision(), MonitorRevision(1));
    assert_eq!(source.interface_revision(), InterfaceRevision::INITIAL);
    assert!(!source.quickening_enabled());
    let history_id = history_id_for(&source, "x");
    assert!(source.history_store[history_id].is_empty());

    source.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(output, [Value::Deferred]);
}

#[test]
fn matching_root_transfer_preserves_exact_and_initializes_changed_streams() {
    let source_spec = "in x: Int\nout a: Int\nout b: Int\na = x[1]\nb = x";
    let candidate_spec = "in x: Int\nout a: Int\nout b: Int\na = x[1]\nb = x + 1";
    let mut source =
        DataflowMonitor::compile_with_semantics(elaborated(&source_spec), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal, Value::NoVal];
    source.evaluate(&[Value::Int(1)], &mut output).unwrap();

    let candidate =
        DataflowMonitor::compile_with_semantics(elaborated(&candidate_spec), Semantics::Untimed)
            .unwrap();
    let report = source
        .reconfigure(
            candidate.program,
            ContextTransferPolicy::MatchingStreamState,
        )
        .unwrap();
    assert_eq!(
        report_count(
            &report.context_transfer,
            StreamStateTransferOutcome::Transferred
        ),
        1
    );
    assert_eq!(
        report_count(
            &report.context_transfer,
            StreamStateTransferOutcome::Initialized
        ),
        1
    );
    assert_eq!(source.revision(), MonitorRevision(1));

    source.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(1), Value::Int(3)]);
}

#[test]
fn root_transfer_reuses_context_retained_for_an_active_body() {
    let old_spec = "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
    let new_spec = "in x: Int\nin source: Str\nout z: Int\nz = x[3]";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&old_spec), Semantics::Untimed).unwrap();
    let mut output = [Value::NoVal];

    for value in [1, 2, 3] {
        monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[
                        ("x", Value::Int(value)),
                        (
                            "source",
                            if value == 1 {
                                Value::Str("x[3]".into())
                            } else {
                                Value::NoVal
                            },
                        ),
                    ],
                ),
                &mut output,
            )
            .unwrap();
    }
    assert_eq!(output, [Value::Deferred]);
    assert_eq!(history_depth_for(&monitor, "x"), Some(3));

    let replacement =
        DataflowMonitor::compile_with_semantics(elaborated(&new_spec), Semantics::Untimed).unwrap();
    let report = monitor
        .reconfigure(
            replacement.program,
            ContextTransferPolicy::MatchingStreamState,
        )
        .unwrap();
    assert_eq!(
        report.context_transfer.streams.as_slice()[0].outcome,
        StreamStateTransferOutcome::Initialized
    );
    monitor
        .evaluate(
            &input_row(&monitor, &[("x", Value::Int(4)), ("source", Value::NoVal)]),
            &mut output,
        )
        .unwrap();

    assert_eq!(output, [Value::Int(1)]);
}

#[test]
fn matching_root_transfer_preserves_exact_active_body_history() {
    let old_spec = "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
    let new_spec = "in a: Int\nin x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
    let mut old =
        DataflowMonitor::compile_with_semantics(elaborated(&old_spec), Semantics::Untimed).unwrap();
    let mut output = [Value::NoVal];
    old.evaluate(
        &input_row(
            &old,
            &[("x", Value::Int(1)), ("source", Value::Str("x[1]".into()))],
        ),
        &mut output,
    )
    .unwrap();
    old.evaluate(
        &input_row(
            &old,
            &[("x", Value::Int(2)), ("source", Value::Str("x[1]".into()))],
        ),
        &mut output,
    )
    .unwrap();
    assert_eq!(output, [Value::Int(1)]);

    let replacement =
        DataflowMonitor::compile_with_semantics(elaborated(&new_spec), Semantics::Untimed).unwrap();
    let report = old
        .reconfigure(
            replacement.program,
            ContextTransferPolicy::MatchingStreamState,
        )
        .unwrap();
    assert_eq!(
        report_count(
            &report.context_transfer,
            StreamStateTransferOutcome::Transferred
        ),
        1
    );
    old.evaluate(
        &input_row(
            &old,
            &[
                ("a", Value::NoVal),
                ("x", Value::Int(3)),
                ("source", Value::Str("x[1]".into())),
            ],
        ),
        &mut output,
    )
    .unwrap();
    assert_eq!(output, [Value::Int(2)]);
}

#[test]
fn matching_unchanged_dynamic_body_preserves_delay_cells() {
    let specification = "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal];
    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[("x", Value::Int(1)), ("source", Value::Str("x[1]".into()))],
            ),
            &mut output,
        )
        .unwrap();
    assert_eq!(output, [Value::Deferred]);
    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[
                    ("x", Value::Int(2)),
                    ("source", Value::Str("(x[1])".into())),
                ],
            ),
            &mut output,
        )
        .unwrap();
    assert_eq!(output, [Value::Int(1)]);
}

#[test]
fn matching_changed_dynamic_body_starts_cold() {
    let specification = "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal];
    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[("x", Value::Int(1)), ("source", Value::Str("x[1]".into()))],
            ),
            &mut output,
        )
        .unwrap();
    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[("x", Value::Int(2)), ("source", Value::Str("x + 1".into()))],
            ),
            &mut output,
        )
        .unwrap();
    assert_eq!(output, [Value::Int(3)]);
}

#[test]
fn matching_changed_dynamic_transfer_starts_cold() {
    let specification = "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    monitor.set_reconfiguration_transfer_policy(ContextTransferPolicy::MatchingStreamState);
    let mut output = [Value::NoVal];
    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[("x", Value::Int(1)), ("source", Value::Str("x[1]".into()))],
            ),
            &mut output,
        )
        .unwrap();
    assert_eq!(monitor.revision(), MonitorRevision(1));

    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[("x", Value::Int(2)), ("source", Value::Str("x + 1".into()))],
            ),
            &mut output,
        )
        .unwrap();
    assert_eq!(output, [Value::Int(3)]);
    assert_eq!(monitor.revision(), MonitorRevision(2));

    monitor
        .evaluate(
            &input_row(&monitor, &[("x", Value::Int(3)), ("source", Value::NoVal)]),
            &mut output,
        )
        .unwrap();
    assert_eq!(output, [Value::Int(4)]);
}

#[test]
fn changed_dynamic_transfer_does_not_reuse_wrong_provenance_history() {
    let specification =
        "in x: Int\nin y: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal];
    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[
                    ("x", Value::Int(1)),
                    ("y", Value::Int(10)),
                    ("source", Value::Str("(x + 1)[1]".into())),
                ],
            ),
            &mut output,
        )
        .unwrap();
    assert_eq!(output, [Value::Deferred]);

    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[
                    ("x", Value::Int(2)),
                    ("y", Value::Int(20)),
                    ("source", Value::Str("(y + 1)[1]".into())),
                ],
            ),
            &mut output,
        )
        .unwrap();
    assert_eq!(output, [Value::Deferred]);

    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[
                    ("x", Value::Int(3)),
                    ("y", Value::Int(30)),
                    ("source", Value::NoVal),
                ],
            ),
            &mut output,
        )
        .unwrap();
    assert_eq!(output, [Value::Int(21)]);
}

#[test]
fn root_transfer_reconstructs_active_dynamic_dependency_edges() {
    let specification = "in x: Int\nin source: Str\nout z: Int\naux computed: Int\n\
            z = dynamic(source: Int)\ncomputed = x + 1";
    let mut old =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal];

    old.evaluate(
        &input_row(
            &old,
            &[
                ("x", Value::Int(1)),
                ("source", Value::Str("computed".into())),
            ],
        ),
        &mut output,
    )
    .unwrap();
    assert_eq!(output, [Value::Int(2)]);
    old.evaluate(
        &input_row(&old, &[("x", Value::Int(2)), ("source", Value::NoVal)]),
        &mut output,
    )
    .unwrap();
    assert_eq!(output, [Value::Int(3)]);

    let replacement =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    old.reconfigure(
        replacement.program,
        ContextTransferPolicy::MatchingStreamState,
    )
    .unwrap();
    old.evaluate(
        &input_row(&old, &[("x", Value::Int(3)), ("source", Value::NoVal)]),
        &mut output,
    )
    .unwrap();
    assert_eq!(output, [Value::Int(4)]);
}

#[test]
fn invalid_dynamic_candidate_poisons_the_monitor() {
    let specification = "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal];
    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[("x", Value::Int(1)), ("source", Value::Str("x".into()))],
            ),
            &mut output,
        )
        .unwrap();
    let previous_output = output.clone();
    let error = monitor
        .evaluate(
            &input_row(
                &monitor,
                &[("x", Value::Int(2)), ("source", Value::Str("(".into()))],
            ),
            &mut output,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        DataflowEvaluationError::ReconfigurableExpressionParse { .. }
    ));
    assert_eq!(output, previous_output);
    assert!(matches!(
        monitor.evaluate(&[Value::Int(3), Value::Str("x + 1".into())], &mut output,),
        Err(DataflowEvaluationError::MonitorFailed)
    ));
}

#[test]
fn failed_active_monitor_does_not_take_exact_root_retention() {
    let specification = "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal];
    let error = monitor
        .evaluate(
            &input_row(
                &monitor,
                &[("x", Value::Int(1)), ("source", Value::Str("(".into()))],
            ),
            &mut output,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        DataflowEvaluationError::ReconfigurableExpressionParse { .. }
    ));

    let candidate =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    monitor
        .reconfigure(
            candidate.program,
            ContextTransferPolicy::MatchingStreamState,
        )
        .unwrap();

    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[("x", Value::Int(1)), ("source", Value::Str("x[1]".into()))],
            ),
            &mut output,
        )
        .unwrap();
    assert_eq!(output, [Value::Deferred]);
    monitor
        .evaluate(
            &input_row(&monitor, &[("x", Value::Int(2)), ("source", Value::NoVal)]),
            &mut output,
        )
        .unwrap();
    assert_eq!(output, [Value::Int(1)]);
}

#[test]
fn invalid_dynamic_candidate_publishes_no_failed_tick_output() {
    let specification = "in x: Int\nin source_text: Str\nout source: Str\nout z: Int\
            \nsource = source_text\nz = dynamic(source: Int)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal, Value::NoVal];
    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[
                    ("x", Value::Int(1)),
                    ("source_text", Value::Str("x".into())),
                ],
            ),
            &mut output,
        )
        .unwrap();
    let previous_output = output.clone();

    let error = monitor
        .evaluate(
            &input_row(
                &monitor,
                &[
                    ("x", Value::Int(2)),
                    ("source_text", Value::Str("(".into())),
                ],
            ),
            &mut output,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        DataflowEvaluationError::ReconfigurableExpressionParse { .. }
    ));
    assert_eq!(output, previous_output);
    assert!(matches!(
        monitor.evaluate(&[Value::Int(3), Value::Str("x".into())], &mut output),
        Err(DataflowEvaluationError::MonitorFailed)
    ));
}

#[test]
fn invalid_dynamic_source_poisons_the_monitor() {
    let specification = "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal];
    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[("x", Value::Int(1)), ("source", Value::Str("x[1]".into()))],
            ),
            &mut output,
        )
        .unwrap();
    let error = monitor
        .evaluate(
            &input_row(
                &monitor,
                &[("x", Value::Int(2)), ("source", Value::Str("(".into()))],
            ),
            &mut output,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        DataflowEvaluationError::ReconfigurableExpressionParse { .. }
    ));
    assert!(matches!(
        monitor.evaluate(&[Value::Int(3), Value::NoVal], &mut output,),
        Err(DataflowEvaluationError::MonitorFailed)
    ));
}

#[test]
fn invalid_first_defer_source_poisons_the_monitor() {
    let specification = "in x: Int\nin source: Str\nout z: Int\nz = defer(source: Int)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal];
    let error = monitor
        .evaluate(
            &input_row(
                &monitor,
                &[("x", Value::Int(1)), ("source", Value::Str("(".into()))],
            ),
            &mut output,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        DataflowEvaluationError::ReconfigurableExpressionParse { .. }
    ));
    assert!(matches!(
        monitor.evaluate(&[Value::Int(2), Value::NoVal], &mut output,),
        Err(DataflowEvaluationError::MonitorFailed)
    ));
}

#[test]
fn nested_expression_reconfigurations_advance_revision_once_per_tick() {
    let specification = "in x: Int\nin first: Str\nin second: Str\nout a: Int\nout b: Int\
            \na = dynamic(first: Int)\nb = dynamic(second: Int)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal, Value::NoVal];

    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[
                    ("x", Value::Int(1)),
                    ("first", Value::Str("x".into())),
                    ("second", Value::Str("x + 1".into())),
                ],
            ),
            &mut output,
        )
        .unwrap();

    assert_eq!(monitor.revision(), MonitorRevision(1));

    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[
                    ("x", Value::Int(2)),
                    ("first", Value::Str("x + 1".into())),
                    ("second", Value::Str("x + 2".into())),
                ],
            ),
            &mut output,
        )
        .unwrap();
    assert_eq!(monitor.revision(), MonitorRevision(2));
}

#[test]
fn unchanged_dynamic_point_keeps_state_when_another_point_changes() {
    let specification = "in x: Int\nin first: Str\nin second: Str\nout a: Int\nout b: Int\
            \na = dynamic(first: Int)\nb = dynamic(second: Int)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    monitor.set_reconfiguration_transfer_policy(ContextTransferPolicy::None);
    let mut output = [Value::NoVal, Value::NoVal];

    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[
                    ("x", Value::Int(1)),
                    ("first", Value::Str("x[1]".into())),
                    ("second", Value::Str("x[1]".into())),
                ],
            ),
            &mut output,
        )
        .unwrap();
    assert_eq!(output, [Value::Deferred, Value::Deferred]);
    assert_eq!(monitor.revision(), MonitorRevision(1));

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        monitor.evaluate(
            &input_row(
                &monitor,
                &[
                    ("x", Value::Int(2)),
                    ("first", Value::Str("x + 1".into())),
                    ("second", Value::Str("x[1]".into())),
                ],
            ),
            &mut output,
        )
    }));
    assert!(
        result.is_ok(),
        "selective dynamic reconfiguration must not panic"
    );
    result.unwrap().unwrap();

    assert_eq!(output, [Value::Int(3), Value::Int(1)]);
    assert_eq!(monitor.revision(), MonitorRevision(2));
}

#[test]
fn changed_expression_rebuilds_same_stream_dependencies_before_and_after_it() {
    let specification = "in x: Int\nin before_source: Str\nin changed_source: Str\nin after_source: Str\n\
            out z: Int\naux before: Int\naux changed: Int\naux after: Int\naux replacement: Int\n\
            before = x + 1\nchanged = x + 10\nafter = x + 100\nreplacement = x + 1000\n\
            z = defer(before_source: Int) + dynamic(changed_source: Int) + dynamic(after_source: Int)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal];

    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[
                    ("x", Value::Int(1)),
                    ("before_source", Value::Str("before".into())),
                    ("changed_source", Value::Str("changed".into())),
                    ("after_source", Value::Str("after".into())),
                ],
            ),
            &mut output,
        )
        .unwrap();
    assert_eq!(output, [Value::Int(114)]);

    let second = input_row(
        &monitor,
        &[
            ("x", Value::Int(2)),
            ("before_source", Value::NoVal),
            ("changed_source", Value::Str("replacement".into())),
            ("after_source", Value::NoVal),
        ],
    );
    super::test_support::reset_hot_path_counts(&mut monitor);
    monitor.evaluate(&second, &mut output).unwrap();

    assert_eq!(output, [Value::Int(1107)]);
    assert_eq!(super::test_support::hot_path_counts(&monitor), (2, 1));
    assert!(super::test_support::last_reconfiguration_dependencies_changed(&monitor));
}

#[test]
fn one_changed_expression_is_scanned_once_and_updates_the_scheduler_once() {
    let specification = "in x: Int\nin a_source: Str\nin b_source: Str\nout a: Int\nout b: Int\
            \na = dynamic(a_source: Int)\nb = dynamic(b_source: Int)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal, Value::NoVal];
    let first = input_row(
        &monitor,
        &[
            ("x", Value::Int(10)),
            ("a_source", Value::Str("x".into())),
            ("b_source", Value::Str("a + 1".into())),
        ],
    );
    monitor.evaluate(&first, &mut output).unwrap();
    assert_eq!(output, [Value::Int(10), Value::Int(11)]);

    let second = input_row(
        &monitor,
        &[
            ("x", Value::Int(20)),
            ("a_source", Value::Str("x".into())),
            ("b_source", Value::Str("x + 1".into())),
        ],
    );
    super::test_support::reset_hot_path_counts(&mut monitor);
    monitor.evaluate(&second, &mut output).unwrap();

    assert_eq!(super::test_support::hot_path_counts(&monitor), (2, 1));
    assert!(super::test_support::last_reconfiguration_dependencies_changed(&monitor));
    assert_eq!(output, [Value::Int(20), Value::Int(21)]);

    super::test_support::reset_hot_path_counts(&mut monitor);
    monitor.evaluate(&second, &mut output).unwrap();
    assert_eq!(super::test_support::hot_path_counts(&monitor), (2, 1));
    assert!(!super::test_support::last_reconfiguration_dependencies_changed(&monitor));
}

#[test]
fn exact_dynamic_transfer_projects_a_shifted_computed_stream() {
    let old_spec = "in x: Int\nin source: Str\nout z: Int\naux computed: Int\n\
            z = dynamic(source: Int)\ncomputed = x + 1";
    let new_spec = "in added: Int\nin x: Int\nin source: Str\nout z: Int\naux computed: Int\n\
            z = dynamic(source: Int)\ncomputed = x + 1";
    let mut old =
        DataflowMonitor::compile_with_semantics(elaborated(&old_spec), Semantics::Untimed).unwrap();
    let old_computed = old
        .program
        .environment_layout()
        .slot(&VarName::new("computed"))
        .unwrap();
    let old_z = StreamId::new(
        old.program
            .stream_vars()
            .iter()
            .position(|variable| variable == &VarName::new("z"))
            .unwrap(),
    );
    let old_expression = old
        .program
        .monitor_plan()
        .reconfigurable_expressions
        .expressions_for(old_z)[0]
        .node;
    let mut output = [Value::NoVal];
    old.evaluate(
        &input_row(
            &old,
            &[
                ("x", Value::Int(1)),
                ("source", Value::Str("computed".into())),
            ],
        ),
        &mut output,
    )
    .unwrap();
    assert_eq!(output, [Value::Int(2)]);
    assert_eq!(
        old.execution
            .expression_dependency_slots(old_z, old_expression),
        [old_computed]
    );
    assert!(old.history_store.is_empty());

    let replacement =
        DataflowMonitor::compile_with_semantics(elaborated(&new_spec), Semantics::Untimed).unwrap();
    let target_computed = replacement
        .program
        .environment_layout()
        .slot(&VarName::new("computed"))
        .unwrap();
    let target_z = StreamId::new(
        replacement
            .program
            .stream_vars()
            .iter()
            .position(|variable| variable == &VarName::new("z"))
            .unwrap(),
    );
    let target_expression = replacement
        .program
        .monitor_plan()
        .reconfigurable_expressions
        .expressions_for(target_z)[0]
        .node;
    assert_ne!(old_computed, target_computed);
    let mapping = ReconfigurationMapping::between(old.program(), replacement.program());
    assert!(matches!(
        mapping.stream(target_z),
        Some(StreamMapping::Exact(source)) if *source == old_z
    ));

    old.reconfigure(
        replacement.program,
        ContextTransferPolicy::MatchingStreamState,
    )
    .unwrap();
    assert_eq!(
        old.execution
            .expression_dependency_slots(target_z, target_expression),
        [target_computed]
    );
    assert!(old.history_store.is_empty());

    old.evaluate(
        &input_row(
            &old,
            &[
                ("added", Value::Int(100)),
                ("x", Value::Int(2)),
                ("source", Value::NoVal),
            ],
        ),
        &mut output,
    )
    .unwrap();
    assert_eq!(output, [Value::Int(3)]);
}

#[test]
fn exact_defer_transfer_projects_a_shifted_computed_stream() {
    let old_spec = "in x: Int\nin source: Str\nout z: Int\naux computed: Int\n\
            z = defer(source: Int)\ncomputed = x + 1";
    let new_spec = "in added: Int\nin x: Int\nin source: Str\nout z: Int\naux computed: Int\n\
            z = defer(source: Int)\ncomputed = x + 1";
    let mut old = DataflowMonitor::compile_checked(elaborated(&old_spec)).unwrap();
    let mut output = [Value::NoVal];
    old.evaluate(
        &input_row(
            &old,
            &[
                ("x", Value::Int(1)),
                ("source", Value::Str("computed".into())),
            ],
        ),
        &mut output,
    )
    .unwrap();
    assert_eq!(output, [Value::Int(2)]);
    assert!(old.reconfiguration_state.source_order().is_empty());

    let replacement = DataflowMonitor::compile_checked(elaborated(&new_spec)).unwrap();
    let old_computed = old
        .program
        .environment_layout()
        .slot(&VarName::new("computed"))
        .unwrap();
    let target_computed = replacement
        .program
        .environment_layout()
        .slot(&VarName::new("computed"))
        .unwrap();
    assert_ne!(old_computed, target_computed);

    old.reconfigure(
        replacement.program,
        ContextTransferPolicy::MatchingStreamState,
    )
    .unwrap();
    assert!(old.reconfiguration_state.source_order().is_empty());

    old.evaluate(
        &input_row(
            &old,
            &[
                ("added", Value::Int(100)),
                ("x", Value::Int(2)),
                ("source", Value::NoVal),
            ],
        ),
        &mut output,
    )
    .unwrap();
    assert_eq!(output, [Value::Int(3)]);
}

#[test]
fn transferred_active_bodies_project_and_share_shifted_history() {
    let old_spec = "in x: Int\nin first: Str\nin second: Str\nout a: Int\nout b: Int\n\
            aux computed: Int\na = defer(first: Int)\nb = defer(second: Int)\ncomputed = x + 1";
    let new_spec = "in added: Int\nin x: Int\nin first: Str\nin second: Str\nout a: Int\nout b: Int\n\
            aux computed: Int\na = defer(first: Int)\nb = defer(second: Int)\ncomputed = x + 1";
    let mut old =
        DataflowMonitor::compile_with_semantics(elaborated(&old_spec), Semantics::Untimed).unwrap();
    let mut output = [Value::NoVal, Value::NoVal];
    for value in [1, 2, 3] {
        old.evaluate(
            &input_row(
                &old,
                &[
                    ("x", Value::Int(value)),
                    (
                        "first",
                        if value == 1 {
                            Value::Str("computed + default(computed[1], 0)".into())
                        } else {
                            Value::NoVal
                        },
                    ),
                    (
                        "second",
                        if value == 1 {
                            Value::Str("computed + default(computed[3], 0)".into())
                        } else {
                            Value::NoVal
                        },
                    ),
                ],
            ),
            &mut output,
        )
        .unwrap();
    }
    assert_eq!(output, [Value::Int(7), Value::Int(4)]);
    assert_eq!(old.history_store.len(), 1);
    assert_eq!(history_depth_for(&old, "computed"), Some(3));
    let source_history_slots = old.history_store[history_id_for(&old, "computed")].slots_ptr();

    let replacement =
        DataflowMonitor::compile_with_semantics(elaborated(&new_spec), Semantics::Untimed).unwrap();
    let old_computed = old
        .program
        .environment_layout()
        .slot(&VarName::new("computed"))
        .unwrap();
    let target_computed = replacement
        .program
        .environment_layout()
        .slot(&VarName::new("computed"))
        .unwrap();
    assert_ne!(old_computed, target_computed);
    assert_eq!(
        replacement
            .program
            .history_requirements()
            .depth(target_computed),
        0
    );

    old.reconfigure(
        replacement.program,
        ContextTransferPolicy::MatchingStreamState,
    )
    .unwrap();

    let mut requirements = Vec::new();
    old.execution
        .for_each_active_body_history_requirement(&mut |requirement| {
            requirements.push((requirement.slot(), requirement.depth()));
        });
    requirements.sort_unstable();
    assert_eq!(requirements, [(target_computed, 1), (target_computed, 3)]);
    assert_eq!(old.history_store.len(), 1);
    assert_eq!(history_depth_for(&old, "computed"), Some(3));
    assert_eq!(
        old.history_store[history_id_for(&old, "computed")].slots_ptr(),
        source_history_slots
    );

    old.evaluate(
        &input_row(
            &old,
            &[
                ("added", Value::Int(100)),
                ("x", Value::Int(4)),
                ("first", Value::NoVal),
                ("second", Value::NoVal),
            ],
        ),
        &mut output,
    )
    .unwrap();
    assert_eq!(output, [Value::Int(9), Value::Int(7)]);
}

#[test]
fn matching_root_transfer_preserves_sealed_defer_expression() {
    let old_spec = "in x: Int\nin source: Str\nout z: Int\nz = defer(source: Int)";
    let new_spec = "in a: Int\nin x: Int\nin source: Str\nout z: Int\nz = defer(source: Int)";
    let mut old =
        DataflowMonitor::compile_with_semantics(elaborated(&old_spec), Semantics::Untimed).unwrap();
    let mut output = [Value::NoVal];
    for value in [1, 2] {
        old.evaluate(
            &input_row(
                &old,
                &[
                    ("x", Value::Int(value)),
                    ("source", Value::Str("x[1]".into())),
                ],
            ),
            &mut output,
        )
        .unwrap();
    }
    assert_eq!(output, [Value::Int(1)]);
    assert!(old.reconfiguration_state.source_order().is_empty());

    let replacement =
        DataflowMonitor::compile_with_semantics(elaborated(&new_spec), Semantics::Untimed).unwrap();
    let report = old
        .reconfigure(
            replacement.program,
            ContextTransferPolicy::MatchingStreamState,
        )
        .unwrap();
    assert_eq!(
        report_count(
            &report.context_transfer,
            StreamStateTransferOutcome::Transferred
        ),
        1
    );
    old.evaluate(
        &input_row(
            &old,
            &[
                ("a", Value::NoVal),
                ("x", Value::Int(3)),
                ("source", Value::Str("x[1]".into())),
            ],
        ),
        &mut output,
    )
    .unwrap();
    assert_eq!(output, [Value::Int(2)]);
    assert!(old.reconfiguration_state.source_order().is_empty());
}

#[test]
fn unactivated_defer_stays_inactive_across_root_transfer() {
    let specification = "in x: Int\nin source: Str\nout z: Int\nz = defer(source: Int)";
    let mut old =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal];
    old.evaluate(&[Value::Int(1), Value::NoVal], &mut output)
        .unwrap();
    let replacement =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    old.reconfigure(
        replacement.program,
        ContextTransferPolicy::MatchingStreamState,
    )
    .unwrap();
    let point = old
        .program()
        .monitor_plan()
        .reconfigurable_expressions
        .expressions_for(StreamId::new(0))[0]
        .id();
    assert!(!old.reconfiguration_state.is_sealed(point));
    assert!(old.reconfiguration_state.resolution_stream(0).is_some());
    old.evaluate(&[Value::Int(2), Value::Str("x".into())], &mut output)
        .unwrap();
    assert_eq!(output, [Value::Int(2)]);
}

#[test]
fn reconfigurable_monitor_retains_an_outer_environment() {
    let specification =
        elaborated("in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)");
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();

    assert_eq!(
        monitor.retained_environment_values.as_ref().unwrap().len(),
        monitor.environment_values.len()
    );
}

#[test]
fn activated_defer_releases_its_computed_source_into_the_main_plan() {
    let specification = elaborated(
        "in x: Int\nin choose: Bool\nin left: Str\nin right: Str\n\
            aux source: Str\nout result: Int\n\
            source = if choose then left else right\n\
            result = defer(source: Int)",
    );
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();
    assert_eq!(monitor.reconfiguration_state.source_order().len(), 1);

    let mut output = [Value::NoVal];
    let activation = input_row(
        &monitor,
        &[
            ("x", Value::Int(10)),
            ("choose", Value::Bool(true)),
            ("left", Value::Str("x + 1".into())),
            ("right", Value::Str("x + 100".into())),
        ],
    );
    monitor.evaluate(&activation, &mut output).unwrap();
    assert_eq!(output, [Value::Int(11)]);
    assert!(monitor.reconfiguration_state.source_order().is_empty());
    assert!(monitor.reconfiguration_state.resolution_stream(0).is_none());

    let after_sealing = input_row(
        &monitor,
        &[
            ("x", Value::Int(20)),
            ("choose", Value::Bool(false)),
            ("left", Value::Str("x + 1".into())),
            ("right", Value::Str("x + 100".into())),
        ],
    );
    monitor.evaluate(&after_sealing, &mut output).unwrap();
    assert_eq!(output, [Value::Int(21)]);
}

#[test]
fn shared_dynamic_source_keeps_a_sealed_defer_source_in_the_prelude() {
    let specification = elaborated(
        "in x: Int\nin choose: Bool\nin left: Str\nin right: Str\n\
            aux source: Str\nout deferred: Int\nout dynamic_result: Int\n\
            source = if choose then left else right\n\
            deferred = defer(source: Int)\n\
            dynamic_result = dynamic(source: Int)",
    );
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut output = [Value::NoVal, Value::NoVal];

    let first = input_row(
        &monitor,
        &[
            ("x", Value::Int(10)),
            ("choose", Value::Bool(true)),
            ("left", Value::Str("x + 1".into())),
            ("right", Value::Str("x + 2".into())),
        ],
    );
    monitor.evaluate(&first, &mut output).unwrap();
    assert_eq!(output, [Value::Int(11), Value::Int(11)]);
    assert_eq!(monitor.reconfiguration_state.source_order().len(), 1);

    let second = input_row(
        &monitor,
        &[
            ("x", Value::Int(20)),
            ("choose", Value::Bool(false)),
            ("left", Value::Str("x + 1".into())),
            ("right", Value::Str("x + 2".into())),
        ],
    );
    monitor.evaluate(&second, &mut output).unwrap();
    assert_eq!(output, [Value::Int(21), Value::Int(22)]);
    assert_eq!(monitor.reconfiguration_state.source_order().len(), 1);
}

#[test]
fn monitor_without_positive_history_requirements_has_an_empty_store() {
    let specification = "in x: Int\nout z: Int\nz = x";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();

    assert!(monitor.history_store.is_empty());
    assert_eq!(monitor.history_store.len(), 0);

    let mut output = [Value::NoVal];
    monitor.evaluate(&[Value::Int(7)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(7)]);
    assert_eq!(monitor.environment_values[0], Value::Int(7));
}

#[test]
fn successful_tick_projects_outputs_before_capturing_environment_history() {
    let specification = "in x: Int\nout y: Int\nout z: Int\ny = x\nz = x[1]";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal, Value::NoVal];

    monitor.evaluate(&[Value::Int(7)], &mut output).unwrap();

    assert_eq!(output, [Value::Int(7), Value::Deferred]);
    let history_id = history_id_for(&monitor, "x");
    assert_eq!(monitor.history_store.read(history_id, 1), Value::Int(7));
    assert_eq!(monitor.environment_values[0], Value::NoVal);
}

#[test]
fn destructive_root_transfer_shrinks_named_history_without_moving_values_individually() {
    let source_spec = "in x: Int\nout z: Int\nz = x[3]";
    let target_spec = "in x: Int\nout z: Int\nz = x[2]";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&source_spec), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal];
    for value in [10, 20, 30] {
        monitor.evaluate(&[Value::Int(value)], &mut output).unwrap();
    }
    let source_history_id = history_id_for(&monitor, "x");
    let source_history_slots = monitor.history_store[source_history_id].slots_ptr();

    let candidate =
        DataflowMonitor::compile_with_semantics(elaborated(&target_spec), Semantics::Untimed)
            .unwrap();
    monitor
        .reconfigure(
            candidate.program,
            ContextTransferPolicy::MatchingStreamState,
        )
        .unwrap();

    let target_history_id = history_id_for(&monitor, "x");
    let history = &monitor.history_store[target_history_id];
    assert_eq!(history.required_depth(), 2);
    assert_eq!(history.len(), 2);
    assert_eq!(history.read(1), Value::Int(30));
    assert_eq!(history.read(2), Value::Int(20));
    assert_eq!(history.slots_ptr(), source_history_slots);
}

#[test]
fn destructive_root_transfer_grows_named_history_and_preserves_available_values() {
    let source_spec = "in x: Int\nout z: Int\nz = x[2]";
    let target_spec = "in x: Int\nout z: Int\nz = x[3]";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&source_spec), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal];
    for value in [10, 20] {
        monitor.evaluate(&[Value::Int(value)], &mut output).unwrap();
    }

    let candidate =
        DataflowMonitor::compile_with_semantics(elaborated(&target_spec), Semantics::Untimed)
            .unwrap();
    monitor
        .reconfigure(
            candidate.program,
            ContextTransferPolicy::MatchingStreamState,
        )
        .unwrap();

    let target_history_id = history_id_for(&monitor, "x");
    let history = &monitor.history_store[target_history_id];
    assert_eq!(history.required_depth(), 3);
    assert_eq!(history.len(), 2);
    assert_eq!(history.capacity(), 3);
    assert_eq!(history.read(1), Value::Int(20));
    assert_eq!(history.read(2), Value::Int(10));
    assert_eq!(history.read(3), Value::Deferred);
}

#[test]
fn destructive_none_transfer_keeps_target_named_history_cold() {
    let specification = "in x: Int\nout z: Int\nz = x[2]";
    let mut source =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut output = [Value::NoVal];
    source.evaluate(&[Value::Int(10)], &mut output).unwrap();
    source.evaluate(&[Value::Int(20)], &mut output).unwrap();

    let candidate =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    source
        .reconfigure(candidate.program, ContextTransferPolicy::None)
        .unwrap();

    let history_id = history_id_for(&source, "x");
    let history = &source.history_store[history_id];
    assert!(history.is_empty());
    assert_eq!(history.read(1), Value::Deferred);
    assert_eq!(history.read(2), Value::Deferred);
}

#[cfg(feature = "jit")]
#[test]
fn destructive_root_transfer_materializes_fused_branch_and_lift_state() {
    let specification = elaborated(
        "in x: Int\nin choose: Bool\nout result: Int\n\
result = if choose then x + 1 else x + 2",
    );
    let mut continued =
        DataflowMonitor::compile_checked_with_jit(specification.clone(), JitConfig::eager())
            .unwrap();
    let mut canonical = DataflowMonitor::compile_checked(specification.clone()).unwrap();
    let mut continued_output = [Value::NoVal];
    let mut canonical_output = [Value::NoVal];
    let initial = [Value::Int(3), Value::Bool(true)];
    continued.evaluate(&initial, &mut continued_output).unwrap();
    canonical.evaluate(&initial, &mut canonical_output).unwrap();
    assert_eq!(continued_output, [Value::Int(4)]);
    assert_eq!(continued_output, canonical_output);
    assert_eq!(
        continued.jit_report().unwrap().plan(),
        JitPlan::WholeSchedule
    );

    let mut replacement =
        DataflowMonitor::compile_checked_with_jit(specification, JitConfig::eager()).unwrap();
    let mut replacement_output = [Value::NoVal];
    replacement
        .evaluate(
            &[Value::Int(99), Value::Bool(false)],
            &mut replacement_output,
        )
        .unwrap();
    continued
        .reconfigure(
            replacement.program,
            ContextTransferPolicy::MatchingStreamState,
        )
        .unwrap();

    for value in [Value::NoVal, Value::Deferred] {
        let row = [value, Value::Bool(true)];
        continued.evaluate(&row, &mut continued_output).unwrap();
        canonical.evaluate(&row, &mut canonical_output).unwrap();
        assert_eq!(
            continued_output, canonical_output,
            "fused-native root transfer diverged for {row:?}"
        );
    }
}

#[cfg(feature = "jit")]
#[test]
fn defer_sealing_preserves_per_stream_jit_artifacts() {
    let specification = elaborated(
        "in x: Int\nin left: Str\nin right: Str\n\
            aux selector: Int\naux source: Str\nout fixed: Int\nout result: Int\n\
            selector = x + 1\n\
            source = if selector > 0 then left else right\n\
            fixed = x * 2\n\
            result = defer(source: Int)",
    );
    let mut monitor =
        DataflowMonitor::compile_checked_with_jit(specification, JitConfig::eager()).unwrap();
    assert_eq!(monitor.jit_report().unwrap().plan(), JitPlan::Regions);
    assert_eq!(monitor.jit_report().unwrap().compiled_artifacts(), 2);
    let artifacts = monitor.execution.jit_artifact_count();

    let mut output = [Value::NoVal, Value::NoVal];
    let activation = input_row(
        &monitor,
        &[
            ("x", Value::Int(10)),
            ("left", Value::Str("x + 1".into())),
            ("right", Value::Str("x + 2".into())),
        ],
    );
    monitor.evaluate(&activation, &mut output).unwrap();
    assert_eq!(output, [Value::Int(20), Value::Int(11)]);
    assert!(monitor.reconfiguration_state.source_order().is_empty());
    assert_eq!(monitor.execution.jit_artifact_count(), artifacts);

    let next = input_row(
        &monitor,
        &[
            ("x", Value::Int(20)),
            ("left", Value::Str("x + 100".into())),
            ("right", Value::Str("x + 200".into())),
        ],
    );
    monitor.evaluate(&next, &mut output).unwrap();
    assert_eq!(output, [Value::Int(40), Value::Int(21)]);
    assert_eq!(monitor.execution.jit_artifact_count(), artifacts);
}

#[test]
fn static_scalar_chain_preserves_values_across_sparse_inputs() {
    let specification = elaborated(
        "in x: Int\n\
            aux a: Int\n\
            aux b: Int\n\
            out c: Int\n\
            a = x + 1\n\
            b = a * 2\n\
            c = b - 3",
    );
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();

    let mut output = [Value::NoVal];
    for (input, expected) in [
        (Value::Int(1), Value::Int(1)),
        (Value::NoVal, Value::Int(1)),
        (Value::Int(3), Value::Int(5)),
    ] {
        monitor.evaluate(&[input], &mut output).unwrap();
        assert_eq!(output[0], expected);
    }
}

#[test]
fn scalar_run_deoptimizes_only_the_mismatched_stream() {
    let specification = elaborated(
        "in x: Int\n\
            aux equal: Bool\n\
            out negated: Bool\n\
            equal = x == 1\n\
            negated = !equal",
    );
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();

    let mut output = [Value::NoVal];
    for (input, expected) in [
        (Value::Int(1), Value::Bool(false)),
        (Value::Bool(true), Value::Bool(true)),
        (Value::Int(1), Value::Bool(false)),
    ] {
        monitor.evaluate(&[input], &mut output).unwrap();
        assert_eq!(output, [expected]);
    }
}

#[test]
fn fusion_preserves_fanout_and_intermediate_outputs() {
    let specification = elaborated(
        "in x: Int\n\
            out a: Int\n\
            aux b: Int\n\
            out c: Int\n\
            a = x + 1\n\
            b = a * 2\n\
            c = a + b",
    );
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();

    let mut output = [Value::NoVal, Value::NoVal];
    monitor.evaluate(&[Value::Int(4)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(5), Value::Int(15)]);
}

#[test]
fn nested_graph_scope_preserves_values() {
    let specification = elaborated(
        "in x: Int\n\
            in choose: Bool\n\
            aux a: Int\n\
            aux b: Int\n\
            aux c: Int\n\
            aux d: Int\n\
            out e: Int\n\
            a = x + 1\n\
            b = a + 1\n\
            c = if choose then b else x\n\
            d = c + 1\n\
            e = d + 1",
    );
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();

    let mut output = [Value::NoVal];
    monitor
        .evaluate(&[Value::Int(4), Value::Bool(true)], &mut output)
        .unwrap();
    assert_eq!(output, [Value::Int(8)]);
}

#[test]
fn delay_captures_internal_stream_after_the_completed_tick() {
    let specification = elaborated(
        "in x: Int\n\
            aux current: Int\n\
            out delayed: Int\n\
            current = x + 1\n\
            delayed = default(current[1], 0) + 1",
    );
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();

    let mut output = [Value::NoVal];
    for (input, expected) in [
        (Value::Int(10), Value::Int(1)),
        (Value::Int(20), Value::Int(12)),
        (Value::Int(30), Value::Int(22)),
    ] {
        monitor.evaluate(&[input], &mut output).unwrap();
        assert_eq!(output, [expected]);
    }
}

#[test]
fn temporal_stream_preserves_values_between_scalar_streams() {
    let specification = elaborated(
        "in x: Int\n\
            aux current: Int\n\
            aux delayed: Int\n\
            out result: Int\n\
            current = x + 1\n\
            delayed = default(current[1], 0) + 1\n\
            result = delayed * 2",
    );
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();

    let mut output = [Value::NoVal];
    for (input, expected) in [(10, 2), (20, 24), (30, 44)] {
        monitor.evaluate(&[Value::Int(input)], &mut output).unwrap();
        assert_eq!(output, [Value::Int(expected)]);
    }
}

#[test]
fn temporal_maple_cycle_preserves_outputs() {
    let specification = elaborated(crate::dsrv_fixtures::spec_maple_sequence());
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();

    let mut output = vec![Value::NoVal; 6];
    for (stage, active) in ["m", "a", "p", "l", "e"].into_iter().zip(0..) {
        monitor
            .evaluate(&[Value::Str(stage.into())], &mut output)
            .unwrap();
        let mut expected = vec![Value::Bool(false); 6];
        expected[active] = Value::Bool(true);
        expected[5] = Value::Bool(true);
        assert_eq!(output, expected);
    }
}

#[test]
fn recursive_delay_state_survives_scheduled_plan() {
    let specification = elaborated(
        "out counter: Int\n\
            aux incremented: Int\n\
            out result: Int\n\
            counter = default(counter[1], 0) + 1\n\
            incremented = counter + 1\n\
            result = incremented + 1",
    );
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();

    let mut output = [Value::NoVal, Value::NoVal];
    for expected in [
        [Value::Int(1), Value::Int(3)],
        [Value::Int(2), Value::Int(4)],
        [Value::Int(3), Value::Int(5)],
    ] {
        monitor.evaluate(&[], &mut output).unwrap();
        assert_eq!(output, expected);
    }
}

#[test]
fn dynamic_schedule_changes_preserve_outputs() {
    let specification = elaborated(
        "in x: Int\n\
            in a_source: Str\n\
            in b_source: Str\n\
            out a: Int\n\
            out b: Int\n\
            a = dynamic(a_source: Int)\n\
            b = dynamic(b_source: Int)",
    );
    let mut monitor =
        DataflowMonitor::compile_with_semantics(specification, Semantics::Untimed).unwrap();

    let mut output = [Value::NoVal, Value::NoVal];
    for (values, expected) in [
        (
            [
                ("x", Value::Int(10)),
                ("a_source", Value::Str("b + 1".into())),
                ("b_source", Value::Str("x".into())),
            ],
            [Value::Int(11), Value::Int(10)],
        ),
        (
            [
                ("x", Value::Int(20)),
                ("a_source", Value::Str("x".into())),
                ("b_source", Value::Str("a + 1".into())),
            ],
            [Value::Int(20), Value::Int(21)],
        ),
        (
            [
                ("x", Value::Int(30)),
                ("a_source", Value::Str("x".into())),
                ("b_source", Value::Str("a + 1".into())),
            ],
            [Value::Int(30), Value::Int(31)],
        ),
    ] {
        let input = input_row(&monitor, &values);
        monitor.evaluate(&input, &mut output).unwrap();
        assert_eq!(output, expected);
    }
}

#[test]
fn deoptimization_state_survives_cached_plan_swaps() {
    let specification = elaborated(
        "in x: Int\n\
            in a_source: Str\n\
            in b_source: Str\n\
            out a: Int\n\
            out b: Int\n\
            out equal: Bool\n\
            a = dynamic(a_source: Int)\n\
            b = dynamic(b_source: Int)\n\
            equal = x == 1",
    );
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut output = [Value::NoVal, Value::NoVal, Value::NoVal];

    let input = input_row(
        &monitor,
        &[
            ("x", Value::Bool(true)),
            ("a_source", Value::Str("b + 1".into())),
            ("b_source", Value::Str("2".into())),
        ],
    );
    monitor.evaluate(&input, &mut output).unwrap();
    assert_eq!(output, [Value::Int(3), Value::Int(2), Value::Bool(false)]);

    let input = input_row(
        &monitor,
        &[
            ("x", Value::Int(1)),
            ("a_source", Value::Str("1".into())),
            ("b_source", Value::Str("a + 1".into())),
        ],
    );
    monitor.evaluate(&input, &mut output).unwrap();
    assert_eq!(output, [Value::Int(1), Value::Int(2), Value::Bool(true)]);
}

#[derive(Clone, Debug, PartialEq)]
struct LifecycleSnapshot {
    failed: bool,
    revision: MonitorRevision,
    interface_revision: InterfaceRevision,
    definition_key: DefinitionKey,
    input_vars: Vec<VarName>,
    output_vars: Vec<VarName>,
    environment_values: Vec<Value>,
    retained_environment_values: Option<Vec<Value>>,
    history_state: Vec<(bool, usize, usize, usize)>,
    execution_capacity: (usize, usize, usize, usize),
    quickening: bool,
}

fn lifecycle_snapshot(monitor: &DataflowMonitor) -> LifecycleSnapshot {
    let history_state = monitor
        .history_bindings
        .iter()
        .map(|binding| {
            binding.map_or((false, 0, 0, 0), |history_id| {
                let history = &monitor.history_store[history_id];
                (
                    true,
                    history.required_depth(),
                    history.len(),
                    history.capacity(),
                )
            })
        })
        .collect();
    LifecycleSnapshot {
        failed: monitor.failed,
        revision: monitor.revision(),
        interface_revision: monitor.interface_revision(),
        definition_key: monitor.definition_key().clone(),
        input_vars: monitor.input_vars().to_vec(),
        output_vars: monitor.output_vars().to_vec(),
        environment_values: monitor.environment_values.clone(),
        retained_environment_values: monitor.retained_environment_values.clone(),
        history_state,
        execution_capacity: monitor.execution.lifecycle_capacity_snapshot(),
        quickening: monitor.quickening_enabled(),
    }
}

fn lifecycle_input_row(monitor: &DataflowMonitor, values: &[(&str, Value)]) -> Vec<Value> {
    input_row(monitor, values)
}

fn lifecycle_counter_program() -> DataflowProgram {
    DataflowProgram::compile_with_semantics(
        elaborated("in x: Int\nout z: Int\nz = default(z[1], 0) + x"),
        Semantics::Untimed,
    )
    .expect("counter specification should compile")
}

fn lifecycle_dynamic_program() -> DataflowProgram {
    DataflowProgram::compile_with_semantics(
        elaborated(
            "in x: Int\nin y: Int\nin source: Str\nout z: Int\naux sum: Int\n\
         z = dynamic(source: Int, {x, y, source, sum})\n\
         sum = x + y",
        ),
        Semantics::Untimed,
    )
    .expect("dynamic specification should compile")
}

#[test]
fn lifecycle_compile_once_clones_payload_but_not_session_state() {
    DataflowProgram::reset_root_compile_counts();
    crate::lang::dsrv::reset_test_pipeline_counts();
    let specification = elaborated("in x: Int\nout z: Int\nz = default(z[1], 0) + x");
    assert_eq!(crate::lang::dsrv::test_pipeline_counts(), (1, 0, 1));
    let program = DataflowProgram::compile_with_semantics(specification, Semantics::Untimed);
    let program = program.expect("counter specification should compile");
    assert_eq!(DataflowProgram::root_compile_counts(), (0, 1));

    let clone = program.clone();
    assert_eq!(
        program.test_payload_identity(),
        clone.test_payload_identity()
    );
    assert_eq!(program.test_payload_strong_count(), 2);

    let mut first = DataflowMonitor::from_program(program.clone());
    let mut second = DataflowMonitor::from_program(program.clone());
    assert_eq!(program.test_payload_strong_count(), 6);
    assert_ne!(
        first.environment_values.as_ptr(),
        second.environment_values.as_ptr(),
        "fresh monitors must own their environments"
    );

    let mut first_rows = Vec::new();
    let mut second_rows = Vec::new();
    first
        .evaluate_trace([[Value::Int(2)], [Value::Int(3)]], &mut first_rows)
        .unwrap();
    second
        .evaluate_trace([[Value::Int(2)], [Value::Int(3)]], &mut second_rows)
        .unwrap();
    assert_eq!(first_rows, vec![vec![Value::Int(2)], vec![Value::Int(5)]]);
    assert_eq!(first_rows, second_rows);
    assert_eq!(DataflowProgram::root_compile_counts(), (0, 1));
    assert_eq!(
        crate::lang::dsrv::test_pipeline_counts(),
        (1, 0, 1),
        "cloning and monitor construction must not parse or type-check the root again"
    );

    DataflowProgram::reset_root_compile_counts();
    let checked_spec = elaborated("in x: Int\nout z: Int\nz = default(z[1], 0) + x");
    let checked_program = DataflowProgram::compile_checked(checked_spec).unwrap();
    let mut checked_monitor = DataflowMonitor::from_program(checked_program);
    checked_monitor.reset();
    checked_monitor.reset();
    assert_eq!(
        DataflowProgram::root_compile_counts(),
        (1, 0),
        "instantiate and reset must not compile the immutable root again"
    );
}

#[test]
fn lifecycle_reset_matches_fresh_after_nominal_and_repeated_cycles() {
    let program = lifecycle_counter_program();
    let mut subject = DataflowMonitor::from_program(program.clone());
    let mut fresh = DataflowMonitor::from_program(program);
    let mut output = [Value::NoVal];

    subject.evaluate(&[Value::Int(2)], &mut output).unwrap();
    subject.evaluate(&[Value::Int(3)], &mut output).unwrap();
    assert_eq!(subject.revision(), MonitorRevision::INITIAL);
    subject.reset();
    assert_eq!(subject.revision(), MonitorRevision::INITIAL);
    assert_eq!(subject.interface_revision(), InterfaceRevision::INITIAL);
    assert_eq!(lifecycle_snapshot(&subject), lifecycle_snapshot(&fresh));

    subject.reset();
    subject.reset();
    let mut subject_rows = Vec::new();
    let mut fresh_rows = Vec::new();
    subject
        .evaluate_trace([[Value::Int(4)], [Value::Int(1)]], &mut subject_rows)
        .unwrap();
    fresh
        .evaluate_trace([[Value::Int(4)], [Value::Int(1)]], &mut fresh_rows)
        .unwrap();
    assert_eq!(subject_rows, fresh_rows);
    assert_eq!(lifecycle_snapshot(&subject), lifecycle_snapshot(&fresh));
}

#[test]
fn lifecycle_interleaved_reset_of_one_monitor_does_not_change_the_other() {
    let program = lifecycle_counter_program();
    let mut first = DataflowMonitor::from_program(program.clone());
    let mut second = DataflowMonitor::from_program(program);
    let mut first_output = [Value::NoVal];
    let mut second_output = [Value::NoVal];
    first.evaluate(&[Value::Int(2)], &mut first_output).unwrap();
    second
        .evaluate(&[Value::Int(10)], &mut second_output)
        .unwrap();
    let second_key = second.definition_key().clone();
    let second_environment = second.environment_values.as_ptr();
    let second_execution = &second.execution as *const _;
    let second_revision = second.revision();
    first.reset();
    first.evaluate(&[Value::Int(2)], &mut first_output).unwrap();
    second
        .evaluate(&[Value::Int(1)], &mut second_output)
        .unwrap();

    assert_eq!(first_output, [Value::Int(2)]);
    assert_eq!(second_output, [Value::Int(11)]);
    assert_eq!(second.definition_key(), &second_key);
    assert_eq!(second.environment_values.as_ptr(), second_environment);
    assert_eq!(&second.execution as *const _, second_execution);
    assert_eq!(second.revision(), second_revision);
}

#[test]
fn lifecycle_reset_after_evaluator_error_clears_poison_and_staging() {
    let program = lifecycle_dynamic_program();
    let mut subject = DataflowMonitor::from_program(program.clone());
    let mut fresh = DataflowMonitor::from_program(program);
    let mut output = [Value::NoVal];
    let valid = |monitor: &DataflowMonitor, source: &str| {
        lifecycle_input_row(
            monitor,
            &[
                ("x", Value::Int(2)),
                ("y", Value::Int(3)),
                ("source", Value::Str(source.into())),
            ],
        )
    };

    subject
        .evaluate(&valid(&subject, "x"), &mut output)
        .unwrap();
    subject
        .evaluate(&valid(&subject, "sum"), &mut output)
        .unwrap();
    let error = subject.evaluate(&valid(&subject, "("), &mut output);
    assert!(matches!(
        error,
        Err(DataflowEvaluationError::ReconfigurableExpressionParse { .. })
    ));
    assert!(subject.failed);
    assert!(matches!(
        subject.evaluate(&valid(&subject, "sum"), &mut output),
        Err(DataflowEvaluationError::MonitorFailed)
    ));

    subject.reset();
    assert!(!subject.failed);
    assert_eq!(lifecycle_snapshot(&subject), lifecycle_snapshot(&fresh));
    subject
        .evaluate(&valid(&subject, "sum"), &mut output)
        .unwrap();
    fresh.evaluate(&valid(&fresh, "sum"), &mut output).unwrap();
    assert_eq!(output, [Value::Int(5)]);
    assert_eq!(lifecycle_snapshot(&subject), lifecycle_snapshot(&fresh));
}

#[test]
fn lifecycle_dynamic_error_variants_preserve_prefix_and_reset_to_nominal() {
    // `z` is declared, so it checks, but it is the node's own stream and so
    // outside its scope: checking on arrival now refuses an undeclared name
    // such as `unknown` before the scope is considered.
    for (source, expected_error) in [("(", "parse"), ("x > 0", "type"), ("z", "context")] {
        let program = if expected_error == "type" {
            let checked = elaborated(
                "in x: Int\nin y: Int\nin source: Str\nout z: Int\naux sum: Int\n\
                           z = dynamic(source: Int, {x, y, source, sum})\n\
                           sum = x + y",
            );
            DataflowProgram::compile_checked(checked).unwrap()
        } else {
            lifecycle_dynamic_program()
        };
        let mut monitor = DataflowMonitor::from_program(program);
        let mut output = [Value::NoVal];
        let valid = lifecycle_input_row(
            &monitor,
            &[
                ("x", Value::Int(2)),
                ("y", Value::Int(3)),
                ("source", Value::Str("x".into())),
            ],
        );
        monitor.evaluate(&valid, &mut output).unwrap();
        let invalid = lifecycle_input_row(
            &monitor,
            &[
                ("x", Value::Int(4)),
                ("y", Value::Int(5)),
                ("source", Value::Str(source.into())),
            ],
        );
        let mut rows = vec![vec![Value::Int(2)]];
        let error = monitor
            .evaluate_trace([invalid], &mut rows)
            .err()
            .unwrap_or_else(|| panic!("expected {expected_error} error for source {source:?}"));
        assert_eq!(rows, vec![vec![Value::Int(2)]]);
        match (expected_error, error) {
            (
                "parse",
                DataflowEvaluationError::ReconfigurableExpressionParse {
                    expression,
                    message,
                },
            ) => {
                assert_eq!(expression.as_str(), "(");
                assert!(!message.is_empty());
            }
            (
                "type",
                DataflowEvaluationError::ReconfigurableExpressionType {
                    expression,
                    message,
                },
            ) => {
                assert_eq!(expression.as_str(), "x > 0");
                assert!(!message.is_empty());
            }
            ("context", DataflowEvaluationError::ReconfigurableExpressionContext(variables)) => {
                assert!(!variables.is_empty());
            }
            (expected, error) => panic!("expected {expected} error, got {error:?}"),
        }
        assert!(monitor.failed);
        assert!(matches!(
            monitor.evaluate(&valid, &mut output),
            Err(DataflowEvaluationError::MonitorFailed)
        ));

        monitor.reset();
        let nominal = lifecycle_input_row(
            &monitor,
            &[
                ("x", Value::Int(2)),
                ("y", Value::Int(3)),
                ("source", Value::Str("sum".into())),
            ],
        );
        monitor.evaluate(&nominal, &mut output).unwrap();
        assert_eq!(output, [Value::Int(5)]);
        assert_eq!(monitor.revision(), MonitorRevision(1));
    }
}

#[test]
fn lifecycle_dynamic_cycle_error_poison_and_reset_are_contained() {
    let program = DataflowProgram::compile_with_semantics(
        elaborated(
            "in x: Int\nin first: Str\nin second: Str\nout a: Int\nout b: Int\n\
         a = dynamic(first: Int)\n\
         b = dynamic(second: Int)",
        ),
        Semantics::Untimed,
    )
    .unwrap();
    let mut monitor = DataflowMonitor::from_program(program);
    let mut output = [Value::NoVal, Value::NoVal];
    let row = |monitor: &DataflowMonitor, first: &str, second: &str| {
        lifecycle_input_row(
            monitor,
            &[
                ("x", Value::Int(1)),
                ("first", Value::Str(first.into())),
                ("second", Value::Str(second.into())),
            ],
        )
    };
    monitor
        .evaluate(&row(&monitor, "x", "a"), &mut output)
        .unwrap();
    let mut rows = vec![vec![Value::Int(1), Value::Int(1)]];
    let cycle = row(&monitor, "b", "a");
    let error = monitor.evaluate_trace([cycle], &mut rows).unwrap_err();
    assert!(matches!(
        error,
        DataflowEvaluationError::DynamicDependencyCycle(variable)
            if variable == VarName::new("a")
    ));
    assert_eq!(rows, vec![vec![Value::Int(1), Value::Int(1)]]);
    monitor.reset();
    let nominal = row(&monitor, "x", "a");
    monitor.evaluate(&nominal, &mut output).unwrap();
    assert_eq!(output, [Value::Int(1), Value::Int(1)]);
}

#[test]
fn lifecycle_trace_is_append_only_and_empty_calls_do_not_check_poison() {
    let no_output =
        DataflowProgram::compile_with_semantics(elaborated("in x: Int"), Semantics::Untimed)
            .expect("no-output specification should compile");
    let mut monitor = DataflowMonitor::from_program(no_output);
    let mut rows = vec![vec![Value::Int(99)]];
    monitor
        .evaluate_trace(std::iter::empty::<Vec<Value>>(), &mut rows)
        .unwrap();
    assert_eq!(rows, vec![vec![Value::Int(99)]]);
    monitor
        .evaluate_trace(
            [[Value::Int(1)], [Value::Int(2)], [Value::Int(3)]],
            &mut rows,
        )
        .unwrap();
    assert_eq!(rows, vec![vec![Value::Int(99)], vec![], vec![], vec![]]);

    let mut poisoned = DataflowMonitor::from_program(lifecycle_dynamic_program());
    let mut output = [Value::NoVal];
    let bad = lifecycle_input_row(
        &poisoned,
        &[
            ("x", Value::Int(1)),
            ("y", Value::Int(1)),
            ("source", Value::Str("(".into())),
        ],
    );
    assert!(poisoned.evaluate(&bad, &mut output).is_err());
    let before = lifecycle_snapshot(&poisoned);
    let mut poisoned_rows = vec![vec![Value::Int(7)]];
    poisoned
        .evaluate_trace(std::iter::empty::<Vec<Value>>(), &mut poisoned_rows)
        .unwrap();
    assert_eq!(poisoned_rows, vec![vec![Value::Int(7)]]);
    assert_eq!(before, lifecycle_snapshot(&poisoned));
    assert!(matches!(
        poisoned.evaluate_trace([bad], &mut poisoned_rows),
        Err(DataflowEvaluationError::MonitorFailed)
    ));
}

#[test]
fn lifecycle_trace_preserves_complete_declared_rows_in_output_order() {
    let source = VarName::new("source");
    let names = [
        "integer", "boolean", "string", "list", "tuple", "map", "unit", "absent", "waiting",
    ];
    let expressions = BTreeMap::from([
        (VarName::new("integer"), Expr::Val(SyntaxLiteral::Int(7))),
        (
            VarName::new("boolean"),
            Expr::Val(SyntaxLiteral::Bool(true)),
        ),
        (
            VarName::new("string"),
            Expr::Val(SyntaxLiteral::Str("a".into())),
        ),
        (
            VarName::new("list"),
            Expr::List(
                vec![
                    Expr::Val(SyntaxLiteral::Int(1)),
                    Expr::Val(SyntaxLiteral::Int(2)),
                ]
                .into(),
            ),
        ),
        (
            VarName::new("tuple"),
            Expr::Tuple(
                vec![
                    Expr::Val(SyntaxLiteral::Int(1)),
                    Expr::Val(SyntaxLiteral::Bool(false)),
                ]
                .into(),
            ),
        ),
        (
            VarName::new("map"),
            Expr::Map(BTreeMap::from([(
                "k".into(),
                Expr::Val(SyntaxLiteral::Int(3)),
            )])),
        ),
        (VarName::new("unit"), Expr::Val(SyntaxLiteral::Unit)),
        (VarName::new("absent"), Expr::Var(source.clone())),
        (
            VarName::new("waiting"),
            Expr::SIndex(Box::new(Expr::Var(source.clone())), 1),
        ),
    ]);
    let specification = DsrvSpecification::new(
        BTreeSet::from([source]),
        names.iter().map(|name| VarName::new(*name)).collect(),
        expressions,
        BTreeMap::from([(VarName::new("source"), crate::core::StreamType::Int)]),
        [],
    )
    .check_and_elaborate(crate::TypeCheckOptions::GRADUAL)
    .without_warnings()
    .expect("complete-value spec should check");
    let mut monitor = DataflowMonitor::compile_with_semantics(specification, Semantics::Untimed)
        .expect("complete-value spec should compile");
    let mut output = Vec::new();
    monitor
        .evaluate_trace([[Value::NoVal]], &mut output)
        .unwrap();
    assert_eq!(
        monitor.output_vars(),
        &names
            .iter()
            .map(|name| VarName::new(*name))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        output,
        vec![vec![
            Value::Int(7),
            Value::Bool(true),
            Value::Str("a".into()),
            Value::List(vec![Value::Int(1), Value::Int(2)].into()),
            Value::Tuple(vec![Value::Int(1), Value::Bool(false)].into()),
            Value::Map(BTreeMap::from([("k".into(), Value::Int(3))])),
            Value::Unit,
            Value::NoVal,
            Value::Deferred,
        ]]
    );
}

#[test]
fn lifecycle_equal_encoded_timestamps_are_distinct_ticks_and_chunking_is_exact() {
    let program = DataflowProgram::compile_with_semantics(
        elaborated(
            "in timestamp: Int\nout seen: Int\nout ticks: Int\n\
         seen = timestamp\nticks = default(ticks[1], 0) + 1",
        ),
        Semantics::Untimed,
    )
    .unwrap();
    let rows = vec![vec![Value::Int(7)], vec![Value::Int(7)]];
    let mut whole = DataflowMonitor::from_program(program.clone());
    let mut chunked = DataflowMonitor::from_program(program.clone());
    let mut individual = DataflowMonitor::from_program(program);
    let mut whole_output = Vec::new();
    let mut chunked_output = Vec::new();
    let mut individual_output = Vec::new();
    whole
        .evaluate_trace(rows.iter(), &mut whole_output)
        .unwrap();
    for chunk in [&rows[0..0], &rows[0..1], &rows[1..1], &rows[1..2]] {
        chunked
            .evaluate_trace(chunk.iter(), &mut chunked_output)
            .unwrap();
    }
    for row in &rows {
        let mut one = vec![Value::NoVal, Value::NoVal];
        individual.evaluate(row, &mut one).unwrap();
        individual_output.push(one);
    }
    assert_eq!(
        whole_output,
        vec![
            vec![Value::Int(7), Value::Int(1)],
            vec![Value::Int(7), Value::Int(2)]
        ]
    );
    assert_eq!(whole_output, chunked_output);
    assert_eq!(
        individual_output,
        vec![
            vec![Value::Int(7), Value::Int(1)],
            vec![Value::Int(7), Value::Int(2)]
        ]
    );
}

#[test]
fn lifecycle_past_recursive_and_lazy_state_restarts_cold() {
    let spec = "in x: Int\nin choose: Bool\nout past: Int\nout sum: Int\nout branch: Int\n\
                past = x[2]\n\
                sum = default(sum[1], 0) + x\n\
                branch = if choose then x[1] else default(x[2], -1)";
    let program =
        DataflowProgram::compile_with_semantics(elaborated(&spec), Semantics::Untimed).unwrap();
    let rows_a = vec![
        vec![Value::Int(10), Value::Bool(true)],
        vec![Value::Int(20), Value::Bool(false)],
        vec![Value::Int(30), Value::Bool(true)],
        vec![Value::Int(40), Value::Bool(false)],
    ];
    let rows_b = vec![
        vec![Value::Int(5), Value::Bool(false)],
        vec![Value::Int(6), Value::Bool(true)],
        vec![Value::Int(7), Value::Bool(false)],
    ];
    let expected_a = vec![
        vec![Value::Deferred, Value::Int(10), Value::Deferred],
        vec![Value::Deferred, Value::Int(30), Value::Int(-1)],
        vec![Value::Int(10), Value::Int(60), Value::Int(20)],
        vec![Value::Int(20), Value::Int(100), Value::Int(20)],
    ];
    let expected_b = vec![
        vec![Value::Deferred, Value::Int(5), Value::Int(-1)],
        vec![Value::Deferred, Value::Int(11), Value::Int(5)],
        vec![Value::Int(5), Value::Int(18), Value::Int(5)],
    ];
    let mut subject = DataflowMonitor::from_program(program.clone());
    let mut fresh = DataflowMonitor::from_program(program);
    let mut subject_a = Vec::new();
    subject
        .evaluate_trace(rows_a.iter(), &mut subject_a)
        .unwrap();
    assert_eq!(subject_a, expected_a);
    subject.reset();
    let mut subject_b = Vec::new();
    let mut fresh_b = Vec::new();
    subject
        .evaluate_trace(rows_b.iter(), &mut subject_b)
        .unwrap();
    fresh.evaluate_trace(rows_b.iter(), &mut fresh_b).unwrap();
    assert_eq!(subject_b, expected_b);
    assert_eq!(subject_b, fresh_b);
    assert_eq!(lifecycle_snapshot(&subject), lifecycle_snapshot(&fresh));
}

#[test]
fn lifecycle_past_recursive_and_lazy_reset_matches_both_quickening_modes() {
    let spec = "in x: Int\nin choose: Bool\nout past: Int\nout sum: Int\nout branch: Int\n\
                past = x[2]\n\
                sum = default(sum[1], 0) + x\n\
                branch = if choose then x[1] else default(x[2], -1)";
    let rows_a = [
        vec![Value::Int(10), Value::Bool(true)],
        vec![Value::Int(20), Value::Bool(false)],
        vec![Value::Int(30), Value::Bool(true)],
    ];
    let rows_b = [
        vec![Value::Int(5), Value::Bool(false)],
        vec![Value::Int(6), Value::Bool(true)],
    ];
    for quickening in [false, true] {
        let program =
            DataflowProgram::compile_with_semantics(elaborated(&spec), Semantics::Untimed).unwrap();
        let mut subject = DataflowMonitor::from_program(program.clone());
        let mut fresh = DataflowMonitor::from_program(program);
        subject.set_quickening(quickening);
        fresh.set_quickening(quickening);
        let mut ignored = Vec::new();
        subject.evaluate_trace(rows_a.iter(), &mut ignored).unwrap();
        subject.reset();
        let mut subject_rows = Vec::new();
        let mut fresh_rows = Vec::new();
        subject
            .evaluate_trace(rows_b.iter(), &mut subject_rows)
            .unwrap();
        fresh
            .evaluate_trace(rows_b.iter(), &mut fresh_rows)
            .unwrap();
        assert_eq!(subject_rows, fresh_rows);
        assert_eq!(lifecycle_snapshot(&subject), lifecycle_snapshot(&fresh));
        assert_eq!(subject.quickening_enabled(), quickening);
    }
}

#[test]
fn lifecycle_defer_noval_and_deferred_are_rows_and_eof_does_not_finalize() {
    let program = DataflowProgram::compile_with_semantics(
        elaborated("in x: Int\nin source: Str\nout z: Int\nz = defer(source: Int)"),
        Semantics::Untimed,
    )
    .unwrap();
    for source in [Value::NoVal, Value::Deferred] {
        let mut monitor = DataflowMonitor::from_program(program.clone());
        let mut rows = Vec::new();
        let input = lifecycle_input_row(
            &monitor,
            &[("x", Value::Int(2)), ("source", source.clone())],
        );
        monitor.evaluate_trace([input], &mut rows).unwrap();
        assert_eq!(rows, vec![vec![source]]);
        let before_eof = lifecycle_snapshot(&monitor);
        monitor
            .evaluate_trace(std::iter::empty::<Vec<Value>>(), &mut rows)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(before_eof, lifecycle_snapshot(&monitor));

        let continuation = lifecycle_input_row(
            &monitor,
            &[("x", Value::Int(3)), ("source", Value::Str("x + 1".into()))],
        );
        let mut continuation_output = [Value::NoVal];
        monitor
            .evaluate(&continuation, &mut continuation_output)
            .unwrap();
        assert_eq!(continuation_output, [Value::Int(4)]);

        let mut reset_monitor = DataflowMonitor::from_program(program.clone());
        let mut fresh = DataflowMonitor::from_program(program.clone());
        reset_monitor.reset();
        let reset_input = lifecycle_input_row(
            &reset_monitor,
            &[
                ("x", Value::Int(4)),
                ("source", Value::Str("x + 100".into())),
            ],
        );
        let fresh_input = lifecycle_input_row(
            &fresh,
            &[
                ("x", Value::Int(4)),
                ("source", Value::Str("x + 100".into())),
            ],
        );
        let mut reset_output = [Value::NoVal];
        let mut fresh_output = [Value::NoVal];
        reset_monitor
            .evaluate(&reset_input, &mut reset_output)
            .unwrap();
        fresh.evaluate(&fresh_input, &mut fresh_output).unwrap();
        assert_eq!(reset_output, fresh_output);
        assert_eq!(reset_output, [Value::Int(104)]);
    }
}

#[test]
fn lifecycle_dynamic_and_defer_activation_history_and_sealing_reset() {
    let dynamic = lifecycle_dynamic_program();
    let mut subject = DataflowMonitor::from_program(dynamic.clone());
    let mut fresh = DataflowMonitor::from_program(dynamic);
    let mut output = [Value::NoVal];
    for (x, y, source, expected) in [
        (2, 3, "x", Value::Int(2)),
        (4, 5, "sum", Value::Int(9)),
        (6, 7, "default(x[2], 0)", Value::Int(0)),
    ] {
        let input = lifecycle_input_row(
            &subject,
            &[
                ("x", Value::Int(x)),
                ("y", Value::Int(y)),
                ("source", Value::Str(source.into())),
            ],
        );
        subject.evaluate(&input, &mut output).unwrap();
        assert_eq!(output, [expected]);
    }
    assert!(history_depth_for(&subject, "x").unwrap_or(0) >= 2);
    assert!(subject.retained_environment_values.is_some());
    subject.reset();
    assert_eq!(lifecycle_snapshot(&subject), lifecycle_snapshot(&fresh));
    let reset_input = lifecycle_input_row(
        &subject,
        &[
            ("x", Value::Int(2)),
            ("y", Value::Int(3)),
            ("source", Value::Str("sum".into())),
        ],
    );
    subject.evaluate(&reset_input, &mut output).unwrap();
    fresh.evaluate(&reset_input, &mut output).unwrap();
    assert_eq!(output, [Value::Int(5)]);
    assert_eq!(lifecycle_snapshot(&subject), lifecycle_snapshot(&fresh));

    let defer = DataflowProgram::compile_with_semantics(
        elaborated("in x: Int\nin source: Str\nout z: Int\nz = defer(source: Int)"),
        Semantics::Untimed,
    )
    .unwrap();
    let mut deferred = DataflowMonitor::from_program(defer.clone());
    let mut deferred_fresh = DataflowMonitor::from_program(defer);
    let mut deferred_output = [Value::NoVal];
    let first = lifecycle_input_row(
        &deferred,
        &[("x", Value::Int(2)), ("source", Value::Str("x + 1".into()))],
    );
    deferred.evaluate(&first, &mut deferred_output).unwrap();
    assert_eq!(deferred_output, [Value::Int(3)]);
    assert!(deferred.revision().0 > 0);
    deferred.reset();
    assert_eq!(
        lifecycle_snapshot(&deferred),
        lifecycle_snapshot(&deferred_fresh)
    );
    let after_reset = lifecycle_input_row(
        &deferred,
        &[
            ("x", Value::Int(4)),
            ("source", Value::Str("x + 100".into())),
        ],
    );
    let fresh_after_reset = after_reset.clone();
    let mut reset_output = [Value::NoVal];
    let mut fresh_output = [Value::NoVal];
    deferred.evaluate(&after_reset, &mut reset_output).unwrap();
    deferred_fresh
        .evaluate(&fresh_after_reset, &mut fresh_output)
        .unwrap();
    assert_eq!(reset_output, fresh_output);
    assert_eq!(reset_output, [Value::Int(104)]);

    let continuation = lifecycle_input_row(
        &deferred,
        &[
            ("x", Value::Int(5)),
            ("source", Value::Str("x + 100".into())),
        ],
    );
    let fresh_continuation = continuation.clone();
    deferred.evaluate(&continuation, &mut reset_output).unwrap();
    deferred_fresh
        .evaluate(&fresh_continuation, &mut fresh_output)
        .unwrap();
    assert_eq!(reset_output, fresh_output);
    assert_eq!(reset_output, [Value::Int(105)]);
    assert_eq!(
        lifecycle_snapshot(&deferred),
        lifecycle_snapshot(&deferred_fresh)
    );
}

#[test]
fn lifecycle_arity_errors_are_preflight_and_trace_stops_at_first_failed_row() {
    let mut monitor = DataflowMonitor::from_program(lifecycle_counter_program());
    let mut output = [Value::Int(77)];
    assert!(matches!(
        monitor.evaluate(&[], &mut output),
        Err(DataflowEvaluationError::InputCountMismatch {
            expected: 1,
            actual: 0
        })
    ));
    assert_eq!(output, [Value::Int(77)]);
    assert!(matches!(
        monitor.evaluate(&[Value::Int(1), Value::Int(2)], &mut output),
        Err(DataflowEvaluationError::InputCountMismatch {
            expected: 1,
            actual: 2
        })
    ));
    assert!(matches!(
        monitor.evaluate(&[Value::Int(1)], &mut []),
        Err(DataflowEvaluationError::OutputCountMismatch {
            expected: 1,
            actual: 0
        })
    ));
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(2)]);

    struct CountingRows {
        rows: Vec<Vec<Value>>,
        pulls: usize,
    }
    impl Iterator for CountingRows {
        type Item = Vec<Value>;
        fn next(&mut self) -> Option<Self::Item> {
            self.pulls += 1;
            self.rows.pop()
        }
    }
    let mut iterator = CountingRows {
        rows: vec![
            vec![Value::Int(4)],
            vec![],
            vec![Value::Int(3)],
            vec![Value::Int(99)],
        ]
        .into_iter()
        .rev()
        .collect(),
        pulls: 0,
    };
    let mut trace = vec![vec![Value::Int(99)]];
    let error = monitor
        .evaluate_trace(&mut iterator, &mut trace)
        .unwrap_err();
    assert!(matches!(
        error,
        DataflowEvaluationError::InputCountMismatch {
            expected: 1,
            actual: 0
        }
    ));
    assert_eq!(iterator.pulls, 2);
    assert_eq!(trace, vec![vec![Value::Int(99)], vec![Value::Int(6)]]);
    monitor.evaluate(&[Value::Int(4)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(10)]);

    let mut poisoned = DataflowMonitor::from_program(lifecycle_dynamic_program());
    let mut poisoned_output = [Value::NoVal];
    let invalid = lifecycle_input_row(
        &poisoned,
        &[
            ("x", Value::Int(1)),
            ("y", Value::Int(1)),
            ("source", Value::Str("(".into())),
        ],
    );
    assert!(poisoned.evaluate(&invalid, &mut poisoned_output).is_err());
    assert!(matches!(
        poisoned.evaluate(&[], &mut poisoned_output),
        Err(DataflowEvaluationError::MonitorFailed)
    ));
}

#[test]
fn lifecycle_transfer_is_stateful_before_reset_but_reset_is_cold_and_no_transfer() {
    let spec = "in x: Int\nout z: Int\nz = default(z[1], 0) + x";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&spec), Semantics::Untimed).unwrap();
    let mut output = [Value::NoVal];
    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    let candidate = DataflowMonitor::compile_with_semantics(
        elaborated("in a: Int\nin x: Int\nout z: Int\nz = default(z[1], 0) + x"),
        Semantics::Untimed,
    )
    .unwrap();
    let report = monitor
        .reconfigure(
            candidate.program,
            ContextTransferPolicy::MatchingStreamState,
        )
        .unwrap();
    assert_eq!(
        report.context_transfer.streams[0].outcome,
        StreamStateTransferOutcome::Transferred
    );
    // Positional rows follow declaration order, so the new `a` is absent and
    // `x` receives 3: the transferred `z` state continues accumulating.
    monitor
        .evaluate(&[Value::NoVal, Value::Int(3)], &mut output)
        .unwrap();
    assert_eq!(output, [Value::Int(6)]);
    assert_eq!(
        monitor.configuration.reconfiguration_transfer_policy,
        ContextTransferPolicy::MatchingStreamState
    );
    monitor.reset();
    assert_eq!(monitor.revision(), MonitorRevision::INITIAL);
    monitor.evaluate(&[Value::Int(3)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(3)]);

    monitor.set_reconfiguration_transfer_policy(ContextTransferPolicy::None);
    monitor.reset();
    assert_eq!(
        monitor.configuration.reconfiguration_transfer_policy,
        ContextTransferPolicy::None
    );
}

#[test]
fn lifecycle_reset_restores_original_root_and_interface_after_replacement() {
    let base = DataflowProgram::compile_with_semantics(
        elaborated("in x: Int\nout z: Int\nz = default(z[1], 0) + x"),
        Semantics::Untimed,
    )
    .unwrap();
    let compatible = DataflowProgram::compile_with_semantics(
        elaborated("in x: Int\nout z: Int\nz = default(z[1], 0) + x + 1"),
        Semantics::Untimed,
    )
    .unwrap();
    let interface = DataflowProgram::compile_with_semantics(
        elaborated("in y: Int\nin x: Int\nout other: Int\nout z: Int\nother = y\nz = x"),
        Semantics::Untimed,
    )
    .unwrap();
    let base_key = base.definition_key().clone();
    let mut monitor = DataflowMonitor::from_program(base);
    monitor.set_quickening(false);
    let mut output = [Value::NoVal];
    let report = monitor
        .reconfigure(compatible, ContextTransferPolicy::None)
        .unwrap();
    assert!(report.monitor_changed);
    assert_eq!(monitor.revision(), report.monitor_revision);
    assert_eq!(monitor.interface_revision(), report.interface_revision);
    assert_eq!(monitor.input_vars(), &[VarName::new("x")]);
    assert_eq!(monitor.output_vars(), &[VarName::new("z")]);
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(3)]);
    let interface_report = monitor
        .reconfigure(interface, ContextTransferPolicy::None)
        .unwrap();
    assert!(interface_report.monitor_changed);
    assert!(interface_report.interface_changed);
    assert_eq!(monitor.revision(), interface_report.monitor_revision);
    assert_eq!(
        monitor.interface_revision(),
        interface_report.interface_revision
    );
    assert_eq!(
        monitor.input_vars(),
        &[VarName::new("y"), VarName::new("x")]
    );
    assert_eq!(
        monitor.output_vars(),
        &[VarName::new("other"), VarName::new("z")]
    );
    monitor.reset();
    assert_eq!(monitor.definition_key(), &base_key);
    assert_eq!(monitor.input_vars(), &[VarName::new("x")]);
    assert_eq!(monitor.output_vars(), &[VarName::new("z")]);
    assert_eq!(monitor.revision(), MonitorRevision::INITIAL);
    assert_eq!(monitor.interface_revision(), InterfaceRevision::INITIAL);
    assert!(!monitor.quickening_enabled());
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(2)]);
}

#[test]
fn lifecycle_repeated_reset_rebuilds_bounded_session_capacity() {
    let program = DataflowProgram::compile_with_semantics(
        elaborated("in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)"),
        Semantics::Untimed,
    )
    .unwrap();
    let mut monitor = DataflowMonitor::from_program(program.clone());
    let fresh = DataflowMonitor::from_program(program);
    let fresh_shape = lifecycle_snapshot(&fresh);
    let mut output = [Value::NoVal];
    for cycle in 0..100 {
        let source = if cycle % 2 == 0 { "x[4]" } else { "x[1]" };
        let input = lifecycle_input_row(
            &monitor,
            &[
                ("x", Value::Int(cycle)),
                ("source", Value::Str(source.into())),
            ],
        );
        monitor.evaluate(&input, &mut output).unwrap();
        monitor.reset();
        let snapshot = lifecycle_snapshot(&monitor);
        assert_eq!(
            snapshot.execution_capacity, fresh_shape.execution_capacity,
            "reset cycle {cycle} retained an execution cache"
        );
        assert_eq!(snapshot.history_state, fresh_shape.history_state);
        assert_eq!(snapshot.environment_values, fresh_shape.environment_values);
        assert_eq!(
            monitor.history_store.len(),
            fresh.history_store.len(),
            "reset cycle {cycle} retained dynamic history entries"
        );
    }
}

#[test]
fn lifecycle_old_constructors_and_iterator_forms_remain_usable() {
    let specification = elaborated("in x: Int\nout z: Int\nz = x");
    let program = DataflowProgram::compile_checked(specification.clone()).unwrap();
    let mut monitor = DataflowMonitor::new(program.clone());
    assert_eq!(monitor.program().definition_key(), program.definition_key());

    let borrowed = vec![vec![Value::Int(1)], vec![Value::Int(2)]];
    let mut outputs = Vec::new();
    monitor.evaluate_trace(&borrowed, &mut outputs).unwrap();
    let mut iterator = borrowed.iter();
    monitor.reset();
    outputs.clear();
    monitor.evaluate_trace(&mut iterator, &mut outputs).unwrap();
    assert_eq!(outputs, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);

    let mut from_try = DataflowMonitor::compile_checked(specification).unwrap();
    let mut single = vec![Value::NoVal];
    from_try.evaluate(&[Value::Int(3)], &mut single).unwrap();
    assert_eq!(single, [Value::Int(3)]);
}

#[cfg(feature = "jit")]
#[test]
fn lifecycle_jit_reset_preserves_selection_but_clears_hotness_and_artifacts() {
    let specification = elaborated("in x: Int\nout z: Int\nz = x + 1");
    let program = DataflowProgram::compile_checked(specification).unwrap();
    let mut monitor =
        DataflowMonitor::from_program_with_jit(program.clone(), JitConfig::after_events(2));
    let mut output = [Value::NoVal];
    assert_eq!(monitor.jit_report().unwrap().plan(), JitPlan::Pending);
    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    monitor.evaluate(&[Value::Int(3)], &mut output).unwrap();
    assert!(test_support::jit_artifact_count(&monitor) > 0);
    assert_eq!(monitor.jit_report().unwrap().plan(), JitPlan::WholeSchedule);

    monitor.reset();
    assert_eq!(monitor.configuration.jit, Some(JitConfig::after_events(2)));
    assert_eq!(monitor.jit_report().unwrap().plan(), JitPlan::Pending);
    assert_eq!(test_support::jit_artifact_count(&monitor), 0);
    monitor.evaluate(&[Value::Int(10)], &mut output).unwrap();
    monitor.evaluate(&[Value::Int(11)], &mut output).unwrap();
    assert_eq!(test_support::jit_artifact_count(&monitor), 0);
    monitor.evaluate(&[Value::Int(12)], &mut output).unwrap();
    assert!(test_support::jit_artifact_count(&monitor) > 0);

    let eager_specification = elaborated("in x: Int\nout z: Int\nz = x + 1");
    let eager_program = DataflowProgram::compile_checked(eager_specification).unwrap();
    let mut eager = DataflowMonitor::from_program_with_jit(eager_program, JitConfig::eager());
    assert_eq!(eager.jit_report().unwrap().plan(), JitPlan::WholeSchedule);
    assert!(test_support::jit_artifact_count(&eager) > 0);
    eager.reset();
    assert_eq!(eager.jit_report().unwrap().plan(), JitPlan::WholeSchedule);

    let unsupported_specification = elaborated("in x: Str\nout z: Str\nz = x");
    let unsupported_program = DataflowProgram::compile_checked(unsupported_specification).unwrap();
    let mut unsupported =
        DataflowMonitor::from_program_with_jit(unsupported_program, JitConfig::eager());
    assert_eq!(
        unsupported.jit_report().unwrap().plan(),
        JitPlan::Unavailable
    );
    assert_eq!(unsupported.jit_report().unwrap().unsupported_streams(), [0]);
    assert_eq!(unsupported.jit_report().unwrap().backend_error(), None);
    let mut string_output = [Value::NoVal];
    unsupported
        .evaluate(&[Value::Str("hello".into())], &mut string_output)
        .unwrap();
    assert_eq!(string_output, [Value::Str("hello".into())]);
    unsupported.reset();
    assert_eq!(
        unsupported.jit_report().unwrap().plan(),
        JitPlan::Unavailable
    );
}

/// Monitors of one specification in every execution configuration.
fn branch_recursion_monitors(specification: &str) -> Vec<(String, DataflowMonitor)> {
    let mut monitors = Vec::new();
    for quickening in [false, true] {
        let mut untyped =
            DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
                .unwrap();
        untyped.set_quickening(quickening);
        monitors.push((format!("untyped, quickening {quickening}"), untyped));
        let checked = elaborated(&specification);
        let mut typed = DataflowMonitor::compile_checked(checked).unwrap();
        typed.set_quickening(quickening);
        monitors.push((format!("typed, quickening {quickening}"), typed));
    }
    #[cfg(feature = "jit")]
    {
        let checked = elaborated(&specification);
        monitors.push((
            "typed, eager JIT".to_owned(),
            DataflowMonitor::compile_checked_with_jit(checked, JitConfig::eager()).unwrap(),
        ));
    }
    monitors
}

fn int_rows(values: &[i64]) -> Vec<Vec<Value>> {
    values
        .iter()
        .map(|value| vec![Value::Int(*value)])
        .collect()
}

#[test]
fn a_recursive_delay_inside_a_branch_reads_the_stream_history() {
    // `held[1]` is the stream's previous output, whichever branch produced
    // it; the branch that did not run at that tick contributes nothing.
    let inputs = [-1, 5, -1, -1, 7, -1];
    for (equation, expected) in [
        (
            "if x >= 0 then x else default(held[1], -1)",
            [-1, 5, 5, 5, 7, 7],
        ),
        (
            "if x >= 0 then x else default(held[1], 0) + 100",
            [100, 5, 105, 205, 7, 107],
        ),
        (
            "if x >= 0 then 1 else default(held[1], 0) + 1",
            [1, 1, 2, 3, 1, 2],
        ),
        (
            "if x < 0 then default(held[1], -1) else x",
            [-1, 5, 5, 5, 7, 7],
        ),
        (
            "if x >= 0 then x else if x < -5 then 0 else default(held[1], -1)",
            [-1, 5, 5, 5, 7, 7],
        ),
        (
            "if x >= 0 then default(held[1], 0) + x else default(held[1], 0) - 1",
            [-1, 4, 3, 2, 9, 8],
        ),
    ] {
        let specification = format!("in x: Int\nout held: Int\nheld = {equation}");
        for (label, mut monitor) in branch_recursion_monitors(&specification) {
            let mut rows = Vec::new();
            monitor
                .evaluate_trace(int_rows(&inputs), &mut rows)
                .unwrap();
            let expected: Vec<_> = expected
                .iter()
                .map(|value| vec![Value::Int(*value)])
                .collect();
            assert_eq!(rows, expected, "{equation} ({label})");
        }
    }
}

#[test]
fn a_latched_branch_verdict_stays_latched() {
    // The shape of a sticky trustworthiness verdict written with a branch.
    let specification = "in x: Int\nout ok: Bool\n\
        ok = if x < 0 then false else default(ok[1], true)";
    for (label, mut monitor) in branch_recursion_monitors(specification) {
        let mut rows = Vec::new();
        monitor
            .evaluate_trace(int_rows(&[1, 2, -1, 3, 4]), &mut rows)
            .unwrap();
        let verdicts: Vec<_> = rows.into_iter().map(|row| row[0].clone()).collect();
        assert_eq!(
            verdicts,
            [true, true, false, false, false].map(Value::Bool),
            "{label}"
        );
    }
}

#[test]
fn branch_recursion_restarts_cold_and_transfers_with_its_stream() {
    let specification = "in x: Int\nout held: Int\n\
        held = if x >= 0 then x else default(held[1], -1)";
    let mut monitor =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    let mut rows = Vec::new();
    monitor
        .evaluate_trace(int_rows(&[4, -1]), &mut rows)
        .unwrap();
    assert_eq!(rows[1], [Value::Int(4)]);
    monitor.reset();
    rows.clear();
    monitor.evaluate_trace(int_rows(&[-1]), &mut rows).unwrap();
    assert_eq!(rows[0], [Value::Int(-1)]);

    // A compatible replacement keeps the held value.
    monitor.evaluate_trace(int_rows(&[9]), &mut rows).unwrap();
    let candidate =
        DataflowMonitor::compile_with_semantics(elaborated(&specification), Semantics::Untimed)
            .unwrap();
    monitor
        .reconfigure(
            candidate.program,
            ContextTransferPolicy::MatchingStreamState,
        )
        .unwrap();
    rows.clear();
    monitor.evaluate_trace(int_rows(&[-1]), &mut rows).unwrap();
    assert_eq!(rows[0], [Value::Int(9)]);
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(64))]

    #[test]
    fn branch_recursion_matches_a_reference_hold(
        inputs in proptest::collection::vec(-3i64..4, 1..40),
    ) {
        let specification = "in x: Int\nout held: Int\nout count: Int\n\
            held = if x >= 0 then x else default(held[1], -1)\n\
            count = if x == 0 then 0 else default(count[1], 0) + 1";
        let mut last = -1;
        let mut count = 0;
        let expected: Vec<_> = inputs
            .iter()
            .map(|&x| {
                if x >= 0 {
                    last = x;
                }
                count = if x == 0 { 0 } else { count + 1 };
                vec![Value::Int(last), Value::Int(count)]
            })
            .collect();
        for (label, mut monitor) in branch_recursion_monitors(specification) {
            let mut rows = Vec::new();
            monitor.evaluate_trace(int_rows(&inputs), &mut rows).unwrap();
            proptest::prop_assert_eq!(&rows, &expected, "{}", label);
        }
    }
}
