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
use crate::dataflow::execution::evaluator_state::{reset_state_clone_count, state_clone_count};
use crate::dataflow::stream_id::StreamId;
use crate::dataflow::{
    ContextTransferReport, DataflowEvaluationError, ReconfigurationMapping, StreamMapping,
    StreamStateTransferOutcome,
};
#[cfg(feature = "jit")]
use crate::dataflow::{JitConfig, JitPlan};
use crate::{CheckedDsrvSpecification, DsrvSpecification};

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
    let specification = "in x: Int\nin property: Expr<Int>\nout result: Int\n\
                         result = dynamic(property)"
        .parse::<CheckedDsrvSpecification>()
        .expect("Expr<T> source should type-check");
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
    let mut monitor = DataflowMonitor::compile_untyped(old_spec.parse().unwrap()).unwrap();
    let candidate = DataflowMonitor::compile_untyped(new_spec.parse().unwrap()).unwrap();

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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
    monitor.set_quickening(false);
    assert_eq!(monitor.revision(), MonitorRevision::INITIAL);
    assert!(!monitor.quickening_enabled());

    let mut output = [Value::NoVal, Value::NoVal];
    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(1), Value::Deferred]);

    let mut candidate = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
    let mut output = [Value::NoVal];
    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    let source_history_id = history_id_for(&monitor, "x");
    let source_history_slots = monitor.history_store[source_history_id].slots_ptr();

    let candidate = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(old_spec.parse().unwrap()).unwrap();
    #[cfg(feature = "jit")]
    monitor.enable_jit(JitConfig::eager());
    let mut output = [Value::NoVal];
    for value in [1, 2, 3] {
        monitor.evaluate(&[Value::Int(value)], &mut output).unwrap();
    }

    let candidate = DataflowMonitor::compile_untyped(new_spec.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(old_spec.parse().unwrap()).unwrap();
    let mut output = [Value::NoVal];
    for value in [1, 2] {
        monitor.evaluate(&[Value::Int(value)], &mut output).unwrap();
    }

    let candidate = DataflowMonitor::compile_untyped(new_spec.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(old_spec.parse().unwrap()).unwrap();
    let mut output = [Value::NoVal];
    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    assert_eq!(output, [Value::Deferred]);
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(2)]);

    let candidate = DataflowMonitor::compile_untyped(new_spec.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(old_spec.parse().unwrap()).unwrap();
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

    let candidate = DataflowMonitor::compile_untyped(new_spec.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(old_spec.parse().unwrap()).unwrap();
    let mut output = [Value::NoVal];
    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();

    let candidate = DataflowMonitor::compile_untyped(new_spec.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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

    let candidate = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let compact =
        DataflowMonitor::compile_untyped("in x: Int\nout z: Int\nz = x + 1".parse().unwrap())
            .unwrap();
    let formatted =
        DataflowMonitor::compile_untyped("in x: Int\nout z: Int\nz = ( x + 1 )".parse().unwrap())
            .unwrap();
    let changed =
        DataflowMonitor::compile_untyped("in x: Int\nout z: Int\nz = x + 2".parse().unwrap())
            .unwrap();
    assert_eq!(compact.definition_key(), formatted.definition_key());
    assert_ne!(compact.definition_key(), changed.definition_key());
}

#[test]
fn compiled_program_is_separate_from_monitor_state() {
    let specification = "in x: Int\nout z: Int\nz = x + 1"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
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
    let specification = "in x: Int\nout z: Int\nz = x + 1"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();

    assert!(monitor.retained_environment_values.is_none());
}

#[test]
fn matching_root_transfer_initializes_changed_stream_state_after_execution() {
    let old_spec = "in x: Int\nout z: Int\nz = x";
    let new_spec = "in x: Int\nout z: Int\nz = x + 1";
    let mut monitor = DataflowMonitor::compile_untyped(old_spec.parse().unwrap()).unwrap();
    let mut output = [Value::NoVal];
    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    let replacement = DataflowMonitor::compile_untyped(new_spec.parse().unwrap()).unwrap();

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
    let mut source = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
    source.set_quickening(false);
    let mut output = [Value::NoVal];
    source.evaluate(&[Value::Int(1)], &mut output).unwrap();

    let mut candidate = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
    candidate.set_quickening(true);
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
    assert!(source.quickening_enabled());
    let history_id = history_id_for(&source, "x");
    assert!(source.history_store[history_id].is_empty());

    source.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(output, [Value::Deferred]);
}

#[test]
fn matching_root_transfer_preserves_exact_and_initializes_changed_streams() {
    let source_spec = "in x: Int\nout a: Int\nout b: Int\na = x[1]\nb = x";
    let candidate_spec = "in x: Int\nout a: Int\nout b: Int\na = x[1]\nb = x + 1";
    let mut source = DataflowMonitor::compile_untyped(source_spec.parse().unwrap()).unwrap();
    let mut output = [Value::NoVal, Value::NoVal];
    source.evaluate(&[Value::Int(1)], &mut output).unwrap();

    let candidate = DataflowMonitor::compile_untyped(candidate_spec.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(old_spec.parse().unwrap()).unwrap();
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

    let replacement = DataflowMonitor::compile_untyped(new_spec.parse().unwrap()).unwrap();
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
    let mut old = DataflowMonitor::compile_untyped(old_spec.parse().unwrap()).unwrap();
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

    let replacement = DataflowMonitor::compile_untyped(new_spec.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut old = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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

    let replacement = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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

    let candidate = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut old = DataflowMonitor::compile_untyped(old_spec.parse().unwrap()).unwrap();
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

    let replacement = DataflowMonitor::compile_untyped(new_spec.parse().unwrap()).unwrap();
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
    let mut old = DataflowMonitor::compile_checked(old_spec.parse().unwrap()).unwrap();
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

    let replacement = DataflowMonitor::compile_checked(new_spec.parse().unwrap()).unwrap();
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
    let mut old = DataflowMonitor::compile_untyped(old_spec.parse().unwrap()).unwrap();
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

    let replacement = DataflowMonitor::compile_untyped(new_spec.parse().unwrap()).unwrap();
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
    let mut old = DataflowMonitor::compile_untyped(old_spec.parse().unwrap()).unwrap();
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

    let replacement = DataflowMonitor::compile_untyped(new_spec.parse().unwrap()).unwrap();
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
    let mut old = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
    let mut output = [Value::NoVal];
    old.evaluate(&[Value::Int(1), Value::NoVal], &mut output)
        .unwrap();
    let replacement = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let specification = "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();

    assert_eq!(
        monitor.retained_environment_values.as_ref().unwrap().len(),
        monitor.environment_values.len()
    );
}

#[test]
fn activated_defer_releases_its_computed_source_into_the_main_plan() {
    let specification = "in x: Int\nin choose: Bool\nin left: Str\nin right: Str\n\
            aux source: Str\nout result: Int\n\
            source = if choose then left else right\n\
            result = defer(source: Int)"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
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
    let specification = "in x: Int\nin choose: Bool\nin left: Str\nin right: Str\n\
            aux source: Str\nout deferred: Int\nout dynamic_result: Int\n\
            source = if choose then left else right\n\
            deferred = defer(source: Int)\n\
            dynamic_result = dynamic(source: Int)"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();

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
    let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(source_spec.parse().unwrap()).unwrap();
    let mut output = [Value::NoVal];
    for value in [10, 20, 30] {
        monitor.evaluate(&[Value::Int(value)], &mut output).unwrap();
    }
    let source_history_id = history_id_for(&monitor, "x");
    let source_history_slots = monitor.history_store[source_history_id].slots_ptr();

    let candidate = DataflowMonitor::compile_untyped(target_spec.parse().unwrap()).unwrap();
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
    let mut monitor = DataflowMonitor::compile_untyped(source_spec.parse().unwrap()).unwrap();
    let mut output = [Value::NoVal];
    for value in [10, 20] {
        monitor.evaluate(&[Value::Int(value)], &mut output).unwrap();
    }

    let candidate = DataflowMonitor::compile_untyped(target_spec.parse().unwrap()).unwrap();
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
    let mut source = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
    let mut output = [Value::NoVal];
    source.evaluate(&[Value::Int(10)], &mut output).unwrap();
    source.evaluate(&[Value::Int(20)], &mut output).unwrap();

    let candidate = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
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
    let specification = "in x: Int\nin choose: Bool\nout result: Int\n\
result = if choose then x + 1 else x + 2"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
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
    let specification = "in x: Int\nin left: Str\nin right: Str\n\
            aux selector: Int\naux source: Str\nout fixed: Int\nout result: Int\n\
            selector = x + 1\n\
            source = if selector > 0 then left else right\n\
            fixed = x * 2\n\
            result = defer(source: Int)"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
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
    let specification = "in x: Int\n\
            aux a: Int\n\
            aux b: Int\n\
            out c: Int\n\
            a = x + 1\n\
            b = a * 2\n\
            c = b - 3"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
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
    let specification = "in x: Int\n\
            aux equal: Bool\n\
            out negated: Bool\n\
            equal = x == 1\n\
            negated = !equal"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
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
    let specification = "in x: Int\n\
            out a: Int\n\
            aux b: Int\n\
            out c: Int\n\
            a = x + 1\n\
            b = a * 2\n\
            c = a + b"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();

    let mut output = [Value::NoVal, Value::NoVal];
    monitor.evaluate(&[Value::Int(4)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(5), Value::Int(15)]);
}

#[test]
fn nested_graph_scope_preserves_values() {
    let specification = "in x: Int\n\
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
            e = d + 1"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();

    let mut output = [Value::NoVal];
    monitor
        .evaluate(&[Value::Int(4), Value::Bool(true)], &mut output)
        .unwrap();
    assert_eq!(output, [Value::Int(8)]);
}

#[test]
fn delay_captures_internal_stream_after_the_completed_tick() {
    let specification = "in x: Int\n\
            aux current: Int\n\
            out delayed: Int\n\
            current = x + 1\n\
            delayed = default(current[1], 0) + 1"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
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
    let specification = "in x: Int\n\
            aux current: Int\n\
            aux delayed: Int\n\
            out result: Int\n\
            current = x + 1\n\
            delayed = default(current[1], 0) + 1\n\
            result = delayed * 2"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();

    let mut output = [Value::NoVal];
    for (input, expected) in [(10, 2), (20, 24), (30, 44)] {
        monitor.evaluate(&[Value::Int(input)], &mut output).unwrap();
        assert_eq!(output, [Value::Int(expected)]);
    }
}

#[test]
fn temporal_maple_cycle_preserves_outputs() {
    let specification = crate::dsrv_fixtures::spec_maple_sequence()
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
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
    let specification = "out counter: Int\n\
            aux incremented: Int\n\
            out result: Int\n\
            counter = default(counter[1], 0) + 1\n\
            incremented = counter + 1\n\
            result = incremented + 1"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
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
    let specification = "in x: Int\n\
            in a_source: Str\n\
            in b_source: Str\n\
            out a: Int\n\
            out b: Int\n\
            a = dynamic(a_source: Int)\n\
            b = dynamic(b_source: Int)"
        .parse::<DsrvSpecification>()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_untyped(specification).unwrap();

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
    let specification = "in x: Int\n\
            in a_source: Str\n\
            in b_source: Str\n\
            out a: Int\n\
            out b: Int\n\
            out equal: Bool\n\
            a = dynamic(a_source: Int)\n\
            b = dynamic(b_source: Int)\n\
            equal = x == 1"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
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
