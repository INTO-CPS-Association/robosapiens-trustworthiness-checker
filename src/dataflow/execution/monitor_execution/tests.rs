use super::MonitorExecution;
use super::plan::{GraphStep, QuickStep};
use crate::dataflow::execution::evaluator::Evaluator;
use crate::dataflow::execution::evaluator_state::{reset_state_clone_count, state_clone_count};
use crate::dataflow::execution::quickening::ScalarValue;
use crate::dataflow::execution_plan::{ReconfigurableExpressionId, StreamId};
use crate::dataflow::ir::{NodeId, ReconfigurableExpressionKind, StreamOp};
use crate::dataflow::monitor::test_support::execution;
use crate::dataflow::{ContextTransferPolicy, DataflowMonitor, StreamMapping};
use crate::dataflow::{DataflowProgram, ReconfigurationMapping};
#[cfg(feature = "jit")]
use crate::dataflow::{JitConfig, JitPlan};
use crate::{CheckedDsrvSpecification, DsrvSpecification};
use crate::{Value, VarName};
use std::rc::Rc;

#[derive(Debug, PartialEq, Eq)]
enum LayoutSnapshot {
    ScalarRun(Vec<usize>),
    Graph(usize),
}

fn layout_snapshot(monitor: &DataflowMonitor) -> Vec<LayoutSnapshot> {
    execution(monitor)
        .engine
        .active_plan
        .quick
        .main_steps
        .iter()
        .map(|step| match step {
            QuickStep::ScalarRun(run) => {
                LayoutSnapshot::ScalarRun(run.iter().map(|step| step.stream.index()).collect())
            }
            QuickStep::Graph(step) => LayoutSnapshot::Graph(step.stream.index()),
        })
        .collect()
}

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

fn execution_with_ranges(
    monitor: &DataflowMonitor,
    source_order: &[StreamId],
    main_order: &[StreamId],
) -> MonitorExecution {
    let current = execution(monitor);
    MonitorExecution::new_with_source_prelude(
        current.evaluators.programs_rc(),
        current.stream_slots,
        source_order,
        main_order,
        &current.temporal_streams,
    )
}

fn steps_snapshot(steps: &[QuickStep]) -> Vec<LayoutSnapshot> {
    steps
        .iter()
        .map(|step| match step {
            QuickStep::ScalarRun(run) => {
                LayoutSnapshot::ScalarRun(run.iter().map(|step| step.stream.index()).collect())
            }
            QuickStep::Graph(step) => LayoutSnapshot::Graph(step.stream.index()),
        })
        .collect()
}

fn standalone_execution(program: &DataflowProgram) -> MonitorExecution {
    let programs = program.stream_programs().to_vec();
    let order = (0..programs.len()).map(StreamId::new).collect::<Vec<_>>();
    MonitorExecution::new_with_source_prelude(
        programs,
        program.monitor_plan().stream_slots,
        &[],
        &order,
        program.monitor_plan().temporal_streams.as_slice(),
    )
}

fn compile_program(source: &str) -> DataflowProgram {
    source
        .parse::<DsrvSpecification>()
        .unwrap()
        .try_into()
        .unwrap()
}

fn assert_all_node_values(evaluator: &Evaluator, expected: Value) {
    assert!(
        evaluator
            .tier_states
            .canonical
            .node_values
            .iter()
            .all(|value| *value == expected)
    );
}

#[test]
fn destructive_exact_transfer_moves_state_without_cloning_and_clears_scratch() {
    let source_program = compile_program("in x\nout z\nz = x + 1");
    let target_program = compile_program("in x\nout z\nz = x + 1");
    let mapping = ReconfigurationMapping::between(&source_program, &target_program);
    let mut source = standalone_execution(&source_program);
    let mut target = standalone_execution(&target_program);

    source.evaluators.evaluators[0]
        .state_mut()
        .node_values
        .fill(Value::Int(7));
    target.evaluators.evaluators[0]
        .state_mut()
        .node_values
        .fill(Value::Bool(true));
    assert!(target.validate_context_transfer(&source, &mapping));

    reset_state_clone_count();
    target.context_transfer_from(
        &mut source,
        &mapping,
        ContextTransferPolicy::MatchingStreamState,
    );
    assert_eq!(state_clone_count(), 0);
    assert_all_node_values(&target.evaluators.evaluators[0], Value::NoVal);
}

#[cfg(feature = "jit")]
#[test]
fn exact_transfer_keeps_native_artifacts_bound_to_their_compiled_programs() {
    let specification = "in x: Int\naux a: Int\nout y: Int\na = x + 1\ny = a * 2";
    let source_program = DataflowProgram::compile_checked(
        specification.parse::<CheckedDsrvSpecification>().unwrap(),
    )
    .unwrap();
    let target_program = DataflowProgram::compile_checked(
        specification.parse::<CheckedDsrvSpecification>().unwrap(),
    )
    .unwrap();
    let mapping = ReconfigurationMapping::between(&source_program, &target_program);
    let source_order = [StreamId::new(0)];
    let main_order = [StreamId::new(1)];
    let mut source = MonitorExecution::new_with_source_prelude(
        source_program.stream_programs().to_vec(),
        source_program.monitor_plan().stream_slots,
        &source_order,
        &main_order,
        &[],
    );
    let mut target = MonitorExecution::new_with_source_prelude(
        target_program.stream_programs().to_vec(),
        target_program.monitor_plan().stream_slots,
        &source_order,
        &main_order,
        &[],
    );
    source.enable_jit(JitConfig::eager());
    target.enable_jit(JitConfig::eager());

    let source_artifact = source.evaluators.evaluators[0]
        .native_artifact_identity()
        .expect("source stream should have a per-stream native artifact");
    let target_artifact = target.evaluators.evaluators[0]
        .native_artifact_identity()
        .expect("target stream should have a per-stream native artifact");
    assert_ne!(source_artifact, target_artifact);

    target.context_transfer_from(
        &mut source,
        &mapping,
        ContextTransferPolicy::MatchingStreamState,
    );

    assert_eq!(
        target.evaluators.evaluators[0].native_artifact_identity(),
        Some(target_artifact)
    );
    assert_eq!(
        source.evaluators.evaluators[0].native_artifact_identity(),
        Some(source_artifact)
    );
}

#[test]
fn destructive_changed_transfer_resets_without_cloning() {
    let source_program = compile_program("in x\nout z\nz = default(x[1], 0) + 1");
    let target_program = compile_program("in x\nout z\nz = default(x[1], 0) + 2");
    let mapping = ReconfigurationMapping::between(&source_program, &target_program);
    assert!(matches!(
        mapping.stream(StreamId::new(0)),
        Some(StreamMapping::Unmapped)
    ));
    let mut source = standalone_execution(&source_program);
    let mut target = standalone_execution(&target_program);

    source.evaluators.evaluators[0]
        .state_mut()
        .node_values
        .fill(Value::Int(7));
    target.evaluators.evaluators[0]
        .state_mut()
        .node_values
        .fill(Value::Bool(true));
    assert!(target.validate_context_transfer(&source, &mapping));

    reset_state_clone_count();
    target.context_transfer_from(
        &mut source,
        &mapping,
        ContextTransferPolicy::MatchingStreamState,
    );
    assert_eq!(state_clone_count(), 0);
    assert_all_node_values(&target.evaluators.evaluators[0], Value::NoVal);
}

#[test]
fn matching_validation_allows_changed_and_new_streams_to_initialize() {
    let source_program = compile_program("in x\nout z\nz = x + 1");
    let changed_target = compile_program("in x\nout z\nz = x + 2");
    let changed_mapping = ReconfigurationMapping::between(&source_program, &changed_target);
    let source = standalone_execution(&source_program);
    let changed_execution = standalone_execution(&changed_target);
    assert!(changed_execution.validate_context_transfer(&source, &changed_mapping));

    let added_target = compile_program("in x\nout z\nout added\nz = x + 1\nadded = x + 2");
    let added_mapping = ReconfigurationMapping::between(&source_program, &added_target);
    let added_execution = standalone_execution(&added_target);
    assert!(added_execution.validate_context_transfer(&source, &added_mapping));

    let dynamic_source = compile_program("in source: Str\nout z: Int\nz = dynamic(source: Int)");
    let dynamic_target =
        compile_program("in source: Str\nout z: Int\nz = dynamic(source: Int) + 1");
    let dynamic_mapping = ReconfigurationMapping::between(&dynamic_source, &dynamic_target);
    let dynamic_source_execution = standalone_execution(&dynamic_source);
    let dynamic_target_execution = standalone_execution(&dynamic_target);
    assert!(
        dynamic_target_execution
            .validate_context_transfer(&dynamic_source_execution, &dynamic_mapping,)
    );
}

#[test]
fn disabling_quickening_uses_only_canonical_graph_steps() {
    let specification = "in x: Int\n\
        aux a: Int\n\
        out b: Int\n\
        a = x + 1\n\
        b = a * 2"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();
    monitor.set_quickening(false);
    let execution = execution(&monitor);

    assert!(!execution.engine.quickening);
    for step in execution
        .engine
        .active_plan
        .quick
        .source_steps
        .iter()
        .chain(execution.engine.active_plan.quick.main_steps.iter())
    {
        assert!(matches!(
            step,
            QuickStep::Graph(GraphStep {
                schedule_plan: None,
                ..
            })
        ));
    }
}

#[test]
fn toggling_quickening_preserves_lift_state() {
    let specification = "in x: Int\nout y: Int\ny = x + 1"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut output = [Value::NoVal];

    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(2)]);

    monitor.set_quickening(false);
    monitor.evaluate(&[Value::NoVal], &mut output).unwrap();
    assert_eq!(output, [Value::Int(2)]);
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(3)]);

    monitor.set_quickening(true);
    monitor.evaluate(&[Value::NoVal], &mut output).unwrap();
    assert_eq!(output, [Value::Int(3)]);
    monitor.evaluate(&[Value::Int(3)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(4)]);
}

#[test]
fn unchecked_multi_node_graph_is_not_an_adaptive_candidate() {
    let specification = "in x: Int\nout y: Int\ny = (x + 1) * 2"
        .parse::<DsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_untyped(specification).unwrap();
    let [QuickStep::Graph(step)] = execution(&monitor)
        .engine
        .active_plan
        .quick
        .main_steps
        .as_ref()
    else {
        panic!("unchecked multi-node stream did not produce one graph step")
    };

    assert!(!step.adaptive_candidate);
}

#[test]
fn unchecked_scalar_graph_adapts_after_observing_concrete_values() {
    let specification = "in x: Int\nout y: Int\ny = x + 1"
        .parse::<DsrvSpecification>()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_untyped(specification).unwrap();
    let [QuickStep::Graph(step)] = execution(&monitor)
        .engine
        .active_plan
        .quick
        .main_steps
        .as_ref()
    else {
        panic!("unchecked scalar stream did not produce one graph step")
    };
    assert!(step.adaptive_candidate);

    let mut output = [Value::NoVal];
    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(2)]);
    assert_eq!(
        execution(&monitor).evaluators.evaluators[0].quickening_node_value(NodeId::new(0)),
        None,
    );

    monitor.evaluate(&[Value::NoVal], &mut output).unwrap();
    assert_eq!(output, [Value::Int(2)]);
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(3)]);
    assert_eq!(
        execution(&monitor).evaluators.evaluators[0].quickening_node_value(NodeId::new(0)),
        Some(ScalarValue::Int(3)),
    );

    monitor.evaluate(&[Value::Float(2.5)], &mut output).unwrap();
    assert_eq!(output, [Value::Float(3.5)]);
    monitor.evaluate(&[Value::NoVal], &mut output).unwrap();
    assert_eq!(output, [Value::Float(3.5)]);
}

#[test]
fn exact_transfer_moves_adaptive_lift_state() {
    let source = "in x: Int\nout y: Int\ny = x + 1"
        .parse::<DsrvSpecification>()
        .unwrap();
    let target = "in x: Int\nout y: Int\nout z: Int\ny = x + 1\nz = x * 2"
        .parse::<DsrvSpecification>()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_untyped(source).unwrap();
    let mut output = [Value::NoVal];

    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(3)]);

    let target = DataflowProgram::compile_untyped(target).unwrap();
    monitor
        .reconfigure(target, ContextTransferPolicy::MatchingStreamState)
        .unwrap();
    assert_eq!(
        execution(&monitor).evaluators.evaluators[0].quickening_node_value(NodeId::new(0)),
        Some(ScalarValue::Int(3)),
    );

    let mut output = [Value::NoVal, Value::NoVal];
    monitor.evaluate(&[Value::NoVal], &mut output).unwrap();
    assert_eq!(output, [Value::Int(3), Value::NoVal]);
}

#[test]
fn history_aware_execution_does_not_adapt_unchecked_graphs() {
    let specification = "in x: Int\nin delayed_input: Int\nout y: Int\nout delayed: Int\ny = x + 1\ndelayed = delayed_input[1]"
        .parse::<DsrvSpecification>()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_untyped(specification).unwrap();
    let mut output = [Value::NoVal, Value::NoVal];

    monitor
        .evaluate(&[Value::Int(1), Value::Int(10)], &mut output)
        .unwrap();
    monitor
        .evaluate(&[Value::Int(2), Value::Int(11)], &mut output)
        .unwrap();
    assert_eq!(
        execution(&monitor).evaluators.evaluators[0].quickening_node_value(NodeId::new(0)),
        None,
    );
}

#[test]
fn disabling_quickening_disables_unchecked_adaptation() {
    let specification = "in x: Int\nout y: Int\ny = x + 1"
        .parse::<DsrvSpecification>()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_untyped(specification).unwrap();
    monitor.set_quickening(false);
    let [QuickStep::Graph(step)] = execution(&monitor)
        .engine
        .active_plan
        .quick
        .main_steps
        .as_ref()
    else {
        panic!("disabled quickening did not produce one graph step")
    };
    assert!(!step.adaptive_candidate);

    let mut output = [Value::NoVal];
    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(3)]);
    assert_eq!(
        execution(&monitor).evaluators.evaluators[0].quickening_node_value(NodeId::new(0)),
        None,
    );
}

#[test]
fn scalar_streams_form_one_execution_run() {
    let specification = "in x: Int\n\
        aux a: Int\n\
        aux b: Int\n\
        out c: Int\n\
        a = x + 1\n\
        b = a * 2\n\
        c = b - 3"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();

    assert_eq!(
        layout_snapshot(&monitor),
        [LayoutSnapshot::ScalarRun(vec![0, 1, 2])]
    );
}

#[test]
fn graph_stream_splits_scalar_runs() {
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
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();

    assert_eq!(
        layout_snapshot(&monitor),
        [
            LayoutSnapshot::ScalarRun(vec![0, 1]),
            LayoutSnapshot::Graph(2),
            LayoutSnapshot::ScalarRun(vec![3, 4]),
        ]
    );
}

#[test]
fn temporal_stream_splits_scalar_runs() {
    let specification = "in x: Int\n\
        aux current: Int\n\
        aux delayed: Int\n\
        out result: Int\n\
        current = x + 1\n\
        delayed = default(current[1], 0) + 1\n\
        result = delayed * 2"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();

    assert_eq!(
        layout_snapshot(&monitor),
        [
            LayoutSnapshot::ScalarRun(vec![0]),
            LayoutSnapshot::Graph(1),
            LayoutSnapshot::ScalarRun(vec![2]),
        ]
    );
}

#[test]
fn shared_history_does_not_disable_separate_scalar_quickening() {
    let specification = "in delayed_input: Int\nin scalar_input: Int\nout delayed: Int\naux scalar_base: Int\nout scalar_result: Int\ndelayed = delayed_input[1] + 1\nscalar_base = scalar_input + 1\nscalar_result = scalar_base * 2"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut output = [Value::NoVal, Value::NoVal];

    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[
                    ("delayed_input", Value::Int(10)),
                    ("scalar_input", Value::Int(2)),
                ],
            ),
            &mut output,
        )
        .unwrap();
    assert_eq!(output, [Value::Deferred, Value::Int(6)]);
    {
        let execution = execution(&monitor);
        assert_eq!(
            execution.evaluators.evaluators[1].quickening_node_value(NodeId::new(0)),
            Some(ScalarValue::Int(3))
        );
        assert_eq!(
            execution.evaluators.evaluators[2].quickening_node_value(NodeId::new(0)),
            Some(ScalarValue::Int(6))
        );
    }

    monitor
        .evaluate(
            &input_row(
                &monitor,
                &[
                    ("delayed_input", Value::Int(20)),
                    ("scalar_input", Value::Int(3)),
                ],
            ),
            &mut output,
        )
        .unwrap();
    assert_eq!(output, [Value::Int(11), Value::Int(8)]);
    assert_eq!(
        execution(&monitor).evaluators.evaluators[0].quickening_node_value(NodeId::new(1)),
        Some(ScalarValue::Int(11))
    );
}

#[test]
fn no_history_scalar_runs_still_publish_quickened_values() {
    let specification = "in x: Int\nout result: Int\nresult = x + 1"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut output = [Value::NoVal];

    monitor.evaluate(&[Value::Int(4)], &mut output).unwrap();

    assert_eq!(output, [Value::Int(5)]);
    assert_eq!(
        execution(&monitor).evaluators.evaluators[0].quickening_node_value(NodeId::new(0)),
        Some(ScalarValue::Int(5))
    );
}

#[test]
fn semantic_plan_records_schedule_publication_effects_and_state_identity() {
    let specification = "in x: Int\n\
        aux base: Int\n\
        out result: Int\n\
        base = x + 1\n\
        result = default(base[1], 0) + base"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let plan = &execution(&monitor).engine.active_plan.semantic;

    assert_eq!(
        plan.order().map(StreamId::index).collect::<Vec<_>>(),
        [0, 1]
    );
    assert_eq!(plan.streams[0].output.environment().index(), 1);
    assert_eq!(plan.streams[1].output.environment().index(), 2);
    assert!(!plan.streams[0].effects.reads_temporal_state);
    assert!(plan.streams[1].effects.reads_temporal_state);
    assert!(plan.streams[1].effects.writes_temporal_state);
    assert_eq!(plan.commit_streams.as_ref(), [StreamId::new(1)]);
    let state = plan.streams[1].temporal.operations[0].state();
    assert_eq!(state.stream, StreamId::new(1));
    assert_eq!(state.node, NodeId::new(0));
}

#[test]
fn source_boundary_is_part_of_cached_plan_identity() {
    let specification = "in x: Int\n\
        aux source: Int\n\
        out result: Int\n\
        source = x + 1\n\
        result = source * 2"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut execution = execution_with_ranges(&monitor, &[StreamId::new(0)], &[StreamId::new(1)]);
    let source_plan = execution.engine.active_plan.semantic.id;

    execution.select_schedule_ranges(
        &[],
        &[StreamId::new(0), StreamId::new(1)],
        execution.stream_slots,
    );
    let main_plan = execution.engine.active_plan.semantic.id;
    assert_ne!(main_plan, source_plan);
    assert_eq!(execution.engine.active_plan.semantic.source_stream_count, 0);
    assert_eq!(
        execution
            .engine
            .active_plan
            .semantic
            .order()
            .map(StreamId::index)
            .collect::<Vec<_>>(),
        [0, 1]
    );

    execution.select_schedule_ranges(
        &[StreamId::new(0)],
        &[StreamId::new(1)],
        execution.stream_slots,
    );
    assert_eq!(execution.engine.active_plan.semantic.id, source_plan);
}

#[test]
fn unchanged_schedule_keeps_the_active_plan() {
    let specification = "in x: Int\n\
        aux a: Int\n\
        out b: Int\n\
        a = x + 1\n\
        b = a * 2"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut execution = execution_with_ranges(&monitor, &[StreamId::new(0)], &[StreamId::new(1)]);
    let active_plan = execution.engine.active_plan.semantic.as_ref() as *const _;
    let plan_id = execution.engine.active_plan.semantic.id;

    execution.select_schedule_ranges(
        &[StreamId::new(0)],
        &[StreamId::new(1)],
        execution.stream_slots,
    );

    assert_eq!(execution.engine.active_plan.semantic.id, plan_id);
    assert!(std::ptr::eq(
        active_plan,
        execution.engine.active_plan.semantic.as_ref()
    ));
}

#[test]
fn unseen_schedule_reuses_per_stream_metadata() {
    let specification = "in x: Int\n\
        aux base: Int\n\
        out result: Int\n\
        base = x + 1\n\
        result = default(base[1], 0) + base"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut execution = execution_with_ranges(&monitor, &[StreamId::new(0)], &[StreamId::new(1)]);
    let metadata = Rc::clone(&execution.engine.active_plan.semantic.metadata);

    execution.select_schedule_ranges(
        &[],
        &[StreamId::new(0), StreamId::new(1)],
        execution.stream_slots,
    );

    assert!(Rc::ptr_eq(
        &metadata,
        &execution.engine.active_plan.semantic.metadata
    ));
    assert_eq!(
        execution.engine.active_plan.semantic.streams[1]
            .temporal
            .operations[0]
            .state()
            .stream,
        StreamId::new(1)
    );
}

#[test]
fn source_and_main_have_separate_scalar_runs() {
    let specification = "in x: Int\n\
        aux source: Int\n\
        aux middle: Int\n\
        out result: Int\n\
        source = x + 1\n\
        middle = source * 2\n\
        result = middle - 3"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let execution = execution_with_ranges(
        &monitor,
        &[StreamId::new(0)],
        &[StreamId::new(1), StreamId::new(2)],
    );

    assert_eq!(
        steps_snapshot(&execution.engine.active_plan.quick.source_steps),
        [LayoutSnapshot::ScalarRun(vec![0])]
    );
    assert_eq!(
        steps_snapshot(&execution.engine.active_plan.quick.main_steps),
        [LayoutSnapshot::ScalarRun(vec![1, 2])]
    );
}

#[test]
fn source_scalar_publication_is_available_to_main_range() {
    let specification = "in x: Int\n\
        aux source: Int\n\
        out result: Int\n\
        source = x + 1\n\
        result = source * 2"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut execution = execution_with_ranges(&monitor, &[StreamId::new(0)], &[StreamId::new(1)]);
    let mut environment = vec![Value::NoVal; execution.engine.active_plan.semantic.environment_len];
    environment[0] = Value::Int(3);

    execution
        .evaluate_source_prelude(&mut environment, None)
        .unwrap();
    let source_slot = execution.stream_slots.slot(StreamId::new(0)).index();
    assert_eq!(environment[source_slot], Value::Int(4));
    environment[source_slot] = Value::NoVal;
    execution
        .evaluate_main_and_commit(&mut environment, None)
        .unwrap();
    assert_eq!(
        environment[execution.stream_slots.slot(StreamId::new(1)).index()],
        Value::Int(8)
    );
}

#[test]
fn temporal_source_moved_to_main_is_evaluated_once_per_tick() {
    let specification = "in x: Int\n\
        aux delayed: Int\n\
        out result: Int\n\
        delayed = default(x[1], 0)\n\
        result = delayed"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut execution = execution_with_ranges(&monitor, &[StreamId::new(0)], &[StreamId::new(1)]);
    #[cfg(feature = "jit")]
    execution.enable_jit(JitConfig::eager());
    let mut environment = vec![Value::NoVal; execution.engine.active_plan.semantic.environment_len];
    let output = execution.stream_slots.slot(StreamId::new(1)).index();

    environment[0] = Value::Int(10);
    execution
        .evaluate_source_prelude(&mut environment, None)
        .unwrap();
    execution
        .evaluate_main_and_commit(&mut environment, None)
        .unwrap();
    assert_eq!(environment[output], Value::Int(0));

    execution.select_schedule_ranges(
        &[],
        &[StreamId::new(0), StreamId::new(1)],
        execution.stream_slots,
    );
    for (input, expected) in [(20, 10), (30, 20)] {
        environment[0] = Value::Int(input);
        execution
            .evaluate_source_prelude(&mut environment, None)
            .unwrap();
        execution
            .evaluate_main_and_commit(&mut environment, None)
            .unwrap();
        assert_eq!(environment[output], Value::Int(expected));
    }
}

#[cfg(feature = "jit")]
#[test]
fn temporal_source_barrier_prohibits_fused_kernel() {
    let specification = "in x: Int\n\
        out result: Bool\n\
        result = x > 3 && default(x[1], 4) > 3 && default(x[2], 4) > 3"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut execution = execution_with_ranges(&monitor, &[StreamId::new(0)], &[]);
    execution.enable_jit(JitConfig::eager());

    let report = execution.jit_report().unwrap();
    assert_eq!(report.plan(), JitPlan::PerStream);
    assert_eq!(report.compiled_artifacts(), 1);
}

#[cfg(feature = "jit")]
#[test]
fn source_barrier_uses_and_retains_evaluator_local_native_tiers() {
    let specification = "in x: Int\n\
        aux source: Int\n\
        out result: Int\n\
        source = x + 1\n\
        result = source * 2"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut execution = execution_with_ranges(&monitor, &[StreamId::new(0)], &[StreamId::new(1)]);
    execution.enable_jit(JitConfig::eager());

    let report = execution.jit_report().unwrap();
    assert_eq!(report.plan(), JitPlan::PerStream);
    assert_eq!(report.compiled_artifacts(), 2);
    assert!(report.unsupported_streams().is_empty());
    let artifacts = execution.jit_artifact_count();

    execution.select_schedule_ranges(
        &[],
        &[StreamId::new(0), StreamId::new(1)],
        execution.stream_slots,
    );
    assert_eq!(execution.jit_artifact_count(), artifacts);
    assert_eq!(execution.jit_report().unwrap().plan(), JitPlan::PerStream);
}

#[cfg(feature = "jit")]
#[test]
fn hotness_advances_once_across_both_ranges() {
    let specification = "in x: Int\n\
        aux source: Int\n\
        out result: Int\n\
        source = x + 1\n\
        result = source * 2"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut execution = execution_with_ranges(&monitor, &[StreamId::new(0)], &[StreamId::new(1)]);
    execution.enable_jit(JitConfig::after_events(1));
    let mut environment = vec![Value::NoVal; execution.engine.active_plan.semantic.environment_len];

    environment[0] = Value::Int(3);
    execution
        .evaluate_source_prelude(&mut environment, None)
        .unwrap();
    execution
        .evaluate_main_and_commit(&mut environment, None)
        .unwrap();
    assert_eq!(execution.jit_report().unwrap().plan(), JitPlan::Pending);

    environment[0] = Value::Int(4);
    execution
        .evaluate_source_prelude(&mut environment, None)
        .unwrap();
    assert_eq!(execution.jit_report().unwrap().plan(), JitPlan::PerStream);
    execution
        .evaluate_main_and_commit(&mut environment, None)
        .unwrap();
}

#[cfg(feature = "jit")]
#[test]
fn jit_reports_unsupported_streams_from_both_ranges() {
    let specification = "in x: Str\n\
        aux source: Str\n\
        out result: Str\n\
        source = x\n\
        result = source"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut execution = execution_with_ranges(&monitor, &[StreamId::new(0)], &[StreamId::new(1)]);
    execution.enable_jit(JitConfig::eager());

    let report = execution.jit_report().unwrap();
    assert_eq!(report.plan(), JitPlan::Unavailable);
    assert_eq!(report.unsupported_streams(), [0, 1]);
}

#[test]
fn expression_location_validation_requires_the_planned_stream_and_node() {
    let specification = "in source: Str\n\
        out result: Int\n\
        result = dynamic(source: Int)"
        .parse::<DsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_untyped(specification).unwrap();
    let execution = execution(&monitor);

    assert!(execution.validate_expression_location(
        ReconfigurableExpressionId::new(0),
        StreamId::new(0),
        NodeId::new(0),
    ));
    assert!(!execution.validate_expression_location(
        ReconfigurableExpressionId::new(0),
        StreamId::new(1),
        NodeId::new(0),
    ));
    assert!(!execution.validate_expression_location(
        ReconfigurableExpressionId::new(1),
        StreamId::new(0),
        NodeId::new(0),
    ));
}

#[test]
fn dynamic_and_defer_owners_share_templates_but_not_state() {
    let specification = "in x: Int\n\
        in a_source: Str\n\
        in b_source: Str\n\
        out a: Int\n\
        out b: Int\n\
        a = dynamic(a_source: Int, {x})\n\
        b = defer(b_source: Int, {x})";
    let mut monitor =
        DataflowMonitor::compile_untyped(specification.parse::<DsrvSpecification>().unwrap())
            .unwrap();
    let mut output = [Value::NoVal, Value::NoVal];
    let input = input_row(
        &monitor,
        &[
            ("x", Value::Int(10)),
            ("a_source", Value::Str("x".into())),
            ("b_source", Value::Str("x".into())),
        ],
    );
    monitor.evaluate(&input, &mut output).unwrap();
    assert_eq!(output, [Value::Int(10), Value::Int(10)]);

    let execution = execution(&monitor);
    let mut active = Vec::new();
    for evaluator in &execution.evaluators.evaluators {
        let StreamOp::Dynamic(spec) = &evaluator.program.graph.nodes[0] else {
            panic!("test stream must start with a dynamic expression");
        };
        let dynamic = evaluator
            .tier_states
            .canonical
            .dynamic_expression_state(NodeId::new(0));
        let active_expression = dynamic
            .active_expression
            .as_ref()
            .expect("the source barrier must activate the body");
        if spec.kind == ReconfigurableExpressionKind::Deferred {
            assert_eq!(dynamic.last_defer_result, Some(Value::Int(10)));
        } else {
            assert_eq!(dynamic.last_defer_result, None);
        }
        active.push(active_expression);
    }
    assert_eq!(active.len(), 2);
    assert!(Rc::ptr_eq(&active[0].template, &active[1].template));
    assert!(!std::ptr::eq(
        active[0].evaluator.tier_states.canonical.as_ref(),
        active[1].evaluator.tier_states.canonical.as_ref(),
    ));
}

#[test]
fn dynamic_schedule_reuses_cached_plan_identity() {
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
    let forward_plan_id = execution(&monitor).engine.active_plan.semantic.id;

    let reverse = input_row(
        &monitor,
        &[
            ("x", Value::Int(10)),
            ("a_source", Value::Str("b + 1".into())),
            ("b_source", Value::Str("x".into())),
        ],
    );
    monitor.evaluate(&reverse, &mut output).unwrap();
    assert_eq!(
        layout_snapshot(&monitor),
        [LayoutSnapshot::Graph(1), LayoutSnapshot::Graph(0)]
    );
    assert_eq!(execution(&monitor).engine.cached_plans.len(), 1);
    assert_ne!(
        execution(&monitor).engine.active_plan.semantic.id,
        forward_plan_id
    );

    let forward = input_row(
        &monitor,
        &[
            ("x", Value::Int(20)),
            ("a_source", Value::Str("x".into())),
            ("b_source", Value::Str("a + 1".into())),
        ],
    );
    monitor.evaluate(&forward, &mut output).unwrap();
    assert_eq!(
        layout_snapshot(&monitor),
        [LayoutSnapshot::Graph(0), LayoutSnapshot::Graph(1)]
    );
    assert_eq!(execution(&monitor).engine.cached_plans.len(), 1);
    assert_eq!(
        execution(&monitor).engine.active_plan.semantic.id,
        forward_plan_id
    );

    monitor.evaluate(&forward, &mut output).unwrap();
    assert_eq!(execution(&monitor).engine.cached_plans.len(), 1);
}
