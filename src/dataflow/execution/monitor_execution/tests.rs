use super::MonitorExecution;
use super::plan::{ExecutableSegment, ExecutionPlan, ExecutionStep};
#[cfg(feature = "jit")]
use crate::dataflow::StreamStateTransferOutcome;
use crate::dataflow::execution::evaluator::Evaluator;
use crate::dataflow::execution::evaluator_state::{
    NodeState, reset_state_clone_count, state_clone_count,
};
#[cfg(feature = "jit")]
use crate::dataflow::execution::quickening::ScalarValue;
use crate::dataflow::ir::{NodeId, ReconfigurableExpressionKind, StreamOp};
use crate::dataflow::monitor::test_support::execution;
use crate::dataflow::monitor_plan::ReconfigurableExpressionId;
use crate::dataflow::stream_id::StreamId;
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

#[derive(Debug, PartialEq, Eq)]
enum SegmentSnapshot {
    Island(Vec<usize>),
    Canonical(Vec<usize>),
}

fn segments_snapshot(monitor: &DataflowMonitor, stream: usize) -> Vec<SegmentSnapshot> {
    execution(monitor)
        .engine
        .active_plan
        .main_steps
        .iter()
        .find_map(|step| match step {
            ExecutionStep::Graph(step) if step.stream.index() == stream => Some(
                step.segments
                    .iter()
                    .map(|segment| match segment {
                        ExecutableSegment::Island { nodes, .. } => {
                            SegmentSnapshot::Island(nodes.clone().collect())
                        }
                        ExecutableSegment::Canonical(nodes) => {
                            SegmentSnapshot::Canonical(nodes.clone().collect())
                        }
                    })
                    .collect(),
            ),
            _ => None,
        })
        .expect("the stream should be evaluated as a graph step")
}

fn node_count(monitor: &DataflowMonitor, stream: usize) -> usize {
    execution(monitor).evaluators.evaluators[stream]
        .program
        .graph
        .nodes
        .len()
}

fn layout_snapshot(monitor: &DataflowMonitor) -> Vec<LayoutSnapshot> {
    let plan = &execution(monitor).engine.active_plan;
    steps_snapshot(plan, &plan.main_steps)
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

fn steps_snapshot(plan: &ExecutionPlan, steps: &[ExecutionStep]) -> Vec<LayoutSnapshot> {
    steps
        .iter()
        .map(|step| match step {
            ExecutionStep::ScalarRegion(region) => LayoutSnapshot::ScalarRun(
                plan.regions[*region]
                    .outputs()
                    .map(|(stream, _, _)| stream.index())
                    .collect(),
            ),
            ExecutionStep::Graph(step) => LayoutSnapshot::Graph(step.stream.index()),
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

/// Pins the scalar program the scalar-IR page lists in full.
///
/// `docs/src/architecture/dataflow/scalar-ir.md` prints the island for
/// `total = default(total[1], 0) + merged` value by value. The listing claims a specific shape, so
/// the counts and the value origins are checked rather than left to drift.
#[test]
fn documented_scalar_program_has_the_listed_shape() {
    use crate::dataflow::execution::scalar_ir::ScalarValueDefinition;
    use crate::dataflow::execution::scalar_region::ScalarRegion;

    let specification = "in x: Int\nin y: Int\n\
        out scaled: Int\nout offset: Int\nout merged: Int\nout total: Int\n\
        scaled = x * 2\n\
        offset = y + 5\n\
        merged = scaled + offset\n\
        total = default(total[1], 0) + merged"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let execution = execution(&monitor);

    let island = execution
        .engine
        .active_plan
        .regions
        .iter()
        .find_map(|region| match region {
            ScalarRegion::Graph(graph) => graph.islands.first(),
            ScalarRegion::Streams(_) => None,
        })
        .expect("total lowers to one graph island");

    // Five values, three instructions, one output, as the listing prints them.
    assert_eq!(island.program.values.len(), 5);
    assert_eq!(island.program.instructions.len(), 3);
    assert_eq!(island.program.output.index(), 4);
    assert_eq!(&*island.exports, &[NodeId::new(2)]);

    // One constant, one external read, and three instruction results: no island boundary input,
    // because this island covers the whole graph.
    let origins = island
        .program
        .values
        .iter()
        .map(|value| match value.definition {
            ScalarValueDefinition::Constant(_) => "constant",
            ScalarValueDefinition::External(_) => "external",
            ScalarValueDefinition::CanonicalNode(_) => "canonical",
            ScalarValueDefinition::Instruction(_) => "instruction",
        })
        .collect::<Vec<_>>();
    assert_eq!(
        origins,
        [
            "instruction",
            "constant",
            "instruction",
            "external",
            "instruction"
        ],
    );
}

/// Pins the partition the fusion page documents for its larger companion specification.
///
/// `docs/src/architecture/dataflow/fusion.md` shows this specification partitioned into six steps
/// and says that `blended` splits into island, canonical run, island. Both claims are drawn in
/// figures, so they are checked here rather than left to drift.
#[test]
fn documented_fusion_example_partitions_into_six_steps() {
    let specification = "in x: Int\nin y: Int\nin flag: Bool\nin lbl: Str\n\
        out scaled: Int\nout offset: Int\nout merged: Int\nout blended: Int\n\
        out delta: Int\nout ratio: Int\nout total: Int\nout echoed: Str\n\
        out level: Int\nout alert: Bool\n\
        scaled = x * 2\n\
        offset = y + 5\n\
        merged = scaled + offset\n\
        blended = default(blended[1], 0) + (if flag then merged else scaled)\n\
        delta = blended - merged\n\
        ratio = delta * 3\n\
        total = default(total[1], 0) + ratio\n\
        echoed = lbl\n\
        level = total + ratio\n\
        alert = level > 20"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let execution = execution(&monitor);
    let plan = &execution.engine.active_plan;

    // Three stream regions, each a maximal contiguous run, separated by the three streams that
    // cannot join one: two temporal and one Str.
    assert_eq!(
        steps_snapshot(plan, &plan.main_steps),
        [
            LayoutSnapshot::ScalarRun(vec![0, 1, 2]),
            LayoutSnapshot::Graph(3),
            LayoutSnapshot::ScalarRun(vec![4, 5]),
            LayoutSnapshot::Graph(6),
            LayoutSnapshot::Graph(7),
            LayoutSnapshot::ScalarRun(vec![8, 9]),
        ],
    );

    // The figure shows blended as island, canonical run, island; total as a single island; and
    // echoed with no island at all.
    let segments = |stream: usize| {
        plan.main_steps
            .iter()
            .find_map(|step| match step {
                ExecutionStep::Graph(graph) if graph.stream.index() == stream => {
                    Some(graph.segments.len())
                }
                _ => None,
            })
            .expect("graph step")
    };
    assert_eq!(
        segments(3),
        3,
        "blended alternates island, canonical, island"
    );
    assert_eq!(
        segments(6),
        1,
        "total is one island covering its whole graph"
    );
    assert_eq!(segments(7), 0, "echoed has no scalar form, so no island");
}

/// Pins the partition the dataflow architecture guide documents for its running example.
///
/// The guide carries this specification from the execution model through to tier selection and
/// states that `scaled` and `alert` occupy two *separate* stream regions because the temporal
/// `total` sits between them in scheduler order. That claim spans several pages, so it is checked
/// here rather than left to drift.
#[test]
fn documented_running_example_partitions_into_two_stream_regions_around_a_graph_step() {
    let specification = "in x: Int\n\
        out alert: Bool\n\
        out total: Int\n\
        out scaled: Int\n\
        alert = total > 20\n\
        total = default(total[1], 0) + scaled\n\
        scaled = x * 2"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let execution = execution(&monitor);

    // Scheduler order is scaled -> total -> alert. Only `total` is temporal, so it is the graph
    // step, and it separates the two static scalar streams into regions of their own.
    assert_eq!(
        steps_snapshot(
            &execution.engine.active_plan,
            &execution.engine.active_plan.main_steps,
        ),
        [
            LayoutSnapshot::ScalarRun(vec![0]),
            LayoutSnapshot::Graph(1),
            LayoutSnapshot::ScalarRun(vec![2]),
        ],
        "a stream region is a maximal contiguous run, so the graph step splits scaled from alert",
    );
    assert_eq!(execution.engine.active_plan.regions.len(), 3);

    // Every member is scalar, so quickening owns the row rather than canonical evaluation.
    assert!(matches!(
        execution.authoritative_tier,
        super::AuthoritativeTier::Regions(_)
    ));
}

#[test]
fn disabling_quickening_keeps_regions_but_selects_canonical_execution() {
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
    assert_eq!(
        steps_snapshot(
            &execution.engine.active_plan,
            &execution.engine.active_plan.main_steps,
        ),
        [LayoutSnapshot::ScalarRun(vec![0, 1])]
    );
    assert!(matches!(
        execution.authoritative_tier,
        super::AuthoritativeTier::Canonical
    ));
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
fn a_temporal_stream_quickens_as_one_region() {
    let specification = "in x: Int\n\
        out result: Bool\n\
        result = x > 3 && default(x[1], 4) > 3 && default(x[2], 4) > 3"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();

    assert_eq!(layout_snapshot(&monitor), [LayoutSnapshot::Graph(0)]);
    let segments = segments_snapshot(&monitor, 0);
    // Delays and defaults belong to the quickened instruction set, so nothing splits this graph.
    assert_eq!(
        segments,
        [SegmentSnapshot::Island(
            (0..node_count(&monitor, 0)).collect()
        )]
    );
}

#[test]
fn scalar_islands_match_canonical_results_across_special_rows() {
    let source = "in x: Int\n\
        out result: Bool\n\
        result = x > 3 && default(x[1], 4) > 3 && default(x[2], 4) > 3";
    let mut quickened =
        DataflowMonitor::compile_checked(source.parse::<CheckedDsrvSpecification>().unwrap())
            .unwrap();
    let mut canonical =
        DataflowMonitor::compile_checked(source.parse::<CheckedDsrvSpecification>().unwrap())
            .unwrap();
    canonical.set_quickening(false);

    let mut quickened_output = [Value::NoVal];
    let mut canonical_output = [Value::NoVal];
    for value in [
        Value::Int(5),
        Value::Int(2),
        Value::NoVal,
        Value::Int(9),
        Value::NoVal,
        Value::Int(1),
    ] {
        quickened
            .evaluate(&[value.clone()], &mut quickened_output)
            .unwrap();
        canonical.evaluate(&[value], &mut canonical_output).unwrap();
        assert_eq!(quickened_output, canonical_output);
    }
}

#[test]
fn island_lifting_state_materializes_into_the_canonical_arena_on_transition() {
    let specification = "in x: Int\n\
        out result: Int\n\
        result = default(x[1], 0) + 1"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut output = [Value::NoVal];

    monitor.evaluate(&[Value::Int(4)], &mut output).unwrap();
    monitor.evaluate(&[Value::Int(7)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(5)]);

    // The addition is the graph output, so its value is published; its lifting state is not,
    // because the region owns it until a transition materializes it.
    let addition = node_count(&monitor, 0) - 1;
    assert_eq!(
        execution(&monitor).evaluators.evaluators[0]
            .canonical
            .node_values[addition],
        Value::Int(5)
    );
    assert!(matches!(
        execution(&monitor).evaluators.evaluators[0]
            .canonical
            .node_states[addition],
        NodeState::BinaryLift {
            last_left: None,
            last_right: None,
        }
    ));

    monitor.set_quickening(false);
    assert!(matches!(
        execution(&monitor).evaluators.evaluators[0]
            .canonical
            .node_states[addition],
        NodeState::BinaryLift {
            last_left: Some(Value::Int(4)),
            last_right: Some(Value::Int(1)),
        }
    ));
    assert!(
        segments_snapshot(&monitor, 0)
            .iter()
            .all(|segment| matches!(segment, SegmentSnapshot::Canonical(_)))
    );

    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(8)]);
    monitor.set_quickening(true);
    monitor.evaluate(&[Value::Int(0)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(2)]);
}

#[test]
fn an_incompatible_row_returns_one_island_to_canonical_evaluation() {
    let specification = "in x: Int\nout result: Int\nresult = default(x[1], 0) + 1"
        .parse::<DsrvSpecification>()
        .unwrap();
    let program = DataflowProgram::compile_untyped(specification).unwrap();
    // The untyped program has no scalar signatures, so its graph stays wholly canonical.
    let execution = standalone_execution(&program);
    assert!(execution.engine.active_plan.regions.is_empty());
}

#[test]
fn unchecked_graphs_have_no_scalar_regions() {
    let specification = "in x: Int\nout y: Int\ny = x + 1"
        .parse::<DsrvSpecification>()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_untyped(specification).unwrap();

    assert!(execution(&monitor).engine.active_plan.regions.is_empty());
    assert_eq!(layout_snapshot(&monitor), [LayoutSnapshot::Graph(0)]);

    let mut output = [Value::NoVal];
    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(2)]);
    monitor.evaluate(&[Value::NoVal], &mut output).unwrap();
    assert_eq!(output, [Value::Int(2)]);
    monitor.evaluate(&[Value::Float(2.5)], &mut output).unwrap();
    assert_eq!(output, [Value::Float(3.5)]);
}

#[test]
fn exact_transfer_moves_island_lift_state() {
    let source = "in x: Int\nout y: Int\ny = default(x[1], 0) + 1"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let target = "in x: Int\nout y: Int\nout z: Int\ny = default(x[1], 0) + 1\nz = x * 2"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_checked(source).unwrap();
    let mut output = [Value::NoVal];

    monitor.evaluate(&[Value::Int(4)], &mut output).unwrap();
    monitor.evaluate(&[Value::Int(7)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(5)]);

    let target = DataflowProgram::compile_checked(target).unwrap();
    monitor
        .reconfigure(target, ContextTransferPolicy::MatchingStreamState)
        .unwrap();

    let mut output = [Value::NoVal, Value::NoVal];
    monitor.evaluate(&[Value::NoVal], &mut output).unwrap();
    assert_eq!(output, [Value::Int(8), Value::NoVal]);
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
    assert_eq!(
        execution(&monitor).evaluators.evaluators[1]
            .canonical
            .node_values[0],
        Value::NoVal
    );
    assert_eq!(
        execution(&monitor).evaluators.evaluators[2]
            .canonical
            .node_values[0],
        Value::NoVal
    );

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

    monitor.set_quickening(false);
    assert_eq!(
        execution(&monitor).evaluators.evaluators[0]
            .canonical
            .node_values[1],
        Value::Int(11)
    );
    assert_eq!(
        execution(&monitor).evaluators.evaluators[1]
            .canonical
            .node_values[0],
        Value::Int(4)
    );
    assert_eq!(
        execution(&monitor).evaluators.evaluators[2]
            .canonical
            .node_values[0],
        Value::Int(8)
    );
}

#[test]
fn fused_scalar_runs_materialize_authoritative_arena_state_on_transition() {
    let specification = "in x: Int\nout result: Int\nresult = x + 1"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut output = [Value::NoVal];

    monitor.evaluate(&[Value::Int(4)], &mut output).unwrap();

    assert_eq!(output, [Value::Int(5)]);
    assert_eq!(
        execution(&monitor).evaluators.evaluators[0]
            .canonical
            .node_values[0],
        Value::NoVal
    );

    monitor.set_quickening(false);

    assert_eq!(
        execution(&monitor).evaluators.evaluators[0]
            .canonical
            .node_values[0],
        Value::Int(5)
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
        steps_snapshot(
            &execution.engine.active_plan,
            &execution.engine.active_plan.source_steps,
        ),
        [LayoutSnapshot::ScalarRun(vec![0])]
    );
    assert_eq!(
        steps_snapshot(
            &execution.engine.active_plan,
            &execution.engine.active_plan.main_steps,
        ),
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
fn partial_temporal_region_is_not_compiled() {
    let specification = "in x: Int\n\
        out result: Bool\n\
        result = x > 3 && default(x[1], 4) > 3 && default(x[2], 4) > 3"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut execution = execution_with_ranges(&monitor, &[StreamId::new(0)], &[]);
    execution.enable_jit(JitConfig::eager());

    let report = execution.jit_report().unwrap();
    assert_eq!(report.plan(), JitPlan::Unavailable);
    assert_eq!(report.compiled_artifacts(), 0);
    assert_eq!(report.unsupported_streams(), [0]);
}

#[cfg(feature = "jit")]
#[test]
fn source_barrier_uses_schedule_owned_native_regions() {
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
    assert_eq!(report.plan(), JitPlan::Regions);
    assert_eq!(report.compiled_artifacts(), 2);
    assert!(report.unsupported_streams().is_empty());
    let artifacts = execution.jit_artifact_count();

    execution.select_schedule_ranges(
        &[],
        &[StreamId::new(0), StreamId::new(1)],
        execution.stream_slots,
    );
    assert_eq!(artifacts, 2);
    assert_eq!(execution.jit_artifact_count(), 1);
    assert_eq!(
        execution.jit_report().unwrap().plan(),
        JitPlan::WholeSchedule
    );
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
    assert_eq!(execution.jit_report().unwrap().plan(), JitPlan::Regions);
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
        let StreamOp::Reconfigurable(spec) = &evaluator.program.graph.nodes[0] else {
            panic!("test stream must start with a dynamic expression");
        };
        let expression = evaluator
            .canonical
            .reconfigurable_expression_state(NodeId::new(0));
        let active_expression = expression
            .active_expression
            .as_ref()
            .expect("the source barrier must activate the body");
        if spec.kind == ReconfigurableExpressionKind::Deferred {
            assert_eq!(expression.last_defer_result, Some(Value::Int(10)));
        } else {
            assert_eq!(expression.last_defer_result, None);
        }
        active.push(active_expression);
    }
    assert_eq!(active.len(), 2);
    assert!(Rc::ptr_eq(&active[0].template, &active[1].template));
    assert!(!std::ptr::eq(
        active[0].evaluator.canonical.as_ref(),
        active[1].evaluator.canonical.as_ref(),
    ));
}

#[cfg(feature = "jit")]
#[test]
fn special_row_falls_back_and_dense_row_resumes_whole_native_execution() {
    let specification = "in x: Int\nout result: Int\nresult = x + 1"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor =
        DataflowMonitor::compile_checked_with_jit(specification, JitConfig::eager()).unwrap();
    let mut output = [Value::NoVal];

    monitor.evaluate(&[Value::Int(3)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(4)]);
    assert_eq!(
        execution(&monitor).evaluators.published_scalars.as_ref(),
        &[None]
    );

    // Special values are outside the concrete native contract and trigger a cold transition.
    monitor.evaluate(&[Value::NoVal], &mut output).unwrap();
    assert_eq!(output, [Value::Int(4)]);
    assert_eq!(
        execution(&monitor).evaluators.published_scalars.as_ref(),
        &[Some(ScalarValue::Int(4))]
    );

    monitor.evaluate(&[Value::Int(4)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(5)]);
    assert_eq!(
        execution(&monitor).evaluators.published_scalars.as_ref(),
        &[Some(ScalarValue::Int(4))]
    );
    assert!(matches!(
        execution(&monitor).authoritative_tier,
        super::AuthoritativeTier::WholeNative
    ));
}

#[cfg(feature = "jit")]
#[test]
fn consecutive_sparse_rows_keep_canonical_retention_until_native_resumes() {
    let specification = "in x: Int\nin y: Int\nout result: Int\nresult = x + y"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor =
        DataflowMonitor::compile_checked_with_jit(specification, JitConfig::eager()).unwrap();
    let mut output = [Value::NoVal];
    for (row, expected) in [
        ([Value::Int(1), Value::Int(10)], Value::Int(11)),
        ([Value::Int(2), Value::NoVal], Value::Int(12)),
        ([Value::NoVal, Value::Int(20)], Value::Int(22)),
        ([Value::Deferred, Value::NoVal], Value::Deferred),
        ([Value::NoVal, Value::NoVal], Value::Deferred),
        ([Value::Int(3), Value::Int(4)], Value::Int(7)),
    ] {
        monitor.evaluate(&row, &mut output).unwrap();
        assert_eq!(output, [expected]);
    }
    assert!(matches!(
        execution(&monitor).authoritative_tier,
        super::AuthoritativeTier::WholeNative
    ));
}

#[cfg(feature = "jit")]
#[test]
fn fused_temporal_state_materializes_before_context_transfer() {
    let old_specification = "in x: Int\nout result: Int\nresult = default(result[1], 0) + x"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor =
        DataflowMonitor::compile_checked_with_jit(old_specification, JitConfig::eager()).unwrap();
    let mut output = [Value::NoVal];

    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(3)]);
    assert_eq!(monitor.jit_report().unwrap().plan(), JitPlan::WholeSchedule);

    let new_specification =
        "in added: Int\nin x: Int\nout result: Int\nresult = default(result[1], 0) + x"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
    let candidate = DataflowProgram::compile_checked(new_specification).unwrap();
    let report = monitor
        .reconfigure(candidate, ContextTransferPolicy::MatchingStreamState)
        .unwrap();
    assert_eq!(
        report.context_transfer.streams.as_slice()[0].outcome,
        StreamStateTransferOutcome::Transferred
    );

    let input = input_row(
        &monitor,
        &[("added", Value::Int(100)), ("x", Value::Int(3))],
    );
    monitor.evaluate(&input, &mut output).unwrap();
    assert_eq!(output, [Value::Int(6)]);
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

#[cfg(feature = "jit")]
#[test]
fn late_eager_jit_activation_preserves_fused_lift_state() {
    let specification = "in x: Int\nout result: Int\nresult = x + 1"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut output = [Value::NoVal];

    monitor.evaluate(&[Value::Int(4)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(5)]);

    monitor.enable_jit(JitConfig::eager());
    monitor.evaluate(&[Value::NoVal], &mut output).unwrap();
    assert_eq!(output, [Value::Int(5)]);
}

#[cfg(feature = "jit")]
#[test]
fn hot_jit_activation_preserves_fused_lift_state() {
    let specification = "in x: Int\nout result: Int\nresult = x + 1"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();
    monitor.enable_jit(JitConfig::after_events(1));
    let mut output = [Value::NoVal];

    monitor.evaluate(&[Value::Int(4)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(5)]);
    monitor.evaluate(&[Value::NoVal], &mut output).unwrap();
    assert_eq!(output, [Value::Int(5)]);
}

#[cfg(feature = "jit")]
#[test]
fn schedule_change_after_native_tick_materializes_native_before_fused_state() {
    let specification = "in x: Int\n\
        aux first: Int\n\
        aux second: Int\n\
        out result: Int\n\
        first = (x + 1) * 2\n\
        second = x * 3\n\
        result = first + second"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut execution = execution_with_ranges(
        &monitor,
        &[],
        &[StreamId::new(0), StreamId::new(1), StreamId::new(2)],
    );
    execution.enable_jit(JitConfig::eager());
    let mut environment = vec![Value::NoVal; execution.engine.active_plan.semantic.environment_len];
    environment[0] = Value::Int(1);
    execution
        .evaluate_with_history(&mut environment, None, None)
        .unwrap();
    assert_eq!(
        environment[execution.stream_slots.slot(StreamId::new(2)).index()],
        Value::Int(7)
    );

    execution.select_schedule_ranges(
        &[],
        &[StreamId::new(1), StreamId::new(0), StreamId::new(2)],
        execution.stream_slots,
    );
    environment[0] = Value::NoVal;
    execution
        .evaluate_with_history(&mut environment, None, None)
        .unwrap();
    assert_eq!(
        environment[execution.stream_slots.slot(StreamId::new(2)).index()],
        Value::Int(7)
    );
}

#[cfg(feature = "jit")]
#[test]
fn scalar_native_context_transfer_preserves_canonical_lift_state() {
    let specification = "in x: Int\n\
        aux first: Int\n\
        out result: Int\n\
        first = (x + 1) * 2\n\
        result = first + 1"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let source_program = DataflowProgram::compile_checked(specification.clone()).unwrap();
    let target_program = DataflowProgram::compile_checked(specification).unwrap();
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

    let mut source_environment =
        vec![Value::NoVal; source.engine.active_plan.semantic.environment_len];
    source_environment[0] = Value::Int(3);
    source
        .evaluate_source_prelude(&mut source_environment, None)
        .unwrap();

    source
        .evaluate_main_and_commit(&mut source_environment, None)
        .unwrap();
    assert!(matches!(
        source.authoritative_tier,
        super::AuthoritativeTier::Regions(_)
    ));

    let result_slot = source.stream_slots.slot(StreamId::new(1)).index();
    assert_eq!(source_environment[result_slot], Value::Int(9));

    let mut target_environment =
        vec![Value::NoVal; target.engine.active_plan.semantic.environment_len];
    target.context_transfer_from(
        &mut source,
        &mapping,
        ContextTransferPolicy::MatchingStreamState,
    );
    target
        .evaluate_source_prelude(&mut target_environment, None)
        .unwrap();
    target
        .evaluate_main_and_commit(&mut target_environment, None)
        .unwrap();
    assert_eq!(target_environment[result_slot], Value::Int(9));
}

#[test]
fn fused_plan_transition_restores_cached_plan_identity() {
    let specification = "in left_input: Int\n\
        in right_input: Int\n\
        out left: Int\n\
        out right: Int\n\
        left = left_input + 1\n\
        right = right_input * 2"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let monitor = DataflowMonitor::compile_checked(specification).unwrap();
    let mut execution = execution_with_ranges(&monitor, &[], &[StreamId::new(0), StreamId::new(1)]);
    assert_eq!(execution.engine.active_plan.regions.len(), 1);
    let forward_identity = execution.engine.active_plan.identity;
    let left_slot = execution.stream_slots.slot(StreamId::new(0)).index();
    let right_slot = execution.stream_slots.slot(StreamId::new(1)).index();
    let mut environment = vec![Value::NoVal; execution.engine.active_plan.semantic.environment_len];
    environment[0] = Value::Int(1);
    environment[1] = Value::Int(2);

    execution
        .evaluate_with_history(&mut environment, None, None)
        .unwrap();
    assert_eq!(environment[left_slot], Value::Int(2));
    assert_eq!(environment[right_slot], Value::Int(4));

    execution.select_schedule_ranges(
        &[],
        &[StreamId::new(1), StreamId::new(0)],
        execution.stream_slots,
    );
    let reverse_identity = execution.engine.active_plan.identity;
    assert_ne!(reverse_identity, forward_identity);
    assert_ne!(reverse_identity.generation, forward_identity.generation);
    execution
        .evaluate_with_history(&mut environment, None, None)
        .unwrap();
    assert_eq!(environment[left_slot], Value::Int(2));
    assert_eq!(environment[right_slot], Value::Int(4));

    execution.select_schedule_ranges(
        &[],
        &[StreamId::new(0), StreamId::new(1)],
        execution.stream_slots,
    );
    assert_eq!(execution.engine.active_plan.identity, forward_identity);
    execution
        .evaluate_with_history(&mut environment, None, None)
        .unwrap();
    assert_eq!(environment[left_slot], Value::Int(2));
    assert_eq!(environment[right_slot], Value::Int(4));
}

#[cfg(feature = "jit")]
#[test]
fn eager_jit_retains_deferred_through_trailing_no_val() {
    let specification = "in x: Int\nout result: Int\nresult = x + 1"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor =
        DataflowMonitor::compile_checked_with_jit(specification, JitConfig::eager()).unwrap();
    let mut output = [Value::NoVal];

    for (input, expected) in [
        (Value::Int(1), Value::Int(2)),
        (Value::NoVal, Value::Int(2)),
        (Value::Deferred, Value::Deferred),
        (Value::NoVal, Value::Deferred),
    ] {
        monitor.evaluate(&[input], &mut output).unwrap();
        assert_eq!(output, [expected]);
    }
}

#[cfg(feature = "jit")]
#[test]
fn eager_jit_conditional_retention_matches_checked_canonical() {
    let specification = "in c: Bool\n\
        in x: Int\n\
        in y: Int\n\
        out result: Int\n\
        result = if c then x + 1 else y + 2"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut canonical = DataflowMonitor::compile_checked(specification.clone()).unwrap();
    let mut eager =
        DataflowMonitor::compile_checked_with_jit(specification, JitConfig::eager()).unwrap();
    let mut canonical_output = [Value::NoVal];
    let mut eager_output = [Value::NoVal];

    for (row, expected) in [
        (
            [Value::Bool(true), Value::Int(1), Value::Int(10)],
            Value::Int(2),
        ),
        (
            [Value::Bool(false), Value::Int(3), Value::Int(4)],
            Value::Int(6),
        ),
        ([Value::NoVal, Value::NoVal, Value::NoVal], Value::Int(6)),
        (
            [Value::Deferred, Value::Deferred, Value::NoVal],
            Value::Deferred,
        ),
        ([Value::NoVal, Value::NoVal, Value::NoVal], Value::Deferred),
    ] {
        canonical.evaluate(&row, &mut canonical_output).unwrap();
        eager.evaluate(&row, &mut eager_output).unwrap();
        assert_eq!(canonical_output, [expected]);
        assert_eq!(eager_output, canonical_output);
    }
}

#[cfg(feature = "jit")]
#[test]
fn eager_jit_materialization_preserves_deferred_after_no_val_replay() {
    let specification = "in x: Int\nout result: Int\nresult = x + 1"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor =
        DataflowMonitor::compile_checked_with_jit(specification, JitConfig::eager()).unwrap();
    let mut output = [Value::NoVal];

    monitor.evaluate(&[Value::Int(4)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(5)]);
    monitor.evaluate(&[Value::Deferred], &mut output).unwrap();
    assert_eq!(output, [Value::Deferred]);
    monitor.evaluate(&[Value::NoVal], &mut output).unwrap();
    assert_eq!(output, [Value::Deferred]);

    let target = DataflowProgram::compile_checked(
        "in added: Int\nin x: Int\nout result: Int\nresult = x + 1"
            .parse::<CheckedDsrvSpecification>()
            .unwrap(),
    )
    .unwrap();
    monitor
        .reconfigure(target, ContextTransferPolicy::MatchingStreamState)
        .unwrap();
    let input = input_row(&monitor, &[("added", Value::Int(0)), ("x", Value::NoVal)]);
    monitor.evaluate(&input, &mut output).unwrap();
    assert_eq!(output, [Value::Deferred]);
}

/// `out y = x` binds a stream to an external directly, so its graph has no nodes at all.
#[test]
fn a_pass_through_stream_still_forms_a_scalar_region() {
    let specification = "in x: Int\nout y: Int\ny = x"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();

    assert_eq!(
        layout_snapshot(&monitor),
        [LayoutSnapshot::ScalarRun(vec![0])]
    );

    let mut output = [Value::NoVal];
    monitor.evaluate(&[Value::Int(7)], &mut output).unwrap();
    assert_eq!(output, [Value::Int(7)]);
    monitor.evaluate(&[Value::NoVal], &mut output).unwrap();
    assert_eq!(output, [Value::NoVal]);
}
