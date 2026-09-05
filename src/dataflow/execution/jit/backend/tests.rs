use super::ir::{LoweredNode, ScalarRef};
use super::lowering::Lowering;

use crate::core::{BinaryOperator, Value};
use crate::dataflow::execution::scalar_ir::ScalarProgram;
use crate::dataflow::ir::ScalarKind;
use crate::dataflow::monitor::test_support::{execution, jit_artifact_count};
use crate::dataflow::{DataflowMonitor, DataflowProgram, JitConfig, JitPlan};
use crate::{CheckedDsrvSpecification, DsrvSpecification};

fn compile_pair(source: &str) -> (DataflowMonitor, DataflowMonitor) {
    let checked = source
        .parse::<CheckedDsrvSpecification>()
        .expect("test specification should type check");
    let canonical = source
        .parse::<DsrvSpecification>()
        .expect("test specification should parse")
        .try_into()
        .expect("untyped monitor should compile");
    let jitted = DataflowMonitor::compile_checked_with_jit(checked, JitConfig::eager())
        .expect("checked monitor should compile");
    assert!(
        jit_artifact_count(&jitted) > 0,
        "test specification should contain at least one JIT-eligible graph"
    );
    (canonical, jitted)
}

fn assert_rows(source: &str, rows: &[Vec<Value>]) {
    let (mut canonical, mut jitted) = compile_pair(source);
    let mut canonical_output = vec![Value::NoVal; canonical.output_vars().len()];
    let mut jitted_output = canonical_output.clone();
    for row in rows {
        canonical.evaluate(row, &mut canonical_output).unwrap();
        jitted.evaluate(row, &mut jitted_output).unwrap();
        assert_eq!(jitted_output, canonical_output);
    }
}

fn assert_jit_row(source: &str, row: &[Value], expected: &[Value]) {
    let checked = source
        .parse::<CheckedDsrvSpecification>()
        .expect("test specification should type check");
    let mut monitor = DataflowMonitor::compile_checked_with_jit(checked, JitConfig::eager())
        .expect("checked monitor should compile");
    let mut output = vec![Value::NoVal; monitor.output_vars().len()];
    monitor.evaluate(row, &mut output).unwrap();
    assert_eq!(output, expected);
}

fn assert_complete_temporal_kernel(source: &str) {
    let checked = source.parse::<CheckedDsrvSpecification>().unwrap();
    let monitor = DataflowMonitor::compile_checked_with_jit(checked, JitConfig::eager())
        .expect("temporal kernel should compile");
    assert_eq!(
        monitor
            .jit_report()
            .unwrap()
            .complete_temporal_kernel_streams(),
        [0]
    );
}

#[test]
fn repeated_integer_adds_lower_to_one_reachable_add() {
    let expression = std::iter::repeat_n("1", 32).collect::<Vec<_>>().join(" + ");
    let source = format!("in x: Int\nout result: Int\nresult = x + {expression}");
    let checked = source.parse::<CheckedDsrvSpecification>().unwrap();
    let program = DataflowProgram::compile_checked(checked).unwrap();
    let graph = &program.stream_programs()[0].graph;
    let scalar = ScalarProgram::from_bound_graph(graph, ScalarKind::Int).unwrap();
    let lowered = Lowering::new()
        .lower_scalar_program(&scalar)
        .expect("the chain should lower natively");
    let ScalarRef::Node { index, .. } = lowered.graph.output else {
        panic!("the optimized result should remain input-dependent");
    };
    let LoweredNode::Binary {
        op: BinaryOperator::Add,
        lhs: ScalarRef::Input { .. },
        rhs,
        ..
    } = &lowered.graph.nodes[index as usize]
    else {
        panic!("the output should be one add of the input and folded constant");
    };
    assert_eq!(rhs.int_constant(), Some(32));
}

#[test]
fn complete_arithmetic_graph_runs_natively() {
    assert_rows(
        "in x: Int\nin y: Int\nout result: Int\nresult = (x + y) * 3 - x",
        &[
            vec![Value::Int(5), Value::Int(2)],
            vec![Value::Int(11), Value::Int(7)],
        ],
    );
}

#[test]
fn integer_division_and_remainder_are_native_eligible() {
    for operation in ["/", "%"] {
        for rhs in ["5", "y"] {
            let source =
                format!("in x: Int\nin y: Int\nout result: Int\nresult = x {operation} {rhs}");
            assert_rows(
                &source,
                &[
                    vec![Value::Int(17), Value::Int(5)],
                    vec![Value::Int(-17), Value::Int(5)],
                    vec![Value::Int(-19), Value::Int(-1)],
                ],
            );
        }
    }
}

#[test]
fn hotness_activates_before_the_tick_after_the_threshold() {
    let checked = "in x: Int\nout result: Int\nresult = x + 1"
        .parse::<CheckedDsrvSpecification>()
        .expect("test specification should type check");
    let mut monitor =
        DataflowMonitor::compile_checked_with_jit(checked, JitConfig::after_events(2))
            .expect("checked monitor should compile");
    let mut output = [Value::NoVal];

    assert_eq!(jit_artifact_count(&monitor), 0);
    assert_eq!(monitor.jit_report().unwrap().plan(), JitPlan::Pending);
    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    assert_eq!(jit_artifact_count(&monitor), 0);
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    assert_eq!(jit_artifact_count(&monitor), 0);
    monitor.evaluate(&[Value::Int(3)], &mut output).unwrap();
    assert!(jit_artifact_count(&monitor) > 0);
    let report = monitor.jit_report().unwrap();
    assert_eq!(report.plan(), JitPlan::WholeSchedule);
    assert_eq!(report.compiled_artifacts(), 1);
    assert!(report.unsupported_streams().is_empty());
    assert!(report.scheduled_temporal_streams().is_empty());
    assert_eq!(report.backend_error(), None);
    assert_eq!(output, [Value::Int(4)]);
}

#[test]
fn history_backed_delays_promote_into_the_fused_temporal_kernel() {
    let checked = "in x: Int\nout result: Int\nresult = default(x[2], 0) + x"
        .parse::<CheckedDsrvSpecification>()
        .expect("test specification should type check");
    let mut monitor =
        DataflowMonitor::compile_checked_with_jit(checked, JitConfig::after_events(2))
            .expect("checked monitor should compile");
    let mut output = [Value::NoVal];

    // The quick tier already promoted this delay to the typed representation, so reaching a ring of
    // two below also pins that native activation hydrates shared history independently of which
    // tier promoted first.
    assert_eq!(execution(&monitor).delay_ring_lengths(), [0]);
    monitor.evaluate(&[Value::Int(1)], &mut output).unwrap();
    monitor.evaluate(&[Value::Int(2)], &mut output).unwrap();
    monitor.evaluate(&[Value::Int(3)], &mut output).unwrap();

    assert_eq!(output, [Value::Int(4)]);
    assert_eq!(execution(&monitor).delay_ring_lengths(), [2]);
    assert_eq!(monitor.jit_report().unwrap().plan(), JitPlan::WholeSchedule);
}

#[test]
fn unsupported_streams_are_reported_and_interpreted() {
    let checked = "in x: Str\nout result: Str\nresult = x"
        .parse::<CheckedDsrvSpecification>()
        .expect("test specification should type check");
    let mut monitor = DataflowMonitor::compile_checked_with_jit(checked, JitConfig::eager())
        .expect("checked monitor should compile");
    let report = monitor.jit_report().unwrap();
    assert_eq!(report.plan(), JitPlan::Unavailable);
    assert_eq!(report.compiled_artifacts(), 0);
    assert_eq!(report.unsupported_streams(), [0]);
    assert!(report.scheduled_temporal_streams().is_empty());

    let mut output = [Value::NoVal];
    monitor
        .evaluate(&[Value::Str("hello".into())], &mut output)
        .unwrap();
    assert_eq!(output, [Value::Str("hello".into())]);
}

#[test]
fn pure_conditional_graph_runs_natively() {
    assert_rows(
        "in x: Int\nin y: Int\nout result: Int\nresult = if x > 0 then y * 3 else x + y",
        &[
            vec![Value::Int(1), Value::Int(4)],
            vec![Value::Int(-2), Value::Int(9)],
        ],
    );
}

#[test]
fn first_special_value_replays_lifting_state_before_fallback() {
    assert_rows(
        "in x: Int\nout result: Int\nresult = (x + 1) * 2",
        &[
            vec![Value::Int(3)],
            vec![Value::Int(8)],
            vec![Value::NoVal],
            vec![Value::Int(4)],
            vec![Value::NoVal],
        ],
    );
}

#[test]
fn native_arithmetic_overflow_wraps() {
    assert_jit_row(
        "in x: Int\nout result: Int\nresult = x + 1",
        &[Value::Int(i64::MAX)],
        &[Value::Int(i64::MIN)],
    );
}

#[test]
fn dependent_streams_publish_native_results_to_the_environment() {
    assert_rows(
        "in x: Int\nout first: Int\nout second: Int\nfirst = x + 1\nsecond = first * 2",
        &[vec![Value::Int(3)], vec![Value::Int(8)]],
    );
}

#[test]
fn floating_point_graph_runs_natively() {
    assert_rows(
        "in x: Float\nin y: Float\nout result: Bool\nresult = (x + y) * 1.5 > y",
        &[
            vec![Value::Float(2.0), Value::Float(4.0)],
            vec![Value::Float(-8.0), Value::Float(3.5)],
        ],
    );
}

#[test]
fn mixed_numeric_graph_converts_integers_natively() {
    assert_rows(
        "in x: Int\nin y: Float\nout result: Float\nresult = x + y * 2.0",
        &[
            vec![Value::Int(2), Value::Float(4.25)],
            vec![Value::Int(-8), Value::Float(3.5)],
        ],
    );
}

#[test]
fn floating_point_inputs_replay_before_special_value_fallback() {
    assert_rows(
        "in x: Float\nout result: Float\nresult = (x + 0.5) * 2.0",
        &[
            vec![Value::Float(3.25)],
            vec![Value::Float(-1.5)],
            vec![Value::NoVal],
            vec![Value::Float(8.0)],
        ],
    );
}

#[test]
fn fixed_delays_and_defaults_run_as_one_complete_temporal_kernel() {
    let source = "in x: Int\nout result: Bool\nresult = x > 3 && default(x[1], 4) > 3 && default(x[2], 4) > 3";
    assert_complete_temporal_kernel(source);
    assert_rows(
        source,
        &[
            vec![Value::Int(5)],
            vec![Value::Int(6)],
            vec![Value::Int(7)],
            vec![Value::Int(1)],
            vec![Value::Int(9)],
            vec![Value::NoVal],
            vec![Value::Int(8)],
        ],
    );
}

#[test]
fn recursive_delay_accumulator_runs_as_one_complete_temporal_kernel() {
    let source = "in x: Int\nout result: Int\nresult = default(result[1], 0) + x";
    assert_complete_temporal_kernel(source);
    assert_rows(
        source,
        &[
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(4)],
            vec![Value::NoVal],
            vec![Value::Int(5)],
        ],
    );
}

#[test]
fn scheduler_plan_fuses_scalar_streams_around_temporal_state() {
    let source = "in x: Int\n\
            aux base: Int\n\
            aux delayed: Int\n\
            out result: Int\n\
            base = x + 1\n\
            delayed = default(base[1], 0) + base\n\
            result = delayed * 2";
    let checked = source.parse::<CheckedDsrvSpecification>().unwrap();
    let monitor = DataflowMonitor::compile_checked_with_jit(checked, JitConfig::eager())
        .expect("the complete scheduled plan should compile");
    let report = monitor.jit_report().unwrap();
    assert_eq!(report.plan(), JitPlan::WholeSchedule);
    assert_eq!(report.compiled_artifacts(), 1);
    assert_eq!(report.complete_temporal_kernel_streams(), [1]);
    assert_rows(
        source,
        &[
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::NoVal],
            vec![Value::Int(5)],
        ],
    );
}

#[test]
fn hot_activation_preserves_scheduled_temporal_state() {
    for source in [
        "in x: Int\nout result: Bool\nresult = x > 3 && default(x[1], 4) > 3 && default(x[2], 4) > 3",
        "in x: Int\nout result: Int\nresult = default(result[1], 0) + x",
    ] {
        let checked = source
            .parse::<CheckedDsrvSpecification>()
            .expect("test specification should type check");
        let canonical = DataflowMonitor::compile_checked(checked.clone()).unwrap();
        let jitted =
            DataflowMonitor::compile_checked_with_jit(checked, JitConfig::after_events(3)).unwrap();
        let rows = [
            vec![Value::Int(5)],
            vec![Value::Int(6)],
            vec![Value::Int(7)],
            vec![Value::Int(8)],
            vec![Value::NoVal],
            vec![Value::Int(9)],
        ];
        let mut canonical = canonical;
        let mut jitted = jitted;
        let mut expected = [Value::NoVal];
        let mut actual = [Value::NoVal];
        for row in rows {
            canonical.evaluate(&row, &mut expected).unwrap();
            jitted.evaluate(&row, &mut actual).unwrap();
            assert_eq!(actual, expected);
        }
        assert!(
            jitted
                .jit_report()
                .expect("JIT should have activated")
                .scheduled_temporal_streams()
                .is_empty()
        );
        assert_eq!(
            jitted
                .jit_report()
                .unwrap()
                .complete_temporal_kernel_streams(),
            [0]
        );
    }
}
