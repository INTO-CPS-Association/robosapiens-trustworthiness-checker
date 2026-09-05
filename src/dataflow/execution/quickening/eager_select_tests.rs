use crate::CheckedDsrvSpecification;
use crate::core::Value;
use crate::dataflow::execution::evaluator::Evaluator;
use crate::dataflow::execution::evaluator_state::NodeState;
use crate::dataflow::execution::scalar_region::ScalarRegion;
use crate::dataflow::execution::scheduled_plan::{PlanId, ScheduledExecutionPlan};
use crate::dataflow::stream_id::StreamId;
use crate::dataflow::{DataflowMonitor, DataflowProgram};
#[cfg(feature = "jit")]
use crate::dataflow::{JitConfig, JitPlan};

use super::{QuickenedRegionPlan, QuickenedRegionState};

fn conditional() -> (
    QuickenedRegionPlan,
    QuickenedRegionState,
    Vec<Evaluator>,
    Vec<Value>,
) {
    let compiled = DataflowProgram::compile_checked(
        "in c: Bool\nin x: Int\nin y: Int\nout result: Int\n\
         result = if c then x + 1 else y + 2"
            .parse::<CheckedDsrvSpecification>()
            .expect("conditional should type check"),
    )
    .expect("conditional should compile");
    let programs = compiled.stream_programs().to_vec();
    let order = [StreamId::new(0)];
    let schedule = ScheduledExecutionPlan::new(
        PlanId(0),
        &programs,
        compiled.monitor_plan().stream_slots,
        &[],
        &order,
        compiled.monitor_plan().temporal_streams.as_slice(),
    );
    let semantic = ScalarRegion::new(&schedule).expect("conditional should be a scalar region");
    let plan = QuickenedRegionPlan::from_region(&schedule, &semantic)
        .expect("eager select should quicken");
    let state = QuickenedRegionState::new(&plan);
    let evaluators = programs.into_iter().map(Evaluator::new).collect();
    let environment = vec![Value::NoVal; schedule.environment_len];
    (plan, state, evaluators, environment)
}

#[test]
fn eager_select_observes_both_branches_but_ignores_unselected_deferred() {
    let (plan, mut state, _evaluators, mut environment) = conditional();
    environment[0] = Value::Bool(true);
    environment[1] = Value::Int(4);
    environment[2] = Value::Deferred;

    assert!(plan.execute(&mut state, &mut environment));
    assert_eq!(environment[3], Value::Int(5));

    environment[1] = Value::NoVal;
    environment[2] = Value::NoVal;
    assert!(plan.execute(&mut state, &mut environment));
    assert_eq!(environment[3], Value::Int(5));
}

#[test]
fn eager_select_gives_unselected_no_val_precedence() {
    let (plan, mut state, _evaluators, mut environment) = conditional();
    environment[0] = Value::Bool(true);
    environment[1] = Value::Int(4);
    environment[2] = Value::NoVal;

    assert!(plan.execute(&mut state, &mut environment));
    assert_eq!(environment[3], Value::NoVal);
}

#[test]
fn eager_select_reads_an_earlier_member_and_supports_nested_conditionals() {
    let compiled = DataflowProgram::compile_checked(
        "in c: Bool\nin x: Int\nin y: Int\naux a: Int\nout result: Int\n\
         a = x + 1\n\
         result = if c then (if c then a + 2 else y + 3) else y + 4"
            .parse::<CheckedDsrvSpecification>()
            .expect("dependent conditionals should type check"),
    )
    .expect("dependent conditionals should compile");
    let programs = compiled.stream_programs().to_vec();
    let order = [StreamId::new(0), StreamId::new(1)];
    let schedule = ScheduledExecutionPlan::new(
        PlanId(0),
        &programs,
        compiled.monitor_plan().stream_slots,
        &[],
        &order,
        compiled.monitor_plan().temporal_streams.as_slice(),
    );
    let semantic = ScalarRegion::new(&schedule).expect("both streams should form one region");
    assert_eq!(semantic.streams().len(), 2);
    let plan = QuickenedRegionPlan::from_region(&schedule, &semantic)
        .expect("dependent nested conditionals should quicken");
    let mut state = QuickenedRegionState::new(&plan);
    let mut environment = vec![Value::NoVal; schedule.environment_len];
    environment[0] = Value::Bool(true);
    environment[1] = Value::Int(4);
    environment[2] = Value::Int(20);
    let first_output = plan.outputs().next().unwrap().1;
    environment[first_output.index()] = Value::Str("stale wrong type".into());

    assert!(plan.execute(&mut state, &mut environment));
    let outputs = plan
        .outputs()
        .map(|(_, slot, _)| environment[slot.index()].clone())
        .collect::<Vec<_>>();
    assert_eq!(outputs, [Value::Int(5), Value::Int(7)]);
}

#[test]
fn eager_select_nested_state_round_trips_through_canonical_state() {
    let (plan, mut state, mut evaluators, mut environment) = conditional();
    environment[0] = Value::Bool(false);
    environment[1] = Value::Int(4);
    environment[2] = Value::Int(10);
    assert!(plan.execute(&mut state, &mut environment));
    assert_eq!(environment[3], Value::Int(12));

    plan.materialize(&state, &mut evaluators);
    let NodeState::LazyIf(lazy) = &evaluators[0].canonical.node_states[0] else {
        panic!("conditional should retain lazy-if state");
    };
    assert_eq!(lazy.last_condition, Some(Value::Bool(false)));
    assert_eq!(lazy.last_then_value, Some(Value::Int(5)));
    assert_eq!(lazy.last_else_value, Some(Value::Int(12)));
    assert_eq!(lazy.then_state.node_values[0], Value::Int(5));
    assert_eq!(lazy.else_state.node_values[0], Value::Int(12));

    assert!(plan.synchronize(&mut state, &mut evaluators));
    environment[0] = Value::NoVal;
    environment[1] = Value::NoVal;
    environment[2] = Value::NoVal;
    assert!(plan.execute(&mut state, &mut environment));
    assert_eq!(environment[3], Value::Int(12));
}

#[test]
fn eager_select_matches_canonical_in_a_mixed_temporal_schedule() {
    let specification = "in c: Bool\nin x: Int\nin y: Int\n\
        aux previous: Int\nout result: Int\n\
        previous = default(x[1], 0)\n\
        result = if c then previous + x else y + 2"
        .parse::<CheckedDsrvSpecification>()
        .expect("mixed schedule should type check");
    let mut quick = DataflowMonitor::compile_checked(specification.clone()).unwrap();
    let mut canonical = DataflowMonitor::compile_checked(specification).unwrap();
    canonical.set_quickening(false);
    let mut quick_output = [Value::NoVal];
    let mut canonical_output = [Value::NoVal];

    for row in [
        [Value::Bool(true), Value::Int(3), Value::Int(20)],
        [Value::Bool(false), Value::Int(4), Value::Int(10)],
        [Value::NoVal, Value::NoVal, Value::NoVal],
        [Value::Bool(true), Value::Deferred, Value::Int(8)],
    ] {
        quick.evaluate(&row, &mut quick_output).unwrap();
        canonical.evaluate(&row, &mut canonical_output).unwrap();
        assert_eq!(quick_output, canonical_output, "row {row:?}");
    }
}

#[cfg(feature = "jit")]
#[test]
fn eager_select_state_survives_native_quick_and_canonical_transitions() {
    let specification = "in c: Bool\nin x: Int\nin y: Int\nout result: Int\n\
        result = if c then x + 1 else y + 2"
        .parse::<CheckedDsrvSpecification>()
        .expect("conditional should type check");
    let mut tiered =
        DataflowMonitor::compile_checked_with_jit(specification.clone(), JitConfig::eager())
            .unwrap();
    let mut canonical = DataflowMonitor::compile_checked(specification).unwrap();
    canonical.set_quickening(false);
    assert_eq!(tiered.jit_report().unwrap().plan(), JitPlan::WholeSchedule);
    let mut tiered_output = [Value::NoVal];
    let mut canonical_output = [Value::NoVal];

    let rows = [
        [Value::Bool(true), Value::Int(3), Value::Int(20)],
        // With quickening disabled, this sparse input declines native execution and exercises the
        // canonical handoff while retaining the unselected branch's prior value.
        [Value::Bool(false), Value::NoVal, Value::Int(10)],
        [Value::NoVal, Value::NoVal, Value::NoVal],
    ];
    for (index, row) in rows.into_iter().enumerate() {
        if index == 1 {
            tiered.set_quickening(false);
        } else if index == 2 {
            tiered.set_quickening(true);
        }
        tiered.evaluate(&row, &mut tiered_output).unwrap();
        canonical.evaluate(&row, &mut canonical_output).unwrap();
        assert_eq!(tiered_output, canonical_output, "row {row:?}");
    }
}

#[cfg(feature = "jit")]
#[test]
fn eager_select_region_native_recovers_after_successive_sparse_rows() {
    let specification = "in c: Bool\nin x: Int\nin y: Int\nin text: Str\n\
        out result: Int\nout label: Str\n\
        result = if c then x + 1 else y + 2\nlabel = text"
        .parse::<CheckedDsrvSpecification>()
        .expect("mixed native regions should type check");
    let mut tiered =
        DataflowMonitor::compile_checked_with_jit(specification.clone(), JitConfig::eager())
            .unwrap();
    let mut canonical = DataflowMonitor::compile_checked(specification).unwrap();
    canonical.set_quickening(false);
    let report = tiered.jit_report().unwrap();
    assert_eq!(report.plan(), JitPlan::Regions);
    assert!(report.compiled_artifacts() > 0);
    let mut tiered_output = [Value::NoVal, Value::NoVal];
    let mut canonical_output = [Value::NoVal, Value::NoVal];

    for row in [
        [
            Value::Bool(true),
            Value::Int(1),
            Value::Int(10),
            Value::Str("a".into()),
        ],
        [
            Value::Bool(true),
            Value::NoVal,
            Value::Int(20),
            Value::Str("b".into()),
        ],
        [
            Value::NoVal,
            Value::Int(3),
            Value::NoVal,
            Value::Str("c".into()),
        ],
        [
            Value::Bool(false),
            Value::Int(4),
            Value::NoVal,
            Value::Str("d".into()),
        ],
        [
            Value::Bool(false),
            Value::Int(5),
            Value::Int(7),
            Value::Str("e".into()),
        ],
    ] {
        tiered.evaluate(&row, &mut tiered_output).unwrap();
        canonical.evaluate(&row, &mut canonical_output).unwrap();
        assert_eq!(tiered_output, canonical_output, "row {row:?}");
    }
}
