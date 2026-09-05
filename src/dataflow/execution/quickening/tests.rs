//! Focused tests for the one physical region representation.
//!
//! Stream regions and graph islands share this plan, state, and executor, so these exercise the
//! shared behaviour once per scope rather than duplicating a second engine's coverage.

use std::rc::Rc;

use super::region::{QuickenedRegionPlan, QuickenedRegionState};
use super::scalar::ScalarValue;
use crate::VarName;
use crate::core::{BinaryOperator, Value};
use crate::dataflow::environment::{EnvironmentLayout, EnvironmentSlot};
use crate::dataflow::execution::evaluator::Evaluator;
use crate::dataflow::execution::evaluator_state::{EvaluatorState, NodeState};
use crate::dataflow::execution::quickening::CanonicalArena;
use crate::dataflow::execution::scalar_region::{ScalarRegion, segment_stream_graph};
use crate::dataflow::execution::scheduled_plan::{PlanId, ScheduledExecutionPlan};
use crate::dataflow::ir::{
    BoundEvaluationGraph, BoundOp, BoundRef, NodeId, ScalarKind, ScalarSignature, StreamProgram,
};
use crate::dataflow::stream_id::{StreamId, StreamSlots};

/// Wraps one evaluator's canonical arena the way a graph step does.
fn arena<'a>(node_values: &'a mut [Value], node_states: &'a mut [NodeState]) -> CanonicalArena<'a> {
    CanonicalArena {
        node_values,
        node_states,
        history: None,
    }
}

/// Runs every member of a graph region against one evaluator's canonical arena.
fn run(
    region: &QuickenedRegionPlan,
    state: &mut QuickenedRegionState,
    environment: &mut [Value],
    evaluators: &mut [Evaluator],
) -> bool {
    let EvaluatorState {
        node_values,
        node_states,
    } = evaluators[0].canonical.as_mut();
    region.execute_with_published(
        state,
        environment,
        &[],
        &mut arena(node_values, node_states),
    )
}

/// Runs one member of a graph region against one evaluator's canonical arena.
fn run_member(
    region: &QuickenedRegionPlan,
    member: usize,
    state: &mut QuickenedRegionState,
    environment: &mut [Value],
    evaluators: &mut [Evaluator],
) -> bool {
    let EvaluatorState {
        node_values,
        node_states,
    } = evaluators[0].canonical.as_mut();
    region.execute_member(
        member,
        state,
        environment,
        &[],
        &mut arena(node_values, node_states),
    )
}

fn layout() -> Rc<EnvironmentLayout> {
    Rc::new(EnvironmentLayout::from_variables([
        VarName::new("x"),
        VarName::new("first"),
        VarName::new("second"),
    ]))
}

fn program(graph: BoundEvaluationGraph, layout: &Rc<EnvironmentLayout>) -> Rc<StreamProgram> {
    Rc::new(StreamProgram::new(graph, Rc::clone(layout)))
}

fn binary_graph(op: BinaryOperator, left: BoundRef, right: BoundRef) -> BoundEvaluationGraph {
    BoundEvaluationGraph::new(
        vec![BoundOp::Binary {
            op,
            lhs: left,
            rhs: right,
        }],
        vec![Some(ScalarSignature::Binary {
            left: ScalarKind::Int,
            right: ScalarKind::Int,
            output: ScalarKind::Int,
        })],
        BoundRef::Node(NodeId::new(0)),
    )
}

fn add_graph(left: BoundRef, right: BoundRef) -> BoundEvaluationGraph {
    binary_graph(BinaryOperator::Add, left, right)
}

fn schedule(
    programs: &[Rc<StreamProgram>],
    source_order: &[StreamId],
    main_order: &[StreamId],
) -> ScheduledExecutionPlan {
    ScheduledExecutionPlan::new(
        PlanId(0),
        programs,
        StreamSlots::new(EnvironmentSlot::new(1), programs.len()),
        source_order,
        main_order,
        &[],
    )
}

/// A stream whose conditional stays canonical while the addition after it quickens.
///
/// Eager selection is outside the quickened instruction set, so node 0 remains a canonical boundary
/// the island must read from the arena.
fn conditional_boundary() -> BoundEvaluationGraph {
    BoundEvaluationGraph::new(
        vec![
            BoundOp::If {
                cond: BoundRef::Const(Value::Bool(true)),
                then_branch: add_graph(
                    BoundRef::External(EnvironmentSlot::new(0)),
                    BoundRef::Const(Value::Int(1)),
                ),
                else_branch: add_graph(
                    BoundRef::External(EnvironmentSlot::new(0)),
                    BoundRef::Const(Value::Int(2)),
                ),
            },
            BoundOp::Binary {
                op: BinaryOperator::Add,
                lhs: BoundRef::Node(NodeId::new(0)),
                rhs: BoundRef::Const(Value::Int(1)),
            },
        ],
        vec![
            None,
            Some(ScalarSignature::Binary {
                left: ScalarKind::Int,
                right: ScalarKind::Int,
                output: ScalarKind::Int,
            }),
        ],
        BoundRef::Node(NodeId::new(1)),
    )
}

/// The first node of a graph that no island covers.
fn canonical_node(program: &StreamProgram) -> usize {
    let (region, _) =
        segment_stream_graph(StreamId::new(0), program, super::region::supports_program)
            .expect("the graph should contain at least one island");
    (0..region.node_count)
        .find(|index| {
            !region
                .islands
                .iter()
                .any(|island| island.nodes.contains(index))
        })
        .expect("the graph should contain a canonical node")
}

fn graph_region(
    plan: &ScheduledExecutionPlan,
    stream: StreamId,
    program: &StreamProgram,
) -> QuickenedRegionPlan {
    let _ = plan;
    let (region, _) = segment_stream_graph(stream, program, super::region::supports_program)
        .expect("the graph should contain at least one island");
    QuickenedRegionPlan::from_region(plan, &ScalarRegion::Graph(region))
        .expect("the islands should be quickenable")
}

/// Compiles a real specification into stream programs plus a whole-schedule scheduled plan.
fn compiled(source: &str) -> (Vec<Rc<StreamProgram>>, ScheduledExecutionPlan) {
    let program = crate::dataflow::DataflowProgram::compile_checked(
        source
            .parse::<crate::CheckedDsrvSpecification>()
            .expect("the specification should type check"),
    )
    .expect("the specification should compile");
    let programs = program.stream_programs().to_vec();
    let order = (0..programs.len()).map(StreamId::new).collect::<Vec<_>>();
    let plan = ScheduledExecutionPlan::new(
        PlanId(0),
        &programs,
        program.monitor_plan().stream_slots,
        &[],
        &order,
        program.monitor_plan().temporal_streams.as_slice(),
    );
    (programs, plan)
}

/// `result = x > 3 && default(x[1], 4) > 3 && default(x[2], 4) > 3` interleaves three scalar
/// islands with two canonical delay/default pairs.
const WINDOW3: &str =
    "in x: Int\nout result: Bool\nresult = x > 3 && default(x[1], 4) > 3 && default(x[2], 4) > 3";

/// A conditional between two scalar runs: the only remaining reason for a graph to hold more than
/// one island now that temporal operations join them.
const TWO_ISLANDS: &str =
    "in x: Int\nin c: Bool\nout result: Int\nresult = x * 2 + (if c then x else 1) + 1";

#[test]
fn a_source_barrier_keeps_a_schedule_from_forming_one_region() {
    let layout = layout();
    let programs = vec![
        program(
            add_graph(
                BoundRef::External(EnvironmentSlot::new(0)),
                BoundRef::Const(Value::Int(1)),
            ),
            &layout,
        ),
        program(
            binary_graph(
                BinaryOperator::Multiply,
                BoundRef::External(EnvironmentSlot::new(1)),
                BoundRef::Const(Value::Int(2)),
            ),
            &layout,
        ),
    ];
    let order = [StreamId::new(0), StreamId::new(1)];

    assert!(QuickenedRegionPlan::new(&schedule(&programs, &[], &order)).is_some());
    assert!(
        QuickenedRegionPlan::new(&schedule(&programs, &order[..1], &order[1..])).is_none(),
        "regions must not cross a source barrier"
    );
}

/// Whole-stream conditionals share the scalar program between quickened and native lowering.
#[test]
fn an_eager_select_stream_forms_a_quickened_region() {
    let layout = layout();
    let conditional = program(
        BoundEvaluationGraph::new(
            vec![BoundOp::If {
                cond: BoundRef::Const(Value::Bool(true)),
                then_branch: add_graph(
                    BoundRef::External(EnvironmentSlot::new(0)),
                    BoundRef::Const(Value::Int(1)),
                ),
                else_branch: add_graph(
                    BoundRef::External(EnvironmentSlot::new(0)),
                    BoundRef::Const(Value::Int(2)),
                ),
            }],
            vec![None],
            BoundRef::Node(NodeId::new(0)),
        ),
        &layout,
    );
    let programs = vec![conditional];
    let scheduled = schedule(&programs, &[], &[StreamId::new(0)]);

    assert!(
        ScalarRegion::from_streams(&scheduled, &scheduled.streams).is_some(),
        "native lowering still consumes the eager-select program"
    );
    let plan = QuickenedRegionPlan::new(&scheduled).unwrap();
    let mut state = QuickenedRegionState::new(&plan);
    let mut environment = vec![Value::NoVal; scheduled.environment_len];
    environment[0] = Value::Int(4);
    assert!(plan.execute(&mut state, &mut environment));
    assert_eq!(
        environment[scheduled.streams[0].output.environment().index()],
        Value::Int(5)
    );
}

#[test]
fn stream_region_execution_defers_canonical_materialization_until_transition() {
    let layout = layout();
    let programs = vec![
        program(
            add_graph(
                BoundRef::External(EnvironmentSlot::new(0)),
                BoundRef::Const(Value::Int(1)),
            ),
            &layout,
        ),
        program(
            binary_graph(
                BinaryOperator::Multiply,
                BoundRef::External(EnvironmentSlot::new(1)),
                BoundRef::Const(Value::Int(2)),
            ),
            &layout,
        ),
    ];
    let order = [StreamId::new(0), StreamId::new(1)];
    let scheduled = schedule(&programs, &[], &order);
    let region =
        QuickenedRegionPlan::new(&scheduled).expect("the scalar schedule should be eligible");
    let mut state = QuickenedRegionState::new(&region);
    let mut evaluators = programs
        .iter()
        .cloned()
        .map(Evaluator::new)
        .collect::<Vec<_>>();
    let mut environment = vec![Value::Int(3), Value::NoVal, Value::NoVal];

    assert!(region.execute(&mut state, &mut environment));
    assert_eq!(environment, [Value::Int(3), Value::Int(4), Value::Int(8)]);
    assert_eq!(evaluators[0].canonical.node_values, [Value::NoVal]);
    assert_eq!(evaluators[1].canonical.node_values, [Value::NoVal]);

    region.materialize(&state, &mut evaluators);
    assert_eq!(evaluators[0].canonical.node_values, [Value::Int(4)]);
    assert_eq!(evaluators[1].canonical.node_values, [Value::Int(8)]);
    assert!(matches!(
        evaluators[0].canonical.node_states[0],
        NodeState::BinaryLift {
            last_left: Some(Value::Int(3)),
            last_right: Some(Value::Int(1)),
        }
    ));

    environment[0] = Value::Int(5);
    assert!(region.execute(&mut state, &mut environment));
    assert_eq!(environment, [Value::Int(5), Value::Int(6), Value::Int(12)]);
    // The region stays authoritative between transitions.
    assert_eq!(evaluators[0].canonical.node_values, [Value::Int(4)]);

    region.materialize(&state, &mut evaluators);
    assert_eq!(evaluators[0].canonical.node_values, [Value::Int(6)]);

    let state_before = state.clone();
    let environment_before = environment.clone();
    environment[0] = Value::Bool(true);
    assert!(!region.execute(&mut state, &mut environment));
    assert_eq!(state, state_before);
    assert_eq!(environment[1..], environment_before[1..]);
}

#[test]
fn no_val_and_deferred_lift_across_a_region_boundary() {
    let layout = layout();
    let programs = vec![program(
        add_graph(
            BoundRef::External(EnvironmentSlot::new(0)),
            BoundRef::Const(Value::Int(1)),
        ),
        &layout,
    )];
    let scheduled = schedule(&programs, &[], &[StreamId::new(0)]);
    let region =
        QuickenedRegionPlan::new(&scheduled).expect("the scalar schedule should be eligible");
    let mut state = QuickenedRegionState::new(&region);
    let mut environment = vec![Value::Deferred, Value::NoVal, Value::NoVal];

    assert!(region.execute(&mut state, &mut environment));
    assert_eq!(environment[1], Value::Deferred);

    // `NoVal` retains the last observed operand rather than clearing it.
    environment[0] = Value::NoVal;
    assert!(region.execute(&mut state, &mut environment));
    assert_eq!(environment[1], Value::Deferred);

    let mut evaluators = programs
        .iter()
        .cloned()
        .map(Evaluator::new)
        .collect::<Vec<_>>();
    region.materialize(&state, &mut evaluators);
    assert!(matches!(
        evaluators[0].canonical.node_states[0],
        NodeState::BinaryLift {
            last_left: Some(Value::Deferred),
            last_right: Some(Value::Int(1)),
        }
    ));

    state.reset();
    environment[0] = Value::NoVal;
    assert!(region.execute(&mut state, &mut environment));
    assert_eq!(environment[1], Value::NoVal);
}

#[test]
fn an_island_reads_and_publishes_canonical_node_values() {
    let layout = layout();
    let programs = vec![program(conditional_boundary(), &layout)];
    let scheduled = schedule(&programs, &[], &[StreamId::new(0)]);
    let stream = StreamId::new(0);
    let region = graph_region(&scheduled, stream, &programs[0]);
    let mut state = QuickenedRegionState::new(&region);
    let mut evaluators = programs
        .iter()
        .cloned()
        .map(Evaluator::new)
        .collect::<Vec<_>>();
    let mut environment = vec![Value::Int(9), Value::NoVal, Value::NoVal];

    // The canonical delay node runs outside the island and publishes into the node arena.
    evaluators[0].canonical.node_values[0] = Value::Int(4);
    assert!(run(&region, &mut state, &mut environment, &mut evaluators));
    assert_eq!(evaluators[0].canonical.node_values[1], Value::Int(5));
    // Island lifting state stays region-owned until a transition.
    assert!(matches!(
        evaluators[0].canonical.node_states[1],
        NodeState::BinaryLift {
            last_left: None,
            last_right: None,
        }
    ));

    region.materialize(&state, &mut evaluators);
    assert!(matches!(
        evaluators[0].canonical.node_states[1],
        NodeState::BinaryLift {
            last_left: Some(Value::Int(4)),
            last_right: Some(Value::Int(1)),
        }
    ));
}

#[test]
fn an_island_rejects_a_canonical_boundary_value_it_cannot_type() {
    let layout = layout();
    let programs = vec![program(conditional_boundary(), &layout)];
    let scheduled = schedule(&programs, &[], &[StreamId::new(0)]);
    let region = graph_region(&scheduled, StreamId::new(0), &programs[0]);
    let mut state = QuickenedRegionState::new(&region);
    let mut node_values = vec![Value::Str("boundary".into()), Value::NoVal];
    let mut environment = vec![Value::Int(9), Value::NoVal, Value::NoVal];

    let mut node_states = vec![
        NodeState::Default { last_input: None },
        NodeState::BinaryLift {
            last_left: None,
            last_right: None,
        },
    ];
    assert!(!region.execute_with_published(
        &mut state,
        &mut environment,
        &[],
        &mut arena(&mut node_values, &mut node_states),
    ));
    assert_eq!(node_values[1], Value::NoVal, "no export may be published");
}

#[test]
fn island_state_round_trips_through_the_canonical_arena() {
    let layout = layout();
    let programs = vec![program(conditional_boundary(), &layout)];
    let scheduled = schedule(&programs, &[], &[StreamId::new(0)]);
    let region = graph_region(&scheduled, StreamId::new(0), &programs[0]);
    let mut evaluators = programs
        .iter()
        .cloned()
        .map(Evaluator::new)
        .collect::<Vec<_>>();
    evaluators[0].canonical.node_values[1] = Value::Int(5);
    evaluators[0].canonical.node_states[1] = NodeState::BinaryLift {
        last_left: Some(Value::Int(4)),
        last_right: Some(Value::Int(1)),
    };

    let mut state = QuickenedRegionState::new(&region);
    assert!(region.synchronize(&mut state, &mut evaluators));

    // A `NoVal` boundary must retain the synchronized operand rather than restart from nothing.
    let mut environment = vec![Value::NoVal, Value::NoVal, Value::NoVal];
    evaluators[0].canonical.node_values[0] = Value::NoVal;
    assert!(run(&region, &mut state, &mut environment, &mut evaluators));
    assert_eq!(evaluators[0].canonical.node_values[1], Value::Int(5));
}

#[test]
fn published_stream_scalars_feed_a_later_region() {
    let layout = layout();
    let programs = vec![
        program(conditional_boundary(), &layout),
        program(
            add_graph(
                BoundRef::External(EnvironmentSlot::new(1)),
                BoundRef::Const(Value::Int(10)),
            ),
            &layout,
        ),
    ];
    let scheduled = schedule(&programs, &[], &[StreamId::new(0), StreamId::new(1)]);
    let region = QuickenedRegionPlan::from_region(
        &scheduled,
        &ScalarRegion::from_streams(&scheduled, &scheduled.streams[1..])
            .expect("the trailing stream is scalar"),
    )
    .expect("the trailing stream should quicken");
    let mut state = QuickenedRegionState::new(&region);
    let mut environment = vec![Value::NoVal, Value::NoVal, Value::NoVal];

    // Without a publication for its input stream, the region declines the row.
    assert!(!region.execute_with_published(
        &mut state,
        &mut environment,
        &[None, None],
        &mut CanonicalArena::empty()
    ));

    assert!(region.execute_with_published(
        &mut state,
        &mut environment,
        &[Some(ScalarValue::Int(5)), None],
        &mut CanonicalArena::empty(),
    ));
    assert_eq!(environment[1], Value::Int(5));
    assert_eq!(environment[2], Value::Int(15));
}

#[test]
fn a_temporal_stream_quickens_as_one_region() {
    let (programs, _) = compiled(WINDOW3);
    let (region, segments) = segment_stream_graph(
        StreamId::new(0),
        &programs[0],
        super::region::supports_program,
    )
    .expect("the stream should quicken");

    // Delays and defaults are part of the quickened instruction set, so nothing splits this graph.
    assert_eq!(region.islands.len(), 1);
    assert_eq!(segments.len(), 1);
    assert_eq!(region.islands[0].nodes, 0..region.node_count);
    // The graph output is published; the delay inputs are external, so nothing else needs to be.
    assert_eq!(
        region.islands[0].exports.as_ref(),
        [NodeId::new(region.node_count - 1)]
    );
}

#[test]
fn islands_of_one_graph_form_one_region_and_pass_values_in_registers() {
    let (programs, _) = compiled(TWO_ISLANDS);
    let (region, segments) = segment_stream_graph(
        StreamId::new(0),
        &programs[0],
        super::region::supports_program,
    )
    .expect("the scalar runs should form islands");

    assert_eq!(region.islands.len(), 2);
    assert_eq!(
        segments.len(),
        3,
        "the conditional should separate the two islands: {segments:?}"
    );
    // The first island feeds only the second, so its result travels by register and is not
    // published at all. Only the graph output is.
    assert!(region.islands[0].exports.is_empty());
    assert_eq!(
        region.islands[1].exports.as_ref(),
        [NodeId::new(region.node_count - 1)]
    );
}

#[test]
fn synchronizing_one_member_repairs_the_registers_later_members_read() {
    let (programs, plan) = compiled(TWO_ISLANDS);
    let region = graph_region(&plan, StreamId::new(0), &programs[0]);
    let mut state = QuickenedRegionState::new(&region);
    let mut evaluators = programs
        .iter()
        .cloned()
        .map(Evaluator::new)
        .collect::<Vec<_>>();
    let mut environment = vec![Value::NoVal; plan.environment_len];
    environment[0] = Value::Int(5);
    environment[1] = Value::Bool(true);

    // Stand in for the canonical conditional between the two islands.
    let conditional = canonical_node(&programs[0]);
    assert!(run_member(
        &region,
        0,
        &mut state,
        &mut environment,
        &mut evaluators
    ));
    evaluators[0].canonical.node_values[conditional] = Value::Int(5);
    assert!(run_member(
        &region,
        1,
        &mut state,
        &mut environment,
        &mut evaluators
    ));
    let output = programs[0].graph.nodes.len() - 1;
    assert_eq!(
        evaluators[0].canonical.node_values[output],
        Value::Int(16),
        "5 * 2 + 5 + 1"
    );

    // A member handed to canonical evaluation writes its node values; synchronizing it back must
    // repair the registers the following member reads.
    evaluators[0].canonical.node_values[0] = Value::Int(100);
    assert!(region.synchronize_member(0, &mut state, evaluators[0].state_mut()));
    assert!(run_member(
        &region,
        1,
        &mut state,
        &mut environment,
        &mut evaluators
    ));
    assert_eq!(
        evaluators[0].canonical.node_values[output],
        Value::Int(106),
        "the second island must read the repaired register, not a stale one"
    );
}

#[test]
fn a_declining_member_leaves_its_neighbours_untouched() {
    let (programs, plan) = compiled(TWO_ISLANDS);
    let region = graph_region(&plan, StreamId::new(0), &programs[0]);
    let mut state = QuickenedRegionState::new(&region);
    let mut evaluators = programs
        .iter()
        .cloned()
        .map(Evaluator::new)
        .collect::<Vec<_>>();
    let mut environment = vec![Value::NoVal; plan.environment_len];
    environment[0] = Value::Int(5);
    environment[1] = Value::Bool(true);

    assert!(run_member(
        &region,
        0,
        &mut state,
        &mut environment,
        &mut evaluators
    ));
    // The second island reads the conditional, which a canonical run left non-scalar.
    let conditional = canonical_node(&programs[0]);
    evaluators[0].canonical.node_values[conditional] = Value::Str("boundary".into());
    let before = evaluators[0].canonical.node_values.clone();
    assert!(!run_member(
        &region,
        1,
        &mut state,
        &mut environment,
        &mut evaluators
    ));
    assert_eq!(
        evaluators[0].canonical.node_values, before,
        "a declining member must publish nothing"
    );
}
