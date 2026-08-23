//! Backend-neutral executable plans produced from a scheduler order.
//!
//! This is the semantic contract shared by canonical execution, quickening, and native lowering.
//! It owns immutable program references and stable value/state identities, but no evaluator state
//! and no backend artifact. Backends may derive different physical instruction and storage layouts
//! as long as those layouts map back to the identities recorded here.

#![cfg_attr(not(feature = "jit"), allow(dead_code))]

use std::rc::Rc;

use crate::dataflow::environment::EnvironmentSlot;
use crate::dataflow::execution_plan::{StreamId, StreamSlots};
use crate::dataflow::ir::{BoundRef, NodeId, StreamOp, StreamProgram};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::dataflow) struct PlanId(pub(in crate::dataflow) u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::dataflow) struct PlanValueSlot(EnvironmentSlot);

impl PlanValueSlot {
    pub(in crate::dataflow) fn environment(self) -> EnvironmentSlot {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::dataflow) struct PlanStateSlot {
    pub(in crate::dataflow) stream: StreamId,
    pub(in crate::dataflow) node: NodeId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::dataflow) struct PlanEffects {
    pub(in crate::dataflow) may_fail: bool,
    pub(in crate::dataflow) reads_temporal_state: bool,
    pub(in crate::dataflow) writes_temporal_state: bool,
}

#[derive(Clone)]
pub(in crate::dataflow) struct ScheduledExecutionPlan {
    pub(in crate::dataflow) id: PlanId,
    pub(in crate::dataflow) stream_slots: StreamSlots,
    pub(in crate::dataflow) streams: Box<[PlannedStream]>,
    pub(in crate::dataflow) commit_streams: Box<[StreamId]>,
    pub(in crate::dataflow) environment_len: usize,
}

#[derive(Clone)]
pub(in crate::dataflow) struct PlannedStream {
    pub(in crate::dataflow) stream: StreamId,
    pub(in crate::dataflow) output: PlanValueSlot,
    pub(in crate::dataflow) program: Rc<StreamProgram>,
    pub(in crate::dataflow) temporal: TemporalPlan,
    pub(in crate::dataflow) effects: PlanEffects,
}

#[derive(Clone, Default)]
pub(in crate::dataflow) struct TemporalPlan {
    pub(in crate::dataflow) operations: Box<[TemporalOperation]>,
    pub(in crate::dataflow) commits: Box<[TemporalCommit]>,
}

#[derive(Clone)]
pub(in crate::dataflow) enum TemporalOperation {
    Delay {
        state: PlanStateSlot,
        input: BoundRef,
        offset: u64,
    },
    RecursiveDelay {
        state: PlanStateSlot,
        offset: u64,
    },
    Default {
        state: PlanStateSlot,
        input: BoundRef,
        fallback: BoundRef,
    },
}

#[derive(Clone)]
pub(in crate::dataflow) enum TemporalCommit {
    Delay {
        state: PlanStateSlot,
        input: BoundRef,
    },
    RecursiveDelay {
        state: PlanStateSlot,
    },
}

impl ScheduledExecutionPlan {
    pub(in crate::dataflow) fn new(
        id: PlanId,
        programs: &[Rc<StreamProgram>],
        stream_slots: StreamSlots,
        order: &[StreamId],
        commit_streams: &[StreamId],
    ) -> Self {
        let streams = order
            .iter()
            .copied()
            .map(|stream| {
                let program = Rc::clone(&programs[stream.index()]);
                let temporal = TemporalPlan::new(stream, &program);
                PlannedStream {
                    stream,
                    output: PlanValueSlot(stream_slots.slot(stream)),
                    effects: PlanEffects {
                        may_fail: !program.is_infallible(),
                        reads_temporal_state: !temporal.operations.is_empty(),
                        writes_temporal_state: !temporal.commits.is_empty()
                            || temporal.operations.iter().any(|operation| {
                                matches!(operation, TemporalOperation::Default { .. })
                            }),
                    },
                    program,
                    temporal,
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let environment_len = programs
            .first()
            .map_or(stream_slots.start().index(), |program| {
                program.environment_layout.len()
            });
        Self {
            id,
            stream_slots,
            streams,
            commit_streams: commit_streams.to_vec().into_boxed_slice(),
            environment_len,
        }
    }

    pub(in crate::dataflow) fn order(&self) -> impl Iterator<Item = StreamId> + '_ {
        self.streams.iter().map(|step| step.stream)
    }

    pub(in crate::dataflow) fn is_infallible(&self) -> bool {
        self.streams.iter().all(|stream| !stream.effects.may_fail)
    }

    pub(in crate::dataflow) fn has_temporal_state(&self) -> bool {
        !self.commit_streams.is_empty()
    }
}

impl TemporalPlan {
    fn new(stream: StreamId, program: &StreamProgram) -> Self {
        let mut operations = Vec::new();
        let mut commits = Vec::new();
        for (index, operation) in program.graph.nodes.iter().enumerate() {
            let state = PlanStateSlot {
                stream,
                node: NodeId::new(index),
            };
            match operation {
                StreamOp::Delay { input, offset } => {
                    operations.push(TemporalOperation::Delay {
                        state,
                        input: input.clone(),
                        offset: *offset,
                    });
                    if *offset > 0 {
                        commits.push(TemporalCommit::Delay {
                            state,
                            input: input.clone(),
                        });
                    }
                }
                StreamOp::RecursiveDelay { offset } => {
                    operations.push(TemporalOperation::RecursiveDelay {
                        state,
                        offset: offset.get(),
                    });
                    commits.push(TemporalCommit::RecursiveDelay { state });
                }
                StreamOp::Default { input, fallback } => {
                    operations.push(TemporalOperation::Default {
                        state,
                        input: input.clone(),
                        fallback: fallback.clone(),
                    });
                }
                _ => {}
            }
        }
        Self {
            operations: operations.into_boxed_slice(),
            commits: commits.into_boxed_slice(),
        }
    }

    pub(in crate::dataflow) fn nodes(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.operations.iter().map(TemporalOperation::node)
    }
}

impl TemporalOperation {
    pub(in crate::dataflow) fn state(&self) -> PlanStateSlot {
        match self {
            Self::Delay { state, .. }
            | Self::RecursiveDelay { state, .. }
            | Self::Default { state, .. } => *state,
        }
    }

    pub(in crate::dataflow) fn node(&self) -> NodeId {
        self.state().node
    }
}
