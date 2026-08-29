//! Backend-neutral executable plans produced from a scheduler order.
//!
//! This is the semantic contract shared by canonical execution, quickening, and native lowering.
//! It owns immutable program references and stable value/state identities, but no evaluator state
//! and no backend artifact. Backends may derive different physical instruction and storage layouts
//! as long as those layouts map back to the identities recorded here.

#![cfg_attr(not(feature = "jit"), allow(dead_code))]

use std::ops::Deref;
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
    pub(in crate::dataflow) source_stream_count: usize,
    pub(in crate::dataflow) commit_streams: Box<[StreamId]>,
    pub(in crate::dataflow) environment_len: usize,
    pub(in crate::dataflow) metadata: Rc<PlanMetadata>,
}

pub(in crate::dataflow) struct PlanMetadata {
    pub(in crate::dataflow) streams: Box<[Rc<StreamMetadata>]>,
    pub(in crate::dataflow) environment_len: usize,
}

pub(in crate::dataflow) struct StreamMetadata {
    pub(in crate::dataflow) output: PlanValueSlot,
    pub(in crate::dataflow) program: Rc<StreamProgram>,
    pub(in crate::dataflow) temporal: TemporalPlan,
    pub(in crate::dataflow) effects: PlanEffects,
}

#[derive(Clone)]
pub(in crate::dataflow) struct PlannedStream {
    pub(in crate::dataflow) stream: StreamId,
    metadata: Rc<StreamMetadata>,
}

impl Deref for PlannedStream {
    type Target = StreamMetadata;

    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
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

impl PlanMetadata {
    fn new(programs: &[Rc<StreamProgram>], stream_slots: StreamSlots) -> Self {
        let streams = programs
            .iter()
            .enumerate()
            .map(|(index, program)| {
                Rc::new(StreamMetadata::new(
                    StreamId::new(index),
                    program,
                    stream_slots.slot(StreamId::new(index)),
                ))
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let environment_len = programs
            .first()
            .map_or(stream_slots.start().index(), |program| {
                program.environment_layout.len()
            });
        Self {
            streams,
            environment_len,
        }
    }
}

impl StreamMetadata {
    fn new(stream: StreamId, program: &Rc<StreamProgram>, output: EnvironmentSlot) -> Self {
        let temporal = TemporalPlan::new(stream, program);
        let effects = PlanEffects {
            may_fail: !program.is_infallible(),
            reads_temporal_state: !temporal.operations.is_empty(),
            writes_temporal_state: !temporal.commits.is_empty()
                || temporal
                    .operations
                    .iter()
                    .any(|operation| matches!(operation, TemporalOperation::Default { .. })),
        };
        Self {
            output: PlanValueSlot(output),
            program: Rc::clone(program),
            temporal,
            effects,
        }
    }
}

impl ScheduledExecutionPlan {
    pub(in crate::dataflow) fn new(
        id: PlanId,
        programs: &[Rc<StreamProgram>],
        stream_slots: StreamSlots,
        source_order: &[StreamId],
        main_order: &[StreamId],
        commit_streams: &[StreamId],
    ) -> Self {
        let metadata = Rc::new(PlanMetadata::new(programs, stream_slots));
        Self::from_metadata(
            id,
            metadata,
            stream_slots,
            source_order,
            main_order,
            commit_streams,
        )
    }

    pub(in crate::dataflow) fn from_metadata(
        id: PlanId,
        metadata: Rc<PlanMetadata>,
        stream_slots: StreamSlots,
        source_order: &[StreamId],
        main_order: &[StreamId],
        commit_streams: &[StreamId],
    ) -> Self {
        debug_assert_eq!(
            source_order.len() + main_order.len(),
            metadata.streams.len(),
            "a scheduled plan must contain every logical stream exactly once"
        );
        #[cfg(debug_assertions)]
        {
            let mut seen = vec![false; metadata.streams.len()];
            for stream in source_order.iter().chain(main_order) {
                debug_assert!(
                    stream.index() < metadata.streams.len()
                        && !std::mem::replace(&mut seen[stream.index()], true),
                    "a scheduled plan must contain every logical stream exactly once"
                );
            }
        }

        let streams = source_order
            .iter()
            .chain(main_order)
            .copied()
            .map(|stream| PlannedStream {
                stream,
                metadata: Rc::clone(&metadata.streams[stream.index()]),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            id,
            stream_slots,
            streams,
            source_stream_count: source_order.len(),
            commit_streams: commit_streams.to_vec().into_boxed_slice(),
            environment_len: metadata.environment_len,
            metadata,
        }
    }

    #[cfg(test)]
    pub(in crate::dataflow) fn order(&self) -> impl Iterator<Item = StreamId> + '_ {
        self.streams.iter().map(|step| step.stream)
    }

    pub(in crate::dataflow) fn source_streams(&self) -> &[PlannedStream] {
        &self.streams[..self.source_stream_count]
    }

    pub(in crate::dataflow) fn main_streams(&self) -> &[PlannedStream] {
        &self.streams[self.source_stream_count..]
    }

    pub(in crate::dataflow) fn source_order(&self) -> impl Iterator<Item = StreamId> + '_ {
        self.source_streams().iter().map(|step| step.stream)
    }

    pub(in crate::dataflow) fn main_order(&self) -> impl Iterator<Item = StreamId> + '_ {
        self.main_streams().iter().map(|step| step.stream)
    }

    pub(in crate::dataflow) fn has_source_barrier(&self) -> bool {
        self.source_stream_count != 0
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
