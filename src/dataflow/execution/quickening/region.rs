//! The physical quickened region: registers, typed instructions, and lifting state.
//!
//! One representation executes every scalar region the plan owns, as an ordered list of *members*
//! over one register file. A *stream* region's members are whole streams, each publishing its
//! result to an environment slot. A *graph* region's members are the scalar islands of one stream,
//! separated by canonical node runs and publishing their exported node values back into that
//! stream's canonical arena. Both share this instruction encoding, operand model, scalar
//! operations, and lifting-state model.
//!
//! Members share registers, so a value produced by one island reaches a later island without a
//! round trip through `Value`. Only a graph region's members can be executed one at a time, and
//! only they can decline a row individually: a member's canonical boundary values are not known
//! until the canonical run before it has finished, so they cannot join the region-level preflight.
//!
//! While a region is active its state is the semantic authority for the nodes it covers. Canonical
//! evaluator state is updated only by [`QuickenedRegionPlan::materialize`], and refreshed from the
//! canonical arena only by [`QuickenedRegionPlan::synchronize`]. A region contains no canonical
//! escape hatch: every instruction in it is executable by the scalar engine, and an input that does
//! not match its declared kind rejects the whole region for the tick.

use std::ops::Range;

use crate::core::{BinaryOperator, PartialMarker, UnaryOperator, Value, propagated_special};
use crate::dataflow::environment::EnvironmentSlot;
use crate::dataflow::execution::evaluator::Evaluator;
use crate::dataflow::execution::evaluator_state::{EvaluatorState, LazyIfState, NodeState};
use crate::dataflow::execution::scalar_ir::{
    ScalarProgram, ScalarSsaInstruction, ScalarTemporalOp, ScalarValueDefinition, ScalarValueId,
};
use crate::dataflow::execution::scalar_region::{GraphRegion, ScalarRegion, ScalarRegionStream};
use crate::dataflow::execution::scheduled_plan::ScheduledExecutionPlan;
use crate::dataflow::history::HistoryAccess;
use crate::dataflow::ir::{NodeId, ScalarKind};
use crate::dataflow::stream_id::{StreamId, StreamSlots};

use super::scalar::{
    ScalarValue, apply_binary, apply_unary, retain_last, supports_binary, supports_unary,
};

/// The canonical arena a graph region's members read, write, and retain temporal state in.
///
/// Stream regions pass an empty arena: their members neither name canonical nodes nor retain
/// temporal state.
pub(in crate::dataflow) struct CanonicalArena<'a> {
    pub(in crate::dataflow) node_values: &'a mut [Value],
    pub(in crate::dataflow) node_states: &'a mut [NodeState],
    pub(in crate::dataflow) history: Option<HistoryAccess<'a>>,
}

impl CanonicalArena<'_> {
    #[inline]
    pub(in crate::dataflow) fn empty() -> CanonicalArena<'static> {
        CanonicalArena {
            node_values: &mut [],
            node_states: &mut [],
            history: None,
        }
    }
}

/// Flattened instructions and register layout for one scalar region.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::dataflow) struct QuickenedRegionPlan {
    instructions: Box<[RegionInstruction]>,
    /// Canonical node identity per instruction, parallel to `instructions`. It is needed only by
    /// the cold state handoffs, so it stays out of the executed instruction record.
    instruction_nodes: Box<[NodeId]>,
    members: Box<[RegionMember]>,
    inputs: Box<[RegionInput]>,
    boundary_inputs: Box<[BoundaryInput]>,
    exports: Box<[RegionExport]>,
    register_count: usize,
    environment_len: usize,
    stream_count: usize,
}

/// Persistent register and lifting storage for a [`QuickenedRegionPlan`].
///
/// The instruction state arena has the same flattened order as the plan's instructions.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::dataflow) struct QuickenedRegionState {
    registers: Box<[ScalarValue]>,
    instruction_states: Box<[QuickenedInstructionState]>,
}

/// One region member: the instructions of a single [`ScalarProgram`] plus its publication.
#[derive(Clone, Debug, PartialEq)]
struct RegionMember {
    stream: StreamId,
    instructions: Range<usize>,
    boundary_inputs: Range<usize>,
    exports: Range<usize>,
    /// Stream members publish to the environment. Island members publish through `exports`.
    output: Option<MemberOutput>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct MemberOutput {
    value: RegionRef,
    register: usize,
    slot: EnvironmentSlot,
    kind: ScalarKind,
}

/// An external input read from the environment or from an earlier stream's publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RegionInput {
    source: RegionInputSource,
    slot: EnvironmentSlot,
    kind: ScalarKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RegionInputSource {
    Environment,
    Published(StreamId),
}

/// An island input produced by a canonical node of the same graph, loaded into a register.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BoundaryInput {
    node: NodeId,
    kind: ScalarKind,
    register: usize,
}

/// An island result published back into the canonical node arena.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RegionExport {
    node: NodeId,
    register: usize,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum RegionRef {
    Constant(ScalarValue),
    Environment {
        slot: EnvironmentSlot,
        kind: ScalarKind,
    },
    Published {
        stream: StreamId,
        kind: ScalarKind,
    },
    Register {
        register: usize,
        kind: ScalarKind,
    },
}

#[derive(Clone, Debug, PartialEq)]
struct RegionInstruction {
    target: usize,
    operation: RegionOperation,
}

/// A temporal operation dispatched by type but retained by the canonical arena.
#[derive(Clone, Copy, Debug, PartialEq)]
enum RegionTemporalOp {
    Delay {
        input: RegionRef,
        offset: u64,
        external: Option<EnvironmentSlot>,
    },
    RecursiveDelay,
    Default {
        input: RegionRef,
        fallback: RegionRef,
    },
}

#[derive(Clone, Debug, PartialEq)]
enum RegionOperation {
    Temporal {
        node: NodeId,
        op: RegionTemporalOp,
        output_kind: ScalarKind,
    },
    Unary {
        op: UnaryOperator,
        input: RegionRef,
        input_kind: ScalarKind,
        output_kind: ScalarKind,
    },
    Binary {
        op: BinaryOperator,
        left: RegionRef,
        right: RegionRef,
        left_kind: ScalarKind,
        right_kind: ScalarKind,
        output_kind: ScalarKind,
    },
    EagerSelect {
        condition: RegionRef,
        then_program: Box<QuickenedBranchPlan>,
        else_program: Box<QuickenedBranchPlan>,
        output_kind: ScalarKind,
    },
}

#[derive(Clone, Debug, PartialEq)]
struct QuickenedBranchPlan {
    instructions: Box<[RegionInstruction]>,
    instruction_nodes: Box<[NodeId]>,
    output: RegionRef,
    register_count: usize,
}

#[derive(Clone, Debug, PartialEq)]
struct QuickenedBranchState {
    registers: Box<[ScalarValue]>,
    instruction_states: Box<[QuickenedInstructionState]>,
}

#[derive(Clone, Debug, PartialEq)]
enum QuickenedInstructionState {
    /// A temporal instruction. Its retention belongs to the canonical arena, so the region keeps
    /// nothing for it and the state handoffs leave its `NodeState` alone.
    Canonical,
    Unary {
        last_input: Option<ScalarValue>,
    },
    Binary {
        last_left: Option<ScalarValue>,
        last_right: Option<ScalarValue>,
    },
    EagerSelect {
        then_state: Box<QuickenedBranchState>,
        else_state: Box<QuickenedBranchState>,
        last_condition: Option<ScalarValue>,
        last_then_value: Option<ScalarValue>,
        last_else_value: Option<ScalarValue>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OutputInfo {
    register: usize,
    kind: ScalarKind,
}

/// Lowering scratch shared by both region scopes.
struct RegionBuilder {
    stream_slots: StreamSlots,
    environment_len: usize,
    stream_outputs: Vec<Option<OutputInfo>>,
    instructions: Vec<RegionInstruction>,
    instruction_nodes: Vec<NodeId>,
    inputs: Vec<RegionInput>,
    boundary_inputs: Vec<BoundaryInput>,
    exports: Vec<RegionExport>,
    members: Vec<RegionMember>,
    /// Registers holding each canonical node produced so far by this region. Only graph regions
    /// use it, and it is what lets a later island read an earlier one without an export.
    node_registers: Vec<Option<(usize, ScalarKind)>>,
    member_boundary_start: usize,
    register_count: usize,
}

/// Whether the quickened engine can execute every instruction of `program`.
///
/// Region planning applies this before choosing islands, so a graph step never holds a member the
/// engine would refuse.
pub(in crate::dataflow) fn supports_program(program: &ScalarProgram) -> bool {
    program
        .instructions
        .iter()
        .all(|instruction| match instruction {
            ScalarSsaInstruction::Unary {
                op,
                input_kind,
                output_kind,
                ..
            } => supports_unary(*op, *input_kind, *output_kind),
            ScalarSsaInstruction::Binary {
                op,
                left_kind,
                right_kind,
                output_kind,
                ..
            } => supports_binary(*op, *left_kind, *right_kind, *output_kind),
            // Temporal state stays in the canonical arena; the engine only dispatches these by type.
            ScalarSsaInstruction::Temporal { .. } => true,
            ScalarSsaInstruction::EagerSelect {
                then_program,
                else_program,
                ..
            } => supports_program(then_program) && supports_program(else_program),
        })
}

impl QuickenedRegionPlan {
    /// Builds a quickened plan covering the complete scheduler-produced execution plan.
    #[cfg(test)]
    pub(in crate::dataflow) fn new(plan: &ScheduledExecutionPlan) -> Option<Self> {
        let region = ScalarRegion::new(plan)?;
        Self::from_region(plan, &region)
    }

    pub(in crate::dataflow) fn from_region(
        plan: &ScheduledExecutionPlan,
        region: &ScalarRegion,
    ) -> Option<Self> {
        let mut builder = RegionBuilder::new(plan);
        match region {
            ScalarRegion::Streams(streams) => builder.lower_streams(streams)?,
            ScalarRegion::Graph(graph) => builder.lower_graph_region(graph)?,
        }
        Some(builder.finish())
    }

    pub(in crate::dataflow) fn outputs(
        &self,
    ) -> impl Iterator<Item = (StreamId, EnvironmentSlot, ScalarKind)> + '_ {
        self.members.iter().filter_map(|member| {
            member
                .output
                .map(|output| (member.stream, output.slot, output.kind))
        })
    }

    /// Materializes the region registers and lifting state into canonical evaluators.
    ///
    /// This is the cold handoff used at tier, context, or schedule transition boundaries. While the
    /// region remains active its instruction-state arena is authoritative and the canonical node
    /// state it covers may intentionally be stale.
    #[cold]
    #[inline(never)]
    pub(in crate::dataflow) fn materialize(
        &self,
        state: &QuickenedRegionState,
        evaluators: &mut [Evaluator],
    ) {
        debug_assert_eq!(state.registers.len(), self.register_count);
        debug_assert_eq!(state.instruction_states.len(), self.instructions.len());
        debug_assert!(evaluators.len() >= self.stream_count);

        for member in 0..self.members.len() {
            let canonical = evaluators[self.members[member].stream.index()].state_mut();
            self.materialize_member(member, state, canonical);
        }
    }

    /// Synchronizes newly selected quickened state from canonical evaluator state.
    ///
    /// `false` means the region cannot be taken over, because a temporal node it covers holds
    /// state outside the scalar domain. Members synchronized before that point keep their promoted
    /// state, which canonical execution reads just as well.
    #[cold]
    #[inline(never)]
    #[must_use]
    pub(in crate::dataflow) fn synchronize(
        &self,
        state: &mut QuickenedRegionState,
        evaluators: &mut [Evaluator],
    ) -> bool {
        debug_assert_eq!(state.registers.len(), self.register_count);
        debug_assert_eq!(state.instruction_states.len(), self.instructions.len());
        debug_assert!(evaluators.len() >= self.stream_count);

        (0..self.members.len()).all(|member| {
            let canonical = evaluators[self.members[member].stream.index()].state_mut();
            self.synchronize_member(member, state, canonical)
        })
    }

    /// Materializes one member's registers and lifting state into its canonical evaluator.
    ///
    /// Temporal nodes return to the canonical `Value` representation here, since the evaluator that
    /// takes the member over reads and writes that shape.
    #[cold]
    #[inline(never)]
    pub(in crate::dataflow) fn materialize_member(
        &self,
        member: usize,
        state: &QuickenedRegionState,
        canonical: &mut EvaluatorState,
    ) {
        for index in self.members[member].instructions.clone() {
            let instruction = &self.instructions[index];
            let node = self.instruction_nodes[index].index();
            canonical.node_values[node] = state.registers[instruction.target].into_value();
            state.instruction_states[index]
                .materialize(&instruction.operation, &mut canonical.node_states[node]);
        }
    }

    /// Refreshes one member's registers and lifting state from its canonical evaluator.
    ///
    /// Temporal nodes are promoted to the typed scalar representation here, through the same
    /// conversion the native temporal tier uses, and that promotion is a precondition of running
    /// the member: the region executor reads only the typed shape. `false` reports a temporal node
    /// whose state falls outside the scalar domain, which declines the member rather than
    /// executing it. This is the only moment such a node can appear, because once quickened the
    /// region is the sole writer of that state and only ever writes scalars into it.
    #[cold]
    #[inline(never)]
    #[must_use]
    pub(in crate::dataflow) fn synchronize_member(
        &self,
        member: usize,
        state: &mut QuickenedRegionState,
        canonical: &mut EvaluatorState,
    ) -> bool {
        let mut synchronized = true;
        for index in self.members[member].instructions.clone() {
            let instruction = &self.instructions[index];
            let node = self.instruction_nodes[index].index();
            state.registers[instruction.target] =
                ScalarValue::from_untyped_value(&canonical.node_values[node])
                    .unwrap_or(ScalarValue::NoVal);
            synchronized &= state.instruction_states[index]
                .synchronize(&instruction.operation, &mut canonical.node_states[node]);
        }
        synchronized
    }

    /// Executes one region tick.
    ///
    /// Inputs are checked before any environment or node value changes. `false` means that the
    /// current row is incompatible with the region and the tick belongs to the canonical evaluator
    /// instead.
    #[cfg(test)]
    #[inline]
    pub(in crate::dataflow) fn execute(
        &self,
        state: &mut QuickenedRegionState,
        environment_values: &mut [Value],
    ) -> bool {
        self.execute_with_published(state, environment_values, &[], &mut CanonicalArena::empty())
    }

    #[cfg(test)]
    #[inline]
    pub(in crate::dataflow) fn execute_with_published(
        &self,
        state: &mut QuickenedRegionState,
        environment_values: &mut [Value],
        published_scalars: &[Option<ScalarValue>],
        arena: &mut CanonicalArena<'_>,
    ) -> bool {
        self.prepare_inputs(environment_values, published_scalars)
            && self.execute_prepared(state, environment_values, published_scalars, arena)
    }

    /// Validates the region's environment and publication inputs, then republishes stream inputs
    /// into the environment.
    ///
    /// This runs before the native and quickened executors alike, so both agree on when a row
    /// belongs to the region at all.
    pub(in crate::dataflow) fn prepare_inputs(
        &self,
        environment_values: &mut [Value],
        published_scalars: &[Option<ScalarValue>],
    ) -> bool {
        if !self.inputs_match(environment_values, published_scalars) {
            return false;
        }
        for input in &self.inputs {
            if let RegionInputSource::Published(stream) = input.source {
                environment_values[input.slot.index()] = published_scalars[stream.index()]
                    .unwrap_or_else(|| unreachable!("validated region publication is missing"))
                    .into_value();
            }
        }
        true
    }

    /// Runs every member in order after [`Self::prepare_inputs`].
    ///
    /// Only a graph region's members read canonical node values, and only they can decline; a
    /// stream region always completes.
    pub(in crate::dataflow) fn execute_prepared(
        &self,
        state: &mut QuickenedRegionState,
        environment_values: &mut [Value],
        published_scalars: &[Option<ScalarValue>],
        arena: &mut CanonicalArena<'_>,
    ) -> bool {
        (0..self.members.len()).all(|member| {
            self.execute_member(member, state, environment_values, published_scalars, arena)
        })
    }

    /// Runs one member after [`Self::prepare_inputs`].
    ///
    /// Boundary inputs are checked and loaded first, so a member that declines the row has changed
    /// nothing but its own input registers, which no handoff reads.
    #[inline]
    pub(in crate::dataflow) fn execute_member(
        &self,
        member: usize,
        state: &mut QuickenedRegionState,
        environment_values: &mut [Value],
        published_scalars: &[Option<ScalarValue>],
        arena: &mut CanonicalArena<'_>,
    ) -> bool {
        debug_assert_eq!(state.registers.len(), self.register_count);
        debug_assert_eq!(state.instruction_states.len(), self.instructions.len());
        debug_assert!(environment_values.len() >= self.environment_len);
        let member = &self.members[member];

        for input in &self.boundary_inputs[member.boundary_inputs.clone()] {
            let Some(value) = arena
                .node_values
                .get(input.node.index())
                .and_then(|value| ScalarValue::from_value(value, input.kind))
            else {
                return false;
            };
            state.registers[input.register] = value;
        }

        for index in member.instructions.clone() {
            let instruction = &self.instructions[index];
            let value = match &instruction.operation {
                RegionOperation::Temporal { node, op, .. } => execute_temporal(
                    *node,
                    op,
                    &state.registers,
                    environment_values,
                    published_scalars,
                    arena,
                ),
                operation => execute_instruction(
                    operation,
                    &state.registers,
                    environment_values,
                    published_scalars,
                    &mut state.instruction_states[index],
                ),
            };
            state.registers[instruction.target] = value;
        }

        if let Some(output) = member.output {
            let value = read_region_ref(
                output.value,
                &state.registers,
                environment_values,
                published_scalars,
            );
            state.registers[output.register] = value;
            debug_assert!(value.has_kind(output.kind));
            environment_values[output.slot.index()] = value.into_value();
        }

        for export in &self.exports[member.exports.clone()] {
            arena.node_values[export.node.index()] = state.registers[export.register].into_value();
        }
        true
    }

    fn inputs_match(
        &self,
        environment_values: &[Value],
        published_scalars: &[Option<ScalarValue>],
    ) -> bool {
        self.inputs.iter().all(|input| match input.source {
            RegionInputSource::Environment => environment_values
                .get(input.slot.index())
                .is_some_and(|value| ScalarValue::from_value(value, input.kind).is_some()),
            RegionInputSource::Published(stream) => published_scalars
                .get(stream.index())
                .copied()
                .flatten()
                .is_some_and(|value| value.has_kind(input.kind)),
        })
    }
}

impl QuickenedRegionState {
    pub(in crate::dataflow) fn new(plan: &QuickenedRegionPlan) -> Self {
        Self {
            registers: vec![ScalarValue::NoVal; plan.register_count].into_boxed_slice(),
            instruction_states: plan
                .instructions
                .iter()
                .map(|instruction| QuickenedInstructionState::new(&instruction.operation))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    #[cfg(test)]
    pub(in crate::dataflow) fn reset(&mut self) {
        self.registers.fill(ScalarValue::NoVal);
        for instruction_state in &mut self.instruction_states {
            instruction_state.reset();
        }
    }
}

impl QuickenedBranchState {
    fn new(plan: &QuickenedBranchPlan) -> Self {
        Self {
            registers: vec![ScalarValue::NoVal; plan.register_count].into_boxed_slice(),
            instruction_states: plan
                .instructions
                .iter()
                .map(|instruction| QuickenedInstructionState::new(&instruction.operation))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    #[cfg(test)]
    fn reset(&mut self) {
        self.registers.fill(ScalarValue::NoVal);
        for state in &mut self.instruction_states {
            state.reset();
        }
    }
}

impl QuickenedBranchPlan {
    fn materialize(&self, state: &QuickenedBranchState, canonical: &mut EvaluatorState) {
        for (index, instruction) in self.instructions.iter().enumerate() {
            let node = self.instruction_nodes[index].index();
            canonical.node_values[node] = state.registers[instruction.target].into_value();
            state.instruction_states[index]
                .materialize(&instruction.operation, &mut canonical.node_states[node]);
        }
    }

    fn synchronize(
        &self,
        state: &mut QuickenedBranchState,
        canonical: &mut EvaluatorState,
    ) -> bool {
        let mut synchronized = true;
        for (index, instruction) in self.instructions.iter().enumerate() {
            let node = self.instruction_nodes[index].index();
            state.registers[instruction.target] =
                ScalarValue::from_untyped_value(&canonical.node_values[node])
                    .unwrap_or(ScalarValue::NoVal);
            synchronized &= state.instruction_states[index]
                .synchronize(&instruction.operation, &mut canonical.node_states[node]);
        }
        synchronized
    }
}

impl RegionBuilder {
    fn new(plan: &ScheduledExecutionPlan) -> Self {
        Self {
            stream_slots: plan.stream_slots,
            environment_len: plan.environment_len,
            stream_outputs: vec![None; plan.stream_slots.len()],
            instructions: Vec::new(),
            instruction_nodes: Vec::new(),
            inputs: Vec::new(),
            boundary_inputs: Vec::new(),
            exports: Vec::new(),
            members: Vec::new(),
            node_registers: Vec::new(),
            member_boundary_start: 0,
            register_count: 0,
        }
    }

    fn finish(self) -> QuickenedRegionPlan {
        QuickenedRegionPlan {
            instructions: self.instructions.into_boxed_slice(),
            instruction_nodes: self.instruction_nodes.into_boxed_slice(),
            members: self.members.into_boxed_slice(),
            inputs: self.inputs.into_boxed_slice(),
            boundary_inputs: self.boundary_inputs.into_boxed_slice(),
            exports: self.exports.into_boxed_slice(),
            register_count: self.register_count,
            environment_len: self.environment_len,
            stream_count: self.stream_slots.len(),
        }
    }

    fn lower_streams(&mut self, streams: &[ScalarRegionStream]) -> Option<()> {
        for stream in streams {
            let mut value_refs = vec![None; stream.program.values.len()];
            let start = self.instructions.len();
            self.member_boundary_start = self.boundary_inputs.len();
            self.lower_program(&stream.program, &mut value_refs)?;
            let value =
                self.lower_value(&stream.program, stream.program.output, &mut value_refs)?;
            let register = match value {
                RegionRef::Register { register, .. } => register,
                _ => self.allocate_register()?,
            };
            self.members.push(RegionMember {
                stream: stream.stream,
                instructions: start..self.instructions.len(),
                boundary_inputs: self.member_boundary_start..self.boundary_inputs.len(),
                exports: self.exports.len()..self.exports.len(),
                output: Some(MemberOutput {
                    value,
                    register,
                    slot: stream.output,
                    kind: stream.output_kind,
                }),
            });
            self.stream_outputs[stream.stream.index()] = Some(OutputInfo {
                register,
                kind: stream.output_kind,
            });
        }
        Some(())
    }

    fn lower_graph_region(&mut self, region: &GraphRegion) -> Option<()> {
        self.node_registers = vec![None; region.node_count];
        for island in region.islands.iter() {
            let mut value_refs = vec![None; island.program.values.len()];
            let start = self.instructions.len();
            self.member_boundary_start = self.boundary_inputs.len();
            self.lower_program(&island.program, &mut value_refs)?;
            let export_start = self.exports.len();
            for node in island.exports.iter() {
                let (register, _) = self.node_registers.get(node.index()).copied().flatten()?;
                self.exports.push(RegionExport {
                    node: *node,
                    register,
                });
            }
            self.members.push(RegionMember {
                stream: region.stream,
                instructions: start..self.instructions.len(),
                boundary_inputs: self.member_boundary_start..self.boundary_inputs.len(),
                exports: export_start..self.exports.len(),
                output: None,
            });
        }
        Some(())
    }

    fn lower_program(
        &mut self,
        program: &ScalarProgram,
        value_refs: &mut [Option<RegionRef>],
    ) -> Option<()> {
        for instruction in program.instructions.iter() {
            let (node, operation) = match instruction {
                ScalarSsaInstruction::Unary {
                    node,
                    op,
                    input,
                    input_kind,
                    output_kind,
                    ..
                } => {
                    if !supports_unary(*op, *input_kind, *output_kind) {
                        return None;
                    }
                    (
                        *node,
                        RegionOperation::Unary {
                            op: *op,
                            input: self.lower_value(program, *input, value_refs)?,
                            input_kind: *input_kind,
                            output_kind: *output_kind,
                        },
                    )
                }
                ScalarSsaInstruction::Binary {
                    node,
                    op,
                    left,
                    right,
                    left_kind,
                    right_kind,
                    output_kind,
                    ..
                } => {
                    if !supports_binary(*op, *left_kind, *right_kind, *output_kind) {
                        return None;
                    }
                    (
                        *node,
                        RegionOperation::Binary {
                            op: *op,
                            left: self.lower_value(program, *left, value_refs)?,
                            right: self.lower_value(program, *right, value_refs)?,
                            left_kind: *left_kind,
                            right_kind: *right_kind,
                            output_kind: *output_kind,
                        },
                    )
                }
                ScalarSsaInstruction::Temporal {
                    node,
                    op,
                    output_kind,
                    ..
                } => (
                    *node,
                    RegionOperation::Temporal {
                        node: *node,
                        op: match op {
                            ScalarTemporalOp::Delay {
                                input,
                                offset,
                                external,
                            } => RegionTemporalOp::Delay {
                                input: self.lower_value(program, *input, value_refs)?,
                                offset: *offset,
                                external: *external,
                            },
                            ScalarTemporalOp::RecursiveDelay => RegionTemporalOp::RecursiveDelay,
                            ScalarTemporalOp::Default { input, fallback } => {
                                RegionTemporalOp::Default {
                                    input: self.lower_value(program, *input, value_refs)?,
                                    fallback: self.lower_value(program, *fallback, value_refs)?,
                                }
                            }
                        },
                        output_kind: *output_kind,
                    },
                ),
                ScalarSsaInstruction::EagerSelect {
                    node,
                    condition,
                    then_program,
                    else_program,
                    output_kind,
                    ..
                } => {
                    let condition = self.lower_value(program, *condition, value_refs)?;
                    let then_program = Box::new(self.lower_branch(then_program)?);
                    let else_program = Box::new(self.lower_branch(else_program)?);
                    (
                        *node,
                        RegionOperation::EagerSelect {
                            condition,
                            then_program,
                            else_program,
                            output_kind: *output_kind,
                        },
                    )
                }
            };
            let target = self.allocate_register()?;
            let output_kind = operation.output_kind();
            self.instructions
                .push(RegionInstruction { target, operation });
            self.instruction_nodes.push(node);
            // Graph regions record where each canonical node lives so a later island can read it
            // from a register instead of the canonical arena.
            if let Some(slot) = self.node_registers.get_mut(node.index()) {
                *slot = Some((target, output_kind));
            }
            value_refs[instruction.result().index()] = Some(RegionRef::Register {
                register: target,
                kind: output_kind,
            });
        }
        Some(())
    }

    fn lower_value(
        &mut self,
        program: &ScalarProgram,
        value: ScalarValueId,
        value_refs: &mut [Option<RegionRef>],
    ) -> Option<RegionRef> {
        if let Some(reference) = value_refs.get(value.index()).copied().flatten() {
            return Some(reference);
        }
        let scalar_value = program.values.get(value.index())?;
        let kind = scalar_value.ty.kind;
        let reference = match &scalar_value.definition {
            ScalarValueDefinition::Constant(constant) => {
                RegionRef::Constant(ScalarValue::from_value(constant, kind)?)
            }
            ScalarValueDefinition::External(slot) => self.lower_external(*slot, kind)?,
            ScalarValueDefinition::CanonicalNode(node) => self.lower_boundary(*node, kind)?,
            // Instruction results are recorded when the instruction is lowered.
            ScalarValueDefinition::Instruction(_) => return None,
        };
        value_refs[value.index()] = Some(reference);
        Some(reference)
    }

    fn lower_branch(&mut self, program: &ScalarProgram) -> Option<QuickenedBranchPlan> {
        let mut builder = Self {
            stream_slots: self.stream_slots,
            environment_len: self.environment_len,
            // Branch registers are local, so earlier stream results cross through publication.
            stream_outputs: vec![None; self.stream_outputs.len()],
            instructions: Vec::new(),
            instruction_nodes: Vec::new(),
            inputs: Vec::new(),
            boundary_inputs: Vec::new(),
            exports: Vec::new(),
            members: Vec::new(),
            node_registers: Vec::new(),
            member_boundary_start: 0,
            register_count: 0,
        };
        let mut value_refs = vec![None; program.values.len()];
        builder.lower_program(program, &mut value_refs)?;
        let mut output = builder.lower_value(program, program.output, &mut value_refs)?;
        // Branch graphs produced by whole-program scalar lowering have no canonical boundaries.
        if !builder.boundary_inputs.is_empty() {
            return None;
        }
        // A prior stream in this region has already written its output to the environment when the
        // branch runs. Nested registers cannot name the parent's register file, and the immutable
        // publication array does not yet contain that same-region result, so branches consistently
        // read stream inputs through the environment boundary.
        for instruction in &mut builder.instructions {
            instruction
                .operation
                .use_environment_for_published(self.stream_slots);
        }
        output.use_environment_for_published(self.stream_slots);
        for input in builder.inputs.iter().copied() {
            if let RegionInputSource::Published(stream) = input.source
                && let Some(produced) = self.stream_outputs.get(stream.index()).copied().flatten()
            {
                if produced.kind != input.kind {
                    return None;
                }
                // This value is produced by an earlier member after region preflight, so the stale
                // environment slot must neither be checked nor copied before execution.
                continue;
            }
            match self
                .inputs
                .iter()
                .find(|existing| existing.slot == input.slot)
            {
                Some(existing) if existing.kind != input.kind => return None,
                Some(_) => {}
                None => self.inputs.push(input),
            }
        }
        Some(QuickenedBranchPlan {
            instructions: builder.instructions.into_boxed_slice(),
            instruction_nodes: builder.instruction_nodes.into_boxed_slice(),
            output,
            register_count: builder.register_count,
        })
    }

    fn lower_external(&mut self, slot: EnvironmentSlot, kind: ScalarKind) -> Option<RegionRef> {
        if slot.index() >= self.environment_len {
            return None;
        }
        let stream = self.stream_slots.stream(slot);
        if let Some(stream) = stream
            && let Some(output) = self.stream_outputs.get(stream.index()).copied().flatten()
        {
            return (output.kind == kind).then_some(RegionRef::Register {
                register: output.register,
                kind,
            });
        }
        let source = stream
            .map(RegionInputSource::Published)
            .unwrap_or(RegionInputSource::Environment);
        let input = RegionInput { source, slot, kind };
        match self.inputs.iter().find(|existing| existing.slot == slot) {
            Some(existing) if existing.kind != kind => return None,
            Some(_) => {}
            None => self.inputs.push(input),
        }
        Some(match source {
            RegionInputSource::Published(stream) => RegionRef::Published { stream, kind },
            RegionInputSource::Environment => RegionRef::Environment { slot, kind },
        })
    }

    /// Resolves a reference to a node this program does not contain.
    ///
    /// An earlier island of the same region already holds that node in a register. Anything else is
    /// canonical and is loaded once per member, because a member that declines a row must leave the
    /// registers of the members around it untouched.
    fn lower_boundary(&mut self, node: NodeId, kind: ScalarKind) -> Option<RegionRef> {
        if let Some((register, produced)) = self.node_registers.get(node.index()).copied().flatten()
        {
            return (produced == kind).then_some(RegionRef::Register { register, kind });
        }
        if let Some(existing) = self.boundary_inputs[self.member_boundary_start..]
            .iter()
            .find(|existing| existing.node == node)
        {
            return (existing.kind == kind).then_some(RegionRef::Register {
                register: existing.register,
                kind,
            });
        }
        let register = self.allocate_register()?;
        self.boundary_inputs.push(BoundaryInput {
            node,
            kind,
            register,
        });
        Some(RegionRef::Register { register, kind })
    }

    fn allocate_register(&mut self) -> Option<usize> {
        let register = self.register_count;
        self.register_count = self.register_count.checked_add(1)?;
        Some(register)
    }
}

impl RegionRef {
    fn use_environment_for_published(&mut self, stream_slots: StreamSlots) {
        if let Self::Published { stream, kind } = *self {
            *self = Self::Environment {
                slot: stream_slots.slot(stream),
                kind,
            };
        }
    }
}

impl RegionOperation {
    fn use_environment_for_published(&mut self, stream_slots: StreamSlots) {
        match self {
            Self::Temporal { op, .. } => match op {
                RegionTemporalOp::Delay { input, .. } => {
                    input.use_environment_for_published(stream_slots)
                }
                RegionTemporalOp::Default { input, fallback } => {
                    input.use_environment_for_published(stream_slots);
                    fallback.use_environment_for_published(stream_slots);
                }
                RegionTemporalOp::RecursiveDelay => {}
            },
            Self::Unary { input, .. } => input.use_environment_for_published(stream_slots),
            Self::Binary { left, right, .. } => {
                left.use_environment_for_published(stream_slots);
                right.use_environment_for_published(stream_slots);
            }
            Self::EagerSelect {
                condition,
                then_program,
                else_program,
                ..
            } => {
                condition.use_environment_for_published(stream_slots);
                for instruction in then_program
                    .instructions
                    .iter_mut()
                    .chain(else_program.instructions.iter_mut())
                {
                    instruction
                        .operation
                        .use_environment_for_published(stream_slots);
                }
                then_program
                    .output
                    .use_environment_for_published(stream_slots);
                else_program
                    .output
                    .use_environment_for_published(stream_slots);
            }
        }
    }
}

/// Executes one temporal instruction against the canonical arena.
///
/// This dispatches by type and keeps the result in a register, but every retention decision is
/// still made by the canonical `NodeState`, so delay, recursion, and default semantics have exactly
/// one implementation and the tick commit barrier is untouched.
///
/// The nodes are always in their typed scalar form here: [`QuickenedRegionPlan::synchronize`]
/// promotes them as a precondition of taking the region over, and once quickened the region is the
/// only writer of that state and only ever writes scalars into it.
#[inline]
#[allow(clippy::too_many_arguments)]
fn execute_temporal(
    node: NodeId,
    op: &RegionTemporalOp,
    registers: &[ScalarValue],
    environment_values: &[Value],
    published_scalars: &[Option<ScalarValue>],
    arena: &mut CanonicalArena<'_>,
) -> ScalarValue {
    let index = node.index();
    match op {
        RegionTemporalOp::Delay {
            input,
            offset,
            external,
        } => {
            let NodeState::ScalarDelay(history) = &mut arena.node_states[index] else {
                unreachable!("a quickened delay is a scalar delay")
            };
            if *offset == 0 {
                let current =
                    read_region_ref(*input, registers, environment_values, published_scalars);
                return history.retain_current_value(current);
            }
            if let (Some(history_access), Some(slot)) = (arena.history, external)
                && history_access.has_binding(*slot)
            {
                let value = history_access.read(
                    *slot,
                    usize::try_from(*offset).expect("sindex offset does not fit usize"),
                );
                let Some(value) = ScalarValue::from_untyped_value(&value) else {
                    unreachable!("shared history of a quickened delay holds a scalar")
                };
                return history.read_shared_value(value);
            }
            history.read_and_stage_write()
        }
        RegionTemporalOp::RecursiveDelay => {
            let NodeState::ScalarDelay(history) = &mut arena.node_states[index] else {
                unreachable!("a quickened recursive delay is a scalar delay")
            };
            history.read_delayed_value()
        }
        RegionTemporalOp::Default { input, fallback } => {
            let current = read_region_ref(*input, registers, environment_values, published_scalars);
            let fallback =
                read_region_ref(*fallback, registers, environment_values, published_scalars);
            let NodeState::ScalarDefault { last_input } = &mut arena.node_states[index] else {
                unreachable!("a quickened default is a scalar default")
            };
            let retained = retain_last(current, last_input);
            if retained == ScalarValue::Deferred {
                fallback
            } else {
                retained
            }
        }
    }
}

#[inline(always)]
fn execute_instruction(
    operation: &RegionOperation,
    registers: &[ScalarValue],
    environment_values: &[Value],
    published_scalars: &[Option<ScalarValue>],
    state: &mut QuickenedInstructionState,
) -> ScalarValue {
    match (operation, state) {
        (
            RegionOperation::Unary {
                op,
                input,
                input_kind,
                output_kind,
            },
            QuickenedInstructionState::Unary { last_input },
        ) => {
            let input = read_region_ref(*input, registers, environment_values, published_scalars);
            let input = retain_last(input, last_input);
            let output = if input.is_special() {
                input
            } else {
                apply_unary(*op, input)
            };
            debug_assert!(input.has_kind(*input_kind));
            debug_assert!(output.has_kind(*output_kind));
            output
        }
        (
            RegionOperation::Binary {
                op,
                left,
                right,
                left_kind,
                right_kind,
                output_kind,
            },
            QuickenedInstructionState::Binary {
                last_left,
                last_right,
            },
        ) => {
            let left = read_region_ref(*left, registers, environment_values, published_scalars);
            let right = read_region_ref(*right, registers, environment_values, published_scalars);
            let left = retain_last(left, last_left);
            let right = retain_last(right, last_right);
            let output = if left.is_special() || right.is_special() {
                propagated_special([PartialMarker::of(&left), PartialMarker::of(&right)])
                    .expect("a special operand propagates a marker")
                    .into_value()
            } else {
                apply_binary(*op, left, right)
            };
            debug_assert!(left.has_kind(*left_kind));
            debug_assert!(right.has_kind(*right_kind));
            debug_assert!(output.has_kind(*output_kind));
            output
        }
        (
            RegionOperation::EagerSelect {
                condition,
                then_program,
                else_program,
                output_kind,
            },
            QuickenedInstructionState::EagerSelect {
                then_state,
                else_state,
                last_condition,
                last_then_value,
                last_else_value,
            },
        ) => {
            let condition = retain_last(
                read_region_ref(*condition, registers, environment_values, published_scalars),
                last_condition,
            );
            let then_value = retain_last(
                execute_branch(
                    then_program,
                    then_state,
                    environment_values,
                    published_scalars,
                ),
                last_then_value,
            );
            let else_value = retain_last(
                execute_branch(
                    else_program,
                    else_state,
                    environment_values,
                    published_scalars,
                ),
                last_else_value,
            );
            let branch_marker = propagated_special([
                PartialMarker::of(&then_value),
                PartialMarker::of(&else_value),
            ]);
            let output = if branch_marker == Some(PartialMarker::NoVal) {
                ScalarValue::NoVal
            } else {
                match condition {
                    ScalarValue::Bool(true) => then_value,
                    ScalarValue::Bool(false) => else_value,
                    ScalarValue::Deferred => ScalarValue::Deferred,
                    ScalarValue::NoVal => ScalarValue::NoVal,
                    _ => unreachable!("eager-select condition is boolean"),
                }
            };
            debug_assert!(output.has_kind(*output_kind));
            output
        }
        _ => unreachable!("region instruction has incompatible lifting state"),
    }
}

// Keep recursive branch execution out of line so the ordinary scalar instruction body can inline
// into the region loop without pulling branch machinery into that common path.
#[cold]
#[inline(never)]
fn execute_branch(
    plan: &QuickenedBranchPlan,
    state: &mut QuickenedBranchState,
    environment_values: &[Value],
    published_scalars: &[Option<ScalarValue>],
) -> ScalarValue {
    for (instruction, instruction_state) in plan
        .instructions
        .iter()
        .zip(state.instruction_states.iter_mut())
    {
        if !matches!(&instruction.operation, RegionOperation::Temporal { .. }) {
            let value = execute_instruction(
                &instruction.operation,
                &state.registers,
                environment_values,
                published_scalars,
                instruction_state,
            );
            state.registers[instruction.target] = value;
            continue;
        }
        unreachable!("temporal eager-select branches are excluded from scalar regions");
    }
    read_region_ref(
        plan.output,
        &state.registers,
        environment_values,
        published_scalars,
    )
}

#[inline(always)]
fn read_region_ref(
    reference: RegionRef,
    registers: &[ScalarValue],
    environment_values: &[Value],
    published_scalars: &[Option<ScalarValue>],
) -> ScalarValue {
    match reference {
        RegionRef::Constant(value) => value,
        RegionRef::Register { register, kind } => {
            let value = registers[register];
            debug_assert!(value.has_kind(kind));
            value
        }
        RegionRef::Environment { slot, kind } => {
            ScalarValue::from_value(&environment_values[slot.index()], kind)
                .unwrap_or_else(|| unreachable!("scalar region input passed the type preflight"))
        }
        RegionRef::Published { stream, kind } => published_scalars[stream.index()]
            .filter(|value| value.has_kind(kind))
            .unwrap_or_else(|| unreachable!("scalar region publication passed the type preflight")),
    }
}

impl RegionOperation {
    #[inline]
    fn output_kind(&self) -> ScalarKind {
        match self {
            Self::Unary { output_kind, .. }
            | Self::Binary { output_kind, .. }
            | Self::Temporal { output_kind, .. }
            | Self::EagerSelect { output_kind, .. } => *output_kind,
        }
    }
}

impl QuickenedInstructionState {
    fn new(operation: &RegionOperation) -> Self {
        match operation {
            RegionOperation::Temporal { .. } => Self::Canonical,
            RegionOperation::Unary { .. } => Self::Unary { last_input: None },
            RegionOperation::Binary { .. } => Self::Binary {
                last_left: None,
                last_right: None,
            },
            RegionOperation::EagerSelect {
                then_program,
                else_program,
                ..
            } => Self::EagerSelect {
                then_state: Box::new(QuickenedBranchState::new(then_program)),
                else_state: Box::new(QuickenedBranchState::new(else_program)),
                last_condition: None,
                last_then_value: None,
                last_else_value: None,
            },
        }
    }

    #[cfg(test)]
    fn reset(&mut self) {
        match self {
            Self::Canonical => {}
            Self::Unary { last_input } => *last_input = None,
            Self::Binary {
                last_left,
                last_right,
            } => {
                *last_left = None;
                *last_right = None;
            }
            Self::EagerSelect {
                then_state,
                else_state,
                last_condition,
                last_then_value,
                last_else_value,
            } => {
                then_state.reset();
                else_state.reset();
                *last_condition = None;
                *last_then_value = None;
                *last_else_value = None;
            }
        }
    }

    fn materialize(&self, operation: &RegionOperation, canonical: &mut NodeState) {
        match (self, operation, canonical) {
            (Self::Canonical, RegionOperation::Temporal { .. }, canonical) => {
                canonical.demote_scalar_temporal();
            }
            (
                Self::Unary { last_input },
                RegionOperation::Unary { .. },
                NodeState::UnaryLift {
                    last_input: canonical_last_input,
                },
            ) => *canonical_last_input = last_input.map(ScalarValue::into_value),
            (
                Self::Binary {
                    last_left,
                    last_right,
                },
                RegionOperation::Binary { .. },
                NodeState::BinaryLift {
                    last_left: canonical_last_left,
                    last_right: canonical_last_right,
                },
            ) => {
                *canonical_last_left = last_left.map(ScalarValue::into_value);
                *canonical_last_right = last_right.map(ScalarValue::into_value);
            }
            (
                Self::EagerSelect {
                    then_state,
                    else_state,
                    last_condition,
                    last_then_value,
                    last_else_value,
                },
                RegionOperation::EagerSelect {
                    then_program,
                    else_program,
                    ..
                },
                NodeState::LazyIf(LazyIfState {
                    then_state: canonical_then,
                    else_state: canonical_else,
                    last_condition: canonical_condition,
                    last_then_value: canonical_then_value,
                    last_else_value: canonical_else_value,
                }),
            ) => {
                then_program.materialize(then_state, canonical_then);
                else_program.materialize(else_state, canonical_else);
                *canonical_condition = last_condition.map(ScalarValue::into_value);
                *canonical_then_value = last_then_value.map(ScalarValue::into_value);
                *canonical_else_value = last_else_value.map(ScalarValue::into_value);
            }
            _ => unreachable!("region instruction has incompatible canonical state"),
        }
    }

    fn synchronize(&mut self, operation: &RegionOperation, canonical: &mut NodeState) -> bool {
        match (self, operation, canonical) {
            (Self::Canonical, RegionOperation::Temporal { .. }, canonical) => {
                return canonical.promote_scalar_temporal();
            }
            (
                Self::Unary { last_input },
                RegionOperation::Unary { .. },
                NodeState::UnaryLift { last_input: source },
            ) => {
                *last_input = source.as_ref().and_then(ScalarValue::from_untyped_value);
            }
            (
                Self::Binary {
                    last_left,
                    last_right,
                },
                RegionOperation::Binary { .. },
                NodeState::BinaryLift {
                    last_left: source_left,
                    last_right: source_right,
                },
            ) => {
                *last_left = source_left
                    .as_ref()
                    .and_then(ScalarValue::from_untyped_value);
                *last_right = source_right
                    .as_ref()
                    .and_then(ScalarValue::from_untyped_value);
            }
            (
                Self::EagerSelect {
                    then_state,
                    else_state,
                    last_condition,
                    last_then_value,
                    last_else_value,
                },
                RegionOperation::EagerSelect {
                    then_program,
                    else_program,
                    ..
                },
                NodeState::LazyIf(LazyIfState {
                    then_state: canonical_then,
                    else_state: canonical_else,
                    last_condition: source_condition,
                    last_then_value: source_then_value,
                    last_else_value: source_else_value,
                }),
            ) => {
                *last_condition = source_condition
                    .as_ref()
                    .and_then(ScalarValue::from_untyped_value);
                *last_then_value = source_then_value
                    .as_ref()
                    .and_then(ScalarValue::from_untyped_value);
                *last_else_value = source_else_value
                    .as_ref()
                    .and_then(ScalarValue::from_untyped_value);
                return then_program.synchronize(then_state, canonical_then)
                    && else_program.synchronize(else_state, canonical_else);
            }
            _ => unreachable!("region instruction has incompatible canonical state"),
        }
        true
    }
}
