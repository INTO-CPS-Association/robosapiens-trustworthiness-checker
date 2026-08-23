//! Runtime bridge, side exits, and canonical-state replay for native artifacts.

use std::rc::Rc;

use crate::dataflow::environment::EnvironmentLayout;
use crate::dataflow::execution::interpreter::{evaluate_node, evaluate_nodes};
use crate::dataflow::execution::quickening::ScalarValue;
use crate::dataflow::execution::scheduled_plan::{ScheduledExecutionPlan, TemporalPlan};
use crate::dataflow::execution::stream_evaluator::EvaluationContext;
use crate::dataflow::execution::stream_evaluator::StreamEvaluator;
use crate::dataflow::execution::stream_state::{NodeState, StreamState};
use crate::dataflow::execution_plan::StreamSlots;
use crate::dataflow::ir::{BoundEvaluationGraph, NodeId, ScalarKind};
use crate::dataflow::*;

use super::backend::{
    CompiledGraph, CompiledRun, CompiledTemporalRun, InputSource, InputSpec, Lowering, STATUS_OK,
    TemporalNodeLayout, TemporalStateLayout, compile_graphs, compile_run, compile_temporal_run,
    decode,
};
use super::scheduled_state::ScheduledTemporalPlan;

pub(in crate::dataflow) enum JittedRunOutcome {
    Success,
    SuccessCommitted,
    Fallback {
        replay_environment: Option<Vec<Value>>,
    },
}

pub(in crate::dataflow) struct JittedRunEvaluator {
    compiled: CompiledRun,
    environment_scratch: Vec<i64>,
    previous_environment: Vec<i64>,
    has_previous: bool,
    last_tick_native: bool,
    disabled: bool,
}

impl JittedRunEvaluator {
    pub(in crate::dataflow) fn compile(
        plan: &ScheduledExecutionPlan,
    ) -> Result<Option<Self>, String> {
        if plan.has_source_barrier() {
            return Ok(None);
        }
        let graphs = plan
            .streams
            .iter()
            .map(|stream| {
                (
                    &stream.program.graph,
                    stream.output.environment(),
                    stream.stream.index(),
                )
            })
            .collect::<Vec<_>>();
        let Some(compiled) = compile_run(&graphs, plan.stream_slots)? else {
            return Ok(None);
        };
        let environment_len = compiled.environment_len;
        Ok(Some(Self {
            compiled,
            environment_scratch: vec![0; environment_len],
            previous_environment: vec![0; environment_len],
            has_previous: false,
            last_tick_native: false,
            disabled: false,
        }))
    }

    pub(in crate::dataflow) fn reset_after_context_transfer(&mut self) {
        self.environment_scratch.fill(0);
        self.previous_environment.fill(0);
        self.has_previous = false;
        self.last_tick_native = false;
        self.disabled = false;
    }

    pub(in crate::dataflow) fn snapshot_replay_environment(&self) -> Option<Vec<Value>> {
        self.last_tick_native.then(|| {
            replay_environment(
                &self.compiled.external_inputs,
                &self.previous_environment,
                self.compiled.environment_len,
            )
        })
    }

    #[inline(always)]
    pub(in crate::dataflow) fn evaluate(
        &mut self,
        environment_values: &mut [Value],
        published_scalars: &mut [Option<ScalarValue>],
    ) -> JittedRunOutcome {
        self.last_tick_native = false;
        if self.disabled {
            return JittedRunOutcome::Fallback {
                replay_environment: None,
            };
        }
        for input in self.compiled.external_inputs.iter() {
            let InputSource::External(slot) = input.source else {
                unreachable!("fused scalar runs cannot contain canonical boundary inputs")
            };
            let encoded = match (&environment_values[slot.index()], input.kind) {
                (Value::Int(value), ScalarKind::Int) => *value,
                (Value::Float(value), ScalarKind::Float) => value.to_bits() as i64,
                (Value::Bool(value), ScalarKind::Bool) => i64::from(*value),
                (Value::NoVal | Value::Deferred, _) => return self.fallback(false),
                _ => return self.fallback(true),
            };
            self.environment_scratch[slot.index()] = encoded;
        }
        // SAFETY: `compile_run` fixes the raw-environment layout and keeps its module alive.
        let status = unsafe { (self.compiled.function)(self.environment_scratch.as_mut_ptr()) };
        if status != STATUS_OK {
            return self.fallback(true);
        }
        for &(slot, kind, stream) in self.compiled.outputs.iter() {
            let value = decode(self.environment_scratch[slot.index()], kind);
            published_scalars[stream] = ScalarValue::from_untyped_value(&value);
            environment_values[slot.index()] = value;
        }
        std::mem::swap(
            &mut self.previous_environment,
            &mut self.environment_scratch,
        );
        self.has_previous = true;
        self.last_tick_native = true;
        JittedRunOutcome::Success
    }

    fn fallback(&mut self, permanent: bool) -> JittedRunOutcome {
        self.last_tick_native = false;
        self.disabled |= permanent;
        let replay_environment = self.has_previous.then(|| {
            let mut values = vec![Value::NoVal; self.compiled.environment_len];
            for input in self.compiled.external_inputs.iter() {
                let InputSource::External(slot) = input.source else {
                    unreachable!()
                };
                values[slot.index()] = decode(self.previous_environment[slot.index()], input.kind);
            }
            values
        });
        JittedRunOutcome::Fallback { replay_environment }
    }
}

pub(in crate::dataflow) struct JittedTemporalRunEvaluator {
    compiled: CompiledTemporalRun,
    environment_scratch: Vec<i64>,
    previous_environment: Vec<i64>,
    temporal_state: Vec<i64>,
    temporal_plans: Box<[Option<ScheduledTemporalPlan>]>,
    state_ready: bool,
    has_previous: bool,
    last_tick_native: bool,
    disabled: bool,
}

impl JittedTemporalRunEvaluator {
    pub(in crate::dataflow) fn compile(
        plan: &ScheduledExecutionPlan,
    ) -> Result<Option<Self>, String> {
        if plan.has_source_barrier() {
            return Ok(None);
        }
        let Some(compiled) = compile_temporal_run(plan)? else {
            return Ok(None);
        };
        let mut temporal_plans = vec![None; plan.stream_slots.len()];
        for planned in plan.streams.iter() {
            let nodes = planned.temporal.nodes().collect::<Vec<_>>();
            temporal_plans[planned.stream.index()] =
                ScheduledTemporalPlan::build(&planned.temporal, &nodes);
        }
        if compiled
            .states
            .iter()
            .any(|layout| temporal_plans[layout.stream].is_none())
        {
            return Ok(None);
        }
        Ok(Some(Self {
            environment_scratch: vec![0; compiled.environment_len],
            previous_environment: vec![0; compiled.environment_len],
            temporal_state: vec![0; compiled.state_len],
            temporal_plans: temporal_plans.into_boxed_slice(),
            compiled,
            state_ready: false,
            has_previous: false,
            last_tick_native: false,
            disabled: false,
        }))
    }

    pub(in crate::dataflow) fn reset_after_context_transfer(&mut self) {
        self.environment_scratch.fill(0);
        self.previous_environment.fill(0);
        self.temporal_state.fill(0);
        self.state_ready = false;
        self.has_previous = false;
        self.last_tick_native = false;
        self.disabled = false;
    }

    pub(in crate::dataflow) fn snapshot_replay_environment(&self) -> Option<Vec<Value>> {
        self.last_tick_native.then(|| {
            replay_environment(
                &self.compiled.external_inputs,
                &self.previous_environment,
                self.compiled.environment_len,
            )
        })
    }

    #[inline(always)]
    pub(in crate::dataflow) fn evaluate(
        &mut self,
        evaluators: &mut [StreamEvaluator],
        environment_values: &mut [Value],
        published_scalars: &mut [Option<ScalarValue>],
    ) -> JittedRunOutcome {
        self.last_tick_native = false;
        if self.disabled {
            return JittedRunOutcome::Fallback {
                replay_environment: None,
            };
        }
        if !self.state_ready && !self.promote(evaluators) {
            self.disabled = true;
            return JittedRunOutcome::Fallback {
                replay_environment: None,
            };
        }
        for input in self.compiled.external_inputs.iter() {
            let InputSource::External(slot) = input.source else {
                unreachable!()
            };
            let Some(value) = ScalarValue::from_untyped_value(&environment_values[slot.index()])
            else {
                return self.fallback(evaluators);
            };
            let Some(value) = encode_present(value, input.kind) else {
                return self.fallback(evaluators);
            };
            self.environment_scratch[slot.index()] = value;
        }
        // SAFETY: the compiled plan fixes this environment/state ABI and owns both layouts.
        let status = unsafe {
            (self.compiled.function)(
                self.environment_scratch.as_mut_ptr(),
                self.temporal_state.as_mut_ptr(),
            )
        };
        if status != STATUS_OK {
            return self.fallback(evaluators);
        }
        for &(slot, kind, stream) in self.compiled.outputs.iter() {
            let value = decode(self.environment_scratch[slot.index()], kind);
            published_scalars[stream] = ScalarValue::from_untyped_value(&value);
            environment_values[slot.index()] = value;
        }
        std::mem::swap(
            &mut self.previous_environment,
            &mut self.environment_scratch,
        );
        self.has_previous = true;
        self.last_tick_native = true;
        JittedRunOutcome::SuccessCommitted
    }

    fn promote(&mut self, evaluators: &mut [StreamEvaluator]) -> bool {
        for (index, layout) in self.compiled.states.iter().enumerate() {
            let temporal_plan = self.temporal_plans[layout.stream].as_ref().unwrap();
            let state = &mut evaluators[layout.stream].state;
            if !temporal_plan.promote(state) {
                self.deopt_promoted(evaluators, index);
                return false;
            }
            let Some(promoted) = NativeTemporalState::promote(&layout.layout, state) else {
                temporal_plan.deopt(state);
                self.deopt_promoted(evaluators, index);
                return false;
            };
            let end = layout.offset + layout.layout.len;
            self.temporal_state[layout.offset..end].copy_from_slice(&promoted.cells);
        }
        self.state_ready = true;
        true
    }

    fn deopt_promoted(&self, evaluators: &mut [StreamEvaluator], count: usize) {
        for layout in self.compiled.states[..count].iter() {
            self.temporal_plans[layout.stream]
                .as_ref()
                .unwrap()
                .deopt(&mut evaluators[layout.stream].state);
        }
    }

    /// Materialize the packed temporal state into a snapshot evaluator arena without disabling
    /// the active native artifact.
    pub(in crate::dataflow) fn snapshot_into(&self, evaluators: &mut [StreamEvaluator]) {
        if !self.state_ready {
            return;
        }
        for layout in self.compiled.states.iter() {
            let end = layout.offset + layout.layout.len;
            let native = NativeTemporalState {
                cells: self.temporal_state[layout.offset..end].to_vec(),
            };
            let state = &mut evaluators[layout.stream].state;
            native.materialize(&layout.layout, state);
            self.temporal_plans[layout.stream]
                .as_ref()
                .unwrap()
                .deopt(state);
        }
    }

    fn fallback(&mut self, evaluators: &mut [StreamEvaluator]) -> JittedRunOutcome {
        self.last_tick_native = false;
        for layout in self.compiled.states.iter() {
            let end = layout.offset + layout.layout.len;
            let native = NativeTemporalState {
                cells: self.temporal_state[layout.offset..end].to_vec(),
            };
            let state = &mut evaluators[layout.stream].state;
            native.materialize(&layout.layout, state);
            self.temporal_plans[layout.stream]
                .as_ref()
                .unwrap()
                .deopt(state);
        }
        self.disabled = true;
        self.state_ready = false;
        let replay_environment = self.has_previous.then(|| {
            let mut values = vec![Value::NoVal; self.compiled.environment_len];
            for input in self.compiled.external_inputs.iter() {
                let InputSource::External(slot) = input.source else {
                    unreachable!()
                };
                values[slot.index()] = decode(self.previous_environment[slot.index()], input.kind);
            }
            values
        });
        JittedRunOutcome::Fallback { replay_environment }
    }
}

fn replay_environment(
    inputs: &[InputSpec],
    previous_environment: &[i64],
    environment_len: usize,
) -> Vec<Value> {
    let mut values = vec![Value::NoVal; environment_len];
    for input in inputs {
        let InputSource::External(slot) = input.source else {
            continue;
        };
        values[slot.index()] = decode(previous_environment[slot.index()], input.kind);
    }
    values
}

#[derive(Clone)]
pub(in crate::dataflow) struct JittedGraphEvaluator {
    compiled: Rc<CompiledGraph>,
    input_scratch: Vec<i64>,
    previous_inputs: Vec<i64>,
    output_scratch: i64,
    has_previous_inputs: bool,
    last_tick_native: bool,
    disabled: bool,
    temporal_plan: ScheduledTemporalPlan,
    scheduled_scalars: Vec<Option<ScalarValue>>,
    handled_current_tick: bool,
    scalar_state_ready: bool,
    temporal_kernel_state: Option<NativeTemporalState>,
    temporal_kernel_completed_tick: bool,
}

#[derive(Clone)]
struct NativeTemporalState {
    cells: Vec<i64>,
}

impl NativeTemporalState {
    fn promote(layout: &TemporalStateLayout, state: &StreamState) -> Option<Self> {
        let mut cells = vec![0; layout.len];
        for node in &layout.nodes {
            match node {
                TemporalNodeLayout::Delay {
                    node,
                    kind,
                    cursor,
                    filled,
                    last_bits,
                    last_tag,
                    cells: values_start,
                    len,
                    ..
                } => {
                    let NodeState::ScalarDelay(delay) = &state.node_states[node.index()] else {
                        return None;
                    };
                    let (values, next, used, last) = delay.native_parts();
                    if values.len() != *len {
                        return None;
                    }
                    cells[*cursor] = next as i64;
                    cells[*filled] = used as i64;
                    let mut bits = 0;
                    let mut tag = 0;
                    encode_optional(last, *kind, &mut bits, &mut tag)?;
                    cells[*last_bits] = bits;
                    cells[*last_tag] = tag;
                    for (index, value) in values.iter().copied().enumerate() {
                        let written = used == *len || index < used;
                        cells[*values_start + index] = if written {
                            encode_present(value, *kind)?
                        } else {
                            0
                        };
                    }
                }
                TemporalNodeLayout::Default {
                    node,
                    kind,
                    last_bits,
                    last_tag,
                } => {
                    let NodeState::ScalarDefault { last_input } = &state.node_states[node.index()]
                    else {
                        return None;
                    };
                    let mut bits = 0;
                    let mut tag = 0;
                    encode_optional(*last_input, *kind, &mut bits, &mut tag)?;
                    cells[*last_bits] = bits;
                    cells[*last_tag] = tag;
                }
            }
        }
        Some(Self { cells })
    }

    fn materialize(&self, layout: &TemporalStateLayout, state: &mut StreamState) {
        for node in &layout.nodes {
            match node {
                TemporalNodeLayout::Delay {
                    node,
                    kind,
                    cursor,
                    filled,
                    last_bits,
                    last_tag,
                    cells: values_start,
                    len,
                    ..
                } => {
                    let next = self.cells[*cursor] as usize;
                    let used = self.cells[*filled] as usize;
                    let values = (0..*len)
                        .map(|index| {
                            let written = used == *len || index < used;
                            if written {
                                decode_present(self.cells[*values_start + index], *kind)
                            } else {
                                ScalarValue::NoVal
                            }
                        })
                        .collect::<Vec<_>>();
                    let last =
                        decode_optional(self.cells[*last_bits], self.cells[*last_tag], *kind);
                    let NodeState::ScalarDelay(delay) = &mut state.node_states[node.index()] else {
                        unreachable!("promoted temporal kernel lost scalar delay state")
                    };
                    delay.restore_native_parts(&values, next, used, last);
                }
                TemporalNodeLayout::Default {
                    node,
                    kind,
                    last_bits,
                    last_tag,
                } => {
                    let last =
                        decode_optional(self.cells[*last_bits], self.cells[*last_tag], *kind);
                    let NodeState::ScalarDefault { last_input } =
                        &mut state.node_states[node.index()]
                    else {
                        unreachable!("promoted temporal kernel lost scalar default state")
                    };
                    *last_input = last;
                }
            }
        }
    }
}

fn encode_present(value: ScalarValue, kind: ScalarKind) -> Option<i64> {
    match (value, kind) {
        (ScalarValue::Int(value), ScalarKind::Int) => Some(value),
        (ScalarValue::Float(value), ScalarKind::Float) => Some(value.to_bits() as i64),
        (ScalarValue::Bool(value), ScalarKind::Bool) => Some(i64::from(value)),
        _ => None,
    }
}

fn decode_present(value: i64, kind: ScalarKind) -> ScalarValue {
    match kind {
        ScalarKind::Int => ScalarValue::Int(value),
        ScalarKind::Float => ScalarValue::Float(f64::from_bits(value as u64)),
        ScalarKind::Bool => ScalarValue::Bool(value != 0),
    }
}

fn encode_optional(
    value: Option<ScalarValue>,
    kind: ScalarKind,
    bits: &mut i64,
    tag: &mut i64,
) -> Option<()> {
    match value {
        None => *tag = 0,
        Some(ScalarValue::Deferred) => *tag = 1,
        Some(value) => {
            *bits = encode_present(value, kind)?;
            *tag = 2;
        }
    }
    Some(())
}

fn decode_optional(bits: i64, tag: i64, kind: ScalarKind) -> Option<ScalarValue> {
    match tag {
        0 => None,
        1 => Some(ScalarValue::Deferred),
        2 => Some(decode_present(bits, kind)),
        _ => unreachable!("invalid native temporal-state tag"),
    }
}

fn encode_temporal_external_inputs(
    inputs: &[InputSpec],
    scratch: &mut [i64],
    environment_values: &[Value],
    published_scalars: &[Option<ScalarValue>],
    stream_slots: StreamSlots,
) -> bool {
    for (index, input) in inputs.iter().enumerate() {
        let InputSource::External(slot) = input.source else {
            continue;
        };
        let value = stream_slots
            .stream(slot)
            .and_then(|stream| published_scalars[stream.index()])
            .or_else(|| ScalarValue::from_untyped_value(&environment_values[slot.index()]));
        let Some(value) = value else {
            return false;
        };
        let Some(encoded) = encode_present(value, input.kind) else {
            return false;
        };
        scratch[index] = encoded;
    }
    true
}

impl JittedGraphEvaluator {
    /// Compile every eligible graph in one Cranelift module and finalize executable memory once.
    /// The returned vector is indexed by global `StreamId` and contains `None` for unsupported
    /// graphs, independent of the combined source/main schedule order.
    pub(in crate::dataflow) fn compile_many(
        plan: &ScheduledExecutionPlan,
    ) -> Result<Vec<Option<Self>>, String> {
        let lowered = plan
            .streams
            .iter()
            .map(|stream| Lowering::new().lower(&stream.program.graph, None))
            .collect::<Vec<_>>();
        let compiled = compile_graphs(lowered)?;
        let mut result = vec![None; plan.stream_slots.len()];
        for (planned, compiled) in plan.streams.iter().zip(compiled) {
            result[planned.stream.index()] = compiled.and_then(|compiled| {
                Self::from_compiled(compiled, &planned.program.graph, &planned.temporal)
            });
        }
        Ok(result)
    }

    fn from_compiled(
        compiled: Rc<CompiledGraph>,
        graph: &BoundEvaluationGraph,
        temporal: &TemporalPlan,
    ) -> Option<Self> {
        let input_count = compiled.inputs.len();
        let temporal_plan = ScheduledTemporalPlan::build(temporal, &compiled.boundary_nodes)?;
        Some(Self {
            compiled,
            input_scratch: vec![0; input_count],
            previous_inputs: vec![0; input_count],
            output_scratch: 0,
            has_previous_inputs: false,
            last_tick_native: false,
            disabled: false,
            temporal_plan,
            scheduled_scalars: vec![None; graph.nodes.len()],
            handled_current_tick: false,
            scalar_state_ready: false,
            temporal_kernel_state: None,
            temporal_kernel_completed_tick: false,
        })
    }

    pub(in crate::dataflow) fn is_disabled(&self) -> bool {
        self.disabled
    }

    pub(in crate::dataflow) fn has_scheduled_temporal_state(&self) -> bool {
        self.temporal_plan.has_scheduled_state()
    }

    pub(in crate::dataflow) fn has_complete_temporal_kernel(&self) -> bool {
        self.compiled.temporal_function.is_some()
    }

    #[inline(always)]
    pub(in crate::dataflow) fn evaluate(
        &mut self,
        graph: &BoundEvaluationGraph,
        state: &mut StreamState,
        environment_values: &[Value],
        environment_layout: &Rc<EnvironmentLayout>,
        published_scalars: &[Option<ScalarValue>],
        stream_slots: StreamSlots,
        allow_complete_temporal_kernel: bool,
    ) -> Option<Value> {
        self.handled_current_tick = false;
        self.temporal_kernel_completed_tick = false;
        self.last_tick_native = false;
        if self.disabled {
            return None;
        }

        if self.compiled.temporal_function.is_some() {
            if !allow_complete_temporal_kernel {
                return None;
            }
            return self.evaluate_temporal_kernel(
                graph,
                state,
                environment_values,
                environment_layout,
                published_scalars,
                stream_slots,
            );
        }

        let current_context = EvaluationContext {
            environment_values,
            environment_layout,
            retained_environment_values: None,
            recursive_call: None,
        };
        if !self.scalar_state_ready {
            self.scalar_state_ready = self.temporal_plan.promote(state);
        }
        if self.scalar_state_ready {
            if !self
                .temporal_plan
                .evaluate(state, current_context, &mut self.scheduled_scalars)
            {
                self.temporal_plan.deopt(state);
                self.scalar_state_ready = false;
                for &node in self.compiled.boundary_nodes.iter() {
                    let value =
                        evaluate_node(node, &graph.nodes[node.index()], state, current_context);
                    self.scheduled_scalars[node.index()] = ScalarValue::from_untyped_value(&value);
                    state.node_values[node.index()] = value;
                }
            }
        } else {
            for &node in self.compiled.boundary_nodes.iter() {
                let value = evaluate_node(node, &graph.nodes[node.index()], state, current_context);
                self.scheduled_scalars[node.index()] = ScalarValue::from_untyped_value(&value);
                state.node_values[node.index()] = value;
            }
        }
        self.handled_current_tick = true;

        for (index, input) in self.compiled.inputs.iter().enumerate() {
            if let InputSource::External(slot) = input.source
                && let Some(stream) = stream_slots.stream(slot)
                && let Some(value) = published_scalars[stream.index()]
            {
                let encoded = match (value, input.kind) {
                    (ScalarValue::Int(value), ScalarKind::Int) => value,
                    (ScalarValue::Float(value), ScalarKind::Float) => value.to_bits() as i64,
                    (ScalarValue::Bool(value), ScalarKind::Bool) => i64::from(value),
                    (ScalarValue::NoVal | ScalarValue::Deferred, _) => {
                        return Some(self.fallback_current(
                            graph,
                            state,
                            environment_values,
                            environment_layout,
                            false,
                        ));
                    }
                    _ => {
                        return Some(self.fallback_current(
                            graph,
                            state,
                            environment_values,
                            environment_layout,
                            true,
                        ));
                    }
                };
                self.input_scratch[index] = encoded;
                continue;
            }
            let value = match input.source {
                InputSource::External(slot) => &environment_values[slot.index()],
                InputSource::Node(node) => {
                    if let Some(value) = self.scheduled_scalars[node.index()] {
                        let encoded = match (value, input.kind) {
                            (ScalarValue::Int(value), ScalarKind::Int) => value,
                            (ScalarValue::Float(value), ScalarKind::Float) => {
                                value.to_bits() as i64
                            }
                            (ScalarValue::Bool(value), ScalarKind::Bool) => i64::from(value),
                            (ScalarValue::NoVal | ScalarValue::Deferred, _) => {
                                return Some(self.fallback_current(
                                    graph,
                                    state,
                                    environment_values,
                                    environment_layout,
                                    false,
                                ));
                            }
                            _ => {
                                return Some(self.fallback_current(
                                    graph,
                                    state,
                                    environment_values,
                                    environment_layout,
                                    true,
                                ));
                            }
                        };
                        self.input_scratch[index] = encoded;
                        continue;
                    }
                    &state.node_values[node.index()]
                }
            };
            let encoded = match (value, input.kind) {
                (Value::Int(value), ScalarKind::Int) => *value,
                (Value::Float(value), ScalarKind::Float) => value.to_bits() as i64,
                (Value::Bool(value), ScalarKind::Bool) => i64::from(*value),
                (Value::NoVal | Value::Deferred, _) => {
                    // Presence is a per-tick property, not evidence that the graph's checked
                    // scalar types were wrong. Reconstruct canonical lift state for this tick,
                    // then resume the native path when concrete values return.
                    return Some(self.fallback_current(
                        graph,
                        state,
                        environment_values,
                        environment_layout,
                        false,
                    ));
                }
                _ => {
                    return Some(self.fallback_current(
                        graph,
                        state,
                        environment_values,
                        environment_layout,
                        true,
                    ));
                }
            };
            self.input_scratch[index] = encoded;
        }

        // SAFETY: compilation creates this exact two-pointer ABI. Both buffers have the sizes
        // recorded in `CompiledGraph`, and the owning JITModule stays alive through `Rc`.
        let function = self
            .compiled
            .function
            .expect("non-temporal compiled graphs have a scalar function");
        let status = unsafe {
            function(
                self.input_scratch.as_ptr(),
                std::ptr::from_mut(&mut self.output_scratch),
            )
        };
        if status != STATUS_OK {
            return Some(self.fallback_current(
                graph,
                state,
                environment_values,
                environment_layout,
                true,
            ));
        }

        // The next tick overwrites its input buffer, so swapping retains this row for a possible
        // later deoptimization without copying it on every successful native tick.
        std::mem::swap(&mut self.previous_inputs, &mut self.input_scratch);
        self.has_previous_inputs = true;
        self.last_tick_native = true;
        Some(decode(self.output_scratch, self.compiled.output_kind))
    }

    #[inline(always)]
    fn evaluate_temporal_kernel(
        &mut self,
        graph: &BoundEvaluationGraph,
        state: &mut StreamState,
        environment_values: &[Value],
        environment_layout: &Rc<EnvironmentLayout>,
        published_scalars: &[Option<ScalarValue>],
        stream_slots: StreamSlots,
    ) -> Option<Value> {
        let function = self.compiled.temporal_function.unwrap();
        let layout = self.compiled.temporal_state.as_ref().unwrap();
        if !self.scalar_state_ready {
            self.scalar_state_ready = self.temporal_plan.promote(state);
        }
        if self.scalar_state_ready && self.temporal_kernel_state.is_none() {
            self.temporal_kernel_state = NativeTemporalState::promote(layout, state);
        }
        if self.temporal_kernel_state.is_some()
            && encode_temporal_external_inputs(
                &self.compiled.inputs,
                &mut self.input_scratch,
                environment_values,
                published_scalars,
                stream_slots,
            )
        {
            let native_state = self.temporal_kernel_state.as_mut().unwrap();
            // SAFETY: the backend fixes this three-pointer ABI and the state/input/output buffers
            // are allocated from the compiled layout retained by `CompiledGraph`.
            let status = unsafe {
                function(
                    self.input_scratch.as_mut_ptr(),
                    native_state.cells.as_mut_ptr(),
                    std::ptr::from_mut(&mut self.output_scratch),
                )
            };
            if status == STATUS_OK {
                std::mem::swap(&mut self.previous_inputs, &mut self.input_scratch);
                self.has_previous_inputs = true;
                self.last_tick_native = true;
                self.handled_current_tick = true;
                self.temporal_kernel_completed_tick = true;
                return Some(decode(self.output_scratch, self.compiled.output_kind));
            }
        }

        // A side exit occurs before any native state write. Complete kernels deliberately do not
        // compile a duplicate scalar artifact: reconstruct the interpreter state from the last
        // successful row and continue this tick in the shared canonical tier.
        if let Some(native_state) = self.temporal_kernel_state.take() {
            native_state.materialize(layout, state);
        }
        self.temporal_plan.deopt(state);
        self.scalar_state_ready = false;
        self.replay_previous(graph, state, environment_values, environment_layout);
        self.disabled = true;
        None
    }

    pub(in crate::dataflow) fn commit(
        &mut self,
        state: &mut StreamState,
        environment_values: &[Value],
        environment_layout: &Rc<EnvironmentLayout>,
        retained_environment_values: Option<&[Value]>,
    ) -> bool {
        if self.temporal_kernel_completed_tick {
            self.temporal_kernel_completed_tick = false;
            self.handled_current_tick = false;
            return true;
        }
        if !self.handled_current_tick
            || !self.scalar_state_ready
            || !self.temporal_plan.has_temporal_state()
        {
            return false;
        }
        self.handled_current_tick = false;
        let committed = self.temporal_plan.commit(
            state,
            EvaluationContext {
                environment_values,
                environment_layout,
                retained_environment_values,
                recursive_call: None,
            },
            &self.scheduled_scalars,
        );
        self.scalar_state_ready &= committed;
        committed
    }

    pub(in crate::dataflow) fn reset_after_context_transfer(&mut self) {
        self.input_scratch.fill(0);
        self.previous_inputs.fill(0);
        self.output_scratch = 0;
        self.has_previous_inputs = false;
        self.last_tick_native = false;
        self.disabled = false;
        self.scheduled_scalars.fill(None);
        self.handled_current_tick = false;
        self.scalar_state_ready = false;
        self.temporal_kernel_state = None;
        self.temporal_kernel_completed_tick = false;
    }

    /// Materialize native state into a temporary snapshot without changing this active artifact.
    pub(in crate::dataflow) fn snapshot_into(&self, evaluator: &mut StreamEvaluator) {
        {
            let state = &mut evaluator.state;
            if let Some(native_state) = &self.temporal_kernel_state {
                let layout = self.compiled.temporal_state.as_ref().unwrap();
                native_state.clone().materialize(layout, state);
                self.temporal_plan.deopt(state);
            } else if self.scalar_state_ready {
                self.temporal_plan
                    .materialize(state, &self.scheduled_scalars);
                self.temporal_plan.deopt(state);
            }
        }
        if self.last_tick_native && self.has_previous_inputs {
            self.replay_previous_canonical(evaluator);
        }
    }

    fn replay_previous_canonical(&self, evaluator: &mut StreamEvaluator) {
        let mut environment = vec![Value::NoVal; evaluator.program.environment_layout.len()];
        for (index, input) in self.compiled.inputs.iter().enumerate() {
            let value = decode(self.previous_inputs[index], input.kind);
            match input.source {
                InputSource::External(slot) => environment[slot.index()] = value,
                InputSource::Node(node) => evaluator.state.node_values[node.index()] = value,
            }
        }
        evaluator.evaluate_canonical_infallible(&environment);
        // This is a replay of the already committed native row, not another logical tick.
        evaluator.discard_staged_temporal_state();
    }

    fn fallback_current(
        &mut self,
        graph: &BoundEvaluationGraph,
        state: &mut StreamState,
        current_environment: &[Value],
        environment_layout: &Rc<EnvironmentLayout>,
        permanent: bool,
    ) -> Value {
        self.last_tick_native = false;
        self.disabled |= permanent;
        if self.scalar_state_ready {
            self.temporal_plan
                .materialize(state, &self.scheduled_scalars);
            if permanent {
                self.temporal_plan.deopt(state);
                self.scalar_state_ready = false;
            }
        }
        let current_boundaries = self
            .compiled
            .boundary_nodes
            .iter()
            .map(|node| (*node, state.node_values[node.index()].clone()))
            .collect::<Vec<_>>();
        self.replay_previous(graph, state, current_environment, environment_layout);
        for (node, value) in current_boundaries {
            state.node_values[node.index()] = value;
        }
        let context = EvaluationContext {
            environment_values: current_environment,
            environment_layout,
            retained_environment_values: None,
            recursive_call: None,
        };
        self.evaluate_non_boundary(graph, state, context);
        context.read_value(state, &graph.output)
    }

    fn replay_previous(
        &mut self,
        graph: &BoundEvaluationGraph,
        state: &mut StreamState,
        current_environment: &[Value],
        environment_layout: &Rc<EnvironmentLayout>,
    ) {
        if !self.has_previous_inputs {
            return;
        }

        let mut replay_environment = current_environment.to_vec();
        for (index, input) in self.compiled.inputs.iter().enumerate() {
            let value = decode(self.previous_inputs[index], input.kind);
            match input.source {
                InputSource::External(slot) => replay_environment[slot.index()] = value,
                InputSource::Node(node) => state.node_values[node.index()] = value,
            }
        }
        let context = EvaluationContext {
            environment_values: &replay_environment,
            environment_layout,
            retained_environment_values: None,
            recursive_call: None,
        };
        self.evaluate_non_boundary(graph, state, context);
    }

    fn evaluate_non_boundary(
        &self,
        graph: &BoundEvaluationGraph,
        state: &mut StreamState,
        context: EvaluationContext<'_>,
    ) {
        if self.compiled.boundary_nodes.is_empty() {
            evaluate_nodes(&graph.nodes, state, context);
            return;
        }
        for (index, op) in graph.nodes.iter().enumerate() {
            let node = NodeId::new(index);
            if self.compiled.boundary_nodes.binary_search(&node).is_ok() {
                continue;
            }
            state.node_values[index] = evaluate_node(node, op, state, context);
        }
    }
}
