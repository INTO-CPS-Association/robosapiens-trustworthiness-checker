use crate::dataflow::typed::TypedIoLayout;

use crate::dataflow::execution::evaluator::Evaluator;
use crate::dataflow::execution::evaluator_state::{EvaluatorState, NodeState};
use crate::dataflow::execution::quickening::ScalarValue;
use crate::dataflow::execution::scheduled_plan::ScheduledExecutionPlan;

use crate::dataflow::history::HistoryAccess;

use crate::dataflow::*;

use super::super::backend::{
    CompiledTemporalMonitor, InputSource, TemporalNodeLayout, TemporalStateLayout,
    ValueTemporalFunction, compile_temporal_monitor, decode,
};
use super::super::scheduled_state::ScheduledTemporalPlan;
use super::value_adapter::{
    ValueRegionAdapter, decode_optional, decode_present, encode_optional, encode_present,
    replay_environment, value_adapter_temporal_state_matches_plan,
};
use super::{NativeRunOutcome, PreparedDirectJit};

pub(in crate::dataflow) struct NativeTemporalMonitor {
    compiled: CompiledTemporalMonitor,
    value_adapter: ValueRegionAdapter,
    input_scratch: Vec<i64>,
    output_scratch: Vec<i64>,
    previous_environment: Vec<i64>,
    temporal_state: Vec<i64>,
    temporal_plans: Box<[Option<ScheduledTemporalPlan>]>,
    state_ready: bool,
    has_previous: bool,
    last_tick_native: bool,
    disabled: bool,
}

impl NativeTemporalMonitor {
    pub(in crate::dataflow) fn has_direct_entry(&self) -> bool {
        self.compiled.direct_function.is_some() && !self.disabled
    }

    pub(in crate::dataflow) fn compile(
        plan: &ScheduledExecutionPlan,
        direct_layout: Option<&TypedIoLayout>,
    ) -> Result<Option<Self>, String> {
        if plan.has_source_barrier() {
            return Ok(None);
        }
        let Some(compiled) = compile_temporal_monitor(plan, direct_layout)? else {
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
        let value_adapter = ValueRegionAdapter::install(
            &compiled.external_inputs,
            &compiled.outputs,
            compiled.environment_len,
        )
        .filter(|_| value_adapter_temporal_state_matches_plan(&compiled.states, plan))
        .ok_or_else(|| "compiled temporal value adapter ABI metadata mismatch".to_owned())?;
        let evaluator = Self {
            input_scratch: vec![0; value_adapter.expected_input_len()],
            output_scratch: vec![0; value_adapter.expected_output_len()],
            value_adapter,
            previous_environment: vec![0; compiled.environment_len],
            temporal_state: vec![0; compiled.state_len],
            temporal_plans: temporal_plans.into_boxed_slice(),
            compiled,
            state_ready: false,
            has_previous: false,
            last_tick_native: false,
            disabled: false,
        };
        debug_assert!(evaluator.value_adapter_invariants_hold());
        Ok(Some(evaluator))
    }

    pub(in crate::dataflow) fn into_prepared_direct(
        mut self,
        evaluators: &mut [Evaluator],
        history_access: Option<HistoryAccess<'_>>,
    ) -> Result<PreparedDirectJit, Self> {
        let Some(function) = self.compiled.direct_function else {
            return Err(self);
        };
        if self.disabled || (!self.state_ready && !self.promote(evaluators, history_access)) {
            return Err(self);
        }
        debug_assert!(self.state_ready && !self.disabled);
        Ok(PreparedDirectJit::temporal(self, function))
    }

    pub(super) fn temporal_state_ptr(&mut self) -> *mut i64 {
        self.temporal_state.as_mut_ptr()
    }

    #[inline(always)]
    pub(in crate::dataflow) fn evaluate(
        &mut self,
        evaluators: &mut [Evaluator],
        environment_values: &mut [Value],
        history_access: Option<HistoryAccess<'_>>,
    ) -> NativeRunOutcome {
        self.last_tick_native = false;
        if self.disabled {
            return NativeRunOutcome::Fallback {
                replay_environment: None,
            };
        }
        if !self.state_ready && !self.promote(evaluators, history_access) {
            self.disabled = true;
            return NativeRunOutcome::Fallback {
                replay_environment: None,
            };
        }

        for (index, input) in self.compiled.external_inputs.iter().enumerate() {
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
            self.input_scratch[index] = value;
        }
        // SAFETY: compilation and value adapter metadata installation fix the three-slice temporal ABI,
        // and the owning module and packed state remain alive for the evaluator's lifetime.
        let status = unsafe {
            match self.compiled.value_function {
                ValueTemporalFunction::Void(function) => {
                    function(
                        self.input_scratch.as_ptr(),
                        self.output_scratch.as_mut_ptr(),
                        self.temporal_state.as_mut_ptr(),
                    );
                    0
                }
                ValueTemporalFunction::Checked(function) => function(
                    self.input_scratch.as_ptr(),
                    self.output_scratch.as_mut_ptr(),
                    self.temporal_state.as_mut_ptr(),
                ),
            }
        };
        if status != 0 {
            return self.fallback(evaluators);
        }
        for (index, &(slot, kind, _)) in self.compiled.outputs.iter().enumerate() {
            environment_values[slot.index()] = decode(self.output_scratch[index], kind);
        }
        self.value_adapter
            .copy_inputs(&mut self.previous_environment, &self.input_scratch);
        self.has_previous = true;
        self.last_tick_native = true;
        NativeRunOutcome::Completed
    }

    fn promote(
        &mut self,
        evaluators: &mut [Evaluator],
        history_access: Option<HistoryAccess<'_>>,
    ) -> bool {
        for (index, layout) in self.compiled.states.iter().enumerate() {
            let temporal_plan = self.temporal_plans[layout.stream].as_ref().unwrap();
            let state = evaluators[layout.stream].state_mut();
            if !temporal_plan.promote(state, history_access) {
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

    pub(in crate::dataflow) fn snapshot_into(&self, evaluators: &mut [Evaluator]) {
        if !self.state_ready {
            return;
        }
        for layout in self.compiled.states.iter() {
            let end = layout.offset + layout.layout.len;
            let native = NativeTemporalState {
                cells: self.temporal_state[layout.offset..end].to_vec(),
            };
            let state = evaluators[layout.stream].state_mut();
            native.materialize(&layout.layout, state);
            self.temporal_plans[layout.stream]
                .as_ref()
                .unwrap()
                .deopt(state);
        }
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

    pub(in crate::dataflow) fn reset_after_context_transfer(&mut self) {
        self.input_scratch.fill(0);
        self.output_scratch.fill(0);
        self.previous_environment.fill(0);
        self.temporal_state.fill(0);
        self.state_ready = false;
        self.has_previous = false;
        self.last_tick_native = false;
        self.disabled = false;
    }

    fn deopt_promoted(&self, evaluators: &mut [Evaluator], count: usize) {
        for layout in self.compiled.states[..count].iter() {
            self.temporal_plans[layout.stream]
                .as_ref()
                .unwrap()
                .deopt(evaluators[layout.stream].state_mut());
        }
    }

    fn value_adapter_invariants_hold(&self) -> bool {
        self.input_scratch.len() == self.compiled.external_inputs.len()
            && self.output_scratch.len() == self.compiled.outputs.len()
            && self.previous_environment.len() == self.compiled.environment_len
            && self.temporal_state.len() == self.compiled.state_len
            && self.value_adapter.expected_input_len() == self.compiled.external_inputs.len()
            && self.value_adapter.expected_output_len() == self.compiled.outputs.len()
            && self
                .value_adapter
                .slots_within(self.compiled.environment_len)
            && self.compiled.states.iter().all(|state| {
                state
                    .offset
                    .checked_add(state.layout.len)
                    .is_some_and(|end| end <= self.temporal_state.len())
            })
    }

    fn fallback(&mut self, evaluators: &mut [Evaluator]) -> NativeRunOutcome {
        self.last_tick_native = false;
        for layout in self.compiled.states.iter() {
            let end = layout.offset + layout.layout.len;
            let native = NativeTemporalState {
                cells: self.temporal_state[layout.offset..end].to_vec(),
            };
            let state = evaluators[layout.stream].state_mut();
            native.materialize(&layout.layout, state);
            self.temporal_plans[layout.stream]
                .as_ref()
                .unwrap()
                .deopt(state);
        }
        self.disabled = true;
        self.state_ready = false;
        NativeRunOutcome::Fallback {
            replay_environment: self.has_previous.then(|| {
                replay_environment(
                    &self.compiled.external_inputs,
                    &self.previous_environment,
                    self.compiled.environment_len,
                )
            }),
        }
    }
}

#[derive(Clone)]
struct NativeTemporalState {
    cells: Vec<i64>,
}

impl NativeTemporalState {
    fn promote(layout: &TemporalStateLayout, state: &EvaluatorState) -> Option<Self> {
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

    fn materialize(&self, layout: &TemporalStateLayout, state: &mut EvaluatorState) {
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
