use crate::dataflow::typed::TypedIoLayout;

use crate::dataflow::execution::evaluator::Evaluator;

use crate::dataflow::execution::scalar_region::ScalarRegion;
use crate::dataflow::execution::scheduled_plan::ScheduledExecutionPlan;
use crate::dataflow::ir::ScalarKind;
use crate::dataflow::stream_id::StreamSlots;
use crate::dataflow::*;

use super::super::backend::{
    CompiledScalarRegion, InputSource, ValueScalarFunction, compile_scalar_region, decode,
};
use super::value_adapter::{ValueRegionAdapter, replay_environment};
use super::{NativeRunOutcome, PreparedDirectJit};

pub(in crate::dataflow) struct NativeScalarRegion {
    compiled: CompiledScalarRegion,
    value_adapter: ValueRegionAdapter,
    input_scratch: Vec<i64>,
    output_scratch: Vec<i64>,
    previous_environment: Vec<i64>,
    has_previous: bool,
    last_tick_native: bool,
    disabled: bool,
}

impl NativeScalarRegion {
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
        let Some(region) = ScalarRegion::new(plan) else {
            return Ok(None);
        };
        Self::compile_region(
            &region,
            plan.stream_slots,
            plan.environment_len,
            direct_layout,
        )
    }

    pub(in crate::dataflow) fn compile_region(
        region: &ScalarRegion,
        stream_slots: StreamSlots,
        environment_len: usize,
        direct_layout: Option<&TypedIoLayout>,
    ) -> Result<Option<Self>, String> {
        let Some(compiled) = compile_scalar_region(region, stream_slots, direct_layout)? else {
            return Ok(None);
        };
        let value_adapter = ValueRegionAdapter::install(
            &compiled.external_inputs,
            &compiled.outputs,
            environment_len,
        )
        .ok_or_else(|| "compiled scalar value adapter ABI metadata mismatch".to_owned())?;
        let evaluator = Self {
            input_scratch: vec![0; value_adapter.expected_input_len()],
            output_scratch: vec![0; value_adapter.expected_output_len()],
            value_adapter,
            previous_environment: vec![0; environment_len],
            compiled,
            has_previous: false,
            last_tick_native: false,
            disabled: false,
        };
        debug_assert!(evaluator.value_adapter_invariants_hold());
        Ok(Some(evaluator))
    }

    pub(in crate::dataflow) fn into_prepared_direct(self) -> Result<PreparedDirectJit, Self> {
        let Some(function) = self.compiled.direct_function else {
            return Err(self);
        };
        Ok(PreparedDirectJit::scalar(self, function))
    }

    #[inline(always)]
    pub(in crate::dataflow) fn evaluate(
        &mut self,
        environment_values: &mut [Value],
    ) -> NativeRunOutcome {
        self.last_tick_native = false;
        if self.disabled {
            return NativeRunOutcome::Fallback {
                replay_environment: None,
            };
        }
        for (index, input) in self.compiled.external_inputs.iter().enumerate() {
            let InputSource::External(slot) = input.source else {
                unreachable!("scalar regions cannot contain canonical boundary inputs")
            };
            self.input_scratch[index] = match (&environment_values[slot.index()], input.kind) {
                (Value::Int(value), ScalarKind::Int) => *value,
                (Value::Float(value), ScalarKind::Float) => value.to_bits() as i64,
                (Value::Bool(value), ScalarKind::Bool) => i64::from(*value),
                (Value::NoVal | Value::Deferred, _) => return self.fallback(false),
                _ => return self.fallback(true),
            };
        }

        // SAFETY: compilation and value adapter metadata installation fix the two-slice ABI and retain
        // the owning module for the evaluator's lifetime.
        let status = unsafe {
            match self.compiled.value_function {
                ValueScalarFunction::Void(function) => {
                    function(
                        self.input_scratch.as_ptr(),
                        self.output_scratch.as_mut_ptr(),
                    );
                    0
                }
                ValueScalarFunction::Checked(function) => function(
                    self.input_scratch.as_ptr(),
                    self.output_scratch.as_mut_ptr(),
                ),
            }
        };
        if status != 0 {
            return self.fallback(true);
        }
        for (index, &(slot, kind, _)) in self.compiled.outputs.iter().enumerate() {
            environment_values[slot.index()] = decode(self.output_scratch[index], kind);
        }
        self.save_previous_inputs();
        self.last_tick_native = true;
        NativeRunOutcome::Completed
    }

    /// Restores canonical node and lifting state for the streams this region owns.
    ///
    /// The native kernel keeps no canonical retention, so the last row it ran is replayed through
    /// the canonical evaluators of exactly this region's streams, in publication order, so an
    /// intra-region operand resolves from the replayed publication rather than the saved inputs.
    /// This is the same reconstruction the whole-schedule tiers use; there is no separate scalar
    /// evaluator for it. Returns `false` when no native row has run, which leaves canonical state
    /// untouched because the region never took it over.
    pub(in crate::dataflow) fn materialize_into(&mut self, evaluators: &mut [Evaluator]) -> bool {
        if !self.has_previous {
            return false;
        }
        let replayed = self.replay_into(evaluators, self.replay_previous_environment());
        if replayed {
            self.has_previous = false;
            self.last_tick_native = false;
        }
        replayed
    }

    /// Apply a consumed native replay row to this region's canonical stream owners.
    pub(in crate::dataflow) fn replay_into(
        &self,
        evaluators: &mut [Evaluator],
        mut environment: Vec<Value>,
    ) -> bool {
        for &(slot, _, stream) in self.compiled.outputs.iter() {
            let Some(evaluator) = evaluators.get_mut(stream) else {
                return false;
            };
            let value = evaluator.evaluate_static_and_stage(&environment, None);
            // The replayed row was already committed natively, so it must not stage another
            // logical tick. Scalar regions carry no temporal state, making this a guard rather
            // than a correction.
            evaluator.discard_staged_temporal_state();
            environment[slot.index()] = value;
        }
        true
    }

    pub(in crate::dataflow) fn snapshot_replay_environment(&self) -> Option<Vec<Value>> {
        self.last_tick_native
            .then(|| self.replay_previous_environment())
    }

    pub(in crate::dataflow) fn reset_after_context_transfer(&mut self) {
        self.input_scratch.fill(0);
        self.output_scratch.fill(0);
        self.previous_environment.fill(0);
        self.has_previous = false;
        self.last_tick_native = false;
        self.disabled = false;
    }

    fn save_previous_inputs(&mut self) {
        self.value_adapter
            .copy_inputs(&mut self.previous_environment, &self.input_scratch);
        self.has_previous = true;
    }

    fn value_adapter_invariants_hold(&self) -> bool {
        self.input_scratch.len() == self.compiled.external_inputs.len()
            && self.output_scratch.len() == self.compiled.outputs.len()
            && self.previous_environment.len() == self.compiled.environment_len
            && self.value_adapter.expected_input_len() == self.compiled.external_inputs.len()
            && self.value_adapter.expected_output_len() == self.compiled.outputs.len()
            && self
                .value_adapter
                .slots_within(self.compiled.environment_len)
    }

    fn replay_previous_environment(&self) -> Vec<Value> {
        replay_environment(
            &self.compiled.external_inputs,
            &self.previous_environment,
            self.compiled.environment_len,
        )
    }

    fn fallback(&mut self, disable: bool) -> NativeRunOutcome {
        self.last_tick_native = false;
        self.disabled |= disable;
        let replay_environment = self
            .has_previous
            .then(|| self.replay_previous_environment());
        // Canonical execution owns retention after this handoff. Replaying the old dense row
        // again on a second sparse tick would overwrite the intervening canonical updates.
        self.has_previous = false;
        NativeRunOutcome::Fallback { replay_environment }
    }
}
