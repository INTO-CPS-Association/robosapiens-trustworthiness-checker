use crate::dataflow::environment::EnvironmentSlot;

use crate::dataflow::execution::quickening::ScalarValue;
use crate::dataflow::execution::scheduled_plan::ScheduledExecutionPlan;

use crate::dataflow::ir::ScalarKind;
use crate::dataflow::*;

use super::super::backend::{InputSource, InputSpec, TemporalRunStateLayout, decode};

#[derive(Clone)]
enum ValueIoLayout {
    OneInputOneOutput {
        input_slot: usize,
        output_slot: usize,
    },
    TwoInputsOneOutput {
        first_input_slot: usize,
        second_input_slot: usize,
        output_slot: usize,
    },
    Generic {
        input_slots: Box<[usize]>,
        output_slots: Box<[usize]>,
    },
}

#[derive(Clone)]
pub(super) struct ValueRegionAdapter {
    io_layout: ValueIoLayout,
    expected_input_len: usize,
    expected_output_len: usize,
}

impl ValueRegionAdapter {
    pub(super) fn install(
        external_inputs: &[InputSpec],
        outputs: &[(EnvironmentSlot, ScalarKind, usize)],
        environment_len: usize,
    ) -> Option<Self> {
        let (input_slots, output_slots) = value_slot_indices(external_inputs, outputs)?;
        let io_layout =
            classify_value_io_layout(&input_slots, &output_slots, environment_len).ok()?;
        Some(Self {
            expected_input_len: input_slots.len(),
            expected_output_len: output_slots.len(),
            io_layout,
        })
    }

    pub(super) fn expected_input_len(&self) -> usize {
        self.expected_input_len
    }

    pub(super) fn expected_output_len(&self) -> usize {
        self.expected_output_len
    }

    pub(super) fn slots_within(&self, environment_len: usize) -> bool {
        match &self.io_layout {
            ValueIoLayout::OneInputOneOutput {
                input_slot,
                output_slot,
            } => *input_slot < environment_len && *output_slot < environment_len,
            ValueIoLayout::TwoInputsOneOutput {
                first_input_slot,
                second_input_slot,
                output_slot,
            } => {
                *first_input_slot < environment_len
                    && *second_input_slot < environment_len
                    && *output_slot < environment_len
            }
            ValueIoLayout::Generic {
                input_slots,
                output_slots,
            } => input_slots
                .iter()
                .chain(output_slots.iter())
                .all(|&slot| slot < environment_len),
        }
    }

    pub(super) fn copy_inputs(&self, scratch: &mut [i64], input: &[i64]) {
        copy_value_inputs(scratch, input, &self.io_layout);
    }
}

fn value_slot_indices(
    external_inputs: &[InputSpec],
    outputs: &[(EnvironmentSlot, ScalarKind, usize)],
) -> Option<(Box<[usize]>, Box<[usize]>)> {
    let input_slots = external_inputs
        .iter()
        .map(|spec| match spec.source {
            InputSource::External(slot) => Some(slot.index()),
            InputSource::Node(_) => None,
        })
        .collect::<Option<Vec<_>>>()
        .map(Vec::into_boxed_slice)?;
    let output_slots = outputs
        .iter()
        .map(|(slot, _, _)| slot.index())
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Some((input_slots, output_slots))
}

fn classify_value_io_layout(
    input_slots: &[usize],
    output_slots: &[usize],
    environment_len: usize,
) -> Result<ValueIoLayout, ()> {
    if input_slots
        .iter()
        .chain(output_slots.iter())
        .any(|&slot| slot >= environment_len)
    {
        return Err(());
    }

    match (input_slots.len(), output_slots.len()) {
        (1, 1) => Ok(ValueIoLayout::OneInputOneOutput {
            input_slot: input_slots[0],
            output_slot: output_slots[0],
        }),
        (2, 1) => Ok(ValueIoLayout::TwoInputsOneOutput {
            first_input_slot: input_slots[0],
            second_input_slot: input_slots[1],
            output_slot: output_slots[0],
        }),
        _ => Ok(ValueIoLayout::Generic {
            input_slots: input_slots.to_vec().into_boxed_slice(),
            output_slots: output_slots.to_vec().into_boxed_slice(),
        }),
    }
}

pub(super) fn value_adapter_temporal_state_matches_plan(
    states: &[TemporalRunStateLayout],
    plan: &ScheduledExecutionPlan,
) -> bool {
    states
        .iter()
        .all(|state| state.stream < plan.stream_slots.len())
        && plan
            .commit_streams
            .iter()
            .all(|stream| states.iter().any(|state| state.stream == stream.index()))
}

#[inline(always)]
fn copy_value_inputs(scratch: &mut [i64], input: &[i64], layout: &ValueIoLayout) {
    debug_assert!(
        input.len()
            == match layout {
                ValueIoLayout::OneInputOneOutput { .. } => 1,
                ValueIoLayout::TwoInputsOneOutput { .. } => 2,
                ValueIoLayout::Generic { input_slots, .. } => input_slots.len(),
            }
    );
    // SAFETY: ValueRegionAdapter::install validates every environment slot, while the Value adapter
    // validates slice lengths before reaching this helper.
    match layout {
        ValueIoLayout::OneInputOneOutput { input_slot, .. } => unsafe {
            *scratch.get_unchecked_mut(*input_slot) = *input.get_unchecked(0);
        },
        ValueIoLayout::TwoInputsOneOutput {
            first_input_slot,
            second_input_slot,
            ..
        } => unsafe {
            *scratch.get_unchecked_mut(*first_input_slot) = *input.get_unchecked(0);
            *scratch.get_unchecked_mut(*second_input_slot) = *input.get_unchecked(1);
        },
        ValueIoLayout::Generic { input_slots, .. } => {
            for index in 0..input_slots.len() {
                unsafe {
                    *scratch.get_unchecked_mut(*input_slots.get_unchecked(index)) =
                        *input.get_unchecked(index);
                }
            }
        }
    }
}

pub(super) fn replay_environment(
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

pub(super) fn encode_present(value: ScalarValue, kind: ScalarKind) -> Option<i64> {
    match (value, kind) {
        (ScalarValue::Int(value), ScalarKind::Int) => Some(value),
        (ScalarValue::Float(value), ScalarKind::Float) => Some(value.to_bits() as i64),
        (ScalarValue::Bool(value), ScalarKind::Bool) => Some(i64::from(value)),
        _ => None,
    }
}

pub(super) fn decode_present(value: i64, kind: ScalarKind) -> ScalarValue {
    match kind {
        ScalarKind::Int => ScalarValue::Int(value),
        ScalarKind::Float => ScalarValue::Float(f64::from_bits(value as u64)),
        ScalarKind::Bool => ScalarValue::Bool(value != 0),
    }
}

pub(super) fn encode_optional(
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

pub(super) fn decode_optional(bits: i64, tag: i64, kind: ScalarKind) -> Option<ScalarValue> {
    match tag {
        0 => None,
        1 => Some(ScalarValue::Deferred),
        2 => Some(decode_present(bits, kind)),
        _ => unreachable!("invalid native temporal-state tag"),
    }
}
