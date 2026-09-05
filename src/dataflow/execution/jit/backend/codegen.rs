#[cfg(test)]
use std::cell::Cell;
use std::collections::BTreeMap;

use cranelift_codegen::ir::condcodes::{FloatCC, IntCC};
use cranelift_codegen::ir::immediates::Ieee64;
use cranelift_codegen::ir::{self, AbiParam, InstBuilder, MemFlagsData, types};
use cranelift_codegen::settings::{self, Configurable};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{FuncId, Linkage, Module};

use crate::core::{BinaryOperator, UnaryOperator};
use crate::dataflow::environment::EnvironmentSlot;
use crate::dataflow::execution::scalar_region::ScalarRegion;
use crate::dataflow::execution::scheduled_plan::{PlannedStream, ScheduledExecutionPlan};
use crate::dataflow::ir::ScalarKind;
use crate::dataflow::stream_id::StreamSlots;
use crate::dataflow::typed::{TypedBoundField, TypedIoLayout, TypedKind};

use super::artifact::{
    CheckedDirectRunFn, CheckedValueRunFn, CheckedValueTemporalRunFn, CompiledScalarRegion,
    CompiledTemporalMonitor, DirectFunction, DirectRunFn, InputSource, InputSpec,
    TemporalRunStateLayout, ValueRunFn, ValueScalarFunction, ValueTemporalFunction,
    ValueTemporalRunFn,
};
use super::ir::{
    IntegerDivision, LoweredGraph, LoweredNode, LoweredProgram, ScalarRef, TemporalOp,
    TemporalProgram, TemporalSource,
};
use super::lowering::Lowering;

#[derive(Clone, Copy)]
struct TemporalValue {
    value: NativeValue,
    available: ir::Value,
}

fn read_temporal_source(
    builder: &mut FunctionBuilder<'_>,
    source: TemporalSource,
    inputs: &[Option<NativeValue>],
    output: Option<NativeValue>,
) -> NativeValue {
    match source {
        TemporalSource::Constant { bits, kind } => NativeValue {
            value: match kind {
                ScalarKind::Float => builder.ins().f64const(Ieee64::with_bits(bits as u64)),
                ScalarKind::Int | ScalarKind::Bool => builder.ins().iconst(types::I64, bits),
            },
            kind,
        },
        TemporalSource::Input { index } => inputs[index as usize].unwrap(),
        TemporalSource::Output => output.unwrap(),
    }
}

fn load_i64(builder: &mut FunctionBuilder<'_>, base: ir::Value, index: usize) -> ir::Value {
    builder.ins().load(
        types::I64,
        MemFlagsData::trusted(),
        base,
        (index as i32) * 8,
    )
}

fn store_i64(builder: &mut FunctionBuilder<'_>, base: ir::Value, index: usize, value: ir::Value) {
    builder
        .ins()
        .store(MemFlagsData::trusted(), value, base, (index as i32) * 8);
}

fn store_native(
    builder: &mut FunctionBuilder<'_>,
    base: ir::Value,
    index: usize,
    value: NativeValue,
) {
    builder.ins().store(
        MemFlagsData::trusted(),
        value.value,
        base,
        (index as i32) * 8,
    );
}

enum TemporalRunAbi {
    Value {
        input_indices: BTreeMap<EnvironmentSlot, usize>,
        output_indices: BTreeMap<EnvironmentSlot, usize>,
    },
    Direct {
        input_fields: BTreeMap<EnvironmentSlot, TypedBoundField>,
        output_fields: BTreeMap<EnvironmentSlot, TypedBoundField>,
    },
}

fn define_temporal_run_function(
    module: &mut JITModule,
    name: &str,
    lowered: &[(&PlannedStream, LoweredProgram)],
    _stream_slots: StreamSlots,
    abi: TemporalRunAbi,
    checked: bool,
) -> Result<FuncId, String> {
    let frontend_config = module.target_config();
    let pointer_type = frontend_config.pointer_type();
    let mut signature = module.make_signature();
    signature.params.push(AbiParam::new(pointer_type));
    signature.params.push(AbiParam::new(pointer_type));
    signature.params.push(AbiParam::new(pointer_type));
    if checked {
        signature.returns.push(AbiParam::new(types::I8));
    }
    let function_id = module
        .declare_function(name, Linkage::Export, &signature)
        .map_err(|error| error.to_string())?;

    let mut context = module.make_context();
    context.func.signature = signature;
    let mut function_builder_context = FunctionBuilderContext::new();
    {
        let mut builder = FunctionBuilder::new(&mut context.func, &mut function_builder_context);
        let entry = builder.create_block();
        builder.append_block_params_for_function_params(entry);
        builder.switch_to_block(entry);
        builder.seal_block(entry);
        let input_base = builder.block_params(entry)[0];
        let output_base = builder.block_params(entry)[1];
        let state = builder.block_params(entry)[2];
        let failure_block = checked.then(|| builder.create_block());
        let mut pending = Vec::new();
        let mut next_state_offset = 0usize;
        let mut produced_values = BTreeMap::<EnvironmentSlot, NativeValue>::new();
        let mut value_output_stores = Vec::<(usize, NativeValue)>::with_capacity(lowered.len());
        let mut direct_output_stores =
            Vec::<(TypedBoundField, NativeValue)>::with_capacity(lowered.len());

        for (planned, program) in lowered {
            let mut input_values = program
                .inputs
                .iter()
                .map(|input| match input.source {
                    InputSource::External(slot) => match &abi {
                        TemporalRunAbi::Value {
                            input_indices,
                            output_indices,
                        } => {
                            if let Some(stream) = _stream_slots.stream(slot) {
                                let producer_slot = _stream_slots.slot(stream);
                                if let Some(value) = produced_values.get(&producer_slot).copied() {
                                    Some(value)
                                } else {
                                    let Some(&index) = output_indices.get(&producer_slot) else {
                                        unreachable!(
                                            "value adapter temporal ABI is missing a stream output mapping"
                                        )
                                    };
                                    Some(NativeValue {
                                        value: builder.ins().load(
                                            native_type(input.kind),
                                            MemFlagsData::trusted(),
                                            output_base,
                                            (index as i32) * 8,
                                        ),
                                        kind: input.kind,
                                    })
                                }
                            } else {
                                let Some(&index) = input_indices.get(&slot) else {
                                    unreachable!("value adapter temporal ABI is missing an input mapping")
                                };
                                Some(NativeValue {
                                    value: builder.ins().load(
                                        native_type(input.kind),
                                        MemFlagsData::trusted(),
                                        input_base,
                                        (index as i32) * 8,
                                    ),
                                    kind: input.kind,
                                })
                            }
                        }
                        TemporalRunAbi::Direct { input_fields, .. } => Some({
                            if let Some(stream) = _stream_slots.stream(slot) {
                                let producer_slot = _stream_slots.slot(stream);
                                produced_values
                                    .get(&producer_slot)
                                    .copied()
                                    .unwrap_or_else(|| {
                                        unreachable!(
                                            "direct temporal input precedes its producing stream"
                                        )
                                    })
                            } else {
                                let field = input_fields.get(&slot).copied().unwrap_or_else(|| {
                                    unreachable!("direct temporal ABI is missing an input mapping")
                                });
                                load_typed_native(&mut builder, input_base, field, input.kind)
                            }
                        }),
                    },
                    InputSource::Node(_) => None,
                })
                .collect::<Vec<_>>();
            let mut temporal_values = vec![
                None;
                program
                    .boundary_nodes
                    .last()
                    .map_or(0, |node| node.index() + 1)
            ];
            let state_offset = next_state_offset;
            if let Some(temporal) = &program.temporal {
                next_state_offset += temporal.state.len;
                emit_temporal_reads(
                    &mut builder,
                    state,
                    state_offset,
                    program,
                    temporal,
                    &mut input_values,
                    &mut temporal_values,
                )?;
            }
            let input_values = input_values
                .into_iter()
                .collect::<Option<Vec<_>>>()
                .expect("scheduled temporal lowering supplies every scalar input");
            let temporal_inputs = input_values.clone();
            let value = GraphCodegen {
                builder: &mut builder,
                input_values,
                failure_block,
            }
            .compile(&program.graph);
            match &abi {
                TemporalRunAbi::Value { output_indices, .. } => {
                    let output_slot = planned.output.environment();
                    let Some(&index) = output_indices.get(&output_slot) else {
                        unreachable!("value adapter temporal ABI is missing an output mapping")
                    };
                    produced_values.insert(output_slot, value);
                    value_output_stores.push((index, value));
                }
                TemporalRunAbi::Direct { output_fields, .. } => {
                    let output_slot = planned.output.environment();
                    produced_values.insert(output_slot, value);
                    if let Some(field) = output_fields.get(&output_slot).copied() {
                        direct_output_stores.push((field, value));
                    }
                }
            }
            if let Some(temporal) = &program.temporal {
                pending.push(PendingTemporalCommit {
                    temporal,
                    state_offset,
                    temporal_values,
                    temporal_inputs,
                    output: value,
                });
            }
        }

        // The plan's logical commit barrier is lowered here. Nothing before this point mutates
        // externally owned temporal state, so failure in any stream remains tick-atomic.
        for commit in pending {
            emit_temporal_commits(&mut builder, state, commit);
        }
        match &abi {
            TemporalRunAbi::Value { .. } => {
                // Keep outputs hidden until every graph and temporal commit succeeds. Dependent
                // graphs consume the computed SSA value instead of caller output memory.
                for (index, value) in value_output_stores {
                    builder.ins().store(
                        MemFlagsData::trusted(),
                        value.value,
                        output_base,
                        (index as i32) * 8,
                    );
                }
            }
            TemporalRunAbi::Direct { .. } => {
                for (field, value) in direct_output_stores {
                    store_typed_native(&mut builder, output_base, field, value);
                }
            }
        }
        if let Some(failure_block) = failure_block {
            let success = builder.ins().iconst(types::I8, 0);
            builder.ins().return_(&[success]);
            builder.switch_to_block(failure_block);
            builder.seal_block(failure_block);
            let failure = builder.ins().iconst(types::I8, 1);
            builder.ins().return_(&[failure]);
        } else {
            builder.ins().return_(&[]);
        }
        builder.finalize(frontend_config);
    }
    module
        .define_function(function_id, &mut context)
        .map_err(|error| error.to_string())?;
    module.clear_context(&mut context);
    Ok(function_id)
}

/// Compiles one complete scheduler-produced temporal plan.
pub(in crate::dataflow::execution::jit) fn compile_temporal_monitor(
    plan: &ScheduledExecutionPlan,
    direct_layout: Option<&TypedIoLayout>,
) -> Result<Option<CompiledTemporalMonitor>, String> {
    #[cfg(test)]
    COMPILE_COUNT.set(COMPILE_COUNT.get() + 1);
    if !plan.uses_static_evaluation() || !plan.has_temporal_state() {
        return Ok(None);
    }
    let mut lowered = Vec::with_capacity(plan.streams.len());
    let mut state_len = 0usize;
    let mut states = Vec::new();
    for planned in plan.streams.iter() {
        let Some(program) =
            Lowering::new().lower_temporal_run(&planned.program.graph, &planned.temporal)
        else {
            return Ok(None);
        };
        if let Some(temporal) = &program.temporal {
            states.push(TemporalRunStateLayout {
                stream: planned.stream.index(),
                offset: state_len,
                layout: temporal.state.clone(),
            });
            state_len = state_len
                .checked_add(temporal.state.len)
                .ok_or_else(|| "native temporal state layout overflow".to_owned())?;
        }
        lowered.push((planned, program));
    }
    let checked = lowered
        .iter()
        .any(|(_, program)| program.requires_division_guard());
    if states.is_empty()
        || !plan
            .commit_streams
            .iter()
            .all(|stream| states.iter().any(|layout| layout.stream == stream.index()))
    {
        return Ok(None);
    }

    let mut external_inputs = BTreeMap::<EnvironmentSlot, ScalarKind>::new();
    for (_, program) in &lowered {
        for input in &program.inputs {
            let InputSource::External(slot) = input.source else {
                continue;
            };
            if plan.stream_slots.stream(slot).is_none() {
                if let Some(previous) = external_inputs.insert(slot, input.kind)
                    && previous != input.kind
                {
                    return Ok(None);
                }
            }
        }
    }

    let mut flag_builder = settings::builder();
    flag_builder
        .set("opt_level", "speed")
        .map_err(|error| error.to_string())?;
    let isa = cranelift_native::builder()
        .map_err(|error| error.to_string())?
        .finish(settings::Flags::new(flag_builder))
        .map_err(|error| error.to_string())?;
    let mut module = JITModule::new(JITBuilder::with_isa(
        isa,
        cranelift_module::default_libcall_names(),
    ));

    let value_function_id = {
        let input_indices = external_inputs
            .keys()
            .copied()
            .enumerate()
            .map(|(index, slot)| (slot, index))
            .collect();
        let output_indices = lowered
            .iter()
            .enumerate()
            .map(|(index, (planned, _))| (planned.output.environment(), index))
            .collect();
        define_temporal_run_function(
            &mut module,
            "dsrv_jitted_value_adapter_scheduled_temporal_plan",
            &lowered,
            plan.stream_slots,
            TemporalRunAbi::Value {
                input_indices,
                output_indices,
            },
            checked,
        )?
    };
    let direct_function_id = if let Some(layout) = direct_layout {
        let input_fields = layout
            .inputs
            .iter()
            .copied()
            .map(|field| (field.slot, field))
            .collect::<BTreeMap<_, _>>();
        let output_fields = layout
            .outputs
            .iter()
            .copied()
            .map(|field| (field.slot, field))
            .collect::<BTreeMap<_, _>>();
        let inputs_match = external_inputs.iter().all(|(slot, kind)| {
            input_fields
                .get(slot)
                .is_some_and(|field| typed_kind_matches_scalar(field.kind, *kind))
        });
        let outputs_match = output_fields.iter().all(|(slot, field)| {
            lowered.iter().any(|(planned, program)| {
                planned.output.environment() == *slot
                    && typed_kind_matches_scalar(field.kind, program.output_kind())
            })
        });
        if !inputs_match || !outputs_match {
            return Ok(None);
        }
        Some(define_temporal_run_function(
            &mut module,
            "dsrv_jitted_direct_scheduled_temporal_plan",
            &lowered,
            plan.stream_slots,
            TemporalRunAbi::Direct {
                input_fields,
                output_fields,
            },
            checked,
        )?)
    } else {
        None
    };

    module
        .finalize_definitions()
        .map_err(|error| error.to_string())?;

    let value_function = {
        let function = module.get_finalized_function(value_function_id);
        if checked {
            // SAFETY: checked temporal functions return the declared one-byte status.
            ValueTemporalFunction::Checked(unsafe {
                std::mem::transmute::<*const u8, CheckedValueTemporalRunFn>(function)
            })
        } else {
            // SAFETY: this temporal function uses the void value ABI.
            ValueTemporalFunction::Void(unsafe {
                std::mem::transmute::<*const u8, ValueTemporalRunFn>(function)
            })
        }
    };
    let direct_function = direct_function_id.map(|function_id| {
        let function = module.get_finalized_function(function_id);
        if checked {
            // SAFETY: checked direct temporal functions return the declared one-byte status.
            DirectFunction::Checked(unsafe {
                std::mem::transmute::<*const u8, CheckedDirectRunFn>(function)
            })
        } else {
            // SAFETY: this temporal function uses the void direct ABI.
            DirectFunction::Void(unsafe { std::mem::transmute::<*const u8, DirectRunFn>(function) })
        }
    });

    let external_inputs = external_inputs
        .into_iter()
        .map(|(slot, kind)| InputSpec {
            source: InputSource::External(slot),
            kind,
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let outputs = lowered
        .iter()
        .map(|(planned, program)| {
            (
                planned.output.environment(),
                program.output_kind(),
                planned.stream.index(),
            )
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Ok(Some(CompiledTemporalMonitor {
        _module: module,
        value_function,
        direct_function,
        external_inputs,
        outputs,
        states: states.into_boxed_slice(),
        state_len,
        environment_len: plan.environment_len,
    }))
}

struct PendingTemporalCommit<'a> {
    temporal: &'a TemporalProgram,
    state_offset: usize,
    temporal_values: Vec<Option<TemporalValue>>,
    temporal_inputs: Vec<NativeValue>,
    output: NativeValue,
}

fn emit_temporal_reads(
    builder: &mut FunctionBuilder<'_>,
    state: ir::Value,
    state_offset: usize,
    program: &LoweredProgram,
    temporal: &TemporalProgram,
    input_values: &mut [Option<NativeValue>],
    temporal_values: &mut Vec<Option<TemporalValue>>,
) -> Result<(), String> {
    for operation in &temporal.ops {
        match operation {
            TemporalOp::Delay {
                node,
                kind,
                cursor,
                filled,
                cells,
                len,
                ..
            } => {
                let cursor_value = load_i64(builder, state, state_offset + *cursor);
                let filled_value = load_i64(builder, state, state_offset + *filled);
                let available = builder.ins().icmp_imm_s(
                    IntCC::SignedGreaterThanOrEqual,
                    filled_value,
                    *len as i64,
                );
                let byte_offset = builder.ins().imul_imm_s(cursor_value, 8);
                let cells_offset = builder
                    .ins()
                    .iconst(types::I64, ((state_offset + *cells) * 8) as i64);
                let address = builder.ins().iadd(state, cells_offset);
                let address = builder.ins().iadd(address, byte_offset);
                let value =
                    builder
                        .ins()
                        .load(native_type(*kind), MemFlagsData::trusted(), address, 0);
                temporal_values.resize(temporal_values.len().max(node.index() + 1), None);
                temporal_values[node.index()] = Some(TemporalValue {
                    value: NativeValue { value, kind: *kind },
                    available,
                });
            }
            TemporalOp::Default {
                node,
                kind,
                input,
                fallback,
                ..
            } => {
                let delayed = temporal_values[input.index()]
                    .expect("temporal plan orders a delay before its default");
                let fallback = read_temporal_source(builder, *fallback, input_values, None);
                let value =
                    builder
                        .ins()
                        .select(delayed.available, delayed.value.value, fallback.value);
                let available = builder.ins().iconst(types::I8, 1);
                temporal_values.resize(temporal_values.len().max(node.index() + 1), None);
                temporal_values[node.index()] = Some(TemporalValue {
                    value: NativeValue { value, kind: *kind },
                    available,
                });
            }
        }
    }
    for (index, input) in program.inputs.iter().enumerate() {
        if let InputSource::Node(node) = input.source {
            let value = temporal_values[node.index()]
                .ok_or_else(|| "scheduled temporal input was not lowered".to_owned())?
                .value;
            input_values[index] = Some(value);
        }
    }
    Ok(())
}

fn emit_temporal_commits(
    builder: &mut FunctionBuilder<'_>,
    state: ir::Value,
    pending: PendingTemporalCommit<'_>,
) {
    let inputs = pending
        .temporal_inputs
        .iter()
        .copied()
        .map(Some)
        .collect::<Vec<_>>();
    for operation in &pending.temporal.ops {
        match operation {
            TemporalOp::Delay {
                kind,
                source,
                recursive,
                cursor,
                filled,
                last_bits,
                last_tag,
                cells,
                len,
                node,
            } => {
                let delayed = pending.temporal_values[node.index()].unwrap();
                if !recursive {
                    store_native(
                        builder,
                        state,
                        pending.state_offset + *last_bits,
                        delayed.value,
                    );
                    let concrete = builder.ins().iconst(types::I64, 2);
                    let deferred = builder.ins().iconst(types::I64, 1);
                    let tag = builder.ins().select(delayed.available, concrete, deferred);
                    store_i64(builder, state, pending.state_offset + *last_tag, tag);
                }
                let source = read_temporal_source(builder, *source, &inputs, Some(pending.output));
                let cursor_index = pending.state_offset + *cursor;
                let cursor_value = load_i64(builder, state, cursor_index);
                let byte_offset = builder.ins().imul_imm_s(cursor_value, 8);
                let cells_offset = builder
                    .ins()
                    .iconst(types::I64, ((pending.state_offset + *cells) * 8) as i64);
                let address = builder.ins().iadd(state, cells_offset);
                let address = builder.ins().iadd(address, byte_offset);
                builder
                    .ins()
                    .store(MemFlagsData::trusted(), source.value, address, 0);
                let next_cursor = builder.ins().iadd_imm_s(cursor_value, 1);
                let wraps = builder
                    .ins()
                    .icmp_imm_s(IntCC::Equal, next_cursor, *len as i64);
                let zero = builder.ins().iconst(types::I64, 0);
                let next_cursor = builder.ins().select(wraps, zero, next_cursor);
                store_i64(builder, state, cursor_index, next_cursor);
                let filled_index = pending.state_offset + *filled;
                let filled_value = load_i64(builder, state, filled_index);
                let incremented = builder.ins().iadd_imm_s(filled_value, 1);
                let full = builder.ins().icmp_imm_s(
                    IntCC::SignedGreaterThanOrEqual,
                    incremented,
                    *len as i64,
                );
                let capacity = builder.ins().iconst(types::I64, *len as i64);
                let next_filled = builder.ins().select(full, capacity, incremented);
                store_i64(builder, state, filled_index, next_filled);
                debug_assert_eq!(source.kind, *kind);
            }
            TemporalOp::Default {
                input,
                last_bits,
                last_tag,
                ..
            } => {
                let delayed = pending.temporal_values[input.index()].unwrap();
                store_native(
                    builder,
                    state,
                    pending.state_offset + *last_bits,
                    delayed.value,
                );
                let concrete = builder.ins().iconst(types::I64, 2);
                let deferred = builder.ins().iconst(types::I64, 1);
                let tag = builder.ins().select(delayed.available, concrete, deferred);
                store_i64(builder, state, pending.state_offset + *last_tag, tag);
            }
        }
    }
}

enum ScalarRunAbi {
    Value {
        input_indices: BTreeMap<EnvironmentSlot, usize>,
        output_indices: BTreeMap<EnvironmentSlot, usize>,
    },
    Direct {
        input_fields: BTreeMap<EnvironmentSlot, TypedBoundField>,
        output_fields: BTreeMap<EnvironmentSlot, TypedBoundField>,
    },
}

fn define_scalar_run_function(
    module: &mut JITModule,
    name: &str,
    lowered: &[(LoweredProgram, EnvironmentSlot, usize)],
    _stream_slots: StreamSlots,
    abi: ScalarRunAbi,
    checked: bool,
) -> Result<FuncId, String> {
    let frontend_config = module.target_config();
    let pointer_type = frontend_config.pointer_type();
    let mut signature = module.make_signature();
    signature.params.push(AbiParam::new(pointer_type));
    signature.params.push(AbiParam::new(pointer_type));
    if matches!(&abi, ScalarRunAbi::Direct { .. }) {
        signature.params.push(AbiParam::new(pointer_type));
    }
    if checked {
        signature.returns.push(AbiParam::new(types::I8));
    }
    let function_id = module
        .declare_function(name, Linkage::Export, &signature)
        .map_err(|error| error.to_string())?;

    let mut context = module.make_context();
    context.func.signature = signature;
    let mut function_builder_context = FunctionBuilderContext::new();
    {
        let mut builder = FunctionBuilder::new(&mut context.func, &mut function_builder_context);
        let entry = builder.create_block();
        builder.append_block_params_for_function_params(entry);
        builder.switch_to_block(entry);
        builder.seal_block(entry);
        let input_base = builder.block_params(entry)[0];
        let output_base = builder.block_params(entry)[1];
        let failure_block = checked.then(|| builder.create_block());
        let mut produced_values = BTreeMap::<EnvironmentSlot, NativeValue>::new();
        let mut value_output_stores = Vec::<(usize, NativeValue)>::with_capacity(lowered.len());
        let mut direct_output_stores =
            Vec::<(TypedBoundField, NativeValue)>::with_capacity(lowered.len());

        for (program, output_slot, _) in lowered {
            let input_values = program
                .inputs
                .iter()
                .map(|input| {
                    let InputSource::External(slot) = input.source else {
                        unreachable!()
                    };
                    match &abi {
                        ScalarRunAbi::Value { input_indices, .. } => {
                            if let Some(value) = produced_values.get(&slot).copied() {
                                return value;
                            }
                            let Some(&index) = input_indices.get(&slot) else {
                                unreachable!("value adapter scalar ABI is missing an input mapping")
                            };
                            NativeValue {
                                value: builder.ins().load(
                                    native_type(input.kind),
                                    MemFlagsData::trusted(),
                                    input_base,
                                    (index as i32) * 8,
                                ),
                                kind: input.kind,
                            }
                        }
                        ScalarRunAbi::Direct { input_fields, .. } => {
                            if let Some(stream) = _stream_slots.stream(slot) {
                                let producer_slot = _stream_slots.slot(stream);
                                produced_values
                                    .get(&producer_slot)
                                    .copied()
                                    .unwrap_or_else(|| {
                                        unreachable!(
                                            "direct scalar input precedes its producing stream"
                                        )
                                    })
                            } else {
                                let field = input_fields.get(&slot).copied().unwrap_or_else(|| {
                                    unreachable!("direct scalar ABI is missing an input mapping")
                                });
                                load_typed_native(&mut builder, input_base, field, input.kind)
                            }
                        }
                    }
                })
                .collect();
            let value = GraphCodegen {
                builder: &mut builder,
                input_values,
                failure_block,
            }
            .compile(&program.graph);
            match &abi {
                ScalarRunAbi::Value { output_indices, .. } => {
                    let Some(&index) = output_indices.get(output_slot) else {
                        unreachable!("value adapter scalar ABI is missing an output mapping")
                    };
                    produced_values.insert(*output_slot, value);
                    value_output_stores.push((index, value));
                }
                ScalarRunAbi::Direct { output_fields, .. } => {
                    produced_values.insert(*output_slot, value);
                    if let Some(field) = output_fields.get(output_slot).copied() {
                        direct_output_stores.push((field, value));
                    }
                }
            }
        }

        match &abi {
            ScalarRunAbi::Value { .. } => {
                for (index, value) in value_output_stores {
                    builder.ins().store(
                        MemFlagsData::trusted(),
                        value.value,
                        output_base,
                        (index as i32) * 8,
                    );
                }
            }
            ScalarRunAbi::Direct { .. } => {
                for (field, value) in direct_output_stores {
                    store_typed_native(&mut builder, output_base, field, value);
                }
            }
        }
        if let Some(failure_block) = failure_block {
            let success = builder.ins().iconst(types::I8, 0);
            builder.ins().return_(&[success]);
            builder.switch_to_block(failure_block);
            builder.seal_block(failure_block);
            let failure = builder.ins().iconst(types::I8, 1);
            builder.ins().return_(&[failure]);
        } else {
            builder.ins().return_(&[]);
        }
        builder.finalize(frontend_config);
    }
    module
        .define_function(function_id, &mut context)
        .map_err(|error| error.to_string())?;
    module.clear_context(&mut context);
    Ok(function_id)
}

pub(in crate::dataflow::execution::jit) fn compile_scalar_region(
    region: &ScalarRegion,
    stream_slots: StreamSlots,
    direct_layout: Option<&TypedIoLayout>,
) -> Result<Option<CompiledScalarRegion>, String> {
    #[cfg(test)]
    COMPILE_COUNT.set(COMPILE_COUNT.get() + 1);
    let programs = region.programs().collect::<Vec<_>>();
    if programs.is_empty() {
        return Ok(None);
    }
    let mut lowered = Vec::with_capacity(programs.len());
    for (program, output_slot, stream) in programs {
        let Some(program) = Lowering::new().lower_scalar_program(program) else {
            return Ok(None);
        };
        if !program.boundary_nodes.is_empty()
            || program
                .inputs
                .iter()
                .any(|input| matches!(input.source, InputSource::Node(_)))
        {
            return Ok(None);
        }
        lowered.push((program, output_slot, stream));
    }

    let checked = lowered
        .iter()
        .any(|(program, _, _)| program.requires_division_guard());
    let produced_slots = lowered
        .iter()
        .map(|(_, slot, _)| *slot)
        .collect::<std::collections::BTreeSet<_>>();
    let mut external_inputs = BTreeMap::<EnvironmentSlot, ScalarKind>::new();
    for (program, _, _) in &lowered {
        for input in &program.inputs {
            let InputSource::External(slot) = input.source else {
                unreachable!()
            };
            if !produced_slots.contains(&slot)
                && let Some(previous) = external_inputs.insert(slot, input.kind)
                && previous != input.kind
            {
                return Ok(None);
            }
        }
    }

    let mut flag_builder = settings::builder();
    flag_builder
        .set("opt_level", "speed")
        .map_err(|error| error.to_string())?;
    let isa = cranelift_native::builder()
        .map_err(|error| error.to_string())?
        .finish(settings::Flags::new(flag_builder))
        .map_err(|error| error.to_string())?;
    let mut module = JITModule::new(JITBuilder::with_isa(
        isa,
        cranelift_module::default_libcall_names(),
    ));

    let value_function_id = {
        let input_indices = external_inputs
            .keys()
            .copied()
            .enumerate()
            .map(|(index, slot)| (slot, index))
            .collect();
        let output_indices = lowered
            .iter()
            .enumerate()
            .map(|(index, (_, slot, _))| (*slot, index))
            .collect();
        define_scalar_run_function(
            &mut module,
            "dsrv_jitted_value_adapter_scalar_run",
            &lowered,
            stream_slots,
            ScalarRunAbi::Value {
                input_indices,
                output_indices,
            },
            checked,
        )?
    };
    let direct_function_id = if let Some(layout) = direct_layout {
        let input_fields = layout
            .inputs
            .iter()
            .copied()
            .map(|field| (field.slot, field))
            .collect::<BTreeMap<_, _>>();
        let output_fields = layout
            .outputs
            .iter()
            .copied()
            .map(|field| (field.slot, field))
            .collect::<BTreeMap<_, _>>();
        let inputs_match = external_inputs.iter().all(|(slot, kind)| {
            input_fields
                .get(slot)
                .is_some_and(|field| typed_kind_matches_scalar(field.kind, *kind))
        });
        let outputs_match = output_fields.iter().all(|(slot, field)| {
            lowered.iter().any(|(program, output_slot, _)| {
                output_slot == slot && typed_kind_matches_scalar(field.kind, program.output_kind())
            })
        });
        if !inputs_match || !outputs_match {
            return Ok(None);
        }
        Some(define_scalar_run_function(
            &mut module,
            "dsrv_jitted_direct_scalar_run",
            &lowered,
            stream_slots,
            ScalarRunAbi::Direct {
                input_fields,
                output_fields,
            },
            checked,
        )?)
    } else {
        None
    };

    module
        .finalize_definitions()
        .map_err(|error| error.to_string())?;

    let value_function = {
        let function = module.get_finalized_function(value_function_id);
        if checked {
            // SAFETY: checked scalar functions return the declared one-byte status.
            ValueScalarFunction::Checked(unsafe {
                std::mem::transmute::<*const u8, CheckedValueRunFn>(function)
            })
        } else {
            // SAFETY: this scalar function uses the void value ABI.
            ValueScalarFunction::Void(unsafe {
                std::mem::transmute::<*const u8, ValueRunFn>(function)
            })
        }
    };
    let direct_function = direct_function_id.map(|function_id| {
        let function = module.get_finalized_function(function_id);
        if checked {
            // SAFETY: checked direct scalar functions return the declared one-byte status.
            DirectFunction::Checked(unsafe {
                std::mem::transmute::<*const u8, CheckedDirectRunFn>(function)
            })
        } else {
            // SAFETY: this scalar function uses the void direct ABI.
            DirectFunction::Void(unsafe { std::mem::transmute::<*const u8, DirectRunFn>(function) })
        }
    });
    let outputs = lowered
        .iter()
        .map(|(program, slot, stream)| (*slot, program.output_kind(), *stream))
        .collect();
    let external_inputs = external_inputs
        .into_iter()
        .map(|(slot, kind)| InputSpec {
            source: InputSource::External(slot),
            kind,
        })
        .collect();
    Ok(Some(CompiledScalarRegion {
        _module: module,
        value_function,
        direct_function,
        external_inputs,
        outputs,
        environment_len: stream_slots.start().index() + stream_slots.len(),
    }))
}

fn native_integer_division(
    builder: &mut FunctionBuilder<'_>,
    division: IntegerDivision,
    failure_block: Option<ir::Block>,
    lhs: ir::Value,
    rhs: ir::Value,
    remainder: bool,
) -> ir::Value {
    if division == IntegerDivision::Checked {
        let failure_block = failure_block.expect("checked division requires a failure block");
        let nonzero = builder.ins().icmp_imm_s(IntCC::NotEqual, rhs, 0);
        let continuation = builder.create_block();
        builder
            .ins()
            .brif(nonzero, continuation, &[], failure_block, &[]);
        builder.switch_to_block(continuation);
        builder.seal_block(continuation);
    }

    // Signed division traps on MIN / -1. Replacing that divisor with one gives the
    // native wrapping quotient and remainder without adding a hot control-flow edge.
    let minimum = builder.ins().icmp_imm_s(IntCC::Equal, lhs, i64::MIN);
    let negative_one = builder.ins().icmp_imm_s(IntCC::Equal, rhs, -1);
    let overflow = builder.ins().band(minimum, negative_one);
    let one = builder.ins().iconst(types::I64, 1);
    let divisor = builder.ins().select(overflow, one, rhs);
    if remainder {
        builder.ins().srem(lhs, divisor)
    } else {
        builder.ins().sdiv(lhs, divisor)
    }
}

struct GraphCodegen<'a, 'b> {
    builder: &'a mut FunctionBuilder<'b>,
    input_values: Vec<NativeValue>,
    failure_block: Option<ir::Block>,
}

#[derive(Clone, Copy)]
struct NativeValue {
    value: ir::Value,
    kind: ScalarKind,
}

impl GraphCodegen<'_, '_> {
    fn compile(&mut self, graph: &LoweredGraph) -> NativeValue {
        let mut values = vec![None; graph.nodes.len()];
        self.read(graph.output, graph, &mut values)
    }

    fn compile_node(
        &mut self,
        index: usize,
        graph: &LoweredGraph,
        values: &mut [Option<NativeValue>],
    ) -> NativeValue {
        if let Some(value) = values[index] {
            return value;
        }
        let value = match &graph.nodes[index] {
            LoweredNode::Unary {
                op,
                arg,
                input_kind,
                output_kind,
            } => {
                let arg = self.read(*arg, graph, values);
                debug_assert_eq!(arg.kind, *input_kind);
                let value = match (op, input_kind, output_kind) {
                    (UnaryOperator::Not, ScalarKind::Bool, ScalarKind::Bool) => {
                        self.builder.ins().bxor_imm_u(arg.value, 1)
                    }
                    (UnaryOperator::Negate, ScalarKind::Int, ScalarKind::Int) => {
                        self.builder.ins().ineg(arg.value)
                    }
                    (UnaryOperator::Absolute, ScalarKind::Int, ScalarKind::Int) => {
                        self.builder.ins().iabs(arg.value)
                    }
                    (UnaryOperator::Negate, ScalarKind::Float, ScalarKind::Float) => {
                        self.builder.ins().fneg(arg.value)
                    }
                    (UnaryOperator::Absolute, ScalarKind::Float, ScalarKind::Float) => {
                        self.builder.ins().fabs(arg.value)
                    }
                    _ => unreachable!("lowering admitted an unsupported unary operation"),
                };
                NativeValue {
                    value,
                    kind: *output_kind,
                }
            }
            LoweredNode::Binary {
                op,
                lhs,
                rhs,
                left_kind,
                right_kind,
                output_kind,
                division,
            } => {
                let lhs = self.read(*lhs, graph, values);
                let rhs = self.read(*rhs, graph, values);
                debug_assert_eq!(lhs.kind, *left_kind);
                debug_assert_eq!(rhs.kind, *right_kind);
                self.binary(*op, lhs, rhs, *output_kind, *division)
            }
            LoweredNode::If {
                condition,
                then_graph,
                else_graph,
            } => {
                let condition = self.read(*condition, graph, values);
                debug_assert_eq!(condition.kind, ScalarKind::Bool);
                // Canonical non-recursive `if` advances both branch graphs every tick.
                let then_value = self.compile(then_graph);
                let else_value = self.compile(else_graph);
                debug_assert_eq!(then_value.kind, else_value.kind);
                let condition = self
                    .builder
                    .ins()
                    .icmp_imm_s(IntCC::NotEqual, condition.value, 0);
                NativeValue {
                    value: self
                        .builder
                        .ins()
                        .select(condition, then_value.value, else_value.value),
                    kind: then_value.kind,
                }
            }
        };
        values[index] = Some(value);
        value
    }

    fn read(
        &mut self,
        reference: ScalarRef,
        graph: &LoweredGraph,
        nodes: &mut [Option<NativeValue>],
    ) -> NativeValue {
        match reference {
            ScalarRef::Constant { bits, kind } => NativeValue {
                value: match kind {
                    ScalarKind::Float => {
                        self.builder.ins().f64const(Ieee64::with_bits(bits as u64))
                    }
                    ScalarKind::Int | ScalarKind::Bool => {
                        self.builder.ins().iconst(types::I64, bits)
                    }
                },
                kind,
            },
            ScalarRef::Input { index, kind } => {
                let value = self.input_values[index as usize];
                debug_assert_eq!(value.kind, kind);
                value
            }
            ScalarRef::Node { index, kind } => {
                let value = self.compile_node(index as usize, graph, nodes);
                debug_assert_eq!(value.kind, kind);
                value
            }
        }
    }

    fn binary(
        &mut self,
        op: BinaryOperator,
        lhs: NativeValue,
        rhs: NativeValue,
        output_kind: ScalarKind,
        division: IntegerDivision,
    ) -> NativeValue {
        use BinaryOperator as Op;
        let value = if output_kind == ScalarKind::Float {
            let lhs = self.to_float(lhs);
            let rhs = self.to_float(rhs);
            match op {
                Op::Add => self.builder.ins().fadd(lhs, rhs),
                Op::Subtract => self.builder.ins().fsub(lhs, rhs),
                Op::Multiply => self.builder.ins().fmul(lhs, rhs),
                Op::Divide => self.builder.ins().fdiv(lhs, rhs),
                _ => unreachable!("lowering admitted an unsupported float operation"),
            }
        } else if matches!(
            op,
            Op::Equal | Op::Less | Op::LessEqual | Op::Greater | Op::GreaterEqual
        ) && (lhs.kind == ScalarKind::Float || rhs.kind == ScalarKind::Float)
        {
            let lhs = self.to_float(lhs);
            let rhs = self.to_float(rhs);
            let condition = match op {
                Op::Equal => FloatCC::Equal,
                Op::Less => FloatCC::LessThan,
                Op::LessEqual => FloatCC::LessThanOrEqual,
                Op::Greater => FloatCC::GreaterThan,
                Op::GreaterEqual => FloatCC::GreaterThanOrEqual,
                _ => unreachable!(),
            };
            self.compare_float(condition, lhs, rhs)
        } else {
            let lhs = lhs.value;
            let rhs = rhs.value;
            match op {
                Op::Add => self.builder.ins().iadd(lhs, rhs),
                Op::Subtract => self.builder.ins().isub(lhs, rhs),
                Op::Multiply => self.builder.ins().imul(lhs, rhs),
                Op::Divide => native_integer_division(
                    self.builder,
                    division,
                    self.failure_block,
                    lhs,
                    rhs,
                    false,
                ),
                Op::Modulo => native_integer_division(
                    self.builder,
                    division,
                    self.failure_block,
                    lhs,
                    rhs,
                    true,
                ),
                Op::And => self.builder.ins().band(lhs, rhs),
                Op::Or => self.builder.ins().bor(lhs, rhs),
                Op::Implication => {
                    let not_lhs = self.builder.ins().bxor_imm_u(lhs, 1);
                    self.builder.ins().bor(not_lhs, rhs)
                }
                Op::Equal => self.compare(IntCC::Equal, lhs, rhs),
                Op::Less => self.compare(IntCC::SignedLessThan, lhs, rhs),
                Op::LessEqual => self.compare(IntCC::SignedLessThanOrEqual, lhs, rhs),
                Op::Greater => self.compare(IntCC::SignedGreaterThan, lhs, rhs),
                Op::GreaterEqual => self.compare(IntCC::SignedGreaterThanOrEqual, lhs, rhs),
                Op::Concatenate => unreachable!("concatenation is not JIT eligible"),
            }
        };
        NativeValue {
            value,
            kind: output_kind,
        }
    }

    fn to_float(&mut self, value: NativeValue) -> ir::Value {
        match value.kind {
            ScalarKind::Float => value.value,
            ScalarKind::Int => self.builder.ins().fcvt_from_sint(types::F64, value.value),
            ScalarKind::Bool => unreachable!("booleans are not converted to floats"),
        }
    }

    fn compare(&mut self, condition: IntCC, lhs: ir::Value, rhs: ir::Value) -> ir::Value {
        let value = self.builder.ins().icmp(condition, lhs, rhs);
        self.builder.ins().uextend(types::I64, value)
    }

    fn compare_float(&mut self, condition: FloatCC, lhs: ir::Value, rhs: ir::Value) -> ir::Value {
        let value = self.builder.ins().fcmp(condition, lhs, rhs);
        self.builder.ins().uextend(types::I64, value)
    }
}

fn native_type(kind: ScalarKind) -> ir::Type {
    match kind {
        ScalarKind::Int | ScalarKind::Bool => types::I64,
        ScalarKind::Float => types::F64,
    }
}

fn typed_kind_matches_scalar(direct: TypedKind, scalar: ScalarKind) -> bool {
    matches!(
        (direct, scalar),
        (TypedKind::Int, ScalarKind::Int)
            | (TypedKind::Float, ScalarKind::Float)
            | (TypedKind::Bool, ScalarKind::Bool)
    )
}

fn typed_offset(field: TypedBoundField) -> i32 {
    i32::try_from(field.offset).expect("direct row field offset exceeds the native ABI limit")
}

fn load_typed_native(
    builder: &mut FunctionBuilder<'_>,
    base: ir::Value,
    field: TypedBoundField,
    expected: ScalarKind,
) -> NativeValue {
    assert!(typed_kind_matches_scalar(field.kind, expected));
    let value = match field.kind {
        TypedKind::Int => builder.ins().load(
            types::I64,
            MemFlagsData::trusted(),
            base,
            typed_offset(field),
        ),
        TypedKind::Float => builder.ins().load(
            types::F64,
            MemFlagsData::trusted(),
            base,
            typed_offset(field),
        ),
        TypedKind::Bool => {
            let value = builder.ins().load(
                types::I8,
                MemFlagsData::trusted(),
                base,
                typed_offset(field),
            );
            builder.ins().uextend(types::I64, value)
        }
    };
    NativeValue {
        value,
        kind: expected,
    }
}

fn store_typed_native(
    builder: &mut FunctionBuilder<'_>,
    base: ir::Value,
    field: TypedBoundField,
    value: NativeValue,
) {
    assert!(typed_kind_matches_scalar(field.kind, value.kind));
    let value = match field.kind {
        TypedKind::Int | TypedKind::Float => value.value,
        TypedKind::Bool => builder.ins().ireduce(types::I8, value.value),
    };
    builder
        .ins()
        .store(MemFlagsData::trusted(), value, base, typed_offset(field));
}
#[cfg(test)]
thread_local! {
    static COMPILE_COUNT: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_compile_count() {
    COMPILE_COUNT.set(0);
}

#[cfg(test)]
pub(crate) fn compile_count() -> usize {
    COMPILE_COUNT.get()
}
