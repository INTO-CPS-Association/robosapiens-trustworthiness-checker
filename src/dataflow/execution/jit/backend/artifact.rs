use cranelift_jit::JITModule;

use crate::core::Value;
use crate::dataflow::environment::EnvironmentSlot;
use crate::dataflow::ir::{NodeId, ScalarKind};

pub(in crate::dataflow::execution::jit) type ValueRunFn =
    unsafe extern "C" fn(*const i64, *mut i64);
pub(in crate::dataflow::execution::jit) type CheckedValueRunFn =
    unsafe extern "C" fn(*const i64, *mut i64) -> u8;
pub(in crate::dataflow::execution::jit) type DirectRunFn =
    unsafe extern "C" fn(*const u8, *mut u8, *mut i64);
pub(in crate::dataflow::execution::jit) type CheckedDirectRunFn =
    unsafe extern "C" fn(*const u8, *mut u8, *mut i64) -> u8;
pub(in crate::dataflow::execution::jit) type ValueTemporalRunFn =
    unsafe extern "C" fn(*const i64, *mut i64, *mut i64);
pub(in crate::dataflow::execution::jit) type CheckedValueTemporalRunFn =
    unsafe extern "C" fn(*const i64, *mut i64, *mut i64) -> u8;

#[derive(Clone, Copy)]
pub(in crate::dataflow::execution::jit) enum ValueScalarFunction {
    Void(ValueRunFn),
    Checked(CheckedValueRunFn),
}

#[derive(Clone, Copy)]
pub(in crate::dataflow::execution::jit) enum DirectFunction {
    Void(DirectRunFn),
    Checked(CheckedDirectRunFn),
}

#[derive(Clone, Copy)]
pub(in crate::dataflow::execution::jit) enum ValueTemporalFunction {
    Void(ValueTemporalRunFn),
    Checked(CheckedValueTemporalRunFn),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::dataflow::execution::jit) enum InputSource {
    External(EnvironmentSlot),
    Node(NodeId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::dataflow::execution::jit) struct InputSpec {
    pub(in crate::dataflow::execution::jit) source: InputSource,
    pub(in crate::dataflow::execution::jit) kind: ScalarKind,
}

#[derive(Clone)]
pub(in crate::dataflow::execution::jit) struct TemporalStateLayout {
    pub(in crate::dataflow::execution::jit) len: usize,
    pub(in crate::dataflow::execution::jit) nodes: Box<[TemporalNodeLayout]>,
}

#[derive(Clone)]
pub(in crate::dataflow::execution::jit) enum TemporalNodeLayout {
    Delay {
        node: NodeId,
        kind: ScalarKind,
        cursor: usize,
        filled: usize,
        last_bits: usize,
        last_tag: usize,
        cells: usize,
        len: usize,
    },
    Default {
        node: NodeId,
        kind: ScalarKind,
        last_bits: usize,
        last_tag: usize,
    },
}

pub(in crate::dataflow::execution::jit) struct CompiledScalarRegion {
    pub(super) _module: JITModule,

    pub(in crate::dataflow::execution::jit) value_function: ValueScalarFunction,
    pub(in crate::dataflow::execution::jit) direct_function: Option<DirectFunction>,

    pub(in crate::dataflow::execution::jit) external_inputs: Box<[InputSpec]>,
    pub(in crate::dataflow::execution::jit) outputs: Box<[(EnvironmentSlot, ScalarKind, usize)]>,
    pub(in crate::dataflow::execution::jit) environment_len: usize,
}

pub(in crate::dataflow::execution::jit) struct CompiledTemporalMonitor {
    pub(super) _module: JITModule,

    pub(in crate::dataflow::execution::jit) value_function: ValueTemporalFunction,
    pub(in crate::dataflow::execution::jit) direct_function: Option<DirectFunction>,

    pub(in crate::dataflow::execution::jit) external_inputs: Box<[InputSpec]>,
    pub(in crate::dataflow::execution::jit) outputs: Box<[(EnvironmentSlot, ScalarKind, usize)]>,
    pub(in crate::dataflow::execution::jit) states: Box<[TemporalRunStateLayout]>,
    pub(in crate::dataflow::execution::jit) state_len: usize,
    pub(in crate::dataflow::execution::jit) environment_len: usize,
}

#[derive(Clone)]
pub(in crate::dataflow::execution::jit) struct TemporalRunStateLayout {
    pub(in crate::dataflow::execution::jit) stream: usize,
    pub(in crate::dataflow::execution::jit) offset: usize,
    pub(in crate::dataflow::execution::jit) layout: TemporalStateLayout,
}

pub(in crate::dataflow::execution::jit) fn decode(value: i64, kind: ScalarKind) -> Value {
    match kind {
        ScalarKind::Int => Value::Int(value),
        ScalarKind::Bool => Value::Bool(value != 0),
        ScalarKind::Float => Value::Float(f64::from_bits(value as u64)),
    }
}
