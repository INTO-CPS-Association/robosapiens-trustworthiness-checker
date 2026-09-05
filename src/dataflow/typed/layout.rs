use thiserror::Error;

use super::{TypedField, TypedInput, TypedKind, TypedOutput};
use crate::core::StreamType;
use crate::dataflow::environment::EnvironmentSlot;
use crate::dataflow::execution::quickening::ScalarValue;
use crate::dataflow::{DataflowCompilationError, DataflowProgram};

/// Failure to bind a compile-time row type to a checked monitor interface.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TypedBindingError {
    #[error("checked monitor compilation failed")]
    Compilation(#[from] DataflowCompilationError),
    #[error("typed {side} row has {actual} fields, but the monitor requires {expected}")]
    FieldCount {
        side: &'static str,
        expected: usize,
        actual: usize,
    },
    #[error("typed {side} field {index} has kind {actual:?}, but `{variable}` has type {expected}")]
    TypeMismatch {
        side: &'static str,
        index: usize,
        variable: String,
        expected: StreamType,
        actual: TypedKind,
    },
    #[error("typed {side} variable `{variable}` has no checked scalar type")]
    UnsupportedType {
        side: &'static str,
        variable: String,
    },
    #[error("the checked monitor has no complete direct {backend} plan")]
    UnsupportedPlan { backend: &'static str },
}

#[derive(Clone, Copy, Debug)]
pub(in crate::dataflow) struct TypedBoundField {
    /// Only native lowering resolves fields back to environment slots.
    #[cfg_attr(not(feature = "jit"), allow(dead_code))]
    pub(in crate::dataflow) slot: EnvironmentSlot,
    pub(in crate::dataflow) kind: TypedKind,
    pub(in crate::dataflow) offset: usize,
}

#[derive(Clone, Debug)]
pub(in crate::dataflow) struct TypedIoLayout {
    pub(in crate::dataflow) inputs: Box<[TypedBoundField]>,
    pub(in crate::dataflow) outputs: Box<[TypedBoundField]>,
}

impl TypedIoLayout {
    pub(super) fn bind<I: TypedInput, O: TypedOutput>(
        program: &DataflowProgram,
    ) -> Result<Self, TypedBindingError> {
        let inputs = bind_fields("input", &I::typed_fields(), program.input_vars(), program)?;
        let outputs = bind_fields("output", &O::typed_fields(), program.output_vars(), program)?;
        Ok(Self { inputs, outputs })
    }
}

fn bind_fields(
    side: &'static str,
    fields: &[TypedField],
    variables: &[crate::VarName],
    program: &DataflowProgram,
) -> Result<Box<[TypedBoundField]>, TypedBindingError> {
    if fields.len() != variables.len() {
        return Err(TypedBindingError::FieldCount {
            side,
            expected: variables.len(),
            actual: fields.len(),
        });
    }
    fields
        .iter()
        .copied()
        .zip(variables)
        .enumerate()
        .map(|(index, (field, variable))| {
            let slot = program
                .environment_layout()
                .slot(variable)
                .expect("compiled interface variable must have an environment slot");
            let expected = program
                .environment_layout()
                .stream_type(slot)
                .cloned()
                .ok_or_else(|| TypedBindingError::UnsupportedType {
                    side,
                    variable: variable.to_string(),
                })?;
            let actual_type = match field.kind() {
                TypedKind::Int => StreamType::Int,
                TypedKind::Float => StreamType::Float,
                TypedKind::Bool => StreamType::Bool,
            };
            if expected != actual_type {
                return Err(TypedBindingError::TypeMismatch {
                    side,
                    index,
                    variable: variable.to_string(),
                    expected,
                    actual: field.kind(),
                });
            }
            Ok(TypedBoundField {
                slot,
                kind: field.kind(),
                offset: field.offset(),
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Vec::into_boxed_slice)
}

/// The positional variable order bound to a typed monitor's tuple rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypedInterface {
    input_vars: Box<[crate::VarName]>,
    output_vars: Box<[crate::VarName]>,
}

impl TypedInterface {
    pub(super) fn from_program(program: &DataflowProgram) -> Self {
        Self {
            input_vars: program.input_vars().into(),
            output_vars: program.output_vars().into(),
        }
    }

    pub fn input_vars(&self) -> &[crate::VarName] {
        &self.input_vars
    }

    pub fn output_vars(&self) -> &[crate::VarName] {
        &self.output_vars
    }
}

#[inline(always)]
pub(in crate::dataflow) unsafe fn load_typed_scalar(
    base: *const u8,
    field: TypedBoundField,
) -> ScalarValue {
    // SAFETY: row binding verifies that the offset and primitive kind came from the concrete tuple
    // type whose pointer is passed to the direct executor.
    unsafe {
        match field.kind {
            TypedKind::Int => ScalarValue::Int((base.add(field.offset) as *const i64).read()),
            TypedKind::Float => ScalarValue::Float((base.add(field.offset) as *const f64).read()),
            TypedKind::Bool => ScalarValue::Bool((base.add(field.offset) as *const bool).read()),
        }
    }
}

#[inline(always)]
pub(in crate::dataflow) unsafe fn store_typed_scalar(
    base: *mut u8,
    field: TypedBoundField,
    value: ScalarValue,
) -> bool {
    // SAFETY: row binding verifies the output field offset and type. Only the matching primitive is
    // written, and callers expose the output row only after every field has been written.
    unsafe {
        match (field.kind, value) {
            (TypedKind::Int, ScalarValue::Int(value)) => {
                (base.add(field.offset) as *mut i64).write(value);
                true
            }
            (TypedKind::Float, ScalarValue::Float(value)) => {
                (base.add(field.offset) as *mut f64).write(value);
                true
            }
            (TypedKind::Bool, ScalarValue::Bool(value)) => {
                (base.add(field.offset) as *mut bool).write(value);
                true
            }
            _ => false,
        }
    }
}
