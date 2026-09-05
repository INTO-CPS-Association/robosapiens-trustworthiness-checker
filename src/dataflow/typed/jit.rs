//! Ahead-of-time native compilation for a statically typed monitor.
//!
//! [`TypedJitMonitor`] compiles a whole-schedule native artifact when it is constructed and fails
//! if the specification cannot be lowered. Prefer [`super::TypedDataflowMonitor`] unless a caller
//! genuinely needs that guarantee up front rather than after warming.

use std::marker::PhantomData;
use std::mem::MaybeUninit;

use super::{
    TypedBindingError, TypedInput, TypedInterface, TypedIoLayout, TypedMonitor, TypedOutput,
};
use crate::dataflow::execution::jit::PreparedDirectJit;
use crate::dataflow::{DataflowMonitor, DataflowProgram};
use crate::lang::dsrv::ast::CheckedDsrvSpecification;

/// A stateful native monitor specialized for statically typed tuple rows.
///
/// Evaluation panics if a dynamically computed integer divisor is zero.
#[cfg(feature = "jit")]
pub struct TypedJitMonitor<I: TypedInput, O: TypedOutput> {
    evaluator: PreparedDirectJit,
    interface: TypedInterface,
    marker: PhantomData<fn(&I) -> O>,
}

#[cfg(feature = "jit")]
impl<I: TypedInput, O: TypedOutput> TypedJitMonitor<I, O> {
    pub fn compile_checked(
        specification: CheckedDsrvSpecification,
    ) -> Result<Self, TypedBindingError> {
        let program = DataflowProgram::compile_checked(specification)?;
        Self::from_program(program)
    }

    pub fn from_program(program: DataflowProgram) -> Result<Self, TypedBindingError> {
        let layout = TypedIoLayout::bind::<I, O>(&program)?;
        let interface = TypedInterface::from_program(&program);
        let monitor = DataflowMonitor::from_program(program);
        let evaluator = monitor
            .into_direct_jit(layout)
            .map_err(|_| TypedBindingError::UnsupportedPlan { backend: "JIT" })?;
        Ok(Self {
            evaluator,
            interface,
            marker: PhantomData,
        })
    }

    pub fn interface(&self) -> &TypedInterface {
        &self.interface
    }

    #[inline(always)]
    pub fn evaluate(&mut self, input: &I) -> O {
        let mut output = MaybeUninit::<O>::uninit();
        unsafe {
            self.evaluator.evaluate(
                input as *const I as *const u8,
                output.as_mut_ptr() as *mut u8,
            );
            // SAFETY: construction binds every output field to the native kernel. Checked kernels
            // panic before returning when they do not initialize the output.
            output.assume_init()
        }
    }
}

#[cfg(feature = "jit")]
impl<I: TypedInput, O: TypedOutput> TypedMonitor for TypedJitMonitor<I, O> {
    type Input = I;
    type Output = O;

    #[inline(always)]
    fn evaluate(&mut self, input: &I) -> O {
        self.evaluate(input)
    }
}
