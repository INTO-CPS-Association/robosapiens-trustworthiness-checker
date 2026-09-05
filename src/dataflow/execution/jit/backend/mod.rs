//! Cranelift lowering and code generation for typed scalar graphs and complete runs.
//!
//! The backend consumes bound canonical IR and returns native artifacts. Monitor lifecycle,
//! scheduling, hotness, and deoptimization remain in the coordinator and runtime modules.
//! Unsupported IR produces no artifact; backend failures are returned as diagnostics.

mod artifact;
mod codegen;
mod ir;
mod lowering;

pub(super) use artifact::{
    CompiledScalarRegion, CompiledTemporalMonitor, DirectFunction, InputSource, InputSpec,
    TemporalNodeLayout, TemporalRunStateLayout, TemporalStateLayout, ValueScalarFunction,
    ValueTemporalFunction, decode,
};
#[cfg(test)]
pub(crate) use codegen::{compile_count, reset_compile_count};
pub(super) use codegen::{compile_scalar_region, compile_temporal_monitor};

#[cfg(test)]
mod tests;
