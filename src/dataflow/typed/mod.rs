//! Statically typed rows for dataflow monitor evaluation.
//!
//! A typed monitor binds a Rust tuple to a checked specification once, matching tuple fields
//! positionally against the specification's input and output variables. Evaluation then moves
//! scalars through the bound row instead of constructing [`crate::Value`]s at the API boundary.
//!
//! [`TypedDataflowMonitor`] is the entry point to prefer: it drives the ordinary
//! [`crate::dataflow::DataflowMonitor`] lifecycle and, with the `jit` feature, switches to a
//! direct native entry once the monitor's own hotness policy promotes it. `TypedJitMonitor` is
//! the narrower `jit`-only alternative for callers that need native compilation to succeed at
//! construction time or not at all.

#[cfg(feature = "jit")]
mod jit;
mod layout;
mod monitor;
mod row;

#[cfg(feature = "jit")]
pub(in crate::dataflow) use layout::TypedBoundField;
pub use layout::{TypedBindingError, TypedInterface};
pub(in crate::dataflow) use layout::{TypedIoLayout, load_typed_scalar, store_typed_scalar};
pub use row::{TypedField, TypedInput, TypedKind, TypedOutput, TypedScalar};

#[cfg(feature = "jit")]
pub use jit::TypedJitMonitor;
pub use monitor::{TypedDataflowMonitor, TypedEvaluationError};

/// Common statically dispatched interface used by typed monitor backends.
pub trait TypedMonitor {
    type Input;
    type Output;

    fn evaluate(&mut self, input: &Self::Input) -> Self::Output;
}

#[cfg(test)]
mod tests;
