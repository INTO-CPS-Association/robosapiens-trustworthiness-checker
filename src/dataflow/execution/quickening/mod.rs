//! The quick tier: one region executor over the shared scalar IR.
//!
//! [`region`] owns the physical plan, register layout, and lifting state for every scalar region an
//! execution plan selects, whether that region covers a run of whole streams or one island inside a
//! stream's otherwise canonical graph. [`scalar`] owns the runtime scalar value and the typed
//! operations both share.

#[cfg(test)]
mod eager_select_tests;
mod region;
mod scalar;
#[cfg(test)]
mod tests;

pub(in crate::dataflow) use region::{
    CanonicalArena, QuickenedRegionPlan, QuickenedRegionState, supports_program,
};
pub(in crate::dataflow) use scalar::{ScalarValue, retain_last};
