use crate::dataflow::Value;

#[cfg_attr(not(feature = "jit"), allow(dead_code))]
pub(in crate::dataflow) enum FusedTickOutcome {
    NotAvailable,
    Completed,
    CompletedAndCommitted,
    CanonicalFallback,
}

#[cfg_attr(not(feature = "jit"), allow(dead_code))]
pub(in crate::dataflow) enum GraphTickOutcome {
    NotAvailable,
    Value(Value),
    CanonicalFallback,
}
