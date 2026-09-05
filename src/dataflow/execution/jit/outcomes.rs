#[cfg_attr(not(feature = "jit"), allow(dead_code))]
pub(in crate::dataflow) enum WholeTickOutcome {
    NotAvailable,
    Completed,
    CompletedAndCommitted,
    CanonicalFallback,
}

#[cfg_attr(not(feature = "jit"), allow(dead_code))]
pub(in crate::dataflow) enum NativeRegionOutcome {
    Unavailable,
    Completed,
    Deoptimized,
}
