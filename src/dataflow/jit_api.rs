/// Activation policy for the integrated JIT. All configurations use the same optimizer and
/// generated-code path; only the point at which native compilation occurs differs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JitConfig {
    /// `None` compiles eagerly. `Some(n)` runs `n` canonical ticks before compiling all eligible
    /// graphs together, keeping short-lived monitors on the low-startup interpreter tier.
    hotness_threshold: Option<u64>,
}

impl JitConfig {
    pub const fn eager() -> Self {
        Self {
            hotness_threshold: None,
        }
    }

    pub const fn after_events(events: u64) -> Self {
        Self {
            hotness_threshold: Some(events),
        }
    }

    pub(in crate::dataflow) const fn hotness_threshold(self) -> Option<u64> {
        self.hotness_threshold
    }
}

impl Default for JitConfig {
    fn default() -> Self {
        Self::eager()
    }
}

/// The native execution layout selected for a monitor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JitPlan {
    /// The monitor is still below its configured hotness threshold.
    Pending,
    /// One native artifact evaluates the complete static schedule.
    Fused,
    /// Eligible streams have individual native artifacts; other streams remain interpreted.
    PerStream,
    /// Native compilation produced no usable artifact.
    Unavailable,
}

/// Observable result of enabling the JIT.
///
/// Unsupported streams are normal and continue through the canonical interpreter. A backend
/// error is also non-fatal: execution remains canonical, but the error is retained here rather
/// than silently discarded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JitReport {
    plan: JitPlan,
    compiled_artifacts: usize,
    unsupported_streams: Vec<usize>,
    scheduled_temporal_streams: Vec<usize>,
    complete_temporal_kernel_streams: Vec<usize>,
    backend_error: Option<String>,
}

impl JitReport {
    pub fn plan(&self) -> JitPlan {
        self.plan
    }

    pub fn compiled_artifacts(&self) -> usize {
        self.compiled_artifacts
    }

    pub fn unsupported_streams(&self) -> &[usize] {
        &self.unsupported_streams
    }

    /// Stream indices whose native regions are fed by evaluator-scheduled temporal state.
    pub fn scheduled_temporal_streams(&self) -> &[usize] {
        &self.scheduled_temporal_streams
    }

    /// Stream indices evaluated and committed by one complete native temporal kernel call.
    pub fn complete_temporal_kernel_streams(&self) -> &[usize] {
        &self.complete_temporal_kernel_streams
    }

    pub fn backend_error(&self) -> Option<&str> {
        self.backend_error.as_deref()
    }

    pub(in crate::dataflow) fn pending() -> Self {
        Self {
            plan: JitPlan::Pending,
            compiled_artifacts: 0,
            unsupported_streams: Vec::new(),
            scheduled_temporal_streams: Vec::new(),
            complete_temporal_kernel_streams: Vec::new(),
            backend_error: None,
        }
    }

    pub(in crate::dataflow) fn compiled(
        plan: JitPlan,
        compiled_artifacts: usize,
        unsupported_streams: Vec<usize>,
        scheduled_temporal_streams: Vec<usize>,
        complete_temporal_kernel_streams: Vec<usize>,
        backend_error: Option<String>,
    ) -> Self {
        Self {
            plan,
            compiled_artifacts,
            unsupported_streams,
            scheduled_temporal_streams,
            complete_temporal_kernel_streams,
            backend_error,
        }
    }
}
