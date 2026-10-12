//! The hook surface the execution owners call.
//!
//! [`ExecutionProbe`] is owned by `MonitorExecution` and lent to the step loop. Without the
//! `diagnostics` feature it is a zero-sized type whose methods are empty and inlined, and
//! [`Stamp`] is `()`, so every hook disappears from the compiled evaluator. With the feature it
//! holds an optional boxed recorder; a disabled probe costs one branch per hook.

/// Which range of an execution plan a step belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "diagnostics", derive(serde::Serialize))]
#[cfg_attr(feature = "diagnostics", serde(rename_all = "snake_case"))]
pub(in crate::dataflow) enum StepPhase {
    /// The `dynamic`/`defer` source prelude, before the source barrier.
    Source,
    /// The scheduled main range.
    Main,
    /// One whole-schedule native artifact covering every stream.
    Whole,
}

/// The executor that actually ran one step on one tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "diagnostics", derive(serde::Serialize))]
#[cfg_attr(feature = "diagnostics", serde(rename_all = "snake_case"))]
pub(in crate::dataflow) enum StepPath {
    /// A graph step evaluated entirely by the canonical interpreter.
    Canonical,
    /// A graph step whose scalar islands all ran quickened.
    Islands,
    /// A graph step where at least one island declined the row and ran canonically.
    IslandsDeclined,
    /// A scalar region run by its native artifact.
    RegionNative,
    /// A scalar region run by the quickened interpreter.
    RegionQuickened,
    /// A scalar region whose row was rejected by the faster tiers and ran canonically.
    RegionCanonical,
    /// The whole schedule ran in one native artifact.
    WholeNative,
    /// The whole-schedule native artifact deoptimized and the tick was replayed canonically.
    WholeNativeFallback,
    /// The step returned an evaluation error. No executor path is claimed for it.
    Failed,
}

impl StepPath {
    #[cfg(feature = "diagnostics")]
    pub(in crate::dataflow) const COUNT: usize = 9;

    #[cfg(feature = "diagnostics")]
    pub(in crate::dataflow) const ALL: [Self; Self::COUNT] = [
        Self::Canonical,
        Self::Islands,
        Self::IslandsDeclined,
        Self::RegionNative,
        Self::RegionQuickened,
        Self::RegionCanonical,
        Self::WholeNative,
        Self::WholeNativeFallback,
        Self::Failed,
    ];

    #[cfg(feature = "diagnostics")]
    pub(in crate::dataflow) const fn index(self) -> usize {
        self as usize
    }
}

#[cfg(feature = "diagnostics")]
mod enabled {
    use std::time::Instant;

    use super::super::recorder::DataflowRecorder;
    use super::{StepPath, StepPhase};

    pub(in crate::dataflow) type Stamp = Option<Instant>;

    #[derive(Default)]
    pub(in crate::dataflow) struct ExecutionProbe {
        recorder: Option<Box<DataflowRecorder>>,
    }

    impl ExecutionProbe {
        pub(in crate::dataflow) fn disabled() -> Self {
            Self { recorder: None }
        }

        pub(in crate::dataflow) fn install(&mut self, recorder: Box<DataflowRecorder>) {
            self.recorder = Some(recorder);
        }

        #[inline]
        pub(in crate::dataflow) fn recorder(&self) -> Option<&DataflowRecorder> {
            self.recorder.as_deref()
        }

        #[inline]
        pub(in crate::dataflow) fn recorder_mut(&mut self) -> Option<&mut DataflowRecorder> {
            self.recorder.as_deref_mut()
        }

        pub(in crate::dataflow) fn take(&mut self) -> Self {
            std::mem::take(self)
        }

        /// A timestamp when any recording is enabled.
        #[inline]
        pub(in crate::dataflow) fn stamp(&self) -> Stamp {
            self.recorder.as_ref().map(|_| Instant::now())
        }

        /// A timestamp when per-step timing is enabled.
        #[inline]
        pub(in crate::dataflow) fn step_stamp(&self) -> Stamp {
            match &self.recorder {
                Some(recorder) if recorder.step_timing() => Some(Instant::now()),
                _ => None,
            }
        }

        #[inline]
        pub(in crate::dataflow) fn record_step(
            &mut self,
            stamp: Stamp,
            phase: StepPhase,
            index: usize,
            path: StepPath,
        ) {
            if let (Some(start), Some(recorder)) = (stamp, self.recorder.as_deref_mut()) {
                recorder.record_step(start, phase, index, path);
            }
        }

        /// Record a step that returned an error, before the error propagates. Recorded whenever a
        /// recorder is installed; without step timing its elapsed time is zero.
        #[cold]
        pub(in crate::dataflow) fn record_step_failed(
            &mut self,
            stamp: Stamp,
            phase: StepPhase,
            index: usize,
        ) {
            if let Some(recorder) = self.recorder.as_deref_mut() {
                let start = stamp.unwrap_or_else(Instant::now);
                recorder.record_step(start, phase, index, StepPath::Failed);
            }
        }

        #[inline]
        pub(in crate::dataflow) fn record_commit(&mut self, stamp: Stamp, streams: usize) {
            if let (Some(start), Some(recorder)) = (stamp, self.recorder.as_deref_mut()) {
                recorder.record_commit(start, streams);
            }
        }
    }
}

#[cfg(not(feature = "diagnostics"))]
mod disabled {
    use super::{StepPath, StepPhase};

    pub(in crate::dataflow) type Stamp = ();

    #[derive(Default)]
    pub(in crate::dataflow) struct ExecutionProbe;

    impl ExecutionProbe {
        #[inline(always)]
        pub(in crate::dataflow) fn disabled() -> Self {
            Self
        }

        #[inline(always)]
        pub(in crate::dataflow) fn take(&mut self) -> Self {
            Self
        }

        #[inline(always)]
        pub(in crate::dataflow) fn stamp(&self) -> Stamp {}

        #[inline(always)]
        pub(in crate::dataflow) fn step_stamp(&self) -> Stamp {}

        #[inline(always)]
        pub(in crate::dataflow) fn record_step(
            &mut self,
            _stamp: Stamp,
            _phase: StepPhase,
            _index: usize,
            _path: StepPath,
        ) {
        }

        #[inline(always)]
        pub(in crate::dataflow) fn record_step_failed(
            &mut self,
            _stamp: Stamp,
            _phase: StepPhase,
            _index: usize,
        ) {
        }

        #[inline(always)]
        pub(in crate::dataflow) fn record_commit(&mut self, _stamp: Stamp, _streams: usize) {}
    }
}

#[cfg(not(feature = "diagnostics"))]
pub(in crate::dataflow) use disabled::{ExecutionProbe, Stamp};
#[cfg(feature = "diagnostics")]
pub(in crate::dataflow) use enabled::{ExecutionProbe, Stamp};
