//! The hook an input source calls to record its own phases.
//!
//! [`SourceProbe`] is carried by source configurations that support diagnostics. It is attached by
//! [`crate::io::InputPipeline::diagnostics`] when the source opens. Without the `diagnostics`
//! feature it is zero-sized and every method is empty; with it, an unattached probe costs one
//! branch per hook. Probes only time work the source already does: they never add reads, network
//! requests or ticks.

// Diagnostics only read state; none of this code may need `unsafe`.
#![forbid(unsafe_code)]

#[cfg(feature = "diagnostics")]
mod enabled {
    use std::time::Instant;

    use crate::diagnostics::{PipelinePhase, SourceRecorder};

    pub type SourceStamp = Option<Instant>;

    #[derive(Clone, Debug, Default)]
    pub struct SourceProbe(Option<SourceRecorder>);

    impl SourceProbe {
        pub fn attached(recorder: SourceRecorder) -> Self {
            Self(Some(recorder))
        }

        pub fn is_attached(&self) -> bool {
            self.0.is_some()
        }

        #[inline]
        pub fn stamp(&self) -> SourceStamp {
            self.0.as_ref().map(|_| Instant::now())
        }

        #[inline]
        pub fn phase(&self, stamp: SourceStamp, phase: PipelinePhase) {
            if let (Some(start), Some(recorder)) = (stamp, &self.0) {
                recorder.phase(phase, start);
            }
        }

        #[inline]
        pub fn count(&self, name: &'static str, amount: u64) {
            if let Some(recorder) = &self.0 {
                recorder.count(name, amount);
            }
        }

        #[inline]
        pub fn delivered(&self) {
            if let Some(recorder) = &self.0 {
                recorder.delivered();
            }
        }
    }
}

#[cfg(not(feature = "diagnostics"))]
mod disabled {
    pub type SourceStamp = ();

    /// Phases a source can record; only names without the `diagnostics` feature.
    #[derive(Clone, Copy, Debug)]
    pub enum SourcePhase {
        InputWait,
        InputDecode,
        InputProjection,
        InputConformance,
        InputProvenance,
    }

    #[derive(Clone, Debug, Default)]
    pub struct SourceProbe;

    impl SourceProbe {
        #[inline(always)]
        pub fn stamp(&self) -> SourceStamp {}

        #[inline(always)]
        pub fn phase(&self, _stamp: SourceStamp, _phase: SourcePhase) {}

        #[inline(always)]
        pub fn count(&self, _name: &'static str, _amount: u64) {}

        #[inline(always)]
        pub fn delivered(&self) {}
    }
}

#[cfg(feature = "diagnostics")]
pub use crate::diagnostics::PipelinePhase as SourcePhase;
#[cfg(not(feature = "diagnostics"))]
pub use disabled::{SourcePhase, SourceProbe, SourceStamp};
#[cfg(feature = "diagnostics")]
pub use enabled::{SourceProbe, SourceStamp};
