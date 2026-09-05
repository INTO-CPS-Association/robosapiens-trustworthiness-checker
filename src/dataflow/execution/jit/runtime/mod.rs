//! Typed direct and `Value` adapters for native scalar regions and temporal monitors.

mod scalar;
mod temporal;
mod value_adapter;

use crate::dataflow::*;

use super::backend::DirectFunction;

pub(in crate::dataflow) use scalar::NativeScalarRegion;
pub(in crate::dataflow) use temporal::NativeTemporalMonitor;

pub(in crate::dataflow) enum NativeRunOutcome {
    Completed,
    Fallback {
        replay_environment: Option<Vec<Value>>,
    },
}

enum DirectArtifact {
    Scalar { _evaluator: NativeScalarRegion },
    Temporal { _evaluator: NativeTemporalMonitor },
}

pub(in crate::dataflow) struct PreparedDirectJit {
    _artifact: DirectArtifact,
    function: DirectFunction,
    state: *mut i64,
}

impl PreparedDirectJit {
    fn scalar(evaluator: NativeScalarRegion, function: DirectFunction) -> Self {
        Self {
            _artifact: DirectArtifact::Scalar {
                _evaluator: evaluator,
            },
            function,
            state: std::ptr::null_mut(),
        }
    }

    fn temporal(mut evaluator: NativeTemporalMonitor, function: DirectFunction) -> Self {
        let state = evaluator.temporal_state_ptr();
        Self {
            _artifact: DirectArtifact::Temporal {
                _evaluator: evaluator,
            },
            function,
            state,
        }
    }

    #[inline(always)]
    pub(in crate::dataflow) unsafe fn evaluate(&mut self, input: *const u8, output: *mut u8) {
        // SAFETY: preparation binds the row pointers and state allocation to this function. The
        // owning artifact keeps both native code and temporal state alive without hot-path dispatch.
        match self.function {
            DirectFunction::Void(function) => unsafe { function(input, output, self.state) },
            DirectFunction::Checked(function) => {
                if unsafe { function(input, output, self.state) } != 0 {
                    panic!("integer division by zero in direct JIT monitor");
                }
            }
        }
    }
}
