//! The typed front door to a dataflow monitor.
//!
//! [`TypedDataflowMonitor`] binds a Rust tuple to a checked specification once and then drives the
//! ordinary [`DataflowMonitor`] lifecycle. Evaluation goes through the normal `Value` path, so
//! hotness counting, reconfiguration, and every language feature behave exactly as they do for an
//! untyped monitor; the typed rows only replace `Value` construction at the API boundary.
//!
//! With the `jit` feature and [`TypedDataflowMonitor::compile_checked_with_jit`], the monitor also
//! watches its own hotness policy. Once it promotes itself to a whole-schedule native artifact the
//! warmed state moves into a direct native entry, and later ticks neither construct nor decode a
//! [`Value`] at all.
//!
//! This is the typed entry point to prefer. Use [`super::TypedJitMonitor`] only when native
//! compilation must be guaranteed at construction time rather than reached by warming.
//!
//! # Limits
//!
//! Rows are tuples of at most eight [`super::TypedScalar`] fields (`i64`, `f64`, `bool`), matched
//! positionally against the specification's input and output variables. Sparse ticks are not
//! representable: a `NoVal` or `Deferred` output makes [`TypedDataflowMonitor::try_evaluate`]
//! return [`TypedEvaluationError::NonConcreteOutput`].

use std::marker::PhantomData;
use std::mem::MaybeUninit;

use thiserror::Error;

use super::{
    TypedBindingError, TypedInput, TypedInterface, TypedIoLayout, TypedMonitor, TypedOutput,
    load_typed_scalar, store_typed_scalar,
};
use crate::core::Value;
use crate::dataflow::execution::quickening::ScalarValue;
use crate::dataflow::{DataflowEvaluationError, DataflowMonitor, DataflowProgram};
use crate::lang::dsrv::ast::CheckedDsrvSpecification;

#[cfg(feature = "jit")]
use crate::dataflow::execution::jit::PreparedDirectJit;
#[cfg(feature = "jit")]
use crate::dataflow::{JitConfig, JitReport};

/// Failure while a typed interface is driving the ordinary monitor lifecycle.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TypedEvaluationError {
    #[error(transparent)]
    Evaluation(#[from] DataflowEvaluationError),
    #[error("monitor output field {index} was not a concrete scalar value")]
    NonConcreteOutput { index: usize },
    #[error("the warmed whole-schedule JIT could not bind its direct entry")]
    DirectActivation,
}

struct TypedWarmState {
    monitor: DataflowMonitor,
    layout: TypedIoLayout,
    input: Vec<Value>,
    output: Vec<Value>,
}

/// A typed interface to the ordinary monitor and, when enabled, its hotness-driven JIT lifecycle.
///
/// Evaluation follows the normal monitor path using reusable `Value` storage. With the `jit`
/// feature, once the monitor selects a whole-schedule JIT its warmed state moves into the direct
/// native entry and subsequent ticks do not construct or decode `Value`s.
pub struct TypedDataflowMonitor<I: TypedInput, O: TypedOutput> {
    #[cfg(feature = "jit")]
    direct: Option<PreparedDirectJit>,
    warming: Option<Box<TypedWarmState>>,
    interface: TypedInterface,
    #[cfg(feature = "jit")]
    report: Option<JitReport>,
    marker: PhantomData<fn(&I) -> O>,
}

impl<I: TypedInput, O: TypedOutput> TypedDataflowMonitor<I, O> {
    /// Binds typed rows to a checked specification, without enabling native compilation.
    pub fn compile_checked(
        specification: CheckedDsrvSpecification,
    ) -> Result<Self, TypedBindingError> {
        let program = DataflowProgram::compile_checked(specification)?;
        Self::from_program(program)
    }

    /// Binds typed rows to an already-compiled program, without enabling native compilation.
    pub fn from_program(program: DataflowProgram) -> Result<Self, TypedBindingError> {
        Self::build(program, |_monitor, _layout| {})
    }

    /// Binds typed rows and enables the monitor's hotness-driven native tier.
    #[cfg(feature = "jit")]
    pub fn compile_checked_with_jit(
        specification: CheckedDsrvSpecification,
        config: JitConfig,
    ) -> Result<Self, TypedBindingError> {
        let program = DataflowProgram::compile_checked(specification)?;
        Self::from_program_with_jit(program, config)
    }

    /// Binds typed rows to an already-compiled program and enables its native tier.
    #[cfg(feature = "jit")]
    pub fn from_program_with_jit(
        program: DataflowProgram,
        config: JitConfig,
    ) -> Result<Self, TypedBindingError> {
        Self::build(program, |monitor, layout| {
            monitor.enable_typed_jit(config, layout.clone())
        })
    }

    fn build(
        program: DataflowProgram,
        configure: impl FnOnce(&mut DataflowMonitor, &TypedIoLayout),
    ) -> Result<Self, TypedBindingError> {
        let layout = TypedIoLayout::bind::<I, O>(&program)?;
        let interface = TypedInterface::from_program(&program);
        let input = vec![Value::NoVal; layout.inputs.len()];
        let output = vec![Value::NoVal; layout.outputs.len()];
        let mut monitor = DataflowMonitor::from_program(program);
        configure(&mut monitor, &layout);
        #[cfg(feature = "jit")]
        let report = monitor.jit_report().cloned();
        let mut typed = Self {
            #[cfg(feature = "jit")]
            direct: None,
            warming: Some(Box::new(TypedWarmState {
                monitor,
                layout,
                input,
                output,
            })),
            interface,
            #[cfg(feature = "jit")]
            report,
            marker: PhantomData,
        };
        typed
            .prepare_direct_if_ready()
            .map_err(|_| TypedBindingError::UnsupportedPlan {
                backend: "typed whole-monitor JIT",
            })?;
        Ok(typed)
    }

    /// The positional variable order bound to this monitor's input and output rows.
    pub fn interface(&self) -> &TypedInterface {
        &self.interface
    }

    #[cfg(feature = "jit")]
    pub fn jit_report(&self) -> Option<&JitReport> {
        self.warming
            .as_ref()
            .and_then(|state| state.monitor.jit_report())
            .or(self.report.as_ref())
    }

    /// Whether ticks currently bypass `Value` construction through a native entry.
    #[cfg(feature = "jit")]
    pub fn is_direct_jit_active(&self) -> bool {
        self.direct.is_some()
    }

    #[cfg(all(test, feature = "jit"))]
    pub(crate) fn fail_next_direct_extraction(&mut self) {
        self.warming
            .as_mut()
            .expect("direct monitor has no warming executor")
            .monitor
            .fail_next_direct_extraction();
    }

    /// Evaluates one tick, panicking on failure.
    ///
    /// Prefer [`Self::try_evaluate`] unless the specification is known to produce a concrete
    /// scalar for every output on every tick.
    #[inline(always)]
    pub fn evaluate(&mut self, input: &I) -> O {
        self.try_evaluate(input)
            .unwrap_or_else(|error| panic!("typed monitor evaluation failed: {error}"))
    }

    /// Evaluates one tick.
    #[inline(always)]
    pub fn try_evaluate(&mut self, input: &I) -> Result<O, TypedEvaluationError> {
        #[cfg(feature = "jit")]
        if let Some(direct) = &mut self.direct {
            let mut output = MaybeUninit::<O>::uninit();
            // SAFETY: construction bound both pointers to the tuple layouts compiled into this entry.
            unsafe {
                direct.evaluate(
                    input as *const I as *const u8,
                    output.as_mut_ptr() as *mut u8,
                );
                return Ok(output.assume_init());
            }
        }
        self.evaluate_warming(input)
    }

    #[cfg_attr(feature = "jit", cold)]
    fn evaluate_warming(&mut self, input: &I) -> Result<O, TypedEvaluationError> {
        let state = self
            .warming
            .as_mut()
            .expect("typed monitor has neither a warm nor direct executor");
        let input_base = input as *const I as *const u8;
        for (index, field) in state.layout.inputs.iter().copied().enumerate() {
            // SAFETY: the layout was bound to `I` during construction.
            state.input[index] = unsafe { load_typed_scalar(input_base, field) }.into_value();
        }
        state.monitor.evaluate(&state.input, &mut state.output)?;

        let mut output = MaybeUninit::<O>::uninit();
        let output_base = output.as_mut_ptr() as *mut u8;
        for (index, (value, field)) in state
            .output
            .iter()
            .zip(state.layout.outputs.iter().copied())
            .enumerate()
        {
            let Some(value) = ScalarValue::from_untyped_value(value) else {
                return Err(TypedEvaluationError::NonConcreteOutput { index });
            };
            // SAFETY: the layout was bound to `O`; success means this concrete scalar initialized
            // the corresponding field.
            if !unsafe { store_typed_scalar(output_base, field, value) } {
                return Err(TypedEvaluationError::NonConcreteOutput { index });
            }
        }
        #[cfg(feature = "jit")]
        {
            self.report = state.monitor.jit_report().cloned();
        }
        self.prepare_direct_if_ready()?;
        // SAFETY: every output field was initialized above.
        Ok(unsafe { output.assume_init() })
    }

    #[cfg(feature = "jit")]
    fn prepare_direct_if_ready(&mut self) -> Result<(), TypedEvaluationError> {
        let ready = self
            .warming
            .as_ref()
            .is_some_and(|state| state.monitor.direct_entry_ready());
        if !ready {
            return Ok(());
        }
        let state = self.warming.as_mut().unwrap();
        let Some(prepared) = state
            .monitor
            .take_prepared_direct()
            .map_err(|_| TypedEvaluationError::DirectActivation)?
        else {
            return Ok(());
        };
        self.report = state.monitor.jit_report().cloned();
        self.direct = Some(prepared);
        self.warming = None;
        Ok(())
    }

    #[cfg(not(feature = "jit"))]
    fn prepare_direct_if_ready(&mut self) -> Result<(), TypedEvaluationError> {
        Ok(())
    }
}

impl<I: TypedInput, O: TypedOutput> TypedMonitor for TypedDataflowMonitor<I, O> {
    type Input = I;
    type Output = O;

    #[inline(always)]
    fn evaluate(&mut self, input: &I) -> O {
        self.evaluate(input)
    }
}
