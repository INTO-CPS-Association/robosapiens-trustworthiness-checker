use super::super::super::history::HistoryId;
use super::super::super::ir::StreamProgram;
use super::super::evaluator_state::EvaluatorState;
#[cfg(feature = "jit")]
use super::super::jit::JittedGraphEvaluator;
use super::super::quickening;
use crate::core::Value;
use std::rc::Rc;

/// Evaluator-local state for the canonical, quickened, and per-stream native tiers.
/// Field order keeps colder tier ownership behind the hot canonical state.
#[derive(Clone)]
#[repr(C)]
pub(in crate::dataflow) struct EvaluatorTierStates {
    pub(in crate::dataflow) canonical: Box<EvaluatorState>,
    pub(super) quickening: Option<quickening::State>,
    pub(super) quick_plan: Option<Rc<quickening::Plan>>,
    #[cfg(feature = "jit")]
    pub(in crate::dataflow) native: Option<JittedGraphEvaluator>,
}

impl EvaluatorTierStates {
    pub(super) fn new(program: &StreamProgram, history_bindings: &[Option<HistoryId>]) -> Self {
        let canonical = Box::new(EvaluatorState::new_with_history(
            &program.graph,
            history_bindings,
        ));
        let quick_plan = program
            .is_infallible()
            .then(|| quickening::Plan::new(&program.graph))
            .flatten();
        let quickening = quick_plan.as_ref().map(quickening::State::new);
        let quick_plan = quick_plan.map(Rc::new);
        debug_assert_eq!(canonical.node_values.len(), program.graph.nodes.len());
        debug_assert_eq!(canonical.node_states.len(), program.graph.nodes.len());
        Self {
            canonical,
            quickening,
            quick_plan,
            #[cfg(feature = "jit")]
            native: None,
        }
    }

    pub(super) fn reset(&mut self) {
        self.canonical.reset();
        if let Some(quickening) = &mut self.quickening {
            quickening.reset();
        }
        #[cfg(feature = "jit")]
        if let Some(native) = &mut self.native {
            native.reset_after_context_transfer();
        }
    }

    pub(super) fn move_exact_from(&mut self, source: &mut Self) {
        #[cfg(feature = "jit")]
        let target_native = self.native.take();
        #[cfg(feature = "jit")]
        let source_native = source.native.take();

        std::mem::swap(self, source);

        // Compiled native artifacts remain bound to their evaluator's program and environment ABI.
        #[cfg(feature = "jit")]
        {
            self.native = target_native;
            source.native = source_native;
        }
        self.canonical.node_values.fill(Value::NoVal);
    }

    #[cold]
    #[inline(never)]
    pub(super) fn materialize_quickening(&mut self) {
        if let Some(quickening) = self.quickening.take() {
            quickening.materialize_into(self.canonical.as_mut());
        }
    }

    pub(super) fn detach_quick_plan(&mut self) {
        self.quick_plan = None;
    }

    pub(super) fn install_adaptive_plan(&mut self, plan: quickening::Plan) {
        let quick_plan = Rc::new(plan);
        let mut quickening = quickening::State::new(quick_plan.as_ref());
        quickening.synchronize_from(self.canonical.as_ref());
        self.quickening = Some(quickening);
        self.quick_plan = Some(quick_plan);
    }
}
