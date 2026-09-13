use super::ContextTransferPolicy;
#[cfg(feature = "jit")]
use super::execution::jit::PreparedDirectJit;
use super::execution::monitor_execution::MonitorExecution;
use super::expression_activation::ExpressionActivationState;
#[cfg(feature = "jit")]
use super::history::HistoryAccess;
use super::history::{HistoryId, HistoryStore};
use super::program::DataflowProgram;
use super::reconfiguration::{DefinitionKey, InterfaceRevision, MonitorRevision};
use super::scheduler::Scheduler;
#[cfg(feature = "jit")]
use super::typed::TypedIoLayout;
use crate::VarName;
use crate::core::Value;

mod evaluation;
mod history;
mod reconfiguration;
#[cfg(test)]
mod tests;

pub(crate) use reconfiguration::MonitorReconfigurationPlan;

#[cfg(test)]
pub(in crate::dataflow) use tests::test_support;

#[derive(Clone, Copy)]
pub(crate) struct MonitorConfiguration {
    pub(crate) quickening: bool,
    #[cfg(feature = "jit")]
    pub(crate) jit: Option<super::JitConfig>,
    reconfiguration_transfer_policy: ContextTransferPolicy,
}

impl Default for MonitorConfiguration {
    fn default() -> Self {
        Self {
            quickening: true,
            #[cfg(feature = "jit")]
            jit: None,
            reconfiguration_transfer_policy: ContextTransferPolicy::MatchingStreamState,
        }
    }
}

/// A compiled, stateful synchronous dataflow monitor.
///
/// Each tick evaluates expression sources, resolves reconfigurable expressions, updates the dependency
/// schedule, evaluates every remaining stream once, and commits staged temporal state. Static
/// monitors are the empty-reconfiguration quickening of the same flow.
pub struct DataflowMonitor {
    original_program: DataflowProgram,
    configuration: MonitorConfiguration,
    program: DataflowProgram,
    execution: MonitorExecution,
    reconfiguration_state: ExpressionActivationState,
    scheduler: Scheduler,
    environment_values: Vec<Value>,
    history_store: HistoryStore,
    history_bindings: Box<[Option<HistoryId>]>,
    effective_history_depths: Box<[usize]>,
    retained_environment_values: Option<Vec<Value>>,
    revision: MonitorRevision,
    interface_revision: InterfaceRevision,
    failed: bool,
    #[cfg(test)]
    expression_scan_count: usize,
    #[cfg(test)]
    last_reconfiguration_dependencies_changed: bool,
}

impl DataflowMonitor {
    /// Create a stateful monitor from an immutable compiled definition.
    pub fn from_program(program: DataflowProgram) -> Self {
        Self::from_configured_program(program.clone(), program, MonitorConfiguration::default())
    }

    fn from_configured_program(
        original_program: DataflowProgram,
        program: DataflowProgram,
        configuration: MonitorConfiguration,
    ) -> Self {
        let environment_size = program.environment_size();
        let monitor_plan = program.monitor_plan();
        let reconfiguration_state = ExpressionActivationState::new(
            &monitor_plan.reconfigurable_expressions,
            monitor_plan.dependencies.stream_count(),
        );
        let scheduler = Scheduler::new(
            monitor_plan.stream_slots,
            &monitor_plan.dependencies,
            reconfiguration_state.source_streams(),
        );
        let mut history_store = HistoryStore::new();
        let mut history_bindings = vec![None; environment_size].into_boxed_slice();
        let mut effective_history_depths = vec![0; environment_size].into_boxed_slice();
        for requirement in program.history_requirements().iter() {
            let slot = requirement.slot();
            let history_id = history_store.allocate(requirement.depth());
            history_bindings[slot.index()] = Some(history_id);
            effective_history_depths[slot.index()] = requirement.depth();
        }
        let mut execution = MonitorExecution::new_with_source_prelude_and_history(
            program.stream_programs().to_vec(),
            monitor_plan.stream_slots,
            reconfiguration_state.source_order(),
            scheduler.execution_schedule().evaluation_order(),
            monitor_plan.temporal_streams.as_slice(),
            &history_bindings,
        );
        execution.set_quickening(configuration.quickening);
        #[cfg(feature = "jit")]
        if let Some(config) = configuration.jit {
            execution.enable_jit(config);
        }

        let retained_environment_values = (!monitor_plan.reconfigurable_expressions.is_empty())
            .then(|| vec![Value::NoVal; environment_size]);
        Self {
            original_program,
            configuration,
            program,
            execution,
            reconfiguration_state,
            scheduler,
            environment_values: vec![Value::NoVal; environment_size],
            history_store,
            history_bindings,
            effective_history_depths,
            retained_environment_values,
            revision: MonitorRevision::INITIAL,
            interface_revision: InterfaceRevision::INITIAL,
            failed: false,
            #[cfg(test)]
            expression_scan_count: 0,
            #[cfg(test)]
            last_reconfiguration_dependencies_changed: false,
        }
    }

    /// Create a stateful monitor from an immutable compiled definition.
    pub fn new(program: DataflowProgram) -> Self {
        Self::from_program(program)
    }

    /// Create a monitor from a compiled program and enable integrated native execution.
    ///
    /// The selected JIT activation policy is retained by [`Self::reset`]. Native artifacts and
    /// hotness are session state and are recreated rather than retained.
    #[cfg(feature = "jit")]
    pub fn from_program_with_jit(program: DataflowProgram, config: super::JitConfig) -> Self {
        let mut configuration = MonitorConfiguration::default();
        configuration.jit = Some(config);
        Self::from_configured_program(program.clone(), program, configuration)
    }

    /// Discard all session state and reconstruct this monitor from its original compiled program.
    ///
    /// Reset preserves the latest quickening, integrated-JIT, and internal reconfiguration-transfer
    /// selections. It restores the original definition even if [`Self::reconfigure`] replaced the
    /// active root, clears a terminal evaluation failure, and returns both revisions to their
    /// initial values. It does not parse or compile the original dataflow definition again.
    pub fn reset(&mut self) {
        let fresh = Self::from_configured_program(
            self.original_program.clone(),
            self.original_program.clone(),
            self.configuration,
        );
        *self = fresh;
    }

    /// Return the immutable compiled definition backing this monitor.
    pub fn program(&self) -> &DataflowProgram {
        &self.program
    }

    pub fn input_vars(&self) -> &[VarName] {
        self.program.input_vars()
    }

    pub fn output_vars(&self) -> &[VarName] {
        self.program.output_vars()
    }

    pub fn revision(&self) -> MonitorRevision {
        self.revision
    }

    pub fn interface_revision(&self) -> InterfaceRevision {
        self.interface_revision
    }

    pub fn definition_key(&self) -> &DefinitionKey {
        self.program.definition_key()
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn into_direct_jit(
        self,
        layout: TypedIoLayout,
    ) -> Result<PreparedDirectJit, ()> {
        if self.failed || self.revision != MonitorRevision::INITIAL {
            return Err(());
        }
        let Self {
            execution,
            history_store,
            history_bindings,
            ..
        } = self;
        let history_access = (!history_store.is_empty())
            .then(|| HistoryAccess::new(&history_store, &history_bindings));
        execution.into_direct_jit(layout, history_access)
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn direct_entry_ready(&self) -> bool {
        !self.failed
            && self.revision == MonitorRevision::INITIAL
            && self.execution.direct_entry_ready()
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn take_prepared_direct(
        &mut self,
    ) -> Result<Option<PreparedDirectJit>, ()> {
        if self.failed || self.revision != MonitorRevision::INITIAL {
            return Err(());
        }
        let history_access = (!self.history_store.is_empty())
            .then(|| HistoryAccess::new(&self.history_store, &self.history_bindings));
        self.execution.take_prepared_direct(history_access)
    }

    #[cfg(all(test, feature = "jit"))]
    pub(in crate::dataflow) fn fail_next_direct_extraction(&mut self) {
        self.execution.fail_next_direct_extraction();
    }
}
