use super::ContextTransferPolicy;
use super::execution::monitor_execution::MonitorExecution;
use super::execution_plan::ReconfigurableExpressionState;
use super::history::{HistoryId, HistoryStore};
use super::program::DataflowProgram;
use super::reconfiguration::{DefinitionKey, InterfaceRevision, MonitorRevision};
use super::scheduler::Scheduler;
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
/// A compiled, stateful synchronous dataflow monitor.
///
/// Each tick evaluates expression sources, resolves reconfigurable expressions, updates the dependency
/// schedule, evaluates every remaining stream once, and commits staged temporal state. Static
/// monitors are the empty-reconfiguration quickening of the same flow.
pub struct DataflowMonitor {
    program: DataflowProgram,
    execution: MonitorExecution,
    reconfiguration_state: ReconfigurableExpressionState,
    scheduler: Scheduler,
    environment_values: Vec<Value>,
    history_store: HistoryStore,
    history_bindings: Box<[Option<HistoryId>]>,
    effective_history_depths: Box<[usize]>,
    retained_environment_values: Option<Vec<Value>>,
    revision: MonitorRevision,
    interface_revision: InterfaceRevision,
    reconfiguration_transfer_policy: ContextTransferPolicy,
    failed: bool,
    #[cfg(test)]
    expression_scan_count: usize,
    #[cfg(test)]
    last_reconfiguration_dependencies_changed: bool,
}

impl DataflowMonitor {
    /// Create a stateful monitor from an immutable compiled definition.
    pub fn from_program(program: DataflowProgram) -> Self {
        let environment_size = program.environment_size();
        let monitor_plan = program.monitor_plan();
        let reconfiguration_state = ReconfigurableExpressionState::new(
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
        let execution = MonitorExecution::new_with_source_prelude_and_history(
            program.stream_programs().to_vec(),
            monitor_plan.stream_slots,
            reconfiguration_state.source_order(),
            scheduler.execution_schedule().evaluation_order(),
            monitor_plan.temporal_streams.as_slice(),
            &history_bindings,
        );

        let retained_environment_values = (!monitor_plan.reconfigurable_expressions.is_empty())
            .then(|| vec![Value::NoVal; environment_size]);
        Self {
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
            reconfiguration_transfer_policy: ContextTransferPolicy::MatchingStreamState,
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
}
