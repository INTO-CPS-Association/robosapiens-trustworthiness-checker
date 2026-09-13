use std::rc::Rc;

use super::VarName;
use super::environment::{EnvironmentLayout, EnvironmentSlot};
use super::history_requirements::HistoryRequirements;
use super::ir::StreamProgram;
use super::monitor_plan::MonitorPlan;
use super::reconfiguration::DefinitionKey;
use crate::fingerprint::FingerprintBuilder;

#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
thread_local! {
    static ROOT_COMPILE_COUNTS: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
}

/// An immutable compiled definition for a [`DataflowMonitor`](super::monitor::DataflowMonitor).
///
/// A program is a cheaply cloned handle to the bound stream semantics and fixed monitor layout. It
/// does not contain evaluator, scheduler, or other per-monitor execution state; call
/// [`DataflowMonitor::from_program`](super::monitor::DataflowMonitor::from_program) to create a
/// stateful monitor. Clones share only immutable compiled data; monitors created from them have
/// independent execution state.
#[derive(Clone)]
pub struct DataflowProgram {
    payload: Rc<DataflowProgramPayload>,
}

struct DataflowProgramPayload {
    input_vars: Vec<VarName>,
    output_vars: Vec<VarName>,
    output_slots: Vec<EnvironmentSlot>,
    stream_vars: Vec<VarName>,
    stream_programs: Vec<Rc<StreamProgram>>,
    environment_layout: Rc<EnvironmentLayout>,
    monitor_plan: MonitorPlan,
    history_requirements: HistoryRequirements,
    environment_size: usize,
    definition_key: DefinitionKey,
}

impl DataflowProgram {
    #[cfg(test)]
    pub(in crate::dataflow) fn record_root_compile(checked: bool) {
        ROOT_COMPILE_COUNTS.with(|counts| {
            let (checked_count, untyped_count) = counts.get();
            counts.set(if checked {
                (checked_count + 1, untyped_count)
            } else {
                (checked_count, untyped_count + 1)
            });
        });
    }

    #[cfg(test)]
    pub(in crate::dataflow) fn reset_root_compile_counts() {
        ROOT_COMPILE_COUNTS.with(|counts| counts.set((0, 0)));
    }

    #[cfg(test)]
    pub(in crate::dataflow) fn root_compile_counts() -> (usize, usize) {
        ROOT_COMPILE_COUNTS.with(Cell::get)
    }

    /// Test-only identity for the shared immutable payload.
    #[cfg(test)]
    pub(in crate::dataflow) fn test_payload_identity(&self) -> *const () {
        Rc::as_ptr(&self.payload).cast()
    }

    /// Test-only owner count for the shared immutable payload.
    #[cfg(test)]
    pub(in crate::dataflow) fn test_payload_strong_count(&self) -> usize {
        Rc::strong_count(&self.payload)
    }

    /// Input variables in the order expected by monitor evaluation.
    pub fn input_vars(&self) -> &[VarName] {
        &self.payload.input_vars
    }

    /// Output variables in the order written by monitor evaluation.
    pub fn output_vars(&self) -> &[VarName] {
        &self.payload.output_vars
    }

    /// Computed stream variables in their stable program order.
    pub fn stream_vars(&self) -> &[VarName] {
        &self.payload.stream_vars
    }

    /// Number of values in the monitor's environment row.
    pub fn environment_size(&self) -> usize {
        self.payload.environment_size
    }

    /// Canonical semantic identity of this definition.
    pub fn definition_key(&self) -> &DefinitionKey {
        &self.payload.definition_key
    }

    pub(in crate::dataflow) fn output_slots(&self) -> &[EnvironmentSlot] {
        &self.payload.output_slots
    }

    pub(in crate::dataflow) fn stream_programs(&self) -> &[Rc<StreamProgram>] {
        &self.payload.stream_programs
    }

    pub(in crate::dataflow) fn environment_layout(&self) -> &EnvironmentLayout {
        &self.payload.environment_layout
    }

    pub(in crate::dataflow) fn monitor_plan(&self) -> &MonitorPlan {
        &self.payload.monitor_plan
    }

    pub(in crate::dataflow) fn history_requirements(&self) -> &HistoryRequirements {
        &self.payload.history_requirements
    }

    pub(in crate::dataflow) fn from_parts(
        input_vars: Vec<VarName>,
        output_vars: Vec<VarName>,
        output_slots: Vec<EnvironmentSlot>,
        stream_vars: Vec<VarName>,
        stream_programs: Vec<Rc<StreamProgram>>,
        monitor_plan: MonitorPlan,
        environment_size: usize,
    ) -> Self {
        debug_assert_eq!(output_vars.len(), output_slots.len());
        debug_assert_eq!(stream_vars.len(), stream_programs.len());
        debug_assert_eq!(environment_size, input_vars.len() + stream_programs.len());
        debug_assert!(
            output_slots
                .iter()
                .all(|slot| slot.index() < environment_size)
        );
        let environment_layout = stream_programs
            .first()
            .map(|program| Rc::clone(&program.environment_layout))
            .unwrap_or_else(|| {
                Rc::new(EnvironmentLayout::from_variables(
                    input_vars
                        .iter()
                        .cloned()
                        .chain(stream_vars.iter().cloned()),
                ))
            });
        debug_assert_eq!(environment_layout.len(), environment_size);
        debug_assert!(
            stream_programs
                .iter()
                .all(|program| program.environment_layout == environment_layout)
        );

        let history_requirements = HistoryRequirements::analyze(
            &input_vars,
            &stream_vars,
            &stream_programs,
            &environment_layout,
        );
        let definition_key =
            monitor_definition_key(&input_vars, &output_vars, &stream_vars, &stream_programs);
        Self {
            payload: Rc::new(DataflowProgramPayload {
                input_vars,
                output_vars,
                output_slots,
                stream_vars,
                stream_programs,
                environment_layout,
                monitor_plan,
                history_requirements,
                environment_size,
                definition_key,
            }),
        }
    }
}

fn monitor_definition_key(
    input_vars: &[VarName],
    output_vars: &[VarName],
    stream_vars: &[VarName],
    stream_programs: &[Rc<StreamProgram>],
) -> DefinitionKey {
    let mut fingerprint = FingerprintBuilder::new("dataflow-definition-v1");
    for variable in input_vars {
        fingerprint.write_str(&variable.name());
    }
    for variable in output_vars {
        fingerprint.write_str(&variable.name());
    }
    for (variable, program) in stream_vars.iter().zip(stream_programs) {
        fingerprint.write_str(&variable.name());
        fingerprint.write_u128(program.state_key().value());
    }
    DefinitionKey::from_fingerprint(fingerprint.finish())
}
