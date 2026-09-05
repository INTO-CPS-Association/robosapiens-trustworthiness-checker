use std::rc::Rc;

use super::VarName;
use super::environment::{EnvironmentLayout, EnvironmentSlot};
use super::history_requirements::HistoryRequirements;
use super::ir::StreamProgram;
use super::monitor_plan::MonitorPlan;
use super::reconfiguration::DefinitionKey;
use crate::fingerprint::FingerprintBuilder;

/// An immutable compiled definition for a [`DataflowMonitor`](super::monitor::DataflowMonitor).
///
/// A program contains the bound stream semantics and fixed monitor layout. It does not contain
/// evaluator, scheduler, or other per-monitor execution state; call
/// [`DataflowMonitor::from_program`](super::monitor::DataflowMonitor::from_program) to create a
/// stateful monitor.
pub struct DataflowProgram {
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
    /// Input variables in the order expected by monitor evaluation.
    pub fn input_vars(&self) -> &[VarName] {
        &self.input_vars
    }

    /// Output variables in the order written by monitor evaluation.
    pub fn output_vars(&self) -> &[VarName] {
        &self.output_vars
    }

    /// Computed stream variables in their stable program order.
    pub fn stream_vars(&self) -> &[VarName] {
        &self.stream_vars
    }

    /// Number of values in the monitor's environment row.
    pub fn environment_size(&self) -> usize {
        self.environment_size
    }

    /// Canonical semantic identity of this definition.
    pub fn definition_key(&self) -> &DefinitionKey {
        &self.definition_key
    }

    pub(in crate::dataflow) fn output_slots(&self) -> &[EnvironmentSlot] {
        &self.output_slots
    }

    pub(in crate::dataflow) fn stream_programs(&self) -> &[Rc<StreamProgram>] {
        &self.stream_programs
    }

    pub(in crate::dataflow) fn environment_layout(&self) -> &EnvironmentLayout {
        &self.environment_layout
    }

    pub(in crate::dataflow) fn monitor_plan(&self) -> &MonitorPlan {
        &self.monitor_plan
    }

    pub(in crate::dataflow) fn history_requirements(&self) -> &HistoryRequirements {
        &self.history_requirements
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
