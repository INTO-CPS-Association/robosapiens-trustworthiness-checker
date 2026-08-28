use super::environment::EnvironmentSlot;
use super::error::DataflowEvaluationError;
use super::execution::monitor_execution::MonitorExecution;
use super::execution::stream_evaluator::StreamEvaluator;
use super::execution_plan::{MonitorPlan, ReconfigurationState, StreamId};
use super::ir::{DynamicExpressionMode, StreamProgram, canonical_graph_descriptor};
use super::reconfiguration::{
    ActivationFrontier, DefinitionKey, DefinitionSource, InterfaceEpoch, RegionAddress,
    ReplacementTarget, RevisionId, StateKey, TransferDecision, TransferReportEntry,
    validate_replacement,
};
use super::scheduler::Scheduler;
use super::*;

/// Canonical state captured at a logical tick boundary for runtime-level monitor replacement.
///
/// The evaluator values are intentionally kept opaque. A context can only be applied through
/// [`DataflowMonitor::import_context`], which validates the replacement-local program identity
/// before copying state.
#[derive(Clone)]
pub struct DataflowContext {
    stream_evaluators: BTreeMap<VarName, StreamEvaluator>,
    retained_values: BTreeMap<VarName, Value>,
    sealed_regions: Vec<RegionAddress>,
    dynamic_dependencies: BTreeMap<RegionAddress, Vec<VarName>>,
    revision: RevisionId,
    interface_epoch: InterfaceEpoch,
}

impl DataflowContext {
    /// Revision from which this canonical snapshot was exported.
    pub fn revision(&self) -> RevisionId {
        self.revision
    }

    /// Input/output binding epoch from which this snapshot was exported.
    pub fn interface_epoch(&self) -> InterfaceEpoch {
        self.interface_epoch
    }
}

/// A compiled, stateful synchronous dataflow monitor.
///
/// Each tick evaluates expression sources, resolves reconfiguration points, updates the dependency
/// schedule, evaluates every remaining stream once, and commits staged temporal state. Static
/// monitors are the empty-reconfiguration quickening of the same flow.
pub struct DataflowMonitor {
    input_vars: Vec<VarName>,
    output_vars: Vec<VarName>,
    output_slots: Vec<EnvironmentSlot>,
    stream_vars: Vec<VarName>,
    execution: MonitorExecution,
    monitor_plan: MonitorPlan,
    reconfiguration_state: ReconfigurationState,
    scheduler: Scheduler,
    environment_values: Vec<Value>,
    retained_environment_values: Option<Vec<Value>>,
    revision: RevisionId,
    interface_epoch: InterfaceEpoch,
    definition_key: DefinitionKey,
    reconfiguration_transfer_policy: ContextTransferPolicy,
    failed: bool,
}

impl DataflowMonitor {
    pub(in crate::dataflow) fn new(
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

        let reconfiguration_state = ReconfigurationState::new(
            &monitor_plan.reconfiguration,
            monitor_plan.dependencies.stream_count(),
        );
        let scheduler = Scheduler::new(
            monitor_plan.stream_slots,
            &monitor_plan.dependencies,
            reconfiguration_state.source_streams(),
        );
        let definition_key = DefinitionKey::from_canonical(canonical_monitor_key(
            &input_vars,
            &output_vars,
            &stream_vars,
            &stream_programs,
        ));
        let execution = MonitorExecution::new_with_source_prelude(
            stream_programs,
            monitor_plan.stream_slots,
            reconfiguration_state.source_order(),
            scheduler.execution_schedule().evaluation_order(),
            monitor_plan.temporal_streams.as_slice(),
        );

        let retained_environment_values = (!monitor_plan.reconfiguration.is_empty())
            .then(|| vec![Value::NoVal; environment_size]);
        Self {
            input_vars,
            output_vars,
            output_slots,
            stream_vars,
            execution,
            monitor_plan,
            reconfiguration_state,
            scheduler,
            environment_values: vec![Value::NoVal; environment_size],
            retained_environment_values,
            revision: RevisionId::INITIAL,
            interface_epoch: InterfaceEpoch::INITIAL,
            definition_key,
            reconfiguration_transfer_policy: ContextTransferPolicy::Compatible,
            failed: false,
        }
    }

    pub(crate) fn set_quickening(&mut self, enabled: bool) {
        self.execution.set_quickening(enabled);
    }

    #[cfg(test)]
    pub(crate) fn quickening_enabled(&self) -> bool {
        self.execution.quickening_enabled()
    }

    #[cfg(feature = "jit")]
    pub(crate) fn enable_jit(&mut self, config: JitConfig) {
        self.execution.enable_jit(config);
    }

    pub fn input_vars(&self) -> &[VarName] {
        &self.input_vars
    }

    pub fn output_vars(&self) -> &[VarName] {
        &self.output_vars
    }

    pub fn revision(&self) -> RevisionId {
        self.revision
    }

    pub fn interface_epoch(&self) -> InterfaceEpoch {
        self.interface_epoch
    }

    pub fn definition_key(&self) -> &DefinitionKey {
        &self.definition_key
    }

    pub(crate) fn install_revision(
        &mut self,
        revision: RevisionId,
        interface_epoch: InterfaceEpoch,
    ) {
        self.revision = revision;
        self.interface_epoch = interface_epoch;
    }

    pub(crate) fn set_reconfiguration_transfer_policy(&mut self, policy: ContextTransferPolicy) {
        self.reconfiguration_transfer_policy = policy;
    }

    /// Capture canonical evaluator state for a replacement monitor.
    ///
    /// This operation is valid only between logical ticks. The returned context is independent of
    /// the monitor and can be retained while a replacement monitor is compiled.
    pub fn export_context(&self) -> Result<DataflowContext, DataflowStateError> {
        if self.execution.tick_in_progress() {
            return Err(DataflowStateError::TickInProgress);
        }
        // Native/JIT state is materialized only into this snapshot. The active monitor retains its
        // artifact, activation counter, and native state until the serial replacement commits.
        let evaluators = self.execution.snapshot_evaluators();
        let stream_evaluators = self.stream_vars.iter().cloned().zip(evaluators).collect();
        let retained_values = self
            .retained_environment_values
            .as_ref()
            .map(|values| {
                self.input_vars
                    .iter()
                    .cloned()
                    .chain(self.stream_vars.iter().cloned())
                    .zip(values.iter().cloned())
                    .collect()
            })
            .unwrap_or_default();
        let mut dynamic_dependencies = BTreeMap::new();
        for stream_index in 0..self.stream_vars.len() {
            let stream = StreamId::new(stream_index);
            for point in self.monitor_plan.reconfiguration.points_for(stream) {
                let dependencies = self
                    .execution
                    .reconfiguration_point_dependency_slots(stream, point.node)
                    .iter()
                    .filter_map(|slot| self.variable_for_slot(*slot))
                    .collect::<Vec<_>>();
                if !dependencies.is_empty() {
                    dynamic_dependencies.insert(point.address().clone(), dependencies);
                }
            }
        }
        Ok(DataflowContext {
            stream_evaluators,
            retained_values,
            sealed_regions: self
                .reconfiguration_state
                .sealed_addresses(&self.monitor_plan.reconfiguration),
            dynamic_dependencies,
            revision: self.revision,
            interface_epoch: self.interface_epoch,
        })
    }

    /// Import compatible canonical state into a freshly compiled replacement monitor.
    ///
    /// The replacement is a local value owned by the caller until installation succeeds, so this
    /// applies state directly rather than through a rollback clone. A failure poisons the monitor:
    /// the partially transferred value must be discarded, and the caller applies its own terminal
    /// policy.
    pub fn import_context(
        &mut self,
        context: &DataflowContext,
        policy: ContextTransferPolicy,
    ) -> Result<ContextTransferReport, DataflowStateError> {
        let result = self.import_context_inner(context, policy);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn import_context_inner(
        &mut self,
        context: &DataflowContext,
        policy: ContextTransferPolicy,
    ) -> Result<ContextTransferReport, DataflowStateError> {
        if self.execution.tick_in_progress() {
            return Err(DataflowStateError::TickInProgress);
        }
        let mut report = ContextTransferReport::default();
        if policy == ContextTransferPolicy::None {
            for stream in &self.stream_vars {
                report.reset_streams += 1;
                report.entries.push(TransferReportEntry {
                    address: RegionAddress::stream(stream),
                    state: StateKey::new(RegionAddress::stream(stream), "stream"),
                    decision: TransferDecision::Reset("policy=None".into()),
                });
            }
            self.environment_values.fill(Value::NoVal);
            self.execution.reset_after_context_transfer();
            return Ok(report);
        }

        let Self {
            input_vars,
            stream_vars,
            execution,
            monitor_plan,
            reconfiguration_state,
            scheduler,
            environment_values,
            retained_environment_values,
            ..
        } = self;
        let mut exactly_transferred_streams = BTreeSet::new();
        for stream_index in 0..stream_vars.len() {
            let stream = stream_vars[stream_index].clone();
            let address = RegionAddress::stream(&stream);
            let state_key = StateKey::new(address.clone(), "stream");
            let Some(source) = context.stream_evaluators.get(&stream) else {
                report.reset_streams += 1;
                report.entries.push(TransferReportEntry {
                    address,
                    state: state_key,
                    decision: TransferDecision::Reset("new owner".into()),
                });
                continue;
            };
            let stream_id = StreamId::new(stream_index);
            let transferred_exactly = execution.transfer_evaluator(stream_id, source);
            let has_reconfiguration_points = !monitor_plan
                .reconfiguration
                .points_for(stream_id)
                .is_empty();
            let transferred = transferred_exactly
                || (policy == ContextTransferPolicy::Compatible
                    && !has_reconfiguration_points
                    && execution.transfer_compatible_evaluator(stream_id, source));
            if transferred {
                if transferred_exactly {
                    exactly_transferred_streams.insert(stream.clone());
                }
                report.transferred_streams += 1;
                report.entries.push(TransferReportEntry {
                    address,
                    state: state_key,
                    decision: TransferDecision::Transferred,
                });
            } else if policy == ContextTransferPolicy::Strict {
                report.rejected_streams += 1;
                report.entries.push(TransferReportEntry {
                    address,
                    state: state_key,
                    decision: TransferDecision::Rejected("incompatible or ambiguous state".into()),
                });
                return Err(DataflowStateError::IncompatibleStream(stream));
            } else {
                report.reset_streams += 1;
                report.entries.push(TransferReportEntry {
                    address,
                    state: state_key,
                    decision: TransferDecision::Reset("incompatible or ambiguous state".into()),
                });
            }
        }

        if let Some(retained) = retained_environment_values {
            retained.fill(Value::NoVal);
            for (variable, value) in &context.retained_values {
                let input_slot = input_vars
                    .iter()
                    .position(|candidate| candidate == variable);
                let stream_slot = exactly_transferred_streams.contains(variable).then(|| {
                    stream_vars
                        .iter()
                        .position(|candidate| candidate == variable)
                        .map(|index| input_vars.len() + index)
                });
                if let Some(slot) = input_slot.or_else(|| stream_slot.flatten()) {
                    retained[slot] = value.clone();
                }
            }
        }

        let transferred_sealed_regions = context
            .sealed_regions
            .iter()
            .filter(|address| {
                (0..stream_vars.len()).any(|stream_index| {
                    let stream = StreamId::new(stream_index);
                    exactly_transferred_streams.contains(&stream_vars[stream_index])
                        && monitor_plan
                            .reconfiguration
                            .points_for(stream)
                            .iter()
                            .any(|point| point.address() == *address)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        reconfiguration_state
            .restore_sealed_addresses(&monitor_plan.reconfiguration, &transferred_sealed_regions);
        for stream_index in 0..stream_vars.len() {
            if !exactly_transferred_streams.contains(&stream_vars[stream_index]) {
                continue;
            }
            let stream_id = StreamId::new(stream_index);
            for point in monitor_plan.reconfiguration.points_for(stream_id) {
                let Some(dependencies) = context.dynamic_dependencies.get(point.address()) else {
                    continue;
                };
                let mut slots = Vec::with_capacity(dependencies.len());
                for variable in dependencies {
                    let Some(slot) = environment_slot(input_vars, stream_vars, variable) else {
                        return Err(DataflowStateError::IncompatibleDependencies(format!(
                            "active dynamic dependency `{variable}` is unavailable in the replacement"
                        )));
                    };
                    slots.push(slot);
                }
                scheduler.restore_dynamic_dependencies(stream_id, &slots);
            }
        }
        scheduler
            .update_schedule(
                &monitor_plan.dependencies,
                reconfiguration_state.source_streams(),
                stream_vars,
            )
            .map_err(|error| DataflowStateError::IncompatibleDependencies(error.to_string()))?;

        environment_values.fill(Value::NoVal);
        execution.reset_after_context_transfer();
        self.select_execution_schedule();
        Ok(report)
    }

    /// Reports which native plan was selected, including safe fallback and backend failures.
    #[cfg(feature = "jit")]
    pub fn jit_report(&self) -> Option<&JitReport> {
        self.execution.jit_report()
    }

    pub fn evaluate(
        &mut self,
        input: &[Value],
        output: &mut [Value],
    ) -> Result<(), DataflowEvaluationError> {
        if self.failed {
            return Err(DataflowEvaluationError::MonitorFailed);
        }
        if input.len() != self.input_vars.len() {
            return Err(DataflowEvaluationError::InputCountMismatch {
                expected: self.input_vars.len(),
                actual: input.len(),
            });
        }
        if output.len() != self.output_vars.len() {
            return Err(DataflowEvaluationError::OutputCountMismatch {
                expected: self.output_vars.len(),
                actual: output.len(),
            });
        }

        if let Err(error) = self.execute_tick(input) {
            self.failed = true;
            return Err(error);
        }
        self.write_outputs(output);
        Ok(())
    }

    fn execute_tick(&mut self, input: &[Value]) -> Result<(), DataflowEvaluationError> {
        if self.monitor_plan.reconfiguration.is_empty() {
            self.environment_values[..input.len()].clone_from_slice(input);
            self.execution
                .evaluate(&mut self.environment_values, None)?;
            return Ok(());
        }
        self.load_reconfigurable_inputs(input);
        self.evaluate_expression_sources()?;
        self.resolve_reconfiguration_points()?;
        let schedule_changed = self.scheduler.update_schedule(
            &self.monitor_plan.dependencies,
            self.reconfiguration_state.source_streams(),
            &self.stream_vars,
        )?;
        if schedule_changed {
            self.select_execution_schedule();
        }
        self.evaluate_scheduled_streams()?;
        self.apply_defer_sealing();
        Ok(())
    }

    fn load_reconfigurable_inputs(&mut self, input: &[Value]) {
        self.environment_values.fill(Value::NoVal);
        self.environment_values[..input.len()].clone_from_slice(input);
        if let Some(retained) = &mut self.retained_environment_values {
            for (retained, current) in retained.iter_mut().zip(input) {
                if current != &Value::NoVal {
                    retained.clone_from(current);
                }
            }
        }
    }

    fn evaluate_expression_sources(&mut self) -> Result<(), DataflowEvaluationError> {
        self.execution.evaluate_source_prelude(
            &mut self.environment_values,
            self.retained_environment_values.as_deref_mut(),
        )
    }

    /// Install every pending nested `dynamic`/`defer` body directly at the source barrier.
    ///
    /// Replacement bodies are compiled into a local evaluator and installed in place. There is no
    /// rollback arena, so any failure leaves the monitor poisoned by [`Self::evaluate`] rather than
    /// resuming the previous body for the remainder of the tick.
    fn resolve_reconfiguration_points(&mut self) -> Result<(), DataflowEvaluationError> {
        if !self.has_pending_region_replacement() {
            return Ok(());
        }

        let Self {
            stream_vars,
            execution,
            monitor_plan,
            reconfiguration_state,
            scheduler,
            environment_values,
            revision,
            reconfiguration_transfer_policy,
            ..
        } = self;
        let reconfiguration = &monitor_plan.reconfiguration;
        let transfer = *reconfiguration_transfer_policy;

        let mut resolution_index = 0;
        while let Some(stream) = reconfiguration_state.resolution_stream(resolution_index) {
            resolution_index += 1;
            let mut dependency_slots_changed = false;
            for point in reconfiguration.points_for(stream) {
                debug_assert_eq!(point.stream, stream);
                if reconfiguration_state.is_sealed(point.id()) {
                    continue;
                }
                let source_value = point.source.read_value(environment_values);
                let requires_update = execution.reconfiguration_point_requires_update(
                    stream,
                    point.node,
                    &source_value,
                );
                if !requires_update {
                    continue;
                }
                if let Value::Str(source) = &source_value {
                    validate_replacement(
                        &ReplacementTarget::Region(point.address().clone()),
                        &DefinitionSource::text(source.to_string()),
                        &ActivationFrontier::SourceBarrier {
                            revision: *revision,
                            region: point.address().clone(),
                            owner_executed: false,
                        },
                        *revision,
                    )
                    .map_err(|error| {
                        DataflowEvaluationError::InvalidRegionReplacement {
                            region: point.address().clone(),
                            source: error,
                        }
                    })?;
                }

                let replacement = execution.replace_reconfiguration_point(
                    stream,
                    point.node,
                    source_value,
                    transfer,
                )?;
                if !replacement.state_transferred && transfer == ContextTransferPolicy::Strict {
                    return Err(DataflowEvaluationError::IncompatibleRegionTransfer(
                        point.address().clone(),
                    ));
                }
                let activation = replacement.activation;
                let semantic_replacement = !matches!(
                    activation,
                    super::execution::dynamic_expressions::DynamicExpressionActivation::Unchanged
                );
                dependency_slots_changed |= activation.dependency_slots_changed();
                execution.install_reconfiguration_point(stream, point.node, replacement)?;
                if semantic_replacement {
                    *revision = revision
                        .checked_next()
                        .ok_or(DataflowEvaluationError::RevisionOverflow)?;
                }
                if point.mode == DynamicExpressionMode::Defer && activation.activated() {
                    reconfiguration_state.mark_defer_activated(point.id());
                }
            }
            if dependency_slots_changed {
                let dependencies = scheduler.begin_dynamic_dependency_update(stream);
                for point in reconfiguration.points_for(stream) {
                    dependencies.extend(
                        execution.reconfiguration_point_dependency_slots(stream, point.node),
                    );
                }
                dependencies.finish();
            }
        }

        let schedule_changed = scheduler.update_schedule(
            &monitor_plan.dependencies,
            reconfiguration_state.source_streams(),
            stream_vars,
        )?;

        if schedule_changed {
            self.select_execution_schedule();
        }
        Ok(())
    }

    fn has_pending_region_replacement(&self) -> bool {
        let reconfiguration = &self.monitor_plan.reconfiguration;
        let mut resolution_index = 0;
        while let Some(stream) = self
            .reconfiguration_state
            .resolution_stream(resolution_index)
        {
            resolution_index += 1;
            for point in reconfiguration.points_for(stream) {
                if self.reconfiguration_state.is_sealed(point.id()) {
                    continue;
                }
                let source_value = point.source.read_value(&self.environment_values);
                if self.execution.reconfiguration_point_requires_update(
                    stream,
                    point.node,
                    &source_value,
                ) {
                    return true;
                }
            }
        }
        false
    }

    fn evaluate_scheduled_streams(&mut self) -> Result<(), DataflowEvaluationError> {
        self.execution.evaluate_main_and_commit(
            &mut self.environment_values,
            self.retained_environment_values.as_deref_mut(),
        )
    }

    fn apply_defer_sealing(&mut self) {
        if !self.reconfiguration_state.apply_pending_releases() {
            return;
        }
        self.scheduler
            .refresh_main_execution_schedule(self.reconfiguration_state.source_streams());
        self.select_execution_schedule();
    }

    fn select_execution_schedule(&mut self) {
        self.execution.select_schedule_ranges(
            self.reconfiguration_state.source_order(),
            self.scheduler.execution_schedule().evaluation_order(),
            self.monitor_plan.stream_slots,
        );
    }

    fn write_outputs(&self, output: &mut [Value]) {
        for (value, &slot) in output.iter_mut().zip(&self.output_slots) {
            *value = self.environment_values[slot.index()].clone();
        }
    }

    fn variable_for_slot(&self, slot: EnvironmentSlot) -> Option<VarName> {
        self.input_vars.get(slot.index()).cloned().or_else(|| {
            slot.index()
                .checked_sub(self.input_vars.len())
                .and_then(|index| self.stream_vars.get(index).cloned())
        })
    }
}

/// Resolve a portable variable name to this monitor's dense environment slot.
fn environment_slot(
    input_vars: &[VarName],
    stream_vars: &[VarName],
    variable: &VarName,
) -> Option<EnvironmentSlot> {
    input_vars
        .iter()
        .position(|candidate| candidate == variable)
        .map(EnvironmentSlot::new)
        .or_else(|| {
            stream_vars
                .iter()
                .position(|candidate| candidate == variable)
                .map(|index| EnvironmentSlot::new(input_vars.len() + index))
        })
}

fn canonical_monitor_key(
    input_vars: &[VarName],
    output_vars: &[VarName],
    stream_vars: &[VarName],
    stream_programs: &[Rc<StreamProgram>],
) -> String {
    let names = |vars: &[VarName]| vars.iter().map(VarName::name).collect::<Vec<_>>().join(",");
    let programs = stream_programs
        .iter()
        .map(|program| canonical_graph_descriptor(&program.graph, &program.environment_layout))
        .collect::<Vec<_>>()
        .join("|");
    format!(
        "dataflow-semantic-v2|inputs={}|outputs={}|streams={}|programs={}",
        names(input_vars),
        names(output_vars),
        names(stream_vars),
        programs
    )
}

#[cfg(test)]
pub(in crate::dataflow) mod test_support {
    use super::*;

    pub(in crate::dataflow) fn execution(monitor: &DataflowMonitor) -> &MonitorExecution {
        &monitor.execution
    }

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn jit_artifact_count(monitor: &DataflowMonitor) -> usize {
        monitor.execution.jit_artifact_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CheckedDsrvSpecification, DsrvSpecification};

    fn input_row(monitor: &DataflowMonitor, values: &[(&str, Value)]) -> Vec<Value> {
        monitor
            .input_vars()
            .iter()
            .map(|variable| {
                values
                    .iter()
                    .find_map(|(name, value)| {
                        (variable == &VarName::new(name)).then(|| value.clone())
                    })
                    .unwrap()
            })
            .collect()
    }

    #[test]
    fn checked_expression_typed_source_uses_string_runtime_values() {
        let specification = "in x: Int\nin property: Expr<Int>\nout result: Int\n\
                             result = dynamic(property)"
            .parse::<CheckedDsrvSpecification>()
            .expect("Expr<T> source should type-check");
        let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();
        let mut output = [Value::NoVal];

        monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[
                        ("x", Value::Int(41)),
                        ("property", Value::Str("x + 1".into())),
                    ],
                ),
                &mut output,
            )
            .unwrap();

        assert_eq!(output, [Value::Int(42)]);
    }

    #[test]
    fn definition_keys_follow_normalized_semantics_not_source_formatting() {
        let compact =
            DataflowMonitor::compile_untyped("in x: Int\nout z: Int\nz = x + 1".parse().unwrap())
                .unwrap();
        let formatted = DataflowMonitor::compile_untyped(
            "in x: Int\nout z: Int\nz = ( x + 1 )".parse().unwrap(),
        )
        .unwrap();
        let changed =
            DataflowMonitor::compile_untyped("in x: Int\nout z: Int\nz = x + 2".parse().unwrap())
                .unwrap();
        assert_eq!(compact.definition_key(), formatted.definition_key());
        assert_ne!(compact.definition_key(), changed.definition_key());
    }

    #[test]
    fn static_monitor_does_not_allocate_a_retained_environment() {
        let specification = "in x: Int\nout z: Int\nz = x + 1"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let monitor = DataflowMonitor::compile_checked(specification).unwrap();

        assert!(monitor.retained_environment_values.is_none());
    }

    #[test]
    fn compatible_context_transfer_preserves_delay_history() {
        let specification = "in x: Int\nout z: Int\nz = x[2]"
            .parse::<DsrvSpecification>()
            .unwrap();
        let mut old = DataflowMonitor::compile_untyped(specification.clone()).unwrap();
        let mut output = [Value::NoVal];
        for value in [1, 2] {
            old.evaluate(&[Value::Int(value)], &mut output).unwrap();
            assert_eq!(output, [Value::Deferred]);
        }

        let context = old.export_context().unwrap();
        let mut replacement = DataflowMonitor::compile_untyped(specification).unwrap();
        let report = replacement
            .import_context(&context, ContextTransferPolicy::Compatible)
            .unwrap();
        assert_eq!(report.transferred_streams, 1);
        assert_eq!(report.reset_streams, 0);

        replacement.evaluate(&[Value::Int(3)], &mut output).unwrap();
        assert_eq!(output, [Value::Int(1)]);
    }

    #[test]
    fn compatible_root_transfer_preserves_unchanged_delay_across_stateless_edit() {
        let old_spec = "in x: Int\nout z: Int\nz = x[1] + 1";
        let new_spec = "in x: Int\nout z: Int\nz = x[1] + 2";
        let mut old = DataflowMonitor::compile_untyped(old_spec.parse().unwrap()).unwrap();
        let mut output = [Value::NoVal];
        old.evaluate(&[Value::Int(1)], &mut output).unwrap();
        assert_eq!(output, [Value::Deferred]);
        old.evaluate(&[Value::Int(2)], &mut output).unwrap();
        assert_eq!(output, [Value::Int(2)]);

        let context = old.export_context().unwrap();
        let mut replacement = DataflowMonitor::compile_untyped(new_spec.parse().unwrap()).unwrap();
        let report = replacement
            .import_context(&context, ContextTransferPolicy::Compatible)
            .unwrap();
        assert_eq!(report.transferred_streams, 1);
        assert_eq!(report.reset_streams, 0);

        replacement.evaluate(&[Value::Int(3)], &mut output).unwrap();
        assert_eq!(output, [Value::Int(4)]);
    }

    #[test]
    fn context_transfer_survives_an_added_input_slot() {
        let old_spec = "in x: Int\nout z: Int\nz = x[2]";
        let new_spec = "in x: Int\nin y: Int\nout z: Int\nz = x[2]";
        let mut old = DataflowMonitor::compile_untyped(old_spec.parse().unwrap()).unwrap();
        let mut output = [Value::NoVal];
        old.evaluate(&[Value::Int(1)], &mut output).unwrap();
        old.evaluate(&[Value::Int(2)], &mut output).unwrap();
        let context = old.export_context().unwrap();
        let mut replacement = DataflowMonitor::compile_untyped(new_spec.parse().unwrap()).unwrap();
        let report = replacement
            .import_context(&context, ContextTransferPolicy::Compatible)
            .unwrap();
        assert_eq!(report.transferred_streams, 1);
        replacement
            .evaluate(&[Value::Int(3), Value::NoVal], &mut output)
            .unwrap();
        assert_eq!(output, [Value::Int(1)]);
    }

    #[test]
    fn strict_context_transfer_rejects_changed_stream_state() {
        let old_spec = "in x: Int\nout z: Int\nz = x";
        let new_spec = "in x: Int\nout z: Int\nz = x + 1";
        let mut old = DataflowMonitor::compile_untyped(old_spec.parse().unwrap()).unwrap();
        let mut output = [Value::NoVal];
        old.evaluate(&[Value::Int(1)], &mut output).unwrap();
        let context = old.export_context().unwrap();
        let mut replacement = DataflowMonitor::compile_untyped(new_spec.parse().unwrap()).unwrap();

        let error = replacement
            .import_context(&context, ContextTransferPolicy::Strict)
            .unwrap_err();
        assert!(matches!(error, DataflowStateError::IncompatibleStream(_)));
    }

    #[test]
    fn compatible_context_transfer_preserves_active_dynamic_region_across_slot_shift() {
        let old_spec = "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
        let new_spec = "in a: Int\nin x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
        let mut old = DataflowMonitor::compile_untyped(old_spec.parse().unwrap()).unwrap();
        let mut output = [Value::NoVal];
        old.evaluate(
            &input_row(
                &old,
                &[("x", Value::Int(1)), ("source", Value::Str("x[1]".into()))],
            ),
            &mut output,
        )
        .unwrap();
        old.evaluate(
            &input_row(
                &old,
                &[("x", Value::Int(2)), ("source", Value::Str("x[1]".into()))],
            ),
            &mut output,
        )
        .unwrap();
        assert_eq!(output, [Value::Int(1)]);

        let context = old.export_context().unwrap();
        let mut replacement = DataflowMonitor::compile_untyped(new_spec.parse().unwrap()).unwrap();
        let report = replacement
            .import_context(&context, ContextTransferPolicy::Compatible)
            .unwrap();
        assert_eq!(report.transferred_streams, 1);
        replacement
            .evaluate(
                &input_row(
                    &replacement,
                    &[
                        ("a", Value::NoVal),
                        ("x", Value::Int(3)),
                        ("source", Value::Str("x[1]".into())),
                    ],
                ),
                &mut output,
            )
            .unwrap();
        assert_eq!(output, [Value::Int(2)]);
    }

    #[test]
    fn compatible_changed_dynamic_body_preserves_unchanged_delay_cells() {
        let specification = "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
        let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
        let mut output = [Value::NoVal];
        monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[("x", Value::Int(1)), ("source", Value::Str("x[1]".into()))],
                ),
                &mut output,
            )
            .unwrap();
        assert_eq!(output, [Value::Deferred]);
        monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[
                        ("x", Value::Int(2)),
                        ("source", Value::Str("(x[1])".into())),
                    ],
                ),
                &mut output,
            )
            .unwrap();
        assert_eq!(output, [Value::Int(1)]);
    }

    #[test]
    fn incompatible_changed_dynamic_body_resets_under_compatible_transfer() {
        let specification = "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
        let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
        let mut output = [Value::NoVal];
        monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[("x", Value::Int(1)), ("source", Value::Str("x[1]".into()))],
                ),
                &mut output,
            )
            .unwrap();
        monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[("x", Value::Int(2)), ("source", Value::Str("x + 1".into()))],
                ),
                &mut output,
            )
            .unwrap();
        assert_eq!(output, [Value::Int(3)]);
    }

    #[test]
    fn strict_changed_dynamic_transfer_poisons_the_monitor() {
        let specification = "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
        let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
        monitor.set_reconfiguration_transfer_policy(ContextTransferPolicy::Strict);
        let mut output = [Value::NoVal];
        monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[("x", Value::Int(1)), ("source", Value::Str("x[1]".into()))],
                ),
                &mut output,
            )
            .unwrap();
        let previous_output = output.clone();
        let error = monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[("x", Value::Int(2)), ("source", Value::Str("x + 1".into()))],
                ),
                &mut output,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            DataflowEvaluationError::IncompatibleRegionTransfer(_)
        ));
        assert_eq!(
            output, previous_output,
            "failed ticks publish no output row"
        );
        assert!(matches!(
            monitor.evaluate(&[Value::Int(3), Value::Str("x[1]".into())], &mut output),
            Err(DataflowEvaluationError::MonitorFailed)
        ));
    }

    #[test]
    fn strict_dynamic_transfer_requires_every_state_owner_and_poisons_on_mismatch() {
        let specification =
            "in x: Int\nin y: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
        let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
        monitor.set_reconfiguration_transfer_policy(ContextTransferPolicy::Strict);
        let mut output = [Value::NoVal];
        monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[
                        ("x", Value::Int(1)),
                        ("y", Value::Int(10)),
                        ("source", Value::Str("x[1] + y[1]".into())),
                    ],
                ),
                &mut output,
            )
            .unwrap();
        assert_eq!(output, [Value::Deferred]);

        let error = monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[
                        ("x", Value::Int(2)),
                        ("y", Value::Int(20)),
                        ("source", Value::Str("x[1] + y[2]".into())),
                    ],
                ),
                &mut output,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            DataflowEvaluationError::IncompatibleRegionTransfer(_)
        ));
        assert!(matches!(
            monitor.evaluate(&[Value::Int(3), Value::Int(30), Value::NoVal], &mut output,),
            Err(DataflowEvaluationError::MonitorFailed)
        ));
    }

    #[test]
    fn compatible_dynamic_transfer_does_not_reuse_wrong_provenance_history() {
        let specification =
            "in x: Int\nin y: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
        let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
        let mut output = [Value::NoVal];
        monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[
                        ("x", Value::Int(1)),
                        ("y", Value::Int(10)),
                        ("source", Value::Str("(x + 1)[1]".into())),
                    ],
                ),
                &mut output,
            )
            .unwrap();
        assert_eq!(output, [Value::Deferred]);

        monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[
                        ("x", Value::Int(2)),
                        ("y", Value::Int(20)),
                        ("source", Value::Str("(y + 1)[1]".into())),
                    ],
                ),
                &mut output,
            )
            .unwrap();
        assert_eq!(output, [Value::Deferred]);

        monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[
                        ("x", Value::Int(3)),
                        ("y", Value::Int(30)),
                        ("source", Value::NoVal),
                    ],
                ),
                &mut output,
            )
            .unwrap();
        assert_eq!(output, [Value::Int(21)]);
    }

    #[test]
    fn root_transfer_reconstructs_active_dynamic_dependency_edges() {
        let specification = "in x: Int\nin source: Str\nout z: Int\naux computed: Int\n\
            z = dynamic(source: Int)\ncomputed = x + 1";
        let mut old = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
        let mut output = [Value::NoVal];

        old.evaluate(
            &input_row(
                &old,
                &[
                    ("x", Value::Int(1)),
                    ("source", Value::Str("computed".into())),
                ],
            ),
            &mut output,
        )
        .unwrap();
        assert_eq!(output, [Value::Int(2)]);
        old.evaluate(
            &input_row(&old, &[("x", Value::Int(2)), ("source", Value::NoVal)]),
            &mut output,
        )
        .unwrap();
        assert_eq!(output, [Value::Int(3)]);

        let context = old.export_context().unwrap();
        let mut replacement =
            DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
        replacement
            .import_context(&context, ContextTransferPolicy::Compatible)
            .unwrap();
        replacement
            .evaluate(
                &input_row(
                    &replacement,
                    &[("x", Value::Int(3)), ("source", Value::NoVal)],
                ),
                &mut output,
            )
            .unwrap();
        assert_eq!(output, [Value::Int(4)]);
    }

    #[test]
    fn invalid_dynamic_candidate_poisons_the_monitor() {
        let specification = "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
        let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
        let mut output = [Value::NoVal];
        monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[("x", Value::Int(1)), ("source", Value::Str("x".into()))],
                ),
                &mut output,
            )
            .unwrap();
        let previous_output = output.clone();
        let error = monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[("x", Value::Int(2)), ("source", Value::Str("(".into()))],
                ),
                &mut output,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            DataflowEvaluationError::DynamicExpressionParse { .. }
        ));
        assert_eq!(output, previous_output);
        assert!(matches!(
            monitor.evaluate(&[Value::Int(3), Value::Str("x + 1".into())], &mut output,),
            Err(DataflowEvaluationError::MonitorFailed)
        ));
    }

    #[test]
    fn invalid_dynamic_candidate_publishes_no_failed_tick_output() {
        let specification = "in x: Int\nin source_text: Str\nout source: Str\nout z: Int\
            \nsource = source_text\nz = dynamic(source: Int)";
        let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
        let mut output = [Value::NoVal, Value::NoVal];
        monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[
                        ("x", Value::Int(1)),
                        ("source_text", Value::Str("x".into())),
                    ],
                ),
                &mut output,
            )
            .unwrap();
        let previous_output = output.clone();

        let error = monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[
                        ("x", Value::Int(2)),
                        ("source_text", Value::Str("(".into())),
                    ],
                ),
                &mut output,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            DataflowEvaluationError::DynamicExpressionParse { .. }
        ));
        assert_eq!(output, previous_output);
        assert!(matches!(
            monitor.evaluate(&[Value::Int(3), Value::Str("x".into())], &mut output),
            Err(DataflowEvaluationError::MonitorFailed)
        ));
    }

    #[test]
    fn invalid_dynamic_source_poisons_the_monitor() {
        let specification = "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
        let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
        let mut output = [Value::NoVal];
        monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[("x", Value::Int(1)), ("source", Value::Str("x[1]".into()))],
                ),
                &mut output,
            )
            .unwrap();
        let error = monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[("x", Value::Int(2)), ("source", Value::Str("(".into()))],
                ),
                &mut output,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            DataflowEvaluationError::DynamicExpressionParse { .. }
        ));
        assert!(matches!(
            monitor.evaluate(&[Value::Int(3), Value::NoVal], &mut output,),
            Err(DataflowEvaluationError::MonitorFailed)
        ));
    }

    #[test]
    fn invalid_first_defer_source_poisons_the_monitor() {
        let specification = "in x: Int\nin source: Str\nout z: Int\nz = defer(source: Int)";
        let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
        let mut output = [Value::NoVal];
        let error = monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[("x", Value::Int(1)), ("source", Value::Str("(".into()))],
                ),
                &mut output,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            DataflowEvaluationError::DynamicExpressionParse { .. }
        ));
        assert!(matches!(
            monitor.evaluate(&[Value::Int(2), Value::NoVal], &mut output,),
            Err(DataflowEvaluationError::MonitorFailed)
        ));
    }

    #[test]
    fn each_nested_region_replacement_advances_the_revision() {
        let specification = "in x: Int\nin first: Str\nin second: Str\nout a: Int\nout b: Int\
            \na = dynamic(first: Int)\nb = dynamic(second: Int)";
        let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
        let mut output = [Value::NoVal, Value::NoVal];

        monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[
                        ("x", Value::Int(1)),
                        ("first", Value::Str("x".into())),
                        ("second", Value::Str("x + 1".into())),
                    ],
                ),
                &mut output,
            )
            .unwrap();

        assert_eq!(monitor.revision(), RevisionId(2));
    }

    #[test]
    fn unchanged_dynamic_point_keeps_state_when_another_point_changes() {
        let specification = "in x: Int\nin first: Str\nin second: Str\nout a: Int\nout b: Int\
            \na = dynamic(first: Int)\nb = dynamic(second: Int)";
        let mut monitor = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
        monitor.set_reconfiguration_transfer_policy(ContextTransferPolicy::None);
        let mut output = [Value::NoVal, Value::NoVal];

        monitor
            .evaluate(
                &input_row(
                    &monitor,
                    &[
                        ("x", Value::Int(1)),
                        ("first", Value::Str("x[1]".into())),
                        ("second", Value::Str("x[1]".into())),
                    ],
                ),
                &mut output,
            )
            .unwrap();
        assert_eq!(output, [Value::Deferred, Value::Deferred]);
        assert_eq!(monitor.revision(), RevisionId(2));

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            monitor.evaluate(
                &input_row(
                    &monitor,
                    &[
                        ("x", Value::Int(2)),
                        ("first", Value::Str("x + 1".into())),
                        ("second", Value::Str("x[1]".into())),
                    ],
                ),
                &mut output,
            )
        }));
        assert!(
            result.is_ok(),
            "selective dynamic replacement must not panic"
        );
        result.unwrap().unwrap();

        assert_eq!(output, [Value::Int(3), Value::Int(1)]);
        assert_eq!(monitor.revision(), RevisionId(3));
    }

    #[test]
    fn compatible_context_transfer_preserves_sealed_defer_region() {
        let old_spec = "in x: Int\nin source: Str\nout z: Int\nz = defer(source: Int)";
        let new_spec = "in a: Int\nin x: Int\nin source: Str\nout z: Int\nz = defer(source: Int)";
        let mut old = DataflowMonitor::compile_untyped(old_spec.parse().unwrap()).unwrap();
        let mut output = [Value::NoVal];
        for value in [1, 2] {
            old.evaluate(
                &input_row(
                    &old,
                    &[
                        ("x", Value::Int(value)),
                        ("source", Value::Str("x[1]".into())),
                    ],
                ),
                &mut output,
            )
            .unwrap();
        }
        assert_eq!(output, [Value::Int(1)]);
        assert!(old.reconfiguration_state.source_order().is_empty());

        let context = old.export_context().unwrap();
        let mut replacement = DataflowMonitor::compile_untyped(new_spec.parse().unwrap()).unwrap();
        let report = replacement
            .import_context(&context, ContextTransferPolicy::Compatible)
            .unwrap();
        assert_eq!(report.transferred_streams, 1);
        replacement
            .evaluate(
                &input_row(
                    &replacement,
                    &[
                        ("a", Value::NoVal),
                        ("x", Value::Int(3)),
                        ("source", Value::Str("x[1]".into())),
                    ],
                ),
                &mut output,
            )
            .unwrap();
        assert_eq!(output, [Value::Int(2)]);
        assert!(replacement.reconfiguration_state.source_order().is_empty());
    }

    #[test]
    fn unactivated_defer_stays_inactive_across_root_transfer() {
        let specification = "in x: Int\nin source: Str\nout z: Int\nz = defer(source: Int)";
        let mut old = DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
        let mut output = [Value::NoVal];
        old.evaluate(&[Value::Int(1), Value::NoVal], &mut output)
            .unwrap();
        let context = old.export_context().unwrap();
        let mut replacement =
            DataflowMonitor::compile_untyped(specification.parse().unwrap()).unwrap();
        replacement
            .import_context(&context, ContextTransferPolicy::Compatible)
            .unwrap();
        let point = replacement
            .monitor_plan
            .reconfiguration
            .points_for(StreamId::new(0))[0]
            .id();
        assert!(!replacement.reconfiguration_state.is_sealed(point));
        assert!(
            replacement
                .reconfiguration_state
                .resolution_stream(0)
                .is_some()
        );
        replacement
            .evaluate(&[Value::Int(2), Value::Str("x".into())], &mut output)
            .unwrap();
        assert_eq!(output, [Value::Int(2)]);
    }

    #[test]
    fn reconfigurable_monitor_retains_an_outer_environment() {
        let specification = "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let monitor = DataflowMonitor::compile_checked(specification).unwrap();

        assert_eq!(
            monitor.retained_environment_values.as_ref().unwrap().len(),
            monitor.environment_values.len()
        );
    }

    #[test]
    fn activated_defer_releases_its_computed_source_into_the_main_plan() {
        let specification = "in x: Int\nin choose: Bool\nin left: Str\nin right: Str\n\
            aux source: Str\nout result: Int\n\
            source = if choose then left else right\n\
            result = defer(source: Int)"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();
        assert_eq!(monitor.reconfiguration_state.source_order().len(), 1);

        let mut output = [Value::NoVal];
        let activation = input_row(
            &monitor,
            &[
                ("x", Value::Int(10)),
                ("choose", Value::Bool(true)),
                ("left", Value::Str("x + 1".into())),
                ("right", Value::Str("x + 100".into())),
            ],
        );
        monitor.evaluate(&activation, &mut output).unwrap();
        assert_eq!(output, [Value::Int(11)]);
        assert!(monitor.reconfiguration_state.source_order().is_empty());
        assert!(monitor.reconfiguration_state.resolution_stream(0).is_none());

        let after_sealing = input_row(
            &monitor,
            &[
                ("x", Value::Int(20)),
                ("choose", Value::Bool(false)),
                ("left", Value::Str("x + 1".into())),
                ("right", Value::Str("x + 100".into())),
            ],
        );
        monitor.evaluate(&after_sealing, &mut output).unwrap();
        assert_eq!(output, [Value::Int(21)]);
    }

    #[test]
    fn shared_dynamic_source_keeps_a_sealed_defer_source_in_the_prelude() {
        let specification = "in x: Int\nin choose: Bool\nin left: Str\nin right: Str\n\
            aux source: Str\nout deferred: Int\nout dynamic_result: Int\n\
            source = if choose then left else right\n\
            deferred = defer(source: Int)\n\
            dynamic_result = dynamic(source: Int)"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();
        let mut output = [Value::NoVal, Value::NoVal];

        let first = input_row(
            &monitor,
            &[
                ("x", Value::Int(10)),
                ("choose", Value::Bool(true)),
                ("left", Value::Str("x + 1".into())),
                ("right", Value::Str("x + 2".into())),
            ],
        );
        monitor.evaluate(&first, &mut output).unwrap();
        assert_eq!(output, [Value::Int(11), Value::Int(11)]);
        assert_eq!(monitor.reconfiguration_state.source_order().len(), 1);

        let second = input_row(
            &monitor,
            &[
                ("x", Value::Int(20)),
                ("choose", Value::Bool(false)),
                ("left", Value::Str("x + 1".into())),
                ("right", Value::Str("x + 2".into())),
            ],
        );
        monitor.evaluate(&second, &mut output).unwrap();
        assert_eq!(output, [Value::Int(21), Value::Int(22)]);
        assert_eq!(monitor.reconfiguration_state.source_order().len(), 1);
    }

    #[cfg(feature = "jit")]
    #[test]
    fn context_transfer_materializes_native_temporal_state() {
        let specification = "in x: Int\nout z: Int\nz = default(x[1], 0)"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let mut old =
            DataflowMonitor::compile_checked_with_jit(specification.clone(), JitConfig::eager())
                .unwrap();
        let mut canonical = DataflowMonitor::compile_checked(specification.clone()).unwrap();
        let mut output = [Value::NoVal];
        let mut canonical_output = [Value::NoVal];
        for value in [Value::Int(1), Value::Int(2)] {
            old.evaluate(&[value.clone()], &mut output).unwrap();
            canonical
                .evaluate(std::slice::from_ref(&value), &mut canonical_output)
                .unwrap();
            assert_eq!(output, canonical_output);
        }
        let context = old.export_context().unwrap();

        let mut replacement =
            DataflowMonitor::compile_checked_with_jit(specification, JitConfig::eager()).unwrap();
        replacement
            .evaluate(&[Value::Int(99)], &mut output)
            .unwrap();
        let report = replacement
            .import_context(&context, ContextTransferPolicy::Compatible)
            .unwrap();
        assert_eq!(report.transferred_streams, 1);
        old.evaluate(&[Value::Int(3)], &mut output).unwrap();
        canonical
            .evaluate(&[Value::Int(3)], &mut canonical_output)
            .unwrap();
        replacement.evaluate(&[Value::Int(3)], &mut output).unwrap();
        assert_eq!(output, canonical_output);
        assert_eq!(output, [Value::Int(2)]);
    }

    #[cfg(feature = "jit")]
    #[test]
    fn context_transfer_materializes_per_stream_temporal_branch_and_lift_state() {
        let specification = "in x: Int\nin choose: Bool\naux delayed: Int\nout result: Int\n\
            delayed = default(x[1], 0)\n\
            result = if choose then x + delayed else x + delayed"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let mut old =
            DataflowMonitor::compile_checked_with_jit(specification.clone(), JitConfig::eager())
                .unwrap();
        let report = old.jit_report().unwrap();
        assert_eq!(report.plan(), JitPlan::PerStream);
        assert!(report.compiled_artifacts() > 0);
        let mut canonical = DataflowMonitor::compile_checked(specification.clone()).unwrap();
        let mut old_output = [Value::NoVal];
        let mut canonical_output = [Value::NoVal];
        for value in [Value::Int(3), Value::Int(4)] {
            let row = [value, Value::Bool(true)];
            old.evaluate(&row, &mut old_output).unwrap();
            canonical.evaluate(&row, &mut canonical_output).unwrap();
            assert_eq!(old_output, canonical_output);
        }

        let context = old.export_context().unwrap();
        let mut replacement =
            DataflowMonitor::compile_checked_with_jit(specification, JitConfig::eager()).unwrap();
        replacement
            .evaluate(&[Value::Int(99), Value::Bool(false)], &mut old_output)
            .unwrap();
        replacement
            .import_context(&context, ContextTransferPolicy::Compatible)
            .unwrap();

        for value in [Value::NoVal, Value::Deferred] {
            let row = [value, Value::Bool(true)];
            let mut replacement_output = [Value::NoVal];
            old.evaluate(&row, &mut old_output).unwrap();
            canonical.evaluate(&row, &mut canonical_output).unwrap();
            replacement.evaluate(&row, &mut replacement_output).unwrap();
            assert_eq!(old_output, canonical_output);
            assert_eq!(
                replacement_output, canonical_output,
                "replacement diverged after per-stream temporal context transfer for {row:?}"
            );
        }
    }

    #[cfg(feature = "jit")]
    #[test]
    fn context_transfer_materializes_fused_branch_and_lift_state() {
        let specification = "in x: Int\nin choose: Bool\nout result: Int\n\
result = if choose then x + 1 else x + 2"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let mut continued =
            DataflowMonitor::compile_checked_with_jit(specification.clone(), JitConfig::eager())
                .unwrap();
        let mut canonical = DataflowMonitor::compile_checked(specification.clone()).unwrap();
        let mut continued_output = [Value::NoVal];
        let mut canonical_output = [Value::NoVal];
        let initial = [Value::Int(3), Value::Bool(true)];
        continued.evaluate(&initial, &mut continued_output).unwrap();
        canonical.evaluate(&initial, &mut canonical_output).unwrap();
        assert_eq!(continued_output, [Value::Int(4)]);
        assert_eq!(continued_output, canonical_output);
        assert_eq!(continued.jit_report().unwrap().plan(), JitPlan::Fused);

        let context = continued.export_context().unwrap();
        let mut replacement =
            DataflowMonitor::compile_checked_with_jit(specification, JitConfig::eager()).unwrap();
        replacement
            .evaluate(&[Value::Int(99), Value::Bool(false)], &mut continued_output)
            .unwrap();
        replacement
            .import_context(&context, ContextTransferPolicy::Compatible)
            .unwrap();

        for value in [Value::NoVal, Value::Deferred] {
            let row = [value, Value::Bool(true)];
            let mut replacement_output = [Value::NoVal];
            continued.evaluate(&row, &mut continued_output).unwrap();
            canonical.evaluate(&row, &mut canonical_output).unwrap();
            replacement.evaluate(&row, &mut replacement_output).unwrap();
            assert_eq!(continued_output, canonical_output);
            assert_eq!(
                replacement_output, canonical_output,
                "replacement diverged after fused-native context transfer for {row:?}"
            );
        }
    }

    #[cfg(feature = "jit")]
    #[test]
    fn defer_sealing_preserves_per_stream_jit_artifacts() {
        let specification = "in x: Int\nin left: Str\nin right: Str\n\
            aux selector: Int\naux source: Str\nout fixed: Int\nout result: Int\n\
            selector = x + 1\n\
            source = if selector > 0 then left else right\n\
            fixed = x * 2\n\
            result = defer(source: Int)"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let mut monitor =
            DataflowMonitor::compile_checked_with_jit(specification, JitConfig::eager()).unwrap();
        assert_eq!(monitor.jit_report().unwrap().plan(), JitPlan::PerStream);
        assert_eq!(monitor.jit_report().unwrap().compiled_artifacts(), 2);
        let artifacts = monitor.execution.jit_artifact_count();

        let mut output = [Value::NoVal, Value::NoVal];
        let activation = input_row(
            &monitor,
            &[
                ("x", Value::Int(10)),
                ("left", Value::Str("x + 1".into())),
                ("right", Value::Str("x + 2".into())),
            ],
        );
        monitor.evaluate(&activation, &mut output).unwrap();
        assert_eq!(output, [Value::Int(20), Value::Int(11)]);
        assert!(monitor.reconfiguration_state.source_order().is_empty());
        assert_eq!(monitor.execution.jit_artifact_count(), artifacts);

        let next = input_row(
            &monitor,
            &[
                ("x", Value::Int(20)),
                ("left", Value::Str("x + 100".into())),
                ("right", Value::Str("x + 200".into())),
            ],
        );
        monitor.evaluate(&next, &mut output).unwrap();
        assert_eq!(output, [Value::Int(40), Value::Int(21)]);
        assert_eq!(monitor.execution.jit_artifact_count(), artifacts);
    }

    #[test]
    fn static_scalar_chain_preserves_values_across_sparse_inputs() {
        let specification = "in x: Int\n\
            aux a: Int\n\
            aux b: Int\n\
            out c: Int\n\
            a = x + 1\n\
            b = a * 2\n\
            c = b - 3"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();

        let mut output = [Value::NoVal];
        for (input, expected) in [
            (Value::Int(1), Value::Int(1)),
            (Value::NoVal, Value::Int(1)),
            (Value::Int(3), Value::Int(5)),
        ] {
            monitor.evaluate(&[input], &mut output).unwrap();
            assert_eq!(output[0], expected);
        }
    }

    #[test]
    fn scalar_run_deoptimizes_only_the_mismatched_stream() {
        let specification = "in x: Int\n\
            aux equal: Bool\n\
            out negated: Bool\n\
            equal = x == 1\n\
            negated = !equal"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();

        let mut output = [Value::NoVal];
        for (input, expected) in [
            (Value::Int(1), Value::Bool(false)),
            (Value::Bool(true), Value::Bool(true)),
            (Value::Int(1), Value::Bool(false)),
        ] {
            monitor.evaluate(&[input], &mut output).unwrap();
            assert_eq!(output, [expected]);
        }
    }

    #[test]
    fn fusion_preserves_fanout_and_intermediate_outputs() {
        let specification = "in x: Int\n\
            out a: Int\n\
            aux b: Int\n\
            out c: Int\n\
            a = x + 1\n\
            b = a * 2\n\
            c = a + b"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();

        let mut output = [Value::NoVal, Value::NoVal];
        monitor.evaluate(&[Value::Int(4)], &mut output).unwrap();
        assert_eq!(output, [Value::Int(5), Value::Int(15)]);
    }

    #[test]
    fn nested_graph_scope_preserves_values() {
        let specification = "in x: Int\n\
            in choose: Bool\n\
            aux a: Int\n\
            aux b: Int\n\
            aux c: Int\n\
            aux d: Int\n\
            out e: Int\n\
            a = x + 1\n\
            b = a + 1\n\
            c = if choose then b else x\n\
            d = c + 1\n\
            e = d + 1"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();

        let mut output = [Value::NoVal];
        monitor
            .evaluate(&[Value::Int(4), Value::Bool(true)], &mut output)
            .unwrap();
        assert_eq!(output, [Value::Int(8)]);
    }

    #[test]
    fn delay_captures_internal_stream_after_the_completed_tick() {
        let specification = "in x: Int\n\
            aux current: Int\n\
            out delayed: Int\n\
            current = x + 1\n\
            delayed = default(current[1], 0) + 1"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();

        let mut output = [Value::NoVal];
        for (input, expected) in [
            (Value::Int(10), Value::Int(1)),
            (Value::Int(20), Value::Int(12)),
            (Value::Int(30), Value::Int(22)),
        ] {
            monitor.evaluate(&[input], &mut output).unwrap();
            assert_eq!(output, [expected]);
        }
    }

    #[test]
    fn temporal_stream_preserves_values_between_scalar_streams() {
        let specification = "in x: Int\n\
            aux current: Int\n\
            aux delayed: Int\n\
            out result: Int\n\
            current = x + 1\n\
            delayed = default(current[1], 0) + 1\n\
            result = delayed * 2"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();

        let mut output = [Value::NoVal];
        for (input, expected) in [(10, 2), (20, 24), (30, 44)] {
            monitor.evaluate(&[Value::Int(input)], &mut output).unwrap();
            assert_eq!(output, [Value::Int(expected)]);
        }
    }

    #[test]
    fn temporal_maple_cycle_preserves_outputs() {
        let specification = crate::dsrv_fixtures::spec_maple_sequence()
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();

        let mut output = vec![Value::NoVal; 6];
        for (stage, active) in ["m", "a", "p", "l", "e"].into_iter().zip(0..) {
            monitor
                .evaluate(&[Value::Str(stage.into())], &mut output)
                .unwrap();
            let mut expected = vec![Value::Bool(false); 6];
            expected[active] = Value::Bool(true);
            expected[5] = Value::Bool(true);
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn recursive_delay_state_survives_scheduled_plan() {
        let specification = "out counter: Int\n\
            aux incremented: Int\n\
            out result: Int\n\
            counter = default(counter[1], 0) + 1\n\
            incremented = counter + 1\n\
            result = incremented + 1"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();

        let mut output = [Value::NoVal, Value::NoVal];
        for expected in [
            [Value::Int(1), Value::Int(3)],
            [Value::Int(2), Value::Int(4)],
            [Value::Int(3), Value::Int(5)],
        ] {
            monitor.evaluate(&[], &mut output).unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn dynamic_schedule_changes_preserve_outputs() {
        let specification = "in x: Int\n\
            in a_source: Str\n\
            in b_source: Str\n\
            out a: Int\n\
            out b: Int\n\
            a = dynamic(a_source: Int)\n\
            b = dynamic(b_source: Int)"
            .parse::<DsrvSpecification>()
            .unwrap();
        let mut monitor = DataflowMonitor::compile_untyped(specification).unwrap();

        let mut output = [Value::NoVal, Value::NoVal];
        for (values, expected) in [
            (
                [
                    ("x", Value::Int(10)),
                    ("a_source", Value::Str("b + 1".into())),
                    ("b_source", Value::Str("x".into())),
                ],
                [Value::Int(11), Value::Int(10)],
            ),
            (
                [
                    ("x", Value::Int(20)),
                    ("a_source", Value::Str("x".into())),
                    ("b_source", Value::Str("a + 1".into())),
                ],
                [Value::Int(20), Value::Int(21)],
            ),
            (
                [
                    ("x", Value::Int(30)),
                    ("a_source", Value::Str("x".into())),
                    ("b_source", Value::Str("a + 1".into())),
                ],
                [Value::Int(30), Value::Int(31)],
            ),
        ] {
            let input = input_row(&monitor, &values);
            monitor.evaluate(&input, &mut output).unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn deoptimization_state_survives_cached_plan_swaps() {
        let specification = "in x: Int\n\
            in a_source: Str\n\
            in b_source: Str\n\
            out a: Int\n\
            out b: Int\n\
            out equal: Bool\n\
            a = dynamic(a_source: Int)\n\
            b = dynamic(b_source: Int)\n\
            equal = x == 1"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();
        let mut output = [Value::NoVal, Value::NoVal, Value::NoVal];

        let input = input_row(
            &monitor,
            &[
                ("x", Value::Bool(true)),
                ("a_source", Value::Str("b + 1".into())),
                ("b_source", Value::Str("2".into())),
            ],
        );
        monitor.evaluate(&input, &mut output).unwrap();
        assert_eq!(output, [Value::Int(3), Value::Int(2), Value::Bool(false)]);

        let input = input_row(
            &monitor,
            &[
                ("x", Value::Int(1)),
                ("a_source", Value::Str("1".into())),
                ("b_source", Value::Str("a + 1".into())),
            ],
        );
        monitor.evaluate(&input, &mut output).unwrap();
        assert_eq!(output, [Value::Int(1), Value::Int(2), Value::Bool(true)]);
    }
}
