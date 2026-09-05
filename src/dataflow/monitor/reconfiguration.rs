use super::super::environment::EnvironmentSlot;
use super::super::error::{DataflowEvaluationError, DataflowStateError};
use super::super::execution::environment_projection::EnvironmentProjection;
use super::super::execution::reconfigurable_expressions::ReconfigurableExpressionActivation;

use super::super::expression_activation::ExpressionActivationState;
use super::super::ir::{NodeId, ReconfigurableExpressionKind, StreamOp};
use super::super::program::DataflowProgram;
use super::super::reconfiguration::{
    InterfaceRevision, MonitorRevision, StreamStateTransfer, StreamStateTransferOutcome,
};
use super::super::scheduler::{DynamicDependencyCollector, Scheduler};
use super::super::stream_id::StreamId;
use super::super::{
    ContextTransferPolicy, ContextTransferReport, ReconfigurationMapping, ReconfigurationReport,
    StreamMapping,
};
use super::DataflowMonitor;
use crate::core::Value;
use std::collections::BTreeSet;

/// A pure root decision made from the active monitor and an immutable target program.
pub(crate) enum MonitorReconfigurationPlan {
    RetainExact,
    InstallCold {
        target: DataflowProgram,
    },
    Transfer {
        target: DataflowProgram,
        mapping: ReconfigurationMapping,
        policy: ContextTransferPolicy,
    },
}

/// Transfer state assembled before physical context materialization or movement.
struct PreparedContextTransfer {
    mapping: ReconfigurationMapping,
    policy: ContextTransferPolicy,
    report: ContextTransferReport,
    prepared_reconfiguration_state: ExpressionActivationState,
    prepared_scheduler: Scheduler,
    prepared_environment_projections: Option<Box<[PreparedEnvironmentProjection]>>,
    candidate_retained_environment_values: Option<Vec<Value>>,
}

struct PreparedEnvironmentProjection {
    stream: StreamId,
    node: NodeId,
    projection: EnvironmentProjection,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct ReconfigurationResolution {
    pub(super) semantic_reconfiguration: bool,
    pub(super) dependencies_changed: bool,
    #[cfg(test)]
    pub(super) expressions_scanned: usize,
}

impl DataflowMonitor {
    pub(crate) fn install_revision(
        &mut self,
        revision: MonitorRevision,
        interface_revision: InterfaceRevision,
    ) {
        self.revision = revision;
        self.interface_revision = interface_revision;
    }

    pub(crate) fn set_reconfiguration_transfer_policy(&mut self, policy: ContextTransferPolicy) {
        self.reconfiguration_transfer_policy = policy;
    }

    pub(crate) fn plan_reconfiguration(
        &self,
        target: DataflowProgram,
        policy: ContextTransferPolicy,
    ) -> MonitorReconfigurationPlan {
        if policy == ContextTransferPolicy::None || self.failed {
            return MonitorReconfigurationPlan::InstallCold { target };
        }
        if target.definition_key() == self.definition_key() {
            return MonitorReconfigurationPlan::RetainExact;
        }
        let mapping = ReconfigurationMapping::between(self.program(), &target);
        MonitorReconfigurationPlan::Transfer {
            target,
            mapping,
            policy,
        }
    }

    /// Prepare transfer state without materializing or moving monitor-owned data.
    fn prepare_context_transfer(
        &self,
        source: &DataflowMonitor,
        mapping: ReconfigurationMapping,
        policy: ContextTransferPolicy,
    ) -> Result<PreparedContextTransfer, DataflowStateError> {
        if self.execution.tick_in_progress() || source.execution.tick_in_progress() {
            return Err(DataflowStateError::TickInProgress);
        }

        if !self
            .execution
            .validate_context_transfer(&source.execution, &mapping)
        {
            return Err(DataflowStateError::InvalidReconfiguration(
                "context transfer execution validation failed".to_owned(),
            ));
        }

        let report = ContextTransferReport::new(
            self.program
                .stream_vars()
                .iter()
                .enumerate()
                .map(|(target_index, stream)| {
                    let target_stream = StreamId::new(target_index);
                    let outcome = match (policy, mapping.stream(target_stream)) {
                        (
                            ContextTransferPolicy::MatchingStreamState,
                            Some(StreamMapping::Exact(_)),
                        ) => StreamStateTransferOutcome::Transferred,
                        _ => StreamStateTransferOutcome::Initialized,
                    };
                    StreamStateTransfer::new(stream.clone(), outcome)
                }),
            [],
        );

        let target_plan = self.program.monitor_plan();
        let source_plan = source.program.monitor_plan();
        let exact_target_streams = mapping
            .streams()
            .iter()
            .enumerate()
            .filter_map(|(index, stream_mapping)| {
                matches!(stream_mapping, StreamMapping::Exact(_)).then_some(StreamId::new(index))
            })
            .collect::<Vec<_>>();

        let mut candidate_reconfiguration_state = ExpressionActivationState::new(
            &target_plan.reconfigurable_expressions,
            target_plan.dependencies.stream_count(),
        );
        if policy != ContextTransferPolicy::None {
            let source_sealed_expressions = source
                .reconfiguration_state
                .sealed_addresses(&source_plan.reconfigurable_expressions);
            let transferred_sealed_expressions = exact_target_streams
                .iter()
                .flat_map(|&target_stream| {
                    target_plan
                        .reconfigurable_expressions
                        .expressions_for(target_stream)
                        .iter()
                })
                .filter(|expression| {
                    source_sealed_expressions
                        .iter()
                        .any(|address| address == expression.address())
                })
                .map(|expression| expression.address().clone())
                .collect::<Vec<_>>();
            candidate_reconfiguration_state.restore_sealed_addresses(
                &target_plan.reconfigurable_expressions,
                &transferred_sealed_expressions,
            );
        }

        let mut candidate_scheduler = Scheduler::new(
            target_plan.stream_slots,
            &target_plan.dependencies,
            candidate_reconfiguration_state.source_streams(),
        );
        let prepared_environment_projections = if policy != ContextTransferPolicy::None
            && !target_plan.reconfigurable_expressions.is_empty()
        {
            Some(
                self.prepare_environment_projections(
                    source,
                    &mapping,
                    &exact_target_streams,
                    &mut candidate_scheduler,
                )?
                .into_boxed_slice(),
            )
        } else {
            None
        };
        candidate_scheduler
            .update_schedule(
                &target_plan.dependencies,
                candidate_reconfiguration_state.source_streams(),
                self.program.stream_vars(),
            )
            .map_err(|error| DataflowStateError::InvalidReconfiguration(error.to_string()))?;

        let candidate_retained_environment_values = self
            .retained_environment_values
            .as_ref()
            .map(|values| vec![Value::NoVal; values.len()]);
        if candidate_retained_environment_values
            .as_ref()
            .is_some_and(|values| values.len() != self.program.environment_size())
            || source
                .retained_environment_values
                .as_ref()
                .is_some_and(|values| values.len() != source.program.environment_size())
        {
            return Err(DataflowStateError::InvalidReconfiguration(
                "retained environment layout is invalid".to_owned(),
            ));
        }

        Ok(PreparedContextTransfer {
            mapping,
            policy,
            report,
            prepared_reconfiguration_state: candidate_reconfiguration_state,
            prepared_scheduler: candidate_scheduler,
            prepared_environment_projections,
            candidate_retained_environment_values,
        })
    }

    #[cold]
    #[inline(never)]
    fn prepare_environment_projections(
        &self,
        source: &DataflowMonitor,
        mapping: &ReconfigurationMapping,
        exact_target_streams: &[StreamId],
        candidate_scheduler: &mut Scheduler,
    ) -> Result<Vec<PreparedEnvironmentProjection>, DataflowStateError> {
        let target_expressions = &self.program.monitor_plan().reconfigurable_expressions;
        let source_expressions = &source.program.monitor_plan().reconfigurable_expressions;
        let mut prepared = Vec::new();

        for &target_stream in exact_target_streams {
            let Some(StreamMapping::Exact(source_stream)) = mapping.stream(target_stream) else {
                unreachable!("exact target stream set contains a non-exact mapping")
            };
            let mut dependency_slots = BTreeSet::new();
            for target_expression in target_expressions.expressions_for(target_stream) {
                let Some(source_expression) = source_expressions
                    .expressions_for(*source_stream)
                    .iter()
                    .find(|expression| expression.address() == target_expression.address())
                else {
                    return Err(DataflowStateError::InvalidReconfiguration(format!(
                        "exact stream `{}` has no matching reconfigurable expression `{}`",
                        self.program.stream_vars()[target_stream.index()],
                        target_expression.address(),
                    )));
                };
                let StreamOp::Reconfigurable(target_spec) = &self.program.stream_programs()
                    [target_stream.index()]
                .graph
                .nodes[target_expression.node.index()] else {
                    unreachable!(
                        "reconfigurable expression plan referenced a non-reconfigurable node"
                    )
                };
                let allowed_variables = target_spec.scope.allowed_variables();
                let projection = source
                    .execution
                    .prepare_expression_environment_projection(
                        *source_stream,
                        source_expression.node,
                        self.program.environment_layout(),
                        allowed_variables,
                    )
                    .map_err(|variable| {
                        DataflowStateError::InvalidReconfiguration(format!(
                            "active expression variable `{variable}` is unavailable in the candidate monitor"
                        ))
                    })?;
                if let Some(projection) = projection {
                    dependency_slots.extend(projection.outer_dependency_slots());
                    prepared.push(PreparedEnvironmentProjection {
                        stream: target_stream,
                        node: target_expression.node,
                        projection,
                    });
                }
            }
            if !dependency_slots.is_empty() {
                let dependency_slots = dependency_slots.into_iter().collect::<Vec<_>>();
                candidate_scheduler.restore_dynamic_dependencies(target_stream, &dependency_slots);
            }
        }
        Ok(prepared)
    }

    /// Materialize and apply context from a source monitor using a prepared exact mapping.
    ///
    /// Preparation owns the release-mode checks; this destructive application has no recovery path.
    fn context_transfer_from(
        &mut self,
        source: &mut DataflowMonitor,
        prepared: PreparedContextTransfer,
    ) -> ContextTransferReport {
        let PreparedContextTransfer {
            mapping,
            policy,
            mut report,
            prepared_reconfiguration_state,
            prepared_scheduler,
            prepared_environment_projections,
            mut candidate_retained_environment_values,
        } = prepared;

        // The execution handoff is the first physical operation and may materialize native state.
        self.execution
            .context_transfer_from(&mut source.execution, &mapping, policy);
        if let Some(prepared_environment_projections) = prepared_environment_projections {
            for prepared_projection in prepared_environment_projections {
                self.execution.install_expression_environment_projection(
                    prepared_projection.stream,
                    prepared_projection.node,
                    prepared_projection.projection,
                );
            }
        }

        if policy != ContextTransferPolicy::None
            && let (Some(target_values), Some(source_values)) = (
                candidate_retained_environment_values.as_mut(),
                source.retained_environment_values.as_mut(),
            )
        {
            for (target_index, environment_mapping) in
                mapping.environments().iter().copied().enumerate()
            {
                let Some(source_slot) = environment_mapping.source() else {
                    continue;
                };
                let target_slot = EnvironmentSlot::new(target_index);
                let target_is_input = target_index < self.program.input_vars().len();
                let target_is_mapped_stream = !target_is_input
                    && mapping
                        .stream(StreamId::new(
                            target_index - self.program.input_vars().len(),
                        ))
                        .is_some_and(|stream_mapping| stream_mapping.source().is_some());
                if target_is_input || target_is_mapped_stream {
                    std::mem::swap(
                        &mut target_values[target_slot.index()],
                        &mut source_values[source_slot.index()],
                    );
                }
            }
        }

        self.history_store.reset();
        self.recompute_history_requirements();
        if policy != ContextTransferPolicy::None {
            report.retained_history = self.transfer_histories_from(source, &mapping);
        }

        self.retained_environment_values = candidate_retained_environment_values;
        self.reconfiguration_state = prepared_reconfiguration_state;
        self.scheduler = prepared_scheduler;
        self.environment_values.fill(Value::NoVal);
        self.select_execution_schedule();
        report
    }
    /// Install every pending nested `dynamic`/`defer` expression directly at the source barrier.
    ///
    /// Reconfigured bodies are compiled into a local evaluator and installed in place. There is no
    /// rollback arena, so any failure leaves the monitor poisoned by [`Self::evaluate`] rather than
    /// resuming the previous body for the remainder of the tick. The returned outcome lets
    /// [`Self::execute_tick`] perform the single schedule update for this tick.
    pub(super) fn resolve_reconfigurable_expressions(
        &mut self,
    ) -> Result<ReconfigurationResolution, DataflowEvaluationError> {
        let Self {
            execution,
            program,
            reconfiguration_state,
            scheduler,
            environment_values,
            reconfiguration_transfer_policy,
            ..
        } = self;
        let reconfigurable_expressions = &program.monitor_plan().reconfigurable_expressions;
        let transfer = *reconfiguration_transfer_policy;
        let mut resolution = ReconfigurationResolution::default();

        let mut resolution_index = 0;
        while let Some(stream) = reconfiguration_state.resolution_stream(resolution_index) {
            resolution_index += 1;
            let expressions = reconfigurable_expressions.expressions_for(stream);
            let mut dependencies: Option<&mut DynamicDependencyCollector> = None;
            for (expression_index, expression) in expressions.iter().enumerate() {
                debug_assert_eq!(expression.stream, stream);
                if reconfiguration_state.is_sealed(expression.id()) {
                    if let Some(dependencies) = dependencies.as_deref_mut() {
                        dependencies.extend(
                            execution
                                .expression_dependency_slots(expression.stream, expression.node),
                        );
                    }
                    continue;
                }
                #[cfg(test)]
                {
                    resolution.expressions_scanned += 1;
                }
                let source_value = expression.source.read_value(environment_values);
                if !execution.expression_requires_reconfiguration(
                    expression.stream,
                    expression.node,
                    &source_value,
                ) {
                    if let Some(dependencies) = dependencies.as_deref_mut() {
                        dependencies.extend(
                            execution
                                .expression_dependency_slots(expression.stream, expression.node),
                        );
                    }
                    continue;
                }

                let expression_id = expression.id();
                let (activation, _) = execution.reconfigure_expression(
                    expression_id,
                    expression.stream,
                    expression.node,
                    source_value,
                    transfer,
                )?;

                resolution.semantic_reconfiguration |=
                    !matches!(activation, ReconfigurableExpressionActivation::Unchanged);
                if expression.kind == ReconfigurableExpressionKind::Deferred
                    && activation.activated()
                {
                    reconfiguration_state.mark_deferred_activated(expression_id);
                }
                if activation.dependency_slots_changed() && dependencies.is_none() {
                    let collector = scheduler.begin_dynamic_dependency_update(stream);
                    for previous_expression in &expressions[..expression_index] {
                        collector.extend(execution.expression_dependency_slots(
                            previous_expression.stream,
                            previous_expression.node,
                        ));
                    }
                    dependencies = Some(collector);
                }
                if let Some(dependencies) = dependencies.as_deref_mut() {
                    dependencies.extend(
                        execution.expression_dependency_slots(expression.stream, expression.node),
                    );
                }
            }
            if let Some(dependencies) = dependencies {
                resolution.dependencies_changed |= dependencies.finish();
            }
        }

        Ok(resolution)
    }
}

impl DataflowMonitor {
    /// Replace the compiled monitor definition between ticks.
    pub fn reconfigure(
        &mut self,
        target: DataflowProgram,
        policy: ContextTransferPolicy,
    ) -> Result<ReconfigurationReport, DataflowStateError> {
        let plan = self.plan_reconfiguration(target, policy);
        self.apply_reconfiguration_plan(plan, false, |_| {})
    }

    pub(crate) fn apply_reconfiguration_plan<F>(
        &mut self,
        plan: MonitorReconfigurationPlan,
        io_interface_changed: bool,
        configure: F,
    ) -> Result<ReconfigurationReport, DataflowStateError>
    where
        F: FnOnce(&mut DataflowMonitor),
    {
        match plan {
            MonitorReconfigurationPlan::RetainExact => self.retain_exact(io_interface_changed),
            MonitorReconfigurationPlan::InstallCold { target } => {
                let mut candidate = DataflowMonitor::from_program(target);
                configure(&mut candidate);
                self.reconfigure_candidate(
                    candidate,
                    ContextTransferPolicy::None,
                    None,
                    io_interface_changed,
                )
            }
            MonitorReconfigurationPlan::Transfer {
                target,
                mapping,
                policy,
            } => {
                let mut candidate = DataflowMonitor::from_program(target);
                configure(&mut candidate);
                self.reconfigure_candidate(candidate, policy, Some(mapping), io_interface_changed)
            }
        }
    }

    fn retain_exact(
        &mut self,
        interface_changed: bool,
    ) -> Result<ReconfigurationReport, DataflowStateError> {
        let active = self;
        if active.execution.tick_in_progress() {
            return Err(DataflowStateError::TickInProgress);
        }
        debug_assert!(!active.failed);
        let monitor_revision = active
            .revision
            .checked_next()
            .ok_or(DataflowStateError::RevisionOverflow)?;
        let transfer = ContextTransferReport::new(
            active.program.stream_vars().iter().map(|stream| {
                StreamStateTransfer::new(stream.clone(), StreamStateTransferOutcome::Transferred)
            }),
            active
                .history_bindings
                .iter()
                .enumerate()
                .filter_map(|(index, binding)| {
                    binding.and_then(|_| {
                        active
                            .program
                            .environment_layout()
                            .variable(EnvironmentSlot::new(index))
                            .cloned()
                    })
                }),
        );
        let interface_revision = if interface_changed {
            active
                .interface_revision
                .checked_next()
                .ok_or(DataflowStateError::RevisionOverflow)?
        } else {
            active.interface_revision
        };
        active.install_revision(monitor_revision, interface_revision);
        Ok(ReconfigurationReport::new(
            false,
            interface_changed,
            monitor_revision,
            interface_revision,
            transfer,
        ))
    }

    fn reconfigure_candidate(
        &mut self,
        mut candidate: DataflowMonitor,
        policy: ContextTransferPolicy,
        mapping: Option<ReconfigurationMapping>,
        io_interface_changed: bool,
    ) -> Result<ReconfigurationReport, DataflowStateError> {
        let active = self;
        if active.execution.tick_in_progress() {
            return Err(DataflowStateError::TickInProgress);
        }

        let monitor_changed =
            candidate.program().definition_key() != active.program().definition_key();
        let interface_changed = io_interface_changed
            || candidate.input_vars() != active.input_vars()
            || candidate.output_vars() != active.output_vars();
        let monitor_revision = active
            .revision
            .checked_next()
            .ok_or(DataflowStateError::RevisionOverflow)?;
        let interface_revision = if interface_changed {
            active
                .interface_revision
                .checked_next()
                .ok_or(DataflowStateError::RevisionOverflow)?
        } else {
            active.interface_revision
        };
        if policy == ContextTransferPolicy::None {
            let transfer = ContextTransferReport::new(
                candidate.program.stream_vars().iter().map(|stream| {
                    StreamStateTransfer::new(
                        stream.clone(),
                        StreamStateTransferOutcome::Initialized,
                    )
                }),
                [],
            );
            candidate.reconfiguration_transfer_policy = active.reconfiguration_transfer_policy;
            candidate.install_revision(monitor_revision, interface_revision);
            let report = ReconfigurationReport::new(
                monitor_changed,
                interface_changed,
                monitor_revision,
                interface_revision,
                transfer,
            );
            *active = candidate;
            return Ok(report);
        }

        if !monitor_changed && !active.failed {
            let transfer = ContextTransferReport::new(
                active.program.stream_vars().iter().map(|stream| {
                    StreamStateTransfer::new(
                        stream.clone(),
                        StreamStateTransferOutcome::Transferred,
                    )
                }),
                active
                    .history_bindings
                    .iter()
                    .enumerate()
                    .filter_map(|(index, binding)| {
                        binding.and_then(|_| {
                            active
                                .program
                                .environment_layout()
                                .variable(EnvironmentSlot::new(index))
                                .cloned()
                        })
                    }),
            );
            active.install_revision(monitor_revision, interface_revision);
            return Ok(ReconfigurationReport::new(
                false,
                false,
                monitor_revision,
                interface_revision,
                transfer,
            ));
        }

        let mapping = mapping.unwrap_or_else(|| {
            ReconfigurationMapping::between(active.program(), candidate.program())
        });
        let prepared = candidate.prepare_context_transfer(active, mapping, policy)?;
        let transfer = candidate.context_transfer_from(active, prepared);
        candidate.reconfiguration_transfer_policy = active.reconfiguration_transfer_policy;
        candidate.install_revision(monitor_revision, interface_revision);
        let report = ReconfigurationReport::new(
            monitor_changed,
            interface_changed,
            monitor_revision,
            interface_revision,
            transfer,
        );
        *active = candidate;
        Ok(report)
    }
}
