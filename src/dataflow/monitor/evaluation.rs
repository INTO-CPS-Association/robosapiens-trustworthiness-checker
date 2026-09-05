use super::super::history::HistoryAccess;
#[cfg(feature = "jit")]
use super::super::{JitConfig, JitReport};
use super::DataflowMonitor;
use crate::core::Value;
use crate::dataflow::DataflowEvaluationError;
#[cfg(feature = "jit")]
use crate::dataflow::typed::TypedIoLayout;

impl DataflowMonitor {
    /// Enables or disables scalar-region quickening without changing monitor semantics.
    ///
    /// Changing this policy materializes any optimized state before rebuilding the active
    /// execution plan, so it can also be used to compare canonical and quickened execution.
    pub fn set_quickening(&mut self, enabled: bool) {
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

    #[cfg(feature = "jit")]
    pub(in crate::dataflow) fn enable_typed_jit(
        &mut self,
        config: JitConfig,
        layout: TypedIoLayout,
    ) {
        self.execution.enable_typed_jit(config, layout);
    }

    /// Reports which native plan was selected, including safe fallback and backend failures.
    #[cfg(feature = "jit")]
    pub fn jit_report(&self) -> Option<&JitReport> {
        self.execution.jit_report()
    }

    #[inline]
    pub fn evaluate(
        &mut self,
        input: &[Value],
        output: &mut [Value],
    ) -> Result<(), DataflowEvaluationError> {
        if self.failed {
            return Err(DataflowEvaluationError::MonitorFailed);
        }
        if input.len() != self.program.input_vars().len() {
            return Err(DataflowEvaluationError::InputCountMismatch {
                expected: self.program.input_vars().len(),
                actual: input.len(),
            });
        }
        if output.len() != self.program.output_vars().len() {
            return Err(DataflowEvaluationError::OutputCountMismatch {
                expected: self.program.output_vars().len(),
                actual: output.len(),
            });
        }

        if self
            .program
            .monitor_plan()
            .reconfigurable_expressions
            .is_empty()
        {
            return self.evaluate_stable_static(input, output);
        }
        self.evaluate_reconfigurable(input, output)
    }

    #[inline(always)]
    fn evaluate_stable_static(
        &mut self,
        input: &[Value],
        output: &mut [Value],
    ) -> Result<(), DataflowEvaluationError> {
        self.environment_values[..input.len()].clone_from_slice(input);
        let history_access = (!self.history_store.is_empty())
            .then(|| HistoryAccess::new(&self.history_store, &self.history_bindings));
        if let Err(error) =
            self.execution
                .evaluate_with_history(&mut self.environment_values, None, history_access)
        {
            return self.fail_tick(error);
        }
        self.write_outputs(output);
        self.commit_histories();
        Ok(())
    }

    fn evaluate_reconfigurable(
        &mut self,
        input: &[Value],
        output: &mut [Value],
    ) -> Result<(), DataflowEvaluationError> {
        if let Err(error) = self.execute_reconfigurable_tick(input) {
            return self.fail_tick(error);
        }
        self.write_outputs(output);
        self.commit_histories();
        Ok(())
    }

    #[cold]
    fn fail_tick(&mut self, error: DataflowEvaluationError) -> Result<(), DataflowEvaluationError> {
        self.execution.abort_tick();
        self.failed = true;
        Err(error)
    }

    fn execute_reconfigurable_tick(
        &mut self,
        input: &[Value],
    ) -> Result<(), DataflowEvaluationError> {
        debug_assert!(
            !self
                .program
                .monitor_plan()
                .reconfigurable_expressions
                .is_empty()
        );
        self.load_reconfigurable_inputs(input);
        // The source barrier. Every `dynamic`/`defer` source must be evaluated and every body
        // installed *before* any stream advances, because installing a body can add or remove
        // same-tick edges. Resolving first means the scheduler sees one coherent dependency graph
        // for the tick rather than a sequence of intermediate ones, and no stream advances twice
        // merely because the order changed under it.
        self.evaluate_expression_sources()?;
        let resolution = self.resolve_reconfigurable_expressions()?;
        #[cfg(test)]
        {
            self.expression_scan_count += resolution.expressions_scanned;
            self.last_reconfiguration_dependencies_changed = resolution.dependencies_changed;
        }

        if resolution.semantic_reconfiguration {
            self.recompute_history_requirements();
        }
        let schedule_changed = self.scheduler.update_schedule(
            &self.program.monitor_plan().dependencies,
            self.reconfiguration_state.source_streams(),
            self.program.stream_vars(),
        )?;
        if schedule_changed {
            self.select_execution_schedule();
        }
        if resolution.semantic_reconfiguration {
            self.revision = self
                .revision
                .checked_next()
                .ok_or(DataflowEvaluationError::RevisionOverflow)?;
        }
        self.evaluate_scheduled_streams()?;
        self.apply_defer_sealing();
        Ok(())
    }

    pub(super) fn load_reconfigurable_inputs(&mut self, input: &[Value]) {
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

    pub(super) fn evaluate_expression_sources(&mut self) -> Result<(), DataflowEvaluationError> {
        let history_access = (!self.history_store.is_empty())
            .then(|| HistoryAccess::new(&self.history_store, &self.history_bindings));
        self.execution.evaluate_source_prelude_with_history(
            &mut self.environment_values,
            self.retained_environment_values.as_deref_mut(),
            history_access,
        )
    }

    pub(super) fn evaluate_scheduled_streams(&mut self) -> Result<(), DataflowEvaluationError> {
        let history_access = (!self.history_store.is_empty())
            .then(|| HistoryAccess::new(&self.history_store, &self.history_bindings));
        self.execution.evaluate_main_and_commit_with_history(
            &mut self.environment_values,
            self.retained_environment_values.as_deref_mut(),
            history_access,
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

    pub(super) fn select_execution_schedule(&mut self) {
        self.execution.select_schedule_ranges(
            self.reconfiguration_state.source_order(),
            self.scheduler.execution_schedule().evaluation_order(),
            self.program.monitor_plan().stream_slots,
        );
    }

    fn write_outputs(&self, output: &mut [Value]) {
        for (value, &slot) in output.iter_mut().zip(self.program.output_slots()) {
            *value = self.environment_values[slot.index()].clone();
        }
    }
}
