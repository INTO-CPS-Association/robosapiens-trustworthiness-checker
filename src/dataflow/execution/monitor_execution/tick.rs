use super::super::super::history::HistoryAccess;
use super::MonitorExecution;
use crate::core::Value;
use crate::dataflow::DataflowEvaluationError;

impl MonitorExecution {
    pub(in crate::dataflow) fn tick_in_progress(&self) -> bool {
        self.tick_in_progress
    }

    pub(in crate::dataflow) fn abort_tick(&mut self) {
        self.tick_in_progress = false;
    }

    #[cfg(test)]
    #[inline]
    pub(in crate::dataflow) fn evaluate_source_prelude(
        &mut self,
        environment_values: &mut [Value],
        retained_environment_values: Option<&mut [Value]>,
    ) -> Result<(), DataflowEvaluationError> {
        self.evaluate_source_prelude_with_history(
            environment_values,
            retained_environment_values,
            None,
        )
    }

    #[inline]
    pub(in crate::dataflow) fn evaluate_source_prelude_with_history(
        &mut self,
        environment_values: &mut [Value],
        mut retained_environment_values: Option<&mut [Value]>,
        history_access: Option<HistoryAccess<'_>>,
    ) -> Result<(), DataflowEvaluationError> {
        self.begin_tick();

        let result = self.evaluate_source_range(
            environment_values,
            retained_environment_values.as_deref_mut(),
            history_access,
        );
        if result.is_err() {
            self.tick_in_progress = false;
        }
        result
    }

    #[cfg(test)]
    #[inline]
    pub(in crate::dataflow) fn evaluate_main_and_commit(
        &mut self,
        environment_values: &mut [Value],
        retained_environment_values: Option<&mut [Value]>,
    ) -> Result<(), DataflowEvaluationError> {
        self.evaluate_main_and_commit_with_history(
            environment_values,
            retained_environment_values,
            None,
        )
    }

    #[inline]
    pub(in crate::dataflow) fn evaluate_main_and_commit_with_history(
        &mut self,
        environment_values: &mut [Value],
        mut retained_environment_values: Option<&mut [Value]>,
        history_access: Option<HistoryAccess<'_>>,
    ) -> Result<(), DataflowEvaluationError> {
        if !self.tick_in_progress {
            self.begin_tick();
        }
        let result = self.evaluate_main_range(
            environment_values,
            retained_environment_values.as_deref_mut(),
            history_access,
        );
        if result.is_ok() {
            self.commit_active_plan(
                environment_values,
                retained_environment_values.as_deref(),
                history_access,
            );
        }
        self.tick_in_progress = false;
        result
    }

    #[inline]
    pub(in crate::dataflow) fn evaluate_with_history(
        &mut self,
        environment_values: &mut [Value],
        _retained_environment_values: Option<&mut [Value]>,
        history_access: Option<HistoryAccess<'_>>,
    ) -> Result<(), DataflowEvaluationError> {
        debug_assert!(!self.engine.active_plan.semantic.has_source_barrier());
        self.begin_tick();
        let result = self.evaluate_unbarriered_tick(environment_values, history_access);
        self.tick_in_progress = false;
        result
    }

    fn begin_tick(&mut self) {
        assert!(
            !self.tick_in_progress,
            "source prelude executed twice in one logical tick"
        );
        self.tick_in_progress = true;
        self.activate_execution_tiers();
    }

    #[inline]
    pub(super) fn commit_active_plan(
        &mut self,
        environment_values: &[Value],
        retained_environment_values: Option<&[Value]>,
        history_access: Option<HistoryAccess<'_>>,
    ) {
        for index in 0..self.engine.active_plan.semantic.commit_streams.len() {
            let stream = self.engine.active_plan.semantic.commit_streams[index];
            self.commit_temporal_state(
                stream,
                environment_values,
                retained_environment_values,
                history_access,
            );
        }
    }

    #[cfg(test)]
    pub(in crate::dataflow) fn delay_ring_lengths(&self) -> Vec<usize> {
        self.evaluators.delay_ring_lengths()
    }
}
