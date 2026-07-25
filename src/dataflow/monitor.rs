use super::environment::EnvironmentSlot;
use super::error::DataflowEvaluationError;
use super::execution::stream_evaluator::StreamEvaluator;
use super::execution_plan::ExecutionPlan;
use super::scheduler::Scheduler;
use super::*;

/// A compiled, stateful synchronous dataflow monitor.
///
/// Each tick evaluates expression sources, resolves reconfiguration points, updates the dependency
/// schedule, evaluates every remaining stream once, and commits staged temporal state. Static
/// monitors are the empty-reconfiguration specialization of the same flow.
pub struct DataflowMonitor {
    input_vars: Vec<VarName>,
    output_vars: Vec<VarName>,
    output_slots: Vec<EnvironmentSlot>,
    stream_vars: Vec<VarName>,
    stream_evaluators: Vec<StreamEvaluator>,
    execution_plan: ExecutionPlan,
    scheduler: Scheduler,
    environment_values: Vec<Value>,
    failed: bool,
}

impl DataflowMonitor {
    pub(in crate::dataflow) fn new(
        input_vars: Vec<VarName>,
        output_vars: Vec<VarName>,
        output_slots: Vec<EnvironmentSlot>,
        stream_vars: Vec<VarName>,
        stream_evaluators: Vec<StreamEvaluator>,
        execution_plan: ExecutionPlan,
        environment_size: usize,
    ) -> Self {
        debug_assert_eq!(output_vars.len(), output_slots.len());
        debug_assert_eq!(stream_vars.len(), stream_evaluators.len());
        debug_assert_eq!(environment_size, input_vars.len() + stream_evaluators.len());
        debug_assert!(
            output_slots
                .iter()
                .all(|slot| slot.index() < environment_size)
        );

        let scheduler = Scheduler::new(
            execution_plan.stream_slots,
            &execution_plan.dependencies,
            &execution_plan.reconfiguration,
        );

        Self {
            input_vars,
            output_vars,
            output_slots,
            stream_vars,
            stream_evaluators,
            execution_plan,
            scheduler,
            environment_values: vec![Value::NoVal; environment_size],
            failed: false,
        }
    }

    pub fn input_vars(&self) -> &[VarName] {
        &self.input_vars
    }

    pub fn output_vars(&self) -> &[VarName] {
        &self.output_vars
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
        self.load_inputs(input);
        self.evaluate_expression_sources();
        self.resolve_reconfiguration_points()?;
        self.scheduler.update_schedule(
            &self.execution_plan.dependencies,
            &self.execution_plan.reconfiguration,
            &self.stream_vars,
        )?;
        self.evaluate_scheduled_streams()?;
        self.commit_temporal_state();
        Ok(())
    }

    fn load_inputs(&mut self, input: &[Value]) {
        if !self.execution_plan.reconfiguration.is_empty() {
            self.environment_values.fill(Value::NoVal);
        }
        self.environment_values[..input.len()].clone_from_slice(input);
    }

    fn evaluate_expression_sources(&mut self) {
        let first_stream_slot = self.execution_plan.stream_slots.start().index();
        for &stream in self.execution_plan.reconfiguration.evaluation_order() {
            let value = self.stream_evaluators[stream.index()]
                .evaluate_infallible_and_stage(&self.environment_values);
            self.environment_values[first_stream_slot + stream.index()] = value;
        }
    }

    fn resolve_reconfiguration_points(&mut self) -> Result<(), DataflowEvaluationError> {
        let reconfiguration = &self.execution_plan.reconfiguration;
        let environment_values = &self.environment_values;
        let stream_evaluators = &mut self.stream_evaluators;
        let scheduler = &mut self.scheduler;

        for stream in self
            .execution_plan
            .dependencies
            .reconfigurable_streams()
            .iter()
        {
            let dependencies = scheduler.begin_dynamic_dependency_update(stream);
            for point in reconfiguration.points_for(stream) {
                debug_assert_eq!(point.stream, stream);
                let source_value = point.source.read_value(environment_values);
                let dependency_slots = stream_evaluators[stream.index()]
                    .resolve_reconfiguration_point(point.node, source_value)?;
                dependencies.extend(dependency_slots);
            }
            dependencies.finish();
        }
        Ok(())
    }

    fn evaluate_scheduled_streams(&mut self) -> Result<(), DataflowEvaluationError> {
        let schedule = self.scheduler.execution_schedule();
        if schedule.uses_static_order() && self.execution_plan.all_streams_infallible {
            self.evaluate_infallible_static_order();
        } else if schedule.uses_static_order() {
            self.try_evaluate_static_order()?;
        } else {
            self.try_evaluate_scheduled_order()?;
        }
        Ok(())
    }

    fn evaluate_infallible_static_order(&mut self) {
        let first_stream_slot = self.execution_plan.stream_slots.start().index();
        for (index, evaluator) in self.stream_evaluators.iter_mut().enumerate() {
            let value = evaluator.evaluate_infallible_and_stage(&self.environment_values);
            self.environment_values[first_stream_slot + index] = value;
        }
    }

    fn try_evaluate_static_order(&mut self) -> Result<(), DataflowEvaluationError> {
        let first_stream_slot = self.execution_plan.stream_slots.start().index();
        for (index, evaluator) in self.stream_evaluators.iter_mut().enumerate() {
            let value = evaluator.evaluate_and_stage(&self.environment_values)?;
            self.environment_values[first_stream_slot + index] = value;
        }
        Ok(())
    }

    fn try_evaluate_scheduled_order(&mut self) -> Result<(), DataflowEvaluationError> {
        for &stream in self.scheduler.execution_schedule().evaluation_order() {
            let value = self.stream_evaluators[stream.index()]
                .evaluate_and_stage(&self.environment_values)?;
            let slot = self.execution_plan.stream_slots.slot(stream);
            self.environment_values[slot.index()] = value;
        }
        Ok(())
    }

    fn commit_temporal_state(&mut self) {
        for stream in self.execution_plan.temporal_streams.iter() {
            self.stream_evaluators[stream.index()].commit_temporal_state(&self.environment_values);
        }
    }

    fn write_outputs(&self, output: &mut [Value]) {
        for (value, &slot) in output.iter_mut().zip(&self.output_slots) {
            *value = self.environment_values[slot.index()].clone();
        }
    }
}
