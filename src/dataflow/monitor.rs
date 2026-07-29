use super::environment::EnvironmentSlot;
use super::error::DataflowEvaluationError;
use super::execution::monitor_execution::MonitorExecution;
use super::execution_plan::MonitorPlan;
use super::ir::StreamProgram;
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
    execution: MonitorExecution,
    monitor_plan: MonitorPlan,
    scheduler: Scheduler,
    environment_values: Vec<Value>,
    retained_environment_values: Option<Vec<Value>>,
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

        let scheduler = Scheduler::new(
            monitor_plan.stream_slots,
            &monitor_plan.dependencies,
            &monitor_plan.reconfiguration,
        );
        let execution = MonitorExecution::new(
            stream_programs,
            monitor_plan.stream_slots,
            scheduler.execution_schedule().evaluation_order(),
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
            scheduler,
            environment_values: vec![Value::NoVal; environment_size],
            retained_environment_values,
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
        if self.monitor_plan.reconfiguration.is_empty() {
            self.execution
                .evaluate(&mut self.environment_values, None)?;
            self.commit_temporal_state();
            return Ok(());
        }
        self.evaluate_expression_sources();
        self.resolve_reconfiguration_points()?;
        let schedule_changed = self.scheduler.update_schedule(
            &self.monitor_plan.dependencies,
            &self.monitor_plan.reconfiguration,
            &self.stream_vars,
        )?;
        if schedule_changed {
            self.execution.select_schedule(
                self.scheduler.execution_schedule().evaluation_order(),
                self.monitor_plan.stream_slots,
            );
        }
        self.evaluate_scheduled_streams()?;
        self.commit_temporal_state();
        self.retain_environment_values();
        Ok(())
    }

    fn load_inputs(&mut self, input: &[Value]) {
        if !self.monitor_plan.reconfiguration.is_empty() {
            self.environment_values.fill(Value::NoVal);
        }
        self.environment_values[..input.len()].clone_from_slice(input);
    }

    fn evaluate_expression_sources(&mut self) {
        let first_stream_slot = self.monitor_plan.stream_slots.start().index();
        for &stream in self.monitor_plan.reconfiguration.evaluation_order() {
            let value = self
                .execution
                .evaluate_infallible_stream(stream, &self.environment_values);
            self.environment_values[first_stream_slot + stream.index()] = value;
        }
    }

    fn resolve_reconfiguration_points(&mut self) -> Result<(), DataflowEvaluationError> {
        let reconfiguration = &self.monitor_plan.reconfiguration;
        let environment_values = &self.environment_values;
        let execution = &mut self.execution;
        let scheduler = &mut self.scheduler;

        for stream in self
            .monitor_plan
            .dependencies
            .reconfigurable_streams()
            .iter()
        {
            let dependencies = scheduler.begin_dynamic_dependency_update(stream);
            for point in reconfiguration.points_for(stream) {
                debug_assert_eq!(point.stream, stream);
                let source_value = point.source.read_value(environment_values);
                let dependency_slots =
                    execution.resolve_reconfiguration_point(stream, point.node, source_value)?;
                dependencies.extend(dependency_slots);
            }
            dependencies.finish();
        }
        Ok(())
    }

    fn evaluate_scheduled_streams(&mut self) -> Result<(), DataflowEvaluationError> {
        self.execution.evaluate(
            &mut self.environment_values,
            self.retained_environment_values.as_deref(),
        )
    }

    fn commit_temporal_state(&mut self) {
        for stream in self.monitor_plan.temporal_streams.iter() {
            self.execution.commit_temporal_state(
                stream,
                &self.environment_values,
                self.retained_environment_values.as_deref(),
            );
        }
    }

    fn retain_environment_values(&mut self) {
        let retained = self
            .retained_environment_values
            .as_mut()
            .expect("reconfigurable monitors retain their outer environment");
        for (retained, current) in retained.iter_mut().zip(&self.environment_values) {
            if current != &Value::NoVal {
                retained.clone_from(current);
            }
        }
    }

    fn write_outputs(&self, output: &mut [Value]) {
        for (value, &slot) in output.iter_mut().zip(&self.output_slots) {
            *value = self.environment_values[slot.index()].clone();
        }
    }
}

#[cfg(test)]
pub(in crate::dataflow) mod test_support {
    use super::*;

    pub(in crate::dataflow) fn execution(monitor: &DataflowMonitor) -> &MonitorExecution {
        &monitor.execution
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
    fn static_monitor_does_not_allocate_a_retained_environment() {
        let specification = "in x: Int\nout z: Int\nz = x + 1"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let monitor = DataflowMonitor::compile_checked(specification).unwrap();

        assert!(monitor.retained_environment_values.is_none());
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
    fn recursive_delay_state_survives_execution_layout() {
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
    fn deoptimization_state_survives_cached_layout_swaps() {
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
