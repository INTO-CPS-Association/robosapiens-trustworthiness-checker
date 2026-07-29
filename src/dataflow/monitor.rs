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

        Self {
            input_vars,
            output_vars,
            output_slots,
            stream_vars,
            execution,
            monitor_plan,
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
        if self.monitor_plan.reconfiguration.is_empty() {
            self.execution.evaluate(&mut self.environment_values)?;
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
        self.execution.evaluate(&mut self.environment_values)
    }

    fn commit_temporal_state(&mut self) {
        for stream in self.monitor_plan.temporal_streams.iter() {
            self.execution
                .commit_temporal_state(stream, &self.environment_values);
        }
    }

    fn write_outputs(&self, output: &mut [Value]) {
        for (value, &slot) in output.iter_mut().zip(&self.output_slots) {
            *value = self.environment_values[slot.index()].clone();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::execution::monitor_execution::TestLayoutStep;
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
    fn static_scalar_chain_uses_published_scalar_sources() {
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
        assert!(monitor.execution.published_source_count() > 0);
        assert_eq!(monitor.execution.scalar_run_stream_count(), 3);
        assert_eq!(monitor.execution.scalar_run_count(), 1);

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
        assert_eq!(monitor.execution.scalar_run_stream_count(), 2);
        assert_eq!(monitor.execution.scalar_run_count(), 1);

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
        assert!(monitor.execution.published_source_count() > 0);

        let mut output = [Value::NoVal, Value::NoVal];
        monitor.evaluate(&[Value::Int(4)], &mut output).unwrap();
        assert_eq!(output, [Value::Int(5), Value::Int(15)]);
    }

    #[test]
    fn nested_graph_scope_uses_the_same_execution_layout() {
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
        assert!(monitor.execution.published_source_count() > 0);
        assert_eq!(
            monitor.execution.layout_steps(),
            [
                TestLayoutStep::ScalarRun(vec![0, 1]),
                TestLayoutStep::Graph(2),
                TestLayoutStep::ScalarRun(vec![3, 4]),
            ]
        );

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
    fn temporal_stream_delimits_scalar_runs() {
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
        assert_eq!(
            monitor.execution.layout_steps(),
            [
                TestLayoutStep::ScalarRun(vec![0]),
                TestLayoutStep::Graph(1),
                TestLayoutStep::ScalarRun(vec![2]),
            ]
        );

        let mut output = [Value::NoVal];
        for (input, expected) in [(10, 2), (20, 24), (30, 44)] {
            monitor.evaluate(&[Value::Int(input)], &mut output).unwrap();
            assert_eq!(output, [Value::Int(expected)]);
        }
    }

    #[test]
    fn temporal_maple_cycle_uses_published_scalar_sources() {
        let specification = crate::dsrv_fixtures::spec_maple_sequence()
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let mut monitor = DataflowMonitor::compile_checked(specification).unwrap();
        assert!(monitor.execution.published_source_count() > 0);

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
        assert!(monitor.execution.published_source_count() > 0);

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
    fn dynamic_schedule_changes_rebuild_only_the_replaceable_layout() {
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
        for (values, expected, layout) in [
            (
                [
                    ("x", Value::Int(10)),
                    ("a_source", Value::Str("b + 1".into())),
                    ("b_source", Value::Str("x".into())),
                ],
                [Value::Int(11), Value::Int(10)],
                [TestLayoutStep::Graph(1), TestLayoutStep::Graph(0)],
            ),
            (
                [
                    ("x", Value::Int(20)),
                    ("a_source", Value::Str("x".into())),
                    ("b_source", Value::Str("a + 1".into())),
                ],
                [Value::Int(20), Value::Int(21)],
                [TestLayoutStep::Graph(0), TestLayoutStep::Graph(1)],
            ),
            (
                [
                    ("x", Value::Int(30)),
                    ("a_source", Value::Str("x".into())),
                    ("b_source", Value::Str("a + 1".into())),
                ],
                [Value::Int(30), Value::Int(31)],
                [TestLayoutStep::Graph(0), TestLayoutStep::Graph(1)],
            ),
        ] {
            let input = input_row(&monitor, &values);
            monitor.evaluate(&input, &mut output).unwrap();
            assert_eq!(output, expected);
            assert_eq!(monitor.execution.layout_steps(), layout);
            assert_eq!(monitor.execution.layout_rebuilds(), 1);
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
        let equal = monitor
            .stream_vars
            .iter()
            .position(|stream| stream == &VarName::new("equal"))
            .unwrap();
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
        assert_eq!(monitor.execution.deoptimized_node_count(equal), 1);
        assert_eq!(monitor.execution.layout_rebuilds(), 1);

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
        assert_eq!(monitor.execution.deoptimized_node_count(equal), 1);
        assert_eq!(monitor.execution.layout_rebuilds(), 1);
    }
}
