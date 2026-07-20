use super::execution::plan_executor::PlanExecutor;
use super::plan::EnvironmentId;
use super::*;
use crate::lang::core::DepGraph;
use std::cell::RefCell;

pub struct DataflowMonitor {
    input_vars: Vec<VarName>,
    output_vars: Vec<VarName>,
    output_ids: Vec<EnvironmentId>,
    // These vectors share stable indices. `evaluation_order` contains indices into them and may
    // change without invalidating environment IDs or moving persistent executor state.
    stream_vars: Vec<VarName>,
    static_dependencies: BTreeMap<VarName, BTreeSet<VarName>>,
    dynamic_dependencies: Vec<Vec<EnvironmentId>>,
    environment_vars: Vec<VarName>,
    stream_ids: Vec<EnvironmentId>,
    stream_executors: Vec<PlanExecutor>,
    evaluation_order: Vec<usize>,
    environment_values: Vec<Value>,
    has_dynamic_dependencies: bool,
    failed: bool,
}

impl DataflowMonitor {
    pub(in crate::dataflow) fn from_compiled_parts(
        input_vars: Vec<VarName>,
        output_vars: Vec<VarName>,
        output_ids: Vec<EnvironmentId>,
        stream_vars: Vec<VarName>,
        static_dependencies: BTreeMap<VarName, BTreeSet<VarName>>,
        stream_executors: Vec<PlanExecutor>,
        environment_size: usize,
    ) -> Self {
        debug_assert_eq!(output_vars.len(), output_ids.len());
        debug_assert_eq!(stream_vars.len(), stream_executors.len());
        debug_assert_eq!(environment_size, input_vars.len() + stream_executors.len());
        debug_assert!(output_ids.iter().all(|id| id.index() < environment_size));
        let stream_ids = (input_vars.len()..environment_size)
            .map(EnvironmentId::new)
            .collect::<Vec<_>>();
        let evaluation_order = (0..stream_executors.len()).collect();
        let dynamic_dependencies = vec![Vec::new(); stream_executors.len()];
        let mut environment_vars = vec![None; environment_size];
        for (index, var) in input_vars.iter().enumerate() {
            environment_vars[index] = Some(var.clone());
        }
        for (index, &id) in stream_ids.iter().enumerate() {
            environment_vars[id.index()] = Some(stream_vars[index].clone());
        }
        let environment_vars = environment_vars
            .into_iter()
            .map(|var| var.expect("every dataflow environment slot must have a variable"))
            .collect();
        let has_dynamic_dependencies = stream_executors
            .iter()
            .any(PlanExecutor::may_reconfigure_dependencies);
        Self {
            input_vars,
            output_vars,
            output_ids,
            stream_vars,
            static_dependencies,
            dynamic_dependencies,
            environment_vars,
            stream_ids,
            stream_executors,
            evaluation_order,
            environment_values: vec![Value::NoVal; environment_size],
            has_dynamic_dependencies,
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
    ) -> Result<(), DataflowEvalError> {
        if self.failed {
            return Err(DataflowEvalError::MonitorFailed);
        }
        if input.len() != self.input_vars.len() {
            return Err(DataflowEvalError::InputCount {
                expected: self.input_vars.len(),
                actual: input.len(),
            });
        }
        if output.len() != self.output_vars.len() {
            return Err(DataflowEvalError::OutputCount {
                expected: self.output_vars.len(),
                actual: output.len(),
            });
        }
        let result = if self.has_dynamic_dependencies {
            self.evaluate_transactionally(input)
        } else {
            // Every computed slot is overwritten exactly once in the known static order, so the
            // static path only needs to replace the input prefix.
            self.environment_values[..input.len()].clone_from_slice(input);
            self.evaluate_static_streams()
        };
        if let Err(error) = result {
            self.failed = true;
            return Err(error);
        }

        for (value, &stream_id) in output.iter_mut().zip(&self.output_ids) {
            *value = self.environment_values[stream_id.index()].clone();
        }
        Ok(())
    }

    fn reset_environment(&mut self, input: &[Value]) {
        self.environment_values.fill(Value::NoVal);
        self.environment_values[..input.len()].clone_from_slice(input);
    }

    fn evaluate_transactionally(&mut self, input: &[Value]) -> Result<(), DataflowEvalError> {
        // A speculative pass may discover dependencies which invalidate the order used for that
        // pass. Every retry therefore starts from the same pre-tick executor state.
        let snapshot = self.stream_executors.clone();
        let previous_order = self.evaluation_order.clone();
        let mut order = previous_order.clone();
        let max_attempts = self
            .stream_executors
            .len()
            .saturating_mul(self.stream_executors.len())
            .saturating_add(2);

        for _ in 0..max_attempts {
            self.stream_executors.clone_from(&snapshot);
            self.reset_environment(input);
            self.evaluation_order.clone_from(&order);
            let observed = match self.evaluate_streams_observing_dependencies() {
                Ok(observed) => observed,
                Err(error) => {
                    self.stream_executors = snapshot;
                    self.evaluation_order = previous_order;
                    self.reset_environment(input);
                    return Err(error);
                }
            };
            let next_order = match self.order_for(&observed) {
                Ok(next_order) => next_order,
                Err(error) => {
                    self.stream_executors = snapshot;
                    self.evaluation_order = previous_order;
                    self.reset_environment(input);
                    return Err(error);
                }
            };
            // The pass is valid precisely when its order is also the order induced by every
            // dependency it observed. The observed edges can change without requiring a retry
            // when the existing order already satisfies them.
            if next_order == order {
                self.dynamic_dependencies = observed;
                return Ok(());
            }
            order = next_order;
        }

        self.stream_executors = snapshot;
        self.evaluation_order = previous_order;
        self.reset_environment(input);
        Err(DataflowEvalError::DynamicSchedulingDidNotConverge)
    }

    fn evaluate_static_streams(&mut self) -> Result<(), DataflowEvalError> {
        for &executor_index in &self.evaluation_order {
            let value =
                self.stream_executors[executor_index].evaluate(&self.environment_values, None)?;
            self.environment_values[self.stream_ids[executor_index].index()] = value;
        }
        Ok(())
    }

    fn evaluate_streams_observing_dependencies(
        &mut self,
    ) -> Result<Vec<Vec<EnvironmentId>>, DataflowEvalError> {
        let mut observed = vec![Vec::new(); self.stream_executors.len()];
        for &executor_index in &self.evaluation_order {
            let dependencies = RefCell::new(Vec::new());
            let value = self.stream_executors[executor_index].evaluate_observing(
                &self.environment_values,
                None,
                Some(&dependencies),
            )?;
            self.environment_values[self.stream_ids[executor_index].index()] = value;
            observed[executor_index] = dependencies.into_inner();
        }
        Ok(observed)
    }

    fn order_for(
        &self,
        dynamic_dependencies: &[Vec<EnvironmentId>],
    ) -> Result<Vec<usize>, DataflowEvalError> {
        let mut dependencies = self.static_dependencies.clone();
        for (stream, dynamic) in self.stream_vars.iter().zip(dynamic_dependencies) {
            dependencies
                .get_mut(stream)
                .expect("compiled stream must have a dependency entry")
                .extend(
                    dynamic
                        .iter()
                        .map(|id| self.environment_vars[id.index()].clone()),
                );
        }
        let stream_set = self.stream_vars.iter().cloned().collect::<BTreeSet<_>>();
        let ordered = DepGraph::from_dependencies(dependencies)
            .topological_streams(&stream_set)
            .map_err(DataflowEvalError::DynamicDependencyCycle)?;
        let indices = self
            .stream_vars
            .iter()
            .enumerate()
            .map(|(index, var)| (var, index))
            .collect::<BTreeMap<_, _>>();
        Ok(ordered.into_iter().map(|var| indices[&var]).collect())
    }
}
