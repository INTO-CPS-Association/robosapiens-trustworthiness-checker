//! Monitor-level ownership and replaceable, cacheable execution layouts.
//!
//! A fixed arena owns one persistent evaluator per logical stream. An execution
//! layout only chooses an evaluation order and replaces eligible environment
//! reads with compact values published by earlier planned streams. Layout rebuilds
//! therefore reorder stream IDs without moving temporal, function,
//! dynamic-expression, or deoptimization state.
//!
//! Logical results are published after every planned stream. Canonical instructions and
//! nested evaluators consequently observe the canonical environment and do not
//! form fusion barriers.
//!
//! Consecutive whole-stream scalar programs form runs. Richer programs delimit
//! those runs and continue through the graph evaluator, which can still use a
//! mixed specialization plan and publish a scalar result for later streams.

use super::super::execution_plan::{StreamId, StreamSlots};
use super::super::ir::{NodeId, StreamProgram};
use super::super::*;
use super::specialization::{self, ScalarValue};
use super::stream_evaluator::StreamEvaluator;

const EXECUTION_LAYOUT_CACHE_SIZE: usize = 4;

struct EvaluatorArena {
    evaluators: Box<[StreamEvaluator]>,
    published_scalars: Box<[Option<ScalarValue>]>,
}

pub(in crate::dataflow) struct MonitorExecution {
    evaluators: EvaluatorArena,
    execution_layout: ExecutionLayout,
    cached_layouts: Vec<ExecutionLayout>,
    #[cfg(test)]
    layout_rebuilds: usize,
}

#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
pub(in crate::dataflow) enum TestLayoutStep {
    ScalarRun(Vec<usize>),
    Graph(usize),
}

impl MonitorExecution {
    pub(in crate::dataflow) fn new(
        programs: Vec<Rc<StreamProgram>>,
        stream_slots: StreamSlots,
        initial_order: &[StreamId],
    ) -> Self {
        let evaluators = EvaluatorArena::new(programs);
        let execution_layout = ExecutionLayout::new(&evaluators, stream_slots, initial_order);
        Self {
            evaluators,
            execution_layout,
            cached_layouts: Vec::new(),
            #[cfg(test)]
            layout_rebuilds: 0,
        }
    }

    #[inline]
    pub(in crate::dataflow) fn evaluate_infallible_stream(
        &mut self,
        stream: StreamId,
        environment_values: &[Value],
    ) -> Value {
        self.evaluator(stream)
            .evaluate_infallible_and_stage_with_plan(environment_values, None, &[])
    }

    #[inline]
    pub(in crate::dataflow) fn resolve_reconfiguration_point(
        &mut self,
        stream: StreamId,
        node: NodeId,
        source_value: Value,
    ) -> Result<&[EnvironmentSlot], DataflowEvaluationError> {
        self.evaluator(stream)
            .resolve_reconfiguration_point(node, source_value)
    }

    #[inline]
    pub(in crate::dataflow) fn commit_temporal_state(
        &mut self,
        stream: StreamId,
        environment_values: &[Value],
        retained_environment_values: Option<&[Value]>,
    ) {
        self.evaluator(stream)
            .commit_temporal_state_with_retained_environment(
                environment_values,
                retained_environment_values,
            );
    }

    pub(in crate::dataflow) fn select_schedule(
        &mut self,
        order: &[StreamId],
        stream_slots: StreamSlots,
    ) {
        if self.execution_layout.order.as_ref() == order {
            return;
        }
        if let Some(cached) = self
            .cached_layouts
            .iter()
            .position(|layout| layout.order.as_ref() == order)
        {
            std::mem::swap(&mut self.execution_layout, &mut self.cached_layouts[cached]);
        } else {
            let new_layout = ExecutionLayout::new(&self.evaluators, stream_slots, order);
            let previous = std::mem::replace(&mut self.execution_layout, new_layout);
            if self.cached_layouts.len() == EXECUTION_LAYOUT_CACHE_SIZE {
                self.cached_layouts.remove(0);
            }
            self.cached_layouts.push(previous);
            #[cfg(test)]
            {
                self.layout_rebuilds += 1;
            }
        }
    }

    pub(in crate::dataflow) fn evaluate(
        &mut self,
        environment_values: &mut [Value],
        retained_environment_values: Option<&[Value]>,
    ) -> Result<(), DataflowEvaluationError> {
        // A published source can only name an earlier stream in this layout, so
        // every value it can read has already been overwritten for this tick.
        let evaluators = &mut self.evaluators;
        for step in &self.execution_layout.steps {
            match step {
                LayoutStep::ScalarRun(run) => {
                    evaluators.evaluate_scalar_run(run, environment_values);
                }
                LayoutStep::Graph(step) => {
                    evaluators.evaluate_graph(
                        step,
                        environment_values,
                        retained_environment_values,
                    )?;
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn evaluator(&mut self, stream: StreamId) -> &mut StreamEvaluator {
        self.evaluators.evaluator(stream.index())
    }

    #[cfg(test)]
    pub(in crate::dataflow) fn published_source_count(&self) -> usize {
        self.execution_layout
            .steps
            .iter()
            .map(|step| match step {
                LayoutStep::ScalarRun(run) => run
                    .iter()
                    .map(|stream| stream.plan.published_source_count())
                    .sum(),
                LayoutStep::Graph(stream) => stream
                    .specialization_plan
                    .as_ref()
                    .map_or(0, specialization::Plan::published_source_count),
            })
            .sum()
    }

    #[cfg(test)]
    pub(in crate::dataflow) fn scalar_run_stream_count(&self) -> usize {
        self.execution_layout
            .steps
            .iter()
            .map(|step| match step {
                LayoutStep::ScalarRun(run) => run.len(),
                LayoutStep::Graph(_) => 0,
            })
            .sum()
    }

    #[cfg(test)]
    pub(in crate::dataflow) fn scalar_run_count(&self) -> usize {
        self.execution_layout
            .steps
            .iter()
            .filter(|step| matches!(step, LayoutStep::ScalarRun(_)))
            .count()
    }

    #[cfg(test)]
    pub(in crate::dataflow) fn layout_steps(&self) -> Vec<TestLayoutStep> {
        self.execution_layout
            .steps
            .iter()
            .map(|step| match step {
                LayoutStep::ScalarRun(run) => {
                    TestLayoutStep::ScalarRun(run.iter().map(|step| step.stream.index()).collect())
                }
                LayoutStep::Graph(step) => TestLayoutStep::Graph(step.stream.index()),
            })
            .collect()
    }

    #[cfg(test)]
    pub(in crate::dataflow) fn deoptimized_node_count(&self, stream: usize) -> usize {
        self.evaluators.evaluators[stream].deoptimized_node_count()
    }

    #[cfg(test)]
    pub(in crate::dataflow) fn layout_rebuilds(&self) -> usize {
        self.layout_rebuilds
    }
}

struct ExecutionLayout {
    order: Box<[StreamId]>,
    steps: Box<[LayoutStep]>,
}

enum LayoutStep {
    ScalarRun(Box<[ScalarStep]>),
    Graph(GraphStep),
}

struct GraphStep {
    stream: StreamId,
    slot: EnvironmentSlot,
    specialization_plan: Option<specialization::Plan>,
}

struct ScalarStep {
    stream: StreamId,
    slot: EnvironmentSlot,
    plan: specialization::SingleScalarPlan,
}

impl ExecutionLayout {
    fn new(evaluators: &EvaluatorArena, stream_slots: StreamSlots, order: &[StreamId]) -> Self {
        let mut available = vec![false; evaluators.len()];
        let mut steps = Vec::with_capacity(order.len());
        let mut scalar_run = Vec::new();

        for &stream in order {
            let program = evaluators.program(stream.index());
            let specialization_plan = program.specialization_plan.as_ref().map(|_| {
                specialization::Plan::with_published_sources(&program.graph, |slot| {
                    stream_slots
                        .stream(slot)
                        .filter(|producer| available[producer.index()])
                        .map(StreamId::index)
                })
                .expect("layout specialization must preserve canonical instruction shapes")
            });
            let slot = stream_slots.slot(stream);
            let (single_scalar_plan, specialization_plan) = match specialization_plan {
                Some(plan) => match plan.try_into_single_scalar(&program.graph) {
                    Ok(plan) => (Some(plan), None),
                    Err(plan) => (None, Some(plan)),
                },
                None => (None, None),
            };
            if let Some(plan) = single_scalar_plan {
                scalar_run.push(ScalarStep { stream, slot, plan });
            } else {
                if !scalar_run.is_empty() {
                    steps.push(LayoutStep::ScalarRun(
                        std::mem::take(&mut scalar_run).into_boxed_slice(),
                    ));
                }
                steps.push(LayoutStep::Graph(GraphStep {
                    stream,
                    slot,
                    specialization_plan,
                }));
            }
            available[stream.index()] = true;
        }
        if !scalar_run.is_empty() {
            steps.push(LayoutStep::ScalarRun(scalar_run.into_boxed_slice()));
        }

        Self {
            order: order.to_vec().into_boxed_slice(),
            steps: steps.into_boxed_slice(),
        }
    }
}

impl EvaluatorArena {
    fn new(programs: Vec<Rc<StreamProgram>>) -> Self {
        let published_scalars = vec![None; programs.len()].into_boxed_slice();
        let evaluators = programs
            .into_iter()
            .map(StreamEvaluator::new)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            evaluators,
            published_scalars,
        }
    }

    #[inline]
    fn evaluator(&mut self, stream: usize) -> &mut StreamEvaluator {
        &mut self.evaluators[stream]
    }

    #[inline]
    fn evaluator_with_published(
        &mut self,
        stream: usize,
    ) -> (&mut StreamEvaluator, &[Option<ScalarValue>]) {
        (&mut self.evaluators[stream], &self.published_scalars)
    }

    #[inline]
    fn evaluate_scalar_run(&mut self, run: &[ScalarStep], environment_values: &mut [Value]) {
        for step in run {
            let index = step.stream.index();
            let (evaluator, published_scalars) = self.evaluator_with_published(index);
            let result = evaluator.evaluate_single_scalar_with_plan(
                environment_values,
                &step.plan,
                published_scalars,
            );
            let value = match result {
                specialization::DirectResult::Scalar(value) => {
                    self.publish(index, Some(value));
                    value.into_value()
                }
                specialization::DirectResult::Canonical(value) => {
                    self.publish(index, ScalarValue::from_untyped_value(&value));
                    value
                }
            };
            environment_values[step.slot.index()] = value;
        }
    }

    #[inline]
    fn evaluate_graph(
        &mut self,
        step: &GraphStep,
        environment_values: &mut [Value],
        retained_environment_values: Option<&[Value]>,
    ) -> Result<(), DataflowEvaluationError> {
        let index = step.stream.index();
        let (evaluator, published_scalars) = self.evaluator_with_published(index);
        let value = if evaluator.program.is_infallible() {
            evaluator.evaluate_infallible_and_stage_with_plan(
                environment_values,
                step.specialization_plan.as_ref(),
                published_scalars,
            )
        } else if let Some(retained) = retained_environment_values {
            evaluator.evaluate_and_stage_with_retained_environment(environment_values, retained)?
        } else {
            evaluator.evaluate_and_stage(environment_values)?
        };
        self.publish(index, ScalarValue::from_untyped_value(&value));
        environment_values[step.slot.index()] = value;
        Ok(())
    }

    #[inline]
    fn publish(&mut self, stream: usize, value: Option<ScalarValue>) {
        self.published_scalars[stream] = value;
    }

    fn len(&self) -> usize {
        self.evaluators.len()
    }

    #[inline]
    fn program(&self, stream: usize) -> &StreamProgram {
        &self.evaluators[stream].program
    }
}
