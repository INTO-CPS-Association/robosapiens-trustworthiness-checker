use std::{cell::OnceCell, rc::Rc, time::Duration};

use async_trait::async_trait;
use futures::{StreamExt, stream::LocalBoxStream};
use smol::Timer;
use tracing::info;

use crate::{
    UntypedDsrvSpecification, Value,
    distributed::{
        distribution_graphs::{DistributionGraph, LabelledDistributionGraph},
        scheduling::planning_context::PlanningContext,
        solvers::brute_solver::BruteForceDistConstraintSolver,
    },
    semantics::{
        AsyncConfig, MonitoringSemantics,
        distributed::{contexts::DistributedContext, localisation::Localisable},
    },
};

use super::core::SchedulerPlanner;

/// Planner for static optimization:
/// computes an optimized labelled distribution graph once and reuses it forever.
pub struct StaticOptimizedSchedulerPlanner<S, AC>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = UntypedDsrvSpecification>,
    AC::Spec: Localisable,
{
    solver: Rc<BruteForceDistConstraintSolver<S, AC>>,
    chosen_dist_graph: OnceCell<Rc<LabelledDistributionGraph>>,
}

impl<S, AC> StaticOptimizedSchedulerPlanner<S, AC>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = UntypedDsrvSpecification>,
    AC::Spec: Localisable,
{
    pub fn new(solver: BruteForceDistConstraintSolver<S, AC>) -> Self {
        Self {
            solver: Rc::new(solver),
            chosen_dist_graph: OnceCell::new(),
        }
    }
}

#[async_trait(?Send)]
impl<S, AC> SchedulerPlanner for StaticOptimizedSchedulerPlanner<S, AC>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = UntypedDsrvSpecification>,
    AC::Spec: Localisable,
{
    async fn plan(
        &self,
        graph: Rc<DistributionGraph>,
        _scheduler_tick: usize,
        planning_context: Option<PlanningContext>,
    ) -> Option<Rc<LabelledDistributionGraph>> {
        if let Some(chosen_dist_graph) = self.chosen_dist_graph.get() {
            return Some(chosen_dist_graph.clone());
        }

        info!("Initial dist graph stream {:?}", graph);

        if !self.solver.input_vars.is_empty() {
            while planning_context
                .as_ref()
                .is_some_and(|context| context.snapshot().history.is_empty())
            {
                info!("Static optimized planner waiting for planning context history");
                Timer::after(Duration::from_millis(25)).await;
            }
        }

        let mut labelled_dist_graphs: LocalBoxStream<Rc<LabelledDistributionGraph>> = self
            .solver
            .clone()
            .possible_labelled_dist_graph_stream(graph);

        let chosen_dist_graph: Rc<LabelledDistributionGraph> = labelled_dist_graphs.next().await?;

        self.chosen_dist_graph
            .set(chosen_dist_graph.clone())
            .unwrap();

        info!("Labelled optimized graph: {:?}", chosen_dist_graph);

        Some(chosen_dist_graph)
    }
}

/// Planner for dynamic optimization:
/// recomputes an optimized labelled distribution graph whenever replanning is triggered.
/// The context history step used for constraint evaluation is derived from the scheduler tick.
pub struct DynamicOptimizedSchedulerPlanner<S, AC>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = UntypedDsrvSpecification>,
    AC::Spec: Localisable,
{
    solver: Rc<BruteForceDistConstraintSolver<S, AC>>,
}

impl<S, AC> DynamicOptimizedSchedulerPlanner<S, AC>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = UntypedDsrvSpecification>,
    AC::Spec: Localisable,
{
    pub fn new(solver: BruteForceDistConstraintSolver<S, AC>) -> Self {
        Self {
            solver: Rc::new(solver),
        }
    }
}

#[async_trait(?Send)]
impl<S, AC> SchedulerPlanner for DynamicOptimizedSchedulerPlanner<S, AC>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = UntypedDsrvSpecification>,
    AC::Spec: Localisable,
{
    async fn plan(
        &self,
        graph: Rc<DistributionGraph>,
        scheduler_tick: usize,
        planning_context: Option<PlanningContext>,
    ) -> Option<Rc<LabelledDistributionGraph>> {
        // Scheduler tick 0 is the bootstrap planning cycle; align context history by offsetting:
        // tick 0 -> context step 0 (bootstrap), tick 1 -> step 0, tick 2 -> step 1, ...
        let context_target_step = scheduler_tick.saturating_sub(1);

        info!(
            "Dynamic optimization planning for graph {:?} at scheduler tick {} (context_target_step={})",
            graph, scheduler_tick, context_target_step
        );

        // Keep searching forward in recorded context time until we find a feasible assignment.
        let mut probe_step = context_target_step;
        loop {
            let latest_context_step = planning_context
                .as_ref()
                .and_then(|context| context.snapshot().history.keys().max().copied());

            let Some(latest_context_step) = latest_context_step else {
                info!(
                    "No planning context history available yet (scheduler tick {}), waiting for input data",
                    scheduler_tick
                );
                Timer::after(Duration::from_millis(25)).await;
                continue;
            };

            if probe_step > latest_context_step {
                info!(
                    "Planning context history not advanced enough (probe_step={}, latest_step={}, scheduler tick {}), waiting for new input data",
                    probe_step, latest_context_step, scheduler_tick
                );
                Timer::after(Duration::from_millis(25)).await;
                continue;
            }

            while probe_step <= latest_context_step {
                let mut labelled_dist_graphs: LocalBoxStream<Rc<LabelledDistributionGraph>> = self
                    .solver
                    .clone()
                    .possible_labelled_dist_graph_stream_with_target_step(
                        graph.clone(),
                        Some(probe_step),
                    );

                if let Some(chosen_dist_graph) = labelled_dist_graphs.next().await {
                    info!(
                        "Labelled optimized graph (dynamic plan, tick {}, context_target_step={}, resolved_step={}): {:?}",
                        scheduler_tick, context_target_step, probe_step, chosen_dist_graph
                    );
                    return Some(chosen_dist_graph);
                }

                info!(
                    "No feasible dynamic optimized graph at context step {} (scheduler tick {}), trying next available context step",
                    probe_step, scheduler_tick
                );

                probe_step = probe_step.saturating_add(1);
            }

            Timer::after(Duration::from_millis(25)).await;
        }
    }
}
