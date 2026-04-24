use std::{cell::OnceCell, rc::Rc, time::Duration};

use async_trait::async_trait;
use futures::{StreamExt, stream::LocalBoxStream};
use smol::Timer;
use tracing::info;

use crate::{
    Value,
    distributed::{
        distribution_graphs::{DistributionGraph, LabelledDistributionGraph},
        solvers::sat_solver::SatMonitoredAtDistConstraintSolver,
    },
    semantics::{
        AsyncConfig, MonitoringSemantics,
        distributed::{contexts::DistributedContext, localisation::Localisable},
    },
};

use super::core::SchedulerPlanner;

/// Planner for static optimization:
/// computes an optimized labelled distribution graph once and reuses it forever.
pub struct StaticOptimizedSchedulerPlannerSat<S, AC>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>>,
    AC::Spec: Localisable,
{
    solver: Rc<SatMonitoredAtDistConstraintSolver<S, AC>>,
    chosen_dist_graph: OnceCell<Rc<LabelledDistributionGraph>>,
}

impl<S, AC> StaticOptimizedSchedulerPlannerSat<S, AC>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>>,
    AC::Spec: Localisable,
{
    pub fn new(solver: SatMonitoredAtDistConstraintSolver<S, AC>) -> Self {
        Self {
            solver: Rc::new(solver),
            chosen_dist_graph: OnceCell::new(),
        }
    }
}

#[async_trait(?Send)]
impl<S, AC> SchedulerPlanner for StaticOptimizedSchedulerPlannerSat<S, AC>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>>,
    AC::Spec: Localisable,
{
    async fn plan(
        &self,
        graph: Rc<DistributionGraph>,
        _scheduler_tick: usize,
    ) -> Option<Rc<LabelledDistributionGraph>> {
        if let Some(chosen_dist_graph) = self.chosen_dist_graph.get() {
            return Some(chosen_dist_graph.clone());
        }

        info!("Initial dist graph stream {:?}", graph);

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
///
/// This SAT-backed solver supports timeless monitored_at constraints. It waits until
/// replay history has at least one row before attempting to plan, and then compiles the DSRV
/// property into a SAT problem with substitutions from the latest replay row.
pub struct DynamicOptimizedSchedulerPlannerSat<S, AC>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>>,
    AC::Spec: Localisable,
{
    solver: Rc<SatMonitoredAtDistConstraintSolver<S, AC>>,
}

impl<S, AC> DynamicOptimizedSchedulerPlannerSat<S, AC>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>>,
    AC::Spec: Localisable,
{
    pub fn new(solver: SatMonitoredAtDistConstraintSolver<S, AC>) -> Self {
        Self {
            solver: Rc::new(solver),
        }
    }
}

#[async_trait(?Send)]
impl<S, AC> SchedulerPlanner for DynamicOptimizedSchedulerPlannerSat<S, AC>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>>,
    AC::Spec: Localisable,
{
    async fn plan(
        &self,
        graph: Rc<DistributionGraph>,
        scheduler_tick: usize,
    ) -> Option<Rc<LabelledDistributionGraph>> {
        info!(
            "Dynamic SAT optimization planning start: scheduler_tick={}, graph={:?}",
            scheduler_tick, graph
        );

        loop {
            let latest_replay_step = self
                .solver
                .replay_history
                .as_ref()
                .and_then(|history| history.snapshot())
                .and_then(|snapshot| snapshot.keys().max().copied());

            let Some(latest_replay_step) = latest_replay_step else {
                info!(
                    "Dynamic SAT planner waiting for replay history before planning (scheduler_tick={})",
                    scheduler_tick
                );
                Timer::after(Duration::from_millis(25)).await;
                continue;
            };

            info!(
                "Dynamic SAT planner using latest replay step {} at scheduler_tick={}",
                latest_replay_step, scheduler_tick
            );

            let mut labelled_dist_graphs: LocalBoxStream<Rc<LabelledDistributionGraph>> = self
                .solver
                .clone()
                .possible_labelled_dist_graph_stream(graph.clone());

            if let Some(chosen_dist_graph) = labelled_dist_graphs.next().await {
                info!(
                    "Dynamic SAT optimized graph selected (scheduler_tick={}, replay_step={}): {:?}",
                    scheduler_tick, latest_replay_step, chosen_dist_graph
                );
                return Some(chosen_dist_graph);
            }

            info!(
                "Dynamic SAT planner found no feasible graph at scheduler_tick={}, replay_step={}; waiting for new replay data",
                scheduler_tick, latest_replay_step
            );
            Timer::after(Duration::from_millis(25)).await;
        }
    }
}
