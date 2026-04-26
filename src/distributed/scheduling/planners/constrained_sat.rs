use std::{cell::OnceCell, rc::Rc, time::Duration};

use async_trait::async_trait;
use futures::{StreamExt, stream::LocalBoxStream};
use smol::Timer;
use tracing::info;

use crate::{
    DsrvSpecification, Value,
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
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = DsrvSpecification>,
    AC::Spec: Localisable,
{
    solver: Rc<SatMonitoredAtDistConstraintSolver<S, AC>>,
    chosen_dist_graph: OnceCell<Rc<LabelledDistributionGraph>>,
}

impl<S, AC> StaticOptimizedSchedulerPlannerSat<S, AC>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = DsrvSpecification>,
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
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = DsrvSpecification>,
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
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = DsrvSpecification>,
    AC::Spec: Localisable,
{
    solver: Rc<SatMonitoredAtDistConstraintSolver<S, AC>>,
}

impl<S, AC> DynamicOptimizedSchedulerPlannerSat<S, AC>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = DsrvSpecification>,
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
    AC: AsyncConfig<Val = Value, Ctx = DistributedContext<AC>, Spec = DsrvSpecification>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsrv_fixtures::TestDistConfig;
    use crate::lang::dsrv::{lalr_parser::LALRParser, parser::dsrv_specification};
    use crate::semantics::distributed::semantics::DistributedSemantics;
    use crate::{VarName, io::replay_history::ReplayHistory};
    use macro_rules_attribute::apply;
    use petgraph::graph::DiGraph;
    use std::collections::BTreeMap;

    fn graph_abc() -> Rc<DistributionGraph> {
        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        let c = graph.add_node("C".into());

        graph.add_edge(a, b, 1);
        graph.add_edge(b, a, 1);
        graph.add_edge(b, c, 1);
        graph.add_edge(c, b, 1);
        graph.add_edge(a, c, 1);
        graph.add_edge(c, a, 1);

        Rc::new(DistributionGraph {
            central_monitor: a,
            graph,
        })
    }

    fn aux_spec() -> DsrvSpecification {
        let src = r#"
in c1
in c2
in c3
out s1
out s2
out s3
out d1
out d2
out d3
aux h1
aux h2
h1 = if c1 then monitored_at(s1, A) else monitored_at(s1, B)
h2 = if c2 then monitored_at(s2, B) else monitored_at(s2, C)
d1 = h1
d2 = h2
d3 = if ((h1 && h2) || c3) then monitored_at(s3, C) else monitored_at(s3, A)
"#
        .trim();
        let mut s = src;
        dsrv_specification(&mut s).expect("aux SAT test spec should parse")
    }

    fn replay(c1: bool, c2: bool, c3: bool) -> ReplayHistory {
        let mut row = BTreeMap::new();
        row.insert(VarName::new("c1"), Value::Bool(c1));
        row.insert(VarName::new("c2"), Value::Bool(c2));
        row.insert(VarName::new("c3"), Value::Bool(c3));

        let mut snapshot = BTreeMap::new();
        snapshot.insert(0usize, row);
        ReplayHistory::store_all_with_snapshot(snapshot)
    }

    #[apply(crate::async_test)]
    async fn static_sat_planner_handles_aux_constraints(
        _executor: Rc<smol::LocalExecutor<'static>>,
    ) {
        let solver = SatMonitoredAtDistConstraintSolver::<
            DistributedSemantics<LALRParser>,
            TestDistConfig,
        >::new(
            vec!["d1".into(), "d2".into(), "d3".into()],
            vec![
                "s1".into(),
                "s2".into(),
                "s3".into(),
                "d1".into(),
                "d2".into(),
                "d3".into(),
            ],
            aux_spec(),
            Some(replay(true, false, false)),
        );

        let planner = StaticOptimizedSchedulerPlannerSat::new(solver);
        let graph = graph_abc();

        let labelled = planner
            .plan(graph.clone(), 0)
            .await
            .expect("static SAT planner should return a graph");

        let a_idx = graph
            .get_node_index_by_name(&"A".into())
            .expect("A must exist");
        let b_idx = graph
            .get_node_index_by_name(&"B".into())
            .expect("B must exist");
        let c_idx = graph
            .get_node_index_by_name(&"C".into())
            .expect("C must exist");

        assert!(labelled.node_labels[&a_idx].contains(&VarName::new("s1")));
        assert!(labelled.node_labels[&c_idx].contains(&VarName::new("s2")));
        assert!(labelled.node_labels[&c_idx].contains(&VarName::new("s3")));
        assert!(!labelled.node_labels[&b_idx].contains(&VarName::new("s1")));
    }

    #[apply(crate::async_test)]
    async fn dynamic_sat_planner_handles_aux_constraints(
        _executor: Rc<smol::LocalExecutor<'static>>,
    ) {
        let solver = SatMonitoredAtDistConstraintSolver::<
            DistributedSemantics<LALRParser>,
            TestDistConfig,
        >::new(
            vec!["d1".into(), "d2".into(), "d3".into()],
            vec![
                "s1".into(),
                "s2".into(),
                "s3".into(),
                "d1".into(),
                "d2".into(),
                "d3".into(),
            ],
            aux_spec(),
            Some(replay(false, true, false)),
        );

        let planner = DynamicOptimizedSchedulerPlannerSat::new(solver);
        let graph = graph_abc();

        let labelled = planner
            .plan(graph.clone(), 1)
            .await
            .expect("dynamic SAT planner should return a graph");

        let a_idx = graph
            .get_node_index_by_name(&"A".into())
            .expect("A must exist");
        let b_idx = graph
            .get_node_index_by_name(&"B".into())
            .expect("B must exist");
        let c_idx = graph
            .get_node_index_by_name(&"C".into())
            .expect("C must exist");

        assert!(labelled.node_labels[&b_idx].contains(&VarName::new("s1")));
        assert!(labelled.node_labels[&b_idx].contains(&VarName::new("s2")));
        assert!(labelled.node_labels[&c_idx].contains(&VarName::new("s3")));
        assert!(!labelled.node_labels[&a_idx].contains(&VarName::new("s1")));
    }
}
