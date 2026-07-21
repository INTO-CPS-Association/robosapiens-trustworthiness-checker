use std::{cell::OnceCell, collections::BTreeMap, rc::Rc, time::Duration};

use async_trait::async_trait;
use futures::{StreamExt, stream::LocalBoxStream};
use smol::Timer;
use tracing::info;

use crate::{
    DsrvSpecification, Value, VarName,
    distributed::{
        distribution_graphs::{DistributionGraph, LabelledDistributionGraph},
        scheduling::planning_context::PlanningContext,
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
        planning_context: Option<PlanningContext>,
    ) -> Option<Rc<LabelledDistributionGraph>> {
        if let Some(chosen_dist_graph) = self.chosen_dist_graph.get() {
            return Some(chosen_dist_graph.clone());
        }

        info!("Initial dist graph stream {:?}", graph);

        let planning_snapshot = loop {
            let snapshot = planning_context
                .as_ref()
                .map(PlanningContext::snapshot)
                .or_else(|| self.solver.default_planning_context.clone());
            let required_inputs = &self.solver.required_planning_inputs;
            if required_inputs.is_empty()
                || snapshot.as_ref().is_some_and(|snapshot| {
                    latest_complete_planning_state(
                        &snapshot.latest_bindings,
                        snapshot.step,
                        required_inputs,
                    )
                    .is_some()
                })
            {
                break snapshot;
            }

            info!(
                required_inputs = ?required_inputs,
                "Static SAT planner waiting for complete planning context before planning"
            );
            Timer::after(Duration::from_millis(25)).await;
        };

        let mut labelled_dist_graphs: LocalBoxStream<Rc<LabelledDistributionGraph>> = self
            .solver
            .clone()
            .possible_labelled_dist_graph_stream_with_context(graph, planning_snapshot);

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
/// planner context has the required inputs before attempting to plan, and then compiles the DSRV
/// property into a SAT problem with substitutions from the latest concrete values.
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
        planning_context: Option<PlanningContext>,
    ) -> Option<Rc<LabelledDistributionGraph>> {
        info!(
            scheduler_tick,
            nodes = graph.graph.node_count(),
            edges = graph.graph.edge_count(),
            "Dynamic SAT optimization planning start"
        );

        loop {
            let required_planning_inputs = &self.solver.required_planning_inputs;
            let snapshot = planning_context.as_ref().map(PlanningContext::snapshot);
            let latest_planning_state = snapshot.as_ref().and_then(|context| {
                latest_complete_planning_state(
                    &context.latest_bindings,
                    context.step,
                    required_planning_inputs,
                )
            });

            let Some(latest_planning_step) = latest_planning_state else {
                if let (Some(planning_context), Some(snapshot)) =
                    (planning_context.as_ref(), snapshot.as_ref())
                {
                    info!(
                        required_inputs = ?required_planning_inputs,
                        "Dynamic SAT planner waiting for complete planning context before planning (scheduler_tick={})",
                        scheduler_tick,
                    );
                    planning_context
                        .wait_for_update_since(snapshot.version)
                        .await;
                } else {
                    Timer::after(Duration::from_millis(25)).await;
                }
                continue;
            };

            info!(
                "Dynamic SAT planner using planning context step {} at scheduler_tick={}",
                latest_planning_step, scheduler_tick
            );
            let snapshot_version = snapshot.as_ref().map(|snapshot| snapshot.version);

            let mut labelled_dist_graphs: LocalBoxStream<Rc<LabelledDistributionGraph>> = self
                .solver
                .clone()
                .possible_labelled_dist_graph_stream_with_context(graph.clone(), snapshot);

            if let Some(chosen_dist_graph) = labelled_dist_graphs.next().await {
                let assigned_streams = chosen_dist_graph
                    .node_labels
                    .values()
                    .map(Vec::len)
                    .sum::<usize>();
                info!(
                    scheduler_tick,
                    planning_step = latest_planning_step,
                    assigned_streams,
                    nodes = chosen_dist_graph.dist_graph.graph.node_count(),
                    "Dynamic SAT optimized graph selected"
                );
                return Some(chosen_dist_graph);
            }

            info!(
                "Dynamic SAT planner found no feasible graph at scheduler_tick={}, planning_step={}; waiting for new planning context data",
                scheduler_tick, latest_planning_step
            );
            if let (Some(planning_context), Some(snapshot_version)) =
                (planning_context.as_ref(), snapshot_version)
            {
                planning_context
                    .wait_for_update_since(snapshot_version)
                    .await;
            } else {
                Timer::after(Duration::from_millis(25)).await;
            }
        }
    }
}

fn latest_complete_planning_state(
    latest_bindings: &BTreeMap<VarName, Value>,
    latest_step: Option<usize>,
    required_inputs: &[VarName],
) -> Option<usize> {
    if required_inputs.is_empty() {
        return latest_step;
    }

    required_inputs
        .iter()
        .all(|required| latest_bindings.contains_key(required))
        .then_some(latest_step)
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VarName;
    use crate::distributed::scheduling::planning_context::PlanningContextSnapshot;
    use crate::dsrv_fixtures::TestDistConfig;
    use crate::lang::dsrv::parser::parse_str;
    use crate::semantics::distributed::semantics::DistributedSemantics;
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
        let s = src;
        parse_str(s).expect("aux SAT test spec should parse")
    }

    fn planning_snapshot(c1: bool, c2: bool, c3: bool) -> PlanningContextSnapshot {
        let mut latest_bindings = BTreeMap::new();
        latest_bindings.insert(VarName::new("c1"), Value::Bool(c1));
        latest_bindings.insert(VarName::new("c2"), Value::Bool(c2));
        latest_bindings.insert(VarName::new("c3"), Value::Bool(c3));

        PlanningContextSnapshot {
            version: 0,
            step: Some(0),
            latest_bindings,
            history: BTreeMap::new(),
        }
    }

    fn planning_context(c1: bool, c2: bool, c3: bool) -> PlanningContext {
        let context = PlanningContext::default();
        context.record_value(VarName::new("c1"), Value::Bool(c1));
        context.record_value(VarName::new("c2"), Value::Bool(c2));
        context.record_value(VarName::new("c3"), Value::Bool(c3));
        context
    }

    #[apply(crate::async_test)]
    async fn static_sat_planner_handles_aux_constraints(
        _executor: Rc<smol::LocalExecutor<'static>>,
    ) {
        let solver =
            SatMonitoredAtDistConstraintSolver::<DistributedSemantics, TestDistConfig>::new(
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
                Some(planning_snapshot(true, false, false)),
            );

        let planner = StaticOptimizedSchedulerPlannerSat::new(solver);
        let graph = graph_abc();

        let labelled = planner
            .plan(graph.clone(), 0, None)
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
        let solver =
            SatMonitoredAtDistConstraintSolver::<DistributedSemantics, TestDistConfig>::new(
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
                None,
            );

        let planner = DynamicOptimizedSchedulerPlannerSat::new(solver);
        let graph = graph_abc();

        let labelled = planner
            .plan(graph.clone(), 1, Some(planning_context(false, true, false)))
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
