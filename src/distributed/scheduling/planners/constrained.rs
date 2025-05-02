use std::{cell::OnceCell, rc::Rc};

use async_trait::async_trait;
use futures::{StreamExt, stream::LocalBoxStream};
use tracing::info;

use crate::{
    Specification, Value,
    distributed::{
        distribution_graphs::{DistributionGraph, LabelledDistributionGraph},
        solvers::brute_solver::BruteForceDistConstraintSolver,
    },
    semantics::{
        MonitoringSemantics,
        distributed::{contexts::DistributedContext, localisation::Localisable},
    },
};

use super::core::SchedulerPlanner;

pub struct StaticOptimizedSchedulerPlanner<Expr, S, M>
where
    S: MonitoringSemantics<Expr, Value, DistributedContext<Value>>,
    M: Specification<Expr = Expr> + Localisable,
{
    solver: Rc<BruteForceDistConstraintSolver<Expr, S, M>>,
    chosen_dist_graph: OnceCell<Rc<LabelledDistributionGraph>>,
}

impl<Expr, S, M> StaticOptimizedSchedulerPlanner<Expr, S, M>
where
    Expr: 'static,
    S: MonitoringSemantics<Expr, Value, DistributedContext<Value>>,
    M: Specification<Expr = Expr> + Localisable,
{
    pub fn new(solver: BruteForceDistConstraintSolver<Expr, S, M>) -> Self {
        Self {
            solver: Rc::new(solver),
            chosen_dist_graph: OnceCell::new(),
        }
    }
}

#[async_trait(?Send)]
impl<Expr, S, M> SchedulerPlanner for StaticOptimizedSchedulerPlanner<Expr, S, M>
where
    Expr: 'static,
    S: MonitoringSemantics<Expr, Value, DistributedContext<Value>>,
    M: Specification<Expr = Expr> + Localisable,
{
    async fn plan(&self, graph: Rc<DistributionGraph>) -> Option<Rc<LabelledDistributionGraph>> {
        if let Some(chosen_dist_graph) = self.chosen_dist_graph.get() {
            return Some(chosen_dist_graph.clone());
        }

        info!("Initial dist graph stream {:?}", graph);

        let mut labelled_dist_graphs: LocalBoxStream<Rc<LabelledDistributionGraph>> = self
            .solver
            .clone()
            .possible_labelled_dist_graph_stream(graph);

        let chosen_dist_graph: Rc<LabelledDistributionGraph> =
            match labelled_dist_graphs.next().await {
                Some(dist_graph) => dist_graph,
                None => return None,
            };

        self.chosen_dist_graph
            .set(chosen_dist_graph.clone())
            .unwrap();

        info!("Labelled optimized graph: {:?}", chosen_dist_graph);

        Some(chosen_dist_graph)
    }
}
