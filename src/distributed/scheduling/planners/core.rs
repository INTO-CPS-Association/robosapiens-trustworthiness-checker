use std::{collections::BTreeMap, rc::Rc};

use async_trait::async_trait;
use tracing::info;

use crate::{
    VarName,
    distributed::distribution_graphs::{
        DistributionGraph, LabelledDistributionGraph, NodeName, graph_to_png,
    },
};

#[async_trait(?Send)]
pub trait SchedulerPlanner {
    async fn plan(&self, graph: Rc<DistributionGraph>) -> Option<Rc<LabelledDistributionGraph>>;
}

pub struct StaticFixedSchedulerPlanner {
    pub fixed_graph: Rc<LabelledDistributionGraph>,
}

#[async_trait(?Send)]
impl SchedulerPlanner for StaticFixedSchedulerPlanner {
    async fn plan(&self, _graph: Rc<DistributionGraph>) -> Option<Rc<LabelledDistributionGraph>> {
        Some(self.fixed_graph.clone())
    }
}

pub struct CentralisedSchedulerPlanner {
    pub var_names: Vec<VarName>,
    pub central_node: NodeName,
}

#[async_trait(?Send)]
impl SchedulerPlanner for CentralisedSchedulerPlanner {
    async fn plan(&self, graph: Rc<DistributionGraph>) -> Option<Rc<LabelledDistributionGraph>> {
        let labels = graph
            .graph
            .node_indices()
            .map(|i| {
                let node = graph.graph.node_weight(i).unwrap();
                (
                    i.clone(),
                    if *node == self.central_node {
                        self.var_names.clone()
                    } else {
                        vec![]
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let labelled_graph = Rc::new(LabelledDistributionGraph {
            dist_graph: graph,
            var_names: self.var_names.clone(),
            node_labels: labels,
        });
        info!("Labelled graph: {:?}", labelled_graph);

        graph_to_png(labelled_graph.clone(), "distributed_graph.png")
            .await
            .unwrap();
        Some(labelled_graph)
    }
}
