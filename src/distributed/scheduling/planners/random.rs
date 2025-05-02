use std::{collections::BTreeMap, rc::Rc};

use async_trait::async_trait;
use petgraph::graph::NodeIndex;
use tracing::info;

use crate::{
    VarName,
    distributed::distribution_graphs::{
        DistributionGraph, LabelledDistributionGraph, graph_to_png,
    },
};

use super::core::SchedulerPlanner;

pub struct RandomSchedulerPlanner {
    pub var_names: Vec<VarName>,
}

#[async_trait(?Send)]
impl SchedulerPlanner for RandomSchedulerPlanner {
    async fn plan(&self, graph: Rc<DistributionGraph>) -> Option<Rc<LabelledDistributionGraph>> {
        info!("Received distribution graph: generating random labelling");
        let node_indicies = graph.graph.node_indices().collect::<Vec<_>>();
        let location_map: BTreeMap<VarName, NodeIndex> = self
            .var_names
            .iter()
            .map(|var| {
                let location_index: usize = rand::random_range(0..node_indicies.len());
                assert!(location_index < node_indicies.len());
                (var.clone(), node_indicies[location_index].clone())
            })
            .collect();

        let node_labels: BTreeMap<NodeIndex, Vec<VarName>> = graph
            .graph
            .node_indices()
            .map(|idx| {
                (
                    idx,
                    self.var_names
                        .iter()
                        .filter(|var| location_map[var] == idx)
                        .cloned()
                        .collect(),
                )
            })
            .collect();
        info!("Generated random labelling: {:?}", node_labels);

        let labelled_graph = Rc::new(LabelledDistributionGraph {
            dist_graph: graph,
            var_names: self.var_names.clone(),
            node_labels,
        });
        info!("Labelled graph: {:?}", labelled_graph);
        graph_to_png(labelled_graph.clone(), "distributed_graph.png")
            .await
            .unwrap();

        Some(labelled_graph)
    }
}
