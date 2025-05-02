use std::rc::Rc;

use tracing::info;

use crate::distributed::distribution_graphs::LabelledDistributionGraph;

use super::communication::SchedulerCommunicator;

pub struct SchedulerExecutor {
    communicator: Box<dyn SchedulerCommunicator>,
}

impl SchedulerExecutor {
    pub fn new(communicator: Box<dyn SchedulerCommunicator>) -> Self {
        SchedulerExecutor { communicator }
    }
    pub async fn execute(&mut self, dist_graph: Rc<LabelledDistributionGraph>) {
        let nodes = dist_graph.dist_graph.graph.node_indices();
        // is this really the best way?
        for node in nodes {
            let node_name = dist_graph.dist_graph.graph[node].clone();
            let work = dist_graph.node_labels[&node].clone();
            info!("Scheduling work {:?} for node {}", work, node_name);
            let _ = self.communicator.schedule_work(node_name, work).await;
        }
    }
}
