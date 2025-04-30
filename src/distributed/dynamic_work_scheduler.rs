use std::rc::Rc;

use crate::OutputStream;
use crate::VarName;
use async_trait::async_trait;
use smol::LocalExecutor;
use smol::stream::StreamExt;
use tracing::debug;
use tracing::info;

use super::distribution_graphs::{LabelledDistributionGraph, NodeName};
use super::scheduling::SchedulerCommunicator;

pub struct NullSchedulerCommunicator;

#[async_trait(?Send)]
impl SchedulerCommunicator for NullSchedulerCommunicator {
    async fn schedule_work(
        &mut self,
        _node_name: NodeName,
        _work: Vec<VarName>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!("NullSchedulerCommunicator called");
        Ok(())
    }
}

pub struct Scheduler {
    executor: Rc<LocalExecutor<'static>>,
    dist_graph_stream: Option<OutputStream<LabelledDistributionGraph>>,
    communicator: Box<dyn SchedulerCommunicator>,
}

impl Scheduler {
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        communicator: Box<dyn SchedulerCommunicator>,
    ) -> Self {
        Scheduler {
            executor,
            dist_graph_stream: None,
            communicator,
        }
    }

    pub fn dist_graph_stream(
        &mut self,
        stream: OutputStream<LabelledDistributionGraph>,
    ) -> &mut Self {
        self.dist_graph_stream = Some(stream);
        self
    }

    pub fn run(self) {
        if let Some(mut stream) = self.dist_graph_stream {
            self.executor
                .spawn(async move {
                    let mut communicator = self.communicator;
                    while let Some(dist_graph) = stream.next().await {
                        let nodes = dist_graph.dist_graph.graph.node_indices();
                        // is this really the best way?
                        for node in nodes {
                            let node_name = dist_graph.dist_graph.graph[node].clone();
                            let work = dist_graph.node_labels[&node].clone();
                            info!("Scheduling work {:?} for node {}", work, node_name);
                            let _ = communicator.schedule_work(node_name, work).await;
                        }
                    }
                })
                .detach();
        };
    }
}
