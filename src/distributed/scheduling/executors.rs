use std::{collections::BTreeMap, rc::Rc};

// There is a false warning about the import of the ensures macro
#[allow(unused_imports)]
use contracts::{ensures, requires};
#[warn(unused_imports)]
use tracing::info;

use crate::{
    Specification, VarName, core::StreamType,
    distributed::distribution_graphs::LabelledDistributionGraph,
    semantics::distributed::localisation::Localisable,
};

use super::communication::SchedulerCommunicator;

// TODO: it is somewhat odd that spec is placed here; we need a Knowledge component in the MAPE-K
// loop
pub struct SchedulerExecutor<M: Specification + Localisable> {
    spec: M,
    communicator: Box<dyn SchedulerCommunicator<M>>,
}

/// Translate the type annotations of a localised spec into ROS types for the work allocation
#[ensures(ret.as_ref().is_ok()
    -> {let output_vars = spec.output_vars(); ret.as_ref().is_ok_and(move |work| work.type_info.keys().all(|v| output_vars.contains(v)))})]
fn monitor_work_for_specification<M: Specification + Localisable>(
    spec: M,
) -> anyhow::Result<super::communication::MonitorWork<M>> {
    let output_vars = spec.output_vars();
    let type_info: BTreeMap<VarName, String> = spec
        .type_annotations()
        .into_iter()
        .filter(|(var, _)| output_vars.contains(var))
        .map(|(var, typ): (VarName, StreamType)| {
            (
                var.clone(),
                match typ {
                    StreamType::Int => String::from("Int32"),
                    StreamType::Float => String::from("Float32"),
                    StreamType::Str => String::from("String"),
                    StreamType::Bool => String::from("Bool"),
                    StreamType::Unit => todo!("Implement support for unit type"),
                },
            )
        })
        .collect();
    Ok(super::communication::MonitorWork {
        // Clone here due to use in contract
        spec: spec.clone(),
        type_info,
    })
}

#[requires(local_topics.iter().all(|v| spec.var_names().contains(v)))]
#[ensures(ret.as_ref().is_ok() -> {
    ret.as_ref().is_ok_and(|work| work.spec.output_vars().iter().all(|v| local_topics.contains(v)))})]
fn local_monitor_work<M: Specification + Localisable>(
    spec: M,
    local_topics: Vec<VarName>,
) -> anyhow::Result<super::communication::MonitorWork<M>> {
    let local_spec = spec.localise(&local_topics);

    monitor_work_for_specification(local_spec)
}

impl<M: Specification + Localisable> SchedulerExecutor<M> {
    pub fn new(spec: M, communicator: Box<dyn SchedulerCommunicator<M>>) -> Self {
        SchedulerExecutor { spec, communicator }
    }

    pub async fn execute(&mut self, dist_graph: Rc<LabelledDistributionGraph>) {
        let nodes = dist_graph.dist_graph.graph.node_indices();
        // is this really the best way?
        for node in nodes {
            let node_name = dist_graph.dist_graph.graph[node].clone();
            let local_topics = dist_graph.node_labels[&node].clone();
            let work = local_monitor_work(self.spec.clone(), local_topics)
                .expect("Failed to get local work assignment from specification");
            info!("Scheduling work {:?} for node {}", work, node_name);
            self.communicator
                .schedule_work(node_name, work)
                .await
                .expect("Failed to schedule work");
        }
    }
}
