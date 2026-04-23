use std::{collections::BTreeMap, rc::Rc};

// There is a false warning about the import of the ensures macro
#[allow(unused_imports)]
use contracts::{ensures, requires};
#[warn(unused_imports)]
use tracing::info;

use crate::{
    Specification, VarName, distributed::distribution_graphs::LabelledDistributionGraph,
    semantics::distributed::localisation::Localisable,
};

use super::communication::SchedulerCommunicator;

// TODO: it is somewhat odd that spec is placed here; we need a Knowledge component in the MAPE-K
// loop
pub struct SchedulerExecutor<M: Specification + Localisable> {
    spec: M,
    var_msg_types: BTreeMap<VarName, String>,
    communicator: Box<dyn SchedulerCommunicator<M>>,
}

/// Translate the type annotations of a localised spec into ROS types for the work allocation
#[ensures(ret.as_ref().is_ok()
    -> {let io_vars: Vec<VarName> = spec
        .input_vars()
        .into_iter()
        .chain(spec.output_vars().into_iter())
        .collect();
        ret.as_ref().is_ok_and(move |work| work.type_info.keys().all(|v| io_vars.contains(v)))})]
fn monitor_work_for_specification<M: Specification + Localisable>(
    spec: M,
    var_msg_types: BTreeMap<VarName, String>,
) -> anyhow::Result<super::communication::MonitorWork<M>> {
    let io_vars: Vec<VarName> = spec
        .input_vars()
        .into_iter()
        .chain(spec.output_vars().into_iter())
        .collect();

    let type_info: BTreeMap<VarName, String> = var_msg_types
        .into_iter()
        .filter(|(var, _)| io_vars.contains(var))
        .collect();

    Ok(super::communication::MonitorWork {
        // Clone here due to use in contract
        spec: spec.clone(),
        type_info,
    })
}

#[requires(local_topics.iter().all(|v| spec.var_names().contains(v) && (spec.aux_vars().contains(v) || var_msg_types.contains_key(v))))]
#[ensures(ret.as_ref().is_ok() -> {
    ret.as_ref().is_ok_and(|work| work.spec.output_vars().iter().all(|v| local_topics.contains(v)
       && (spec.aux_vars().contains(v) || var_msg_types.contains_key(v))))})]
fn local_monitor_work<M: Specification + Localisable>(
    spec: M,
    var_msg_types: BTreeMap<VarName, String>,
    local_topics: Vec<VarName>,
) -> anyhow::Result<super::communication::MonitorWork<M>> {
    let local_spec = spec.localise(&local_topics);

    monitor_work_for_specification(local_spec, var_msg_types.clone())
}

impl<M: Specification + Localisable> SchedulerExecutor<M> {
    pub fn new(
        spec: M,
        var_msg_types: BTreeMap<VarName, String>,
        communicator: Box<dyn SchedulerCommunicator<M>>,
    ) -> Self {
        SchedulerExecutor {
            spec,
            var_msg_types,
            communicator,
        }
    }

    pub async fn execute(&mut self, dist_graph: Rc<LabelledDistributionGraph>) {
        let nodes = dist_graph.dist_graph.graph.node_indices();
        // is this really the best way?
        for node in nodes {
            let node_name = dist_graph.dist_graph.graph[node].clone();
            let local_topics = dist_graph.node_labels[&node].clone();
            let work = local_monitor_work(
                self.spec.clone(),
                self.var_msg_types.clone(),
                local_topics.clone(),
            )
            .expect("Failed to get local work assignment from specification");
            info!(
                "Scheduling work {:?} for node {} with local_topics {:?}",
                work, node_name, local_topics
            );
            self.communicator
                .schedule_work(node_name, work)
                .await
                .expect("Failed to schedule work");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::distribution_graphs::{DistributionGraph, LabelledDistributionGraph};
    use crate::distributed::scheduling::communication::{MockSchedulerCommunicator, WorkTypeInfo};
    use crate::lang::dsrv::parser::dsrv_specification;
    use crate::semantics::distributed::localisation::Localisable;
    use macro_rules_attribute::apply;
    use std::sync::{Arc, Mutex};

    #[test]
    fn monitor_work_keeps_only_localised_io_type_info() -> anyhow::Result<()> {
        let mut spec_src = "in x: Int\nin z: Int\nout y: Int\ny = (x + 1)";
        let spec = dsrv_specification(&mut spec_src).map_err(|e| anyhow::anyhow!(e))?;
        let local_spec = spec.localise(&vec!["y".into()]);

        let var_msg_types: WorkTypeInfo = BTreeMap::from([
            ("x".into(), "Int32".to_string()),
            ("y".into(), "Int32".to_string()),
            ("z".into(), "Int32".to_string()),
            ("unused".into(), "Int32".to_string()),
        ]);

        let work = monitor_work_for_specification(local_spec, var_msg_types)?;

        assert!(work.type_info.contains_key(&"x".into()));
        assert!(work.type_info.contains_key(&"y".into()));
        assert!(!work.type_info.contains_key(&"z".into()));
        assert!(!work.type_info.contains_key(&"unused".into()));
        Ok(())
    }

    #[apply(crate::async_test)]
    async fn scheduler_executor_sends_io_type_info_per_node(
        _executor: Rc<smol::LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let mut spec_src = "in x: Int\nin z: Int\nout y: Int\ny = (x + 1)";
        let spec = dsrv_specification(&mut spec_src).map_err(|e| anyhow::anyhow!(e))?;

        let var_msg_types: WorkTypeInfo = BTreeMap::from([
            ("x".into(), "Int32".to_string()),
            ("y".into(), "Int32".to_string()),
            ("z".into(), "Int32".to_string()),
            ("unused".into(), "Int32".to_string()),
        ]);

        let mut graph = petgraph::prelude::DiGraph::new();
        let node_a = graph.add_node("A".into());
        let dist_graph = DistributionGraph {
            central_monitor: node_a,
            graph,
        };
        let labelled = LabelledDistributionGraph {
            dist_graph: Rc::new(dist_graph),
            var_names: vec!["x".into(), "y".into(), "z".into()],
            node_labels: BTreeMap::from([(node_a, vec!["y".into()])]),
        };

        let communicator = Arc::new(Mutex::new(MockSchedulerCommunicator { log: vec![] }));
        let mut executor = SchedulerExecutor::new(
            spec,
            var_msg_types,
            Box::new(communicator.clone()) as Box<dyn SchedulerCommunicator<_>>,
        );

        executor.execute(Rc::new(labelled)).await;

        let lock = communicator.lock().unwrap();
        assert_eq!(lock.log.len(), 1);

        let (_node, work) = &lock.log[0];
        assert!(work.type_info.contains_key(&"x".into()));
        assert!(work.type_info.contains_key(&"y".into()));
        assert!(!work.type_info.contains_key(&"z".into()));
        assert!(!work.type_info.contains_key(&"unused".into()));
        Ok(())
    }

    #[test]
    fn local_monitor_work_fails_when_local_aux_type_info_is_missing() {
        let mut spec_src = "in x: Int\nout z: Int\naux u: Int\nu = x\nz = u + 1";
        let spec = dsrv_specification(&mut spec_src).unwrap();
        let local_spec = spec.localise(&vec!["z".into(), "u".into()]);

        let var_msg_types: WorkTypeInfo = BTreeMap::from([
            ("x".into(), "Int32".to_string()),
            ("z".into(), "Int32".to_string()),
            // Intentionally missing "u"
        ]);

        let _ = local_monitor_work(local_spec, var_msg_types, vec!["z".into(), "u".into()]);
    }
}
