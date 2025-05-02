use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tracing::debug;

use crate::{VarName, distributed::distribution_graphs::NodeName};

#[async_trait(?Send)]
pub trait SchedulerCommunicator {
    async fn schedule_work(
        &mut self,
        node: NodeName,
        work: Vec<VarName>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

static_assertions::assert_obj_safe!(SchedulerCommunicator);

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

struct MockSchedulerCommunicator {
    pub log: Vec<(NodeName, Vec<VarName>)>,
}

#[async_trait(?Send)]
impl SchedulerCommunicator for MockSchedulerCommunicator {
    async fn schedule_work(
        &mut self,
        node: NodeName,
        work: Vec<VarName>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.log.push((node, work));
        Ok(())
    }
}

#[async_trait(?Send)]
impl SchedulerCommunicator for Arc<Mutex<MockSchedulerCommunicator>> {
    async fn schedule_work(
        &mut self,
        node: NodeName,
        work: Vec<VarName>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Clone the data and drop the MutexGuard before awaiting
        let mock = {
            let mut lock = self.lock().unwrap();
            lock.log.push((node, work));
            Ok(())
        };
        mock
    }
}
