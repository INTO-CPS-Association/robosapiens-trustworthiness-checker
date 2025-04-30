use async_trait::async_trait;

use crate::VarName;

use super::distribution_graphs::NodeName;

#[async_trait(?Send)]
pub trait SchedulerCommunicator {
    async fn schedule_work(
        &mut self,
        node: NodeName,
        work: Vec<VarName>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

static_assertions::assert_obj_safe!(SchedulerCommunicator);
