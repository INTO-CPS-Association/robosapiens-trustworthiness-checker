pub(crate) const ROS_SPIN_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);
pub(crate) const ROS_SPIN_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(1);

pub mod dist_graph_provider;
mod mstlo;
pub use dist_graph_provider::RosDistGraphProvider;
pub use mstlo::{
    MstloRosOutputHandler, duration_from_ros, duration_to_ros, mstlo_value_from_ros,
    mstlo_value_to_ros,
};
mod input_stream;
pub use input_stream::input_stream;
pub mod ros_topic_stream_mapping;
pub use ros_topic_stream_mapping::{RosMsgType, RosStreamMapping};
pub mod output_handler;
pub use output_handler::RosOutputHandler;
pub mod ros_scheduler_communicator;
pub use ros_scheduler_communicator::RosSchedulerCommunicator;

use std::collections::BTreeMap;
use std::rc::Rc;

use smol::LocalExecutor;

use crate::core::{InputStream, OutputHandler, RosStreamValue, Value, VarName};
use crate::runtime::mstlo::MstloTimedValue;

use ros_topic_stream_mapping::{VariableMappingData, string_to_ros_msg_type};

fn raw_mapping_to_ros(
    mapping: BTreeMap<String, (String, String)>,
) -> anyhow::Result<RosStreamMapping> {
    mapping
        .into_iter()
        .map(|(variable, (topic, message_type))| {
            Ok((
                variable,
                VariableMappingData {
                    topic,
                    msg_type: string_to_ros_msg_type(&message_type)?,
                },
            ))
        })
        .collect()
}

fn value_output_mapping(
    mapping: BTreeMap<String, (String, String)>,
) -> anyhow::Result<RosStreamMapping> {
    let mapping = raw_mapping_to_ros(mapping)?;
    if let Some((variable, _)) = mapping
        .iter()
        .find(|(_, data)| data.msg_type == RosMsgType::MstloTimedValue)
    {
        anyhow::bail!(
            "MstloTimedValue ROS output for `{variable}` requires a typed MSTLO output handler"
        );
    }
    Ok(mapping)
}

impl RosStreamValue for Value {
    fn ros_input_stream(
        executor: Rc<LocalExecutor<'static>>,
        mapping: BTreeMap<String, (String, String)>,
    ) -> anyhow::Result<InputStream<Self>> {
        input_stream(executor, raw_mapping_to_ros(mapping)?)
    }

    fn ros_output_handler(
        executor: Rc<LocalExecutor<'static>>,
        node_name: String,
        mapping: BTreeMap<String, (String, String)>,
        aux_info: Vec<VarName>,
    ) -> anyhow::Result<Box<dyn OutputHandler<Val = Self>>> {
        Ok(Box::new(RosOutputHandler::new(
            executor,
            node_name,
            value_output_mapping(mapping)?,
            aux_info,
        )?))
    }
}

impl RosStreamValue for MstloTimedValue {
    fn ros_input_stream(
        executor: Rc<LocalExecutor<'static>>,
        mapping: BTreeMap<String, (String, String)>,
    ) -> anyhow::Result<InputStream<Self>> {
        mstlo::input_stream(executor, mapping)
    }

    fn ros_output_handler(
        executor: Rc<LocalExecutor<'static>>,
        node_name: String,
        mapping: BTreeMap<String, (String, String)>,
        aux_info: Vec<VarName>,
    ) -> anyhow::Result<Box<dyn OutputHandler<Val = Self>>> {
        Ok(Box::new(mstlo::MstloRosOutputHandler::new(
            executor, node_name, mapping, aux_info,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_output_rejects_mstlo_messages_during_configuration() {
        let error = value_output_mapping(BTreeMap::from([(
            "out".to_owned(),
            ("/out".to_owned(), "MstloTimedValue".to_owned()),
        )]))
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "MstloTimedValue ROS output for `out` requires a typed MSTLO output handler"
        );
    }
}
