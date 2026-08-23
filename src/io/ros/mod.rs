pub(crate) const ROS_SPIN_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);
pub(crate) const ROS_SPIN_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(1);

pub mod dist_graph_provider;
mod mstlo;
pub use dist_graph_provider::RosDistGraphProvider;
pub use mstlo::{duration_from_ros, duration_to_ros, mstlo_value_from_ros, mstlo_value_to_ros};
mod input_stream;
pub(crate) use input_stream::control_stream;
pub use input_stream::input_stream;
pub mod ros_topic_stream_mapping;
pub use ros_topic_stream_mapping::{RosMsgType, RosStreamMapping};
mod value_publisher;
pub(crate) use value_publisher::{ValuePublisher, create_value_publisher};

pub mod ros_scheduler_communicator;
pub use ros_scheduler_communicator::RosSchedulerCommunicator;

use std::collections::BTreeMap;
use std::rc::Rc;

use smol::LocalExecutor;

use crate::core::{InputStream, RosStreamValue, SharedOutputBackend, Value};
use crate::io::output::{RosOutputBackend, create_value_ros_publisher, validate_value_interface};
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

impl RosStreamValue for Value {
    fn ros_input_stream(
        executor: Rc<LocalExecutor<'static>>,
        mapping: BTreeMap<String, (String, String)>,
    ) -> anyhow::Result<InputStream<Self>> {
        input_stream(executor, raw_mapping_to_ros(mapping)?)
    }

    fn ros_output_backend(
        executor: Rc<LocalExecutor<'static>>,
        node_name: String,
    ) -> anyhow::Result<SharedOutputBackend<Self>> {
        Ok(Rc::new(RosOutputBackend::<Self>::new(
            executor,
            node_name,
            validate_value_interface,
            create_value_ros_publisher,
        )))
    }
}

impl RosStreamValue for MstloTimedValue {
    fn ros_input_stream(
        executor: Rc<LocalExecutor<'static>>,
        mapping: BTreeMap<String, (String, String)>,
    ) -> anyhow::Result<InputStream<Self>> {
        mstlo::input_stream(executor, mapping)
    }

    fn ros_output_backend(
        executor: Rc<LocalExecutor<'static>>,
        node_name: String,
    ) -> anyhow::Result<SharedOutputBackend<Self>> {
        Ok(Rc::new(RosOutputBackend::<Self>::new(
            executor,
            node_name,
            mstlo::validate_output_interface,
            mstlo::create_mstlo_output_publisher,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{OutputInterface, OutputRole, OutputRoute, VarName};

    #[test]
    fn dynamic_output_rejects_mstlo_messages_during_configuration() {
        let interface = OutputInterface::from_routes([OutputRoute::new(
            VarName::new("out"),
            Some("/out".to_owned()),
            Some("MstloTimedValue".to_owned()),
            OutputRole::Output,
        )])
        .expect("single-route interface should be valid");

        let error = validate_value_interface(&interface).unwrap_err();

        assert!(error.is_invalid());
        assert!(error.to_string().contains("ROS output route `out`"));
        assert!(
            error
                .to_string()
                .contains("MstloTimedValue requires a typed MSTLO ROS output backend")
        );
    }
}
