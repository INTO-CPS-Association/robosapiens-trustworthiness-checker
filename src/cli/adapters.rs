use crate::{io::builders::InputProviderSpec, runtime::distributed::SchedulerCommunication};

use super::args::{InputMode, SchedulingType};

impl From<InputMode> for InputProviderSpec {
    fn from(input_mode: InputMode) -> Self {
        match input_mode {
            InputMode {
                input_file: Some(input_file),
                ..
            } => InputProviderSpec::File(input_file),
            InputMode {
                input_ros_topics: Some(input_ros_topics),
                ..
            } => InputProviderSpec::Ros(input_ros_topics),
            InputMode {
                input_mqtt_topics: Some(input_mqtt_topics),
                ..
            } => InputProviderSpec::MQTT(Some(input_mqtt_topics)),
            InputMode {
                input_map_mqtt_topics: Some(input_map_mqtt_topics),
                ..
            } => InputProviderSpec::MQTTMap(Some(input_map_mqtt_topics)),
            InputMode {
                mqtt_input: true, ..
            } => InputProviderSpec::MQTT(None),
            _ => panic!("Input provider not specified"),
        }
    }
}

impl From<SchedulingType> for SchedulerCommunication {
    fn from(scheduling_type: SchedulingType) -> Self {
        match scheduling_type {
            SchedulingType::Mock => SchedulerCommunication::Null,
            SchedulingType::MQTT => SchedulerCommunication::MQTT,
        }
    }
}
