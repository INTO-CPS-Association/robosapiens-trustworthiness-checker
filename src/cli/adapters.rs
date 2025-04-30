use crate::io::builders::InputProviderSpec;

use super::args::InputMode;

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
