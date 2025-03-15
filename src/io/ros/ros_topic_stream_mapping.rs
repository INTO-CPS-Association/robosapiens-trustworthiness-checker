use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum ROSMsgType {
    Bool,
    String,
    Int32,
    Int64,
    Int8,
    Int16,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct VariableMappingData {
    pub topic: String,
    pub msg_type: ROSMsgType,
}

pub type ROSStreamMapping = BTreeMap<String, VariableMappingData>;

pub fn json_to_mapping(json: &str) -> Result<ROSStreamMapping, serde_json::Error> {
    serde_json::from_str(json)
}

#[cfg(test)]
mod tests {
    use crate::io::ros::ros_topic_stream_mapping::{ROSMsgType, ROSStreamMapping, json_to_mapping};
    use test_log::test;

    #[test]
    fn test_json_to_mapping() -> Result<(), serde_json::Error> {
        let json = r#"
        {
            "x": {
                "topic": "/x",
                "msg_type": "Int32"
            },
            "y": {
                "topic": "/y",
                "msg_type": "String"
            }
        }
        "#;

        let mapping: ROSStreamMapping = json_to_mapping(json)?;
        assert_eq!(mapping.len(), 2);
        assert_eq!(mapping["x"].topic, "/x");
        assert_eq!(mapping["x"].msg_type, ROSMsgType::Int32);
        assert_eq!(mapping["y"].topic, "/y");
        assert_eq!(mapping["y"].msg_type, ROSMsgType::String);
        Ok(())
    }
}
