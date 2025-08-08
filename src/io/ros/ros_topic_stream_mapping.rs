use std::collections::BTreeMap;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_json::Value as JValue;
use tracing::debug;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum ROSMsgType {
    Bool,
    String,
    Int64,
    Int32,
    Int16,
    Int8,
    Float64,
    Float32,
    Human,
    HumanList,
    HumanBodyPart,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct VariableMappingData {
    pub topic: String,
    pub msg_type: ROSMsgType,
}

pub type ROSStreamMapping = BTreeMap<String, VariableMappingData>;

pub fn string_to_ros_msg_type(typ: &str) -> Result<ROSMsgType, anyhow::Error> {
    match typ {
        "Bool" => Ok(ROSMsgType::Bool),
        "String" => Ok(ROSMsgType::String),
        "Int64" => Ok(ROSMsgType::Int64),
        "Int32" => Ok(ROSMsgType::Int32),
        "Int16" => Ok(ROSMsgType::Int16),
        "Int8" => Ok(ROSMsgType::Int8),
        "Float64" => Ok(ROSMsgType::Float64),
        "Float32" => Ok(ROSMsgType::Float32),
        "Human" => Ok(ROSMsgType::Human),
        "HumanList" => Ok(ROSMsgType::HumanList),
        "HumanBodyPart" => Ok(ROSMsgType::HumanBodyPart),
        typ => Err(anyhow!("Unsupported type {}", typ)),
    }
}

pub fn json_to_mapping(json: &str) -> Result<ROSStreamMapping, anyhow::Error> {
    // Note: This was rewritten instead of using `serde_json::from_str` because
    // mhk dreams of one day supporting the custom ROS types automatically...
    let jval = match serde_json5::from_str::<JValue>(&json) {
        Ok(value) => value,
        Err(e) => {
            return Err(e.into());
        }
    };
    if let Some(jval) = jval.as_object() {
        debug!("JSON Mapping raw: {:?}", jval);
        let mut map = BTreeMap::new();
        for (var_name, data) in jval.iter() {
            let topic_opt = data.get("topic");
            let typ_opt = data.get("msg_type");

            match (topic_opt, typ_opt) {
                (Some(topic_v), Some(typ_v)) => match (topic_v.as_str(), typ_v.as_str()) {
                    (Some(topic), Some(typ)) => {
                        let ros_typ = string_to_ros_msg_type(typ)?;
                        map.insert(
                            var_name.clone(),
                            VariableMappingData {
                                topic: topic.to_string(),
                                msg_type: ros_typ,
                            },
                        );
                    }
                    _ => {
                        return Err(anyhow!(
                            "topic and msg_type must be strings for variable '{}'",
                            var_name
                        ));
                    }
                },
                _ => {
                    return Err(anyhow!(
                        "Missing topic or msg_type for variable '{}'",
                        var_name
                    ));
                }
            }
        }
        Ok(map)
    } else {
        return Err(anyhow!("Must be specified as a JSON object"));
    }
}

#[cfg(test)]
mod tests {
    use crate::io::ros::ros_topic_stream_mapping::{ROSMsgType, ROSStreamMapping, json_to_mapping};
    use test_log::test;

    #[test]
    fn test_json_to_mapping() -> Result<(), anyhow::Error> {
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
