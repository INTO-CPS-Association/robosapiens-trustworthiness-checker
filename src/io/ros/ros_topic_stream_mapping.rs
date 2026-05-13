use std::collections::{BTreeMap, BTreeSet};

use anyhow::anyhow;
use contracts::requires;
use serde::{Deserialize, Serialize};

use crate::{
    VarName,
    io::config::{MsgTypeMapping, TopicMapping},
};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub enum ROSMsgType {
    Bool,
    String,
    Int64,
    Int32,
    Int32List,
    Int16,
    Int8,
    Float64,
    Float32,
    HumanModelPart,
    HumanModel,
    HumanModelList,
    RVData,
    RVDataArray,
    /// ROS2 `nav_msgs/msg/Odometry`
    Odom,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
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
        "Int32List" => Ok(ROSMsgType::Int32List),
        "Int16" => Ok(ROSMsgType::Int16),
        "Int8" => Ok(ROSMsgType::Int8),
        "Float64" => Ok(ROSMsgType::Float64),
        "Float32" => Ok(ROSMsgType::Float32),
        "HumanModelPart" => Ok(ROSMsgType::HumanModelPart),
        "HumanModel" => Ok(ROSMsgType::HumanModel),
        "HumanModelList" => Ok(ROSMsgType::HumanModelList),
        "RVData" => Ok(ROSMsgType::RVData),
        "RVDataArray" => Ok(ROSMsgType::RVDataArray),
        "Odom" => Ok(ROSMsgType::Odom),
        typ => Err(anyhow!("Unsupported type {}", typ)),
    }
}

pub fn ros_msg_type_to_string(typ: ROSMsgType) -> Result<String, anyhow::Error> {
    match typ {
        ROSMsgType::Bool => Ok("Bool".to_string()),
        ROSMsgType::String => Ok("String".to_string()),
        ROSMsgType::Int64 => Ok("Int64".to_string()),
        ROSMsgType::Int32 => Ok("Int32".to_string()),
        ROSMsgType::Int32List => Ok("Int32List".to_string()),
        ROSMsgType::Int16 => Ok("Int16".to_string()),
        ROSMsgType::Int8 => Ok("Int8".to_string()),
        ROSMsgType::Float64 => Ok("Float64".to_string()),
        ROSMsgType::Float32 => Ok("Float32".to_string()),
        ROSMsgType::HumanModelPart => Ok("HumanModelPart".to_string()),
        ROSMsgType::HumanModel => Ok("HumanModel".to_string()),
        ROSMsgType::HumanModelList => Ok("HumanModelList".to_string()),
        ROSMsgType::RVData => Ok("RVData".to_string()),
        ROSMsgType::RVDataArray => Ok("RVDataArray".to_string()),
        ROSMsgType::Odom => Ok("Odom".to_string()),
    }
}

pub fn ros_variable_map_to_string_variable_map(
    map: ROSStreamMapping,
) -> Result<BTreeMap<VarName, String>, anyhow::Error> {
    map.into_iter()
        .map(
            |(var_name_str, VariableMappingData { topic: _, msg_type })| {
                let var_name = var_name_str.try_into()?;
                let typ_str = ros_msg_type_to_string(msg_type)?;
                Ok((var_name, typ_str))
            },
        )
        .collect()
}

pub fn ros_stream_mapping_to_topic_mapping(
    map: ROSStreamMapping,
) -> Result<BTreeMap<VarName, String>, anyhow::Error> {
    map.into_iter()
        .map(
            |(var_name_str, VariableMappingData { topic, msg_type: _ })| {
                let var_name = var_name_str.try_into()?;
                Ok((var_name, topic))
            },
        )
        .collect()
}

#[requires(topic_map.keys().cloned().collect::<BTreeSet<_>>() == msg_type_map.keys().cloned().collect::<BTreeSet<_>>())]
#[ensures({
    let expected_keys = topic_map
        .keys()
        .map(|k| k.clone().into())
        .collect::<BTreeSet<String>>();
    ret.as_ref().is_ok_and(|x| x.keys().cloned().collect::<BTreeSet<_>>() == expected_keys)
})]
pub fn ros_stream_mapping_from_topic_and_msg_type_mapping(
    topic_map: TopicMapping,
    msg_type_map: MsgTypeMapping,
) -> Result<ROSStreamMapping, anyhow::Error> {
    let mut ros_stream_mapping: ROSStreamMapping = BTreeMap::new();

    for (var_name, topic) in topic_map.iter() {
        let topic = topic.clone();
        let msg_type = msg_type_map[&var_name].clone();
        let msg_type = string_to_ros_msg_type(msg_type.as_str())?;
        let var_name: String = var_name.clone().into();

        ros_stream_mapping.insert(var_name, VariableMappingData { topic, msg_type });
    }

    Ok(ros_stream_mapping)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{
        VarName,
        io::{
            config::{MsgTypeMapping, TopicMapping},
            ros::ros_topic_stream_mapping::{
                ROSMsgType, ros_stream_mapping_from_topic_and_msg_type_mapping,
            },
        },
    };
    use test_log::test;

    #[test]
    fn test_ros_stream_mapping_from_topic_and_msg_type_mapping() -> Result<(), anyhow::Error> {
        let topic_map: TopicMapping = BTreeMap::from([
            (VarName::new("x"), "/r1/x".to_string()),
            (VarName::new("y"), "/r2/y".to_string()),
        ]);
        let msg_type_map: MsgTypeMapping = BTreeMap::from([
            (VarName::new("x"), "Int32".to_string()),
            (VarName::new("y"), "String".to_string()),
        ]);

        let mapping = ros_stream_mapping_from_topic_and_msg_type_mapping(topic_map, msg_type_map)?;

        assert_eq!(mapping.len(), 2);
        assert_eq!(mapping["x"].topic, "/r1/x");
        assert_eq!(mapping["x"].msg_type, ROSMsgType::Int32);
        assert_eq!(mapping["y"].topic, "/r2/y");
        assert_eq!(mapping["y"].msg_type, ROSMsgType::String);
        Ok(())
    }

    #[test]
    fn test_ros_stream_mapping_from_topic_and_msg_type_mapping_rejects_unknown_type() {
        let topic_map: TopicMapping = BTreeMap::from([(VarName::new("x"), "/topic/x".to_string())]);
        let msg_type_map: MsgTypeMapping =
            BTreeMap::from([(VarName::new("x"), "NotARosType".to_string())]);

        let result = ros_stream_mapping_from_topic_and_msg_type_mapping(topic_map, msg_type_map);

        assert!(result.is_err());
    }

    #[test]
    fn test_ros_stream_mapping_from_topic_and_msg_type_mapping_empty_maps()
    -> Result<(), anyhow::Error> {
        let topic_map: TopicMapping = BTreeMap::new();
        let msg_type_map: MsgTypeMapping = BTreeMap::new();

        let mapping = ros_stream_mapping_from_topic_and_msg_type_mapping(topic_map, msg_type_map)?;

        assert!(mapping.is_empty());
        Ok(())
    }
}
