use std::collections::BTreeMap;

use anyhow::anyhow;
use serde_json::Value as JValue;
use tracing::debug;

use crate::io::config::{MsgTypeMapping, TopicMapping};

// Note: These functions are manually implemented instead of using `serde_json::from_str` because
// mhk dreams of one day supporting the custom ROS types automatically...

// Handle a combined JSON string with both topic names and msg types
pub fn json_to_topic_msg_type_mapping(
    json: &str,
) -> anyhow::Result<(TopicMapping, MsgTypeMapping)> {
    let jval = match serde_json5::from_str::<JValue>(&json) {
        Ok(value) => value,
        Err(e) => {
            return Err(e.into());
        }
    };
    if let Some(jval) = jval.as_object() {
        debug!("JSON Mapping raw: {:?}", jval);
        let mut topic_mapping_map = BTreeMap::new();
        let mut msg_type_map = BTreeMap::new();
        for (var_name, data) in jval.iter() {
            let topic_opt = data.get("topic");
            let typ_opt = data.get("msg_type");

            match (topic_opt, typ_opt) {
                (Some(topic_v), Some(typ_v)) => match (topic_v.as_str(), typ_v.as_str()) {
                    (Some(topic), Some(typ)) => {
                        topic_mapping_map.insert(var_name.clone().into(), topic.to_string());
                        msg_type_map.insert(var_name.clone().into(), typ.to_string());
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
        Ok((topic_mapping_map, msg_type_map))
    } else {
        return Err(anyhow!("Must be specified as a JSON object"));
    }
}

// Handle a combined JSON string with just topic names
pub fn json_to_topic_mapping(json: &str) -> anyhow::Result<TopicMapping> {
    let jval = match serde_json5::from_str::<JValue>(&json) {
        Ok(value) => value,
        Err(e) => {
            return Err(e.into());
        }
    };
    if let Some(jval) = jval.as_object() {
        debug!("JSON Mapping raw: {:?}", jval);
        let mut topic_mapping_map = BTreeMap::new();
        for (var_name, data) in jval.iter() {
            match data.get("topic") {
                Some(topic_v) => match topic_v.as_str() {
                    Some(topic) => {
                        topic_mapping_map.insert(var_name.clone().into(), topic.to_string());
                    }
                    _ => {
                        return Err(anyhow!(
                            "topic must be a string for variable '{}'",
                            var_name
                        ));
                    }
                },
                _ => {
                    return Err(anyhow!("Missing topic for variable '{}'", var_name));
                }
            }
        }
        Ok(topic_mapping_map)
    } else {
        return Err(anyhow!("Must be specified as a JSON object"));
    }
}
