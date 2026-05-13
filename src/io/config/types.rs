use std::collections::BTreeMap;

use crate::VarName;

// A mapping from variable names to topic names
pub type TopicMapping = BTreeMap<VarName, String>;
// A mapping from variable names to topic type strings
pub type MsgTypeMapping = BTreeMap<VarName, String>;
