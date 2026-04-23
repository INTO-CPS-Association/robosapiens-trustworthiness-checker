use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::Specification;
use async_trait::async_trait;
use serde::Serialize;
use serde::ser::{SerializeStruct, Serializer};
use tracing::debug;

use crate::{VarName, distributed::distribution_graphs::NodeName};

/// Additional type information about each variable in the monitor (work) being sent in the form
/// of a String which is interpreted by the receiving runtime. This is currently always the name
/// of a ROS message type, but my be interpreted differently by future communicators.
pub type WorkTypeInfo = BTreeMap<VarName, String>;

#[derive(Clone, Debug)]
pub struct MonitorWork<M: Specification> {
    pub spec: M,
    pub type_info: WorkTypeInfo,
}

impl<M: Specification> Serialize for MonitorWork<M> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("MonitorWork", 2)?;
        let spec_str = format!("{}", self.spec);
        state.serialize_field("spec", &spec_str)?;
        state.serialize_field("type_info", &self.type_info)?;
        state.end()
    }
}

#[async_trait(?Send)]
pub trait SchedulerCommunicator<M: Specification> {
    async fn schedule_work(
        &mut self,
        node: NodeName,
        work: MonitorWork<M>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct NullSchedulerCommunicator;

#[async_trait(?Send)]
impl<M: Specification> SchedulerCommunicator<M> for NullSchedulerCommunicator {
    async fn schedule_work(
        &mut self,
        node_name: NodeName,
        work: MonitorWork<M>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!("NullSchedulerCommunicator called for {node_name} with work {work:?}");
        Ok(())
    }
}

pub struct MockSchedulerCommunicator<M: Specification> {
    pub log: Vec<(NodeName, MonitorWork<M>)>,
}

#[async_trait(?Send)]
impl<M: Specification> SchedulerCommunicator<M> for MockSchedulerCommunicator<M> {
    async fn schedule_work(
        &mut self,
        node: NodeName,
        work: MonitorWork<M>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.log.push((node, work));
        Ok(())
    }
}

#[async_trait(?Send)]
impl<M: Specification> SchedulerCommunicator<M> for Arc<Mutex<MockSchedulerCommunicator<M>>> {
    async fn schedule_work(
        &mut self,
        node: NodeName,
        work: MonitorWork<M>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Clone the data and drop the MutexGuard before awaiting
        let mock = {
            let mut lock = self.lock().unwrap();
            lock.log.push((node, work));
            Ok(())
        };
        mock
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lang::dsrv::parser::dsrv_specification;
    use std::collections::BTreeMap;

    #[test]
    fn test_serialize_simple_add_montor_work() -> anyhow::Result<()> {
        let mut spec_src = "in x: Int\nout y: Int\ny = (x + 1)";
        let spec = dsrv_specification(&mut spec_src).map_err(|e| anyhow::anyhow!(e))?;

        let type_info: WorkTypeInfo = BTreeMap::from([
            (VarName::from("x"), String::from("Int32")),
            (VarName::from("y"), String::from("Int32")),
        ]);

        let work = MonitorWork {
            spec: spec,
            type_info,
        };
        let serialized = serde_json::to_value(&work)?;

        let expected = serde_json::json!({
            "spec": "in x: Int\nout y: Int\ny = (x + 1)\n",
            "type_info": {
                "x": "Int32",
                "y": "Int32"
            }
        });

        assert_eq!(serialized, expected);
        Ok(())
    }

    #[test]
    fn test_serialize_typed_string_expression_monitor_work() -> anyhow::Result<()> {
        let mut spec_src = "in x: Str\nin y: Str\nout z: Str\nz = x ++ \"_suffix\"";
        let spec = dsrv_specification(&mut spec_src).map_err(|e| anyhow::anyhow!(e))?;

        let type_info: WorkTypeInfo = BTreeMap::from([
            (VarName::from("x"), String::from("String")),
            (VarName::from("y"), String::from("String")),
            (VarName::from("z"), String::from("String")),
        ]);

        let work = MonitorWork { spec, type_info };
        let serialized = serde_json::to_value(&work)?;

        let expected = serde_json::json!({
            "spec": "in x: Str\nin y: Str\nout z: Str\nz = (x ++ \"_suffix\")\n",
            "type_info": {
                "x": "String",
                "y": "String",
                "z": "String"
            }
        });

        assert_eq!(serialized, expected);
        Ok(())
    }

    #[test]
    fn test_serialize_map_monitor_work() -> anyhow::Result<()> {
        let mut spec_src = "in records\nout z\nz = Map.get(List.get(records, 0), \"target\")";
        let spec = dsrv_specification(&mut spec_src).map_err(|e| anyhow::anyhow!(e))?;

        let type_info: WorkTypeInfo = BTreeMap::from([
            (
                VarName::from("records"),
                String::from("Vec<Map<String, i32>>"),
            ),
            (VarName::from("z"), String::from("Int32")),
        ]);

        let work = MonitorWork { spec, type_info };
        let serialized = serde_json::to_value(&work)?;

        let expected = serde_json::json!({
            "spec": "in records\nout z\nz = Map.get(List.get(records, 0), \"target\")\n",
            "type_info": {
                "records": "Vec<Map<String, i32>>",
                "z": "Int32"
            }
        });

        assert_eq!(serialized, expected);
        Ok(())
    }
}
