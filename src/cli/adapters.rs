use anyhow::Context;
use tracing::{debug, info};

use crate::distributed::distribution_graphs::NodeName;
use crate::distributed::locality_receiver::LocalityReceiver;
use crate::semantics::distributed::localisation::LocalitySpec;
use crate::{
    VarName, core::MQTT_HOSTNAME, distributed::distribution_graphs::LabelledDistributionGraph,
    io::builders::InputProviderSpec, runtime::distributed::SchedulerCommunication,
};

use super::args::DistributionMode as CliDistributionMode;
use super::args::{DistributionMode, InputMode, SchedulingType};
use crate::runtime::builder::DistributionMode as BuilderDistributionMode;

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
                input_redis_topics: Some(input_redis_topics),
                ..
            } => InputProviderSpec::Redis(Some(input_redis_topics)),
            InputMode {
                input_map_mqtt_topics: Some(input_map_mqtt_topics),
                ..
            } => InputProviderSpec::MQTTMap(Some(input_map_mqtt_topics)),
            InputMode {
                mqtt_input: true, ..
            } => InputProviderSpec::MQTT(None),
            InputMode {
                redis_input: true, ..
            } => InputProviderSpec::Redis(None),
            _ => panic!("Invalid input provider specification"),
        }
    }
}

impl From<SchedulingType> for SchedulerCommunication {
    fn from(scheduling_type: SchedulingType) -> Self {
        match scheduling_type {
            SchedulingType::Mock => SchedulerCommunication::Null,
            SchedulingType::Mqtt => SchedulerCommunication::MQTT,
        }
    }
}

#[derive(Clone)]
pub struct DistributionModeBuilder {
    distribution_mode: CliDistributionMode,
    local_node: Option<NodeName>,
    dist_constraints: Option<Vec<VarName>>,
}

impl DistributionModeBuilder {
    pub fn new(distribution_mode: CliDistributionMode) -> DistributionModeBuilder {
        DistributionModeBuilder {
            distribution_mode,
            local_node: None,
            dist_constraints: None,
        }
    }

    pub fn local_node(mut self, local_node: String) -> Self {
        self.local_node = Some(local_node.into());
        self
    }

    pub fn maybe_local_node(mut self, local_node: Option<String>) -> Self {
        self.local_node = local_node.map(|x| x.into());
        self
    }

    pub fn dist_constraints(mut self, constraints: Vec<String>) -> Self {
        self.dist_constraints = Some(constraints.into_iter().map(|x| x.into()).collect());
        self
    }

    pub fn maybe_dist_constraints(mut self, constraints: Option<Vec<String>>) -> Self {
        self.dist_constraints = constraints.map(|x| x.into_iter().map(|x| x.into()).collect());
        self
    }

    pub async fn build(self) -> anyhow::Result<BuilderDistributionMode> {
        Ok(match self.distribution_mode {
            DistributionMode {
                distribution_graph: Some(file_path),
                ..
            } => {
                debug!("centralised mode");
                let local_node = self.local_node.context("Local node not specified")?;
                let f = smol::fs::read_to_string(&file_path)
                    .await
                    .context("Distribution graph file could not be read")?;
                let distribution_graph: LabelledDistributionGraph =
                    serde_json::from_str(&f).context("Distribution graph could not be parsed")?;

                BuilderDistributionMode::LocalMonitor(Box::new((local_node, distribution_graph)))
            }
            DistributionMode {
                local_topics: Some(topics),
                ..
            } => BuilderDistributionMode::LocalMonitor(Box::new(
                topics
                    .into_iter()
                    .map(|v| v.into())
                    .collect::<Vec<VarName>>(),
            )),
            DistributionMode {
                distributed_work: true,
                ..
            } => {
                let local_node = self.local_node.context("Local node not specified")?;
                info!("Waiting for work assignment on node {}", local_node);
                let receiver = crate::io::mqtt::MQTTLocalityReceiver::new(
                    MQTT_HOSTNAME.to_string(),
                    local_node.into(),
                );
                let locality = receiver
                    .receive()
                    .await
                    .context("Work could not be received")?;
                info!("Received work: {:?}", locality.local_vars());
                BuilderDistributionMode::LocalMonitor(Box::new(locality))
            }
            DistributionMode {
                mqtt_centralised_distributed: Some(locations),
                ..
            } => {
                debug!("setting up distributed centralised mode");
                BuilderDistributionMode::DistributedCentralised(locations)
            }
            DistributionMode {
                mqtt_randomized_distributed: Some(locations),
                ..
            } => {
                debug!("setting up distributed random mode");
                BuilderDistributionMode::DistributedRandom(locations)
            }
            DistributionMode {
                mqtt_static_optimized: Some(locations),
                ..
            } => {
                info!("setting up static optimization mode");
                let dist_constraints = self
                    .dist_constraints
                    .context("Distribution constraints must be provided")?
                    .into_iter()
                    .map(|x| x.into())
                    .collect();
                BuilderDistributionMode::DistributedOptimizedStatic(locations, dist_constraints)
            }
            DistributionMode {
                mqtt_dynamic_optimized: Some(locations),
                ..
            } => {
                info!("setting up static optimization mode");
                let dist_constraints = self
                    .dist_constraints
                    .context("Distribution constraints must be provided")?
                    .into_iter()
                    .map(|x| x.into())
                    .collect();
                BuilderDistributionMode::DistributedOptimizedDynamic(locations, dist_constraints)
            }
            DistributionMode {
                centralised: true, ..
            } => BuilderDistributionMode::CentralMonitor,
            _ => unreachable!(),
        })
    }
}
