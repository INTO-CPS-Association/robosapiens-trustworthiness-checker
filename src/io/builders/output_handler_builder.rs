use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use smol::LocalExecutor;

use crate::core::{MQTT_HOSTNAME, REDIS_HOSTNAME};
use crate::io::cli::StdoutOutputHandler;
use crate::io::mqtt::{MQTTOutputHandler, MqttFactory};
use crate::io::redis::RedisOutputHandler;
use crate::{Value, VarName, core::OutputHandler};

#[derive(Debug, Clone)]
pub enum OutputHandlerSpec {
    /// File input provider
    Stdout,
    /// ROS topics input provider
    Ros(
        /// JSON string with topics and types
        String,
    ),
    /// MQTT topics output provider
    MQTT(
        /// Topics
        Option<Vec<String>>,
    ),
    /// Redis topics output provider
    Redis(
        /// Topics
        Option<Vec<String>>,
    ),
    Manual,
}

#[derive(Debug, Clone)]
pub struct OutputHandlerBuilder {
    executor: Option<Rc<LocalExecutor<'static>>>,
    output_var_names: Option<Vec<VarName>>,
    pub spec: OutputHandlerSpec,
    aux_info: Option<Vec<VarName>>,
    mqtt_port: Option<u16>,
    redis_port: Option<u16>,
}

const MQTT_FACTORY: MqttFactory = MqttFactory::Paho;

impl OutputHandlerBuilder {
    pub fn new(spec: impl Into<OutputHandlerSpec>) -> Self {
        Self {
            executor: None,
            output_var_names: None,
            spec: spec.into(),
            aux_info: None,
            mqtt_port: None,
            redis_port: None,
        }
    }

    pub fn executor(mut self, executor: Rc<LocalExecutor<'static>>) -> Self {
        self.executor = Some(executor);
        self
    }

    pub fn output_var_names(mut self, output_var_names: Vec<VarName>) -> Self {
        self.output_var_names = Some(output_var_names);
        self
    }

    pub fn aux_info(mut self, aux_info: Vec<VarName>) -> Self {
        self.aux_info = Some(aux_info);
        self
    }

    pub fn mqtt_port(mut self, mqtt_port: Option<u16>) -> Self {
        self.mqtt_port = mqtt_port;
        self
    }

    pub fn redis_port(mut self, redis_port: Option<u16>) -> Self {
        self.redis_port = redis_port;
        self
    }

    // CLI topics must be an exact match, i.e., all stream variables have a defined topic, and no
    // extra topics are allowed (since this likely indicates a user error).
    fn validate_cli_topics(
        topics: &BTreeSet<VarName>,
        output_vars: &BTreeSet<VarName>,
    ) -> anyhow::Result<()> {
        if !(topics == output_vars) {
            let outputs_diff: BTreeSet<_> = output_vars.difference(&topics).cloned().collect();
            let topics_diff: BTreeSet<_> = topics.difference(&output_vars).cloned().collect();
            return Err(anyhow::anyhow!(
                "Provided topics do not match ouput variables. Missing topics for variables: {:?}. Extra topics: {:?}",
                outputs_diff,
                topics_diff
            ));
        }
        Ok(())
    }

    pub async fn async_build(self) -> Box<dyn OutputHandler<Val = Value>> {
        let executor = self
            .executor
            .expect("Cannot build without executor")
            .clone();
        let output_vars: BTreeSet<_> = self
            .output_var_names
            .clone()
            .expect("Output vars must be provided")
            .into_iter()
            .collect();
        let aux_info = self.aux_info.unwrap_or(vec![]).clone();

        match self.spec {
            OutputHandlerSpec::Stdout => Box::new(StdoutOutputHandler::new(
                executor,
                output_vars.into_iter().collect(),
                aux_info,
            )) as Box<dyn OutputHandler<Val = Value>>,
            OutputHandlerSpec::Ros(_json_string) => {
                #[cfg(feature = "ros")]
                {
                    use crate::io::ros::output_handler::ROSOutputHandler;
                    use crate::io::ros::ros_topic_stream_mapping::{
                        VariableMappingData, json_to_mapping,
                    };
                    use std::collections::BTreeMap;
                    use tracing::warn;

                    // ROS mapping must contain all output variables in the spec, and is allowed to
                    // contain additional variables (but they will be ignored, with a warning).
                    fn filter_ros_mapping(
                        mapping: BTreeMap<String, VariableMappingData>,
                        output_vars: &BTreeSet<VarName>,
                    ) -> anyhow::Result<BTreeMap<String, VariableMappingData>> {
                        let keys = mapping
                            .keys()
                            .map(|k| VarName::new(k))
                            .collect::<BTreeSet<_>>();
                        let missing_keys: Vec<_> =
                            output_vars.difference(&keys.into()).cloned().collect();
                        if !missing_keys.is_empty() {
                            return Err(anyhow::anyhow!(
                                "ROS mapping is missing topics for the following variables: {:?}",
                                missing_keys
                            ));
                        }
                        let mut ignored_mapping = BTreeMap::new();
                        let mut used_mapping = BTreeMap::new();
                        for (k, v) in mapping {
                            if output_vars.contains(&VarName::new(k.as_str())) {
                                used_mapping.insert(k, v);
                            } else {
                                ignored_mapping.insert(k, v);
                            }
                        }
                        if ignored_mapping.len() > 0 {
                            warn!(
                                "Some ROS topics from mapping file are not used in the spec and will be ignored. Mapping: {:?}. Spec vars: {:?}",
                                ignored_mapping, output_vars
                            );
                        }
                        Ok(used_mapping)
                    }

                    let output_mapping_raw = json_to_mapping(&_json_string)
                        .expect("Output mapping file could not be parsed");
                    let output_mapping: BTreeMap<_, _> =
                        filter_ros_mapping(output_mapping_raw, &output_vars).expect(
                            "ROS mapping file does not contain all variables from the spec",
                        );
                    // TODO: OutputHandler should not need both output_vars and
                    // output_mapping, since the mapping already contains the exact variable names.
                    Box::new(
                        ROSOutputHandler::new(
                            executor,
                            "tc_ros_output".into(),
                            output_vars.into_iter().collect(),
                            output_mapping,
                            aux_info,
                        )
                        .expect("ROS output handler could not be created"),
                    )
                }
                #[cfg(not(feature = "ros"))]
                {
                    unimplemented!("ROS support not enabled")
                }
            }
            OutputHandlerSpec::MQTT(topics) => {
                let topics: BTreeMap<_, _> = if let Some(topics) = topics {
                    // Topics provided by user
                    topics
                        .into_iter()
                        // Only include topics that are in the output_vars
                        // this is necessary for localisation support
                        .filter(|topic| output_vars.contains(&VarName::new(topic.as_str())))
                        .map(|topic| (topic.clone().into(), topic))
                        .collect()
                } else {
                    // Auto generated topics from spec
                    output_vars
                        .iter()
                        .map(|var| (var.clone(), var.into()))
                        .collect()
                };
                Self::validate_cli_topics(
                    &topics.keys().cloned().collect::<BTreeSet<_>>(),
                    &output_vars,
                )
                .expect("Provided MQTT topics do not match output variables");

                // TODO: OutputHandler should not need both output_vars and
                // output_mapping, since the mapping already contains the exact variable names.
                let mut handler = MQTTOutputHandler::new(
                    executor.clone(),
                    MQTT_FACTORY,
                    output_vars.into_iter().collect(),
                    MQTT_HOSTNAME,
                    self.mqtt_port,
                    topics,
                    aux_info,
                )
                .expect("MQTT output handler could not be created");
                handler
                    .connect()
                    .await
                    .expect("MQTT output handler failed to connect");
                Box::new(handler) as Box<dyn OutputHandler<Val = Value>>
            }
            OutputHandlerSpec::Redis(topics) => {
                let topics: BTreeMap<_, _> = if let Some(topics) = topics {
                    // Topics provided by user
                    topics
                        .into_iter()
                        // Only include topics that are in the output_vars
                        // this is necessary for localisation support
                        .filter(|topic| output_vars.contains(&VarName::new(topic.as_str())))
                        .map(|topic| (topic.clone().into(), topic))
                        .collect()
                } else {
                    // Auto generated topics from spec
                    output_vars
                        .iter()
                        .map(|var| (var.clone(), var.into()))
                        .collect()
                };
                Self::validate_cli_topics(
                    &topics.keys().cloned().collect::<BTreeSet<_>>(),
                    &output_vars,
                )
                .expect("Provided Redis topics do not match output variables");

                // TODO: OutputHandler should not need both output_vars and
                // output_mapping, since the mapping already contains the exact variable names.
                let mut handler = RedisOutputHandler::new(
                    executor.clone(),
                    output_vars.into_iter().collect(),
                    REDIS_HOSTNAME,
                    self.redis_port,
                    topics,
                    aux_info,
                )
                .expect("Redis output handler could not be created");
                handler
                    .connect()
                    .await
                    .expect("Redis output handler failed to connect");
                Box::new(handler) as Box<dyn OutputHandler<Val = Value>>
            }
            OutputHandlerSpec::Manual => unimplemented!("Has not been needed yet"),
        }
    }
}
