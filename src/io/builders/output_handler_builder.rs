use std::rc::Rc;

use smol::LocalExecutor;

use crate::cli::args::OutputMode;
use crate::core::{MQTT_HOSTNAME, REDIS_HOSTNAME};
use crate::io::cli::StdoutOutputHandler;
use crate::io::mqtt::{MQTTOutputHandler, MqttFactory};
use crate::io::redis::RedisOutputHandler;
#[cfg(feature = "ros")]
use crate::io::ros::ROSOutputHandler;
#[cfg(feature = "ros")]
use crate::io::ros::json_to_mapping;
use crate::{Value, VarName, core::OutputHandler};

#[derive(Debug, Clone)]
pub struct OutputHandlerBuilder {
    executor: Option<Rc<LocalExecutor<'static>>>,
    output_var_names: Option<Vec<VarName>>,
    pub output_mode: OutputMode,
    aux_info: Option<Vec<VarName>>,
    mqtt_port: Option<u16>,
    redis_port: Option<u16>,
}

const MQTT_FACTORY: MqttFactory = MqttFactory::Paho;

impl OutputHandlerBuilder {
    pub fn new(output_mode: OutputMode) -> Self {
        Self {
            executor: None,
            output_var_names: None,
            output_mode,
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

    pub async fn async_build(self) -> Box<dyn OutputHandler<Val = Value>> {
        let executor = self
            .executor
            .expect("Cannot build without executor")
            .clone();
        // Should this also be expect?
        let output_var_names = self.output_var_names.unwrap_or(vec![]).clone();
        let aux_info = self.aux_info.unwrap_or(vec![]).clone();

        match self.output_mode.clone() {
            OutputMode {
                output_stdout: true,
                ..
            } => Box::new(StdoutOutputHandler::new(
                executor,
                output_var_names,
                aux_info,
            )) as Box<dyn OutputHandler<Val = Value>>,
            OutputMode {
                output_mqtt_topics: Some(topics),
                ..
            } => {
                let topics = topics
                    .into_iter()
                    // Only include topics that are in the output_vars
                    // this is necessary for localisation support
                    .filter(|topic| output_var_names.contains(&VarName::new(topic.as_str())))
                    .map(|topic| (topic.clone().into(), topic))
                    .collect();
                let mut handler = MQTTOutputHandler::new(
                    executor.clone(),
                    MQTT_FACTORY,
                    output_var_names,
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
            OutputMode {
                redis_output: true, ..
            } => {
                let topics = output_var_names
                    .iter()
                    .map(|var| (var.clone(), var.into()))
                    .collect();
                let mut handler = RedisOutputHandler::new(
                    executor.clone(),
                    output_var_names,
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
            OutputMode {
                output_redis_topics: Some(topics),
                ..
            } => {
                let topics = topics
                    .into_iter()
                    // Only include topics that are in the output_vars
                    // this is necessary for localisation support
                    .filter(|topic| output_var_names.contains(&VarName::new(topic.as_str())))
                    .map(|topic| (topic.clone().into(), topic))
                    .collect();
                let mut handler = RedisOutputHandler::new(
                    executor.clone(),
                    output_var_names,
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
            OutputMode {
                mqtt_output: true, ..
            } => {
                let topics = output_var_names
                    .iter()
                    .map(|var| (var.clone(), var.into()))
                    .collect();
                let mut handler = MQTTOutputHandler::new(
                    executor,
                    MQTT_FACTORY,
                    output_var_names,
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
            OutputMode {
                output_ros_file: Some(_output_ros_file),
                ..
            } => {
                cfg_if::cfg_if! {
                    if #[cfg(feature="ros")] {
                        let output_mapping_str = std::fs::read_to_string(&_output_ros_file)
                            .expect("Output mapping file could not be read");
                        let output_mapping = json_to_mapping(&output_mapping_str)
                            .expect("Output mapping file could not be parsed");
                        Box::new(
                            ROSOutputHandler::new(executor, "tc_ros_output".into(), output_var_names, output_mapping, aux_info)
                                .expect("ROS output handler could not be created"),
                        )
                    } else {
                        panic!("ROS support not enabled");
                    }
                }
            }
            // Default to stdout
            _ => Box::new(StdoutOutputHandler::new(
                executor,
                output_var_names,
                aux_info,
            )),
        }
    }
}
