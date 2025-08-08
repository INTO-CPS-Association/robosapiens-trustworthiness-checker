use std::rc::Rc;

use smol::LocalExecutor;

use crate::cli::args::OutputMode;
use crate::core::{MQTT_HOSTNAME, REDIS_HOSTNAME};
use crate::io::cli::StdoutOutputHandler;
use crate::io::mqtt::MQTTOutputHandler;
use crate::io::redis::RedisOutputHandler;
use crate::{self as tc, Value};
use crate::{VarName, core::OutputHandler};

#[derive(Clone)]
pub struct OutputHandlerBuilder {
    executor: Option<Rc<LocalExecutor<'static>>>,
    output_var_names: Option<Vec<VarName>>,
    output_mode: OutputMode,
    mqtt_port: Option<u16>,
    redis_port: Option<u16>,
}

impl OutputHandlerBuilder {
    pub fn new(output_mode: OutputMode) -> Self {
        Self {
            executor: None,
            output_var_names: None,
            output_mode,
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

        match self.output_mode.clone() {
            OutputMode {
                output_stdout: true,
                ..
            } => Box::new(StdoutOutputHandler::<tc::Value>::new(
                executor,
                output_var_names,
            )),
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
                Box::new(
                    MQTTOutputHandler::new(
                        executor.clone(),
                        output_var_names,
                        MQTT_HOSTNAME,
                        self.mqtt_port,
                        topics,
                    )
                    .expect("MQTT output handler could not be created"),
                )
            }
            OutputMode {
                redis_output: true, ..
            } => {
                let topics = output_var_names
                    .iter()
                    .map(|var| (var.clone(), var.into()))
                    .collect();
                Box::new(
                    RedisOutputHandler::new(
                        executor.clone(),
                        output_var_names,
                        REDIS_HOSTNAME,
                        self.redis_port,
                        topics,
                    )
                    .expect("Redis output handler could not be created"),
                )
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
                Box::new(
                    RedisOutputHandler::new(
                        executor.clone(),
                        output_var_names,
                        REDIS_HOSTNAME,
                        self.redis_port,
                        topics,
                    )
                    .expect("Redis output handler could not be created"),
                )
            }
            OutputMode {
                mqtt_output: true, ..
            } => {
                let topics = output_var_names
                    .iter()
                    .map(|var| (var.clone(), var.into()))
                    .collect();
                Box::new(
                    MQTTOutputHandler::new(
                        executor,
                        output_var_names,
                        MQTT_HOSTNAME,
                        self.mqtt_port,
                        topics,
                    )
                    .expect("MQTT output handler could not be created"),
                )
            }
            OutputMode {
                output_ros_topics: Some(_),
                ..
            } => unimplemented!("ROS output not implemented"),
            // Default to stdout
            _ => Box::new(StdoutOutputHandler::<tc::Value>::new(
                executor,
                output_var_names,
            )),
        }
    }
}
