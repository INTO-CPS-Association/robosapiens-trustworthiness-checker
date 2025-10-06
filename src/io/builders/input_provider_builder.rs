use std::rc::Rc;

use smol::LocalExecutor;
use tracing::debug_span;

use crate::core::{MQTT_HOSTNAME, REDIS_HOSTNAME};
use crate::io::mqtt::MqttFactory;
use crate::{self as tc, Value};
use crate::{InputProvider, Specification, VarName, cli::args::Language};

#[derive(Debug, Clone)]
pub enum InputProviderSpec {
    /// File input provider
    File(String),
    /// ROS topics input provider
    Ros(
        /// Topics
        String,
    ),
    /// MQTT topics input provider
    MQTT(
        /// Topics and whether
        Option<Vec<String>>,
    ),
    /// Redis topics input provider
    Redis(
        /// Topics
        Option<Vec<String>>,
    ),
}

#[derive(Clone)]
pub struct InputProviderBuilder {
    spec: InputProviderSpec,
    lang: Option<Language>,
    input_vars: Option<Vec<VarName>>,
    executor: Option<Rc<LocalExecutor<'static>>>,
    redis_port: Option<u16>,
    mqtt_port: Option<u16>,
}

impl InputProviderBuilder {
    pub fn new(spec: impl Into<InputProviderSpec>) -> Self {
        Self {
            spec: spec.into(),
            lang: None,
            input_vars: None,
            executor: None,
            redis_port: None,
            mqtt_port: None,
        }
    }

    pub fn file(path: String) -> Self {
        Self::new(InputProviderSpec::File(path))
    }

    pub fn ros(topics: String) -> Self {
        Self::new(InputProviderSpec::Ros(topics))
    }

    pub fn mqtt(topics: Option<Vec<String>>) -> Self {
        Self::new(InputProviderSpec::MQTT(topics))
    }

    pub fn redis(topics: Option<Vec<String>>) -> Self {
        Self::new(InputProviderSpec::Redis(topics))
    }

    pub fn lang(mut self, lang: Language) -> Self {
        self.lang = Some(lang);
        self
    }

    pub fn model<Expr>(mut self, model: impl Specification<Expr = Expr>) -> Self {
        self.input_vars = Some(model.input_vars());
        self
    }

    pub fn executor(mut self, executor: Rc<LocalExecutor<'static>>) -> Self {
        self.executor = Some(executor);
        self
    }

    pub fn mqtt_port(mut self, port: Option<u16>) -> Self {
        self.mqtt_port = port;
        self
    }

    pub fn redis_port(mut self, port: Option<u16>) -> Self {
        self.redis_port = port;
        self
    }

    pub async fn async_build(self) -> Box<dyn InputProvider<Val = Value>> {
        let _async_build = debug_span!("async_build for input provider").entered();
        match self.spec {
            InputProviderSpec::File(path) => {
                let input_file_parser = match self.lang.unwrap_or(Language::DynSRV) {
                    Language::DynSRV => tc::lang::untimed_input::untimed_input_file,
                    Language::Lola => tc::lang::untimed_input::untimed_input_file,
                };
                Box::new(
                    tc::parse_file(input_file_parser, &path)
                        .await
                        .expect("Input file could not be parsed"),
                ) as Box<dyn InputProvider<Val = Value>>
            }
            InputProviderSpec::Ros(_input_ros_topics) => {
                #[cfg(feature = "ros")]
                {
                    use crate::io::ros::input_provider::ROSInputProvider;
                    use crate::io::ros::ros_topic_stream_mapping::json_to_mapping;
                    let input_mapping_str = std::fs::read_to_string(&_input_ros_topics)
                        .expect("Input mapping file could not be read");
                    let input_mapping = json_to_mapping(&input_mapping_str)
                        .expect("Input mapping file could not be parsed");
                    Box::new(
                        ROSInputProvider::new(self.executor.clone().expect(""), input_mapping)
                            .expect("ROS input provider could not be created"),
                    )
                }
                #[cfg(not(feature = "ros"))]
                {
                    unimplemented!("ROS support not enabled")
                }
            }
            InputProviderSpec::MQTT(topics) => {
                let var_topics = match topics {
                    Some(topics) => topics
                        .iter()
                        .map(|topic| (VarName::new(topic), topic.clone()))
                        .collect(),
                    None => self
                        .input_vars
                        .unwrap()
                        .into_iter()
                        .map(|topic| (topic.clone(), format!("{}", topic)))
                        .collect(),
                };
                let factory = MqttFactory::Paho; // TODO: make configurable
                let mut mqtt_input_provider = tc::io::mqtt::MQTTInputProvider::new(
                    self.executor.unwrap().clone(),
                    factory,
                    MQTT_HOSTNAME,
                    self.mqtt_port,
                    var_topics,
                    u32::MAX,
                );
                mqtt_input_provider
                    .connect()
                    .await
                    .expect("MQTT input provider failed to connect");

                Box::new(mqtt_input_provider) as Box<dyn InputProvider<Val = Value>>
            }
            InputProviderSpec::Redis(topics) => {
                let var_topics = match topics {
                    Some(topics) => topics
                        .iter()
                        .map(|topic| (VarName::new(topic), topic.clone()))
                        .collect(),
                    None => self
                        .input_vars
                        .unwrap()
                        .into_iter()
                        .map(|topic| (topic.clone(), format!("{}", topic)))
                        .collect(),
                };
                let redis_input_provider = tc::io::redis::RedisInputProvider::new(
                    self.executor.unwrap().clone(),
                    REDIS_HOSTNAME,
                    self.redis_port,
                    var_topics,
                )
                .expect("Redis input provider could not be created");
                redis_input_provider
                    .ready()
                    .await
                    .expect("Redis input provider failed to start");
                Box::new(redis_input_provider) as Box<dyn InputProvider<Val = Value>>
            }
        }
    }
}
