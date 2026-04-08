use std::rc::Rc;

use smol::LocalExecutor;
use tracing::debug_span;

use crate::core::{MQTT_HOSTNAME, REDIS_HOSTNAME, Runtime};
use crate::io::file::FileInputProvider;
use crate::io::mqtt::MqttFactory;
use crate::io::replay_history::ReplayHistory;
use crate::io::testing::ManualInputProvider;
use crate::runtime::builder::ValueConfig;
use crate::{self as tc, Value};
use crate::{InputProvider, Specification, VarName, cli::args::Language};

const MQTT_FACTORY: MqttFactory = MqttFactory::Paho;

#[derive(Debug, Clone)]
pub enum InputProviderSpec {
    /// File input provider
    File(String),
    /// ROS topics input provider
    Ros(
        /// JSON string with topics and types
        String,
    ),
    /// MQTT topics input provider
    MQTT(
        /// Topics
        Option<Vec<String>>,
    ),
    /// Redis topics input provider
    Redis(
        /// Topics
        Option<Vec<String>>,
    ),
    Manual,
}

#[derive(Clone, Debug)]
pub struct InputProviderBuilder {
    pub spec: InputProviderSpec,
    lang: Option<Language>,
    input_vars: Option<Vec<VarName>>,
    executor: Option<Rc<LocalExecutor<'static>>>,
    redis_port: Option<u16>,
    mqtt_port: Option<u16>,
    replay_history: ReplayHistory,
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
            replay_history: ReplayHistory::disabled(),
        }
    }

    pub fn file(path: String) -> Self {
        Self::new(InputProviderSpec::File(path))
    }

    pub fn ros(json_info: String) -> Self {
        Self::new(InputProviderSpec::Ros(json_info))
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

    pub fn spec(mut self, spec: InputProviderSpec) -> Self {
        self.spec = spec;
        self
    }

    pub fn replay_history(mut self, replay_history: ReplayHistory) -> Self {
        self.replay_history = replay_history;
        self
    }

    pub fn runtime(mut self, runtime: Runtime) -> Self {
        self.replay_history = if matches!(runtime, Runtime::Distributed) {
            ReplayHistory::store_all()
        } else {
            ReplayHistory::disabled()
        };
        self
    }

    pub async fn async_build(self) -> Box<dyn InputProvider<Val = Value>> {
        let _async_build = debug_span!("async_build for input provider").entered();
        match self.spec {
            InputProviderSpec::File(path) => {
                let input_file_parser = match self.lang.unwrap_or(Language::DSRV) {
                    Language::DSRV => tc::lang::untimed_input::untimed_input_file,
                };
                let data = tc::parse_file(input_file_parser, &path)
                    .await
                    .expect("Input file could not be parsed");
                Box::new(FileInputProvider::with_replay_history(
                    data,
                    self.replay_history.clone(),
                )) as Box<dyn InputProvider<Val = Value>>
            }
            InputProviderSpec::Ros(_json_string) => {
                #[cfg(feature = "ros")]
                {
                    use crate::io::ros::input_provider::ROSInputProvider;
                    use crate::io::ros::ros_topic_stream_mapping::json_to_mapping;
                    let input_mapping = json_to_mapping(&_json_string)
                        .expect("Input mapping file could not be parsed");
                    Box::new(
                        ROSInputProvider::new_with_replay_history(
                            self.executor.clone().expect(""),
                            input_mapping,
                            self.replay_history.clone(),
                        )
                        .expect("ROS input provider could not be created"),
                    )
                }
                #[cfg(not(feature = "ros"))]
                {
                    unimplemented!("ROS support not enabled")
                }
            }
            // TODO: For the Some case to be useful, it should be a mapping of InputStream ->
            // Topic, not just a list of topics (because this makes no difference compared to just
            // using --mqtt-input).
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
                let mut mqtt_input_provider = tc::io::mqtt::MQTTInputProvider::new(
                    self.executor.unwrap().clone(),
                    MQTT_FACTORY,
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
            // TODO: For the Some case to be useful, it should be a mapping of InputStream ->
            // Topic, not just a list of topics (because this makes no difference compared to just
            // using --redis-input).
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
                let mut redis_input_provider = tc::io::redis::RedisInputProvider::new(
                    REDIS_HOSTNAME,
                    self.redis_port,
                    var_topics,
                )
                .expect("Redis input provider could not be created");

                redis_input_provider
                    .connect()
                    .await
                    .expect("Redis input provider failed to connect");

                Box::new(redis_input_provider) as Box<dyn InputProvider<Val = Value>>
            }
            InputProviderSpec::Manual => {
                let input_vars = self
                    .input_vars
                    .expect("Input vars must be provided for manual input provider");
                Box::new(ManualInputProvider::<ValueConfig>::new(input_vars))
                    as Box<dyn InputProvider<Val = Value>>
            }
        }
    }
}
