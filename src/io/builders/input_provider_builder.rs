use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use smol::LocalExecutor;
use tracing::{debug_span, warn};

use crate::core::{MQTT_HOSTNAME, REDIS_HOSTNAME, RuntimeSpec};
use crate::io::config::{MsgTypeMapping, TopicMapping};
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
        /// Topic mapping
        TopicMapping,
        /// Var Msg Type mapping
        MsgTypeMapping,
    ),
    /// MQTT topics input provider
    MQTT(
        /// Topic mapping
        Option<TopicMapping>,
    ),
    /// Redis topics input provider
    Redis(
        /// Topic mapping
        Option<TopicMapping>,
    ),
    Manual,
}

#[derive(Clone, Debug)]
pub struct InputProviderBuilder {
    pub spec: InputProviderSpec,
    lang: Option<Language>,
    input_vars: Option<BTreeSet<VarName>>,
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

    pub fn ros(topic_mapping: TopicMapping, msg_type_mapping: MsgTypeMapping) -> Self {
        Self::new(InputProviderSpec::Ros(topic_mapping, msg_type_mapping))
    }

    pub fn mqtt(topics: Option<TopicMapping>) -> Self {
        Self::new(InputProviderSpec::MQTT(topics))
    }

    pub fn redis(topics: Option<TopicMapping>) -> Self {
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

    pub fn runtime(mut self, runtime: RuntimeSpec) -> Self {
        self.replay_history = if matches!(runtime, RuntimeSpec::Distributed) {
            ReplayHistory::store_all()
        } else {
            ReplayHistory::disabled()
        };
        self
    }

    // Topic mapping must contain all spec input variables. Extra mapping entries are
    // allowed and will be ignored.
    fn filter_cli_topics(
        topics: TopicMapping,
        vars: &BTreeSet<VarName>,
    ) -> anyhow::Result<TopicMapping> {
        let topic_keys = topics.keys().cloned().collect::<BTreeSet<_>>();
        let missing: Vec<_> = vars.difference(&topic_keys).cloned().collect();
        if !missing.is_empty() {
            return Err(anyhow::anyhow!(
                "Topic mapping is missing topics for the following variables: {:?}",
                missing
            ));
        }

        let mut ignored = BTreeMap::new();
        let mut used = BTreeMap::new();
        for (var, topic) in topics {
            if vars.contains(&var) {
                used.insert(var, topic);
            } else {
                ignored.insert(var, topic);
            }
        }

        if !ignored.is_empty() {
            warn!(
                "Some topics from topic mapping are not used in the spec and will be ignored:\nIgnored vars: {:?}.\nUsing vars vars: {:?}",
                ignored.keys(),
                vars
            );
        }

        Ok(used)
    }

    pub async fn async_build(self) -> Box<dyn InputProvider<Val = Value>> {
        let _async_build = debug_span!("async_build for input provider").entered();
        let input_vars: BTreeSet<_> = self
            .input_vars
            .clone()
            .expect("Input vars must be provided")
            .into_iter()
            .collect();
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
            InputProviderSpec::Ros(_topic_mapping, _msg_type_mapping) => {
                #[cfg(feature = "ros")]
                {
                    use crate::io::ros::input_provider::ROSInputProvider;
                    use crate::io::ros::ros_topic_stream_mapping::{
                        VariableMappingData, ros_stream_mapping_from_topic_and_msg_type_mapping,
                    };
                    use tracing::warn;

                    // ROS mapping must contain all input variables in the spec, and is allowed to
                    // contain additional variables (but they will be ignored, with a warning).
                    fn filter_ros_mapping(
                        mapping: BTreeMap<String, VariableMappingData>,
                        input_vars: &BTreeSet<VarName>,
                    ) -> anyhow::Result<BTreeMap<String, VariableMappingData>> {
                        let keys = mapping
                            .keys()
                            .map(|k| VarName::new(k))
                            .collect::<BTreeSet<_>>();
                        let missing_keys: Vec<_> =
                            input_vars.difference(&keys.into()).cloned().collect();
                        if !missing_keys.is_empty() {
                            return Err(anyhow::anyhow!(
                                "ROS mapping is missing topics for the following variables: {:?}",
                                missing_keys
                            ));
                        }
                        let mut ignored_mapping = BTreeMap::new();
                        let mut used_mapping = BTreeMap::new();
                        for (k, v) in mapping {
                            if input_vars.contains(&VarName::new(k.as_str())) {
                                used_mapping.insert(k, v);
                            } else {
                                ignored_mapping.insert(k, v);
                            }
                        }
                        if ignored_mapping.len() > 0 {
                            warn!(
                                "Some ROS topics from input mapping file are not used in the spec and will be ignored:\nIgnored map vars: {:?}.\nUsing input vars: {:?}",
                                ignored_mapping.keys(),
                                input_vars
                            );
                        }
                        Ok(used_mapping)
                    }

                    let input_mapping_raw = ros_stream_mapping_from_topic_and_msg_type_mapping(
                        _topic_mapping,
                        _msg_type_mapping,
                    )
                    .expect("Input mapping data could not be used to build topic mapping");
                    let input_mapping: BTreeMap<_, _> =
                        filter_ros_mapping(input_mapping_raw, &input_vars).expect(
                            "ROS mapping file does not contain all variables from the spec",
                        );

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
            InputProviderSpec::MQTT(topics) => {
                let var_topics: BTreeMap<_, _> = match topics {
                    Some(topics) => Self::filter_cli_topics(topics, &input_vars)
                        .expect("Provided MQTT topics do not match input variables"),
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
            InputProviderSpec::Redis(topics) => {
                let var_topics: BTreeMap<_, _> = match topics {
                    Some(topics) => Self::filter_cli_topics(topics, &input_vars)
                        .expect("Provided Redis topics do not match input variables"),
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
