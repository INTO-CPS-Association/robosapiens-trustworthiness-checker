use std::rc::Rc;

use smol::LocalExecutor;
use tracing::info_span;

use crate::core::MQTT_HOSTNAME;
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
        /// Topics
        Option<Vec<String>>,
    ),
    /// MQTT distributed input provider with data handling
    MQTTMap(
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
}

impl InputProviderBuilder {
    pub fn new(spec: impl Into<InputProviderSpec>) -> Self {
        Self {
            spec: spec.into(),
            lang: None,
            input_vars: None,
            executor: None,
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

    pub fn mqtt_map(topics: Option<Vec<String>>) -> Self {
        Self::new(InputProviderSpec::MQTTMap(topics))
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

    pub async fn async_build(self) -> Box<dyn InputProvider<Val = Value>> {
        match self.spec {
            InputProviderSpec::File(path) => {
                let input_file_parser = match self.lang.unwrap_or(Language::Lola) {
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
                    let input_mapping_str = std::fs::read_to_string(&_input_ros_topics)
                        .expect("Input mapping file could not be read");
                    let input_mapping =
                        ros_topic_stream_mapping::json_to_mapping(&input_mapping_str)
                            .expect("Input mapping file could not be parsed");
                    Box::new(
                        ROSInputProvider::new(executor.clone(), input_mapping)
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
                let mut mqtt_input_provider = tc::io::mqtt::MQTTInputProvider::new(
                    self.executor.unwrap().clone(),
                    MQTT_HOSTNAME,
                    var_topics,
                )
                .expect("MQTT input provider could not be created");
                mqtt_input_provider
                    .started
                    .wait_for(|x| info_span!("Waited for input provider started").in_scope(|| *x))
                    .await
                    .expect("MQTT input provider failed to start");
                Box::new(mqtt_input_provider) as Box<dyn InputProvider<Val = Value>>
            }
            InputProviderSpec::MQTTMap(topics) => {
                let var_topics = match topics {
                    Some(topics) => topics
                        .into_iter()
                        .map(|topic| (VarName::new(topic.as_str()), topic))
                        .collect(),
                    None => self
                        .input_vars
                        .unwrap()
                        .into_iter()
                        .map(|topic| (topic.clone(), format!("{}", topic)))
                        .collect(),
                };
                let mut mqtt_input_provider = tc::io::mqtt::MapMQTTInputProvider::new(
                    self.executor.unwrap().clone(),
                    MQTT_HOSTNAME,
                    var_topics,
                )
                .expect("MQTT input provider could not be created");
                mqtt_input_provider
                    .started
                    .wait_for(|x| info_span!("Waited for input provider started").in_scope(|| *x))
                    .await
                    .expect("MQTT input provider failed to start");
                Box::new(mqtt_input_provider) as Box<dyn InputProvider<Val = Value>>
            }
        }
    }
}
