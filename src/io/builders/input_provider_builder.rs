use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use async_stream::stream;
use smol::LocalExecutor;
use tracing::{debug_span, warn};

use crate::core::{MQTT_HOSTNAME, REDIS_HOSTNAME, RuntimeSpec};
use crate::io::config::{MsgTypeMapping, TopicMapping};
use crate::io::file::FileInputProvider;
use crate::io::mqtt::MqttFactory;
use crate::io::replay_history::ReplayHistory;
use crate::io::testing::ManualInputProvider;
use crate::runtime::builder::ValueConfig;

use crate::stream_utils::Fanout;
use crate::{self as tc, OutputStream, Value};
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
    Mqtt(
        /// Topic mapping
        Option<TopicMapping>,
    ),
    /// Redis topics input provider
    Redis(
        /// Topic mapping
        Option<TopicMapping>,
    ),
    /// Manually receives results based on the Fanout channel, and forwards them to any
    /// constructed InputProviders. Useful for testing.
    Manual(BTreeMap<VarName, Rc<Fanout<Value>>>),
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
        Self::new(InputProviderSpec::Mqtt(topics))
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

    pub async fn build(self) -> Box<dyn InputProvider<Val = Value>> {
        let _build = debug_span!("build for input provider").entered();
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
                    use crate::io::ros::input_provider::RosInputProvider;
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
                        RosInputProvider::new_with_replay_history(
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
            InputProviderSpec::Mqtt(topics) => {
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
                let mut mqtt_input_provider = tc::io::mqtt::MqttInputProvider::new(
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
            InputProviderSpec::Manual(fanout) => {
                let input_vars = self
                    .input_vars
                    .expect("Input vars must be provided for manual input provider");
                assert!(
                    fanout
                        .keys()
                        .cloned()
                        .collect::<BTreeSet<_>>()
                        .is_superset(&input_vars),
                    "Fanout keys must contain all input variables from the spec"
                );
                let mut rxs = BTreeMap::new();
                for (var, fanout) in fanout {
                    // Important that this happens outside stream!
                    let mut sub_rx = fanout.subscribe();
                    let rx: OutputStream<Value> = Box::pin(stream! {
                        while let Some(val) = sub_rx.recv().await {
                            yield val;
                        }
                    });
                    rxs.insert(var.clone(), rx);
                }

                let provider = ManualInputProvider::<ValueConfig>::new_from_streams(rxs);

                Box::new(provider) as Box<dyn InputProvider<Val = Value>>
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lang::dsrv::parser::dsrv_specification;
    use crate::{Value, VarName, async_test, dsrv_fixtures::spec_simple_add_monitor};
    use futures::StreamExt;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::rc::Rc;
    use tc_testutils::streams::with_timeout;

    #[apply(async_test)]
    async fn test_manual_input_builder_regular(ex: Rc<LocalExecutor<'static>>) {
        // Tests that the ManualInputProvider built by the builder correctly receives inputs through
        // the provided channel.
        // (Notice that we are transmitting through the built ManualInputProvider even though we do not
        // call `sender_channel` directly.)
        let model = dsrv_specification(&mut spec_simple_add_monitor()).unwrap();

        let (tx_x, fx) = Fanout::new();
        let (tx_y, fy) = Fanout::new();
        let fanouts = BTreeMap::from([(VarName::new("x"), fx), (VarName::new("y"), fy)]);
        let mut provider = InputProviderBuilder::new(InputProviderSpec::Manual(fanouts))
            .executor(ex.clone())
            .model(model)
            .build()
            .await;

        let mut x_stream = provider
            .var_stream(&VarName::new("x"))
            .expect("x stream should be available");
        let mut y_stream = provider
            .var_stream(&VarName::new("y"))
            .expect("y stream should be available");

        let mut control_stream = provider.control_stream().await;

        tx_x.send(Value::Int(1)).await;
        tx_y.send(Value::Int(3)).await;

        let _ = with_timeout(control_stream.next(), 1, "ctrl_1")
            .await
            .unwrap();

        let x_val = with_timeout(x_stream.next(), 1, "x1")
            .await
            .unwrap()
            .unwrap();
        let y_val = with_timeout(y_stream.next(), 1, "y1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(x_val, Value::Int(1));
        assert_eq!(y_val, Value::Int(3));

        tx_x.send(Value::Int(2)).await;
        tx_y.send(Value::Int(4)).await;

        let _ = with_timeout(control_stream.next(), 1, "ctrl_2")
            .await
            .unwrap();

        let x_val = with_timeout(x_stream.next(), 1, "x2")
            .await
            .unwrap()
            .unwrap();
        let y_val = with_timeout(y_stream.next(), 1, "y2")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(x_val, Value::Int(2));
        assert_eq!(y_val, Value::Int(4));

        drop(tx_x);
        drop(tx_y);

        let _ = with_timeout(control_stream.next(), 1, "ctrl_end")
            .await
            .unwrap();

        assert!(
            with_timeout(x_stream.next(), 1, "x_end")
                .await
                .expect("x stream should end")
                .is_none()
        );
        assert!(
            with_timeout(y_stream.next(), 1, "y_end")
                .await
                .expect("x stream should end")
                .is_none()
        )
    }

    #[apply(async_test)]
    async fn test_manual_input_builder_multi_conc(ex: Rc<LocalExecutor<'static>>) {
        // Tests that two ManualInputProviders built from cloned builders can each
        // receive values through the same user channel.
        let model = dsrv_specification(&mut spec_simple_add_monitor()).unwrap();

        let (tx_x, fx) = Fanout::new();
        let (tx_y, fy) = Fanout::new();
        let fanouts = BTreeMap::from([(VarName::new("x"), fx), (VarName::new("y"), fy)]);
        let builder1 = InputProviderBuilder::new(InputProviderSpec::Manual(fanouts))
            .executor(ex.clone())
            .model(model.clone());
        let builder2 = builder1.clone();

        // Build both providers first
        let mut provider1 = builder1.build().await;
        let mut provider2 = builder2.build().await;

        let mut x_stream1 = provider1.var_stream(&VarName::new("x")).expect("x stream");
        let mut y_stream1 = provider1.var_stream(&VarName::new("y")).expect("y stream");
        let mut ctrl1 = provider1.control_stream().await;

        let mut x_stream2 = provider2.var_stream(&VarName::new("x")).expect("x stream");
        let mut y_stream2 = provider2.var_stream(&VarName::new("y")).expect("y stream");
        let mut ctrl2 = provider2.control_stream().await;

        // Send one pair — both providers should receive the same values
        tx_x.send(Value::Int(10)).await;
        tx_y.send(Value::Int(20)).await;

        let _ = with_timeout(ctrl1.next(), 1, "c1_1").await.unwrap();
        let _ = with_timeout(ctrl2.next(), 1, "c2_1").await.unwrap();

        let x1 = with_timeout(x_stream1.next(), 1, "x1")
            .await
            .unwrap()
            .unwrap();
        let y1 = with_timeout(y_stream1.next(), 1, "y1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(x1, Value::Int(10));
        assert_eq!(y1, Value::Int(20));

        let x2 = with_timeout(x_stream2.next(), 1, "x2")
            .await
            .unwrap()
            .unwrap();
        let y2 = with_timeout(y_stream2.next(), 1, "y2")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(x2, Value::Int(10));
        assert_eq!(y2, Value::Int(20));
    }

    #[apply(async_test)]
    async fn test_manual_input_builder_sequential_rebuild(ex: Rc<LocalExecutor<'static>>) {
        // Tests that after dropping one provider, a new provider built from a
        // clone still receives values through the same user channel.
        let model = dsrv_specification(&mut spec_simple_add_monitor()).unwrap();

        let (tx_x, fx) = Fanout::new();
        let (_tx_y, fy) = Fanout::new();
        let fanouts = BTreeMap::from([(VarName::new("x"), fx), (VarName::new("y"), fy)]);
        let builder1 = InputProviderBuilder::new(InputProviderSpec::Manual(fanouts))
            .executor(ex.clone())
            .model(model.clone());
        let builder2 = builder1.clone();

        // Build and use the first provider
        {
            let mut provider1 = builder1.build().await;
            let mut xs = provider1.var_stream(&VarName::new("x")).expect("x");
            let mut ctrl = provider1.control_stream().await;

            tx_x.send(Value::Int(100)).await;
            let _ = with_timeout(ctrl.next(), 1, "c1").await.unwrap();
            let v = with_timeout(xs.next(), 1, "x1").await.unwrap().unwrap();
            assert_eq!(v, Value::Int(100));
            // provider1 dropped here
        }

        // Build a second provider from the clone — same channel should still work
        let mut provider2 = builder2.build().await;
        let mut xs2 = provider2.var_stream(&VarName::new("x")).expect("x");
        let mut ctrl2 = provider2.control_stream().await;

        tx_x.send(Value::Int(200)).await;
        let _ = with_timeout(ctrl2.next(), 1, "c2").await.unwrap();
        let v2 = with_timeout(xs2.next(), 1, "x2").await.unwrap().unwrap();
        assert_eq!(v2, Value::Int(200));
    }
}
