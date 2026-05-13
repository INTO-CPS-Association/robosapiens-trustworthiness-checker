use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use async_unsync::bounded::Sender as MpscSender;
use futures::StreamExt;
use smol::LocalExecutor;

use crate::core::{MQTT_HOSTNAME, REDIS_HOSTNAME};
use crate::io::cli::StdoutOutputHandler;
use crate::io::config::{MsgTypeMapping, TopicMapping};
use crate::io::mqtt::{MqttFactory, MqttOutputHandler};
use crate::io::redis::RedisOutputHandler;
use crate::io::testing::ManualOutputHandler;
use crate::{Value, VarName, core::OutputHandler};

#[derive(Debug, Clone)]
pub enum OutputHandlerSpec {
    /// File input provider
    Stdout,
    /// ROS topics input provider
    Ros(
        /// Topic mapping
        TopicMapping,
        /// Var Msg Type mapping
        MsgTypeMapping,
    ),
    /// MQTT topics output provider
    Mqtt(
        /// Topics mapping
        Option<TopicMapping>,
    ),
    /// Redis topics output provider
    Redis(
        /// Topic mapping
        Option<TopicMapping>,
    ),
    /// Manually sends the results through the provided channel, and forwards them to any
    /// built OutputHandlers. Useful for testing.
    /// NOTE: Building this spawns a detached background task!
    Manual(MpscSender<BTreeMap<VarName, Value>>),
}

#[derive(Debug, Clone)]
pub struct OutputHandlerBuilder {
    executor: Option<Rc<LocalExecutor<'static>>>,
    output_var_names: Option<BTreeSet<VarName>>,
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

    pub fn output_var_names(mut self, output_var_names: BTreeSet<VarName>) -> Self {
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
            OutputHandlerSpec::Ros(_topic_mapping, _msg_type_mapping) => {
                #[cfg(feature = "ros")]
                {
                    use crate::io::ros::output_handler::RosOutputHandler;
                    use crate::io::ros::ros_topic_stream_mapping::{
                        VariableMappingData, ros_stream_mapping_from_topic_and_msg_type_mapping,
                    };
                    use std::collections::BTreeMap;
                    use tracing::warn;

                    // ROS mapping must contain all output variables in the spec, and is allowed to
                    // contain additional variables (but they will be ignored, with a warning).
                    fn filter_ros_mapping(
                        mapping: BTreeMap<String, VariableMappingData>,
                        output_vars: &BTreeSet<VarName>,
                        aux_info: BTreeSet<VarName>,
                    ) -> anyhow::Result<BTreeMap<String, VariableMappingData>> {
                        let keys = mapping
                            .keys()
                            .map(|k| VarName::new(k))
                            .collect::<BTreeSet<_>>();
                        let missing_keys: Vec<_> = output_vars
                            .difference(&keys.into())
                            .filter(|var| !aux_info.contains(*var))
                            .cloned()
                            .collect();
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
                                "Some ROS topics from output mapping file are not used in the spec and will be ignored:\nIgnored map vars: {:?}.\nUsing output vars: {:?}",
                                ignored_mapping.keys(),
                                output_vars
                            );
                        }
                        Ok(used_mapping)
                    }

                    // json_to_mapping(&_json_string)
                    let output_mapping_raw = ros_stream_mapping_from_topic_and_msg_type_mapping(
                        _topic_mapping,
                        _msg_type_mapping,
                    )
                    .expect("Output mapping file could not be parsed");
                    let output_mapping: BTreeMap<_, _> = filter_ros_mapping(
                        output_mapping_raw,
                        &output_vars,
                        aux_info.clone().into_iter().collect(),
                    )
                    .expect("ROS mapping file does not contain all variables from the spec");
                    Box::new(
                        RosOutputHandler::new(
                            executor,
                            "tc_ros_output".into(),
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
            OutputHandlerSpec::Mqtt(topics) => {
                let topics: BTreeMap<VarName, String> = if let Some(topics) = topics {
                    // Topics provided by user
                    topics
                        .into_iter()
                        // Only include topics that are in the output_vars
                        // this is necessary for localisation support
                        .filter(|(var, _)| output_vars.contains(var))
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
                let mut handler = MqttOutputHandler::new(
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
                let topics: BTreeMap<VarName, String> = if let Some(topics) = topics {
                    // Topics provided by user
                    topics
                        .into_iter()
                        // Only include topics that are in the output_vars
                        // this is necessary for localisation support
                        .filter(|(var, _)| output_vars.contains(var))
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

                let mut handler = RedisOutputHandler::new(
                    executor.clone(),
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
            OutputHandlerSpec::Manual(tx) => {
                let mut handler = ManualOutputHandler::new(executor.clone(), output_vars.clone());
                let stream = handler.get_output();
                executor
                    .spawn(async move {
                        let mut stream = stream;
                        while let Some(output) = stream.next().await {
                            if let Err(e) = tx.send(output).await {
                                tracing::error!(
                                    "Failed to send output through manual channel. Most likely due to receiver dropped: {:?}",
                                    e
                                );
                                break;
                            }
                        }
                    })
                    .detach();
                Box::new(handler) as Box<dyn OutputHandler<Val = Value>>
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{async_test, core::OutputStream};
    use futures::stream;
    use macro_rules_attribute::apply;
    use tc_testutils::streams::{tick_stream, with_timeout, with_timeout_res};

    #[apply(async_test)]
    async fn test_manual_handler_builder_regular(ex: Rc<LocalExecutor<'static>>) {
        // Tests that the ManualOutputHandler built by the builder correctly sends outputs through
        // the provided channel.
        // (Notice that we are receiving from the built ManualOutputHandler even though we do not
        // call `get_output` directly.)
        let output_var_names = BTreeSet::from([VarName::new("x"), VarName::new("y")]);
        let (sender, mut receiver) = async_unsync::bounded::channel(10).into_split();
        let builder = OutputHandlerBuilder::new(OutputHandlerSpec::Manual(sender))
            .executor(ex.clone())
            .output_var_names(output_var_names.clone());
        let mut handler = builder.async_build().await;

        let xs: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let ys: OutputStream<Value> = Box::pin(stream::iter(vec![3.into(), 4.into()]));
        let streams = BTreeMap::from([(VarName::new("x"), xs), (VarName::new("y"), ys)]);

        handler.provide_streams(streams);
        let task = ex.spawn(handler.run());

        let mut results = vec![];
        while let Some(res) = with_timeout(receiver.recv(), 1, "recv").await.unwrap() {
            results.push(res);
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get(&VarName::new("x")).unwrap(), &Value::Int(1));
        assert_eq!(results[0].get(&VarName::new("y")).unwrap(), &Value::Int(3));
        assert_eq!(results[1].get(&VarName::new("x")).unwrap(), &Value::Int(2));
        assert_eq!(results[1].get(&VarName::new("y")).unwrap(), &Value::Int(4));

        with_timeout_res(task, 1, "task_finish").await.unwrap();
    }

    #[apply(async_test)]
    async fn test_manual_handler_builder_multi(ex: Rc<LocalExecutor<'static>>) {
        // Tests that the ManualOutputHandlers built by a cloned builder can be used after each
        // other, and that they correctly send outputs through the provided channel.
        let output_var_names = BTreeSet::from([VarName::new("x"), VarName::new("y")]);
        let (sender, mut receiver) = async_unsync::bounded::channel(10).into_split();
        let builder1 = OutputHandlerBuilder::new(OutputHandlerSpec::Manual(sender))
            .executor(ex.clone())
            .output_var_names(output_var_names.clone());
        let builder2 = builder1.clone();
        let mut handler1 = builder1.async_build().await;

        let xs: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let ys: OutputStream<Value> = Box::pin(stream::iter(vec![3.into(), 4.into()]));
        let streams = BTreeMap::from([(VarName::new("x"), xs), (VarName::new("y"), ys)]);

        handler1.provide_streams(streams);
        let task = ex.spawn(handler1.run());

        let mut results = vec![];
        for i in 0..2 {
            let res = with_timeout(receiver.recv(), 1, format!("recv_{}", i).as_str())
                .await
                .expect("Expected finish")
                .expect("Expected Some");
            results.push(res);
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get(&VarName::new("x")).unwrap(), &Value::Int(1));
        assert_eq!(results[0].get(&VarName::new("y")).unwrap(), &Value::Int(3));
        assert_eq!(results[1].get(&VarName::new("x")).unwrap(), &Value::Int(2));
        assert_eq!(results[1].get(&VarName::new("y")).unwrap(), &Value::Int(4));

        let mut handler2 = builder2.async_build().await;

        let xs: OutputStream<Value> = Box::pin(stream::iter(vec![5.into(), 6.into()]));
        let ys: OutputStream<Value> = Box::pin(stream::iter(vec![7.into(), 8.into()]));
        let streams = BTreeMap::from([(VarName::new("x"), xs), (VarName::new("y"), ys)]);

        handler2.provide_streams(streams);
        let task2 = ex.spawn(handler2.run());

        let mut results = vec![];
        for i in 2..4 {
            let res = with_timeout(receiver.recv(), 1, format!("recv_{}", i).as_str())
                .await
                .expect("Expected finish")
                .expect("Expected Some");
            results.push(res);
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get(&VarName::new("x")).unwrap(), &Value::Int(5));
        assert_eq!(results[0].get(&VarName::new("y")).unwrap(), &Value::Int(7));
        assert_eq!(results[1].get(&VarName::new("x")).unwrap(), &Value::Int(6));
        assert_eq!(results[1].get(&VarName::new("y")).unwrap(), &Value::Int(8));

        with_timeout_res(task, 1, "task1_finish").await.unwrap();
        with_timeout_res(task2, 1, "task2_finish").await.unwrap();
    }

    #[apply(async_test)]
    async fn test_manual_handler_builder_multi_conc(ex: Rc<LocalExecutor<'static>>) {
        // Tests that the ManualOutputHandlers built by a cloned builder can be used concurrently,
        // and that they correctly send outputs through the provided channel.
        let output_var_names = BTreeSet::from([VarName::new("x"), VarName::new("y")]);
        let (sender, mut receiver) = async_unsync::bounded::channel(10).into_split();
        let builder1 = OutputHandlerBuilder::new(OutputHandlerSpec::Manual(sender))
            .executor(ex.clone())
            .output_var_names(output_var_names.clone());
        let builder2 = builder1.clone();
        let mut handler1 = builder1.async_build().await;
        let mut handler2 = builder2.async_build().await;

        let xs1: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let ys1: OutputStream<Value> = Box::pin(stream::iter(vec![3.into(), 4.into()]));
        let (mut x_tick1, xs1) = tick_stream(xs1);
        let (mut y_tick1, ys1) = tick_stream(ys1);
        let streams1 = BTreeMap::from([(VarName::new("x"), xs1), (VarName::new("y"), ys1)]);
        handler1.provide_streams(streams1);
        let task1 = ex.spawn(handler1.run());

        let xs2: OutputStream<Value> = Box::pin(stream::iter(vec![5.into(), 6.into()]));
        let ys2: OutputStream<Value> = Box::pin(stream::iter(vec![7.into(), 8.into()]));
        let (mut x_tick2, xs2) = tick_stream(xs2);
        let (mut y_tick2, ys2) = tick_stream(ys2);
        let streams2 = BTreeMap::from([(VarName::new("x"), xs2), (VarName::new("y"), ys2)]);
        handler2.provide_streams(streams2);
        let task2 = ex.spawn(handler2.run());

        // Allow one from each through:
        with_timeout_res(x_tick1.send(()), 1, "tick_x1_1")
            .await
            .unwrap();
        with_timeout_res(y_tick1.send(()), 1, "tick_y1_1")
            .await
            .unwrap();
        with_timeout_res(x_tick2.send(()), 1, "tick_x2_1")
            .await
            .unwrap();
        with_timeout_res(y_tick2.send(()), 1, "tick_y2_1")
            .await
            .unwrap();

        let mut results = vec![];
        for i in 0..2 {
            let res = with_timeout(receiver.recv(), 1, format!("recv_{}", i).as_str())
                .await
                .expect("Expected finish")
                .expect("Expected Some");
            results.push(res);
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get(&VarName::new("x")).unwrap(), &Value::Int(1));
        assert_eq!(results[0].get(&VarName::new("y")).unwrap(), &Value::Int(3));
        assert_eq!(results[1].get(&VarName::new("x")).unwrap(), &Value::Int(5));
        assert_eq!(results[1].get(&VarName::new("y")).unwrap(), &Value::Int(7));

        // Let handler2 finish:
        with_timeout_res(x_tick2.send(()), 1, "tick_x2_2")
            .await
            .unwrap();
        with_timeout_res(y_tick2.send(()), 1, "tick_y2_2")
            .await
            .unwrap();
        let res = with_timeout(receiver.recv(), 1, "recv_2")
            .await
            .expect("Expected finish")
            .expect("Expected Some");
        results.push(res);
        assert_eq!(results.len(), 3);
        assert_eq!(results[2].get(&VarName::new("x")).unwrap(), &Value::Int(6));
        assert_eq!(results[2].get(&VarName::new("y")).unwrap(), &Value::Int(8));
        with_timeout_res(x_tick1.send(()), 1, "tick_x1_1")
            .await
            .unwrap();
        with_timeout_res(y_tick1.send(()), 1, "tick_y1_1")
            .await
            .unwrap();
        let res = with_timeout(receiver.recv(), 1, "recv_3")
            .await
            .expect("Expected finish")
            .expect("Expected Some");
        results.push(res);
        assert_eq!(results.len(), 4);
        assert_eq!(results[3].get(&VarName::new("x")).unwrap(), &Value::Int(2));
        assert_eq!(results[3].get(&VarName::new("y")).unwrap(), &Value::Int(4));

        // Final ticks:
        x_tick1.send(()).await.unwrap();
        x_tick2.send(()).await.unwrap();
        y_tick1.send(()).await.unwrap();
        y_tick2.send(()).await.unwrap();

        with_timeout_res(task1, 1, "task1_finish").await.unwrap();
        with_timeout_res(task2, 1, "task2_finish").await.unwrap();
    }
}
