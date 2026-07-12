use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use async_stream::stream;
use smol::LocalExecutor;
use tracing::{debug_span, warn};

use crate::core::{MQTT_HOSTNAME, REDIS_HOSTNAME};
use crate::io::InputAggregation;
use crate::io::config::{MsgTypeMapping, TopicMapping};
use crate::io::mqtt::MqttInputBackend;

use crate::stream_utils::Fanout;
use crate::{self as tc, OutputStream, Value};
use crate::{InputStream, VarName};

#[derive(Debug, Clone)]
enum InputFactoryKind {
    File {
        path: String,
    },
    Ros {
        topics: TopicMapping,
        types: MsgTypeMapping,
        executor: Rc<LocalExecutor<'static>>,
    },
    Mqtt {
        topics: Option<TopicMapping>,
        port: Option<u16>,
        backend: MqttInputBackend,
    },
    Redis {
        topics: Option<TopicMapping>,
        port: Option<u16>,
    },
    /// Manually receives results based on the Fanout channel, and forwards them to any
    /// constructed input streams. Useful for testing.
    Manual(BTreeMap<VarName, Rc<Fanout<Value>>>),
}

impl InputFactoryKind {
    fn produces_independent_events(&self) -> bool {
        matches!(
            self,
            Self::Ros { .. } | Self::Mqtt { .. } | Self::Redis { .. }
        )
    }
}

/// Rebuildable input configuration for reconfigurable runtimes.
///
/// Normal runtimes should receive a constructed [`InputStream`] directly.
#[derive(Clone, Debug)]
pub struct InputStreamFactory {
    kind: InputFactoryKind,
    input_aggregation: Option<InputAggregation>,
}

impl InputStreamFactory {
    fn new(kind: InputFactoryKind) -> Self {
        Self {
            kind,
            input_aggregation: None,
        }
    }

    /// Aggregate independent events before delivering them to a runtime.
    ///
    /// File and manual sources already define simultaneous input ticks, so
    /// changing their boundaries is rejected rather than silently ignored.
    pub fn input_aggregation(mut self, aggregation: InputAggregation) -> anyhow::Result<Self> {
        if aggregation.is_passthrough() {
            self.input_aggregation = None;
            return Ok(self);
        }
        anyhow::ensure!(
            self.kind.produces_independent_events(),
            "input aggregation requires an independent-event source; file and manual inputs define atomic ticks"
        );
        self.input_aggregation = Some(aggregation);
        Ok(self)
    }

    pub fn file(path: String) -> Self {
        Self::new(InputFactoryKind::File { path })
    }

    pub fn ros(
        topic_mapping: TopicMapping,
        msg_type_mapping: MsgTypeMapping,
        executor: Rc<LocalExecutor<'static>>,
    ) -> Self {
        Self::new(InputFactoryKind::Ros {
            topics: topic_mapping,
            types: msg_type_mapping,
            executor,
        })
    }

    pub fn mqtt(topics: Option<TopicMapping>, port: Option<u16>) -> Self {
        Self::mqtt_with_backend(topics, port, MqttInputBackend::default())
    }

    pub fn mqtt_with_backend(
        topics: Option<TopicMapping>,
        port: Option<u16>,
        backend: MqttInputBackend,
    ) -> Self {
        Self::new(InputFactoryKind::Mqtt {
            topics,
            port,
            backend,
        })
    }

    pub fn redis(topics: Option<TopicMapping>, port: Option<u16>) -> Self {
        Self::new(InputFactoryKind::Redis { topics, port })
    }

    pub(crate) fn manual(fanout: BTreeMap<VarName, Rc<Fanout<Value>>>) -> Self {
        Self::new(InputFactoryKind::Manual(fanout))
    }

    pub(crate) fn ros_mappings(&self) -> Option<(&TopicMapping, &MsgTypeMapping)> {
        match &self.kind {
            InputFactoryKind::Ros { topics, types, .. } => Some((topics, types)),
            _ => None,
        }
    }

    pub(crate) fn ensure_reconfigurable(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !matches!(self.kind, InputFactoryKind::File { .. }),
            "file input cannot be used by a reconfigurable runtime"
        );
        Ok(())
    }

    pub(crate) fn add_reconfiguration_input(
        &mut self,
        reconf_var: VarName,
        model_vars: BTreeSet<VarName>,
    ) {
        match &mut self.kind {
            InputFactoryKind::File { .. } => {
                // Reconfigurable runtimes reject file factories when opening
                // the stream. Keep injection side-effect free so that the
                // incompatibility is returned as a runtime error, not a panic.
            }
            InputFactoryKind::Manual(_) => {}
            InputFactoryKind::Mqtt { topics, .. } => {
                let mut configured_topics = topics.take().unwrap_or_else(|| {
                    model_vars
                        .into_iter()
                        .map(|var| (var.clone(), var.to_string()))
                        .collect()
                });
                configured_topics.insert(reconf_var.clone(), reconf_var.to_string());
                *topics = Some(configured_topics);
            }
            InputFactoryKind::Redis { topics, .. } => {
                let mut configured_topics = topics.take().unwrap_or_else(|| {
                    model_vars
                        .into_iter()
                        .map(|var| (var.clone(), var.to_string()))
                        .collect()
                });
                configured_topics.insert(reconf_var.clone(), reconf_var.to_string());
                *topics = Some(configured_topics);
            }
            InputFactoryKind::Ros { topics, types, .. } => {
                topics.insert(reconf_var.clone(), format!("/{}", reconf_var.name()));
                types.insert(reconf_var, "String".into());
            }
        }
    }

    pub(crate) fn reconfigure(
        &mut self,
        vars: BTreeSet<VarName>,
        known_topics: &TopicMapping,
        known_types: &MsgTypeMapping,
    ) -> anyhow::Result<()> {
        match &mut self.kind {
            InputFactoryKind::Mqtt { topics, .. } => {
                *topics = Some(Self::merge_topic_mappings(
                    &vars,
                    known_topics,
                    topics.as_ref(),
                ));
            }
            InputFactoryKind::Redis { topics, .. } => {
                *topics = Some(Self::merge_topic_mappings(
                    &vars,
                    known_topics,
                    topics.as_ref(),
                ));
            }
            InputFactoryKind::Ros { topics, types, .. } => {
                let (new_topics, new_types) =
                    Self::merge_ros_mappings(&vars, known_topics, topics, known_types, types)?;
                *topics = new_topics;
                *types = new_types;
            }
            InputFactoryKind::File { .. } => {
                anyhow::bail!(
                    "reconfiguration of file inputs is not supported because rebuilding would restart the input file"
                );
            }
            InputFactoryKind::Manual(_) => {}
        }
        Ok(())
    }

    fn merge_topic_mappings(
        vars: &BTreeSet<VarName>,
        known_topics: &TopicMapping,
        configured_topics: Option<&TopicMapping>,
    ) -> TopicMapping {
        let mut merged = configured_topics.cloned().unwrap_or_default();
        merged.extend(known_topics.clone());
        for var in vars {
            merged.entry(var.clone()).or_insert_with(|| var.to_string());
        }
        merged
    }

    fn merge_ros_mappings(
        vars: &BTreeSet<VarName>,
        known_topics: &TopicMapping,
        configured_topics: &TopicMapping,
        known_types: &MsgTypeMapping,
        configured_types: &MsgTypeMapping,
    ) -> anyhow::Result<(TopicMapping, MsgTypeMapping)> {
        let types: MsgTypeMapping = vars
            .iter()
            .filter_map(|var| {
                known_types
                    .get(var)
                    .cloned()
                    .or_else(|| configured_types.get(var).cloned())
                    .map(|ty| (var.clone(), ty))
            })
            .collect();
        let missing = vars
            .iter()
            .filter(|var| !types.contains_key(*var))
            .map(VarName::name)
            .collect::<Vec<_>>();
        anyhow::ensure!(
            missing.is_empty(),
            "Missing type_info for vars: {missing:?}."
        );

        let topics = vars
            .iter()
            .map(|var| {
                let topic = known_topics
                    .get(var)
                    .cloned()
                    .or_else(|| configured_topics.get(var).cloned())
                    .unwrap_or_else(|| format!("/{}", var.name()));
                (var.clone(), topic)
            })
            .collect();
        Ok((topics, types))
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

    /// Open a fresh stream for the selected model variables.
    pub async fn open(&self, input_vars: BTreeSet<VarName>) -> anyhow::Result<InputStream<Value>> {
        let _open = debug_span!("open input stream").entered();
        let stream = match &self.kind {
            InputFactoryKind::File { path } => {
                let data = tc::parse_file(tc::lang::untimed_input::untimed_input_file, path)
                    .await
                    .map_err(|error| {
                        anyhow::anyhow!(error).context("Input file could not be parsed")
                    })?;
                tc::io::file::input_stream(data, input_vars)
            }
            InputFactoryKind::Ros {
                topics: _topic_mapping,
                types: _msg_type_mapping,
                executor: _executor,
            } => {
                #[cfg(feature = "ros")]
                {
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
                        _topic_mapping.clone(),
                        _msg_type_mapping.clone(),
                    )?;
                    let input_mapping: BTreeMap<_, _> =
                        filter_ros_mapping(input_mapping_raw, &input_vars)?;

                    tc::io::ros::input_stream(_executor.clone(), input_mapping)?
                }
                #[cfg(not(feature = "ros"))]
                {
                    anyhow::bail!("ROS support not enabled")
                }
            }
            InputFactoryKind::Mqtt {
                topics,
                port,
                backend,
            } => {
                let var_topics: BTreeMap<_, _> = match topics {
                    Some(topics) => Self::filter_cli_topics(topics.clone(), &input_vars)?,
                    None => input_vars
                        .iter()
                        .map(|topic| (topic.clone(), format!("{}", topic)))
                        .collect(),
                };
                tc::io::mqtt::input_stream(*backend, MQTT_HOSTNAME, *port, var_topics, u32::MAX)
                    .await?
            }
            InputFactoryKind::Redis { topics, port } => {
                let var_topics: BTreeMap<_, _> = match topics {
                    Some(topics) => Self::filter_cli_topics(topics.clone(), &input_vars)?,
                    None => input_vars
                        .iter()
                        .map(|topic| (topic.clone(), format!("{}", topic)))
                        .collect(),
                };
                tc::io::redis::input_stream(REDIS_HOSTNAME, *port, var_topics).await?
            }
            InputFactoryKind::Manual(fanout) => {
                anyhow::ensure!(
                    fanout
                        .keys()
                        .cloned()
                        .collect::<BTreeSet<_>>()
                        .is_superset(&input_vars),
                    "Fanout keys must contain all input variables from the spec"
                );
                let mut rxs = BTreeMap::new();
                for (var, fanout) in fanout {
                    if !input_vars.contains(var) {
                        continue;
                    }
                    // Important that this happens outside stream!
                    let mut sub_rx = fanout.subscribe();
                    let rx: OutputStream<Value> = Box::pin(stream! {
                        while let Some(val) = sub_rx.recv().await {
                            yield val;
                        }
                    });
                    rxs.insert(var.clone(), rx);
                }

                tc::io::testing::from_streams(rxs)
            }
        };
        if let Some(aggregation) = self.input_aggregation {
            return Ok(crate::io::aggregation::aggregate_input_stream(
                stream,
                aggregation,
            ));
        }
        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lang::dsrv::parser::dsrv_specification;
    use crate::{
        Specification, Value, VarName, async_test, dsrv_fixtures::spec_simple_add_monitor,
    };
    use futures::StreamExt;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::rc::Rc;
    use tc_testutils::streams::with_timeout;

    #[test]
    fn file_input_is_rejected_for_reconfiguration() {
        let error = InputStreamFactory::file("trace.input".into())
            .ensure_reconfigurable()
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "file input cannot be used by a reconfigurable runtime"
        );
    }

    #[test]
    fn atomic_input_is_rejected_for_aggregation() {
        let error = InputStreamFactory::file("trace.input".into())
            .input_aggregation(InputAggregation::new(
                std::time::Duration::from_millis(1),
                crate::io::AggregationSemantics::PreserveTicks,
            ))
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "input aggregation requires an independent-event source; file and manual inputs define atomic ticks"
        );
    }

    #[test]
    fn mqtt_input_uses_rumqttc_by_default() {
        assert!(matches!(
            InputStreamFactory::mqtt(None, None).kind,
            InputFactoryKind::Mqtt {
                backend: MqttInputBackend::Rumqttc,
                ..
            }
        ));
    }

    #[apply(async_test)]
    async fn test_manual_input_factory_regular(ex: Rc<LocalExecutor<'static>>) {
        // Tests that the manual input stream opened by the factory correctly receives inputs through
        // the provided channel.
        // (Notice that we are transmitting through the opened manual input stream even though we do not
        // call `sender_channel` directly.)
        let model = dsrv_specification(&mut spec_simple_add_monitor()).unwrap();

        let (tx_x, fx) = Fanout::new();
        let (tx_y, fy) = Fanout::new();
        let fanouts = BTreeMap::from([(VarName::new("x"), fx), (VarName::new("y"), fy)]);
        let input_vars = model.input_vars();
        let input = InputStreamFactory::manual(fanouts)
            .open(input_vars)
            .await
            .unwrap();
        let mut ticks = crate::into_tick_stream(input);

        tx_x.send(Value::Int(1)).await;
        tx_y.send(Value::Int(3)).await;

        let first = with_timeout(ticks.next(), 1, "tick_1")
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            first,
            vec![
                crate::InputEvent::new(VarName::new("x"), Value::Int(1)),
                crate::InputEvent::new(VarName::new("y"), Value::Int(3)),
            ]
        );

        tx_x.send(Value::Int(2)).await;
        tx_y.send(Value::Int(4)).await;

        let second = with_timeout(ticks.next(), 1, "tick_2")
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            second,
            vec![
                crate::InputEvent::new(VarName::new("x"), Value::Int(2)),
                crate::InputEvent::new(VarName::new("y"), Value::Int(4)),
            ]
        );

        drop(tx_x);
        drop(tx_y);

        assert!(
            with_timeout(ticks.next(), 1, "ticks_end")
                .await
                .expect("step stream should end")
                .is_none()
        )
    }

    #[apply(async_test)]
    async fn test_manual_input_factory_multi_conc(ex: Rc<LocalExecutor<'static>>) {
        // Tests that two manual input streams opened from cloned factories can each
        // receive values through the same user channel.
        let model = dsrv_specification(&mut spec_simple_add_monitor()).unwrap();

        let (tx_x, fx) = Fanout::new();
        let (tx_y, fy) = Fanout::new();
        let fanouts = BTreeMap::from([(VarName::new("x"), fx), (VarName::new("y"), fy)]);
        let input_vars = model.input_vars();
        let factory1 = InputStreamFactory::manual(fanouts);
        let factory2 = factory1.clone();

        // Open both streams first
        let mut ticks1 = crate::into_tick_stream(factory1.open(input_vars.clone()).await.unwrap());
        let mut ticks2 = crate::into_tick_stream(factory2.open(input_vars).await.unwrap());

        // Send one pair — both streams should receive the same values
        tx_x.send(Value::Int(10)).await;
        tx_y.send(Value::Int(20)).await;

        let expected = vec![
            crate::InputEvent::new(VarName::new("x"), Value::Int(10)),
            crate::InputEvent::new(VarName::new("y"), Value::Int(20)),
        ];
        assert_eq!(
            with_timeout(ticks1.next(), 1, "stream1 tick")
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            expected
        );
        assert_eq!(
            with_timeout(ticks2.next(), 1, "stream2 tick")
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            expected
        );
    }

    #[apply(async_test)]
    async fn test_manual_input_factory_sequential_rebuild(ex: Rc<LocalExecutor<'static>>) {
        // Tests that after dropping one stream, a new stream opened from a
        // clone still receives values through the same user channel.
        let model = dsrv_specification(&mut "in x\nout z\nz = x").unwrap();

        let (tx_x, fx) = Fanout::new();
        let (_tx_y, fy) = Fanout::new();
        let fanouts = BTreeMap::from([(VarName::new("x"), fx), (VarName::new("y"), fy)]);
        let input_vars = model.input_vars();
        let factory1 = InputStreamFactory::manual(fanouts);
        let factory2 = factory1.clone();

        // Open and use the first stream
        {
            let mut ticks =
                crate::into_tick_stream(factory1.open(input_vars.clone()).await.unwrap());

            tx_x.send(Value::Int(100)).await;
            assert_eq!(
                with_timeout(ticks.next(), 1, "stream1 tick")
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                vec![crate::InputEvent::new(VarName::new("x"), Value::Int(100))]
            );
            // stream1 dropped here
        }

        // Open a second stream from the clone — same channel should still work
        let mut ticks = crate::into_tick_stream(factory2.open(input_vars).await.unwrap());

        tx_x.send(Value::Int(200)).await;
        assert_eq!(
            with_timeout(ticks.next(), 1, "stream2 tick")
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            vec![crate::InputEvent::new(VarName::new("x"), Value::Int(200))]
        );
    }
}
