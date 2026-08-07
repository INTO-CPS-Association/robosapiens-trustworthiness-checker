use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use anyhow::Context;
use async_stream::stream;
use futures::StreamExt;
use futures::future::LocalBoxFuture;
use smol::LocalExecutor;
use tracing::{debug_span, warn};

use crate::core::{MQTT_HOSTNAME, REDIS_HOSTNAME};
use crate::io::config::{MsgTypeMapping, TopicMapping};
use crate::io::mqtt::MqttInputBackend;
use crate::io::{AggregationSemantics, InputAggregation};

use crate::core::{FileInputValue, RosStreamValue};

use crate::stream_utils::Fanout;
use crate::{self as tc, InputStream, OutputStream, VarName};

#[derive(Debug, Clone)]
enum InputFactoryKind<V = crate::Value> {
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
    Manual(BTreeMap<VarName, Rc<Fanout<V>>>),
    /// Routes disjoint sets of input variables to independently configured
    /// child providers, merging their streams as they arrive.
    Composite(Vec<AssignedInputProvider<V>>),
}

impl<V> InputFactoryKind<V> {
    /// Stable human-readable provider kind used in diagnostics.
    fn description(&self) -> &'static str {
        match self {
            Self::File { .. } => "file",
            Self::Ros { .. } => "ROS",
            Self::Mqtt { .. } => "MQTT",
            Self::Redis { .. } => "Redis",
            Self::Manual(_) => "manual",
            Self::Composite(_) => "composite",
        }
    }

    fn produces_independent_events(&self) -> bool {
        match self {
            Self::Ros { .. } | Self::Mqtt { .. } | Self::Redis { .. } => true,
            Self::File { .. } | Self::Manual(_) => false,
            Self::Composite(providers) => providers
                .iter()
                .all(|provider| provider.provider.produces_independent_events()),
        }
    }
}

/// Render variable names in a stable alphabetical order for diagnostics.
/// [`VarName`] is interned, so its own ordering depends on interning order.
fn sorted_names<'a>(vars: impl Iterator<Item = &'a VarName>) -> String {
    let mut names = vars.map(VarName::name).collect::<Vec<_>>();
    names.sort();
    names.join(", ")
}

/// An [`InputStreamFactory`] together with the input variables it is
/// responsible for inside a composite input provider.
///
/// Construct one with [`InputStreamFactory::for_variables`].
#[derive(Clone, Debug)]
pub struct AssignedInputProvider<V = crate::Value> {
    provider: InputStreamFactory<V>,
    variables: BTreeSet<VarName>,
}

impl<V> AssignedInputProvider<V> {
    /// Identify this child in diagnostics, for example
    /// `MQTT input provider for variables {battery, temperature}`.
    fn describe(&self) -> String {
        format!(
            "{} input provider for variables {{{}}}",
            self.provider.kind.description(),
            sorted_names(self.variables.iter())
        )
    }

    /// The assigned variables which were also requested by the runtime.
    fn requested_variables(&self, requested: &BTreeSet<VarName>) -> BTreeSet<VarName> {
        self.variables.intersection(requested).cloned().collect()
    }
}

/// Rebuildable input configuration for reconfigurable runtimes.
///
/// Normal runtimes should receive a constructed [`InputStream`] directly.
#[derive(Clone, Debug)]
pub struct InputStreamFactory<V = crate::Value> {
    kind: InputFactoryKind<V>,
    input_aggregation: Option<InputAggregation>,
}

impl<V> InputStreamFactory<V> {
    fn new(kind: InputFactoryKind<V>) -> Self {
        Self {
            kind,
            input_aggregation: None,
        }
    }

    /// Whether the streams opened by this factory deliver independent events
    /// rather than atomic simultaneous ticks.
    ///
    /// This accounts for the factory's own aggregation, which may coalesce
    /// independent events into atomic steps, and recurses into composites.
    fn produces_independent_events(&self) -> bool {
        if self
            .input_aggregation
            .is_some_and(|aggregation| aggregation.semantics != AggregationSemantics::PreserveTicks)
        {
            return false;
        }
        self.kind.produces_independent_events()
    }

    /// Aggregate independent events before delivering them to a runtime.
    ///
    /// File and manual sources already define simultaneous input ticks, so
    /// changing their boundaries is rejected rather than silently ignored. A
    /// composite may be aggregated only when every child produces independent
    /// events, in which case events from separate providers may be coalesced
    /// by the same aggregation window.
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

    pub(crate) fn manual(fanout: BTreeMap<VarName, Rc<Fanout<V>>>) -> Self {
        Self::new(InputFactoryKind::Manual(fanout))
    }

    /// Assign this provider to a set of input variables so that it can be
    /// used as a child of [`InputStreamFactory::composite`].
    pub fn for_variables<I, T>(self, variables: I) -> AssignedInputProvider<V>
    where
        I: IntoIterator<Item = T>,
        T: Into<VarName>,
    {
        AssignedInputProvider {
            provider: self,
            variables: variables.into_iter().map(Into::into).collect(),
        }
    }

    /// Combine several providers, each serving a disjoint set of input
    /// variables.
    ///
    /// ```ignore
    /// let input = InputStreamFactory::composite([
    ///     InputStreamFactory::ros(ros_topics, ros_types, executor)
    ///         .for_variables(["position", "velocity"]),
    ///     InputStreamFactory::mqtt(Some(mqtt_topics), mqtt_port)
    ///         .for_variables(["battery", "temperature"]),
    /// ])?;
    /// ```
    ///
    /// Every child must be assigned at least one variable, and no variable may
    /// be assigned to more than one child. Composites may be nested.
    pub fn composite(
        providers: impl IntoIterator<Item = AssignedInputProvider<V>>,
    ) -> anyhow::Result<Self> {
        let providers = providers.into_iter().collect::<Vec<_>>();
        anyhow::ensure!(
            !providers.is_empty(),
            "composite input requires at least one input provider"
        );

        let mut assignments: BTreeMap<VarName, usize> = BTreeMap::new();
        for (index, provider) in providers.iter().enumerate() {
            anyhow::ensure!(
                !provider.variables.is_empty(),
                "{} input provider of a composite input has no assigned input variables",
                provider.provider.kind.description()
            );
            for var in &provider.variables {
                if let Some(previous) = assignments.insert(var.clone(), index) {
                    anyhow::bail!(
                        "input variable `{var}` is assigned to more than one composite input provider: {} and {}",
                        providers[previous].describe(),
                        provider.describe()
                    );
                }
            }
        }

        Ok(Self::new(InputFactoryKind::Composite(providers)))
    }

    /// Collect the ROS topic and message type mappings reachable from this
    /// factory, restricted to the variables routed to each ROS provider.
    pub(crate) fn ros_mappings(&self) -> (TopicMapping, MsgTypeMapping) {
        let mut topics = TopicMapping::new();
        let mut types = MsgTypeMapping::new();
        self.collect_ros_mappings(None, &mut topics, &mut types);
        (topics, types)
    }

    /// Collect ROS mappings, keeping only entries for `assigned` variables when
    /// this factory is reached through a composite assignment.
    fn collect_ros_mappings(
        &self,
        assigned: Option<&BTreeSet<VarName>>,
        topics: &mut TopicMapping,
        types: &mut MsgTypeMapping,
    ) {
        match &self.kind {
            InputFactoryKind::Ros {
                topics: ros_topics,
                types: ros_types,
                ..
            } => {
                let routed = |var: &VarName| assigned.is_none_or(|assigned| assigned.contains(var));
                topics.extend(
                    ros_topics
                        .iter()
                        .filter(|(var, _)| routed(var))
                        .map(|(var, topic)| (var.clone(), topic.clone())),
                );
                types.extend(
                    ros_types
                        .iter()
                        .filter(|(var, _)| routed(var))
                        .map(|(var, ty)| (var.clone(), ty.clone())),
                );
            }
            InputFactoryKind::Composite(providers) => {
                for provider in providers {
                    let routed = match assigned {
                        Some(assigned) => provider.requested_variables(assigned),
                        None => provider.variables.clone(),
                    };
                    provider
                        .provider
                        .collect_ros_mappings(Some(&routed), topics, types);
                }
            }
            InputFactoryKind::File { .. }
            | InputFactoryKind::Mqtt { .. }
            | InputFactoryKind::Redis { .. }
            | InputFactoryKind::Manual(_) => {}
        }
    }

    pub(crate) fn ensure_reconfigurable(&self) -> anyhow::Result<()> {
        match &self.kind {
            InputFactoryKind::File { .. } => {
                anyhow::bail!("file input cannot be used by a reconfigurable runtime")
            }
            InputFactoryKind::Composite(providers) => {
                for provider in providers {
                    provider
                        .provider
                        .ensure_reconfigurable()
                        .with_context(|| provider.describe())?;
                }
            }
            InputFactoryKind::Ros { .. }
            | InputFactoryKind::Mqtt { .. }
            | InputFactoryKind::Redis { .. }
            | InputFactoryKind::Manual(_) => {}
        }
        Ok(())
    }

    /// Check that every requested variable is assigned to a child provider.
    fn ensure_assigned(
        providers: &[AssignedInputProvider<V>],
        requested: &BTreeSet<VarName>,
    ) -> anyhow::Result<()> {
        let missing = requested
            .iter()
            .filter(|var| {
                !providers
                    .iter()
                    .any(|provider| provider.variables.contains(*var))
            })
            .collect::<Vec<_>>();
        anyhow::ensure!(
            missing.is_empty(),
            "composite input has no provider assigned to the input variables {{{}}}; assigned providers are: {}",
            sorted_names(missing.into_iter()),
            providers
                .iter()
                .map(AssignedInputProvider::describe)
                .collect::<Vec<_>>()
                .join("; ")
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
            InputFactoryKind::Composite(providers) => {
                // Only the provider which was explicitly assigned the
                // reconfiguration variable may serve it. When it is unassigned
                // the missing assignment is reported by the coverage check
                // performed when the composite is opened.
                if let Some(provider) = providers
                    .iter_mut()
                    .find(|provider| provider.variables.contains(&reconf_var))
                {
                    let model_vars = provider.requested_variables(&model_vars);
                    provider
                        .provider
                        .add_reconfiguration_input(reconf_var, model_vars);
                }
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
            InputFactoryKind::Composite(providers) => {
                Self::ensure_assigned(providers, &vars)?;
                for provider in providers.iter_mut() {
                    let provider_vars = provider.requested_variables(&vars);
                    if provider_vars.is_empty() {
                        // Leave providers with no remaining variables
                        // untouched so their configuration survives a later
                        // reconfiguration which uses them again.
                        continue;
                    }
                    let description = provider.describe();
                    provider
                        .provider
                        .reconfigure(provider_vars, known_topics, known_types)
                        .with_context(|| description)?;
                }
            }
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

    /// Attach provider context to the errors of a composite child stream.
    fn labelled_stream(stream: InputStream<V>, description: String) -> InputStream<V>
    where
        V: 'static,
    {
        Box::pin(stream.map(move |batch| batch.with_context(|| format!("{description} failed"))))
    }

    /// Open a fresh stream for the selected model variables.
    pub async fn open(&self, input_vars: BTreeSet<VarName>) -> anyhow::Result<InputStream<V>>
    where
        V: FileInputValue + RosStreamValue,
    {
        let _open = debug_span!("open input stream").entered();
        let stream: InputStream<V> = match &self.kind {
            InputFactoryKind::File { path } => {
                let packed_input = tc::parse_file(
                    |contents| {
                        tc::lang::untimed_input::parser::packed_untimed_input::<V>(
                            contents, input_vars,
                        )
                        .map_err(|error| error.to_string())
                    },
                    path,
                )
                .await
                .map_err(|error| {
                    anyhow::anyhow!(error).context("Input file could not be parsed")
                })?;
                tc::io::file::packed_input_stream(packed_input)
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
                        let missing_keys: Vec<_> = input_vars.difference(&keys).cloned().collect();
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
                        if !ignored_mapping.is_empty() {
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
                    let input_mapping = filter_ros_mapping(input_mapping_raw, &input_vars)?
                        .into_iter()
                        .map(|(variable, data)| {
                            let msg_type =
                                crate::io::ros::ros_topic_stream_mapping::ros_msg_type_to_string(
                                    data.msg_type,
                                )?;
                            Ok((variable, (data.topic, msg_type)))
                        })
                        .collect::<anyhow::Result<BTreeMap<_, _>>>()?;

                    V::ros_input_stream(_executor.clone(), input_mapping)?
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
                tc::io::mqtt::input_stream::<V>(
                    *backend,
                    MQTT_HOSTNAME,
                    *port,
                    var_topics,
                    u32::MAX,
                )
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
                tc::io::redis::input_stream::<V>(REDIS_HOSTNAME, *port, var_topics).await?
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
                    let rx: OutputStream<V> = Box::pin(stream! {
                        while let Some(val) = sub_rx.recv().await {
                            yield val;
                        }
                    });
                    rxs.insert(var.clone(), rx);
                }

                tc::io::testing::from_streams(rxs)
            }
            InputFactoryKind::Composite(providers) => {
                Self::ensure_assigned(providers, &input_vars)?;
                let mut streams = Vec::new();
                for provider in providers {
                    let provider_vars = provider.requested_variables(&input_vars);
                    if provider_vars.is_empty() {
                        // Nothing is requested from this provider, so it is
                        // never opened.
                        continue;
                    }
                    // Boxing breaks the recursion in this `async fn`.
                    let opened: LocalBoxFuture<'_, anyhow::Result<InputStream<V>>> =
                        Box::pin(provider.provider.open(provider_vars));
                    let stream = opened
                        .await
                        .with_context(|| format!("{} could not be opened", provider.describe()))?;
                    streams.push(Self::labelled_stream(stream, provider.describe()));
                }

                // Each child keeps its own batches, and hence its own logical
                // tick boundaries; only their arrival order is interleaved.
                let mut merged = futures::stream::select_all(streams);
                Box::pin(async_stream::try_stream! {
                    while let Some(batch) = merged.next().await {
                        yield batch?;
                    }
                })
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
    use crate::stream_utils::FanoutSender;
    use crate::{
        DsrvSpecification, InputBatch, InputEvent, Value, VarName, async_test,
        dsrv_fixtures::spec_simple_add_monitor,
    };
    use futures::StreamExt;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::rc::Rc;
    use std::time::Duration;
    use tc_testutils::streams::with_timeout;

    fn vars(names: &[&str]) -> BTreeSet<VarName> {
        names.iter().map(|name| VarName::new(name)).collect()
    }

    fn event(var: &str, value: i64) -> InputEvent<Value> {
        InputEvent::new(VarName::new(var), Value::Int(value))
    }

    /// A manual provider together with one sender per variable, in the order
    /// the variables were given.
    fn manual_provider(names: &[&str]) -> (Vec<FanoutSender<Value>>, InputStreamFactory<Value>) {
        let mut senders = Vec::new();
        let mut fanouts = BTreeMap::new();
        for name in names {
            let (sender, fanout) = Fanout::new();
            senders.push(sender);
            fanouts.insert(VarName::new(name), fanout);
        }
        (senders, InputStreamFactory::manual(fanouts))
    }

    fn children(factory: &InputStreamFactory<Value>) -> &[AssignedInputProvider<Value>] {
        match &factory.kind {
            InputFactoryKind::Composite(providers) => providers,
            kind => panic!("expected a composite input, found {}", kind.description()),
        }
    }

    fn mqtt_topics(factory: &InputStreamFactory<Value>) -> Option<&TopicMapping> {
        match &factory.kind {
            InputFactoryKind::Mqtt { topics, .. } | InputFactoryKind::Redis { topics, .. } => {
                topics.as_ref()
            }
            kind => panic!(
                "expected an MQTT or Redis input, found {}",
                kind.description()
            ),
        }
    }

    /// `InputStream` is not `Debug`, so `unwrap_err` cannot be used directly.
    fn open_error(opened: anyhow::Result<InputStream<Value>>) -> anyhow::Error {
        match opened {
            Ok(_) => panic!("opening the input stream should have failed"),
            Err(error) => error,
        }
    }

    fn ticks_of(batch: &InputBatch<Value>) -> Vec<Vec<InputEvent<Value>>> {
        batch.ticks().map(|tick| tick.to_events()).collect()
    }

    fn preserving_aggregation() -> InputAggregation {
        InputAggregation::new(
            Duration::from_millis(1),
            AggregationSemantics::PreserveTicks,
        )
    }

    fn coalescing_aggregation() -> InputAggregation {
        InputAggregation::new(
            Duration::from_millis(1),
            AggregationSemantics::CoalesceToAtomicStep,
        )
    }

    #[test]
    fn file_input_is_rejected_for_reconfiguration() {
        let error = InputStreamFactory::<Value>::file("trace.input".into())
            .ensure_reconfigurable()
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "file input cannot be used by a reconfigurable runtime"
        );
    }

    #[test]
    fn atomic_input_is_rejected_for_aggregation() {
        let error = InputStreamFactory::<Value>::file("trace.input".into())
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
            InputStreamFactory::<Value>::mqtt(None, None).kind,
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
        let model = (spec_simple_add_monitor())
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");

        let (tx_x, fx) = Fanout::new();
        let (tx_y, fy) = Fanout::new();
        let fanouts = BTreeMap::from([(VarName::new("x"), fx), (VarName::new("y"), fy)]);
        let input_vars = model.input_vars();
        let input = InputStreamFactory::manual(fanouts)
            .open(input_vars.clone())
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
        let model = (spec_simple_add_monitor())
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");

        let (tx_x, fx) = Fanout::new();
        let (tx_y, fy) = Fanout::new();
        let fanouts = BTreeMap::from([(VarName::new("x"), fx), (VarName::new("y"), fy)]);
        let input_vars = model.input_vars();
        let factory1 = InputStreamFactory::manual(fanouts);
        let factory2 = factory1.clone();

        // Open both streams first
        let mut ticks1 = crate::into_tick_stream(factory1.open(input_vars.clone()).await.unwrap());
        let mut ticks2 = crate::into_tick_stream(factory2.open(input_vars.clone()).await.unwrap());

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
        let model = ("in x\nout z\nz = x")
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");

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
        let mut ticks = crate::into_tick_stream(factory2.open(input_vars.clone()).await.unwrap());

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

    #[test]
    fn composite_requires_at_least_one_provider() {
        let error = InputStreamFactory::<Value>::composite([]).unwrap_err();
        assert_eq!(
            error.to_string(),
            "composite input requires at least one input provider"
        );
    }

    #[test]
    fn composite_requires_every_provider_to_be_assigned_variables() {
        let error = InputStreamFactory::composite([
            InputStreamFactory::<Value>::mqtt(None, None).for_variables(["x"]),
            InputStreamFactory::<Value>::redis(None, None).for_variables(Vec::<VarName>::new()),
        ])
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Redis input provider of a composite input has no assigned input variables"
        );
    }

    #[test]
    fn composite_rejects_variables_assigned_to_several_providers() {
        let error = InputStreamFactory::composite([
            InputStreamFactory::<Value>::mqtt(None, None).for_variables(["a", "b"]),
            InputStreamFactory::<Value>::redis(None, None).for_variables([VarName::new("b")]),
        ])
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "input variable `b` is assigned to more than one composite input provider: \
             MQTT input provider for variables {a, b} and Redis input provider for variables {b}"
        );
    }

    #[apply(async_test)]
    async fn composite_with_one_provider_delivers_its_input(ex: Rc<LocalExecutor<'static>>) {
        let (senders, provider) = manual_provider(&["x"]);
        let factory = InputStreamFactory::composite([provider.for_variables(["x"])]).unwrap();
        let mut ticks = crate::into_tick_stream(factory.open(vars(&["x"])).await.unwrap());

        senders[0].send(Value::Int(1)).await;
        assert_eq!(
            with_timeout(ticks.next(), 1, "tick")
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            vec![event("x", 1)]
        );
    }

    #[apply(async_test)]
    async fn composite_reports_requested_variables_without_a_provider(
        ex: Rc<LocalExecutor<'static>>,
    ) {
        let factory = InputStreamFactory::composite([
            InputStreamFactory::<Value>::mqtt(None, None).for_variables(["x"]),
            InputStreamFactory::<Value>::redis(None, None).for_variables(["y"]),
        ])
        .unwrap();

        let error = open_error(factory.open(vars(&["x", "y", "z", "w"])).await);
        assert_eq!(
            error.to_string(),
            "composite input has no provider assigned to the input variables {w, z}; \
             assigned providers are: MQTT input provider for variables {x}; \
             Redis input provider for variables {y}"
        );
    }

    #[apply(async_test)]
    async fn composite_does_not_open_providers_without_requested_variables(
        ex: Rc<LocalExecutor<'static>>,
    ) {
        // The file provider would fail to open, so opening succeeds only if
        // providers with an empty intersection are skipped.
        let (senders, manual) = manual_provider(&["x"]);
        let factory = InputStreamFactory::composite([
            manual.for_variables(["x"]),
            InputStreamFactory::<Value>::file("missing.input".into()).for_variables(["y"]),
        ])
        .unwrap();

        let mut ticks = crate::into_tick_stream(factory.open(vars(&["x"])).await.unwrap());
        senders[0].send(Value::Int(1)).await;
        assert_eq!(
            with_timeout(ticks.next(), 1, "tick")
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            vec![event("x", 1)]
        );

        assert!(factory.open(vars(&["x", "y"])).await.is_err());
    }

    #[apply(async_test)]
    async fn composite_opens_each_provider_with_its_requested_subset(
        ex: Rc<LocalExecutor<'static>>,
    ) {
        // The manual provider waits for one value per opened variable, so `y`
        // must not be requested from the first provider.
        let (first, provider_xy) = manual_provider(&["x", "y"]);
        let (second, provider_z) = manual_provider(&["z"]);
        let factory = InputStreamFactory::composite([
            provider_xy.for_variables(["x", "y"]),
            provider_z.for_variables(["z"]),
        ])
        .unwrap();
        let mut ticks = crate::into_tick_stream(factory.open(vars(&["x", "z"])).await.unwrap());

        first[0].send(Value::Int(1)).await;
        second[0].send(Value::Int(2)).await;

        let mut received = Vec::new();
        for index in 0..2 {
            received.push(
                with_timeout(ticks.next(), 1, &format!("tick_{index}"))
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
            );
        }
        received.sort_by_key(|tick| tick[0].var.name());
        assert_eq!(received, [vec![event("x", 1)], vec![event("z", 2)]]);
    }

    #[apply(async_test)]
    async fn composite_merges_provider_batches_without_changing_tick_boundaries(
        ex: Rc<LocalExecutor<'static>>,
    ) {
        let (first, provider_xy) = manual_provider(&["x", "y"]);
        let (second, provider_z) = manual_provider(&["z"]);
        let factory = InputStreamFactory::composite([
            provider_xy.for_variables(["x", "y"]),
            provider_z.for_variables(["z"]),
        ])
        .unwrap();
        let mut batches = factory.open(vars(&["x", "y", "z"])).await.unwrap();

        first[0].send(Value::Int(1)).await;
        first[1].send(Value::Int(2)).await;
        let batch = with_timeout(batches.next(), 1, "simultaneous batch")
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        // The first provider still delivers one simultaneous tick of width 2.
        assert_eq!(ticks_of(&batch), [vec![event("x", 1), event("y", 2)]]);

        second[0].send(Value::Int(3)).await;
        let batch = with_timeout(batches.next(), 1, "second provider batch")
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(ticks_of(&batch), [vec![event("z", 3)]]);
    }

    #[apply(async_test)]
    async fn composite_continues_after_one_provider_ends(ex: Rc<LocalExecutor<'static>>) {
        let (first, provider_x) = manual_provider(&["x"]);
        let (second, provider_y) = manual_provider(&["y"]);
        let factory = InputStreamFactory::composite([
            provider_x.for_variables(["x"]),
            provider_y.for_variables(["y"]),
        ])
        .unwrap();
        let mut ticks = crate::into_tick_stream(factory.open(vars(&["x", "y"])).await.unwrap());

        drop(first);
        second[0].send(Value::Int(1)).await;
        assert_eq!(
            with_timeout(ticks.next(), 1, "surviving provider")
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            vec![event("y", 1)]
        );

        drop(second);
        assert!(
            with_timeout(ticks.next(), 1, "composite end")
                .await
                .expect("composite should end once every provider has ended")
                .is_none()
        );
    }

    #[apply(async_test)]
    async fn composite_without_requested_variables_opens_an_empty_stream(
        ex: Rc<LocalExecutor<'static>>,
    ) {
        // The file provider is never opened because nothing is requested.
        let factory = InputStreamFactory::composite([InputStreamFactory::<Value>::file(
            "missing.input".into(),
        )
        .for_variables(["x"])])
        .unwrap();
        let mut batches = factory.open(BTreeSet::new()).await.unwrap();
        assert!(
            with_timeout(batches.next(), 1, "empty composite")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[apply(async_test)]
    async fn composite_open_errors_identify_the_failing_provider(ex: Rc<LocalExecutor<'static>>) {
        let factory = InputStreamFactory::composite([InputStreamFactory::<Value>::file(
            "missing.input".into(),
        )
        .for_variables(["x"])])
        .unwrap();

        let error = open_error(factory.open(vars(&["x"])).await);
        assert_eq!(
            error.to_string(),
            "file input provider for variables {x} could not be opened"
        );
        assert!(format!("{error:#}").contains("Input file could not be parsed"));
    }

    #[test]
    fn composite_stream_errors_identify_the_failing_provider() {
        smol::block_on(async {
            let source: InputStream<Value> = Box::pin(futures::stream::iter([Err(
                anyhow::anyhow!("source failed"),
            )]));
            let mut labelled = InputStreamFactory::<Value>::labelled_stream(
                source,
                "MQTT input provider for variables {x}".into(),
            );
            let error = labelled.next().await.unwrap().unwrap_err();
            assert_eq!(
                format!("{error:#}"),
                "MQTT input provider for variables {x} failed: source failed"
            );
            assert!(labelled.next().await.is_none());
        });
    }

    #[test]
    fn composite_aggregation_accepts_independent_event_providers() {
        let independent = InputStreamFactory::composite([
            InputStreamFactory::<Value>::mqtt(None, None).for_variables(["x"]),
            InputStreamFactory::<Value>::redis(None, None)
                .input_aggregation(preserving_aggregation())
                .unwrap()
                .for_variables(["y"]),
            InputStreamFactory::composite([
                InputStreamFactory::<Value>::mqtt(None, None).for_variables(["z"])
            ])
            .unwrap()
            .for_variables(["z"]),
        ])
        .unwrap();
        assert!(
            independent
                .input_aggregation(preserving_aggregation())
                .is_ok()
        );
    }

    #[test]
    fn composite_aggregation_rejects_atomic_providers() {
        let (_senders, manual) = manual_provider(&["y"]);
        let atomic_message = "input aggregation requires an independent-event source; file and manual inputs define atomic ticks";

        let with_manual = InputStreamFactory::composite([
            InputStreamFactory::<Value>::mqtt(None, None).for_variables(["x"]),
            manual.for_variables(["y"]),
        ])
        .unwrap();
        assert_eq!(
            with_manual
                .input_aggregation(preserving_aggregation())
                .unwrap_err()
                .to_string(),
            atomic_message
        );

        let with_file = InputStreamFactory::composite([InputStreamFactory::<Value>::file(
            "trace.input".into(),
        )
        .for_variables(["x"])])
        .unwrap();
        assert_eq!(
            with_file
                .input_aggregation(preserving_aggregation())
                .unwrap_err()
                .to_string(),
            atomic_message
        );

        // A child which coalesces its events already defines atomic ticks.
        let with_coalescing_child = InputStreamFactory::composite([
            InputStreamFactory::<Value>::mqtt(None, None).for_variables(["x"]),
            InputStreamFactory::<Value>::redis(None, None)
                .input_aggregation(coalescing_aggregation())
                .unwrap()
                .for_variables(["y"]),
        ])
        .unwrap();
        assert_eq!(
            with_coalescing_child
                .input_aggregation(preserving_aggregation())
                .unwrap_err()
                .to_string(),
            atomic_message
        );

        let with_atomic_nested_composite = InputStreamFactory::composite([
            InputStreamFactory::composite([manual_provider(&["x"]).1.for_variables(["x"])])
                .unwrap()
                .for_variables(["x"]),
        ])
        .unwrap();
        assert_eq!(
            with_atomic_nested_composite
                .input_aggregation(preserving_aggregation())
                .unwrap_err()
                .to_string(),
            atomic_message
        );
    }

    #[test]
    fn composite_reconfigurability_is_checked_recursively() {
        let factory = InputStreamFactory::composite([
            InputStreamFactory::<Value>::mqtt(None, None).for_variables(["x"]),
            InputStreamFactory::composite([InputStreamFactory::<Value>::file(
                "trace.input".into(),
            )
            .for_variables(["y"])])
            .unwrap()
            .for_variables(["y"]),
        ])
        .unwrap();

        let error = factory.ensure_reconfigurable().unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            "composite input provider for variables {y}: \
             file input provider for variables {y}: \
             file input cannot be used by a reconfigurable runtime"
        );

        let reconfigurable = InputStreamFactory::composite([
            InputStreamFactory::<Value>::mqtt(None, None).for_variables(["x"]),
            InputStreamFactory::<Value>::redis(None, None).for_variables(["y"]),
        ])
        .unwrap();
        assert!(reconfigurable.ensure_reconfigurable().is_ok());
    }

    #[test]
    fn composite_reconfiguration_input_goes_to_the_assigned_provider() {
        let mut factory = InputStreamFactory::composite([
            InputStreamFactory::<Value>::mqtt(None, None).for_variables(["x", "r"]),
            InputStreamFactory::<Value>::mqtt(None, None).for_variables(["y"]),
        ])
        .unwrap();

        factory.add_reconfiguration_input(VarName::new("r"), vars(&["x", "y"]));

        let providers = children(&factory);
        // The assigned provider subscribes to the reconfiguration variable and
        // only to the model variables routed to it.
        assert_eq!(
            mqtt_topics(&providers[0].provider)
                .unwrap()
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>(),
            vars(&["x", "r"])
        );
        assert!(mqtt_topics(&providers[1].provider).is_none());
    }

    #[apply(async_test)]
    async fn unassigned_reconfiguration_variables_are_reported_when_opening(
        ex: Rc<LocalExecutor<'static>>,
    ) {
        let mut factory = InputStreamFactory::composite([
            InputStreamFactory::<Value>::mqtt(None, None).for_variables(["x"]),
            InputStreamFactory::<Value>::redis(None, None).for_variables(["y"]),
        ])
        .unwrap();

        // No provider is chosen for an unassigned reconfiguration variable.
        factory.add_reconfiguration_input(VarName::new("r"), vars(&["x", "y"]));
        for provider in children(&factory) {
            assert!(mqtt_topics(&provider.provider).is_none());
        }

        let error = open_error(factory.open(vars(&["x", "y", "r"])).await);
        assert!(
            error
                .to_string()
                .starts_with("composite input has no provider assigned to the input variables {r}")
        );
    }

    #[test]
    fn composite_reconfiguration_partitions_variables_between_providers() {
        let mut factory = InputStreamFactory::composite([
            InputStreamFactory::<Value>::mqtt(None, None).for_variables(["x"]),
            InputStreamFactory::<Value>::redis(None, None).for_variables(["y", "z"]),
        ])
        .unwrap();

        factory
            .reconfigure(
                vars(&["x", "y"]),
                &TopicMapping::new(),
                &MsgTypeMapping::new(),
            )
            .unwrap();

        let providers = children(&factory);
        assert_eq!(
            mqtt_topics(&providers[0].provider)
                .unwrap()
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>(),
            vars(&["x"])
        );
        assert_eq!(
            mqtt_topics(&providers[1].provider)
                .unwrap()
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>(),
            vars(&["y"])
        );

        let error = factory
            .reconfigure(
                vars(&["x", "q"]),
                &TopicMapping::new(),
                &MsgTypeMapping::new(),
            )
            .unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("composite input has no provider assigned to the input variables {q}")
        );
    }

    #[test]
    fn composite_collects_ros_mappings_routed_to_each_provider() {
        let executor = Rc::new(LocalExecutor::new());
        let first_topics = TopicMapping::from([
            (VarName::new("x"), "/x".to_string()),
            // Extra mapping entries must not leak into another provider.
            (VarName::new("y"), "/wrong_y".to_string()),
        ]);
        let first_types = MsgTypeMapping::from([
            (VarName::new("x"), "Int32".to_string()),
            (VarName::new("y"), "Int32".to_string()),
        ]);
        let second_topics = TopicMapping::from([(VarName::new("y"), "/y".to_string())]);
        let second_types = MsgTypeMapping::from([(VarName::new("y"), "Float64".to_string())]);

        let factory = InputStreamFactory::<Value>::composite([
            InputStreamFactory::ros(first_topics, first_types, executor.clone())
                .for_variables(["x"]),
            InputStreamFactory::composite([InputStreamFactory::ros(
                second_topics,
                second_types,
                executor.clone(),
            )
            .for_variables(["y"])])
            .unwrap()
            .for_variables(["y"]),
            InputStreamFactory::mqtt(None, None).for_variables(["z"]),
        ])
        .unwrap();

        let (topics, types) = factory.ros_mappings();
        assert_eq!(
            topics,
            TopicMapping::from([
                (VarName::new("x"), "/x".to_string()),
                (VarName::new("y"), "/y".to_string()),
            ])
        );
        assert_eq!(
            types,
            MsgTypeMapping::from([
                (VarName::new("x"), "Int32".to_string()),
                (VarName::new("y"), "Float64".to_string()),
            ])
        );
    }

    #[test]
    fn single_provider_ros_mappings_are_unchanged() {
        let executor = Rc::new(LocalExecutor::new());
        let topics = TopicMapping::from([(VarName::new("x"), "/x".to_string())]);
        let types = MsgTypeMapping::from([(VarName::new("x"), "Int32".to_string())]);
        let factory =
            InputStreamFactory::<Value>::ros(topics.clone(), types.clone(), executor.clone());
        assert_eq!(factory.ros_mappings(), (topics, types));

        assert_eq!(
            InputStreamFactory::<Value>::mqtt(None, None).ros_mappings(),
            (TopicMapping::new(), MsgTypeMapping::new())
        );
    }
}
