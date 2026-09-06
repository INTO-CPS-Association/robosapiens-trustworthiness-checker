use std::any::TypeId;
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    pin::Pin,
    rc::Rc,
    task::{Context as TaskContext, Poll},
};

use anyhow::Context;
use async_stream::stream;
use futures::{Stream, StreamExt};
use smol::LocalExecutor;

use crate::core::{
    FileInputValue, InputBatch, InputStream, MQTT_HOSTNAME, REDIS_HOSTNAME, RosStreamValue, Value,
    VarName,
};
#[cfg(any(feature = "ros", test))]
use crate::io::config::ReconfigurationRequest;
use crate::io::config::{
    FormatId, InputBinding, InputConfigFile, InputConfiguration, ResolvedInput, ResolvedSource,
    Route, SourceId,
};
use crate::io::mqtt::MqttProtocol;
use crate::io::reconfigurable_input::{
    InputSourceControl, InputSourceSet, OpenedInputSource, ReconfigurableInputItem,
    ReconfigurableInputStream, ReconfigurationControl, SharedInputSourceSet,
};
use crate::io::{
    PipelineGeneration, RedisKnowledgeConfig, RetryPolicy, SessionId, SessionRevision,
    ShutdownDeadline,
};
use crate::stream_utils::Fanout;
use ::core::cfg_select;
#[cfg(feature = "redis")]
use std::any::Any;

use super::super::config::InputPolicy;

/// An opened ordinary input pipeline and the resources that keep it live.
///
/// Dropping this owner drops the underlying transports and requests their
/// cancellation. Consume it with [`Self::into_drain`] when locally admitted
/// input and a pending input window must be observed before transport cleanup
/// reaches EOF.
pub struct OpenedInput<V = Value> {
    stream: InputStream<V>,
    sources: Rc<RefCell<InputSourceSet<V>>>,
    stop: Option<Box<dyn FnOnce()>>,
    drain_stream: bool,
}

impl<V: 'static> OpenedInput<V> {
    fn new(stream: InputStream<V>, sources: Rc<RefCell<InputSourceSet<V>>>) -> Self {
        Self {
            stream,
            sources,
            stop: None,
            drain_stream: true,
        }
    }

    pub(crate) fn with_stop(stream: InputStream<V>, stop: impl FnOnce() + 'static) -> Self {
        Self {
            stream,
            sources: Rc::new(RefCell::new(InputSourceSet::new(Vec::new()))),
            stop: Some(Box::new(stop)),
            drain_stream: true,
        }
    }

    pub(crate) fn map_stream(self, map: impl FnOnce(InputStream<V>) -> InputStream<V>) -> Self {
        Self {
            stream: map(self.stream),
            sources: self.sources,
            stop: self.stop,
            drain_stream: self.drain_stream,
        }
    }

    /// Map input values while retaining the original source lifecycle.
    pub(crate) fn map_values<U: 'static>(
        mut self,
        map: impl FnOnce(InputStream<V>) -> InputStream<U>,
    ) -> OpenedInput<U> {
        let stream = map(self.stream);
        let sources = self.sources;
        let original_stop = self.stop.take();
        OpenedInput {
            stream,
            sources: Rc::new(RefCell::new(InputSourceSet::new(Vec::new()))),
            stop: Some(Box::new(move || {
                if let Some(stop) = original_stop {
                    stop();
                }
                sources.borrow_mut().stop_all();
            })),
            drain_stream: self.drain_stream,
        }
    }

    pub fn into_drain(self) -> InputDrain<V> {
        self.into_drain_with_deadline(ShutdownDeadline::none())
    }

    pub fn into_drain_with_deadline(mut self, deadline: ShutdownDeadline) -> InputDrain<V>
    where
        V: 'static,
    {
        if let Some(stop) = self.stop.take() {
            stop();
        }
        self.sources.borrow_mut().stop_all();
        let mut stream = if self.drain_stream {
            self.stream
        } else {
            crate::core::empty_input_stream()
        };
        InputDrain {
            stream: Box::pin(async_stream::stream! {
                loop {
                    match deadline.timeout(stream.next()).await {
                        Ok(Some(item)) => yield item,
                        Ok(None) => return,
                        Err(error) => {
                            yield Err(crate::core::InputError::source(error));
                            return;
                        }
                    }
                }
            }),
        }
    }
}

impl<V: 'static> From<InputStream<V>> for OpenedInput<V> {
    fn from(stream: InputStream<V>) -> Self {
        Self {
            stream,
            sources: Rc::new(RefCell::new(InputSourceSet::new(Vec::new()))),
            stop: None,
            drain_stream: false,
        }
    }
}

impl<V: 'static> Stream for OpenedInput<V> {
    type Item = Result<InputBatch<V>, crate::core::InputError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

/// The consuming graceful-shutdown stream returned by [`OpenedInput`].
pub struct InputDrain<V = Value> {
    stream: InputStream<V>,
}

impl<V: 'static> Stream for InputDrain<V> {
    type Item = Result<InputBatch<V>, crate::core::InputError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

#[derive(Clone, Debug)]
enum InputSourceKind<V = Value> {
    File {
        path: String,
    },
    InMemoryRows {
        columns: BTreeMap<VarName, Vec<V>>,
    },
    InMemoryTicks {
        batches: Vec<InputBatch<V>>,
    },
    Ros {
        routes: BTreeMap<VarName, Route>,
        executor: Rc<LocalExecutor<'static>>,
    },
    Mqtt {
        host: String,
        routes: Option<BTreeMap<VarName, Route>>,
        port: Option<u16>,
        protocol: MqttProtocol,
        retry: RetryPolicy,
    },
    Redis {
        host: String,
        routes: Option<BTreeMap<VarName, Route>>,
        port: Option<u16>,
        retry: RetryPolicy,
    },
    RedisKnowledge(RedisKnowledgeConfig),
    Channel {
        fanouts: BTreeMap<VarName, Rc<Fanout<V>>>,
        control: Option<Rc<Fanout<Value>>>,
    },
}

/// Reusable source configuration. It contains no opened transport handles.
#[derive(Clone, Debug)]
pub struct InputSource<V = Value> {
    kind: InputSourceKind<V>,
    /// Transport-local route carrying monitor reconfiguration messages:
    /// an MQTT topic, Redis Pub/Sub channel, ROS topic, or equivalent.
    reconfiguration_route: Option<Box<str>>,
}

impl InputSource<Value> {
    /// Construct a selected-key Redis knowledge source. Redis knowledge
    /// produces the ordinary project [`Value`] domain; it is intentionally not
    /// a constructor on arbitrary `InputSource<V>` specializations.
    pub fn redis_knowledge(config: RedisKnowledgeConfig) -> Self {
        Self::new(InputSourceKind::RedisKnowledge(config))
    }
}

/// Open the Value-specific Redis knowledge provider from the generic source
/// opener. The type check is deliberately before the provider is opened so an
/// unsupported value domain cannot establish a Redis connection.
#[cfg(feature = "redis")]
async fn open_configured_redis_knowledge<V>(
    config: RedisKnowledgeConfig,
    bindings: BTreeMap<VarName, String>,
) -> anyhow::Result<(InputStream<V>, crate::io::redis::RedisKnowledgeInputControl)>
where
    V: FileInputValue + RosStreamValue + 'static,
{
    anyhow::ensure!(
        TypeId::of::<V>() == TypeId::of::<Value>(),
        "Redis knowledge input produces ordinary `Value` input and is unsupported for MSTLO or other non-Value input domains"
    );

    let (mut values, owner) =
        crate::io::redis::open_value_redis_knowledge(config, bindings).await?;
    let stream = Box::pin(async_stream::try_stream! {
        while let Some(batch) = values.next().await {
            let batch = batch?;
            let batch = batch.try_map_values(|value| {
                let value: Box<dyn Any> = Box::new(value);
                value
                    .downcast::<V>()
                    .map(|value| *value)
                    .map_err(|_| anyhow::anyhow!("Redis knowledge value-domain conversion failed"))
            })?;
            yield batch;
        }
    });
    Ok((stream, owner))
}

/// An owned local source set containing source-owned catalogs and
/// security-sensitive transport configuration. It is deliberately separate
/// from request-specific resolved input.
#[derive(Clone, Debug)]
pub struct InputSources<V = Value> {
    sources: BTreeMap<SourceId, InputSource<V>>,
    default: Option<SourceId>,
}

/// The fixed source and route used by a reconfigurable input adapter.
/// This borrows only the reusable source configuration; the route is owned so
/// the adapter can retain it independently of any resolved input.
#[derive(Debug)]
pub(crate) struct ResolvedReconfigurationSource<'a, V> {
    source_id: &'a SourceId,
    source: &'a InputSource<V>,
    route: Box<str>,
}

impl<'a, V> ResolvedReconfigurationSource<'a, V> {
    pub(crate) fn source_id(&self) -> &SourceId {
        self.source_id
    }

    pub(crate) fn source(&self) -> &InputSource<V> {
        self.source
    }

    pub(crate) fn route(&self) -> &str {
        &self.route
    }
}

impl<V> InputSources<V> {
    pub fn new() -> Self {
        Self {
            sources: BTreeMap::new(),
            default: None,
        }
    }

    pub fn single(source: InputSource<V>) -> Self {
        let mut sources = Self::new();
        sources.sources.insert("default".to_owned(), source);
        sources.default = Some("default".to_owned());
        sources
    }

    /// Insert a source while rejecting duplicate or empty stable IDs.
    pub fn try_insert(
        mut self,
        source: impl Into<SourceId>,
        input: InputSource<V>,
    ) -> anyhow::Result<Self> {
        let source = source.into();
        anyhow::ensure!(!source.trim().is_empty(), "input source ID cannot be empty");
        anyhow::ensure!(
            self.sources.insert(source, input).is_none(),
            "input source ID is declared more than once"
        );
        Ok(self)
    }

    /// Infallible convenience form for programmatic construction. Configuration
    /// loading uses [`Self::try_insert`] so malformed input is reported instead
    /// of being able to overwrite an earlier source.
    pub fn insert(self, source: impl Into<SourceId>, input: InputSource<V>) -> Self {
        self.try_insert(source, input)
            .expect("input source IDs must be non-empty and unique")
    }

    pub fn default_source(mut self, source: impl Into<SourceId>) -> anyhow::Result<Self> {
        let source = source.into();
        anyhow::ensure!(
            self.sources.contains_key(&source),
            "default input source `{source}` is not registered"
        );
        self.default = Some(source);
        Ok(self)
    }

    pub fn sources(&self) -> &BTreeMap<SourceId, InputSource<V>> {
        &self.sources
    }

    pub fn default_id(&self) -> Option<&str> {
        self.default.as_deref()
    }

    pub fn source(&self, source: &str) -> Option<&InputSource<V>> {
        self.sources.get(source)
    }

    pub fn source_mut(&mut self, source: &str) -> Option<&mut InputSource<V>> {
        self.sources.get_mut(source)
    }

    fn sole_reconfigurable_source_id(&self) -> Option<SourceId> {
        if self.sources.len() != 1 {
            return None;
        }
        let (source_id, source) = self.sources.iter().next()?;
        source.supports_reconfiguration().then(|| source_id.clone())
    }

    pub(crate) fn resolve_reconfiguration_source(
        &self,
        requested_route: Option<&str>,
    ) -> anyhow::Result<ResolvedReconfigurationSource<'_, V>> {
        let (source_id, source) = match self.sources.len() {
            0 => anyhow::bail!("reconfigurable input has no configured sources"),
            1 => self
                .sources
                .iter()
                .next()
                .expect("source count checked above"),
            _ => {
                let mut declared = self
                    .sources
                    .iter()
                    .filter(|(_, source)| source.reconfiguration_route().is_some());
                let Some(selected) = declared.next() else {
                    anyhow::bail!(
                        "reconfigurable input has multiple sources, but none declares `reconfiguration_route`"
                    );
                };
                let remaining = declared.count();
                anyhow::ensure!(
                    remaining == 0,
                    "reconfigurable input has {} sources declaring `reconfiguration_route`; exactly one is required",
                    remaining + 1
                );
                selected
            }
        };

        if !source.supports_reconfiguration() {
            if self.sources.len() == 1 {
                anyhow::bail!(
                    "the only configured input source `{source_id}` does not support reconfiguration"
                );
            }
            anyhow::bail!(
                "input source `{source_id}` declares `reconfiguration_route` but does not support reconfiguration"
            );
        }

        let route = requested_route
            .map(str::to_owned)
            .or_else(|| source.reconfiguration_route().map(str::to_owned))
            .unwrap_or_else(|| "reconf".to_owned());
        anyhow::ensure!(
            !route.trim().is_empty(),
            "reconfiguration route cannot be empty"
        );

        Ok(ResolvedReconfigurationSource {
            source_id,
            source,
            route: route.into_boxed_str(),
        })
    }

    pub fn from_config(
        config: InputConfigFile,
        executor: Rc<LocalExecutor<'static>>,
        mqtt_port: Option<u16>,
        redis_port: Option<u16>,
        mqtt_protocol: MqttProtocol,
    ) -> anyhow::Result<InputSources<V>>
    where
        V: 'static,
    {
        config.validate()?;
        let mut sources = InputSources::new();
        for (id, source_config) in config.sources {
            let (input, reconfiguration_route) = match source_config {
                crate::io::config::SourceConfig::Mqtt {
                    host,
                    port,
                    routes,
                    reconfiguration_route,
                    retry,
                    protocol,
                } => {
                    let routes = routes;
                    (
                        InputSource::mqtt_with_host_routes(
                            host.unwrap_or_else(|| MQTT_HOSTNAME.to_owned()),
                            if routes.is_empty() {
                                None
                            } else {
                                Some(routes)
                            },
                            port.or(mqtt_port),
                            protocol.unwrap_or(mqtt_protocol),
                            retry.unwrap_or_else(RetryPolicy::input_default),
                        ),
                        reconfiguration_route,
                    )
                }
                crate::io::config::SourceConfig::Redis {
                    host,
                    port,
                    routes,
                    reconfiguration_route,
                    retry,
                } => {
                    let routes = routes;
                    (
                        InputSource::redis_with_host_routes_and_retry(
                            host.unwrap_or_else(|| REDIS_HOSTNAME.to_owned()),
                            if routes.is_empty() {
                                None
                            } else {
                                Some(routes)
                            },
                            port.or(redis_port),
                            retry.unwrap_or_else(RetryPolicy::input_default),
                        ),
                        reconfiguration_route,
                    )
                }
                crate::io::config::SourceConfig::RedisKnowledge {
                    host,
                    port,
                    database,
                    publish_initial,
                    keys,
                    retry,
                } => {
                    anyhow::ensure!(
                        TypeId::of::<V>() == TypeId::of::<Value>(),
                        "Redis knowledge input produces ordinary `Value` input and is unsupported for MSTLO or other non-Value input domains"
                    );
                    (
                        InputSource::new(InputSourceKind::RedisKnowledge(RedisKnowledgeConfig {
                            host: host.unwrap_or_else(|| REDIS_HOSTNAME.to_owned()),
                            port: port.or(redis_port),
                            database,
                            publish_initial,
                            keys,
                            retry,
                        })),
                        None,
                    )
                }
                crate::io::config::SourceConfig::Ros {
                    routes,
                    reconfiguration_route,
                } => (
                    InputSource::ros(routes, executor.clone()),
                    reconfiguration_route,
                ),
            };
            let input = if let Some(route) = reconfiguration_route {
                input.with_reconfiguration_route(route)?
            } else {
                input
            };
            sources = sources.try_insert(id, input)?;
        }
        if let Some(default) = config.default {
            sources = sources.default_source(default)?;
        }
        Ok(sources)
    }

    pub(crate) fn resolve_default(
        &self,
        variables: &BTreeSet<VarName>,
    ) -> anyhow::Result<ResolvedInput> {
        let catalog_ownership = self.catalog_ownership(variables)?;
        let mut by_source = BTreeMap::<SourceId, Vec<InputBinding>>::new();
        for variable in variables {
            let source_id = self.owner_for(variable, &catalog_ownership)?;
            let source = self.sources.get(&source_id).ok_or_else(|| {
                anyhow::anyhow!("resolved input source `{source_id}` is not available")
            })?;
            let route = source.route_for(variable).unwrap_or_else(|| {
                Route::new(variable.to_string(), None).expect("variable names are valid routes")
            });
            anyhow::ensure!(
                !source.requires_route_codec() || route.format().is_some(),
                "source `{source_id}` requires a route format for input variable `{variable}`"
            );
            let binding = InputBinding::new(
                variable.clone(),
                Route::new(
                    route.address(),
                    Some(
                        route
                            .format()
                            .cloned()
                            .unwrap_or_else(|| FormatId::new("json")),
                    ),
                )?,
            );
            source.validate_binding(&binding)?;
            by_source.entry(source_id).or_default().push(binding);
        }
        if variables.is_empty() {
            if let Some(source_id) = self.sole_reconfigurable_source_id() {
                by_source.entry(source_id).or_default();
            }
        }
        let resolved = ResolvedInput::new(
            by_source
                .into_iter()
                .map(|(source, bindings)| ResolvedSource::new(source, bindings)),
        );
        self.validate_resolved(&resolved, variables)
    }

    pub(crate) fn resolve_input_configuration(
        &self,
        config: &InputConfiguration,
        variables: &BTreeSet<VarName>,
    ) -> anyhow::Result<ResolvedInput> {
        config.validate_structure()?;
        if config.inputs.is_none() && config.sources.is_none() {
            return self.resolve_default(variables);
        }

        let explicit_bindings = config.explicit_input_bindings();
        let catalog_ownership = if explicit_bindings
            .iter()
            .any(|(source, _, _)| source.is_none())
        {
            self.catalog_ownership(variables)?
        } else {
            BTreeMap::new()
        };
        let mut grouped = BTreeMap::<SourceId, Vec<InputBinding>>::new();
        if explicit_bindings.is_empty() {
            let configured_source = config.source.clone().or_else(|| {
                config.sources.as_ref().and_then(|sources| {
                    if sources.len() == 1 {
                        sources.keys().next().cloned()
                    } else {
                        None
                    }
                })
            });
            if let Some(source_id) = configured_source {
                grouped.entry(source_id).or_default();
            } else if config.sources.is_none()
                && variables.is_empty()
                && let Some(source_id) = self.sole_reconfigurable_source_id()
            {
                grouped.entry(source_id).or_default();
            }
        }
        for (source, variable, route) in explicit_bindings {
            let source_id = match source {
                Some(source) => source.to_owned(),
                None => self.owner_for(variable, &catalog_ownership)?,
            };
            let source_config = self
                .sources
                .get(&source_id)
                .ok_or_else(|| anyhow::anyhow!("input source `{source_id}` is not registered"))?;
            let binding = InputBinding::new(
                variable.clone(),
                Route::new(
                    route.address(),
                    Some(
                        route
                            .format()
                            .cloned()
                            .unwrap_or_else(|| FormatId::new("json")),
                    ),
                )?,
            );
            anyhow::ensure!(
                !source_config.requires_route_codec() || route.format().is_some(),
                "source `{source_id}` requires a route format for input variable `{variable}`"
            );
            source_config.validate_binding(&binding)?;
            grouped.entry(source_id).or_default().push(binding);
        }
        let resolved = ResolvedInput::new(
            grouped
                .into_iter()
                .map(|(source, bindings)| ResolvedSource::new(source, bindings)),
        );
        self.validate_resolved(&resolved, variables)
    }

    fn catalog_ownership(
        &self,
        variables: &BTreeSet<VarName>,
    ) -> anyhow::Result<BTreeMap<VarName, SourceId>> {
        let mut ownership = BTreeMap::new();
        for (source_id, source) in &self.sources {
            source.record_catalog_ownership(source_id, variables, &mut ownership)?;
        }
        Ok(ownership)
    }

    fn owner_for(
        &self,
        variable: &VarName,
        catalog_ownership: &BTreeMap<VarName, SourceId>,
    ) -> anyhow::Result<SourceId> {
        if let Some(owner) = catalog_ownership.get(variable) {
            return Ok(owner.clone());
        }

        let default = self.default.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "no input source catalog owns variable `{variable}` and no default source is configured"
            )
        })?;
        let source = self
            .sources
            .get(default)
            .ok_or_else(|| anyhow::anyhow!("default input source `{default}` is not registered"))?;
        anyhow::ensure!(
            source.supports_default_route(),
            "default input source `{default}` has no route for variable `{variable}`"
        );
        Ok(default.clone())
    }

    fn validate_resolved(
        &self,
        resolved: &ResolvedInput,
        variables: &BTreeSet<VarName>,
    ) -> anyhow::Result<ResolvedInput> {
        let mut ownership = BTreeMap::<VarName, SourceId>::new();
        for source_plan in resolved.sources() {
            let source = self.sources.get(source_plan.source()).ok_or_else(|| {
                anyhow::anyhow!(
                    "resolved input source `{}` is not registered",
                    source_plan.source()
                )
            })?;
            source.validate_bindings(source_plan.bindings())?;
            for binding in source_plan.bindings() {
                source.validate_binding(binding)?;
                anyhow::ensure!(
                    !source.requires_route_codec()
                        || !binding.route().format().unwrap().as_str().trim().is_empty(),
                    "source `{}` requires a route format for input variable `{}`",
                    source_plan.source(),
                    binding.variable()
                );
                if let Some(previous) =
                    ownership.insert(binding.variable().clone(), source_plan.source().clone())
                {
                    anyhow::bail!(
                        "input variable `{}` is bound by both source `{previous}` and source `{}`",
                        binding.variable(),
                        source_plan.source()
                    );
                }
                anyhow::ensure!(
                    variables.contains(binding.variable()),
                    "resolved input binds undeclared model variable `{}`",
                    binding.variable()
                );
            }
        }
        let missing = variables
            .iter()
            .filter(|variable| !ownership.contains_key(*variable))
            .cloned()
            .collect::<Vec<_>>();
        anyhow::ensure!(
            missing.is_empty(),
            "resolved input is missing bindings for {missing:?}"
        );
        Ok(resolved.clone())
    }
}

impl<V> Default for InputSources<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> InputSource<V> {
    fn is_finite(&self) -> bool {
        matches!(
            self.kind,
            InputSourceKind::File { .. }
                | InputSourceKind::InMemoryRows { .. }
                | InputSourceKind::InMemoryTicks { .. }
        )
    }

    async fn open_owned(
        self,
        bindings: Vec<InputBinding>,
        variables: BTreeSet<VarName>,
    ) -> anyhow::Result<(InputStream<V>, Option<InputSourceControl>)>
    where
        V: FileInputValue + RosStreamValue,
    {
        if let InputSourceKind::Mqtt {
            host,
            port,
            protocol,
            retry,
            ..
        } = &self.kind
        {
            let topics = bindings
                .iter()
                .map(|binding| {
                    (
                        binding.variable().clone(),
                        binding.route().address().to_owned(),
                    )
                })
                .collect();
            let (stream, owner) = protocol
                .open_owned_data::<V>(host, *port, topics, retry.clone())
                .await?;
            return Ok((stream, Some(InputSourceControl::Rumqttc(owner))));
        }
        #[cfg(feature = "redis")]
        if let InputSourceKind::Redis {
            host, port, retry, ..
        } = &self.kind
        {
            let topics = bindings
                .iter()
                .map(|binding| {
                    (
                        binding.variable().clone(),
                        binding.route().address().to_owned(),
                    )
                })
                .collect();
            let (mut items, owner) = crate::io::redis::open_owned_input_stream_items(
                host,
                *port,
                topics,
                None,
                retry.clone(),
            )
            .await?;
            let stream = Box::pin(async_stream::try_stream! {
                while let Some(item) = items.next().await {
                    match item? {
                        crate::io::redis::RedisInputItem::Data(batch) => yield batch,
                        crate::io::redis::RedisInputItem::Control(_) => unreachable!("data-only Redis stream cannot receive control"),
                        crate::io::redis::RedisInputItem::Boundary(_) => unreachable!("data-only Redis stream cannot receive boundary"),
                    }
                }
            });
            return Ok((stream, Some(InputSourceControl::Redis(owner))));
        }
        #[cfg(feature = "ros")]
        if let InputSourceKind::Ros { executor, .. } = &self.kind {
            let active_topics = bindings
                .iter()
                .map(|binding| {
                    (
                        binding.variable().clone(),
                        binding.route().address().to_owned(),
                    )
                })
                .collect();
            let mapping = bindings
                .iter()
                .map(|binding| {
                    let format = binding.route().format().ok_or_else(|| {
                        anyhow::anyhow!(
                            "ROS route for `{}` requires a route format",
                            binding.variable()
                        )
                    })?;
                    Ok((
                        binding.variable().to_string(),
                        (binding.route().address().to_owned(), format.to_string()),
                    ))
                })
                .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
            let (stream, owner) = V::open_ros_input(Rc::clone(executor), mapping)?;
            return Ok((
                stream,
                Some(InputSourceControl::Ros {
                    controls: vec![owner],
                    active_topics,
                }),
            ));
        }
        #[cfg(feature = "redis")]
        if let InputSourceKind::RedisKnowledge(config) = &self.kind {
            let keys = bindings
                .iter()
                .map(|binding| {
                    (
                        binding.variable().clone(),
                        binding.route().address().to_owned(),
                    )
                })
                .collect();
            let (stream, owner) =
                open_configured_redis_knowledge::<V>(config.clone(), keys).await?;
            return Ok((stream, Some(InputSourceControl::RedisKnowledge(owner))));
        }
        self.open(bindings, variables)
            .await
            .map(|stream| (stream, None))
    }

    fn new(kind: InputSourceKind<V>) -> Self {
        Self {
            kind,
            reconfiguration_route: None,
        }
    }

    /// Set the transport-local route carrying monitor reconfiguration messages.
    pub fn with_reconfiguration_route(
        mut self,
        route: impl Into<Box<str>>,
    ) -> anyhow::Result<Self> {
        let route = route.into();
        anyhow::ensure!(
            !route.trim().is_empty(),
            "reconfiguration route cannot be empty"
        );
        anyhow::ensure!(
            !matches!(&self.kind, InputSourceKind::RedisKnowledge(_)),
            "Redis knowledge sources cannot carry `reconfiguration_route`; use a control-capable source"
        );
        self.reconfiguration_route = Some(route);
        Ok(self)
    }

    pub(crate) fn reconfiguration_route(&self) -> Option<&str> {
        self.reconfiguration_route.as_deref()
    }

    pub fn file(path: String) -> Self {
        Self::new(InputSourceKind::File { path })
    }

    pub fn in_memory_rows(columns: BTreeMap<VarName, Vec<V>>) -> Self {
        Self::new(InputSourceKind::InMemoryRows { columns })
    }

    pub fn in_memory_ticks(batches: impl IntoIterator<Item = InputBatch<V>>) -> Self {
        Self::new(InputSourceKind::InMemoryTicks {
            batches: batches.into_iter().collect(),
        })
    }

    pub fn ros(routes: BTreeMap<VarName, Route>, executor: Rc<LocalExecutor<'static>>) -> Self {
        Self::new(InputSourceKind::Ros { routes, executor })
    }

    pub fn mqtt(routes: Option<BTreeMap<VarName, Route>>, port: Option<u16>) -> Self {
        Self::mqtt_with_routes(routes, port, MqttProtocol::default())
    }

    pub fn mqtt_with_routes(
        routes: Option<BTreeMap<VarName, Route>>,
        port: Option<u16>,
        protocol: MqttProtocol,
    ) -> Self {
        Self::mqtt_with_host_routes(
            MQTT_HOSTNAME,
            routes,
            port,
            protocol,
            RetryPolicy::input_default(),
        )
    }

    pub fn mqtt_with_host_routes(
        host: impl Into<String>,
        routes: Option<BTreeMap<VarName, Route>>,
        port: Option<u16>,
        protocol: MqttProtocol,
        retry: RetryPolicy,
    ) -> Self {
        Self::new(InputSourceKind::Mqtt {
            host: host.into(),
            routes,
            port,
            protocol,
            retry,
        })
    }

    pub fn redis(routes: Option<BTreeMap<VarName, Route>>, port: Option<u16>) -> Self {
        Self::redis_with_routes(routes, port)
    }

    pub fn redis_with_routes(routes: Option<BTreeMap<VarName, Route>>, port: Option<u16>) -> Self {
        Self::redis_with_host_routes(REDIS_HOSTNAME, routes, port)
    }

    pub fn redis_with_host_routes(
        host: impl Into<String>,
        routes: Option<BTreeMap<VarName, Route>>,
        port: Option<u16>,
    ) -> Self {
        Self::redis_with_host_routes_and_retry(host, routes, port, RetryPolicy::input_default())
    }

    pub fn redis_with_host_routes_and_retry(
        host: impl Into<String>,
        routes: Option<BTreeMap<VarName, Route>>,
        port: Option<u16>,
        retry: RetryPolicy,
    ) -> Self {
        Self::new(InputSourceKind::Redis {
            host: host.into(),
            routes,
            port,
            retry,
        })
    }

    pub(crate) fn channel(fanouts: BTreeMap<VarName, Rc<Fanout<V>>>) -> Self {
        Self::channel_with_control(fanouts, None)
    }

    pub(crate) fn channel_with_control(
        fanouts: BTreeMap<VarName, Rc<Fanout<V>>>,
        control: Option<Rc<Fanout<Value>>>,
    ) -> Self {
        Self::new(InputSourceKind::Channel { fanouts, control })
    }

    fn record_catalog_ownership(
        &self,
        source_id: &SourceId,
        requested: &BTreeSet<VarName>,
        ownership: &mut BTreeMap<VarName, SourceId>,
    ) -> anyhow::Result<()> {
        let mut record = |variable: &VarName| -> anyhow::Result<()> {
            if !requested.contains(variable) {
                return Ok(());
            }
            if let Some(previous) = ownership.get(variable) {
                anyhow::ensure!(
                    previous == source_id,
                    "input variable `{variable}` has multiple catalog owners: [`{previous}`, `{source_id}`]"
                );
            } else {
                ownership.insert(variable.clone(), source_id.clone());
            }
            Ok(())
        };

        match &self.kind {
            InputSourceKind::File { .. }
            | InputSourceKind::Mqtt { routes: None, .. }
            | InputSourceKind::Redis { routes: None, .. } => {}
            InputSourceKind::InMemoryRows { columns } => {
                for variable in requested {
                    if columns.contains_key(variable) {
                        record(variable)?;
                    }
                }
            }
            InputSourceKind::InMemoryTicks { batches } => {
                for batch in batches {
                    for update in batch.updates() {
                        record(update.variable)?;
                    }
                }
            }
            InputSourceKind::Ros { routes, .. }
            | InputSourceKind::Mqtt {
                routes: Some(routes),
                ..
            }
            | InputSourceKind::Redis {
                routes: Some(routes),
                ..
            } => {
                for variable in requested {
                    if routes.contains_key(variable) {
                        record(variable)?;
                    }
                }
            }
            InputSourceKind::RedisKnowledge(config) => {
                for variable in requested {
                    if config.keys.contains_key(variable) {
                        record(variable)?;
                    }
                }
            }
            InputSourceKind::Channel { fanouts, .. } => {
                for variable in requested {
                    if fanouts.contains_key(variable) {
                        record(variable)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn route_for(&self, variable: &VarName) -> Option<Route> {
        match &self.kind {
            InputSourceKind::Ros { routes, .. }
            | InputSourceKind::Mqtt {
                routes: Some(routes),
                ..
            }
            | InputSourceKind::Redis {
                routes: Some(routes),
                ..
            } => routes.get(variable).cloned(),
            InputSourceKind::Mqtt { routes: None, .. }
            | InputSourceKind::Redis { routes: None, .. } => Some(
                Route::new(variable.to_string(), None).expect("variable names are valid routes"),
            ),
            InputSourceKind::RedisKnowledge(config) => config
                .keys
                .get(variable)
                .map(|key| Route::new(key.clone(), None).expect("configured keys are nonempty")),
            InputSourceKind::File { .. }
            | InputSourceKind::InMemoryRows { .. }
            | InputSourceKind::InMemoryTicks { .. }
            | InputSourceKind::Channel { .. } => None,
        }
    }

    fn supports_default_route(&self) -> bool {
        matches!(
            self.kind,
            InputSourceKind::File { .. }
                | InputSourceKind::Mqtt { routes: None, .. }
                | InputSourceKind::Redis { routes: None, .. }
        )
    }

    fn requires_route_codec(&self) -> bool {
        matches!(self.kind, InputSourceKind::Ros { .. })
    }

    fn validate_binding(&self, binding: &InputBinding) -> anyhow::Result<()> {
        let route = binding.route();
        anyhow::ensure!(
            !route.address().trim().is_empty(),
            "input route for `{}` cannot be empty",
            binding.variable()
        );
        let format = route.format().ok_or_else(|| {
            anyhow::anyhow!("input format for `{}` is missing", binding.variable())
        })?;
        anyhow::ensure!(
            !format.as_str().trim().is_empty(),
            "input format for `{}` cannot be empty",
            binding.variable()
        );
        match &self.kind {
            InputSourceKind::Mqtt { .. } => crate::io::mqtt::validate_input_format(format)?,
            #[cfg(feature = "redis")]
            InputSourceKind::Redis { .. } => crate::io::redis::validate_input_format(format)?,
            _ => {}
        }
        if matches!(&self.kind, InputSourceKind::RedisKnowledge(_)) {
            anyhow::ensure!(
                matches!(format.as_str(), "json" | "json5"),
                "Redis knowledge input for `{}` supports JSON5 decoding only (`json` is a compatibility alias); format `{}` is unsupported",
                binding.variable(),
                format
            );
        }
        Ok(())
    }

    fn validate_bindings(&self, bindings: &[InputBinding]) -> anyhow::Result<()> {
        if matches!(self.kind, InputSourceKind::RedisKnowledge(_)) {
            let mut keys = BTreeMap::<&str, &VarName>::new();
            for binding in bindings {
                if let Some(previous) = keys.insert(binding.route().address(), binding.variable()) {
                    anyhow::bail!(
                        "active Redis knowledge key `{}` is mapped to both `{}` and `{}`",
                        binding.route().address(),
                        previous,
                        binding.variable()
                    );
                }
            }
        }
        Ok(())
    }

    pub(crate) fn supports_reconfiguration(&self) -> bool {
        matches!(
            self.kind,
            InputSourceKind::Mqtt { .. }
                | InputSourceKind::Redis { .. }
                | InputSourceKind::Ros { .. }
                | InputSourceKind::Channel {
                    control: Some(_),
                    ..
                }
        )
    }

    async fn open(
        self,
        bindings: Vec<InputBinding>,
        variables: BTreeSet<VarName>,
    ) -> anyhow::Result<InputStream<V>>
    where
        V: FileInputValue + RosStreamValue,
    {
        let routes = bindings
            .into_iter()
            .map(|binding| (binding.variable().clone(), binding.route().clone()))
            .collect::<BTreeMap<_, _>>();
        let kind = self.kind;
        let _span = tracing::debug_span!("open input source").entered();
        match kind {
            InputSourceKind::File { path } => {
                let packed = crate::parse_file(
                    |contents| {
                        crate::lang::untimed_input::parser::packed_untimed_input::<V>(
                            contents,
                            variables.clone(),
                        )
                        .map_err(|error| error.to_string())
                    },
                    &path,
                )
                .await
                .map_err(|error| {
                    anyhow::anyhow!(error).context("input file could not be parsed")
                })?;
                Ok(crate::io::file::packed_input_stream(packed))
            }
            InputSourceKind::InMemoryRows { columns } => {
                let columns = columns
                    .into_iter()
                    .filter(|(variable, _)| variables.contains(variable))
                    .collect();
                crate::io::map::typed_input_stream(columns)
            }
            InputSourceKind::InMemoryTicks { batches } => Ok(Box::pin(stream! {
                for batch in batches {
                    let batch = batch.select_variables(&variables)?;
                    if !batch.is_empty() {
                        yield Ok(batch);
                    }
                }
            })),
            InputSourceKind::Ros { executor, .. } => {
                cfg_select! {
                    feature = "ros" => {
                    let mapping = routes
                        .into_iter()
                        .map(|(variable, route)| {
                            let codec = route
                                    .format()
                                    .map(ToString::to_string)
                                    .ok_or_else(|| {
                                        anyhow::anyhow!(
                                            "ROS route for `{variable}` requires a route format"
                                        )
                                    })?;
                            Ok((variable.to_string(), (route.address().to_string(), codec)))
                        })
                        .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
                    Ok(V::open_ros_input(executor, mapping)?.0)
                    },
                    _ => {
                        let _ = (executor, routes);
                        anyhow::bail!("ROS support not enabled")
                    },
                }
            }
            InputSourceKind::Mqtt {
                host,
                port,
                protocol,
                retry,
                ..
            } => {
                let topics = routes
                    .into_iter()
                    .map(|(variable, route)| (variable, route.address().to_string()))
                    .collect();
                protocol.open_data::<V>(&host, port, topics, retry).await
            }
            InputSourceKind::Redis {
                host, port, retry, ..
            } => {
                cfg_select! {
                    feature = "redis" => {
                        let topics = routes
                            .into_iter()
                            .map(|(variable, route)| (variable, route.address().to_string()))
                            .collect();
                        let (mut items, mut owner) = crate::io::redis::open_owned_input_stream_items(
                            &host, port, topics, None, retry,
                        ).await?;
                        Ok(Box::pin(async_stream::try_stream! {
                            while let Some(item) = items.next().await {
                                match item? {
                                    crate::io::redis::RedisInputItem::Data(batch) => yield batch,
                                    crate::io::redis::RedisInputItem::Control(_) => unreachable!("data-only Redis stream cannot receive control"),
                                    crate::io::redis::RedisInputItem::Boundary(_) => unreachable!("data-only Redis stream cannot receive boundary"),
                                }
                            }
                            owner.shutdown().await?;
                        }))
                    },
                    _ => {
                        let _ = (host, port, routes, retry);
                        anyhow::bail!("Redis support not enabled")
                    },
                }
            }
            InputSourceKind::RedisKnowledge(config) => {
                cfg_select! {
                    feature = "redis" => {
                        let keys = routes
                            .into_iter()
                            .map(|(variable, route)| (variable, route.address().to_string()))
                            .collect();
                        let (mut stream, mut owner) =
                            open_configured_redis_knowledge::<V>(config, keys).await?;
                        Ok(Box::pin(async_stream::try_stream! {
                            while let Some(batch) = stream.next().await {
                                yield batch?;
                            }
                            owner.shutdown().await?;
                        }))
                    },
                    _ => {
                        let _ = (config, routes);
                        anyhow::bail!("Redis support not enabled")
                    },
                }
            }
            InputSourceKind::Channel { fanouts, .. } => {
                let streams = fanouts
                    .into_iter()
                    .filter(|(variable, _)| variables.contains(variable))
                    .map(|(variable, fanout)| {
                        let mut receiver = fanout.subscribe();
                        let stream: crate::LocalStream<V> = Box::pin(stream! {
                            while let Some(value) = receiver.recv().await {
                                yield value;
                            }
                        });
                        (variable, stream)
                    })
                    .collect();
                Ok(crate::io::channel::from_streams(streams))
            }
        }
    }

    async fn open_with_control_owner(
        self,
        bindings: Vec<InputBinding>,
        variables: BTreeSet<VarName>,
        control_route: Box<str>,
    ) -> anyhow::Result<(ReconfigurableInputStream<V>, Option<InputSourceControl>)>
    where
        V: FileInputValue + RosStreamValue,
    {
        let routes = bindings
            .into_iter()
            .map(|binding| (binding.variable().clone(), binding.route().clone()))
            .collect::<BTreeMap<_, _>>();
        let kind = self.kind;
        match kind {
            InputSourceKind::Mqtt {
                host,
                port,
                protocol,
                retry,
                ..
            } => {
                let topics = routes
                    .into_iter()
                    .map(|(variable, route)| (variable, route.address().to_string()))
                    .collect();
                let (stream, owner) = protocol
                    .open_reconfigurable(&host, port, topics, retry, control_route.to_string())
                    .await?;
                let stream: ReconfigurableInputStream<V> = Box::pin(stream.map(|item| {
                    item.map(|item| match item {
                        crate::io::mqtt::MqttInputItem::Data(batch) => {
                            ReconfigurableInputItem::Data(batch)
                        }
                        crate::io::mqtt::MqttInputItem::Control(config) => {
                            ReconfigurableInputItem::Reconfigure(config)
                        }
                        crate::io::mqtt::MqttInputItem::Boundary(id) => {
                            ReconfigurableInputItem::Boundary(id)
                        }
                    })
                }));
                Ok((stream, Some(InputSourceControl::Rumqttc(owner))))
            }
            InputSourceKind::Redis {
                host, port, retry, ..
            } => {
                cfg_select! {
                    feature = "redis" => {
                        let topics = routes
                            .into_iter()
                            .map(|(variable, route)| (variable, route.address().to_string()))
                            .collect();
                        let (stream, owner) = crate::io::redis::open_owned_input_stream_items(
                            &host,
                            port,
                            topics,
                            Some(control_route.to_string()),
                            retry,
                        )
                        .await?;
                        let stream: ReconfigurableInputStream<V> = Box::pin(stream.map(|item| {
                            item.map(|item| match item {
                                crate::io::redis::RedisInputItem::Data(batch) => {
                                    ReconfigurableInputItem::Data(batch)
                                }
                                crate::io::redis::RedisInputItem::Control(config) => {
                                    ReconfigurableInputItem::Reconfigure(config)
                                }
                                crate::io::redis::RedisInputItem::Boundary(id) => {
                                    ReconfigurableInputItem::Boundary(id)
                                }
                            })
                        }));
                        Ok((stream, Some(InputSourceControl::Redis(owner))))
                    },
                    _ => {
                        let _ = (host, port, retry, routes, control_route);
                        anyhow::bail!("Redis support not enabled")
                    },
                }
            }
            InputSourceKind::Ros { executor, .. } => {
                validate_ros_control_route(&routes, control_route.as_ref())?;
                cfg_select! {
                    feature = "ros" => {
                    let active_topics = routes
                        .iter()
                        .map(|(variable, route)| (variable.clone(), route.address().to_owned()))
                        .collect();
                    let mapping = routes
                        .into_iter()
                        .map(|(variable, route)| {
                            let codec = route
                                    .format()
                                    .map(ToString::to_string)
                                    .ok_or_else(|| {
                                        anyhow::anyhow!(
                                            "ROS route for `{variable}` requires a route format"
                                        )
                                    })?;
                            Ok((variable.to_string(), (route.address().to_string(), codec)))
                        })
                        .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
                    let (data, data_owner) = if mapping.is_empty() {
                        (None, None)
                    } else {
                        let (stream, owner) = V::open_ros_input(executor.clone(), mapping)?;
                        (Some(stream), Some(owner))
                    };
                    let (control, control_owner) =
                        crate::io::ros::control_stream(executor, control_route.to_string())?;
                    let controls = data_owner.into_iter().chain([control_owner]).collect();
                    Ok((controlled_input_stream(data, control), Some(InputSourceControl::Ros {
                        controls,
                        active_topics,
                    })))
                    },
                    _ => {
                        let _ = (executor, routes, variables, control_route);
                        anyhow::bail!("ROS support not enabled")
                    },
                }
            }
            InputSourceKind::RedisKnowledge(_) => {
                anyhow::bail!(
                    "Redis knowledge sources do not support reconfiguration control; use a Redis Pub/Sub, MQTT, ROS, or channel control source"
                )
            }
            InputSourceKind::Channel { fanouts, control } => {
                let Some(control) = control else {
                    anyhow::bail!("channel input has no configured control source")
                };
                let (stream, owner) =
                    crate::io::channel::reconfigurable_stream(fanouts, variables, control);
                Ok((stream, Some(InputSourceControl::Channel(owner))))
            }
            InputSourceKind::File { .. }
            | InputSourceKind::InMemoryRows { .. }
            | InputSourceKind::InMemoryTicks { .. } => {
                anyhow::bail!("file and in-memory row/tick sources do not support reconfiguration")
            }
        }
    }

    async fn open_reconfigurable_owned(
        self,
        bindings: Vec<InputBinding>,
        variables: BTreeSet<VarName>,
    ) -> anyhow::Result<(ReconfigurableInputStream<V>, Option<InputSourceControl>)>
    where
        V: FileInputValue + RosStreamValue,
    {
        let routes = bindings
            .iter()
            .map(|binding| (binding.variable().clone(), binding.route().clone()))
            .collect::<BTreeMap<_, _>>();
        match self.kind {
            InputSourceKind::Mqtt {
                host,
                port,
                protocol,
                retry,
                ..
            } => {
                let topics = routes
                    .into_iter()
                    .map(|(variable, route)| (variable, route.address().to_owned()))
                    .collect();
                let (stream, owner) = protocol
                    .open_owned_items(&host, port, topics, retry)
                    .await?;
                let stream = Box::pin(stream.map(|item| {
                    item.map(|item| match item {
                        crate::io::mqtt::MqttInputItem::Data(batch) => {
                            ReconfigurableInputItem::Data(batch)
                        }
                        crate::io::mqtt::MqttInputItem::Boundary(id) => {
                            ReconfigurableInputItem::Boundary(id)
                        }
                        crate::io::mqtt::MqttInputItem::Control(_) => {
                            unreachable!("MQTT source without a control route cannot emit control")
                        }
                    })
                }));
                Ok((stream, Some(InputSourceControl::Rumqttc(owner))))
            }
            InputSourceKind::Redis {
                host, port, retry, ..
            } => {
                cfg_select! {
                    feature = "redis" => {
                        let topics = routes
                            .into_iter()
                            .map(|(variable, route)| (variable, route.address().to_owned()))
                            .collect();
                        let (stream, owner) = crate::io::redis::open_owned_input_stream_items(
                            &host, port, topics, None, retry,
                        ).await?;
                        let stream = Box::pin(stream.map(|item| {
                            item.map(|item| match item {
                                crate::io::redis::RedisInputItem::Data(batch) => ReconfigurableInputItem::Data(batch),
                                crate::io::redis::RedisInputItem::Boundary(id) => ReconfigurableInputItem::Boundary(id),
                                crate::io::redis::RedisInputItem::Control(_) => unreachable!("Redis source without a control route cannot emit control"),
                            })
                        }));
                        Ok((stream, Some(InputSourceControl::Redis(owner))))
                    },
                    _ => {
                        let _ = (host, port, retry, routes);
                        anyhow::bail!("Redis support not enabled")
                    },
                }
            }
            kind => {
                let source = Self {
                    kind,
                    reconfiguration_route: self.reconfiguration_route,
                };
                let (stream, owner) = source.open_owned(bindings, variables).await?;
                Ok((
                    Box::pin(stream.map(|item| {
                        item.map(ReconfigurableInputItem::Data)
                            .map_err(anyhow::Error::from)
                    })),
                    owner,
                ))
            }
        }
    }
}

#[cfg(any(feature = "ros", test))]
enum ControlledInputNext<V> {
    Data(Result<InputBatch<V>, crate::InputError>),
    Control(anyhow::Result<ReconfigurationRequest>),
    Complete,
}

#[cfg(any(feature = "ros", test))]
fn poll_controlled_input<V>(
    data: &mut Option<InputStream<V>>,
    control: &mut Option<crate::LocalStream<anyhow::Result<ReconfigurationRequest>>>,
    cx: &mut std::task::Context<'_>,
) -> std::task::Poll<ControlledInputNext<V>> {
    // Check control first for responsiveness only; independent ROS/channel
    // streams still have no ordering edge at this boundary.
    if let Some(stream) = control.as_mut() {
        match stream.as_mut().poll_next(cx) {
            std::task::Poll::Ready(Some(request)) => {
                return std::task::Poll::Ready(ControlledInputNext::Control(request));
            }
            std::task::Poll::Ready(None) => *control = None,
            std::task::Poll::Pending => {}
        }
    }

    if let Some(stream) = data.as_mut() {
        match stream.as_mut().poll_next(cx) {
            std::task::Poll::Ready(Some(batch)) => {
                return std::task::Poll::Ready(ControlledInputNext::Data(batch));
            }
            std::task::Poll::Ready(None) => *data = None,
            std::task::Poll::Pending => {}
        }
    }

    if control.is_none() && data.is_none() {
        std::task::Poll::Ready(ControlledInputNext::Complete)
    } else {
        std::task::Poll::Pending
    }
}

fn validate_ros_control_route(
    data_routes: &BTreeMap<VarName, Route>,
    control_route: &str,
) -> anyhow::Result<()> {
    for (variable, route) in data_routes {
        if route.address() == control_route {
            anyhow::bail!(
                "ROS control topic `{control_route}` collides with data topic for variable `{variable}`"
            );
        }
    }
    Ok(())
}

fn contextualize_reconfigurable_stream<V: 'static>(
    stream: ReconfigurableInputStream<V>,
    source: SourceId,
) -> ReconfigurableInputStream<V> {
    Box::pin(stream.map(move |item| {
        item.map_err(|error| error.context(format!("input source `{source}` emitted an error")))
    }))
}

#[cfg(any(feature = "ros", test))]
fn controlled_input_stream<V: 'static>(
    mut data: Option<InputStream<V>>,
    control: crate::LocalStream<anyhow::Result<ReconfigurationRequest>>,
) -> ReconfigurableInputStream<V> {
    Box::pin(async_stream::try_stream! {
        let mut control = Some(control);
        loop {
            match futures::future::poll_fn(|cx| {
                poll_controlled_input(&mut data, &mut control, cx)
            })
            .await
            {
                ControlledInputNext::Control(request) => {
                    yield ReconfigurableInputItem::Reconfigure(request?);
                }
                ControlledInputNext::Data(batch) => {
                    yield ReconfigurableInputItem::Data(batch?);
                }
                ControlledInputNext::Complete => return,
            }
        }
    })
}

/// Resolves reusable source descriptions and opens one logical input stream.
///
/// [`Self::build`] is the ordinary public boundary: it first validates that the
/// configured sources cover the declared model inputs, then opens and composes
/// only the selected sources. Source descriptions remain resource-free until
/// that call.
///
/// ```
/// use std::collections::BTreeSet;
///
/// use futures::StreamExt;
/// use trustworthiness_checker::{InputBatch, Value, VarName};
/// use trustworthiness_checker::io::{InputPipeline, InputSource};
///
/// # fn main() -> anyhow::Result<()> {
/// smol::block_on(async {
///     let source = InputSource::in_memory_ticks([
///         InputBatch::update("x", Value::Int(4)),
///         InputBatch::update("x", Value::Int(8)),
///     ]);
///     let pipeline = InputPipeline::new(source);
///     let mut input = pipeline
///         .build(BTreeSet::from([VarName::new("x")]))
///         .await?;
///
///     let first = input.next().await.expect("first configured batch")?;
///     let second = input.next().await.expect("second configured batch")?;
///     assert_eq!((first.tick_count(), second.tick_count()), (1, 1));
///     assert!(input.next().await.is_none());
///     Ok(())
/// })
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct InputPipeline<V = Value> {
    sources: InputSources<V>,
    policy: Option<InputPolicy>,
    executor: Option<Rc<LocalExecutor<'static>>>,
    generation: PipelineGeneration,
}

impl<V> InputPipeline<V> {
    pub fn new(source: InputSource<V>) -> Self {
        Self {
            sources: InputSources::single(source),
            policy: None,
            executor: None,
            generation: PipelineGeneration::new(),
        }
    }

    pub fn from_sources(sources: InputSources<V>) -> Self {
        Self {
            sources,
            policy: None,
            executor: None,
            generation: PipelineGeneration::new(),
        }
    }

    pub fn sources(&self) -> &InputSources<V> {
        &self.sources
    }

    /// Attach the local executor used only by live source ingress relays.
    pub fn with_executor(mut self, executor: Rc<LocalExecutor<'static>>) -> Self {
        self.executor = Some(executor);
        self
    }

    pub fn with_policy(mut self, policy: InputPolicy) -> anyhow::Result<Self> {
        anyhow::ensure!(
            policy.window().is_bounded(),
            "input window requires max_delay or update_limit"
        );
        anyhow::ensure!(
            self.policy.is_none(),
            "input pipeline already has an input policy"
        );
        self.policy = Some(policy);
        self.generation = PipelineGeneration::new();
        Ok(self)
    }

    pub fn policy(&self) -> Option<&InputPolicy> {
        self.policy.as_ref()
    }

    fn validate_resolved(&self, resolved: &ResolvedInput) -> anyhow::Result<()> {
        resolved.validate_for_pipeline(self.generation)?;

        let mut source_ids = BTreeSet::new();
        let mut variables = BTreeSet::new();
        for source_plan in resolved.sources() {
            anyhow::ensure!(
                source_ids.insert(source_plan.source().clone()),
                "resolved input source `{}` is duplicated",
                source_plan.source()
            );
            let source = self
                .sources
                .sources
                .get(source_plan.source())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "resolved input source `{}` is not registered",
                        source_plan.source()
                    )
                })?;
            source.validate_bindings(source_plan.bindings())?;
            for binding in source_plan.bindings() {
                source.validate_binding(binding)?;
                anyhow::ensure!(
                    variables.insert(binding.variable().clone()),
                    "resolved input variable `{}` is assigned to multiple source owners",
                    binding.variable()
                );
            }
        }
        Ok(())
    }

    /// Plan the source-owner changes between two pure resolutions.
    ///
    /// Unchanged source IDs are implicit. Native live transports retain their
    /// owner across binding changes; distinct source identities are opened or
    /// removed explicitly.
    pub(crate) fn plan_reconfiguration(
        &self,
        active: &ResolvedInput,
        candidate: ResolvedInput,
        session: SessionId,
        expected_revision: SessionRevision,
    ) -> anyhow::Result<InputPipelineReconfigurationPlan> {
        self.validate_resolved(active)?;
        self.validate_resolved(&candidate)?;
        let active_by_id = active
            .sources()
            .iter()
            .map(|source| (source.source(), source))
            .collect::<BTreeMap<_, _>>();
        let candidate_by_id = candidate
            .sources()
            .iter()
            .map(|source| (source.source(), source))
            .collect::<BTreeMap<_, _>>();
        let mut removed = Vec::new();
        let mut added = Vec::new();
        let mut rebound = Vec::new();

        for active_source in active.sources() {
            match candidate_by_id.get(active_source.source()) {
                Some(candidate_source) if active_source == *candidate_source => {}
                Some(candidate_source) => {
                    let source = self
                        .sources
                        .sources
                        .get(active_source.source())
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "input source `{}` is not registered",
                                active_source.source()
                            )
                        })?;
                    match &source.kind {
                        InputSourceKind::Mqtt { .. } | InputSourceKind::Redis { .. } => {
                            rebound.push((*candidate_source).clone());
                        }
                        InputSourceKind::Channel { .. } => {
                            rebound.push((*candidate_source).clone())
                        }
                        _ => anyhow::bail!(
                            "input source `{}` cannot change bindings in place; use a distinct source ID",
                            active_source.source()
                        ),
                    }
                }
                None => removed.push(active_source.source().clone()),
            }
        }
        for candidate_source in candidate.sources() {
            match active_by_id.get(candidate_source.source()) {
                Some(active_source) if *active_source == candidate_source => {}
                None => added.push(candidate_source.clone()),
                Some(_) => {}
            }
        }

        Ok(InputPipelineReconfigurationPlan {
            session,
            expected_revision,
            candidate,
            removed: removed.into_boxed_slice(),
            added: added.into_boxed_slice(),
            rebound: rebound.into_boxed_slice(),
        })
    }

    pub(crate) fn ensure_reconfigurable(
        &self,
        requested_route: Option<&str>,
    ) -> anyhow::Result<()> {
        // Only the selected control source is opened by a reconfigurable
        // runtime. Other configured sources may remain inactive, including
        // sources that cannot carry a live control route.
        self.sources
            .resolve_reconfiguration_source(requested_route)
            .map(|_| ())
    }

    pub(crate) fn resolve(
        &self,
        model_inputs: &BTreeSet<VarName>,
        input_configuration: Option<&InputConfiguration>,
    ) -> anyhow::Result<ResolvedInput> {
        let resolved = match input_configuration {
            Some(config) => self
                .sources
                .resolve_input_configuration(config, model_inputs),
            None => self.sources.resolve_default(model_inputs),
        }?;
        Ok(resolved.attach_to_pipeline(self.generation))
    }

    pub fn into_sources(self) -> InputSources<V> {
        self.sources
    }

    pub async fn build(&self, input_vars: BTreeSet<VarName>) -> anyhow::Result<OpenedInput<V>>
    where
        V: FileInputValue + RosStreamValue,
    {
        let resolved = self.resolve(&input_vars, None)?;
        self.open(resolved).await
    }

    pub(crate) async fn open(&self, resolved: ResolvedInput) -> anyhow::Result<OpenedInput<V>>
    where
        V: FileInputValue + RosStreamValue,
    {
        self.validate_resolved(&resolved)?;
        let mut opened: Vec<OpenedInputSource<V>> = Vec::new();
        for source_plan in resolved.sources() {
            let source = self
                .sources
                .sources
                .get(source_plan.source())
                .ok_or_else(|| {
                    anyhow::anyhow!("input source `{}` is not registered", source_plan.source())
                })?;
            let description = source_plan.source().clone();
            let bindings = source_plan.bindings().to_vec();
            let variables = bindings
                .iter()
                .map(|binding| binding.variable().clone())
                .collect::<BTreeSet<_>>();
            let finite = source.is_finite();
            let executor =
                if finite {
                    None
                } else {
                    Some(self.executor.as_ref().ok_or_else(|| anyhow::anyhow!(
                    "live input source `{description}` requires InputPipeline::with_executor"
                ))?)
                };
            let (source_stream, control) = match source
                .clone()
                .open_owned(bindings, variables)
                .await
                .with_context(|| format!("input source `{description}` could not be opened"))
            {
                Ok(opened_source) => opened_source,
                Err(primary) => {
                    let mut error = primary;
                    for source in opened {
                        for cleanup in source.close().await {
                            error =
                                error.context(format!("input cleanup also failed: {cleanup:#}"));
                        }
                    }
                    return Err(error);
                }
            };
            let stream_description = description.clone();
            let stream: ReconfigurableInputStream<V> = Box::pin(async_stream::stream! {
                let mut source_stream = source_stream;
                while let Some(result) = source_stream.next().await {
                    yield result
                        .map(ReconfigurableInputItem::Data)
                        .map_err(|error| anyhow::Error::new(error).context(
                            format!("input source `{stream_description}` emitted an error")
                        ));
                }
            });
            let source = if finite {
                OpenedInputSource::direct(description, stream)
            } else {
                OpenedInputSource::relay_with_control(
                    description,
                    stream,
                    Rc::clone(executor.unwrap()),
                    control,
                )
            };
            opened.push(source);
        }
        let sources = Rc::new(RefCell::new(InputSourceSet::new(opened)));
        let mut raw: ReconfigurableInputStream<V> =
            Box::pin(SharedInputSourceSet(Rc::clone(&sources)));
        let failure_sources = Rc::clone(&sources);
        let stream: InputStream<V> = Box::pin(async_stream::stream! {
            while let Some(item) = raw.next().await {
                let item = match item {
                    Ok(item) => item,
                    Err(primary) => {
                        failure_sources.borrow_mut().stop_all();
                        let mut error = primary;
                        while let Some(cleanup_item) = raw.next().await {
                            if let Err(cleanup) = cleanup_item {
                                error = error.context(format!("input cleanup also failed: {cleanup:#}"));
                            }
                        }
                        yield Err(crate::core::InputError::from(error));
                        return;
                    }
                };
                yield match item {
                        ReconfigurableInputItem::Data(batch) => Ok(batch),
                        ReconfigurableInputItem::Reconfigure(_) | ReconfigurableInputItem::Boundary(_) => {
                            Err(crate::core::InputError::source("ordinary input received a lifecycle control item"))
                        }
                    };
            }
        });
        let stream = match self.policy.clone() {
            Some(policy) => crate::io::aggregation::apply_policy(stream, policy)?,
            None => stream,
        };
        Ok(OpenedInput::new(stream, sources))
    }

    async fn open_reconfigurable_source_plan(
        &self,
        source_plan: &ResolvedSource,
        control: &ReconfigurationControl,
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<OpenedInputSource<V>>
    where
        V: FileInputValue + RosStreamValue,
    {
        let id = source_plan.source().clone();
        if source_plan.source() == &control.source {
            let source = self
                .sources
                .sources
                .get(source_plan.source())
                .ok_or_else(|| anyhow::anyhow!("input source `{id}` is not registered"))?
                .clone();
            let bindings = source_plan.bindings().to_vec();
            let variables = bindings
                .iter()
                .map(|binding| binding.variable().clone())
                .collect();
            let (stream, owner) = source
                .open_with_control_owner(bindings, variables, control.route.clone())
                .await
                .with_context(|| format!("reconfiguration source `{id}` could not be opened"))?;
            let stream = contextualize_reconfigurable_stream(stream, id.clone());
            return Ok(OpenedInputSource::relay_with_control(
                id, stream, executor, owner,
            ));
        }
        let source = self
            .sources
            .sources
            .get(source_plan.source())
            .ok_or_else(|| anyhow::anyhow!("input source `{id}` is not registered"))?
            .clone();
        let bindings = source_plan.bindings().to_vec();
        let variables = bindings
            .iter()
            .map(|binding| binding.variable().clone())
            .collect();
        let (stream, owner) = source
            .open_reconfigurable_owned(bindings, variables)
            .await
            .with_context(|| format!("input source `{id}` could not be opened"))?;
        let stream = contextualize_reconfigurable_stream(stream, id.clone());
        Ok(OpenedInputSource::relay_with_control(
            id, stream, executor, owner,
        ))
    }

    pub(crate) async fn open_reconfigurable_source_plans(
        &self,
        plans: &[ResolvedSource],
        control: &ReconfigurationControl,
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<Vec<OpenedInputSource<V>>>
    where
        V: FileInputValue + RosStreamValue,
    {
        let mut opened = Vec::with_capacity(plans.len());
        for plan in plans {
            match self
                .open_reconfigurable_source_plan(plan, control, Rc::clone(&executor))
                .await
            {
                Ok(source) => opened.push(source),
                Err(primary) => {
                    let mut error = primary;
                    for source in opened {
                        for cleanup in source.close().await {
                            error =
                                error.context(format!("input cleanup also failed: {cleanup:#}"));
                        }
                    }
                    return Err(error);
                }
            }
        }
        Ok(opened)
    }

    pub(crate) async fn open_reconfigurable_sources(
        &self,
        resolved: &ResolvedInput,
        control: &ReconfigurationControl,
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<Vec<OpenedInputSource<V>>>
    where
        V: FileInputValue + RosStreamValue,
    {
        self.validate_resolved(resolved)?;
        let mut opened = self
            .open_reconfigurable_source_plans(resolved.sources(), control, Rc::clone(&executor))
            .await?;
        if opened.iter().all(|source| source.id != control.source) {
            let control_plan = ResolvedSource::new(control.source.clone(), []);
            match self
                .open_reconfigurable_source_plan(&control_plan, control, executor)
                .await
            {
                Ok(source) => opened.push(source),
                Err(primary) => {
                    let mut error = primary;
                    for source in opened {
                        for cleanup in source.close().await {
                            error =
                                error.context(format!("input cleanup also failed: {cleanup:#}"));
                        }
                    }
                    return Err(error);
                }
            }
        }
        Ok(opened)
    }

    #[cfg(test)]
    pub(crate) async fn open_reconfigurable(
        &self,
        resolved: ResolvedInput,
        control: &ReconfigurationControl,
    ) -> anyhow::Result<ReconfigurableInputStream<V>>
    where
        V: FileInputValue + RosStreamValue,
    {
        let executor = self.executor.as_ref().ok_or_else(|| {
            anyhow::anyhow!("reconfigurable input requires InputPipeline::with_executor")
        })?;
        let sources = self
            .open_reconfigurable_sources(&resolved, control, Rc::clone(executor))
            .await?;
        Ok(Box::pin(SharedInputSourceSet(Rc::new(RefCell::new(
            InputSourceSet::new(sources),
        )))))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InputPipelineReconfigurationPlan {
    session: SessionId,
    expected_revision: SessionRevision,
    candidate: ResolvedInput,
    removed: Box<[SourceId]>,
    added: Box<[ResolvedSource]>,
    rebound: Box<[ResolvedSource]>,
}

impl InputPipelineReconfigurationPlan {
    pub(crate) fn candidate(&self) -> &ResolvedInput {
        &self.candidate
    }

    pub(crate) fn is_changed(&self) -> bool {
        !self.removed.is_empty() || !self.added.is_empty() || !self.rebound.is_empty()
    }

    pub(crate) fn removed_sources(&self) -> &[SourceId] {
        &self.removed
    }

    pub(crate) fn added_sources(&self) -> &[ResolvedSource] {
        &self.added
    }

    pub(crate) fn rebound_sources(&self) -> &[ResolvedSource] {
        &self.rebound
    }

    pub(crate) fn session(&self) -> SessionId {
        self.session
    }

    pub(crate) fn expected_revision(&self) -> SessionRevision {
        self.expected_revision
    }
}

#[cfg(test)]
mod resolution_tests {
    use std::cell::Cell;
    use std::num::NonZeroUsize;

    use super::*;

    #[test]
    fn cross_value_mapping_retains_stop_and_drain_lifecycle() {
        smol::block_on(async {
            let stopped = Rc::new(Cell::new(false));
            let stop_flag = Rc::clone(&stopped);
            let stream: InputStream<Value> = Box::pin(futures::stream::iter([Ok(
                InputBatch::update("x", Value::Int(3)),
            )]));
            let input = OpenedInput::with_stop(stream, move || stop_flag.set(true));
            let mapped = input.map_values(|stream| {
                Box::pin(
                    stream
                        .map(|item| item.map(|batch| batch.map_values(|value| value.to_string()))),
                )
            });
            let mut drain = mapped.into_drain();
            assert!(stopped.get());
            assert_eq!(
                drain
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .updates()
                    .next()
                    .unwrap()
                    .value,
                "3"
            );
            assert!(drain.next().await.is_none());
        });
    }

    fn mqtt_routes(route: &str) -> BTreeMap<VarName, Route> {
        BTreeMap::from([(
            VarName::new("x"),
            Route::new(route.to_owned().into_boxed_str(), None).unwrap(),
        )])
    }

    #[test]
    fn non_knowledge_mstlo_pipeline_does_not_need_redis_capabilities() {
        use crate::runtime::mstlo::{MstloTimedValue, MstloValue, TimedValue};

        let value = TimedValue::new(std::time::Duration::from_millis(1), MstloValue::Float(1.0));
        let source =
            InputSource::<MstloTimedValue>::in_memory_ticks([InputBatch::update("x", value)]);
        let mut stream =
            smol::block_on(InputPipeline::new(source).build(BTreeSet::from([VarName::new("x")])))
                .unwrap();
        let batch = smol::block_on(stream.next()).unwrap().unwrap();
        assert_eq!(
            batch.updates().next().unwrap().value.value,
            MstloValue::Float(1.0)
        );
    }

    #[test]
    fn mstlo_input_config_rejects_redis_knowledge_before_opening() {
        use crate::runtime::mstlo::MstloTimedValue;

        let config: InputConfigFile = json5::from_str(
            r#"{
                sources: {
                    knowledge: {
                        kind: "redis-knowledge",
                        keys: {x: "knowledge:x"}
                    }
                }
            }"#,
        )
        .unwrap();
        let error = InputSources::<MstloTimedValue>::from_config(
            config,
            Rc::new(LocalExecutor::new()),
            None,
            None,
            MqttProtocol::default(),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("produces ordinary `Value` input")
        );
    }

    #[test]
    fn single_source_default_resolution_needs_no_source_id() {
        let pipeline =
            InputPipeline::<Value>::new(InputSource::mqtt(Some(mqtt_routes("/x")), None));
        let variables = BTreeSet::from([VarName::new("x")]);
        let resolved = pipeline.resolve(&variables, None).unwrap();
        assert_eq!(resolved.sources().len(), 1);
        assert_eq!(resolved.sources()[0].source(), "default");
        assert_eq!(resolved.sources()[0].bindings()[0].route().address(), "/x");
    }

    #[test]
    fn config_route_is_transferred_to_reusable_source() {
        let config: InputConfigFile = json5::from_str(
            r#"{
                default: "telemetry",
                sources: {
                    telemetry: {
                        kind: "mqtt",
                        reconfiguration_route: "monitor/reconfigure",
                        routes: { pressure: "/pressure" }
                    }
                }
            }"#,
        )
        .unwrap();
        let sources = InputSources::<Value>::from_config(
            config,
            Rc::new(LocalExecutor::new()),
            None,
            None,
            MqttProtocol::default(),
        )
        .unwrap();

        assert_eq!(
            sources.source("telemetry").unwrap().reconfiguration_route(),
            Some("monitor/reconfigure")
        );
    }

    #[test]
    fn explicit_source_wins_over_ambiguous_catalogs() {
        let sources = InputSources::<Value>::new()
            .insert(
                "telemetry",
                InputSource::mqtt(Some(mqtt_routes("/catalog/telemetry")), None),
            )
            .insert(
                "knowledge",
                InputSource::mqtt(Some(mqtt_routes("/catalog/knowledge")), None),
            );
        let pipeline = InputPipeline::from_sources(sources);
        let config = InputConfiguration {
            source: Some("telemetry".into()),
            inputs: Some(BTreeMap::from([(
                VarName::new("x"),
                Route::new("/replacement/x", None).unwrap(),
            )])),
            sources: None,
        };
        let variables = BTreeSet::from([VarName::new("x")]);
        let resolved = pipeline.resolve(&variables, Some(&config)).unwrap();
        assert_eq!(resolved.sources()[0].source(), "telemetry");
        assert_eq!(
            resolved.sources()[0].bindings()[0].route().address(),
            "/replacement/x"
        );
    }

    #[test]
    fn default_resolution_rejects_duplicate_catalog_ownership() {
        let sources = InputSources::<Value>::new()
            .insert(
                "telemetry",
                InputSource::mqtt(Some(mqtt_routes("/x")), None),
            )
            .insert(
                "knowledge",
                InputSource::mqtt(Some(mqtt_routes("/x")), None),
            );
        let pipeline = InputPipeline::from_sources(sources);
        let variables = BTreeSet::from([VarName::new("x")]);
        let error = pipeline.resolve(&variables, None).unwrap_err();
        assert!(error.to_string().contains("multiple catalog owners"));
    }

    #[test]
    fn explicit_input_bindings_must_cover_all_inputs() {
        let pipeline = InputPipeline::<Value>::new(InputSource::mqtt(None, None));
        let config = InputConfiguration {
            source: None,
            inputs: Some(BTreeMap::from([(
                VarName::new("x"),
                Route::new("/x", None).unwrap(),
            )])),
            sources: None,
        };
        let variables = BTreeSet::from([VarName::new("x"), VarName::new("y")]);
        let error = pipeline.resolve(&variables, Some(&config)).unwrap_err();
        assert!(error.to_string().contains("missing bindings"));
    }

    #[test]
    fn single_source_reconfiguration_defaults_to_reconf() {
        let pipeline = InputPipeline::<Value>::new(InputSource::mqtt(None, None));
        let resolved = pipeline
            .sources()
            .resolve_reconfiguration_source(None)
            .unwrap();
        assert_eq!(resolved.source_id(), "default");
        assert_eq!(resolved.route(), "reconf");
    }

    #[test]
    fn source_route_is_used_and_cli_route_overrides_it() {
        let source = InputSource::<Value>::mqtt(None, None)
            .with_reconfiguration_route("configured-reconf")
            .unwrap();
        let pipeline = InputPipeline::new(source);
        assert_eq!(
            pipeline
                .sources()
                .resolve_reconfiguration_source(None)
                .unwrap()
                .route(),
            "configured-reconf"
        );
        assert_eq!(
            pipeline
                .sources()
                .resolve_reconfiguration_source(Some("cli-reconf"))
                .unwrap()
                .route(),
            "cli-reconf"
        );
    }

    #[test]
    fn single_non_reconfigurable_source_has_contextual_error() {
        let pipeline = InputPipeline::<Value>::new(InputSource::file("trace.input".to_owned()));
        let error = pipeline
            .sources()
            .resolve_reconfiguration_source(None)
            .unwrap_err();
        assert!(error.to_string().contains(
            "the only configured input source `default` does not support reconfiguration"
        ));
    }

    #[test]
    fn one_declared_source_is_selected_from_multiple_sources() {
        let control = InputSource::<Value>::mqtt(None, None)
            .with_reconfiguration_route("control")
            .unwrap();
        let sources = InputSources::<Value>::new()
            .insert("data", InputSource::mqtt(None, None))
            .insert("control", control);
        let resolved = sources.resolve_reconfiguration_source(None).unwrap();
        assert_eq!(resolved.source_id(), "control");
        assert_eq!(resolved.route(), "control");
    }

    #[test]
    fn declared_non_reconfigurable_source_has_contextual_error() {
        let sources = InputSources::<Value>::new()
            .insert("data", InputSource::mqtt(None, None))
            .insert(
                "control",
                InputSource::file("trace.input".to_owned())
                    .with_reconfiguration_route("control")
                    .unwrap(),
            );
        let error = sources.resolve_reconfiguration_source(None).unwrap_err();
        assert!(error.to_string().contains(
            "input source `control` declares `reconfiguration_route` but does not support reconfiguration"
        ));
    }

    #[test]
    fn in_memory_row_open_propagates_unequal_column_error() {
        smol::block_on(async {
            let pipeline = InputPipeline::new(InputSource::in_memory_rows(BTreeMap::from([
                (VarName::new("x"), vec![Value::Int(1), Value::Int(2)]),
                (VarName::new("y"), vec![Value::Int(10)]),
            ])));
            let variables = BTreeSet::from([VarName::new("x"), VarName::new("y")]);

            let error = match pipeline.build(variables).await {
                Ok(_) => panic!("unequal in-memory columns must fail while opening the source"),
                Err(error) => error,
            };
            let message = format!("{error:#}");
            assert!(message.contains("input source `default` could not be opened"));
            assert!(message.contains("typed input columns have unequal lengths"));
        });
    }

    #[test]
    fn input_reconfiguration_plan_retains_adds_removes_and_moves_sources() {
        let (_x_sender, x_fanout) = Fanout::<Value>::new();
        let (_y_sender, y_fanout) = Fanout::<Value>::new();
        let (_control_sender, control_fanout) = Fanout::<Value>::new();
        let pipeline = InputPipeline::from_sources(
            InputSources::new()
                .insert(
                    "a",
                    InputSource::<Value>::channel_with_control(
                        BTreeMap::from([("x".into(), x_fanout.clone())]),
                        Some(control_fanout),
                    )
                    .with_reconfiguration_route("control")
                    .unwrap(),
                )
                .insert(
                    "b",
                    InputSource::<Value>::channel(BTreeMap::from([
                        ("x".into(), x_fanout),
                        ("y".into(), y_fanout),
                    ])),
                ),
        );
        let x = BTreeSet::from([VarName::new("x")]);
        let xy = BTreeSet::from([VarName::new("x"), VarName::new("y")]);
        let on_a = InputConfiguration {
            source: None,
            inputs: None,
            sources: Some(BTreeMap::from([(
                "a".into(),
                BTreeMap::from([("x".into(), Route::new("x", None).unwrap())]),
            )])),
        };
        let split = InputConfiguration {
            source: None,
            inputs: None,
            sources: Some(BTreeMap::from([
                (
                    "a".into(),
                    BTreeMap::from([("x".into(), Route::new("x", None).unwrap())]),
                ),
                (
                    "b".into(),
                    BTreeMap::from([("y".into(), Route::new("y", None).unwrap())]),
                ),
            ])),
        };
        let on_b = InputConfiguration {
            source: None,
            inputs: None,
            sources: Some(BTreeMap::from([(
                "b".into(),
                BTreeMap::from([("x".into(), Route::new("x", None).unwrap())]),
            )])),
        };

        let active = pipeline.resolve(&x, Some(&on_a)).unwrap();
        let added = pipeline.resolve(&xy, Some(&split)).unwrap();
        let session = SessionId::new();
        let revision = SessionRevision::initial();
        let add_plan = pipeline
            .plan_reconfiguration(&active, added.clone(), session, revision)
            .unwrap();
        assert_eq!(add_plan.session(), session);
        assert_eq!(add_plan.expected_revision(), revision);
        assert!(add_plan.removed_sources().is_empty());
        assert_eq!(add_plan.added_sources()[0].source(), "b");

        let moved = pipeline.resolve(&x, Some(&on_b)).unwrap();
        let move_plan = pipeline
            .plan_reconfiguration(&active, moved, SessionId::new(), SessionRevision::initial())
            .unwrap();
        assert_eq!(move_plan.removed_sources(), &[SourceId::from("a")]);
        assert_eq!(move_plan.added_sources()[0].source(), "b");

        let no_op = pipeline
            .plan_reconfiguration(
                &added,
                added.clone(),
                SessionId::new(),
                SessionRevision::initial(),
            )
            .unwrap();
        assert!(!no_op.is_changed());
    }

    #[test]
    fn mqtt_binding_change_plans_native_rebind_without_reopening_source() {
        let pipeline = InputPipeline::<Value>::new(InputSource::mqtt(None, None));
        let variables = BTreeSet::from([VarName::new("x")]);
        let configured = |route: &str| InputConfiguration {
            source: None,
            inputs: Some(BTreeMap::from([(
                VarName::new("x"),
                Route::new(route.to_owned().into_boxed_str(), None).unwrap(),
            )])),
            sources: None,
        };
        let active = pipeline
            .resolve(&variables, Some(&configured("old")))
            .unwrap();
        let candidate = pipeline
            .resolve(&variables, Some(&configured("new")))
            .unwrap();

        let plan = pipeline
            .plan_reconfiguration(
                &active,
                candidate,
                SessionId::new(),
                SessionRevision::initial(),
            )
            .unwrap();

        assert!(plan.removed_sources().is_empty());
        assert!(plan.added_sources().is_empty());
        assert_eq!(plan.rebound_sources()[0].source(), "default");
    }

    #[test]
    fn channel_native_boundary_preserves_complete_rows_and_retained_subscription() {
        smol::block_on(async {
            let (x_sender, x) = Fanout::<Value>::new();
            let (y_sender, y) = Fanout::<Value>::new();
            let (_control_sender, control) = Fanout::<Value>::new();
            let (mut stream, owner) = crate::io::channel::reconfigurable_stream(
                BTreeMap::from([
                    (VarName::new("x"), x.clone()),
                    (VarName::new("y"), y.clone()),
                ]),
                BTreeSet::from([VarName::new("x"), VarName::new("y")]),
                control,
            );
            let x_subscriptions = x.sub_events();

            x_sender.send(Value::Int(1)).await;
            y_sender.send(Value::Int(2)).await;
            owner.pause(7).await.unwrap();

            let ReconfigurableInputItem::Data(batch) = stream.next().await.unwrap().unwrap() else {
                panic!("complete queued channel row must precede its boundary")
            };
            assert_eq!(batch.tick_count(), 1);
            assert!(matches!(
                stream.next().await.unwrap().unwrap(),
                ReconfigurableInputItem::Boundary(7)
            ));

            owner
                .rebind(BTreeSet::from([VarName::new("x")]))
                .await
                .unwrap();
            // Poll the queued rebind command before producing the next row.
            let next = Box::pin(stream.next());
            x_sender.send(Value::Int(3)).await;
            let ReconfigurableInputItem::Data(batch) = next.await.unwrap().unwrap() else {
                panic!("retained channel variable must continue after rebind")
            };
            assert_eq!(*batch.updates().next().unwrap().value, Value::Int(3));
            assert_eq!(x.sub_events(), x_subscriptions);
        });
    }

    #[cfg(feature = "ros")]
    #[test]
    fn ros_binding_change_is_rejected_during_plan_preparation() {
        let executor = Rc::new(LocalExecutor::new());
        let format = FormatId::new("Int32");
        let pipeline = InputPipeline::new(InputSource::ros(
            BTreeMap::from([(
                VarName::new("x"),
                Route::new("/old", Some(format.clone())).unwrap(),
            )]),
            executor,
        ));
        let variables = BTreeSet::from([VarName::new("x")]);
        let active = pipeline.resolve(&variables, None).unwrap();
        let candidate_config = InputConfiguration {
            source: None,
            inputs: None,
            sources: Some(BTreeMap::from([(
                SourceId::from("default"),
                BTreeMap::from([(VarName::new("x"), Route::new("/new", Some(format)).unwrap())]),
            )])),
        };
        let candidate = pipeline
            .resolve(&variables, Some(&candidate_config))
            .unwrap();

        let error = pipeline
            .plan_reconfiguration(
                &active,
                candidate,
                SessionId::new(),
                SessionRevision::initial(),
            )
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("cannot change bindings in place")
        );
    }

    #[test]
    fn reconfigurable_input_composes_data_and_control_from_different_sources() {
        let executor = Rc::new(LocalExecutor::new());
        smol::block_on(executor.clone().run(async {
            let (data_sender, data_fanout) = Fanout::<Value>::new();
            let (control_sender, control_fanout) = Fanout::<Value>::new();
            let data_source =
                InputSource::<Value>::channel(BTreeMap::from([("x".into(), data_fanout)]));
            let control_source =
                InputSource::<Value>::channel_with_control(BTreeMap::new(), Some(control_fanout))
                    .with_reconfiguration_route("control")
                    .unwrap();
            let pipeline = InputPipeline::from_sources(
                InputSources::new()
                    .insert("data", data_source)
                    .insert("control", control_source),
            );
            let variables = BTreeSet::from([VarName::new("x")]);
            let resolved = pipeline.resolve(&variables, None).unwrap();
            let control = ReconfigurationControl::new("control", "control").unwrap();
            let mut stream = pipeline
                .clone()
                .with_executor(executor.clone())
                .open_reconfigurable(resolved, &control)
                .await
                .unwrap();

            data_sender.send(Value::Int(7)).await;
            control_sender
                .send(Value::Str(r#"{"specification":"in x"}"#.into()))
                .await;

            let mut saw_data = false;
            let mut saw_control = false;
            for _ in 0..2 {
                match stream.next().await.unwrap().unwrap() {
                    ReconfigurableInputItem::Data(batch) => {
                        saw_data = *batch.updates().next().unwrap().value == Value::Int(7)
                    }
                    ReconfigurableInputItem::Reconfigure(_) => saw_control = true,
                    ReconfigurableInputItem::Boundary(_) => panic!("no boundary was requested"),
                }
            }
            assert!(saw_data && saw_control);
        }));
    }

    #[test]
    fn reconfigurable_input_composes_bindings_spanning_sources() {
        let executor = Rc::new(LocalExecutor::new());
        smol::block_on(executor.clone().run(async {
            let (x_sender, x_fanout) = Fanout::<Value>::new();
            let (y_sender, y_fanout) = Fanout::<Value>::new();
            let (control_sender, control_fanout) = Fanout::<Value>::new();
            let data_source =
                InputSource::<Value>::channel(BTreeMap::from([("x".into(), x_fanout)]));
            let control_source = InputSource::<Value>::channel_with_control(
                BTreeMap::from([("y".into(), y_fanout)]),
                Some(control_fanout),
            )
            .with_reconfiguration_route("control")
            .unwrap();
            let pipeline = InputPipeline::from_sources(
                InputSources::new()
                    .insert("data", data_source)
                    .insert("control", control_source),
            );
            let variables = BTreeSet::from([VarName::new("x"), VarName::new("y")]);
            let resolved = pipeline.resolve(&variables, None).unwrap();
            let control = ReconfigurationControl::new("control", "control").unwrap();
            let mut stream = pipeline
                .clone()
                .with_executor(executor.clone())
                .open_reconfigurable(resolved, &control)
                .await
                .unwrap();

            x_sender.send(Value::Int(1)).await;
            y_sender.send(Value::Int(2)).await;
            control_sender
                .send(Value::Str(r#"{"specification":"in x\nin y"}"#.into()))
                .await;

            let mut variables = BTreeSet::new();
            let mut saw_control = false;
            for _ in 0..3 {
                match stream.next().await.unwrap().unwrap() {
                    ReconfigurableInputItem::Data(batch) => {
                        variables.insert(batch.updates().next().unwrap().variable.clone());
                    }
                    ReconfigurableInputItem::Reconfigure(_) => saw_control = true,
                    ReconfigurableInputItem::Boundary(_) => panic!("no boundary was requested"),
                }
            }
            assert_eq!(variables, BTreeSet::from(["x".into(), "y".into()]));
            assert!(saw_control);
        }));
    }

    #[test]
    fn inactive_sources_do_not_block_reconfigurable_opening() {
        let executor = Rc::new(LocalExecutor::new());
        smol::block_on(executor.clone().run(async {
            let (control_sender, control_fanout) = Fanout::<Value>::new();
            let (_data_sender, data_fanout) = Fanout::<Value>::new();
            let control_source = InputSource::<Value>::channel_with_control(
                BTreeMap::from([("x".into(), data_fanout)]),
                Some(control_fanout),
            )
            .with_reconfiguration_route("control")
            .unwrap();
            let pipeline = InputPipeline::from_sources(
                InputSources::new()
                    .insert("control", control_source)
                    .insert(
                        "inactive-file",
                        InputSource::file("not-opened.input".to_owned()),
                    ),
            );
            pipeline.ensure_reconfigurable(None).unwrap();
            let variables = BTreeSet::from([VarName::new("x")]);
            let resolved = pipeline.resolve(&variables, None).unwrap();
            let control = ReconfigurationControl::new("control", "control").unwrap();
            let mut stream = pipeline
                .clone()
                .with_executor(executor.clone())
                .open_reconfigurable(resolved, &control)
                .await
                .unwrap();

            control_sender
                .send(Value::Str(r#"{"specification":"in x"}"#.into()))
                .await;
            assert!(matches!(
                stream.next().await.unwrap().unwrap(),
                ReconfigurableInputItem::Reconfigure(_)
            ));
        }));
    }

    #[test]
    fn ros_control_topic_collision_is_rejected_before_opening() {
        let executor = Rc::new(LocalExecutor::new());
        smol::block_on(executor.clone().run(async {
            let source = InputSource::<Value>::ros(
                BTreeMap::from([(
                    VarName::new("x"),
                    Route::new(
                        "/shared".to_owned().into_boxed_str(),
                        Some(FormatId::new("json")),
                    )
                    .unwrap(),
                )]),
                Rc::new(LocalExecutor::new()),
            );
            let pipeline = InputPipeline::new(source);
            let variables = BTreeSet::from([VarName::new("x")]);
            let resolved = pipeline.resolve(&variables, None).unwrap();
            let control = ReconfigurationControl::new("default", "/shared").unwrap();

            let error = match pipeline
                .clone()
                .with_executor(executor.clone())
                .open_reconfigurable(resolved, &control)
                .await
            {
                Ok(_) => panic!("a ROS control topic cannot reuse an active data topic"),
                Err(error) => error,
            };
            let message = format!("{error:#}");
            assert!(
                message.contains(
                    "ROS control topic `/shared` collides with data topic for variable `x`"
                ),
                "unexpected error: {message}"
            );
        }));
    }

    #[test]
    fn channel_control_does_not_discard_data_queued_before_the_barrier() {
        let executor = Rc::new(LocalExecutor::new());
        smol::block_on(executor.clone().run(async {
            let (control_sender, control_fanout) = Fanout::<Value>::new();
            let (data_sender, data_fanout) = Fanout::<Value>::new();
            let source = InputSource::channel_with_control(
                BTreeMap::from([(VarName::new("x"), data_fanout)]),
                Some(control_fanout),
            );
            let pipeline = InputPipeline::new(source);
            let variables = BTreeSet::from([VarName::new("x")]);
            let resolved = pipeline.resolve(&variables, None).unwrap();
            let control = ReconfigurationControl::new("default", "control").unwrap();
            let mut stream = pipeline
                .clone()
                .with_executor(executor.clone())
                .open_reconfigurable(resolved, &control)
                .await
                .unwrap();

            // Independent manual fanouts are not an ordering edge. Control is
            // prioritized for responsiveness, while the reusable boundary
            // still preserves already queued data afterwards.
            data_sender.send(Value::Int(7)).await;
            control_sender
                .send(Value::Str(r#"{"specification":"in x"}"#.into()))
                .await;

            assert!(matches!(
                stream.next().await.unwrap().unwrap(),
                ReconfigurableInputItem::Reconfigure(_)
            ));
            let ReconfigurableInputItem::Data(batch) = stream.next().await.unwrap().unwrap() else {
                panic!("data queued before a control barrier must remain live");
            };
            assert_eq!(*batch.updates().next().unwrap().value, Value::Int(7));
        }));
    }

    #[test]
    fn channel_control_is_eventually_delivered_with_ready_data() {
        let executor = Rc::new(LocalExecutor::new());
        smol::block_on(executor.clone().run(async {
            let (control_sender, control_fanout) = Fanout::<Value>::new();
            let (data_sender, data_fanout) = Fanout::<Value>::new();
            let source = InputSource::channel_with_control(
                BTreeMap::from([(VarName::new("x"), data_fanout)]),
                Some(control_fanout),
            );
            let pipeline = InputPipeline::new(source);
            let variables = BTreeSet::from([VarName::new("x")]);
            let resolved = pipeline.resolve(&variables, None).unwrap();
            let control = ReconfigurationControl::new("default", "control").unwrap();
            let mut stream = pipeline
                .clone()
                .with_executor(executor.clone())
                .open_reconfigurable(resolved, &control)
                .await
                .unwrap();

            for value in 0..32 {
                data_sender.send(Value::Int(value)).await;
            }

            let first = stream.next().await.unwrap().unwrap();
            let ReconfigurableInputItem::Data(first) = first else {
                panic!("expected data")
            };
            let mut values = vec![first.updates().next().unwrap().value.clone()];

            control_sender
                .send(Value::Str(r#"{"specification":"in x"}"#.into()))
                .await;

            let mut control_position = None;
            for position in 1..=32 {
                match stream.next().await.unwrap().unwrap() {
                    ReconfigurableInputItem::Data(batch) => {
                        values.push(batch.updates().next().unwrap().value.clone());
                    }
                    ReconfigurableInputItem::Reconfigure(_) => {
                        assert!(control_position.replace(position).is_none());
                    }
                    ReconfigurableInputItem::Boundary(_) => panic!("no boundary was requested"),
                }
            }
            // At most one queued and one held relay item can precede control.
            assert!(control_position.is_some_and(|position| position <= 3));
            assert_eq!(values, (0..32).map(Value::Int).collect::<Vec<_>>());
        }));
    }

    #[test]
    fn terminated_channel_control_does_not_stop_ready_data() {
        let executor = Rc::new(LocalExecutor::new());
        smol::block_on(executor.clone().run(async {
            let (control_sender, control_fanout) = Fanout::<Value>::new();
            let (data_sender, data_fanout) = Fanout::<Value>::new();
            let source = InputSource::channel_with_control(
                BTreeMap::from([(VarName::new("x"), data_fanout)]),
                Some(control_fanout),
            );
            let pipeline = InputPipeline::new(source);
            let variables = BTreeSet::from([VarName::new("x")]);
            let resolved = pipeline.resolve(&variables, None).unwrap();
            let control = ReconfigurationControl::new("default", "control").unwrap();
            let mut stream = pipeline
                .clone()
                .with_executor(executor.clone())
                .open_reconfigurable(resolved, &control)
                .await
                .unwrap();

            drop(control_sender);
            data_sender.send(Value::Int(7)).await;
            data_sender.send(Value::Int(8)).await;

            for expected in [7, 8] {
                let item = stream.next().await.unwrap().unwrap();
                let ReconfigurableInputItem::Data(batch) = item else {
                    panic!("ready data must survive termination of the control stream");
                };
                assert_eq!(*batch.updates().next().unwrap().value, Value::Int(expected));
            }
        }));
    }

    #[test]
    fn data_eof_leaves_control_active() {
        smol::block_on(async {
            let data: InputStream<Value> = Box::pin(futures::stream::empty());
            let control: crate::LocalStream<anyhow::Result<ReconfigurationRequest>> =
                Box::pin(futures::stream::once(async {
                    smol::future::yield_now().await;
                    ReconfigurationRequest::from_json(r#"{"specification":"in x"}"#)
                }));
            let mut stream = controlled_input_stream(Some(data), control);

            let item = stream.next().await.unwrap().unwrap();
            assert!(matches!(item, ReconfigurableInputItem::Reconfigure(_)));
            assert!(stream.next().await.is_none());
        });
    }

    #[test]
    fn controlled_input_completes_after_both_branches_end() {
        smol::block_on(async {
            let data: InputStream<Value> = Box::pin(futures::stream::empty());
            let control: crate::LocalStream<anyhow::Result<ReconfigurationRequest>> =
                Box::pin(futures::stream::empty());
            let mut stream = controlled_input_stream(Some(data), control);

            assert!(stream.next().await.is_none());
        });
    }

    #[test]
    fn multiple_sources_require_exactly_one_declared_route() {
        let sources = InputSources::<Value>::new()
            .insert("data", InputSource::mqtt(None, None))
            .insert("control", InputSource::mqtt(None, None));
        let error = sources.resolve_reconfiguration_source(None).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("multiple sources, but none declares `reconfiguration_route`")
        );

        let sources = InputSources::<Value>::new()
            .insert(
                "data",
                InputSource::mqtt(None, None)
                    .with_reconfiguration_route("data-control")
                    .unwrap(),
            )
            .insert(
                "control",
                InputSource::mqtt(None, None)
                    .with_reconfiguration_route("control")
                    .unwrap(),
            );
        let error = sources.resolve_reconfiguration_source(None).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("2 sources declaring `reconfiguration_route`")
        );
    }

    #[test]
    fn redis_knowledge_catalog_is_resolution_scoped_and_not_a_control_source() {
        let source = InputSource::<Value>::redis_knowledge(RedisKnowledgeConfig {
            host: "redis".to_owned(),
            port: None,
            database: 2,
            publish_initial: true,
            keys: BTreeMap::from([
                (VarName::new("current"), "knowledge:current".to_owned()),
                (VarName::new("future"), "knowledge:future".to_owned()),
            ]),
            retry: crate::io::RetryPolicy::input_default(),
        });
        let pipeline = InputPipeline::new(source);
        let resolved = pipeline
            .resolve(&BTreeSet::from([VarName::new("current")]), None)
            .unwrap();
        assert_eq!(
            resolved.sources()[0].bindings()[0].route().address(),
            "knowledge:current"
        );
        assert!(
            pipeline
                .sources()
                .resolve_reconfiguration_source(None)
                .unwrap_err()
                .to_string()
                .contains("does not support reconfiguration")
        );
    }

    #[test]
    fn redis_knowledge_active_duplicate_keys_are_rejected_before_opening() {
        let pipeline = InputPipeline::new(InputSource::<Value>::redis_knowledge(
            RedisKnowledgeConfig {
                host: "redis".to_owned(),
                port: None,
                database: 2,
                publish_initial: false,
                keys: BTreeMap::from([(VarName::new("x"), "catalog:x".to_owned())]),
                retry: crate::io::RetryPolicy::input_default(),
            },
        ));
        let config = InputConfiguration {
            source: Some("default".into()),
            inputs: Some(BTreeMap::from([
                (VarName::new("x"), Route::new("same:key", None).unwrap()),
                (VarName::new("y"), Route::new("same:key", None).unwrap()),
            ])),
            sources: None,
        };
        let error = pipeline
            .resolve(
                &BTreeSet::from([VarName::new("x"), VarName::new("y")]),
                Some(&config),
            )
            .unwrap_err();
        assert!(error.to_string().contains("active Redis knowledge key"));
    }

    #[test]
    fn ordinary_owner_stops_live_ingress_and_flushes_its_window() {
        let executor = Rc::new(LocalExecutor::new());
        let task_executor = Rc::clone(&executor);
        smol::block_on(executor.run(async move {
            let (sender, fanout) = Fanout::<Value>::new();
            let pipeline = InputPipeline::new(InputSource::channel(BTreeMap::from([(
                VarName::new("x"),
                fanout,
            )])))
            .with_executor(task_executor)
            .with_policy(InputPolicy::Batch(
                crate::io::InputWindow::new(None, NonZeroUsize::new(10)).unwrap(),
            ))
            .unwrap();
            let input = pipeline
                .build(BTreeSet::from([VarName::new("x")]))
                .await
                .unwrap();

            sender.send(Value::Int(1)).await;
            smol::future::yield_now().await;
            let mut drain = input.into_drain_with_deadline(ShutdownDeadline::after(
                std::time::Duration::from_secs(1),
            ));
            let batch = drain.next().await.unwrap().unwrap();
            assert_eq!(*batch.updates().next().unwrap().value, Value::Int(1));

            // The producer remains live, but stopping the local relay fixes
            // the admission boundary and lets cleanup reach EOF.
            sender.send(Value::Int(2)).await;
            assert!(drain.next().await.is_none());
        }));
    }
}
