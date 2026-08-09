use std::{
    any::{Any, TypeId},
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
};

use anyhow::{Context, anyhow};
use async_stream::stream;
use futures::StreamExt;
use smol::LocalExecutor;

use crate::core::{
    FileInputValue, InputBatch, InputStream, MQTT_HOSTNAME, REDIS_HOSTNAME, RosStreamValue, Value,
    VarName, input,
};
use crate::io::config::{
    CodecId, InputConfigFile, MonitorConfig, ResolvedBinding, ResolvedInput, ResolvedSource, Route,
    SourceId,
};
use crate::io::mqtt::MqttInputBackend;
use crate::io::reconfigurable_input::{
    ReconfigurableInputItem, ReconfigurableInputStream, ReconfigurationControl,
};
use crate::io::redis::RedisKnowledgeConfig;
use crate::stream_utils::Fanout;

use super::super::config::InputStage;

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
        backend: MqttInputBackend,
    },
    Redis {
        host: String,
        routes: Option<BTreeMap<VarName, Route>>,
        port: Option<u16>,
    },
    RedisKnowledge(RedisKnowledgeConfig),
    Manual {
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
async fn open_configured_redis_knowledge<V>(
    config: RedisKnowledgeConfig,
    bindings: BTreeMap<VarName, String>,
) -> anyhow::Result<InputStream<V>>
where
    V: FileInputValue + RosStreamValue + 'static,
{
    anyhow::ensure!(
        TypeId::of::<V>() == TypeId::of::<Value>(),
        "Redis knowledge input produces ordinary `Value` input and is unsupported for MSTLO or other non-Value input domains"
    );

    let mut values = crate::io::redis::open_value_redis_knowledge(config, bindings).await?;
    Ok(Box::pin(async_stream::try_stream! {
        while let Some(batch) = values.next().await {
            let batch = batch?;
            let batch = batch.try_map_values(|value| {
                let value: Box<dyn Any> = Box::new(value);
                value
                    .downcast::<V>()
                    .map(|value| *value)
                    .map_err(|_| anyhow!("Redis knowledge value-domain conversion failed"))
            })?;
            yield batch;
        }
    }))
}

/// An owned local source set containing source-owned catalogs and
/// security-sensitive transport configuration. It is deliberately separate
/// from generation-specific resolved input.
#[derive(Clone, Debug)]
pub struct InputSources<V = Value> {
    sources: BTreeMap<SourceId, InputSource<V>>,
    default: Option<SourceId>,
}

/// The fixed source and route used by a reconfigurable input adapter.
/// This borrows only the reusable source configuration; the route is owned so
/// the adapter can retain it independently of any generation plan.
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

    pub fn insert(mut self, source: impl Into<SourceId>, input: InputSource<V>) -> Self {
        self.sources.insert(source.into(), input);
        self
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
        mqtt_backend: MqttInputBackend,
    ) -> anyhow::Result<InputSources<V>>
    where
        V: 'static,
    {
        config.validate()?;
        let mut sources = InputSources::new();
        for (id, source_config) in config.sources {
            let convert_routes = |routes: BTreeMap<VarName, crate::io::config::WireRoute>| {
                routes
                    .into_iter()
                    .map(|(variable, route)| route.into_route().map(|route| (variable, route)))
                    .collect::<anyhow::Result<BTreeMap<_, _>>>()
            };
            let (input, reconfiguration_route) = match source_config {
                crate::io::config::SourceConfig::Mqtt {
                    host,
                    port,
                    routes,
                    reconfiguration_route,
                } => {
                    let routes = convert_routes(routes)?;
                    (
                        InputSource::mqtt_with_host_routes(
                            host.unwrap_or_else(|| MQTT_HOSTNAME.to_owned()),
                            if routes.is_empty() {
                                None
                            } else {
                                Some(routes)
                            },
                            port.or(mqtt_port),
                            mqtt_backend,
                        ),
                        reconfiguration_route,
                    )
                }
                crate::io::config::SourceConfig::Redis {
                    host,
                    port,
                    routes,
                    reconfiguration_route,
                } => {
                    let routes = convert_routes(routes)?;
                    (
                        InputSource::redis_with_host_routes(
                            host.unwrap_or_else(|| REDIS_HOSTNAME.to_owned()),
                            if routes.is_empty() {
                                None
                            } else {
                                Some(routes)
                            },
                            port.or(redis_port),
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
                    InputSource::ros(convert_routes(routes)?, executor.clone()),
                    reconfiguration_route,
                ),
            };
            let input = if let Some(route) = reconfiguration_route {
                input.with_reconfiguration_route(route)?
            } else {
                input
            };
            sources = sources.insert(id, input);
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
        let mut by_source = BTreeMap::<SourceId, Vec<ResolvedBinding>>::new();
        for variable in variables {
            let source_id = self.owner_for(variable, &catalog_ownership)?;
            let source = self.sources.get(&source_id).ok_or_else(|| {
                anyhow::anyhow!("resolved input source `{source_id}` is not available")
            })?;
            let route = source.route_for(variable).unwrap_or_else(|| Route {
                route: variable.to_string().into_boxed_str(),
                codec: None,
            });
            anyhow::ensure!(
                !source.requires_route_codec() || route.codec.is_some(),
                "source `{source_id}` requires a codec for input variable `{variable}`"
            );
            let binding = ResolvedBinding::new(
                variable.clone(),
                route.route,
                route.codec.unwrap_or_else(|| CodecId::new("json")),
            );
            source.validate_binding(&binding)?;
            by_source.entry(source_id).or_default().push(binding);
        }
        let resolved = ResolvedInput::new(
            by_source
                .into_iter()
                .map(|(source, bindings)| ResolvedSource::new(source, bindings)),
        );
        self.validate_resolved(&resolved, variables)
    }

    pub(crate) fn resolve_monitor_config(
        &self,
        config: &MonitorConfig,
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
        let mut grouped = BTreeMap::<SourceId, Vec<ResolvedBinding>>::new();
        for (source, variable, route) in explicit_bindings {
            let source_id = match source {
                Some(source) => source.to_owned(),
                None => self.owner_for(variable, &catalog_ownership)?,
            };
            let source_config = self
                .sources
                .get(&source_id)
                .ok_or_else(|| anyhow::anyhow!("input source `{source_id}` is not registered"))?;
            let binding = ResolvedBinding::new(
                variable.clone(),
                route.route.clone(),
                route.codec.clone().unwrap_or_else(|| CodecId::new("json")),
            );
            anyhow::ensure!(
                !source_config.requires_route_codec() || route.codec.is_some(),
                "source `{source_id}` requires a codec for input variable `{variable}`"
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
                    !source.requires_route_codec() || !binding.codec().0.trim().is_empty(),
                    "source `{}` requires a codec for input variable `{}`",
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
        Self::mqtt_with_routes(routes, port, MqttInputBackend::default())
    }

    pub fn mqtt_with_routes(
        routes: Option<BTreeMap<VarName, Route>>,
        port: Option<u16>,
        backend: MqttInputBackend,
    ) -> Self {
        Self::mqtt_with_host_routes(MQTT_HOSTNAME, routes, port, backend)
    }

    pub fn mqtt_with_host_routes(
        host: impl Into<String>,
        routes: Option<BTreeMap<VarName, Route>>,
        port: Option<u16>,
        backend: MqttInputBackend,
    ) -> Self {
        Self::new(InputSourceKind::Mqtt {
            host: host.into(),
            routes,
            port,
            backend,
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
        Self::new(InputSourceKind::Redis {
            host: host.into(),
            routes,
            port,
        })
    }

    pub(crate) fn manual(fanouts: BTreeMap<VarName, Rc<Fanout<V>>>) -> Self {
        Self::manual_with_control(fanouts, None)
    }

    pub(crate) fn manual_with_control(
        fanouts: BTreeMap<VarName, Rc<Fanout<V>>>,
        control: Option<Rc<Fanout<Value>>>,
    ) -> Self {
        Self::new(InputSourceKind::Manual { fanouts, control })
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
            InputSourceKind::Manual { fanouts, .. } => {
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
            | InputSourceKind::Redis { routes: None, .. } => Some(Route {
                route: variable.to_string().into_boxed_str(),
                codec: None,
            }),
            InputSourceKind::RedisKnowledge(config) => config.keys.get(variable).map(|key| Route {
                route: key.clone().into_boxed_str(),
                codec: None,
            }),
            InputSourceKind::File { .. }
            | InputSourceKind::InMemoryRows { .. }
            | InputSourceKind::InMemoryTicks { .. }
            | InputSourceKind::Manual { .. } => None,
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

    fn validate_binding(&self, binding: &ResolvedBinding) -> anyhow::Result<()> {
        anyhow::ensure!(
            !binding.route().trim().is_empty(),
            "input route for `{}` cannot be empty",
            binding.variable()
        );
        anyhow::ensure!(
            !binding.codec().0.trim().is_empty(),
            "input codec for `{}` cannot be empty",
            binding.variable()
        );
        if matches!(&self.kind, InputSourceKind::RedisKnowledge(_)) {
            anyhow::ensure!(
                matches!(binding.codec().0.as_ref(), "json" | "json5"),
                "Redis knowledge input for `{}` supports JSON5 decoding only (`json` is a compatibility alias); codec `{}` is unsupported",
                binding.variable(),
                binding.codec()
            );
        }
        Ok(())
    }

    fn validate_bindings(&self, bindings: &[ResolvedBinding]) -> anyhow::Result<()> {
        if matches!(self.kind, InputSourceKind::RedisKnowledge(_)) {
            let mut keys = BTreeMap::<&str, &VarName>::new();
            for binding in bindings {
                if let Some(previous) = keys.insert(binding.route(), binding.variable()) {
                    anyhow::bail!(
                        "active Redis knowledge key `{}` is mapped to both `{}` and `{}`",
                        binding.route(),
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
                | InputSourceKind::Manual {
                    control: Some(_),
                    ..
                }
        )
    }

    async fn open(
        self,
        bindings: Vec<ResolvedBinding>,
        variables: BTreeSet<VarName>,
    ) -> anyhow::Result<InputStream<V>>
    where
        V: FileInputValue + RosStreamValue,
    {
        let routes = bindings
            .into_iter()
            .map(|binding| {
                (
                    binding.variable().clone(),
                    Route {
                        route: binding.route().to_owned().into_boxed_str(),
                        codec: Some(binding.codec().clone()),
                    },
                )
            })
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
                #[cfg(feature = "ros")]
                {
                    let mapping = routes
                        .into_iter()
                        .map(|(variable, route)| {
                            let codec =
                                route
                                    .codec
                                    .map(|codec| codec.0.to_string())
                                    .ok_or_else(|| {
                                        anyhow::anyhow!(
                                            "ROS route for `{variable}` requires a codec"
                                        )
                                    })?;
                            Ok((variable.to_string(), (route.route.to_string(), codec)))
                        })
                        .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
                    Ok(V::ros_input_stream(executor, mapping)?)
                }
                #[cfg(not(feature = "ros"))]
                {
                    let _ = (executor, routes);
                    anyhow::bail!("ROS support not enabled")
                }
            }
            InputSourceKind::Mqtt {
                host,
                port,
                backend,
                ..
            } => {
                let topics = routes
                    .into_iter()
                    .map(|(variable, route)| (variable, route.route.to_string()))
                    .collect();
                backend.open_data::<V>(&host, port, topics, u32::MAX).await
            }
            InputSourceKind::Redis { host, port, .. } => {
                let topics = routes
                    .into_iter()
                    .map(|(variable, route)| (variable, route.route.to_string()))
                    .collect();
                crate::io::redis::input_stream::<V>(&host, port, topics).await
            }
            InputSourceKind::RedisKnowledge(config) => {
                let keys = routes
                    .into_iter()
                    .map(|(variable, route)| (variable, route.route.to_string()))
                    .collect();
                open_configured_redis_knowledge::<V>(config, keys).await
            }
            InputSourceKind::Manual { fanouts, .. } => {
                let streams = fanouts
                    .into_iter()
                    .filter(|(variable, _)| variables.contains(variable))
                    .map(|(variable, fanout)| {
                        let mut receiver = fanout.subscribe();
                        let stream: crate::OutputStream<V> = Box::pin(stream! {
                            while let Some(value) = receiver.recv().await {
                                yield value;
                            }
                        });
                        (variable, stream)
                    })
                    .collect();
                Ok(crate::io::testing::from_streams(streams))
            }
        }
    }

    async fn open_with_control(
        self,
        bindings: Vec<ResolvedBinding>,
        variables: BTreeSet<VarName>,
        control_route: Box<str>,
    ) -> anyhow::Result<ReconfigurableInputStream<V>>
    where
        V: FileInputValue + RosStreamValue,
    {
        let routes = bindings
            .into_iter()
            .map(|binding| {
                (
                    binding.variable().clone(),
                    Route {
                        route: binding.route().to_owned().into_boxed_str(),
                        codec: Some(binding.codec().clone()),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let kind = self.kind;
        match kind {
            InputSourceKind::Mqtt {
                host,
                port,
                backend,
                ..
            } => {
                let topics = routes
                    .into_iter()
                    .map(|(variable, route)| (variable, route.route.to_string()))
                    .collect();
                let stream = backend
                    .open_items(
                        &host,
                        port,
                        topics,
                        u32::MAX,
                        Some(control_route.to_string()),
                    )
                    .await?;
                Ok(Box::pin(stream.map(|item| {
                    item.map(|item| match item {
                        crate::io::mqtt::MqttInputItem::Data(batch) => {
                            ReconfigurableInputItem::Data(batch)
                        }
                        crate::io::mqtt::MqttInputItem::Control(config) => {
                            ReconfigurableInputItem::Reconfigure(config)
                        }
                    })
                })))
            }
            InputSourceKind::Redis { host, port, .. } => {
                let topics = routes
                    .into_iter()
                    .map(|(variable, route)| (variable, route.route.to_string()))
                    .collect();
                let stream = crate::io::redis::input_stream_items(
                    &host,
                    port,
                    topics,
                    Some(control_route.to_string()),
                )
                .await?;
                Ok(Box::pin(stream.map(|item| {
                    item.map(|item| match item {
                        crate::io::redis::RedisInputItem::Data(batch) => {
                            ReconfigurableInputItem::Data(batch)
                        }
                        crate::io::redis::RedisInputItem::Control(config) => {
                            ReconfigurableInputItem::Reconfigure(config)
                        }
                    })
                })))
            }
            InputSourceKind::Ros { executor, .. } => {
                #[cfg(feature = "ros")]
                {
                    let mapping = routes
                        .into_iter()
                        .map(|(variable, route)| {
                            let codec =
                                route
                                    .codec
                                    .map(|codec| codec.0.to_string())
                                    .ok_or_else(|| {
                                        anyhow::anyhow!(
                                            "ROS route for `{variable}` requires a codec"
                                        )
                                    })?;
                            Ok((variable.to_string(), (route.route.to_string(), codec)))
                        })
                        .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
                    let data = if mapping.is_empty() {
                        None
                    } else {
                        Some(V::ros_input_stream(executor.clone(), mapping)?)
                    };
                    let control =
                        crate::io::ros::control_stream(executor, control_route.to_string())?;
                    Ok(controlled_input_stream(data, control))
                }
                #[cfg(not(feature = "ros"))]
                {
                    let _ = (executor, routes, variables, control_route);
                    anyhow::bail!("ROS support not enabled")
                }
            }
            InputSourceKind::RedisKnowledge(_) => {
                anyhow::bail!(
                    "Redis knowledge sources do not support reconfiguration control; use a Redis Pub/Sub, MQTT, ROS, or manual control source"
                )
            }
            InputSourceKind::Manual { fanouts, control } => {
                let data_streams = fanouts
                    .into_iter()
                    .filter(|(variable, _)| variables.contains(variable))
                    .map(|(variable, fanout)| {
                        let mut receiver = fanout.subscribe();
                        let stream: crate::OutputStream<V> = Box::pin(stream! {
                            while let Some(value) = receiver.recv().await {
                                yield value;
                            }
                        });
                        (variable, stream)
                    })
                    .collect::<BTreeMap<_, _>>();
                let data = if data_streams.is_empty() {
                    None
                } else {
                    Some(crate::io::testing::from_streams(data_streams))
                };
                let Some(control) = control else {
                    anyhow::bail!("manual input has no configured control source")
                };
                let mut receiver = control.subscribe();
                let control: crate::OutputStream<anyhow::Result<MonitorConfig>> =
                    Box::pin(async_stream::try_stream! {
                        while let Some(payload) = receiver.recv().await {
                            match payload {
                                Value::NoVal => continue,
                                Value::Str(payload) => {
                                    yield MonitorConfig::from_json(payload.as_str())?;
                                }
                                other => Err(anyhow!(
                                    "manual reconfiguration payload must be a string, got {other:?}"
                                ))?,
                            }
                        }
                    });
                Ok(controlled_input_stream(data, control))
            }
            InputSourceKind::File { .. }
            | InputSourceKind::InMemoryRows { .. }
            | InputSourceKind::InMemoryTicks { .. } => {
                anyhow::bail!("file and in-memory row/tick sources do not support reconfiguration")
            }
        }
    }
}

enum ControlledInputNext<V> {
    Data(anyhow::Result<InputBatch<V>>),
    Control(anyhow::Result<MonitorConfig>),
    Complete,
}

fn poll_controlled_input<V>(
    data: &mut Option<InputStream<V>>,
    control: &mut Option<crate::OutputStream<anyhow::Result<MonitorConfig>>>,
    cx: &mut std::task::Context<'_>,
) -> std::task::Poll<ControlledInputNext<V>> {
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

fn controlled_input_stream<V: 'static>(
    mut data: Option<InputStream<V>>,
    control: crate::OutputStream<anyhow::Result<MonitorConfig>>,
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
                    return;
                }
                ControlledInputNext::Data(batch) => {
                    yield ReconfigurableInputItem::Data(batch?);
                }
                ControlledInputNext::Complete => return,
            }
        }
    })
}

#[derive(Clone, Debug)]
pub struct InputPipeline<V = Value> {
    sources: InputSources<V>,
    stages: Box<[InputStage]>,
}

impl<V> InputPipeline<V> {
    pub fn new(source: InputSource<V>) -> Self {
        Self {
            sources: InputSources::single(source),
            stages: Box::new([]),
        }
    }

    pub fn from_sources(sources: InputSources<V>) -> Self {
        Self {
            sources,
            stages: Box::new([]),
        }
    }

    pub fn sources(&self) -> &InputSources<V> {
        &self.sources
    }

    pub fn with_stage(mut self, stage: InputStage) -> anyhow::Result<Self> {
        anyhow::ensure!(
            stage.window().is_bounded(),
            "input window requires max_delay or update_limit"
        );
        anyhow::ensure!(
            self.stages.is_empty(),
            "input pipeline accepts one window stage"
        );
        self.stages = vec![stage].into_boxed_slice();
        Ok(self)
    }

    pub fn stages(&self) -> &[InputStage] {
        &self.stages
    }

    pub(crate) fn ensure_reconfigurable(
        &self,
        requested_route: Option<&str>,
    ) -> anyhow::Result<()> {
        for source in self.sources.sources.values() {
            anyhow::ensure!(
                !matches!(source.kind, InputSourceKind::File { .. }),
                "file-backed reconfiguration is not supported"
            );
        }
        self.sources
            .resolve_reconfiguration_source(requested_route)
            .map(|_| ())
    }

    pub(crate) fn resolve(
        &self,
        model_inputs: &BTreeSet<VarName>,
        monitor_config: Option<&MonitorConfig>,
    ) -> anyhow::Result<ResolvedInput> {
        match monitor_config {
            Some(config) => self.sources.resolve_monitor_config(config, model_inputs),
            None => self.sources.resolve_default(model_inputs),
        }
    }

    pub fn into_sources(self) -> InputSources<V> {
        self.sources
    }

    pub async fn build(&self, input_vars: BTreeSet<VarName>) -> anyhow::Result<InputStream<V>>
    where
        V: FileInputValue + RosStreamValue,
    {
        let resolved = self.resolve(&input_vars, None)?;
        self.open(resolved).await
    }

    pub(crate) async fn open(&self, resolved: ResolvedInput) -> anyhow::Result<InputStream<V>>
    where
        V: FileInputValue + RosStreamValue,
    {
        let mut streams = Vec::new();
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
            let source_stream = source
                .clone()
                .open(bindings, variables)
                .await
                .with_context(|| format!("input source `{description}` could not be opened"))?;
            let stream: InputStream<V> = Box::pin(async_stream::stream! {
                let mut source_stream = source_stream;
                while let Some(result) = source_stream.next().await {
                    yield result.with_context(|| {
                        format!("input source `{description}` emitted an error")
                    });
                }
            });
            streams.push(stream);
        }
        let mut stream = input::compose_input_streams(streams);
        for stage in self.stages.iter().cloned() {
            stream = crate::io::aggregation::apply_stage(stream, stage)?;
        }
        Ok(stream)
    }

    pub(crate) async fn open_reconfigurable(
        &self,
        resolved: ResolvedInput,
        control: &ReconfigurationControl,
    ) -> anyhow::Result<ReconfigurableInputStream<V>>
    where
        V: FileInputValue + RosStreamValue,
    {
        let mut sources = resolved.into_sources();
        if !sources
            .iter()
            .any(|source| source.source() == &control.source)
        {
            sources.push(ResolvedSource::new(control.source.clone(), []));
        }
        let mut streams = Vec::new();
        for source_plan in sources {
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
            let stream: ReconfigurableInputStream<V> = if source_plan.source() == &control.source {
                let control_stream = source
                    .clone()
                    .open_with_control(bindings, variables, control.route.clone())
                    .await
                    .with_context(|| {
                        format!("reconfiguration source `{description}` could not be opened")
                    })?;
                Box::pin(async_stream::stream! {
                    let mut control_stream = control_stream;
                    while let Some(item) = control_stream.next().await {
                        yield item.map_err(|error| {
                            error.context(format!(
                                "reconfiguration source `{description}` emitted an error"
                            ))
                        });
                    }
                })
            } else {
                let source_stream = source
                    .clone()
                    .open(bindings, variables)
                    .await
                    .with_context(|| format!("input source `{description}` could not be opened"))?;
                Box::pin(async_stream::try_stream! {
                    let mut source_stream = source_stream;
                    while let Some(batch) = source_stream.next().await {
                        yield ReconfigurableInputItem::Data(batch.with_context(|| {
                            format!("input source `{description}` emitted an error")
                        })?);
                    }
                })
            };
            streams.push(stream);
        }
        Ok(Box::pin(async_stream::stream! {
            let mut streams = futures::stream::select_all(streams);
            while let Some(item) = streams.next().await {
                let terminal = matches!(&item, Ok(ReconfigurableInputItem::Reconfigure(_)));
                yield item;
                if terminal {
                    return;
                }
            }
        }))
    }
}

#[cfg(test)]
mod resolution_tests {
    use super::*;

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
            MqttInputBackend::default(),
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
        assert_eq!(resolved.sources()[0].bindings()[0].route(), "/x");
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
            MqttInputBackend::default(),
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
        let config = MonitorConfig::from_json(
            r#"{
                "spec": "in x",
                "source": "telemetry",
                "inputs": {"x": "/replacement/x"}
            }"#,
        )
        .unwrap();
        let variables = BTreeSet::from([VarName::new("x")]);
        let resolved = pipeline.resolve(&variables, Some(&config)).unwrap();
        assert_eq!(resolved.sources()[0].source(), "telemetry");
        assert_eq!(
            resolved.sources()[0].bindings()[0].route(),
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
    fn explicit_monitor_bindings_must_cover_all_inputs() {
        let pipeline = InputPipeline::<Value>::new(InputSource::mqtt(None, None));
        let config = MonitorConfig::from_json(
            r#"{
                "spec": "in x\nin y",
                "inputs": {"x": "/x"}
            }"#,
        )
        .unwrap();
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
    fn control_only_manual_source_remains_open_for_reconfiguration() {
        smol::block_on(async {
            let (control_sender, control_fanout) = Fanout::<Value>::new();
            let (_data_sender, data_fanout) = Fanout::<Value>::new();
            let data_source =
                InputSource::<Value>::manual(BTreeMap::from([("x".into(), data_fanout)]));
            let control_source =
                InputSource::<Value>::manual_with_control(BTreeMap::new(), Some(control_fanout))
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
                .open_reconfigurable(resolved, &control)
                .await
                .unwrap();

            control_sender
                .send(Value::Str(r#"{"spec":"in x"}"#.into()))
                .await;

            let item = stream.next().await.unwrap().unwrap();
            assert!(matches!(item, ReconfigurableInputItem::Reconfigure(_)));
            assert!(stream.next().await.is_none());
        });
    }

    #[test]
    fn manual_control_is_eventually_delivered_with_ready_data() {
        smol::block_on(async {
            let (control_sender, control_fanout) = Fanout::<Value>::new();
            let (data_sender, data_fanout) = Fanout::<Value>::new();
            let source = InputSource::manual_with_control(
                BTreeMap::from([(VarName::new("x"), data_fanout)]),
                Some(control_fanout),
            );
            let pipeline = InputPipeline::new(source);
            let variables = BTreeSet::from([VarName::new("x")]);
            let resolved = pipeline.resolve(&variables, None).unwrap();
            let control = ReconfigurationControl::new("default", "control").unwrap();
            let mut stream = pipeline
                .open_reconfigurable(resolved, &control)
                .await
                .unwrap();

            for value in 0..32 {
                data_sender.send(Value::Int(value)).await;
            }

            let first = stream.next().await.unwrap().unwrap();
            assert!(matches!(first, ReconfigurableInputItem::Data(_)));

            control_sender
                .send(Value::Str(r#"{"spec":"in x"}"#.into()))
                .await;

            let item = stream.next().await.unwrap().unwrap();
            assert!(matches!(item, ReconfigurableInputItem::Reconfigure(_)));
            assert!(stream.next().await.is_none());
        });
    }

    #[test]
    fn terminated_manual_control_does_not_stop_ready_data() {
        smol::block_on(async {
            let (control_sender, control_fanout) = Fanout::<Value>::new();
            let (data_sender, data_fanout) = Fanout::<Value>::new();
            let source = InputSource::manual_with_control(
                BTreeMap::from([(VarName::new("x"), data_fanout)]),
                Some(control_fanout),
            );
            let pipeline = InputPipeline::new(source);
            let variables = BTreeSet::from([VarName::new("x")]);
            let resolved = pipeline.resolve(&variables, None).unwrap();
            let control = ReconfigurationControl::new("default", "control").unwrap();
            let mut stream = pipeline
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
        });
    }

    #[test]
    fn data_eof_leaves_control_active() {
        smol::block_on(async {
            let data: InputStream<Value> = Box::pin(futures::stream::empty());
            let control: crate::OutputStream<anyhow::Result<MonitorConfig>> =
                Box::pin(futures::stream::once(async {
                    smol::future::yield_now().await;
                    MonitorConfig::from_json(r#"{"spec":"in x"}"#)
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
            let control: crate::OutputStream<anyhow::Result<MonitorConfig>> =
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

        let sources = sources
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
    fn redis_knowledge_catalog_is_generation_scoped_and_not_a_control_source() {
        let source = InputSource::<Value>::redis_knowledge(RedisKnowledgeConfig {
            host: "redis".to_owned(),
            port: None,
            database: 2,
            publish_initial: true,
            keys: BTreeMap::from([
                (VarName::new("current"), "knowledge:current".to_owned()),
                (VarName::new("future"), "knowledge:future".to_owned()),
            ]),
            retry: crate::io::redis::RedisKnowledgeRetry::default(),
        });
        let pipeline = InputPipeline::new(source);
        let resolved = pipeline
            .resolve(&BTreeSet::from([VarName::new("current")]), None)
            .unwrap();
        assert_eq!(
            resolved.sources()[0].bindings()[0].route(),
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
                retry: crate::io::redis::RedisKnowledgeRetry::default(),
            },
        ));
        let config = MonitorConfig::from_json(
            r#"{
                spec: "in x\nin y",
                source: "default",
                inputs: {x: "same:key", y: "same:key"}
            }"#,
        )
        .unwrap();
        let error = pipeline
            .resolve(
                &BTreeSet::from([VarName::new("x"), VarName::new("y")]),
                Some(&config),
            )
            .unwrap_err();
        assert!(error.to_string().contains("active Redis knowledge key"));
    }
}
