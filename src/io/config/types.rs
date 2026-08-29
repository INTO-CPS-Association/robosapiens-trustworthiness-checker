use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    rc::Rc,
    time::Duration,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::io::{RedisKnowledgeConfig, RedisKnowledgeRetry};
use crate::{VarName, core::REDIS_HOSTNAME};

pub type TopicMapping = BTreeMap<VarName, String>;
pub type MsgTypeMapping = BTreeMap<VarName, String>;

/// Stable identifier for a configured output destination.
pub type DestinationId = String;

/// Backend names used by the resource-free output configuration file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DestinationKind {
    Stdout,
    Null,
    LimitedNull,
    Mqtt,
    Redis,
    Ros,
}

/// A serializable output stage. Runtime code turns this into an
/// [`crate::io::output::OutputStage`]
/// after validating its numeric bounds. Keeping this wire type independent of
/// executors and opened backends makes output reconfiguration resource-free.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub enum OutputStageConfig {
    Buffer {
        max_batches: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_updates: Option<usize>,
    },
    Coalesce {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_delay_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tick_limit: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        update_limit: Option<usize>,
    },
}

impl OutputStageConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::Buffer {
                max_batches,
                max_updates,
            } => {
                anyhow::ensure!(
                    *max_batches > 0,
                    "output buffer max_batches must be greater than zero"
                );
                if let Some(max_updates) = max_updates {
                    anyhow::ensure!(
                        *max_updates > 0,
                        "output buffer max_updates must be greater than zero"
                    );
                }
                Ok(())
            }
            Self::Coalesce {
                max_delay_ms,
                tick_limit,
                update_limit,
            } => {
                anyhow::ensure!(
                    max_delay_ms.is_some() || tick_limit.is_some() || update_limit.is_some(),
                    "output coalescing requires a delay, tick_limit, or update_limit"
                );
                if let Some(tick_limit) = tick_limit {
                    anyhow::ensure!(
                        *tick_limit > 0,
                        "output coalescing tick_limit must be greater than zero"
                    );
                }
                if let Some(update_limit) = update_limit {
                    anyhow::ensure!(
                        *update_limit > 0,
                        "output coalescing update_limit must be greater than zero"
                    );
                }
                let _ = max_delay_ms;
                Ok(())
            }
        }
    }
}

/// Resource-free configuration for one named output destination.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationConfig {
    pub kind: DestinationKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub routes: BTreeMap<VarName, Route>,
    /// Legacy alias for a route-free partition, primarily for local destinations.
    /// It is mutually exclusive with `partition` and `mirror`; new configs may
    /// use `partition` instead. `None` means that the selector is absent;
    /// present selections must not be empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variables: Option<BTreeSet<VarName>>,
    /// Variables assigned to this destination as a disjoint partition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<BTreeSet<VarName>>,
    /// Mirror all model outputs assigned to a primary destination.
    #[serde(default)]
    pub mirror: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stages: Vec<OutputStageConfig>,
}

impl DestinationConfig {
    pub fn stdout() -> Self {
        Self::new(DestinationKind::Stdout)
    }

    pub fn null() -> Self {
        Self::new(DestinationKind::Null)
    }

    pub fn mqtt() -> Self {
        Self::new(DestinationKind::Mqtt)
    }

    pub fn redis() -> Self {
        Self::new(DestinationKind::Redis)
    }

    pub fn ros() -> Self {
        Self::new(DestinationKind::Ros)
    }

    pub fn new(kind: DestinationKind) -> Self {
        Self {
            kind,
            host: None,
            port: None,
            limit: None,
            routes: BTreeMap::new(),
            variables: None,
            partition: None,
            mirror: false,
            stages: Vec::new(),
        }
    }

    fn establishes_role(&self) -> bool {
        !self.routes.is_empty()
            || self.variables.is_some()
            || self.partition.is_some()
            || self.mirror
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        match self.kind {
            DestinationKind::Mqtt | DestinationKind::Redis => {
                if let Some(host) = &self.host {
                    anyhow::ensure!(
                        !host.trim().is_empty(),
                        "output destination host cannot be empty"
                    );
                }
                anyhow::ensure!(
                    self.limit.is_none(),
                    "output destination kind {:?} does not support `limit`; `limit` is only valid for `limited-null`",
                    self.kind
                );
            }
            DestinationKind::LimitedNull => {
                anyhow::ensure!(
                    self.host.is_none(),
                    "output destination kind {:?} does not support `host`",
                    self.kind
                );
                anyhow::ensure!(
                    self.port.is_none(),
                    "output destination kind {:?} does not support `port`",
                    self.kind
                );
                let limit = self.limit.ok_or_else(|| {
                    anyhow::anyhow!(
                        "limited-null output destination requires a `limit` greater than zero"
                    )
                })?;
                anyhow::ensure!(
                    limit > 0,
                    "limited-null output destination `limit` must be greater than zero"
                );
            }
            DestinationKind::Stdout | DestinationKind::Null | DestinationKind::Ros => {
                anyhow::ensure!(
                    self.host.is_none(),
                    "output destination kind {:?} does not support `host`",
                    self.kind
                );
                anyhow::ensure!(
                    self.port.is_none(),
                    "output destination kind {:?} does not support `port`",
                    self.kind
                );
                anyhow::ensure!(
                    self.limit.is_none(),
                    "output destination kind {:?} does not support `limit`; `limit` is only valid for `limited-null`",
                    self.kind
                );
            }
        }

        let selector_count = usize::from(self.variables.is_some())
            + usize::from(self.partition.is_some())
            + usize::from(self.mirror);
        anyhow::ensure!(
            selector_count <= 1,
            "output destination selector fields `variables`, `partition`, and `mirror` are mutually exclusive"
        );
        if let Some(variables) = &self.variables {
            anyhow::ensure!(
                !variables.is_empty(),
                "output destination `variables` cannot be empty"
            );
            anyhow::ensure!(
                self.routes
                    .keys()
                    .all(|variable| variables.contains(variable)),
                "output destination `variables` must include every declared route variable"
            );
        }
        if let Some(partition) = &self.partition {
            anyhow::ensure!(
                !partition.is_empty(),
                "output destination partition cannot be empty"
            );
            for variable in partition {
                anyhow::ensure!(
                    !variable.name().trim().is_empty(),
                    "output destination partition variable cannot be empty"
                );
            }
        }
        for (variable, route) in &self.routes {
            anyhow::ensure!(
                !variable.name().trim().is_empty(),
                "output route variable cannot be empty"
            );
            let route = Route::new(route.route.clone(), route.codec.clone())?;
            match self.kind {
                DestinationKind::Mqtt | DestinationKind::Redis => {
                    if let Some(codec) = &route.codec {
                        anyhow::ensure!(
                            matches!(codec.0.as_ref(), "json" | "json5"),
                            "output codec `{codec}` is not supported by {:?}",
                            self.kind
                        );
                    }
                }
                DestinationKind::Ros => anyhow::ensure!(
                    route.codec.is_some(),
                    "ROS output route for `{variable}` requires a codec"
                ),
                DestinationKind::Stdout | DestinationKind::Null | DestinationKind::LimitedNull => {
                    anyhow::ensure!(
                        route.codec.is_none(),
                        "output backend {:?} does not support a codec",
                        self.kind
                    )
                }
            }
        }
        if let Some(variables) = &self.variables {
            for variable in variables {
                anyhow::ensure!(
                    !variable.name().trim().is_empty(),
                    "output destination variable cannot be empty"
                );
            }
        }
        for stage in &self.stages {
            stage.validate()?;
        }
        Ok(())
    }
}

/// Durable local output configuration. It contains backend parameters but no
/// opened network clients, ROS nodes, or worker tasks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputConfigFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<DestinationId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shared_stages: Vec<OutputStageConfig>,
    #[serde(default)]
    pub destinations: BTreeMap<DestinationId, DestinationConfig>,
}

impl OutputConfigFile {
    pub fn new(destinations: BTreeMap<DestinationId, DestinationConfig>) -> Self {
        Self {
            default: None,
            shared_stages: Vec::new(),
            destinations,
        }
    }

    pub fn single(id: impl Into<DestinationId>, destination: DestinationConfig) -> Self {
        Self::new(BTreeMap::from([(id.into(), destination)]))
    }

    pub fn from_json(payload: &str) -> anyhow::Result<Self> {
        let config: Self = json5::from_str(payload)
            .map_err(|error| anyhow::anyhow!("invalid output configuration: {error}"))?;
        config.validate()?;
        Ok(config)
    }

    pub(crate) fn inferred_default(&self) -> Option<DestinationId> {
        if self.default.is_some() || self.destinations.len() <= 1 {
            return None;
        }
        let mut unqualified = self
            .destinations
            .iter()
            .filter(|(_, destination)| !destination.establishes_role());
        let (id, _) = unqualified.next()?;
        unqualified.next().is_none().then(|| id.clone())
    }

    pub(crate) fn effective_default(&self) -> Option<DestinationId> {
        self.default.clone().or_else(|| self.inferred_default())
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.destinations.is_empty(),
            "output configuration must contain at least one destination"
        );
        if let Some(default) = &self.default {
            anyhow::ensure!(
                self.destinations.contains_key(default),
                "output config default destination `{default}` does not exist"
            );
        }
        for (id, destination) in &self.destinations {
            anyhow::ensure!(!id.is_empty(), "output destination ID cannot be empty");
            destination.validate().map_err(|error| {
                anyhow::anyhow!("output destination `{id}` is invalid: {error}")
            })?;
        }
        if self.destinations.len() > 1 {
            let inferred_default = self.inferred_default();
            for (id, destination) in &self.destinations {
                let is_configured_default = self.default.as_ref() == Some(id);
                let is_inferred_default = inferred_default.as_ref() == Some(id);
                anyhow::ensure!(
                    is_configured_default || is_inferred_default || destination.establishes_role(),
                    "non-default output destination `{id}` in a multi-destination config must explicitly declare a role with `partition`, `variables`, `mirror`, or `routes`; only the configured or unique inferred default may use primary `All`"
                );
            }
        }
        for stage in &self.shared_stages {
            stage.validate()?;
        }
        Ok(())
    }
}

/// Stable identifier for a configured local input source.
pub type SourceId = String;

/// Codec selected by a route. MQTT and Redis can omit it because their value
/// codec is the source's normal JSON5 codec; ROS routes generally specify it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct CodecId(pub Box<str>);

impl CodecId {
    pub fn new(codec: impl Into<Box<str>>) -> Self {
        Self(codec.into())
    }
}

impl std::fmt::Display for CodecId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Normalized route shared by input, output, and reconfiguration messages.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Route {
    pub route: Box<str>,
    pub codec: Option<CodecId>,
}

impl Route {
    pub fn new(route: impl Into<Box<str>>, codec: Option<CodecId>) -> anyhow::Result<Self> {
        let route = route.into();
        anyhow::ensure!(!route.trim().is_empty(), "route cannot be empty");
        if let Some(codec) = &codec {
            anyhow::ensure!(!codec.0.trim().is_empty(), "route codec cannot be empty");
        }
        Ok(Self { route, codec })
    }
}

impl Serialize for Route {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.codec {
            Some(codec) => (self.route.as_ref(), codec).serialize(serializer),
            None => self.route.serialize(serializer),
        }
    }
}

/// Compact wire route: either a route string or `[route, codec]`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum WireRoute {
    Route(String),
    RouteAndCodec(String, String),
}

impl<'de> Deserialize<'de> for Route {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        WireRoute::deserialize(deserializer)?
            .into_route()
            .map_err(serde::de::Error::custom)
    }
}

impl WireRoute {
    pub fn into_route(self) -> anyhow::Result<Route> {
        match self {
            Self::Route(route) => Route::new(route, None),
            Self::RouteAndCodec(route, codec) => Route::new(route, Some(CodecId::new(codec))),
        }
    }

    fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::Route(route) => Route::new(route.as_str(), None),
            Self::RouteAndCodec(route, codec) => {
                Route::new(route.as_str(), Some(CodecId::new(codec.as_str())))
            }
        }
        .map(|_| ())
    }
}

/// Request-specific input bindings. Source catalogs and their opened
/// resources remain owned by [`crate::io::InputPipeline`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputConfiguration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inputs: Option<BTreeMap<VarName, Route>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sources: Option<BTreeMap<SourceId, BTreeMap<VarName, Route>>>,
}

impl InputConfiguration {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.validate_structure()
    }

    pub fn validate_structure(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !(self.inputs.is_some() && self.sources.is_some()),
            "input configuration cannot contain both `inputs` and `sources`"
        );
        anyhow::ensure!(
            self.source.is_none() || self.inputs.is_some(),
            "input configuration `source` requires `inputs`"
        );
        anyhow::ensure!(
            self.source.is_none() || self.sources.is_none(),
            "input configuration cannot contain `source` together with `sources`"
        );
        if let Some(source) = &self.source {
            anyhow::ensure!(!source.trim().is_empty(), "input source ID cannot be empty");
        }
        if let Some(inputs) = &self.inputs {
            validate_input_routes(inputs)?;
        }
        if let Some(sources) = &self.sources {
            let mut variables = BTreeMap::<VarName, &SourceId>::new();
            for (source, bindings) in sources {
                anyhow::ensure!(!source.trim().is_empty(), "input source ID cannot be empty");
                validate_input_routes(bindings)?;
                for variable in bindings.keys() {
                    if let Some(previous) = variables.insert(variable.clone(), source) {
                        anyhow::bail!(
                            "input variable `{variable}` is bound by both source `{previous}` and source `{source}`"
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// Explicit source-qualified input bindings. An omitted source means the
    /// input source set must resolve the owner from catalogs/defaults.
    pub fn explicit_input_bindings(&self) -> Vec<(Option<&str>, &VarName, &Route)> {
        if let Some(inputs) = &self.inputs {
            return inputs
                .iter()
                .map(|(variable, route)| (self.source.as_deref(), variable, route))
                .collect();
        }
        self.sources
            .as_ref()
            .into_iter()
            .flat_map(|sources| {
                sources.iter().flat_map(|(source, bindings)| {
                    bindings
                        .iter()
                        .map(move |(variable, route)| (Some(source.as_str()), variable, route))
                })
            })
            .collect()
    }
}

fn validate_input_routes(routes: &BTreeMap<VarName, Route>) -> anyhow::Result<()> {
    for (variable, route) in routes {
        anyhow::ensure!(
            !variable.name().trim().is_empty(),
            "input variable cannot be empty"
        );
        Route::new(route.route.clone(), route.codec.clone())?;
    }
    Ok(())
}

/// Request-specific output routing. Destination implementations and their
/// local catalogs remain owned by [`crate::io::OutputBackendBuilder`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputConfiguration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outputs: Option<BTreeMap<VarName, Route>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<DestinationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destinations: Option<BTreeMap<DestinationId, BTreeMap<VarName, Route>>>,
}

impl OutputConfiguration {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.validate_structure()
    }

    pub fn validate_structure(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !(self.outputs.is_some() && self.destinations.is_some()),
            "output configuration cannot contain both `outputs` and `destinations`"
        );
        anyhow::ensure!(
            self.destination.is_none() || self.outputs.is_some(),
            "output configuration `destination` requires `outputs`"
        );
        anyhow::ensure!(
            self.destination.is_none() || self.destinations.is_none(),
            "output configuration cannot contain `destination` together with `destinations`"
        );
        if let Some(destination) = &self.destination {
            anyhow::ensure!(
                !destination.trim().is_empty(),
                "output destination ID cannot be empty"
            );
        }
        if let Some(destinations) = &self.destinations {
            for (destination, routes) in destinations {
                anyhow::ensure!(
                    !destination.trim().is_empty(),
                    "output destination ID cannot be empty"
                );
                validate_output_routes(routes)?;
            }
        }
        if let Some(outputs) = &self.outputs {
            validate_output_routes(outputs)?;
        }
        Ok(())
    }
}

fn validate_output_routes(routes: &BTreeMap<VarName, Route>) -> anyhow::Result<()> {
    for (variable, route) in routes {
        anyhow::ensure!(
            !variable.name().trim().is_empty(),
            "output variable cannot be empty"
        );
        Route::new(route.route.clone(), route.codec.clone())?;
    }
    Ok(())
}

/// A monitor replacement request. The nested input and output values keep
/// transport-independent reconfiguration data separate from runtime state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconfigurationRequest {
    pub specification: String,
    pub input: InputConfiguration,
    pub output: OutputConfiguration,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireReconfigurationRequest {
    specification: String,
    #[serde(default)]
    input: InputConfiguration,
    #[serde(default)]
    output: OutputConfiguration,
}

impl Serialize for ReconfigurationRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        WireReconfigurationRequest {
            specification: self.specification.clone(),
            input: self.input.clone(),
            output: self.output.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ReconfigurationRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let WireReconfigurationRequest {
            specification,
            input,
            output,
        } = WireReconfigurationRequest::deserialize(deserializer)?;
        let request = Self {
            specification,
            input,
            output,
        };
        request
            .validate_structure()
            .map_err(serde::de::Error::custom)?;
        Ok(request)
    }
}

impl ReconfigurationRequest {
    pub fn new(specification: impl Into<String>) -> Self {
        Self {
            specification: specification.into(),
            input: InputConfiguration::default(),
            output: OutputConfiguration::default(),
        }
    }

    pub fn from_json(payload: &str) -> anyhow::Result<Self> {
        let request: Self = json5::from_str(payload)
            .map_err(|error| anyhow::anyhow!("invalid reconfiguration request: {error}"))?;
        request.validate_structure()?;
        Ok(request)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        self.validate_structure()
    }

    pub fn validate_structure(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.specification.trim().is_empty(),
            "reconfiguration specification cannot be empty"
        );
        self.input.validate_structure()?;
        self.output.validate_structure()?;
        Ok(())
    }
}

fn default_redis_knowledge_database() -> u32 {
    2
}

fn default_redis_knowledge_publish_initial() -> bool {
    true
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub enum SourceConfig {
    Mqtt {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        port: Option<u16>,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        routes: BTreeMap<VarName, WireRoute>,
        /// Transport-local route carrying monitor reconfiguration messages.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reconfiguration_route: Option<Box<str>>,
    },
    Redis {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        port: Option<u16>,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        routes: BTreeMap<VarName, WireRoute>,
        /// Transport-local route carrying monitor reconfiguration messages.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reconfiguration_route: Option<Box<str>>,
    },
    #[serde(rename = "redis-knowledge")]
    RedisKnowledge {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        port: Option<u16>,
        #[serde(default = "default_redis_knowledge_database")]
        database: u32,
        #[serde(default = "default_redis_knowledge_publish_initial")]
        publish_initial: bool,
        keys: BTreeMap<VarName, String>,
        #[serde(default)]
        retry: RedisKnowledgeRetry,
    },
    Ros {
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        routes: BTreeMap<VarName, WireRoute>,
        /// Transport-local route carrying monitor reconfiguration messages.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reconfiguration_route: Option<Box<str>>,
    },
}

impl SourceConfig {
    pub fn host(&self) -> Option<&str> {
        match self {
            Self::Mqtt { host, .. }
            | Self::Redis { host, .. }
            | Self::RedisKnowledge { host, .. } => host.as_deref(),
            Self::Ros { .. } => None,
        }
    }

    pub fn routes(&self) -> &BTreeMap<VarName, WireRoute> {
        match self {
            Self::Mqtt { routes, .. } | Self::Redis { routes, .. } | Self::Ros { routes, .. } => {
                routes
            }
            Self::RedisKnowledge { .. } => {
                static EMPTY_ROUTES: std::sync::OnceLock<BTreeMap<VarName, WireRoute>> =
                    std::sync::OnceLock::new();
                EMPTY_ROUTES.get_or_init(BTreeMap::new)
            }
        }
    }

    pub fn reconfiguration_route(&self) -> Option<&str> {
        match self {
            Self::Mqtt {
                reconfiguration_route,
                ..
            }
            | Self::Redis {
                reconfiguration_route,
                ..
            }
            | Self::Ros {
                reconfiguration_route,
                ..
            } => reconfiguration_route.as_deref(),
            Self::RedisKnowledge { .. } => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InputConfigFile {
    pub default: Option<SourceId>,
    pub sources: BTreeMap<SourceId, SourceConfig>,
}

impl InputConfigFile {
    pub fn validate(&self) -> anyhow::Result<()> {
        if let Some(default) = &self.default {
            anyhow::ensure!(
                self.sources.contains_key(default),
                "input config default source `{default}` does not exist"
            );
        }

        let mut declared_reconfiguration_sources = 0;
        let mut variables = BTreeMap::<VarName, &SourceId>::new();
        for (source, config) in &self.sources {
            anyhow::ensure!(!source.trim().is_empty(), "input source ID cannot be empty");
            if let Some(host) = config.host() {
                anyhow::ensure!(
                    !host.trim().is_empty(),
                    "input source `{source}` has an empty `host`"
                );
            }
            if let Some(route) = config.reconfiguration_route() {
                declared_reconfiguration_sources += 1;
                anyhow::ensure!(
                    !route.trim().is_empty(),
                    "input source `{source}` has an empty `reconfiguration_route`"
                );
            }

            match config {
                SourceConfig::RedisKnowledge {
                    host,
                    port,
                    database,
                    publish_initial,
                    keys,
                    retry,
                } => {
                    let knowledge = RedisKnowledgeConfig {
                        host: host.clone().unwrap_or_else(|| REDIS_HOSTNAME.to_owned()),
                        port: *port,
                        database: *database,
                        publish_initial: *publish_initial,
                        keys: keys.clone(),
                        retry: retry.clone(),
                    };
                    knowledge.validate().map_err(|error| {
                        anyhow::anyhow!(
                            "input source `{source}` has invalid Redis knowledge configuration: {error}"
                        )
                    })?;
                    for (variable, _) in keys {
                        if let Some(previous) = variables.insert(variable.clone(), source) {
                            anyhow::bail!(
                                "input config variable `{variable}` appears in both source `{previous}` and source `{source}`"
                            );
                        }
                    }
                }
                _ => {
                    for (variable, route) in config.routes() {
                        route.validate().map_err(|error| {
                            anyhow::anyhow!(
                                "input source `{source}` has an invalid route for `{variable}`: {error}"
                            )
                        })?;
                        if let Some(previous) = variables.insert(variable.clone(), source) {
                            anyhow::bail!(
                                "input config variable `{variable}` appears in both source `{previous}` and source `{source}`"
                            );
                        }
                    }
                }
            }
        }
        anyhow::ensure!(
            declared_reconfiguration_sources <= 1,
            "input config has {declared_reconfiguration_sources} sources declaring `reconfiguration_route`; exactly one is allowed"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputWindow {
    pub max_delay: Option<Duration>,
    pub update_limit: Option<NonZeroUsize>,
}

impl InputWindow {
    pub fn new(
        max_delay: Option<Duration>,
        update_limit: Option<NonZeroUsize>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            max_delay.is_some() || update_limit.is_some(),
            "input window requires max_delay or update_limit"
        );
        Ok(Self {
            max_delay,
            update_limit,
        })
    }

    pub fn is_bounded(&self) -> bool {
        self.max_delay.is_some() || self.update_limit.is_some()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InputReduction {
    LastUpdateWins,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InputStage {
    Batch(InputWindow),
    WindowToStep {
        window: InputWindow,
        reduction: InputReduction,
    },
}

impl InputStage {
    pub fn window(&self) -> &InputWindow {
        match self {
            Self::Batch(window) => window,
            Self::WindowToStep { window, .. } => window,
        }
    }
}

/// An immutable, request-specific binding resolved from source catalogs and
/// monitor configuration. These types stay inside input orchestration; callers
/// configure sources and routes rather than constructing resolved inputs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ResolvedBinding {
    variable: VarName,
    route: Box<str>,
    codec: CodecId,
}

impl ResolvedBinding {
    pub(crate) fn new(variable: VarName, route: Box<str>, codec: CodecId) -> Self {
        Self {
            variable,
            route,
            codec,
        }
    }

    pub(crate) fn variable(&self) -> &VarName {
        &self.variable
    }

    pub(crate) fn route(&self) -> &str {
        &self.route
    }

    pub(crate) fn codec(&self) -> &CodecId {
        &self.codec
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ResolvedSource {
    source: SourceId,
    bindings: Box<[ResolvedBinding]>,
}

impl ResolvedSource {
    pub(crate) fn new(
        source: SourceId,
        bindings: impl IntoIterator<Item = ResolvedBinding>,
    ) -> Self {
        Self {
            source,
            bindings: bindings.into_iter().collect(),
        }
    }

    pub(crate) fn source(&self) -> &SourceId {
        &self.source
    }

    pub(crate) fn bindings(&self) -> &[ResolvedBinding] {
        &self.bindings
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ResolvedInput {
    sources: Box<[ResolvedSource]>,
    #[serde(skip)]
    pipeline_identity: Rc<()>,
    #[serde(skip)]
    pipeline_configuration: Box<str>,
    #[serde(skip)]
    fingerprint: Box<str>,
}

impl ResolvedInput {
    pub(crate) fn new(sources: impl IntoIterator<Item = ResolvedSource>) -> Self {
        let mut resolved = Self {
            sources: sources.into_iter().collect(),
            pipeline_identity: Rc::new(()),
            pipeline_configuration: String::new().into_boxed_str(),
            fingerprint: String::new().into_boxed_str(),
        };
        resolved.fingerprint = resolved.compute_fingerprint().into_boxed_str();
        resolved
    }

    pub(crate) fn attach_to_pipeline(
        mut self,
        pipeline_identity: &Rc<()>,
        pipeline_configuration: &str,
    ) -> Self {
        self.pipeline_identity = Rc::clone(pipeline_identity);
        self.pipeline_configuration = pipeline_configuration.to_owned().into_boxed_str();
        self.fingerprint = self.compute_fingerprint().into_boxed_str();
        self
    }

    pub(crate) fn validate_for_pipeline(
        &self,
        pipeline_identity: &Rc<()>,
        pipeline_configuration: &str,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.fingerprint.as_ref() == self.compute_fingerprint().as_str(),
            "resolved input fingerprint does not match its structure"
        );
        anyhow::ensure!(
            self.pipeline_configuration.as_ref() == pipeline_configuration,
            "resolved input durable configuration does not match the pipeline"
        );
        anyhow::ensure!(
            Rc::ptr_eq(&self.pipeline_identity, pipeline_identity),
            "resolved input belongs to a different pipeline instance"
        );
        Ok(())
    }

    pub(crate) fn sources(&self) -> &[ResolvedSource] {
        &self.sources
    }

    fn compute_fingerprint(&self) -> String {
        format!(
            "pipeline_identity={:p};pipeline_configuration={:?};sources={:?}",
            Rc::as_ptr(&self.pipeline_identity),
            self.pipeline_configuration,
            self.sources,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input_config(json: &str) -> InputConfigFile {
        json5::from_str(json).expect("input config should deserialize")
    }

    #[test]
    fn resolved_input_fingerprint_rejects_tampering() {
        let identity = Rc::new(());
        let mut resolved = ResolvedInput::new([ResolvedSource::new(
            "source".to_owned(),
            [ResolvedBinding::new(
                VarName::new("x"),
                "/x".into(),
                CodecId::new("json"),
            )],
        )])
        .attach_to_pipeline(&identity, "configuration");
        resolved.sources[0].bindings[0].route = "tampered".into();

        let error = resolved
            .validate_for_pipeline(&identity, "configuration")
            .expect_err("tampered resolutions must fail their integrity check");

        assert!(
            error
                .to_string()
                .contains("resolved input fingerprint does not match its structure")
        );
    }

    #[test]
    fn reconfiguration_request_has_explicit_validated_defaults() {
        let request =
            ReconfigurationRequest::from_json(r#"{specification: "in pressure"}"#).unwrap();
        assert_eq!(request, ReconfigurationRequest::new("in pressure"));
        assert_eq!(request.input, InputConfiguration::default());
        assert_eq!(request.output, OutputConfiguration::default());
        request.input.validate().unwrap();
        request.output.validate().unwrap();
        assert_eq!(
            serde_json::to_value(request).unwrap(),
            serde_json::json!({
                "specification": "in pressure",
                "input": {},
                "output": {}
            })
        );
    }

    #[test]
    fn reconfiguration_request_accepts_json5_and_rejects_unknown_fields() {
        let config = ReconfigurationRequest::from_json(
            r#"{
                // Reconfiguration messages use the same JSON5 parser as files and streams.
                specification: "in pressure",
                input: {inputs: {pressure: "/pressure",}},
            }"#,
        )
        .unwrap();
        assert_eq!(config.specification, "in pressure");
        assert_eq!(
            config.input.inputs.unwrap()[&VarName::new("pressure")]
                .route
                .as_ref(),
            "/pressure"
        );

        for json in [
            r#"{specification:"in pressure",revision:1}"#,
            r#"{specification:"in pressure",bogus:true}"#,
            r#"{spec:"in pressure"}"#,
            r#"{specification:"in pressure",inputs:{pressure:"/pressure"}}"#,
        ] {
            assert!(
                ReconfigurationRequest::from_json(json).is_err(),
                "accepted {json}"
            );
        }
    }

    #[test]
    fn source_config_rejects_transport_specific_and_unsupported_fields() {
        let cases = [
            r#"{sources:{robot:{kind:"ros",host:"localhost"}}}"#,
            r#"{sources:{robot:{kind:"ros",port:1234}}}"#,
            r#"{sources:{broker:{kind:"mqtt",bogus:true}}}"#,
            r#"{sources:{local:{kind:"file"}}}"#,
            r#"{sources:{local:{kind:"manual"}}}"#,
        ];

        for json in cases {
            assert!(
                json5::from_str::<InputConfigFile>(json).is_err(),
                "accepted {json}"
            );
        }
    }

    #[test]
    fn input_config_validation_rejects_empty_ids_routes_and_hosts() {
        let cases = [
            (
                r#"{sources:{"":{kind:"mqtt"}}}"#,
                "source ID cannot be empty",
            ),
            (
                r#"{sources:{broker:{kind:"mqtt",host:"  "}}}"#,
                "empty `host`",
            ),
            (
                r#"{sources:{cache:{kind:"redis",host:""}}}"#,
                "empty `host`",
            ),
            (
                r#"{sources:{broker:{kind:"mqtt",routes:{value:""}}}}"#,
                "route cannot be empty",
            ),
            (
                r#"{sources:{robot:{kind:"ros",routes:{pose:["/pose",""]}}}}"#,
                "route codec cannot be empty",
            ),
            (
                r#"{sources:{control:{kind:"mqtt",reconfiguration_route:""}}}"#,
                "empty `reconfiguration_route`",
            ),
        ];

        for (json, expected) in cases {
            let error = input_config(json).validate().unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "{json} produced unexpected error: {error}"
            );
        }
    }

    #[test]
    fn input_config_validation_rejects_cross_source_conflicts() {
        let cases = [
            (
                r#"{default:"missing",sources:{broker:{kind:"mqtt"}}}"#,
                "default source `missing` does not exist",
            ),
            (
                r#"{sources:{first:{kind:"mqtt",routes:{value:"/first"}},second:{kind:"redis",routes:{value:"/second"}}}}"#,
                "appears in both source",
            ),
            (
                r#"{sources:{first:{kind:"mqtt",reconfiguration_route:"first-control"},second:{kind:"redis",reconfiguration_route:"second-control"}}}"#,
                "2 sources declaring `reconfiguration_route`",
            ),
        ];

        for (json, expected) in cases {
            let error = input_config(json).validate().unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "{json} produced unexpected error: {error}"
            );
        }
    }

    #[test]
    fn valid_mqtt_redis_and_ros_sources_deserialize_and_validate() {
        let config = input_config(
            r#"{
                default: "broker",
                sources: {
                    broker: {
                        kind: "mqtt",
                        host: "mqtt.example",
                        port: 1884,
                        routes: {pressure: "/pressure"},
                        reconfiguration_route: "/reconfigure"
                    },
                    cache: {
                        kind: "redis",
                        host: "redis.example",
                        port: 6380,
                        routes: {status: "status"}
                    },
                    robot: {
                        kind: "ros",
                        routes: {pose: ["/pose", "geometry_msgs/msg/Pose"]}
                    }
                }
            }"#,
        );

        config.validate().unwrap();
        let cases = [
            ("broker", "mqtt.example", "/reconfigure"),
            ("cache", "redis.example", ""),
            ("robot", "", ""),
        ];
        for (id, host, reconfiguration_route) in cases {
            let source = &config.sources[id];
            assert_eq!(source.host().unwrap_or_default(), host);
            assert_eq!(
                source.reconfiguration_route().unwrap_or_default(),
                reconfiguration_route
            );
            assert_eq!(source.routes().len(), 1);
        }
    }

    #[test]
    fn old_top_level_control_configuration_is_not_accepted() {
        let result = json5::from_str::<InputConfigFile>(
            r#"{
                sources: { telemetry: { kind: "mqtt" } },
                control: { source: "telemetry", route: "reconf" }
            }"#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn redis_knowledge_source_uses_tagged_name_and_defaults() {
        let config = input_config(
            r#"{
                sources: {
                    knowledge: {
                        kind: "redis-knowledge",
                        host: "redis",
                        keys: {
                            "knowledge.robot_mode": "robot:mode",
                            "knowledge.current_plan": "mape:plan:current"
                        }
                    }
                }
            }"#,
        );
        config.validate().unwrap();
        let SourceConfig::RedisKnowledge {
            database,
            publish_initial,
            retry,
            ..
        } = &config.sources["knowledge"]
        else {
            panic!("expected redis-knowledge source")
        };
        assert_eq!(*database, 2);
        assert!(*publish_initial);
        assert_eq!(retry, &RedisKnowledgeRetry::default());
        let serialized = serde_json::to_value(&config).unwrap();
        assert_eq!(
            serialized["sources"]["knowledge"]["kind"],
            "redis-knowledge"
        );
    }

    #[test]
    fn redis_knowledge_source_rejects_unknown_control_and_bad_keys() {
        for json in [
            r#"{sources:{knowledge:{kind:"redis-knowledge",keys:{x:""}}}}"#,
            r#"{sources:{knowledge:{kind:"redis-knowledge",keys:{x:"same",y:"same"}}}}"#,
            r#"{sources:{knowledge:{kind:"redis-knowledge",keys:{x:"key"},reconfiguration_route:"control"}}}"#,
            r#"{sources:{knowledge:{kind:"redisknowledge",keys:{x:"key"}}}}"#,
            r#"{sources:{knowledge:{kind:"redis-knowledge",keys:{x:"key"},retry:{initial_delay_ms:0,max_delay_ms:1}}}}"#,
        ] {
            let result = json5::from_str::<InputConfigFile>(json);
            assert!(
                result.is_err() || result.expect("checked above").validate().is_err(),
                "accepted {json}"
            );
        }
    }

    #[test]
    fn redis_knowledge_variables_participate_in_cross_source_ownership() {
        let config = input_config(
            r#"{
                sources: {
                    knowledge: {kind:"redis-knowledge",keys:{x:"knowledge:x"}},
                    events: {kind:"redis",routes:{x:"event:x"}}
                }
            }"#,
        );
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("appears in both source"));
    }

    #[test]
    fn output_config_round_trips_compact_routes_and_stages() {
        let config = OutputConfigFile::from_json(
            r#"{
                default: "telemetry",
                shared_stages: [{kind:"buffer",max_batches:4}],
                destinations: {
                    telemetry: {
                        kind: "mqtt",
                        host: "broker",
                        routes: {pressure: "/pressure", pose: ["/pose", "json"]},
                        stages: [{kind:"coalesce",tick_limit:3,max_delay_ms:25}]
                    }
                }
            }"#,
        )
        .unwrap();
        assert_eq!(
            config.destinations[&DestinationId::from("telemetry")]
                .routes
                .len(),
            2
        );
        assert_eq!(
            serde_json::to_value(&config).unwrap()["destinations"]["telemetry"]["routes"]["pose"],
            serde_json::json!(["/pose", "json"])
        );
    }

    #[test]
    fn output_config_rejects_zero_stage_bounds_and_bad_defaults() {
        let cases = [
            (
                r#"{destinations:{out:{kind:"stdout",stages:[{kind:"buffer",max_batches:0}]}}}"#,
                "max_batches",
            ),
            (
                r#"{default:"missing",destinations:{out:{kind:"stdout"}}}"#,
                "default destination",
            ),
            (
                r#"{destinations:{out:{kind:"stdout",routes:{x:["/x",""]}}}}"#,
                "codec",
            ),
            (
                r#"{destinations:{out:{kind:"mqtt",routes:{x:["/x","ros"]}}}}"#,
                "not supported",
            ),
        ];
        for (payload, expected) in cases {
            let error = OutputConfigFile::from_json(payload).unwrap_err();
            assert!(error.to_string().contains(expected), "{payload}: {error}");
        }
    }

    #[test]
    fn output_config_requires_explicit_roles_for_non_default_destinations() {
        for payload in [
            r#"{default:"primary",destinations:{primary:{kind:"null"},secondary:{kind:"stdout"}}}"#,
            r#"{destinations:{primary:{kind:"null"},secondary:{kind:"stdout"}}}"#,
            r#"{destinations:{first:{kind:"stdout"},second:{kind:"null"}}}"#,
        ] {
            let error = OutputConfigFile::from_json(payload).unwrap_err();
            assert!(
                error.to_string().contains("must explicitly declare a role"),
                "{payload}: {error}"
            );
        }

        let config = OutputConfigFile::from_json(
            r#"{
                default: "primary",
                destinations: {
                    primary: {kind: "null"},
                    partitioned: {kind: "stdout", partition: ["x"]},
                    legacy: {kind: "stdout", variables: ["y"]},
                    mirrored: {kind: "stdout", mirror: true},
                    routed: {kind: "stdout", routes: {z: "/z"}}
                }
            }"#,
        )
        .unwrap();
        assert_eq!(config.destinations.len(), 5);

        let inferred_default = OutputConfigFile::from_json(
            r#"{
                destinations: {
                    primary: {kind: "null"},
                    secondary: {kind: "stdout", partition: ["y"]}
                }
            }"#,
        )
        .unwrap();
        assert!(inferred_default.default.is_none());
    }

    #[test]
    fn output_config_rejects_empty_variables_and_presence_conflicts() {
        let error =
            OutputConfigFile::from_json(r#"{destinations:{out:{kind:"stdout",variables:[]}}}"#)
                .unwrap_err();
        assert!(error.to_string().contains("variables") && error.to_string().contains("empty"));

        for payload in [
            r#"{destinations:{out:{kind:"stdout",variables:[],partition:["x"]}}}"#,
            r#"{destinations:{out:{kind:"stdout",variables:[],mirror:true}}}"#,
            r#"{destinations:{out:{kind:"stdout",variables:[],partition:["x"],mirror:true}}}"#,
        ] {
            let error = OutputConfigFile::from_json(payload).unwrap_err();
            assert!(
                error.to_string().contains("variables")
                    && error.to_string().contains("partition")
                    && error.to_string().contains("mirror"),
                "{payload}: {error}"
            );
        }
    }

    #[test]
    fn output_config_rejects_conflicting_selectors() {
        for payload in [
            r#"{destinations:{out:{kind:"stdout",variables:["x"],partition:["x"]}}}"#,
            r#"{destinations:{out:{kind:"stdout",variables:["x"],mirror:true}}}"#,
            r#"{destinations:{out:{kind:"stdout",partition:["x"],mirror:true}}}"#,
        ] {
            let error = OutputConfigFile::from_json(payload).unwrap_err();
            assert!(
                error.to_string().contains("variables")
                    && error.to_string().contains("partition")
                    && error.to_string().contains("mirror"),
                "{payload}: {error}"
            );
        }
    }

    #[test]
    fn output_config_rejects_manual_and_irrelevant_backend_fields() {
        let cases = [
            (r#"{destinations:{out:{kind:"manual"}}}"#, "manual"),
            (
                r#"{destinations:{out:{kind:"stdout",host:"broker"}}}"#,
                "host",
            ),
            (r#"{destinations:{out:{kind:"null",port:1234}}}"#, "port"),
            (r#"{destinations:{out:{kind:"mqtt",limit:1}}}"#, "limit"),
            (
                r#"{destinations:{out:{kind:"limited-null",host:"broker",limit:1}}}"#,
                "host",
            ),
            (
                r#"{destinations:{out:{kind:"limited-null",port:1234,limit:1}}}"#,
                "port",
            ),
            (
                r#"{destinations:{out:{kind:"limited-null"}}}"#,
                "greater than zero",
            ),
            (
                r#"{destinations:{out:{kind:"limited-null",limit:0}}}"#,
                "greater than zero",
            ),
        ];
        for (payload, expected) in cases {
            let error = OutputConfigFile::from_json(payload).unwrap_err();
            assert!(error.to_string().contains(expected), "{payload}: {error}");
        }
    }

    #[test]
    fn output_config_preserves_mqtt_and_redis_defaults() {
        let config = OutputConfigFile::from_json(
            r#"{
                destinations: {
                    mqtt: {kind: "mqtt", routes: {x: "/x"}},
                    redis: {kind: "redis", routes: {y: "/y"}}
                }
            }"#,
        )
        .unwrap();
        assert!(config.destinations["mqtt"].host.is_none());
        assert!(config.destinations["mqtt"].port.is_none());
        assert!(config.destinations["redis"].host.is_none());
        assert!(config.destinations["redis"].port.is_none());
    }
}
