use std::{collections::BTreeMap, num::NonZeroUsize, time::Duration};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::VarName;

pub type TopicMapping = BTreeMap<VarName, String>;
pub type MsgTypeMapping = BTreeMap<VarName, String>;

/// Stable identifier for a configured local input source.
pub type SourceId = String;

/// Codec selected by a route. MQTT and Redis can omit it because their value
/// codec is the source's normal JSON codec; ROS routes generally specify it.
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MonitorConfig {
    pub spec: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inputs: Option<BTreeMap<VarName, Route>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sources: Option<BTreeMap<SourceId, BTreeMap<VarName, Route>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outputs: Option<BTreeMap<VarName, Route>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireMonitorConfig {
    spec: String,
    source: Option<SourceId>,
    inputs: Option<BTreeMap<VarName, WireRoute>>,
    sources: Option<BTreeMap<SourceId, BTreeMap<VarName, WireRoute>>>,
    outputs: Option<BTreeMap<VarName, WireRoute>>,
}

impl<'de> Deserialize<'de> for MonitorConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = WireMonitorConfig::deserialize(deserializer)?;
        let convert = |routes: BTreeMap<VarName, WireRoute>| {
            routes
                .into_iter()
                .map(|(variable, route)| {
                    route
                        .into_route()
                        .map(|route| (variable, route))
                        .map_err(serde::de::Error::custom)
                })
                .collect::<Result<BTreeMap<_, _>, _>>()
        };
        let inputs = wire.inputs.map(convert).transpose()?;
        let sources = wire
            .sources
            .map(|groups| {
                groups
                    .into_iter()
                    .map(|(source, routes)| convert(routes).map(|routes| (source, routes)))
                    .collect::<Result<BTreeMap<_, _>, _>>()
            })
            .transpose()?;
        let outputs = wire.outputs.map(convert).transpose()?;
        let config = Self {
            spec: wire.spec,
            source: wire.source,
            inputs,
            sources,
            outputs,
        };
        config
            .validate_structure()
            .map_err(serde::de::Error::custom)?;
        Ok(config)
    }
}

impl MonitorConfig {
    pub fn from_json(payload: &str) -> anyhow::Result<Self> {
        let config: Self = serde_json5::from_str(payload)
            .map_err(|error| anyhow::anyhow!("invalid monitor configuration: {error}"))?;
        config.validate_structure()?;
        Ok(config)
    }

    pub fn validate_structure(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.spec.trim().is_empty(), "monitor spec cannot be empty");
        anyhow::ensure!(
            !(self.inputs.is_some() && self.sources.is_some()),
            "monitor configuration cannot contain both `inputs` and `sources`"
        );
        anyhow::ensure!(
            self.source.is_none() || self.inputs.is_some(),
            "monitor configuration `source` requires `inputs`"
        );
        anyhow::ensure!(
            self.source.is_none() || self.sources.is_none(),
            "monitor configuration cannot contain `source` together with `sources`"
        );
        if let Some(source) = &self.source {
            anyhow::ensure!(
                !source.trim().is_empty(),
                "monitor source ID cannot be empty"
            );
        }
        if let Some(sources) = &self.sources {
            let mut variables = BTreeMap::<VarName, &SourceId>::new();
            for (source, bindings) in sources {
                anyhow::ensure!(
                    !source.trim().is_empty(),
                    "monitor source ID cannot be empty"
                );
                for variable in bindings.keys() {
                    if let Some(previous) = variables.insert(variable.clone(), source) {
                        anyhow::bail!(
                            "monitor input variable `{variable}` is bound by both source `{previous}` and source `{source}`"
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
            Self::Mqtt { host, .. } | Self::Redis { host, .. } => host.as_deref(),
            Self::Ros { .. } => None,
        }
    }

    pub fn routes(&self) -> &BTreeMap<VarName, WireRoute> {
        match self {
            Self::Mqtt { routes, .. } | Self::Redis { routes, .. } | Self::Ros { routes, .. } => {
                routes
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

/// An immutable, generation-specific binding resolved from source catalogs and
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
}

impl ResolvedInput {
    pub(crate) fn new(sources: impl IntoIterator<Item = ResolvedSource>) -> Self {
        Self {
            sources: sources.into_iter().collect(),
        }
    }

    pub(crate) fn sources(&self) -> &[ResolvedSource] {
        &self.sources
    }

    pub(crate) fn into_sources(self) -> Vec<ResolvedSource> {
        self.sources.into_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input_config(json: &str) -> InputConfigFile {
        serde_json5::from_str(json).expect("input config should deserialize")
    }

    #[test]
    fn monitor_config_compact_routes_round_trip() {
        let cases = [
            r#"{
                "spec": "in pressure",
                "source": "telemetry",
                "inputs": {
                    "pressure": "/pressure",
                    "pose": ["/pose", "geometry_msgs/msg/Pose"]
                },
                "outputs": {"alarm": ["/alarm", "json"]}
            }"#,
            r#"{
                "spec": "in pressure",
                "sources": {
                    "telemetry": {"pressure": "/pressure"},
                    "robot": {"pose": ["/pose", "geometry_msgs/msg/Pose"]}
                }
            }"#,
        ];

        for json in cases {
            let config = MonitorConfig::from_json(json).unwrap();
            let serialized = serde_json::to_value(&config).unwrap();
            assert_eq!(
                serialized,
                serde_json::from_str::<serde_json::Value>(json).unwrap()
            );
            assert_eq!(
                serde_json::from_value::<MonitorConfig>(serialized).unwrap(),
                config
            );
        }
    }

    #[test]
    fn monitor_config_omits_absent_optional_fields() {
        let config = MonitorConfig::from_json(r#"{"spec":"in pressure"}"#).unwrap();
        assert_eq!(
            serde_json::to_value(config).unwrap(),
            serde_json::json!({"spec": "in pressure"})
        );
    }

    #[test]
    fn monitor_config_rejects_unknown_and_obsolete_fields() {
        let cases = [
            r#"{"spec":"in pressure","revision":1}"#,
            r#"{"spec":"in pressure","bogus":true}"#,
        ];

        for json in cases {
            assert!(MonitorConfig::from_json(json).is_err(), "accepted {json}");
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
                serde_json5::from_str::<InputConfigFile>(json).is_err(),
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
        let result = serde_json5::from_str::<InputConfigFile>(
            r#"{
                sources: { telemetry: { kind: "mqtt" } },
                control: { source: "telemetry", route: "reconf" }
            }"#,
        );
        assert!(result.is_err());
    }
}
