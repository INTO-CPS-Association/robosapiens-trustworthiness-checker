use std::{borrow::Borrow, rc::Rc};

use crate::core::{JsonStreamValue, OutputError, OutputWriter, RosStreamValue};
use crate::io::config::{
    DestinationConfig, DestinationId, DestinationKind, OutputConfigFile, OutputConfiguration,
    OutputStageConfig,
};
use crate::io::output::{
    OutputBackendConfig, OutputDestination, OutputDestinations, OutputPipeline,
    OutputPipelineSession, OutputStage, ResolvedOutput,
};
use crate::{Value, VarName};

/// Builder facade for the output destination/pipeline architecture.
///
/// It owns only backend configuration and stage policy. `resolve` is pure;
/// `build` resolves first and then opens all destinations with cleanup on
/// partial failure.
#[derive(Clone, Debug)]
pub struct OutputBackendBuilder<V = Value> {
    pipeline: OutputPipeline<V>,
}

impl<V> OutputBackendBuilder<V> {
    /// Construct a one-destination pipeline using the stable default ID.
    pub fn new(backend: OutputBackendConfig<V>) -> Self {
        Self {
            pipeline: OutputPipeline::from_destination(OutputDestination::new(
                DestinationId::from("default"),
                backend,
            ))
            .expect("the stable default output destination ID is valid"),
        }
    }

    pub fn from_destination(destination: OutputDestination<V>) -> Self {
        Self {
            pipeline: OutputPipeline::from_destination(destination)
                .expect("output destination IDs must be non-empty and unique"),
        }
    }

    pub fn from_destinations(destinations: OutputDestinations<V>) -> Self {
        Self {
            pipeline: OutputPipeline::new(destinations),
        }
    }

    pub fn from_pipeline(pipeline: OutputPipeline<V>) -> Self {
        Self { pipeline }
    }

    pub fn pipeline(&self) -> &OutputPipeline<V> {
        &self.pipeline
    }

    pub fn into_pipeline(self) -> OutputPipeline<V> {
        self.pipeline
    }

    /// Supply the runtime-local executor for worker-backed stages. This does
    /// not open any resource or alter durable destination configuration.
    pub fn executor(mut self, executor: Rc<smol::LocalExecutor<'static>>) -> Self {
        self.pipeline = self.pipeline.with_executor(executor);
        self
    }

    pub fn with_shared_stage(mut self, stage: OutputStage) -> Self {
        self.pipeline = self.pipeline.with_shared_stage(stage);
        self
    }

    pub fn with_shared_stages<I>(mut self, stages: I) -> Self
    where
        I: IntoIterator<Item = OutputStage>,
    {
        self.pipeline = self.pipeline.with_shared_stages(stages);
        self
    }

    pub fn with_destination_stage(
        mut self,
        destination: impl Into<DestinationId>,
        stage: OutputStage,
    ) -> anyhow::Result<Self> {
        self.pipeline = self.pipeline.with_destination_stage(destination, stage)?;
        Ok(self)
    }

    pub fn replace_destination_stages<I>(
        mut self,
        destination: impl Into<DestinationId>,
        stages: I,
    ) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = OutputStage>,
    {
        self.pipeline = self
            .pipeline
            .replace_destination_stages(destination, stages)?;
        Ok(self)
    }

    pub fn resolve<I, A>(
        &self,
        model_outputs: I,
        auxiliary: A,
        output_configuration: Option<&OutputConfiguration>,
    ) -> anyhow::Result<ResolvedOutput>
    where
        I: IntoIterator,
        I::Item: Borrow<VarName>,
        A: IntoIterator,
        A::Item: Borrow<VarName>,
    {
        self.pipeline
            .resolve(model_outputs, auxiliary, output_configuration)
    }

    pub async fn build<I, A>(
        &self,
        model_outputs: I,
        auxiliary: A,
        output_configuration: Option<&OutputConfiguration>,
    ) -> Result<OutputWriter<V>, OutputError>
    where
        I: IntoIterator,
        I::Item: Borrow<VarName>,
        A: IntoIterator,
        A::Item: Borrow<VarName>,
        V: JsonStreamValue + RosStreamValue,
    {
        self.pipeline
            .build(model_outputs, auxiliary, output_configuration)
            .await
    }

    /// Fallible replacement-construction alias for callers that do not need
    /// to retain the resolved output separately.
    pub async fn try_build<I, A>(
        &self,
        model_outputs: I,
        auxiliary: A,
        output_configuration: Option<&OutputConfiguration>,
    ) -> Result<OutputWriter<V>, OutputError>
    where
        I: IntoIterator,
        I::Item: Borrow<VarName>,
        A: IntoIterator,
        A::Item: Borrow<VarName>,
        V: JsonStreamValue + RosStreamValue,
    {
        self.build(model_outputs, auxiliary, output_configuration)
            .await
    }

    pub async fn open(&self, resolved: ResolvedOutput) -> Result<OutputWriter<V>, OutputError>
    where
        V: JsonStreamValue + RosStreamValue,
    {
        self.pipeline.open(resolved).await
    }

    pub async fn open_session(
        &self,
        resolved: ResolvedOutput,
    ) -> Result<OutputPipelineSession<V>, OutputError>
    where
        V: JsonStreamValue + RosStreamValue,
    {
        self.pipeline.open_session(resolved).await
    }

    /// Parse durable local output configuration into a resource-free builder.
    /// ROS destinations require an executor and can instead be constructed with
    /// `from_config_with_executor` when the `ros` feature is enabled.
    pub fn from_config(config: OutputConfigFile) -> anyhow::Result<Self> {
        Self::from_config_with_optional_executor(config, None)
    }

    #[cfg(feature = "ros")]
    pub fn from_config_with_executor(
        config: OutputConfigFile,
        executor: Rc<smol::LocalExecutor<'static>>,
    ) -> anyhow::Result<Self> {
        Self::from_config_with_optional_executor(config, Some(executor))
    }

    fn from_config_with_optional_executor(
        config: OutputConfigFile,
        executor: Option<Rc<smol::LocalExecutor<'static>>>,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let default = config.effective_default();
        let mut destinations = Vec::with_capacity(config.destinations.len());
        for (id, destination_config) in config.destinations {
            destinations.push(destination_from_config(
                id,
                destination_config,
                executor.clone(),
            )?);
        }

        let mut destinations = OutputDestinations::try_new(destinations)?;
        if let Some(default) = default {
            destinations = destinations.with_default(default)?;
        }
        let stages = config
            .shared_stages
            .iter()
            .map(stage_from_config)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut builder = Self::from_destinations(destinations);
        builder.pipeline = builder.pipeline.with_shared_stages(stages);
        Ok(builder)
    }
}

fn stage_from_config(config: &OutputStageConfig) -> anyhow::Result<OutputStage> {
    match config {
        OutputStageConfig::Buffer {
            max_batches,
            max_updates,
        } => {
            OutputStage::buffer_with_limits(*max_batches, *max_updates).map_err(anyhow::Error::from)
        }
        OutputStageConfig::Coalesce {
            max_delay_ms,
            tick_limit,
            update_limit,
        } => OutputStage::coalesce_with_limits(
            max_delay_ms.map(std::time::Duration::from_millis),
            *tick_limit,
            *update_limit,
        )
        .map_err(anyhow::Error::from),
    }
}

fn destination_from_config<V>(
    id: DestinationId,
    config: DestinationConfig,
    executor: Option<Rc<smol::LocalExecutor<'static>>>,
) -> anyhow::Result<OutputDestination<V>> {
    #[cfg(not(feature = "ros"))]
    let _ = &executor;

    let backend = match config.kind {
        DestinationKind::Stdout => OutputBackendConfig::stdout(),
        DestinationKind::Null => OutputBackendConfig::null(),
        DestinationKind::LimitedNull => {
            OutputBackendConfig::limited_null(config.limit.ok_or_else(|| {
                anyhow::anyhow!(
                    "limited-null output destination requires a `limit` greater than zero"
                )
            })?)
        }
        DestinationKind::Mqtt => OutputBackendConfig::mqtt(
            config
                .host
                .unwrap_or_else(|| crate::core::MQTT_HOSTNAME.to_owned()),
            config.port,
        ),
        DestinationKind::Redis => OutputBackendConfig::redis(
            config
                .host
                .unwrap_or_else(|| crate::core::REDIS_HOSTNAME.to_owned()),
            config.port,
        ),
        DestinationKind::Ros => {
            #[cfg(feature = "ros")]
            {
                let executor = executor.ok_or_else(|| {
                    anyhow::anyhow!("ROS output destinations require a local executor")
                })?;
                OutputBackendConfig::Ros {
                    executor,
                    node_name: "tc_ros_output".to_owned(),
                }
            }
            #[cfg(not(feature = "ros"))]
            {
                anyhow::bail!("ROS support not enabled")
            }
        }
    };

    let mut destination = OutputDestination::new(id, backend);
    if !config.routes.is_empty() {
        destination = destination.with_route_catalog(config.routes);
    }

    let variables = config.variables;
    let partition = config.partition;
    match (variables, partition, config.mirror) {
        (Some(variables), None, false) => {
            destination = destination.partition(variables);
        }
        (None, Some(partition), false) => {
            destination = destination.partition(partition);
        }
        (None, None, true) => {
            destination.set_mirror_all();
        }
        (None, None, false) => {}
        _ => anyhow::bail!(
            "output destination selector fields `variables`, `partition`, and `mirror` are mutually exclusive"
        ),
    }
    let stages = config
        .stages
        .iter()
        .map(stage_from_config)
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(destination.with_stages(stages))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_builder_preserves_explicit_route_roles_without_implicit_mirroring() {
        let config = OutputConfigFile::from_json(
            r#"{
                default: "primary",
                destinations: {
                    primary: {kind: "null"},
                    secondary: {kind: "stdout", routes: {y: "/y"}}
                }
            }"#,
        )
        .unwrap();
        let builder = OutputBackendBuilder::<Value>::from_config(config).unwrap();
        let resolved = builder
            .resolve(
                [VarName::new("x"), VarName::new("y")],
                std::iter::empty::<VarName>(),
                None,
            )
            .unwrap();

        let primary = resolved.destination(&"primary".to_owned()).unwrap();
        let secondary = resolved.destination(&"secondary".to_owned()).unwrap();
        assert_eq!(primary.bindings().len(), 1);
        assert_eq!(primary.bindings()[0].variable(), &VarName::new("x"));
        assert_eq!(secondary.bindings().len(), 1);
        assert_eq!(secondary.bindings()[0].variable(), &VarName::new("y"));
    }

    #[test]
    fn config_builder_installs_inferred_default_for_unassigned_outputs() {
        let config = OutputConfigFile::from_json(
            r#"{
                destinations: {
                    primary: {kind: "null"},
                    secondary: {kind: "stdout", partition: ["y"]}
                }
            }"#,
        )
        .unwrap();
        let builder = OutputBackendBuilder::<Value>::from_config(config).unwrap();
        assert_eq!(
            builder.pipeline().destinations().default(),
            Some(&"primary".to_owned())
        );

        let resolved = builder
            .resolve(
                [VarName::new("x"), VarName::new("y")],
                std::iter::empty::<VarName>(),
                None,
            )
            .unwrap();
        let primary = resolved.destination(&"primary".to_owned()).unwrap();
        let secondary = resolved.destination(&"secondary".to_owned()).unwrap();
        assert_eq!(primary.bindings().len(), 1);
        assert_eq!(primary.bindings()[0].variable(), &VarName::new("x"));
        assert_eq!(secondary.bindings().len(), 1);
        assert_eq!(secondary.bindings()[0].variable(), &VarName::new("y"));
    }

    #[test]
    fn config_builder_rejects_an_unselected_non_default_destination() {
        let config = OutputConfigFile::from_json(
            r#"{
                default: "primary",
                destinations: {
                    primary: {kind: "null"},
                    secondary: {kind: "stdout"}
                }
            }"#,
        );
        let error = config.unwrap_err();
        assert!(error.to_string().contains("must explicitly declare a role"));
    }

    #[test]
    fn programmatic_zero_limited_null_fails_during_resolution() {
        let builder = OutputBackendBuilder::<crate::Value>::new(
            OutputBackendConfig::<crate::Value>::limited_null(0),
        );
        let error = builder
            .resolve([VarName::new("x")], std::iter::empty::<VarName>(), None)
            .unwrap_err();
        assert!(error.to_string().contains("greater than zero"));
    }
}
