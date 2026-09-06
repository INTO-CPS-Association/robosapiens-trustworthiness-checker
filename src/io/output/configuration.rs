use std::rc::Rc;

use crate::io::config::{
    DestinationConfig, DestinationId, DestinationKind, OutputConfigFile, OutputDeliveryConfig,
};

use super::{
    CoalescingLimits, DeliveryPolicy, OutputBackendConfig, OutputDestination, OutputDestinations,
    OutputPipeline, QueueLimits,
};

impl<V> OutputPipeline<V> {
    /// Construct a one-destination pipeline using the stable default ID.
    pub fn from_backend(backend: OutputBackendConfig<V>) -> Self {
        Self::from_destination(OutputDestination::new("default", backend))
            .expect("the stable default output destination ID is valid")
    }

    /// Parse durable local output configuration into a resource-free pipeline.
    /// ROS destinations require an executor and can instead be constructed with
    /// [`Self::from_config_with_executor`] when the `ros` feature is enabled.
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
        Ok(Self::new(destinations))
    }
}

fn delivery_from_config(config: &OutputDeliveryConfig) -> anyhow::Result<DeliveryPolicy> {
    config.validate()?;
    let queue = config.queue.as_ref().map(|queue| {
        QueueLimits::new(
            std::num::NonZeroUsize::new(queue.max_batches).expect("validated queue bound"),
            queue.max_updates.and_then(std::num::NonZeroUsize::new),
        )
    });
    let coalesce = config
        .coalesce
        .as_ref()
        .map(|coalesce| {
            CoalescingLimits::new(
                coalesce.tick_limit.and_then(std::num::NonZeroUsize::new),
                coalesce.update_limit.and_then(std::num::NonZeroUsize::new),
                coalesce.max_delay_ms.map(std::time::Duration::from_millis),
            )
        })
        .transpose()?;
    let mut policy = coalesce.map_or(DeliveryPolicy::direct(), DeliveryPolicy::coalesce);
    if let Some(queue) = queue {
        policy = policy.with_queue(queue);
    }
    Ok(policy)
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
        DestinationKind::Mqtt => OutputBackendConfig::mqtt_with_protocol_and_retry(
            config
                .host
                .unwrap_or_else(|| crate::core::MQTT_HOSTNAME.to_owned()),
            config.port,
            config.protocol.unwrap_or_default(),
            config
                .retry
                .unwrap_or_else(crate::io::RetryPolicy::output_default),
        ),
        DestinationKind::Redis => OutputBackendConfig::redis_with_retry(
            config
                .host
                .unwrap_or_else(|| crate::core::REDIS_HOSTNAME.to_owned()),
            config.port,
            config
                .retry
                .unwrap_or_else(crate::io::RetryPolicy::output_default),
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

    let partition = config.partition;
    match (partition, config.mirror) {
        (Some(partition), false) => {
            destination = destination.partition(partition);
        }
        (None, true) => {
            destination.set_mirror_all();
        }
        (None, false) => {}
        _ => anyhow::bail!(
            "output destination selector fields `partition` and `mirror` are mutually exclusive"
        ),
    }
    if let Some(delivery) = config.delivery.as_ref() {
        destination = destination.with_delivery(delivery_from_config(delivery)?);
    }
    Ok(destination)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Value, VarName};

    #[test]
    fn redis_config_threads_explicit_retry_policy() {
        let config = OutputConfigFile::from_json(
            r#"{
                destinations: {
                    redis: {
                        kind: "redis",
                        mirror: true,
                        retry: {max_attempts: 3, initial_delay_ms: 7, max_delay_ms: 21}
                    }
                }
            }"#,
        )
        .unwrap();
        let pipeline = OutputPipeline::<Value>::from_config(config).unwrap();
        let backend = pipeline.destinations().destinations()["redis"].backend();
        let OutputBackendConfig::Redis { retry, .. } = backend else {
            panic!("configured backend should be Redis")
        };
        assert_eq!(
            *retry,
            crate::io::RetryPolicy::new(
                crate::io::RetryLimit::Attempts(std::num::NonZeroU32::new(3).unwrap()),
                std::time::Duration::from_millis(7),
                std::time::Duration::from_millis(21),
            )
            .unwrap()
        );
    }

    #[test]
    fn config_pipeline_preserves_explicit_route_roles_without_implicit_mirroring() {
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
        let pipeline = OutputPipeline::<Value>::from_config(config).unwrap();
        let resolved = pipeline
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
    fn config_pipeline_installs_inferred_default_for_unassigned_outputs() {
        let config = OutputConfigFile::from_json(
            r#"{
                destinations: {
                    primary: {kind: "null"},
                    secondary: {kind: "stdout", partition: ["y"]}
                }
            }"#,
        )
        .unwrap();
        let pipeline = OutputPipeline::<Value>::from_config(config).unwrap();
        assert_eq!(
            pipeline.destinations().default(),
            Some(&"primary".to_owned())
        );

        let resolved = pipeline
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
    fn config_rejects_an_unselected_non_default_destination() {
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
        let pipeline = OutputPipeline::<Value>::from_backend(OutputBackendConfig::limited_null(0));
        let error = pipeline
            .resolve([VarName::new("x")], std::iter::empty::<VarName>(), None)
            .unwrap_err();
        assert!(error.to_string().contains("greater than zero"));
    }
}
