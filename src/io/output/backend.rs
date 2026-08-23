use std::{fmt, rc::Rc};

use crate::core::{
    JsonStreamValue, OutputBackend, OutputError, OutputInterface, OutputWriter, RosStreamValue,
    SharedOutputBackend,
};

use super::{
    LimitedNullOutputBackend, ManualOutputBackend, ManualOutputSender, NullOutputBackend,
    StdoutOutputBackend,
};

#[cfg(feature = "mqtt")]
use super::MqttOutputBackend;
#[cfg(feature = "redis")]
use super::RedisOutputBackend;

/// The MQTT client implementation used by output destinations.
///
/// Output is intentionally Paho-only; rumqttc remains an input-only option.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MqttOutputBackendKind {
    Paho,
}

/// A backend family used for deterministic route validation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OutputBackendKind {
    Stdout,
    Null,
    LimitedNull,
    Manual,
    Mqtt,
    Redis,
    Ros,
    Custom,
}

/// Reusable, resource-free configuration for one output backend.
///
/// Constructing this value never connects to a broker, creates a ROS node, or
/// starts a worker. Those operations happen in [`Self::open`], once the
/// resolved fixed interface is known. `Custom` is useful for embedding and
/// deterministic tests; it stores a reusable [`OutputBackend`] factory, not an
/// opened writer.
#[derive(Clone)]
pub enum OutputBackendConfig<V = crate::Value> {
    Stdout,
    Null,
    LimitedNull(usize),
    Manual(ManualOutputSender<V>),
    Mqtt {
        host: String,
        port: Option<u16>,
        backend: MqttOutputBackendKind,
    },
    Redis {
        host: String,
        port: Option<u16>,
    },
    #[cfg(feature = "ros")]
    Ros {
        executor: Rc<smol::LocalExecutor<'static>>,
        node_name: String,
    },
    Custom(SharedOutputBackend<V>),
}

impl<V> fmt::Debug for OutputBackendConfig<V> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stdout => formatter.write_str("OutputBackendConfig::Stdout"),
            Self::Null => formatter.write_str("OutputBackendConfig::Null"),
            Self::LimitedNull(limit) => formatter
                .debug_tuple("OutputBackendConfig::LimitedNull")
                .field(limit)
                .finish(),
            Self::Manual(_) => formatter.write_str("OutputBackendConfig::Manual(..)"),
            Self::Mqtt {
                host,
                port,
                backend,
            } => formatter
                .debug_struct("OutputBackendConfig::Mqtt")
                .field("host", host)
                .field("port", port)
                .field("backend", backend)
                .finish(),
            Self::Redis { host, port } => formatter
                .debug_struct("OutputBackendConfig::Redis")
                .field("host", host)
                .field("port", port)
                .finish(),
            #[cfg(feature = "ros")]
            Self::Ros { node_name, .. } => formatter
                .debug_struct("OutputBackendConfig::Ros")
                .field("node_name", node_name)
                .finish_non_exhaustive(),
            Self::Custom(_) => formatter.write_str("OutputBackendConfig::Custom(..)"),
        }
    }
}

impl<V> OutputBackendConfig<V> {
    pub fn stdout() -> Self {
        Self::Stdout
    }

    pub fn null() -> Self {
        Self::Null
    }

    pub fn limited_null(limit: usize) -> Self {
        Self::LimitedNull(limit)
    }

    pub fn manual(sender: ManualOutputSender<V>) -> Self {
        Self::Manual(sender)
    }

    pub fn mqtt(host: impl Into<String>, port: Option<u16>) -> Self {
        Self::Mqtt {
            host: host.into(),
            port,
            backend: MqttOutputBackendKind::Paho,
        }
    }

    pub fn redis(host: impl Into<String>, port: Option<u16>) -> Self {
        Self::Redis {
            host: host.into(),
            port,
        }
    }

    #[cfg(feature = "ros")]
    pub fn ros(executor: Rc<smol::LocalExecutor<'static>>, node_name: impl Into<String>) -> Self {
        Self::Ros {
            executor,
            node_name: node_name.into(),
        }
    }

    pub fn custom<B>(backend: B) -> Self
    where
        B: OutputBackend<Val = V> + 'static,
    {
        Self::Custom(Rc::new(backend))
    }

    pub fn shared(backend: SharedOutputBackend<V>) -> Self {
        Self::Custom(backend)
    }

    pub fn kind(&self) -> OutputBackendKind {
        match self {
            Self::Stdout => OutputBackendKind::Stdout,
            Self::Null => OutputBackendKind::Null,
            Self::LimitedNull(_) => OutputBackendKind::LimitedNull,
            Self::Manual(_) => OutputBackendKind::Manual,
            Self::Mqtt { .. } => OutputBackendKind::Mqtt,
            Self::Redis { .. } => OutputBackendKind::Redis,
            #[cfg(feature = "ros")]
            Self::Ros { .. } => OutputBackendKind::Ros,
            Self::Custom(_) => OutputBackendKind::Custom,
        }
    }

    pub fn requires_codec(&self) -> bool {
        #[cfg(feature = "ros")]
        {
            return matches!(self, Self::Ros { .. });
        }
        #[cfg(not(feature = "ros"))]
        {
            false
        }
    }

    pub fn supports_codec(&self, codec: &str) -> bool {
        match self {
            Self::Mqtt { .. } | Self::Redis { .. } => {
                matches!(codec, "json" | "json5")
            }
            #[cfg(feature = "ros")]
            Self::Ros { .. } => !codec.trim().is_empty(),
            Self::Stdout
            | Self::Null
            | Self::LimitedNull(_)
            | Self::Manual(_)
            | Self::Custom(_) => false,
        }
    }

    pub(crate) fn validate_local(&self) -> anyhow::Result<()> {
        match self {
            Self::Mqtt { host, .. } | Self::Redis { host, .. } => {
                anyhow::ensure!(
                    !host.trim().is_empty(),
                    "output backend host cannot be empty"
                );
            }
            Self::LimitedNull(limit) => {
                anyhow::ensure!(
                    *limit > 0,
                    "limited-null output backend `limit` must be greater than zero"
                );
            }
            _ => {}
        }
        Ok(())
    }
}

impl<V> OutputBackendConfig<V>
where
    V: JsonStreamValue + RosStreamValue,
{
    /// Opens a new writer for a resolved interface.
    pub async fn open(&self, interface: OutputInterface) -> Result<OutputWriter<V>, OutputError> {
        self.validate_local().map_err(OutputError::invalid)?;
        match self {
            Self::Stdout => StdoutOutputBackend::<V>::new().open(interface).await,
            Self::Null => NullOutputBackend::<V>::new().open(interface).await,
            Self::LimitedNull(limit) => {
                LimitedNullOutputBackend::<V>::new(*limit)
                    .open(interface)
                    .await
            }
            Self::Manual(sender) => {
                ManualOutputBackend::<V>::new(sender.clone())
                    .open(interface)
                    .await
            }
            Self::Mqtt { host, port, .. } => {
                #[cfg(feature = "mqtt")]
                {
                    MqttOutputBackend::<V>::new(host.clone(), *port)
                        .open(interface)
                        .await
                }
                #[cfg(not(feature = "mqtt"))]
                {
                    let _ = (host, port, interface);
                    Err(OutputError::backend("MQTT support not enabled"))
                }
            }
            Self::Redis { host, port } => {
                #[cfg(feature = "redis")]
                {
                    RedisOutputBackend::<V>::new(host.clone(), *port)
                        .open(interface)
                        .await
                }
                #[cfg(not(feature = "redis"))]
                {
                    let _ = (host, port, interface);
                    Err(OutputError::backend("Redis support not enabled"))
                }
            }
            #[cfg(feature = "ros")]
            Self::Ros {
                executor,
                node_name,
            } => {
                let backend = V::ros_output_backend(Rc::clone(executor), node_name.clone())
                    .map_err(|error| OutputError::backend(error.to_string()))?;
                backend.open(interface).await
            }
            Self::Custom(backend) => backend.open(interface).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limited_null_zero_is_rejected_before_opening() {
        let error = OutputBackendConfig::<crate::Value>::limited_null(0)
            .validate_local()
            .unwrap_err();
        assert!(error.to_string().contains("greater than zero"));
    }

    #[test]
    fn mqtt_constructor_selects_the_paho_output_backend() {
        let backend = OutputBackendConfig::<crate::Value>::mqtt("broker", None);
        assert!(format!("{backend:?}").contains("Paho"));
    }

    #[cfg(not(feature = "redis"))]
    #[test]
    fn redis_backend_reports_that_support_is_disabled_when_opened() {
        let backend = OutputBackendConfig::<crate::Value>::redis("localhost", None);
        let interface = OutputInterface::new(Vec::new()).unwrap();
        let error = match smol::block_on(backend.open(interface)) {
            Ok(_) => panic!("Redis backend unexpectedly opened without Redis support"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("Redis support not enabled"));
    }
}
