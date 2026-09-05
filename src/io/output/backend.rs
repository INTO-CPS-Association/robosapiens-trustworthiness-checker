use super::{open_limited_null, open_null, open_stdout};
use crate::{
    core::{JsonStreamValue, OutputError, OutputInterface, OutputWriter, RosStreamValue},
    io::RetryPolicy,
    io::channel::{ChannelOutputSender, open_output},
};
#[cfg(any(test, feature = "test-support"))]
use async_trait::async_trait;
use std::fmt;
#[cfg(any(test, feature = "test-support"))]
use std::rc::Rc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OutputBackendKind {
    Stdout,
    Null,
    LimitedNull,
    Channel,
    Mqtt,
    Redis,
    Ros,
    #[cfg(any(test, feature = "test-support"))]
    Test,
}

#[cfg(any(test, feature = "test-support"))]
#[async_trait(?Send)]
pub trait TestOutputOpener<V> {
    async fn open(&self, interface: OutputInterface) -> Result<OutputWriter<V>, OutputError>;
}

/// Resource-free configuration for one built-in destination.
#[derive(Clone)]
pub enum OutputBackendConfig<V = crate::Value> {
    Stdout,
    Null,
    LimitedNull(usize),
    Channel(ChannelOutputSender<V>),
    Mqtt {
        host: String,
        port: Option<u16>,
        retry: RetryPolicy,
    },
    Redis {
        host: String,
        port: Option<u16>,
        retry: RetryPolicy,
    },
    #[cfg(feature = "ros")]
    Ros {
        executor: std::rc::Rc<smol::LocalExecutor<'static>>,
        node_name: String,
    },
    #[cfg(any(test, feature = "test-support"))]
    Test(Rc<dyn TestOutputOpener<V>>),
}

impl<V> fmt::Debug for OutputBackendConfig<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stdout => f.write_str("OutputBackendConfig::Stdout"),
            Self::Null => f.write_str("OutputBackendConfig::Null"),
            Self::LimitedNull(n) => f
                .debug_tuple("OutputBackendConfig::LimitedNull")
                .field(n)
                .finish(),
            Self::Channel(_) => f.write_str("OutputBackendConfig::Channel(..)"),
            Self::Mqtt { host, port, retry } => f
                .debug_struct("OutputBackendConfig::Mqtt")
                .field("host", host)
                .field("port", port)
                .field("retry", retry)
                .finish(),
            Self::Redis { host, port, retry } => f
                .debug_struct("OutputBackendConfig::Redis")
                .field("host", host)
                .field("port", port)
                .field("retry", retry)
                .finish(),
            #[cfg(feature = "ros")]
            Self::Ros { node_name, .. } => f
                .debug_struct("OutputBackendConfig::Ros")
                .field("node_name", node_name)
                .finish_non_exhaustive(),
            #[cfg(any(test, feature = "test-support"))]
            Self::Test(_) => f.write_str("OutputBackendConfig::Test(..)"),
        }
    }
}

impl<V> OutputBackendConfig<V> {
    pub const fn stdout() -> Self {
        Self::Stdout
    }
    pub const fn null() -> Self {
        Self::Null
    }
    pub const fn limited_null(limit: usize) -> Self {
        Self::LimitedNull(limit)
    }
    pub fn channel(sender: ChannelOutputSender<V>) -> Self {
        Self::Channel(sender)
    }
    pub fn mqtt(host: impl Into<String>, port: Option<u16>) -> Self {
        Self::mqtt_with_retry(host, port, RetryPolicy::output_default())
    }
    pub fn mqtt_with_retry(host: impl Into<String>, port: Option<u16>, retry: RetryPolicy) -> Self {
        Self::Mqtt {
            host: host.into(),
            port,
            retry,
        }
    }
    pub fn redis(host: impl Into<String>, port: Option<u16>) -> Self {
        Self::redis_with_retry(host, port, RetryPolicy::output_default())
    }
    pub fn redis_with_retry(
        host: impl Into<String>,
        port: Option<u16>,
        retry: RetryPolicy,
    ) -> Self {
        Self::Redis {
            host: host.into(),
            port,
            retry,
        }
    }
    #[cfg(feature = "ros")]
    pub fn ros(
        executor: std::rc::Rc<smol::LocalExecutor<'static>>,
        node_name: impl Into<String>,
    ) -> Self {
        Self::Ros {
            executor,
            node_name: node_name.into(),
        }
    }
    #[cfg(any(test, feature = "test-support"))]
    pub fn test<O: TestOutputOpener<V> + 'static>(opener: O) -> Self {
        Self::Test(Rc::new(opener))
    }
    pub fn kind(&self) -> OutputBackendKind {
        match self {
            Self::Stdout => OutputBackendKind::Stdout,
            Self::Null => OutputBackendKind::Null,
            Self::LimitedNull(_) => OutputBackendKind::LimitedNull,
            Self::Channel(_) => OutputBackendKind::Channel,
            Self::Mqtt { .. } => OutputBackendKind::Mqtt,
            Self::Redis { .. } => OutputBackendKind::Redis,
            #[cfg(feature = "ros")]
            Self::Ros { .. } => OutputBackendKind::Ros,
            #[cfg(any(test, feature = "test-support"))]
            Self::Test(_) => OutputBackendKind::Test,
        }
    }
    pub fn requires_format(&self) -> bool {
        matches!(self.kind(), OutputBackendKind::Ros)
    }
    pub fn supports_format(&self, format: &str) -> bool {
        match self.kind() {
            OutputBackendKind::Mqtt | OutputBackendKind::Redis => {
                matches!(format, "json" | "json5")
            }
            OutputBackendKind::Ros => !format.trim().is_empty(),
            #[cfg(any(test, feature = "test-support"))]
            OutputBackendKind::Test => true,
            _ => false,
        }
    }
    pub(crate) fn validate_local(&self) -> anyhow::Result<()> {
        match self {
            Self::Mqtt { host, .. } | Self::Redis { host, .. } => anyhow::ensure!(
                !host.trim().is_empty(),
                "output backend host cannot be empty"
            ),
            Self::LimitedNull(n) => anyhow::ensure!(
                *n > 0,
                "limited-null output backend `limit` must be greater than zero"
            ),
            _ => {}
        }
        Ok(())
    }
}

impl<V: JsonStreamValue + RosStreamValue> OutputBackendConfig<V> {
    pub(crate) async fn open(
        &self,
        interface: OutputInterface,
    ) -> Result<OutputWriter<V>, OutputError> {
        self.validate_local().map_err(OutputError::invalid)?;
        match self {
            Self::Stdout => open_stdout(interface).await,
            Self::Null => open_null(interface).await,
            Self::LimitedNull(n) => open_limited_null(*n, interface).await,
            Self::Channel(sender) => open_output(sender.clone(), interface).await,
            Self::Mqtt { host, port, retry } => {
                super::mqtt::open(host.clone(), *port, *retry, interface).await
            }
            Self::Redis { host, port, retry } => {
                #[cfg(feature = "redis")]
                {
                    super::redis::open(host, *port, *retry, interface).await
                }
                #[cfg(not(feature = "redis"))]
                {
                    let _ = (host, port, retry, interface);
                    Err(OutputError::backend("Redis support not enabled"))
                }
            }
            #[cfg(feature = "ros")]
            Self::Ros {
                executor,
                node_name,
            } => {
                V::open_ros_output(std::rc::Rc::clone(executor), node_name.clone(), interface).await
            }
            #[cfg(any(test, feature = "test-support"))]
            Self::Test(opener) => opener.open(interface).await,
        }
    }
}
