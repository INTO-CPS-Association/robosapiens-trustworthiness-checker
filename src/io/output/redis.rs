//! Sink-based Redis output.

use std::{cell::RefCell, collections::BTreeMap, marker::PhantomData, rc::Rc};

use async_trait::async_trait;
use futures::future::try_join_all;
use redis::{AsyncTypedCommands, aio::MultiplexedConnection};

use crate::core::{
    JsonStreamValue, OutputBackend, OutputBatch, OutputError, OutputInterface,
    OutputInterfaceReconfigurationHandle, OutputWriter, REDIS_HOSTNAME, VarName,
};

use super::sinks::LocalBatchSink;

type LocalRedisConnection = Rc<MultiplexedConnection>;

/// A resource-free Redis backend configuration. A connection is opened only
/// after a fixed output interface has been resolved.
#[derive(Clone, Debug)]
pub struct RedisOutputBackend<V = crate::Value> {
    host: String,
    port: Option<u16>,
    _value: PhantomData<fn() -> V>,
}

impl<V> RedisOutputBackend<V> {
    pub fn new(host: impl Into<String>, port: Option<u16>) -> Self {
        Self {
            host: host.into(),
            port,
            _value: PhantomData,
        }
    }

    pub fn localhost(port: Option<u16>) -> Self {
        Self::new(REDIS_HOSTNAME, port)
    }

    pub fn uri(&self) -> String {
        match self.port {
            Some(port) => format!("redis://{}:{}", self.host, port),
            None => format!("redis://{}", self.host),
        }
    }
}

impl<V> Default for RedisOutputBackend<V> {
    fn default() -> Self {
        Self::localhost(None)
    }
}

#[async_trait(?Send)]
impl<V: JsonStreamValue> OutputBackend for RedisOutputBackend<V> {
    type Val = V;

    async fn open(
        &self,
        interface: OutputInterface,
    ) -> Result<OutputWriter<Self::Val>, OutputError> {
        let uri = self.uri();
        let client = redis::Client::open(uri.clone()).map_err(|error| {
            OutputError::backend(format!(
                "failed to configure Redis client for `{uri}`: {error}"
            ))
        })?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| {
                OutputError::backend(format!("failed to connect to Redis at `{uri}`: {error}"))
            })?;
        let connection = Rc::new(connection);
        let (interface, interface_reconfiguration) = make_reconfigurable_interface(interface);
        Ok(OutputWriter::from_sink_with_interface_reconfiguration(
            LocalBatchSink::new(move |batch: OutputBatch<V>| {
                let connection = Rc::clone(&connection);
                let interface = Rc::clone(&interface);
                async move {
                    let interface = interface.borrow().clone();
                    publish_batch(connection, interface, batch).await
                }
            }),
            Some(interface_reconfiguration),
        ))
    }
}

fn make_reconfigurable_interface(
    interface: OutputInterface,
) -> (
    Rc<RefCell<OutputInterface>>,
    OutputInterfaceReconfigurationHandle,
) {
    let interface = Rc::new(RefCell::new(interface));
    let handle_interface = Rc::clone(&interface);
    let handle = OutputInterfaceReconfigurationHandle::new(move |replacement| {
        let interface = Rc::clone(&handle_interface);
        Box::pin(async move {
            *interface.borrow_mut() = replacement;
            Ok(())
        })
    });
    (interface, handle)
}

fn payload_for<V: JsonStreamValue>(topic: &str, value: &V) -> Result<Option<String>, OutputError> {
    if value.is_no_val() {
        return Ok(None);
    }
    value.encode_json().map(Some).map_err(|error| {
        OutputError::backend(format!(
            "failed to encode Redis value for `{topic}`: {error}"
        ))
    })
}

fn collect_messages<V: JsonStreamValue>(
    batch: &OutputBatch<V>,
    interface: &OutputInterface,
) -> Result<BTreeMap<VarName, (String, Vec<String>)>, OutputError> {
    interface.validate_batch(batch)?;
    let mut messages = BTreeMap::<VarName, (String, Vec<String>)>::new();
    for tick in batch.ticks() {
        for update in tick.updates() {
            let route = interface.route(update.variable).ok_or_else(|| {
                OutputError::invalid(format!(
                    "output update variable `{}` has no Redis route",
                    update.variable
                ))
            })?;
            if route.role.is_auxiliary() {
                continue;
            }
            let topic = route
                .topic
                .clone()
                .unwrap_or_else(|| update.variable.to_string());
            let Some(payload) = payload_for(&topic, update.value)? else {
                continue;
            };
            let entry = messages
                .entry(update.variable.clone())
                .or_insert_with(|| (topic.clone(), Vec::new()));
            entry.1.push(payload);
        }
    }
    Ok(messages)
}

async fn publish_batch<V: JsonStreamValue>(
    connection: LocalRedisConnection,
    interface: OutputInterface,
    batch: OutputBatch<V>,
) -> Result<(), OutputError> {
    let messages = collect_messages(&batch, &interface)?;
    let publishers = messages
        .into_values()
        .map(|(topic, payloads)| publish_variable(Rc::clone(&connection), topic, payloads));
    try_join_all(publishers).await.map(|_| ())
}

async fn publish_variable(
    connection: LocalRedisConnection,
    topic: String,
    payloads: Vec<String>,
) -> Result<(), OutputError> {
    let mut connection = (*connection).clone();
    for payload in payloads {
        connection
            .publish(topic.clone(), payload)
            .await
            .map_err(|error| {
                OutputError::backend(format!(
                    "failed to publish Redis message on `{topic}`: {error}"
                ))
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Value, core::OutputUpdate};

    fn var(name: &str) -> VarName {
        VarName::new(name)
    }

    #[test]
    fn redis_serialization_keeps_the_json_value() {
        assert_eq!(
            payload_for("mapped/channel", &Value::Int(42)).unwrap(),
            Some("42".into())
        );
    }

    #[test]
    fn redis_interface_reconfiguration_swaps_the_route_view() {
        smol::block_on(async {
            let (interface, handle) = make_reconfigurable_interface(
                OutputInterface::from_routes([crate::core::OutputRoute::new(
                    var("x"),
                    Some("old".into()),
                    None,
                    crate::core::OutputRole::Output,
                )])
                .unwrap(),
            );
            let replacement = OutputInterface::from_routes([crate::core::OutputRoute::new(
                var("x"),
                Some("new".into()),
                None,
                crate::core::OutputRole::Output,
            )])
            .unwrap();
            handle.reconfigure(replacement.clone()).await.unwrap();
            assert_eq!(*interface.borrow(), replacement);
        });
    }

    #[test]
    fn redis_collection_uses_route_channel_and_skips_auxiliary() {
        let interface = OutputInterface::from_routes([
            crate::core::OutputRoute::new(
                var("x"),
                Some("mapped/channel".into()),
                None,
                crate::core::OutputRole::Output,
            ),
            crate::core::OutputRoute::auxiliary(var("debug")),
        ])
        .unwrap();
        let batch = OutputBatch::from_ticks(vec![vec![
            OutputUpdate::new(var("x"), Value::Int(1)),
            OutputUpdate::new(var("debug"), Value::Int(2)),
        ]])
        .unwrap();
        let messages = collect_messages(&batch, &interface).unwrap();
        assert_eq!(messages[&var("x")].0, "mapped/channel");
        assert_eq!(messages[&var("x")].1, ["1"]);
        assert_eq!(messages.len(), 1);
    }
}
