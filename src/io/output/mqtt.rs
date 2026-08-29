//! Sink-based MQTT output.

use std::{cell::RefCell, collections::BTreeMap, marker::PhantomData, rc::Rc};

use async_trait::async_trait;
use futures::future::try_join_all;

use crate::{
    core::{
        JsonStreamValue, MQTT_HOSTNAME, OutputBackend, OutputBatch, OutputError, OutputInterface,
        OutputInterfaceReconfigurationHandle, OutputWriter, VarName,
    },
    io::mqtt::{MqttClient, MqttFactory, MqttMessage},
};

use super::sinks::LocalBatchSink;

/// The number of reconnects attempted after a failed MQTT publish.
pub const MQTT_MAX_RETRIES: usize = 5;

type LocalMqttClient = Rc<dyn MqttClient>;

/// A resource-free MQTT backend configuration. The client is connected only by
/// `OutputBackend::open` after route resolution.
#[derive(Clone, Debug)]
pub struct MqttOutputBackend<V = crate::Value> {
    host: String,
    port: Option<u16>,
    _value: PhantomData<fn() -> V>,
}

impl<V> MqttOutputBackend<V> {
    pub fn new(host: impl Into<String>, port: Option<u16>) -> Self {
        Self {
            host: host.into(),
            port,
            _value: PhantomData,
        }
    }

    pub fn localhost(port: Option<u16>) -> Self {
        Self::new(MQTT_HOSTNAME, port)
    }

    pub fn uri(&self) -> String {
        match self.port {
            Some(port) => format!("tcp://{}:{}", self.host, port),
            None => format!("tcp://{}", self.host),
        }
    }
}

impl<V> Default for MqttOutputBackend<V> {
    fn default() -> Self {
        Self::localhost(None)
    }
}

#[async_trait(?Send)]
impl<V: JsonStreamValue> OutputBackend for MqttOutputBackend<V> {
    type Val = V;

    async fn open(
        &self,
        interface: OutputInterface,
    ) -> Result<OutputWriter<Self::Val>, OutputError> {
        let client = MqttFactory::Paho
            .connect(&self.uri())
            .await
            .map_err(|error| OutputError::backend(format!("failed to connect to MQTT: {error}")))?;
        let client: LocalMqttClient = Rc::from(client);
        let (interface, interface_reconfiguration) = make_reconfigurable_interface(interface);
        let batches_client = Rc::clone(&client);
        let close_client = Rc::clone(&client);
        Ok(OutputWriter::from_sink_with_interface_reconfiguration(
            LocalBatchSink::with_close(
                move |batch: OutputBatch<V>| {
                    let client = Rc::clone(&batches_client);
                    let interface = Rc::clone(&interface);
                    async move { publish_batch(client, interface, batch).await }
                },
                move || {
                    let client = Rc::clone(&close_client);
                    async move {
                        client.disconnect().await.map_err(|error| {
                            OutputError::backend(format!("failed to close MQTT: {error}"))
                        })
                    }
                },
            ),
            Some(interface_reconfiguration),
        ))
    }
}

fn message_for<V: JsonStreamValue>(
    topic: String,
    value: &V,
) -> Result<Option<MqttMessage>, OutputError> {
    if value.is_no_val() {
        return Ok(None);
    }
    let encoded = value.encode_json().map_err(|error| {
        OutputError::backend(format!(
            "failed to encode MQTT value for `{topic}`: {error}"
        ))
    })?;
    Ok(Some(MqttMessage::new(
        topic,
        format!(r#"{{"value": {encoded}}}"#),
        1,
    )))
}

fn collect_messages<V: JsonStreamValue>(
    batch: &OutputBatch<V>,
    interface: &OutputInterface,
) -> Result<BTreeMap<VarName, Vec<MqttMessage>>, OutputError> {
    interface.validate_batch(batch)?;
    let mut messages = BTreeMap::<VarName, Vec<MqttMessage>>::new();
    for tick in batch.ticks() {
        for update in tick.updates() {
            let route = interface.route(update.variable).ok_or_else(|| {
                OutputError::invalid(format!(
                    "output update variable `{}` has no MQTT route",
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
            if let Some(message) = message_for(topic, update.value)? {
                messages
                    .entry(update.variable.clone())
                    .or_default()
                    .push(message);
            }
        }
    }
    Ok(messages)
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
            // Keep the borrow entirely within this synchronous assignment. The
            // next publish observes the replacement without reconnecting.
            *interface.borrow_mut() = replacement;
            Ok(())
        })
    });
    (interface, handle)
}

async fn publish_batch<V: JsonStreamValue>(
    client: LocalMqttClient,
    interface: Rc<RefCell<OutputInterface>>,
    batch: OutputBatch<V>,
) -> Result<(), OutputError> {
    // Take a value snapshot before starting any publish future. In particular,
    // a RefCell borrow must not be retained across `try_join_all`'s await.
    let messages = {
        let interface = interface.borrow();
        collect_messages(&batch, &interface)?
    };
    let publishers = messages
        .into_values()
        .map(|messages| publish_variable(Rc::clone(&client), messages));
    try_join_all(publishers).await.map(|_| ())
}

async fn publish_variable(
    client: LocalMqttClient,
    messages: Vec<MqttMessage>,
) -> Result<(), OutputError> {
    for message in messages {
        let topic = message.topic.clone();
        let mut attempts = 0;
        loop {
            attempts += 1;
            match client.publish(message.clone()).await {
                Ok(()) => break,
                Err(publish_error) => {
                    let _ = client.reconnect().await;
                    if attempts > MQTT_MAX_RETRIES {
                        return Err(OutputError::backend(format!(
                            "failed to publish MQTT message on `{topic}` after {attempts} attempts: {publish_error}"
                        )));
                    }
                }
            }
        }
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
    fn mqtt_serialization_uses_json_value_and_qos_one() {
        let message = message_for("mapped/topic".into(), &Value::Int(42))
            .unwrap()
            .unwrap();
        assert_eq!(message.topic, "mapped/topic");
        assert_eq!(message.payload, r#"{"value": 42}"#);
        assert_eq!(message.qos, 1);
    }

    #[test]
    fn mqtt_interface_reconfiguration_swaps_route_view_in_place() {
        smol::block_on(async {
            let (interface, handle) = make_reconfigurable_interface(
                OutputInterface::from_routes([crate::core::OutputRoute::new(
                    var("x"),
                    Some("old/topic".into()),
                    None,
                    crate::core::OutputRole::Output,
                )])
                .unwrap(),
            );
            let replacement = OutputInterface::from_routes([crate::core::OutputRoute::new(
                var("x"),
                Some("new/topic".into()),
                None,
                crate::core::OutputRole::Output,
            )])
            .unwrap();
            let batch = OutputBatch::update(var("x"), Value::Int(1));

            let old_messages = {
                let interface = interface.borrow();
                collect_messages(&batch, &interface).unwrap()
            };
            assert_eq!(old_messages[&var("x")][0].topic, "old/topic");

            handle.reconfigure(replacement.clone()).await.unwrap();

            assert_eq!(*interface.borrow(), replacement);
            let new_messages = {
                let interface = interface.borrow();
                collect_messages(&batch, &interface).unwrap()
            };
            assert_eq!(new_messages[&var("x")][0].topic, "new/topic");
        });
    }

    #[test]
    fn mqtt_collection_uses_route_topic_and_skips_auxiliary_ticks() {
        let interface = OutputInterface::from_routes([
            crate::core::OutputRoute::new(
                var("x"),
                Some("mapped/topic".into()),
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
        assert_eq!(messages[&var("x")][0].topic, "mapped/topic");
        assert_eq!(messages.len(), 1);
    }
}
