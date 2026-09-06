//! Sink-based MQTT output.

use std::{collections::BTreeMap, rc::Rc};

use futures::future::try_join_all;

use crate::{
    core::{JsonStreamValue, OutputBatch, OutputError, OutputInterface, OutputWriter, VarName},
    io::{
        RetryPolicy,
        mqtt::{MqttClient, MqttMessage, MqttProtocol, connect_with_protocol_and_retry},
    },
};

use super::sinks::InterfaceSink;

pub(crate) async fn open<V: JsonStreamValue>(
    host: String,
    port: Option<u16>,
    protocol: MqttProtocol,
    retry: RetryPolicy,
    interface: OutputInterface,
) -> Result<OutputWriter<V>, OutputError> {
    let uri = match port {
        Some(port) => format!("tcp://{host}:{port}"),
        None => format!("tcp://{host}"),
    };
    let client = connect_with_protocol_and_retry(&uri, protocol, retry)
        .await
        .map_err(|error| OutputError::backend(format!("failed to connect to MQTT: {error}")))?;
    let batches_client = client.clone();
    let close_client = client;
    Ok(OutputWriter::from_output_sink(InterfaceSink::with_close(
        interface,
        move |interface, batch: OutputBatch<V>| {
            let client = batches_client.clone();
            async move { publish_batch(client, interface, batch).await }
        },
        move || {
            let client = close_client.clone();
            async move {
                client
                    .disconnect()
                    .await
                    .map_err(|error| OutputError::backend(format!("failed to close MQTT: {error}")))
            }
        },
    )))
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
            let binding = interface.binding(update.variable).ok_or_else(|| {
                OutputError::invalid(format!(
                    "output update variable `{}` has no MQTT route",
                    update.variable
                ))
            })?;
            if binding.role().is_auxiliary() {
                continue;
            }
            let topic = binding
                .route()
                .map(|route| route.address().to_owned())
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

async fn publish_batch<V: JsonStreamValue>(
    client: MqttClient,
    interface: Rc<OutputInterface>,
    batch: OutputBatch<V>,
) -> Result<(), OutputError> {
    let messages = collect_messages(&batch, &interface)?;
    let publishers = messages
        .into_values()
        .map(|messages| publish_variable(client.clone(), messages));
    try_join_all(publishers).await.map(|_| ())
}

async fn publish_variable(
    client: MqttClient,
    messages: Vec<MqttMessage>,
) -> Result<(), OutputError> {
    for message in messages {
        let topic = message.topic.clone();
        client.publish(message).await.map_err(|publish_error| {
            OutputError::backend(format!(
                "failed to publish MQTT message on `{topic}`: {publish_error}"
            ))
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Value,
        core::{OutputBinding, OutputRole, OutputUpdate, Route},
    };

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
    fn mqtt_message_collection_observes_the_current_route_view() {
        let interface = OutputInterface::from_bindings([OutputBinding::new(
            var("x"),
            Some(Route::new("old/topic", None).unwrap()),
            OutputRole::Output,
        )])
        .unwrap();
        let replacement = OutputInterface::from_bindings([OutputBinding::new(
            var("x"),
            Some(Route::new("new/topic", None).unwrap()),
            OutputRole::Output,
        )])
        .unwrap();
        let batch = OutputBatch::update(var("x"), Value::Int(1));

        let old_messages = collect_messages(&batch, &interface).unwrap();
        assert_eq!(old_messages[&var("x")][0].topic, "old/topic");

        let new_messages = collect_messages(&batch, &replacement).unwrap();
        assert_eq!(new_messages[&var("x")][0].topic, "new/topic");
    }

    #[test]
    fn mqtt_collection_uses_route_topic_and_skips_auxiliary_ticks() {
        let interface = OutputInterface::from_bindings([
            OutputBinding::new(
                var("x"),
                Some(Route::new("mapped/topic", None).unwrap()),
                OutputRole::Output,
            ),
            OutputBinding::auxiliary(var("debug")),
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
