//! Sink-based Redis output.

use std::{collections::BTreeMap, rc::Rc};

use futures::future::try_join_all;
use redis::{AsyncTypedCommands, aio::MultiplexedConnection};

use crate::core::{
    JsonStreamValue, OutputBatch, OutputError, OutputInterface, OutputWriter, VarName,
};
use crate::io::{RetryPolicy, RetryTracker};

use super::sinks::InterfaceSink;

type LocalRedisConnection = Rc<MultiplexedConnection>;

pub(crate) async fn open<V: JsonStreamValue>(
    host: &str,
    port: Option<u16>,
    retry: RetryPolicy,
    interface: OutputInterface,
) -> Result<OutputWriter<V>, OutputError> {
    let uri = format!("redis://{}:{}", host, port.unwrap_or(6379));
    let client = redis::Client::open(uri.clone()).map_err(|error| {
        OutputError::backend(format!(
            "failed to configure Redis client for `{uri}`: {error}"
        ))
    })?;
    let connection = open_connection(&client, &uri, retry).await?;
    let connection = Rc::new(connection);
    Ok(OutputWriter::from_output_sink(InterfaceSink::new(
        interface,
        move |interface, batch: OutputBatch<V>| {
            let connection = Rc::clone(&connection);
            async move { publish_batch(connection, interface, batch).await }
        },
    )))
}

fn retryable_connection_error(error: &redis::RedisError) -> bool {
    match error.kind() {
        redis::ErrorKind::Io => true,
        redis::ErrorKind::Server(kind) => matches!(
            kind,
            redis::ServerErrorKind::BusyLoading
                | redis::ServerErrorKind::TryAgain
                | redis::ServerErrorKind::ClusterDown
                | redis::ServerErrorKind::MasterDown
        ),
        _ => false,
    }
}

async fn open_connection(
    client: &redis::Client,
    uri: &str,
    retry: RetryPolicy,
) -> Result<MultiplexedConnection, OutputError> {
    let mut tracker = retry.tracker();
    loop {
        match client.get_multiplexed_async_connection().await {
            Ok(connection) => return Ok(connection),
            Err(error) if retryable_connection_error(&error) => {
                let Some(delay) = tracker.record_failure() else {
                    return Err(OutputError::backend(format!(
                        "failed to connect to Redis at `{uri}`: {error}"
                    )));
                };
                RetryTracker::backoff(delay).await;
            }
            Err(error) => {
                return Err(OutputError::backend(format!(
                    "failed to connect to Redis at `{uri}`: {error}"
                )));
            }
        }
    }
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
            let binding = interface.binding(update.variable).ok_or_else(|| {
                OutputError::invalid(format!(
                    "output update variable `{}` has no Redis route",
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
    interface: Rc<OutputInterface>,
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
            let interface = OutputInterface::from_bindings([crate::core::OutputBinding::new(
                var("x"),
                Some(crate::core::Route::new("old", None).unwrap()),
                crate::core::OutputRole::Output,
            )])
            .unwrap();
            let seen = Rc::new(std::cell::RefCell::new(Vec::new()));
            let recorded = Rc::clone(&seen);
            let mut writer = OutputWriter::from_output_sink(InterfaceSink::new(
                interface,
                move |interface, batch: OutputBatch<Value>| {
                    let recorded = Rc::clone(&recorded);
                    async move {
                        recorded
                            .borrow_mut()
                            .push(collect_messages(&batch, &interface)?);
                        Ok(())
                    }
                },
            ));
            writer
                .feed(OutputBatch::update(var("x"), Value::Int(1)))
                .await
                .unwrap();
            let replacement = OutputInterface::from_bindings([crate::core::OutputBinding::new(
                var("x"),
                Some(crate::core::Route::new("new", None).unwrap()),
                crate::core::OutputRole::Output,
            )])
            .unwrap();
            writer.rebind(replacement).await.unwrap();
            writer
                .send(OutputBatch::update(var("x"), Value::Int(2)))
                .await
                .unwrap();
            assert_eq!(seen.borrow()[0][&var("x")].0, "old");
            assert_eq!(seen.borrow()[1][&var("x")].0, "new");
        });
    }

    #[test]
    fn redis_collection_uses_route_channel_and_skips_auxiliary() {
        let interface = OutputInterface::from_bindings([
            crate::core::OutputBinding::new(
                var("x"),
                Some(crate::core::Route::new("mapped/channel", None).unwrap()),
                crate::core::OutputRole::Output,
            ),
            crate::core::OutputBinding::auxiliary(var("debug")),
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
