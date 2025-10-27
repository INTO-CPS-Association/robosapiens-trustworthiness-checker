use std::collections::BTreeMap;
use std::mem;
use std::rc::Rc;

use futures::StreamExt;
use futures::future::{Either, LocalBoxFuture};
use smol::LocalExecutor;
use tracing::{Level, debug, error, info, instrument, warn};
// TODO: should we use a cancellation token to cleanup the background task
// or does it go away when anyway the receivers of our outputs go away?
// use crate::utils::cancellation_token::CancellationToken;

use crate::core::OutputHandler;
use crate::io::mqtt::{MqttClient, MqttFactory, MqttMessage};
// use crate::stream_utils::drop_guard_stream;
use crate::{OutputStream, Value, core::VarName};

pub struct VarData {
    pub variable: VarName,
    pub topic_name: String,
    stream: Option<OutputStream<Value>>,
}

// A map between channel names and the MQTT topics they
// correspond to
pub type OutputChannelMap = BTreeMap<VarName, String>;

pub struct MQTTOutputHandler {
    factory: MqttFactory,
    pub var_names: Vec<VarName>,
    pub var_map: BTreeMap<VarName, VarData>,
    pub hostname: String,
    pub port: Option<u16>,
    pub aux_info: Vec<VarName>,
}

#[instrument(level = Level::INFO, skip(stream, client))]
async fn publish_stream(
    topic_name: String,
    mut stream: OutputStream<Value>,
    client: Box<dyn MqttClient>,
) {
    info!("Starting output stream publisher for topic: {}", topic_name);
    let mut message_count = 0;

    while let Some(data) = stream.next().await {
        message_count += 1;
        info!(
            "Received value #{} from stream for topic {}: {:?}",
            message_count, topic_name, data
        );

        if data == Value::NoVal {
            continue;
        }

        let json_str = match serde_json5::to_string(&data) {
            Ok(s) => {
                debug!("Successfully serialized value to JSON: {}", s);
                s
            }
            Err(e) => {
                error!(
                    "Failed to serialize value to JSON: {:?}, error: {}",
                    data, e
                );
                continue; // Skip this item
            }
        };

        // Wrap it in e.g., "{value: 42}" because some MQTT clients don't like to receive the
        // raw JSON primitives such as "42"...
        let data = format!(r#"{{"value": {}}}"#, json_str);
        debug!("Formatted message for topic {}: {}", topic_name, data);

        let message = MqttMessage::new(topic_name.clone(), data, 1);
        let mut attempts = 0;

        loop {
            attempts += 1;
            debug!(
                "Attempt #{} to publish message #{} to topic {}",
                attempts, message_count, topic_name
            );

            match client.publish(message.clone()).await {
                Ok(_) => {
                    debug!(
                        "Successfully published message #{} to topic {} on attempt #{}",
                        message_count, topic_name, attempts
                    );
                    break;
                }
                Err(e) => {
                    warn!(
                        "Lost connection when publishing to topic {}, error: {:?}. Attempt #{}, trying to reconnect...",
                        topic_name, e, attempts
                    );
                    match client.reconnect().await {
                        Ok(_) => debug!("Reconnected successfully, retrying publish"),
                        Err(re) => error!("Failed to reconnect: {:?}", re),
                    }

                    if attempts > 5 {
                        error!(
                            "Failed to publish after {} attempts, giving up on this message",
                            attempts
                        );
                        break;
                    }
                }
            }
        }
    }

    debug!(
        "Exiting publish stream for topic {} after processing {} messages",
        topic_name, message_count
    );
}

async fn await_stream(mut stream: OutputStream<Value>) {
    debug!("Starting to monitor auxiliary stream");
    let mut count = 0;
    while let Some(value) = stream.next().await {
        count += 1;
        debug!("Received auxiliary value #{}: {:?}", count, value);
        // Await the streams but do nothing with them
    }
    debug!("Auxiliary stream ended after {} values", count);
}

impl OutputHandler for MQTTOutputHandler {
    type Val = Value;

    fn var_names(&self) -> Vec<VarName> {
        self.var_names.clone()
    }

    fn provide_streams(&mut self, streams: Vec<OutputStream<Value>>) {
        debug!("Providing {} streams to output handler", streams.len());
        debug!("Expected var_names: {:?}", self.var_names());

        // First collect a list of available variables to avoid borrowing issues
        let available_vars = self.var_map.keys().cloned().collect::<Vec<_>>();

        for (var, stream) in self.var_names().iter().zip(streams.into_iter()) {
            debug!("Assigning stream for output variable: {}", var);
            let var_data = self.var_map.get_mut(var).unwrap_or_else(|| {
                panic!(
                    "Variable {} not found in output handler. Available vars: {:?}",
                    var, available_vars
                )
            });
            var_data.stream = Some(stream);
            debug!("Successfully assigned stream for output variable: {}", var);
        }
        debug!("All output streams provided to output handler");
    }

    #[instrument(level = Level::INFO, skip(self))]
    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        debug!(
            "Starting output handler run with {} variables",
            self.var_map.len()
        );
        debug!(
            "Variables in var_map: {:?}",
            self.var_map.keys().collect::<Vec<_>>()
        );

        let streams = self
            .var_map
            .iter_mut()
            .filter_map(|(var_name, var_data)| {
                let channel_name = var_data.topic_name.clone();
                debug!(
                    "Taking stream for variable '{}' to publish to topic '{}'",
                    var_name, channel_name
                );

                match mem::take(&mut var_data.stream) {
                    Some(s) => {
                        debug!("Successfully got stream for variable '{}'", var_name);
                        Some((var_name.clone(), channel_name, s))
                    }
                    None => {
                        warn!("Stream not found for variable '{}'", var_name);
                        // panic!("Stream not found for variable '{}'", var_name);
                        None
                    }
                }
            })
            .collect::<Vec<_>>();

        debug!("Collected {} streams for publishing", streams.len());
        for (var, topic, _) in &streams {
            debug!("Will publish variable '{}' to topic '{}'", var, topic);
        }

        let hostname = self.hostname.clone();
        let port = self.port.clone();
        info!(?hostname, num_streams = ?streams.len(), "OutputProvider MQTT startup task launched");
        debug!("Auxiliary info variables: {:?}", self.aux_info);

        Box::pin(MQTTOutputHandler::inner_handler(
            self.factory.clone(),
            hostname,
            port,
            streams,
            self.aux_info.clone(),
        ))
    }
}

impl MQTTOutputHandler {
    #[instrument(level = Level::INFO)]
    pub fn new(
        _executor: Rc<LocalExecutor<'static>>,
        factory: MqttFactory,
        var_names: Vec<VarName>,
        host: &str,
        port: Option<u16>,
        var_topics: OutputChannelMap,
        aux_info: Vec<VarName>,
    ) -> anyhow::Result<Self> {
        debug!(
            "Creating new MQTT output handler for {} variables: {:?}",
            var_names.len(),
            var_names
        );

        debug!(
            "MQTT broker: {}, port: {:?}, var_topics: {} entries",
            host,
            port,
            var_topics.len()
        );

        let hostname = host.to_string();

        let var_map = var_topics
            .into_iter()
            .map(|(var, topic_name)| {
                info!("Mapping variable {} to topic {}", var, topic_name);
                (
                    var.clone(),
                    VarData {
                        variable: var,
                        topic_name,
                        stream: None,
                    },
                )
            })
            .collect();

        debug!(
            "Created MQTT output handler with {} variables",
            var_names.len()
        );

        // Log individual topic mappings
        debug!("Variable to topic mappings:");
        for (k, v) in &var_map {
            let var_name: &VarName = k;
            let var_data: &VarData = v;
            debug!(
                "  Variable {} will be published to topic {}",
                var_name, var_data.topic_name
            );
        }

        debug!("Auxiliary variables (not published): {:?}", aux_info);

        Ok(MQTTOutputHandler {
            factory,
            var_names,
            var_map,
            hostname,
            port,
            aux_info,
        })
    }

    async fn inner_handler(
        factory: MqttFactory,
        host: String,
        port: Option<u16>,
        streams: Vec<(VarName, String, OutputStream<Value>)>,
        aux_info: Vec<VarName>,
    ) -> anyhow::Result<()> {
        debug!("Starting MQTT inner handler for {} streams", streams.len());

        let uri = match port {
            Some(port) => format!("tcp://{}:{}", host, port),
            None => format!("tcp://{}", host),
        };

        debug!("Connecting to MQTT broker at URI: {}", uri);

        match factory.connect(&uri).await {
            Ok(client) => {
                debug!("Successfully connected to MQTT broker at {}", uri);

                let stream_futures = streams
                    .into_iter()
                    .map(|(var_name, channel_name, stream)| {
                        if aux_info.contains(&var_name) {
                            debug!(
                                "Variable '{}' is auxiliary, not publishing to MQTT",
                                var_name
                            );
                            Either::Left(await_stream(stream))
                        } else {
                            debug!(
                                "Setting up publishing for variable '{}' to topic '{}'",
                                var_name, channel_name
                            );
                            let client = client.clone();
                            Either::Right(publish_stream(channel_name, stream, client))
                        }
                    })
                    .collect::<Vec<_>>();

                debug!(
                    "Awaiting completion of {} stream handlers ({} aux, {} publishing)",
                    stream_futures.len(),
                    aux_info.len(),
                    stream_futures.len() - aux_info.len()
                );

                // Join all futures and wait for completion
                futures::future::join_all(stream_futures).await;

                debug!("All MQTT output stream processors have completed");
                Ok(())
            }
            Err(e) => {
                error!("Failed to connect to MQTT broker at {}: {:?}", uri, e);
                Err(e)
            }
        }
    }
}
