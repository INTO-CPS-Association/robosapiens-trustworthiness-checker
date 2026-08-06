use std::collections::BTreeMap;
use std::mem;
use std::rc::Rc;

use anyhow::{Context, anyhow};
use futures::StreamExt;
use futures::future::{Either, LocalBoxFuture};
use smol::LocalExecutor;
use tracing::{Level, debug, error, info, instrument, warn};
use unsync::oneshot::{Receiver as OSReceiver, Sender as OSSender};
// TODO: should we use a cancellation token to cleanup the background task
// or does it go away when anyway the receivers of our outputs go away?
// use crate::utils::cancellation_token::CancellationToken;

use crate::core::{JsonStreamValue, OutputHandler};
use crate::io::mqtt::{MqttClient, MqttFactory, MqttMessage};

// use crate::stream_utils::drop_guard_stream;
use crate::{OutputStream, Value, core::VarName};

pub struct VarData<V = Value> {
    pub variable: VarName,
    pub topic_name: String,
    stream: Option<OutputStream<V>>,
}

// A map between channel names and the MQTT topics they
// correspond to
pub type OutputChannelMap = BTreeMap<VarName, String>;

pub struct MqttOutputHandler<V = Value> {
    factory: MqttFactory,
    var_names: Vec<VarName>,
    pub var_map: BTreeMap<VarName, VarData<V>>,
    pub hostname: String,
    pub port: Option<u16>,
    pub aux_info: Vec<VarName>,
    pub uri: String,
    client_tx: Option<OSSender<Box<dyn MqttClient>>>,
    client_rx: Option<OSReceiver<Box<dyn MqttClient>>>,
    connected: bool,
}

#[instrument(level = Level::INFO, skip(stream, client))]
async fn publish_stream<V: JsonStreamValue>(
    topic_name: String,
    mut stream: OutputStream<V>,
    client: Box<dyn MqttClient>,
) -> anyhow::Result<()> {
    info!("Starting output stream publisher for topic: {}", topic_name);
    let mut message_count = 0;

    while let Some(data) = stream.next().await {
        message_count += 1;
        info!(
            "Received value #{} from stream for topic {}: {:?}",
            message_count, topic_name, data
        );

        if data.is_no_val() {
            continue;
        }

        let json_str = data
            .encode_json()
            .with_context(|| format!("failed to encode MQTT value for `{topic_name}`"))?;
        debug!("Successfully serialized value to JSON: {}", json_str);

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
                        return Err(anyhow!(
                            "failed to publish MQTT message on `{topic_name}` after {attempts} attempts"
                        ));
                    }
                }
            }
        }
    }

    debug!(
        "Exiting publish stream for topic {} after processing {} messages",
        topic_name, message_count
    );
    Ok(())
}

async fn await_stream<V: crate::core::StreamData>(
    mut stream: OutputStream<V>,
) -> anyhow::Result<()> {
    debug!("Starting to monitor auxiliary stream");
    let mut count = 0;
    while let Some(value) = stream.next().await {
        count += 1;
        debug!("Received auxiliary value #{}: {:?}", count, value);
        // Await the streams but do nothing with them
    }
    debug!("Auxiliary stream ended after {} values", count);
    Ok(())
}

impl<V: JsonStreamValue> OutputHandler for MqttOutputHandler<V> {
    type Val = V;

    fn provide_streams(&mut self, streams: BTreeMap<VarName, OutputStream<V>>) {
        debug!("Providing {} streams to output handler", streams.len());
        debug!("Expected var_names: {:?}", self.var_names);

        let available_vars: Vec<_> = self.var_map.keys().cloned().collect();
        for (var, stream) in streams {
            debug!("Assigning stream for output variable: {}", var);
            let var_data = self.var_map.get_mut(&var).unwrap_or_else(|| {
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

        let aux_info = self.aux_info.clone();
        let client_rx = mem::take(&mut self.client_rx)
            .expect("MQTT output handler client receiver already taken");
        let connected = self.connected;
        info!(?self.hostname, num_streams = ?streams.len(), "OutputProvider MQTT startup task launched");
        debug!("Auxiliary info variables: {:?}", self.aux_info);

        Box::pin(async move {
            if !connected {
                return Err(anyhow!("MQTTOutputHandler not connected before run"));
            }

            let client = client_rx
                .await
                .ok_or_else(|| anyhow!("Failed to receive MQTT client for output handler"))?;
            MqttOutputHandler::inner_handler(client, streams, aux_info).await
        })
    }
}

impl<V> MqttOutputHandler<V> {
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
        let uri = match port {
            Some(port) => format!("tcp://{}:{}", hostname, port),
            None => format!("tcp://{}", hostname),
        };
        let (client_tx, client_rx) = unsync::oneshot::channel();

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
            let var_data: &VarData<V> = v;
            debug!(
                "  Variable {} will be published to topic {}",
                var_name, var_data.topic_name
            );
        }

        debug!("Auxiliary variables (not published): {:?}", aux_info);

        Ok(MqttOutputHandler {
            factory,
            var_names,
            var_map,
            hostname,
            port,
            aux_info,
            uri,
            client_tx: Some(client_tx),
            client_rx: Some(client_rx),
            connected: false,
        })
    }

    pub async fn connect(&mut self) -> anyhow::Result<()> {
        info!(?self.uri, "Starting MQTT output handler connection");
        let client_tx = mem::take(&mut self.client_tx)
            .expect("MQTT output handler client sender already taken");
        let client = self.factory.connect(&self.uri).await?;
        client_tx
            .send(client)
            .map_err(|_| anyhow!("Failed to send MQTT client to output handler run"))?;
        self.connected = true;
        Ok(())
    }

    async fn inner_handler(
        client: Box<dyn MqttClient>,
        streams: Vec<(VarName, String, OutputStream<V>)>,
        aux_info: Vec<VarName>,
    ) -> anyhow::Result<()>
    where
        V: JsonStreamValue,
    {
        debug!("Starting MQTT inner handler for {} streams", streams.len());

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

        futures::future::try_join_all(stream_futures).await?;
        debug!("All MQTT output stream processors have completed");
        Ok(())
    }
}
