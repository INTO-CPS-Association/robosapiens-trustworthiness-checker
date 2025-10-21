pub(crate) mod common {
    use std::{collections::BTreeMap, rc::Rc};

    use async_cell::unsync::AsyncCell;
    use futures::future::LocalBoxFuture;
    use tracing::{debug, info, warn};

    use unsync::oneshot::Receiver as OSReceiver;
    use unsync::oneshot::Sender as OSSender;
    use unsync::spsc::Sender as SpscSender;

    use crate::io::mqtt::{MqttClient, MqttFactory, MqttMessage};
    use crate::stream_utils::channel_to_output_stream;
    use crate::{
        OutputStream, Value,
        core::VarName,
        utils::cancellation_token::{CancellationToken, DropGuard},
    };
    use anyhow::anyhow;

    pub type Topic = String;
    // A map between channel names and the MQTT channels they
    // correspond to
    pub type VarTopicMap = BTreeMap<VarName, Topic>;
    pub type InverseVarTopicMap = BTreeMap<Topic, VarName>;

    pub const QOS: i32 = 1;
    pub const CHANNEL_SIZE: usize = 10;

    pub struct Base {
        pub factory: MqttFactory,
        pub var_topics: VarTopicMap,
        pub uri: String,
        pub max_reconnect_attempts: u32,

        // Oneshot used to pass the MQTT client and stream from connect() to run()
        pub client_streams_rx: Option<OSReceiver<(Box<dyn MqttClient>, OutputStream<MqttMessage>)>>,
        pub client_streams_tx: Option<OSSender<(Box<dyn MqttClient>, OutputStream<MqttMessage>)>>,

        pub drop_guard: DropGuard,
        // Mainly used for debugging purposes
        pub started: Rc<AsyncCell<bool>>,

        // Channels used to send to the `available_streams`
        pub senders: Option<BTreeMap<VarName, SpscSender<Value>>>,
    }

    impl Base {
        pub fn new(
            factory: MqttFactory,
            host: &str,
            port: Option<u16>,
            var_topics: VarTopicMap,
            max_reconnect_attempts: u32,
        ) -> (BTreeMap<VarName, OutputStream<Value>>, Base) {
            let host: String = host.to_string();

            let (senders, available_streams) = Self::create_senders_receiver(var_topics.iter());
            let senders = Some(senders);

            let started = AsyncCell::new_with(false).into_shared();
            let drop_guard = CancellationToken::new().drop_guard();

            let uri = match port {
                Some(port) => format!("tcp://{}:{}", host, port),
                None => format!("tcp://{}", host),
            };

            let (client_streams_tx, client_streams_rx) = unsync::oneshot::channel();
            let client_streams_tx = Some(client_streams_tx);
            let client_streams_rx = Some(client_streams_rx);

            (
                available_streams,
                Base {
                    factory,
                    var_topics,
                    max_reconnect_attempts,
                    started,
                    uri,
                    client_streams_tx,
                    client_streams_rx,
                    drop_guard,
                    senders,
                },
            )
        }

        pub fn create_senders_receiver<'a, I>(
            var_topics: I,
        ) -> (
            BTreeMap<VarName, SpscSender<Value>>,
            BTreeMap<VarName, OutputStream<Value>>,
        )
        where
            I: IntoIterator<Item = (&'a VarName, &'a Topic)>,
        {
            let (senders, receivers): (
                BTreeMap<_, SpscSender<Value>>,
                BTreeMap<_, OutputStream<Value>>,
            ) = var_topics
                .into_iter()
                .map(|(v, _)| {
                    let (tx, rx) = unsync::spsc::channel(CHANNEL_SIZE);
                    let rx = channel_to_output_stream(rx);
                    ((v.clone(), tx), (v.clone(), rx))
                })
                .unzip();
            (senders, receivers)
        }

        pub async fn connect(&mut self) -> anyhow::Result<()> {
            info!("Starting MQTT input provider connection to {}", self.uri);
            let client_streams_tx = std::mem::take(&mut self.client_streams_tx)
                .expect("Client stream tx already taken");

            // Create and connect to the MQTT client
            info!(
                "Getting client with subscription for {} topics",
                self.var_topics.len()
            );
            let (client, mqtt_stream) = self
                .factory
                .connect_and_receive(&self.uri, self.max_reconnect_attempts)
                .await?;
            info!(?self.uri, "InputProvider MQTT client connected to broker successfully");

            let topics: Vec<String> = self.var_topics.values().cloned().collect();
            let qos = vec![QOS; topics.len()];

            info!(
                "Attempting to subscribe to {} topics: {:?}",
                topics.len(),
                topics
            );

            // Log the full var_topics mapping
            for (var, topic) in &self.var_topics {
                info!("Variable mapping: '{}' <- '{}'", var, topic);
            }

            let mut attempt = 0;
            loop {
                attempt += 1;
                info!("Subscription attempt #{}", attempt);
                match client.subscribe_many(&topics, &qos).await {
                    Ok(_) => {
                        info!("Successfully subscribed to all topics");
                        break;
                    }
                    Err(e) => {
                        warn!(?topics, err=?e, attempt=?attempt, "Failed to subscribe to topics");
                        smol::Timer::after(std::time::Duration::from_millis(100)).await;
                        info!("Retrying subscribing to MQTT topics");
                        match client.reconnect().await {
                            Ok(_) => info!("Reconnected to MQTT broker successfully"),
                            Err(re) => warn!("Failed to reconnect to MQTT broker: {:?}", re),
                        }
                    }
                }

                if attempt > 10 {
                    warn!("Exceeded maximum subscription attempts, continuing anyway");
                    break;
                }
            }
            info!(?self.uri, ?topics, "Connected and subscribed to MQTT broker");

            // Mark as ready as soon as we're connected and subscribed
            self.started.set(true);
            info!("Set MQTT input provider to ready state");

            info!("Sending client and stream to run logic");
            client_streams_tx
                .send((client, mqtt_stream))
                .map_err(|_| anyhow::anyhow!("Failed to send client streams"))?;

            info!("Input provider is fully ready and waiting for messages");
            debug!(
                "Input provider has these topics available: {:?}",
                self.var_topics.values().collect::<Vec<_>>()
            );
            Ok(())
        }

        pub async fn initial_run_logic(
            var_topics: BTreeMap<VarName, String>,
            _started: Rc<AsyncCell<bool>>,
            client_streams_rx: OSReceiver<(Box<dyn MqttClient>, OutputStream<MqttMessage>)>,
        ) -> anyhow::Result<(
            Box<dyn MqttClient>,
            OutputStream<MqttMessage>,
            InverseVarTopicMap,
        )> {
            info!(
                "MQTT input provider run_logic starting with {} variables",
                var_topics.len()
            );

            // Log all input variables and their topics
            for (var, topic) in &var_topics {
                info!("Input variable '{}' mapped to topic '{}'", var, topic);
            }

            // Intentionally consumed - don't want to maintain two maps
            let var_topics_inverse: InverseVarTopicMap = var_topics
                .into_iter()
                .map(|(var, top)| {
                    info!(
                        "Creating reverse mapping: topic '{}' -> variable '{}'",
                        top, var
                    );
                    (top, var)
                })
                .collect();

            info!(
                "Created inverse topic mapping with {} entries",
                var_topics_inverse.len()
            );

            info!("Waiting to receive MQTT client and stream from connect()");
            let (client, mqtt_stream) = client_streams_rx
                .await
                .ok_or_else(|| anyhow::anyhow!("Failed to receive MQTT client and stream"))?;
            info!("Successfully received MQTT client and stream");

            // Started flag is set in connect(), not here
            info!("MQTT input provider run_logic initialization complete");
            Ok((client, mqtt_stream, var_topics_inverse))
        }

        /// Handle a single MQTT message: parse, unwrap, map to variable, and send downstream.
        pub async fn handle_mqtt_message(
            msg: MqttMessage,
            var_topics_inverse: &InverseVarTopicMap,
            senders: &mut BTreeMap<VarName, SpscSender<Value>>,
            // Only for reconfig edge case
            prev_var_topics_inverse: Option<&InverseVarTopicMap>,
        ) -> anyhow::Result<()> {
            // Process the message
            info!(topic = msg.topic, payload = ?msg.payload, "Handling MQTT message");
            let mut value: Value = serde_json5::from_str(&msg.payload).map_err(|e| {
                anyhow!(e).context(format!(
                    "Failed to parse value {:?} sent from MQTT",
                    msg.payload,
                ))
            })?;

            // Unwrap maps wrapped in "value" (done by MQTTOutputHandler to make MQTT clients happy)
            if let Value::Map(map) = &value {
                if let Some(inner) = map.get("value") {
                    value = inner.clone();
                }
            }
            debug!(?value, "MQTT message value:");

            // Resolve the variable name from the topic
            let var = match var_topics_inverse.get(&msg.topic) {
                Some(var) => var,
                // Only relevant if reconfigure:
                None if prev_var_topics_inverse.is_some_and(|map| map.contains_key(&msg.topic)) => {
                    // Drop messages from topics that were unsubscribed during reconfiguration
                    // (Needed because there is a (theoretical?) race condition where messages
                    // are pending while we reconfigure)
                    info!(
                        topic = msg.topic,
                        "Received message during topic reconfiguration for topic which was unsubscribed"
                    );
                    return Ok(());
                }
                None => {
                    return Err(anyhow::anyhow!(
                        "Received message for unknown topic {}",
                        msg.topic
                    ));
                }
            };

            // Get the sender for this variable
            let sender = senders
                .get_mut(var)
                .ok_or_else(|| anyhow::anyhow!("No sender found for variable {}", var))?;

            // Forward the value downstream
            sender
                .send(value)
                .await
                .map_err(|_| anyhow::anyhow!("Failed to send value"))?;

            Ok(())
        }

        pub fn ready(&self) -> LocalBoxFuture<'static, Result<(), anyhow::Error>> {
            let started = self.started.clone();
            Box::pin(async move {
                info!("Checking if MQTT input provider is ready");
                let mut attempts = 0;
                while !started.get().await {
                    attempts += 1;
                    info!(
                        "MQTT input provider not ready yet, checking again (attempt #{})",
                        attempts
                    );
                    smol::Timer::after(std::time::Duration::from_millis(100)).await;

                    if attempts > 50 {
                        warn!(
                            "MQTT input provider still not ready after 5 seconds, continuing to wait"
                        );
                        attempts = 0;
                    }
                }
                info!("MQTT input provider is ready");
                Ok(())
            })
        }

        pub fn vars(&self) -> Vec<VarName> {
            self.var_topics.keys().cloned().collect()
        }

        pub fn take_senders(&mut self) -> BTreeMap<VarName, SpscSender<Value>> {
            std::mem::take(&mut self.senders).expect("Senders already taken")
        }

        pub fn take_client_streams_rx(
            &mut self,
        ) -> OSReceiver<(Box<dyn MqttClient>, OutputStream<MqttMessage>)> {
            std::mem::take(&mut self.client_streams_rx).expect("Client streams rx already taken")
        }
    }
}
