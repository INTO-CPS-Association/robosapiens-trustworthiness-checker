use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use async_cell::unsync::AsyncCell;
use futures::{FutureExt, StreamExt, future::LocalBoxFuture};
use paho_mqtt as mqtt;
use smol::LocalExecutor;
use tracing::{Level, debug, info, info_span, instrument, warn};
use unsync::oneshot::Receiver as OSReceiver;
use unsync::oneshot::Sender as OSSender;
use unsync::spsc::Sender as SpscSender;

use super::client::provide_mqtt_client_with_subscription;
use crate::{
    InputProvider, OutputStream, Value,
    core::VarName,
    stream_utils::channel_to_output_stream,
    utils::cancellation_token::{CancellationToken, DropGuard},
};
use anyhow::anyhow;

const QOS: i32 = 1;
const CHANNEL_SIZE: usize = 10;

type Topic = String;
// A map between channel names and the MQTT channels they
// correspond to
pub type VarTopicMap = BTreeMap<VarName, Topic>;
pub type InverseVarTopicMap = BTreeMap<Topic, VarName>;

pub struct ReconfMQTTInputProvider {
    var_topics: VarTopicMap,
    uri: String,
    max_reconnect_attempts: u32,

    // Streams that can be taken ownership of by calling `input_stream`
    // (Note: Can't use AsyncCell because it is also used outside async context)
    available_streams: Rc<RefCell<BTreeMap<VarName, OutputStream<Value>>>>,
    // Channels used to send to the `available_streams`
    senders: Option<BTreeMap<VarName, SpscSender<Value>>>,

    // Oneshot used to pass the MQTT client and stream from connect() to run()
    client_streams_rx: Option<OSReceiver<(mqtt::AsyncClient, OutputStream<mqtt::Message>)>>,
    client_streams_tx: Option<OSSender<(mqtt::AsyncClient, OutputStream<mqtt::Message>)>>,

    reconfig: Option<OutputStream<VarTopicMap>>,

    drop_guard: DropGuard,
    // Mainly used for debugging purposes
    pub started: Rc<AsyncCell<bool>>,
}

impl ReconfMQTTInputProvider {
    #[instrument(level = Level::INFO, skip(var_topics, reconfig))]
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        host: &str,
        port: Option<u16>,
        var_topics: VarTopicMap,
        max_reconnect_attempts: u32,
        reconfig: OutputStream<VarTopicMap>,
    ) -> Self {
        let host: String = host.to_string();

        let (senders, receivers): (
            BTreeMap<_, SpscSender<Value>>,
            BTreeMap<_, OutputStream<Value>>,
        ) = var_topics
            .iter()
            .map(|(v, _)| {
                let (tx, rx) = unsync::spsc::channel(CHANNEL_SIZE);
                let rx = channel_to_output_stream(rx);
                ((v.clone(), tx), (v.clone(), rx))
            })
            .unzip();
        let senders = Some(senders);
        let available_streams = Rc::new(RefCell::new(receivers));

        let started = AsyncCell::new_with(false).into_shared();
        let drop_guard = CancellationToken::new().drop_guard();

        let uri = match port {
            Some(port) => format!("tcp://{}:{}", host, port),
            None => format!("tcp://{}", host),
        };

        let (client_streams_tx, client_streams_rx) = unsync::oneshot::channel();
        let client_streams_tx = Some(client_streams_tx);
        let client_streams_rx = Some(client_streams_rx);

        let reconfig = Some(reconfig);

        ReconfMQTTInputProvider {
            var_topics,
            started,
            uri,
            max_reconnect_attempts,
            available_streams,
            senders,
            client_streams_tx,
            client_streams_rx,
            reconfig,
            drop_guard,
        }
    }

    pub async fn connect(&mut self) -> anyhow::Result<()> {
        let client_streams_tx =
            std::mem::take(&mut self.client_streams_tx).expect("Client stream tx already taken");

        // Create and connect to the MQTT client
        info!("Getting client with subscription");
        let (client, mqtt_stream) =
            provide_mqtt_client_with_subscription(self.uri.clone(), self.max_reconnect_attempts)
                .await?;
        info!(?self.uri, "InputProvider MQTT client connected to broker");

        let topics = self.var_topics.values().collect::<Vec<_>>();
        let qos = vec![QOS; topics.len()];
        loop {
            match client.subscribe_many(&topics, &qos).await {
                Ok(_) => break,
                Err(e) => {
                    warn!(name: "Failed to subscribe to topics", ?topics, err=?e);
                    smol::Timer::after(std::time::Duration::from_millis(100)).await;
                    info!("Retrying subscribing to MQTT topics");
                    let _e = client.reconnect().await;
                }
            }
        }
        info!(?self.uri, ?topics, "Connected and subscribed to MQTT broker");

        client_streams_tx
            .send((client, mqtt_stream))
            .map_err(|_| anyhow::anyhow!("Failed to send client streams"))?;

        Ok(())
    }

    /// Update the `available_streams` based on the new variable-topic mapping and return the new senders.
    fn update_available_streams(
        var_topics_inverse: &InverseVarTopicMap,
        available_streams: Rc<RefCell<BTreeMap<VarName, OutputStream<Value>>>>,
    ) -> BTreeMap<VarName, SpscSender<Value>> {
        let (senders, receivers): (
            BTreeMap<_, SpscSender<Value>>,
            BTreeMap<_, OutputStream<Value>>,
        ) = var_topics_inverse
            .into_iter()
            .map(|(_, v)| {
                let (tx, rx) = unsync::spsc::channel(CHANNEL_SIZE);
                let rx = channel_to_output_stream(rx);
                ((v.clone(), tx), (v.clone(), rx))
            })
            .unzip();

        *available_streams.borrow_mut() = receivers;
        senders
    }

    /// Handle reconfiguration by updating MQTT subscriptions, `available_streams`, and returning
    /// new senders.
    async fn handle_reconfiguration(
        client: &mqtt::AsyncClient,
        new_var_topics: VarTopicMap,
        var_topics_inverse: InverseVarTopicMap,
        available_streams: Rc<RefCell<BTreeMap<VarName, OutputStream<Value>>>>,
    ) -> (InverseVarTopicMap, BTreeMap<VarName, SpscSender<Value>>) {
        // Unsubscribe to those not in new_var_topics
        let to_unsubscribe: Vec<_> = var_topics_inverse
            .keys()
            .filter(|t| !new_var_topics.values().any(|nt| nt == *t))
            .cloned()
            .collect();

        // Subscribe to those not already in var_topics_inverse
        let to_subscribe: Vec<_> = new_var_topics
            .values()
            .filter(|t| !var_topics_inverse.keys().any(|ot| ot == *t))
            .cloned()
            .collect();

        // Unsubscribe from old topics
        if !to_unsubscribe.is_empty() {
            loop {
                match client.unsubscribe_many(&to_unsubscribe).await {
                    Ok(_) => {
                        info!(name = "Unsubscribed from old topics", ?to_unsubscribe);
                        break;
                    }
                    Err(e) => {
                        warn!(name="Failed to unsubscribe from old topics", ?to_unsubscribe, err=?e);
                        smol::Timer::after(std::time::Duration::from_millis(100)).await;
                        info!("Retrying unsubscribing to MQTT topics");
                        let _e = client.reconnect().await;
                    }
                }
            }
        }
        // Subscribe to new topics
        let qos = vec![QOS; to_subscribe.len()];
        loop {
            match client.subscribe_many(&to_subscribe, &qos).await {
                Ok(_) => {
                    info!(name = "Subscribed to new topics", ?to_subscribe);
                    break;
                }
                Err(e) => {
                    warn!(name: "Failed to subscribe to new topics", ?to_subscribe, err=?e);
                    smol::Timer::after(std::time::Duration::from_millis(100)).await;
                    info!("Retrying subscribing to MQTT topics");
                    let _e = client.reconnect().await;
                }
            }
        }
        let var_topics_inverse = new_var_topics
            .into_iter()
            .map(|(var, top)| (top, var))
            .collect::<InverseVarTopicMap>();
        let senders = Self::update_available_streams(&var_topics_inverse, available_streams);
        (var_topics_inverse, senders)
    }

    /// Handle a single MQTT message: parse, unwrap, map to variable, and send downstream.
    async fn handle_mqtt_message(
        msg: paho_mqtt::Message,
        var_topics_inverse: &InverseVarTopicMap,
        prev_var_topics_inverse: &InverseVarTopicMap,
        senders: &mut BTreeMap<VarName, SpscSender<Value>>,
    ) -> anyhow::Result<()> {
        // Process the message
        debug!(
            name = "Received MQTT message on topic:",
            topic = msg.topic()
        );
        let mut value: Value = serde_json5::from_str(&msg.payload_str()).map_err(|e| {
            anyhow!(e).context(format!(
                "Failed to parse value {:?} sent from MQTT",
                msg.payload_str(),
            ))
        })?;

        // Unwrap maps wrapped in "value" (done by MQTTOutputHandler to make MQTT clients happy)
        if let Value::Map(map) = &value {
            if let Some(inner) = map.get("value") {
                value = inner.clone();
            }
        }
        debug!(name = "MQTT message value:", ?value);

        // Resolve the variable name from the topic
        let var = if let Some(var) = var_topics_inverse.get(msg.topic()) {
            var
        } else if prev_var_topics_inverse.contains_key(msg.topic()) {
            // Drop messages from topics that were unsubscribed during reconfiguration
            // (Needed because there is a (theoretical?) race condition where messages
            // are pending while we reconfigure)
            info!(
                name = "Received message during topic reconfiguration for topic which was unsubscribed",
                topic = msg.topic()
            );
            return Ok(());
        } else {
            return Err(anyhow::anyhow!(
                "Received message for unknown topic {}",
                msg.topic()
            ));
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

    async fn run_logic(
        var_topics: BTreeMap<VarName, String>,
        mut senders: BTreeMap<VarName, SpscSender<Value>>,
        available_streams: Rc<RefCell<BTreeMap<VarName, OutputStream<Value>>>>,
        started: Rc<AsyncCell<bool>>,
        cancellation_token: CancellationToken,
        client_streams_rx: OSReceiver<(mqtt::AsyncClient, OutputStream<paho_mqtt::Message>)>,
        mut reconfig: OutputStream<VarTopicMap>,
    ) -> anyhow::Result<()> {
        let mqtt_input_span = info_span!("ReconfMQTTInputProvider run_logic");
        let _enter = mqtt_input_span.enter();
        info!("run_logic started");

        // Intentionally consumed - don't want to maintain two maps
        let mut var_topics_inverse: InverseVarTopicMap = var_topics
            .into_iter()
            .map(|(var, top)| (top, var))
            .collect();
        let mut prev_var_topics_inverse = InverseVarTopicMap::new();

        let (client, mut mqtt_stream) = client_streams_rx
            .await
            .ok_or_else(|| anyhow::anyhow!("Failed to receive MQTT client and stream"))?;

        debug!("Set started");
        started.set(true);

        let result = async {
            loop {
                // Notably: Handle new configs before receiving data
                futures::select_biased! {
                    new_config = reconfig.next().fuse() => {
                        match new_config {
                            Some(new_var_topics) => {
                                info!(name="Reconfiguring MQTTInputProvider", ?new_var_topics, "Reconfiguring MQTTInputProvider with new variable-topic mapping");
                                prev_var_topics_inverse = var_topics_inverse.clone();
                                (var_topics_inverse, senders) = Self::handle_reconfiguration(
                                    &client, new_var_topics, var_topics_inverse,
                                    available_streams.clone()).await;
                            },
                            None => {
                                debug!("Reconfiguration stream ended, stopping MQTTInputProvider");
                                return Ok(());
                            }
                        }
                    }
                    msg = mqtt_stream.next().fuse() => {
                        match msg {
                            Some(msg) => {
                                Self::handle_mqtt_message(msg, &var_topics_inverse, &prev_var_topics_inverse, &mut senders).await?;
                            }
                            None => {
                                debug!("MQTT stream ended");
                                return Ok(());
                            }
                        }
                    }
                    _ = cancellation_token.cancelled().fuse() => {
                        debug!("MQTTInputProvider: Input monitor task cancelled");
                        return Ok(());
                    }
                }
            }
        }.await;

        // Always disconnect the client when we're done, regardless of success or error
        debug!("Disconnecting MQTT client");
        let _ = client.disconnect(None).await;

        result
    }
}

impl InputProvider for ReconfMQTTInputProvider {
    type Val = Value;

    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        // Take ownership of the stream for the variable, if it exists
        self.available_streams.borrow_mut().remove(var)
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let senders = std::mem::take(&mut self.senders).expect("Senders already taken");
        let client_streams_rx = self
            .client_streams_rx
            .take()
            .expect("Client streams rx already taken");
        let reconfig = self.reconfig.take().expect("Reconfig stream already taken");

        Box::pin(Self::run_logic(
            self.var_topics.clone(),
            senders,
            self.available_streams.clone(),
            self.started.clone(),
            self.drop_guard.clone_tok(),
            client_streams_rx,
            reconfig,
        ))
    }

    fn ready(&self) -> LocalBoxFuture<'static, Result<(), anyhow::Error>> {
        let started = self.started.clone();
        Box::pin(async move {
            debug!("In ready");
            while !started.get().await {
                debug!("Checking if ready");
                smol::Timer::after(std::time::Duration::from_millis(100)).await;
            }
            Ok(())
        })
    }

    fn vars(&self) -> Vec<VarName> {
        self.var_topics.keys().cloned().collect()
    }
}

#[cfg(test)]
#[cfg(feature = "testcontainers")]
mod container_tests {

    use async_compat::Compat as TokioCompat;
    use async_stream::stream;
    use futures::StreamExt;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::vec;
    use std::{collections::BTreeMap, rc::Rc};
    use tracing::info;

    use crate::InputProvider;
    use crate::OutputStream;
    use crate::async_test;
    use crate::io::mqtt::ReconfMQTTInputProvider;
    use crate::io::mqtt::reconfigurable_input_provider::VarTopicMap;
    use crate::{Value, VarName};
    use tc_testutils::mqtt::{dummy_mqtt_publisher, dummy_stream_mqtt_publisher, start_mqtt};
    use tc_testutils::streams::with_timeout_res;
    use tc_testutils::streams::{tick_stream, with_timeout};

    #[apply(async_test)]
    async fn test_reconf_mqtt_no_reconf(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let xs = vec![Value::Int(1), Value::Int(2)];
        let ys = vec![Value::Int(3), Value::Int(4)];
        let expected: Vec<_> = xs.iter().cloned().zip(ys.iter().cloned()).collect();

        let mqtt_server = start_mqtt().await;

        let mqtt_port = TokioCompat::new(mqtt_server.get_host_port_ipv4(1883))
            .await
            .expect("Failed to get host port for MQTT server");

        let var_topics = [
            ("x".into(), "mqtt_input_x".to_string()),
            ("y".into(), "mqtt_input_y".to_string()),
        ]
        .into_iter()
        .collect::<BTreeMap<VarName, _>>();

        // Empty reconfiguration stream that never ends:
        let reconf_stream: OutputStream<VarTopicMap> = futures::stream::pending().boxed_local();

        // Create the MQTT input provider
        let mut input_provider = ReconfMQTTInputProvider::new(
            executor.clone(),
            "localhost",
            Some(mqtt_port),
            var_topics,
            0,
            reconf_stream,
        );
        with_timeout_res(input_provider.connect(), 5, "input_provider_connect").await?;

        let x_stream = input_provider
            .input_stream(&"x".into())
            .ok_or_else(|| anyhow::anyhow!("x stream unavailable"))?;
        let y_stream = input_provider
            .input_stream(&"y".into())
            .ok_or_else(|| anyhow::anyhow!("y stream unavailable"))?;

        let input_provider_ready = input_provider.ready();

        executor.spawn(input_provider.run()).detach();
        with_timeout_res(input_provider_ready, 5, "input_provider_ready").await?;

        // Spawn dummy MQTT publisher nodes and keep handles to wait for completion
        let x_publisher_task = executor.spawn(dummy_mqtt_publisher(
            "x_publisher".to_string(),
            "mqtt_input_x".to_string(),
            xs,
            mqtt_port,
        ));

        let y_publisher_task = executor.spawn(dummy_mqtt_publisher(
            "y_publisher".to_string(),
            "mqtt_input_y".to_string(),
            ys,
            mqtt_port,
        ));

        let xs_fut = with_timeout(
            x_stream.take(expected.len()).collect::<Vec<_>>(),
            10,
            "x_stream.take",
        );
        let ys_fut = with_timeout(
            y_stream.take(expected.len()).collect::<Vec<_>>(),
            10,
            "y_stream.take",
        );
        let (x_vals, y_vals) = futures::try_join!(xs_fut, ys_fut)?;
        let received: Vec<_> = x_vals.into_iter().zip(y_vals.into_iter()).collect();

        info!("Received (x, y): {:?}", received);
        assert_eq!(received, expected);

        // Wait for publishers to complete and then shutdown MQTT server to terminate connections
        info!("Waiting for publishers to complete...");
        with_timeout(x_publisher_task, 5, "x_publisher_task").await?;
        with_timeout(y_publisher_task, 5, "y_publisher_task").await?;
        info!("All publishers completed, shutting down MQTT server");

        Ok(())
    }

    #[apply(async_test)]
    async fn test_reconf_mqtt_reconf_before_run(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let xs = vec![Value::Int(1), Value::Int(2)];
        let ys = vec![Value::Int(3), Value::Int(4)];
        let expected: Vec<_> = xs.iter().cloned().zip(ys.iter().cloned()).collect();

        let mqtt_server = start_mqtt().await;

        let mqtt_port = with_timeout_res(
            TokioCompat::new(mqtt_server.get_host_port_ipv4(1883)),
            5,
            "get_host_port",
        )
        .await
        .expect("Failed to get host port for MQTT server");

        let wrong_var_topics: VarTopicMap = [
            ("xx".into(), "mqtt_input_xx".to_string()),
            ("yy".into(), "mqtt_input_yy".to_string()),
        ]
        .into_iter()
        .collect();
        let var_topics: VarTopicMap = [
            ("x".into(), "mqtt_input_x".to_string()),
            ("y".into(), "mqtt_input_y".to_string()),
        ]
        .into_iter()
        .collect();

        // Reconfiguration stream that first yields topics, then waits forever
        let reconf_stream: OutputStream<VarTopicMap> =
            stream! {yield var_topics; futures::future::pending::<()>().await; }.boxed_local();

        // Create the MQTT input provider
        let mut input_provider = ReconfMQTTInputProvider::new(
            executor.clone(),
            "localhost",
            Some(mqtt_port),
            wrong_var_topics,
            0,
            reconf_stream,
        );
        with_timeout_res(input_provider.connect(), 5, "input_provider_connect").await?;

        let input_provider_ready = input_provider.ready();

        executor.spawn(input_provider.run()).detach();
        with_timeout_res(input_provider_ready, 5, "input_provider_ready").await?;

        // Wait for reconf request to be processed.
        // (Without this, the outcome depends on how the Executor happens to run things)
        smol::Timer::after(std::time::Duration::from_secs(1)).await;

        let x_stream = input_provider
            .input_stream(&"x".into())
            .ok_or_else(|| anyhow::anyhow!("x stream unavailable"))?;
        let y_stream = input_provider
            .input_stream(&"y".into())
            .ok_or_else(|| anyhow::anyhow!("y stream unavailable"))?;

        // Spawn dummy MQTT publisher nodes and keep handles to wait for completion
        let x_publisher_task = executor.spawn(dummy_mqtt_publisher(
            "x_publisher".to_string(),
            "mqtt_input_x".to_string(),
            xs,
            mqtt_port,
        ));

        let y_publisher_task = executor.spawn(dummy_mqtt_publisher(
            "y_publisher".to_string(),
            "mqtt_input_y".to_string(),
            ys,
            mqtt_port,
        ));

        let xs_fut = with_timeout(
            x_stream.take(expected.len()).collect::<Vec<_>>(),
            10,
            "x_stream.take",
        );
        let ys_fut = with_timeout(
            y_stream.take(expected.len()).collect::<Vec<_>>(),
            10,
            "y_stream.take",
        );
        let (x_vals, y_vals) = futures::try_join!(xs_fut, ys_fut)?;
        let received: Vec<_> = x_vals.into_iter().zip(y_vals.into_iter()).collect();

        info!("Received (x, y): {:?}", received);
        assert_eq!(received, expected);

        // Wait for publishers to complete and then shutdown MQTT server to terminate connections
        info!("Waiting for publishers to complete...");
        with_timeout(x_publisher_task, 5, "x_publisher_task").await?;
        with_timeout(y_publisher_task, 5, "y_publisher_task").await?;
        info!("All publishers completed, shutting down MQTT server");

        Ok(())
    }

    #[apply(async_test)]
    async fn test_reconf_mqtt_reconf_during_run(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        // In this test, we first send xs then swap to ys:
        let xs_expected = vec![Value::Int(1), Value::Int(2)];
        let ys_expected = vec![Value::Int(3), Value::Int(4)];

        // Note that even if this stream ends, then the InputProvider does not know
        // that it has ended
        let xs: OutputStream<Value> = futures::stream::iter(xs_expected.clone()).boxed();

        // We need control over when y is being published
        let (mut y_tick, ys): (_, OutputStream<Value>) =
            tick_stream(futures::stream::iter(ys_expected.clone()).boxed());

        let mqtt_server = start_mqtt().await;

        let mqtt_port = with_timeout_res(
            TokioCompat::new(mqtt_server.get_host_port_ipv4(1883)),
            5,
            "get_host_port",
        )
        .await
        .expect("Failed to get host port for MQTT server");

        let initial_var_topics: VarTopicMap = [("x".into(), "mqtt_input_x".to_string())]
            .into_iter()
            .collect();
        let final_var_topics: VarTopicMap = [("y".into(), "mqtt_input_y".to_string())]
            .into_iter()
            .collect();

        // Reconfiguration stream that first waits for a tick, then yields topics, then waits forever
        let reconf_stream: OutputStream<VarTopicMap> =
            stream! {yield final_var_topics; futures::future::pending::<()>().await; }
                .boxed_local();
        let (mut r_tick, reconf_stream) = tick_stream(reconf_stream);

        // Create the MQTT input provider
        let mut input_provider = ReconfMQTTInputProvider::new(
            executor.clone(),
            "localhost",
            Some(mqtt_port),
            initial_var_topics,
            0,
            reconf_stream,
        );
        with_timeout_res(input_provider.connect(), 5, "input_provider_connect").await?;

        let input_provider_ready = input_provider.ready();

        executor.spawn(input_provider.run()).detach();
        with_timeout_res(input_provider_ready, 5, "input_provider_ready").await?;

        let x_stream = input_provider
            .input_stream(&"x".into())
            .ok_or_else(|| anyhow::anyhow!("x stream unavailable"))?;

        // Spawn dummy MQTT publisher nodes and keep handles to wait for completion
        let x_publisher_task = executor.spawn(dummy_stream_mqtt_publisher(
            "x_publisher".to_string(),
            "mqtt_input_x".to_string(),
            xs,
            xs_expected.len(),
            mqtt_port,
        ));

        // Not publishing until ticks arrive
        let y_publisher_task = executor.spawn(dummy_stream_mqtt_publisher(
            "y_publisher".to_string(),
            "mqtt_input_y".to_string(),
            ys,
            ys_expected.len(),
            mqtt_port,
        ));
        let x_vals = with_timeout(
            x_stream.take(xs_expected.len()).collect::<Vec<_>>(),
            5,
            "x_stream.take",
        )
        .await?;
        assert_eq!(x_vals, xs_expected);

        r_tick.send(()).await.unwrap(); // Trigger reconf to y
        //
        // Wait for reconf request to be processed.
        // (Without this, the outcome depends on how the Executor happens to run things)
        smol::Timer::after(std::time::Duration::from_secs(1)).await;

        let y_stream = input_provider
            .input_stream(&"y".into())
            .ok_or_else(|| anyhow::anyhow!("y stream unavailable"))?;

        for _ in 0..ys_expected.len() + 1 {
            // Send all the ys and +1 to let the stream complete
            y_tick.send(()).await?;
        }

        let y_vals = with_timeout(
            y_stream.take(ys_expected.len()).collect::<Vec<_>>(),
            5,
            "y_stream.take",
        )
        .await?;
        assert_eq!(y_vals, ys_expected);

        // Wait for publishers to complete and then shutdown MQTT server to terminate connections
        info!("Waiting for publishers to complete...");
        with_timeout(x_publisher_task, 5, "x_publisher_task").await?;
        with_timeout(y_publisher_task, 5, "y_publisher_task").await?;
        info!("All publishers completed, shutting down MQTT server");

        Ok(())
    }
}
