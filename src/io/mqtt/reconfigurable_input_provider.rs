use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use async_cell::unsync::AsyncCell;
use futures::{FutureExt, StreamExt, future::LocalBoxFuture};
use smol::LocalExecutor;
use tracing::{Level, debug, info, info_span, instrument, warn};
use unsync::oneshot::Receiver as OSReceiver;
use unsync::spsc::Sender as SpscSender;

use crate::{
    InputProvider, OutputStream, Value,
    core::VarName,
    io::mqtt::{MqttClient, MqttFactory, MqttMessage},
    utils::cancellation_token::CancellationToken,
};

use super::common_input_provider::common;

pub struct ReconfMQTTInputProvider {
    base: common::Base,

    // Streams that can be taken ownership of by calling `input_stream`
    // (Note: Can't use AsyncCell because it is also used outside async context)
    available_streams: Rc<RefCell<BTreeMap<VarName, OutputStream<Value>>>>,

    reconfig: Option<OutputStream<common::VarTopicMap>>,
}

impl ReconfMQTTInputProvider {
    #[instrument(level = Level::INFO, skip(var_topics, reconfig))]
    pub fn new(
        _executor: Rc<LocalExecutor<'static>>,
        factory: MqttFactory,
        host: &str,
        port: Option<u16>,
        var_topics: common::VarTopicMap,
        max_reconnect_attempts: u32,
        reconfig: OutputStream<common::VarTopicMap>,
    ) -> Self {
        let (available_streams, base) =
            common::Base::new(factory, host, port, var_topics, max_reconnect_attempts);

        let available_streams = Rc::new(RefCell::new(available_streams));
        let reconfig = Some(reconfig);

        Self {
            base,
            available_streams,
            reconfig,
        }
    }

    pub async fn connect(&mut self) -> anyhow::Result<()> {
        self.base.connect().await
    }

    /// Update the `available_streams` based on the new variable-topic mapping and return the new senders.
    fn update_available_streams(
        var_topics_inverse: &common::InverseVarTopicMap,
        available_streams: Rc<RefCell<BTreeMap<VarName, OutputStream<Value>>>>,
    ) -> BTreeMap<VarName, SpscSender<Value>> {
        let (senders, receivers) =
            common::Base::create_senders_receiver(var_topics_inverse.iter().map(|(t, v)| (v, t)));

        *available_streams.borrow_mut() = receivers;
        senders
    }

    /// Handle reconfiguration by updating MQTT subscriptions, `available_streams`, and returning
    /// new senders.
    async fn handle_reconfiguration(
        client: &Box<dyn MqttClient>,
        new_var_topics: common::VarTopicMap,
        var_topics_inverse: common::InverseVarTopicMap,
        available_streams: Rc<RefCell<BTreeMap<VarName, OutputStream<Value>>>>,
    ) -> (
        common::InverseVarTopicMap,
        BTreeMap<VarName, SpscSender<Value>>,
    ) {
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
                        info!(?to_unsubscribe, "Unsubscribed from old topics");
                        break;
                    }
                    Err(e) => {
                        warn!(?to_unsubscribe, err=?e, "Failed to unsubscribe from old topics");
                        smol::Timer::after(std::time::Duration::from_millis(100)).await;
                        info!("Retrying unsubscribing to MQTT topics");
                        let _e = client.reconnect().await;
                    }
                }
            }
        }
        // Subscribe to new topics
        let qos = vec![common::QOS; to_subscribe.len()];
        loop {
            match client.subscribe_many(&to_subscribe, &qos).await {
                Ok(_) => {
                    info!(?to_subscribe, "Subscribed to new topics");
                    break;
                }
                Err(e) => {
                    warn!(?to_subscribe, err=?e, "Failed to subscribe to new topics");
                    smol::Timer::after(std::time::Duration::from_millis(100)).await;
                    info!("Retrying subscribing to MQTT topics");
                    let _e = client.reconnect().await;
                }
            }
        }
        let var_topics_inverse = new_var_topics
            .into_iter()
            .map(|(var, top)| (top, var))
            .collect::<common::InverseVarTopicMap>();
        let senders = Self::update_available_streams(&var_topics_inverse, available_streams);
        (var_topics_inverse, senders)
    }

    async fn run_logic(
        var_topics: BTreeMap<VarName, String>,
        mut senders: BTreeMap<VarName, SpscSender<Value>>,
        available_streams: Rc<RefCell<BTreeMap<VarName, OutputStream<Value>>>>,
        started: Rc<AsyncCell<bool>>,
        cancellation_token: CancellationToken,
        client_streams_rx: OSReceiver<(Box<dyn MqttClient>, OutputStream<MqttMessage>)>,
        mut reconfig: OutputStream<common::VarTopicMap>,
    ) -> anyhow::Result<()> {
        let mqtt_input_span = info_span!("ReconfMQTTInputProvider run_logic");
        let _enter = mqtt_input_span.enter();
        info!("run_logic started");

        let mut prev_var_topics_inverse = common::InverseVarTopicMap::new();
        let (client, mut mqtt_stream, mut var_topics_inverse) =
            common::Base::initial_run_logic(var_topics.clone(), started.clone(), client_streams_rx)
                .await?;

        let result = async {
            loop {
                // Notably: Handle new configs before receiving data
                futures::select_biased! {
                    new_config = reconfig.next().fuse() => {
                        match new_config {
                            Some(new_var_topics) => {
                                info!(?new_var_topics, "Reconfiguring MQTTInputProvider with new variable-topic mapping");
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
                                common::Base::handle_mqtt_message(msg, &var_topics_inverse, &mut senders, Some(&prev_var_topics_inverse)).await?;
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
        let _ = client.disconnect().await;

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
        let reconfig = self.reconfig.take().expect("Reconfig stream already taken");

        Box::pin(Self::run_logic(
            self.base.var_topics.clone(),
            self.base.take_senders(),
            self.available_streams.clone(),
            self.base.started.clone(),
            self.base.drop_guard.clone_tok(),
            self.base.take_client_streams_rx(),
            reconfig,
        ))
    }

    fn ready(&self) -> LocalBoxFuture<'static, Result<(), anyhow::Error>> {
        self.base.ready()
    }

    fn vars(&self) -> Vec<VarName> {
        self.base.vars()
    }
}

#[cfg(test)]
mod integration_tests {
    use async_stream::stream;
    use futures::StreamExt;
    use futures::stream;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::vec;
    use std::{collections::BTreeMap, rc::Rc};
    use tracing::info;

    use super::super::common_input_provider::common;
    use crate::InputProvider;
    use crate::OutputStream;
    use crate::async_test;
    use crate::io::mqtt::{MqttFactory, ReconfMQTTInputProvider};
    use crate::{Value, VarName};
    use std::any::Any;
    use tc_testutils::mqtt::dummy_stream_mqtt_publisher;
    use tc_testutils::streams::{
        TickSender, interleave_with_constant, receive_values_serially, tick_stream, with_timeout,
        with_timeout_res,
    };

    // TODO: These tests should live with MQTT tests

    const MQTT_FACTORY: MqttFactory = if cfg!(feature = "testcontainers") {
        MqttFactory::Paho
    } else {
        MqttFactory::Mock
    };

    async fn start_mqtt_get_port() -> (Box<dyn Any>, u16) {
        #[cfg(feature = "testcontainers")]
        {
            use async_compat::Compat as TokioCompat;
            use tc_testutils::mqtt::start_mqtt;

            let mqtt_server = start_mqtt().await;
            let port = with_timeout_res(
                TokioCompat::new(mqtt_server.get_host_port_ipv4(1883)),
                5,
                "get_host_port",
            )
            .await
            .expect("Failed to get host port for MQTT server");

            (Box::new(mqtt_server), port)
        }
        #[cfg(not(feature = "testcontainers"))]
        {
            // Mock tests do not care about a client, but they need to run serially,
            // forced with this lock. (Due to file writing)
            async fn lock_test() -> futures::lock::MutexGuard<'static, ()> {
                static TEST_MUTEX: std::sync::OnceLock<futures::lock::Mutex<()>> =
                    std::sync::OnceLock::new();
                TEST_MUTEX
                    .get_or_init(|| futures::lock::Mutex::new(()))
                    .lock()
                    .await
            }

            use rand::Rng;
            let port = rand::rng().random();
            let lock = lock_test().await;
            (Box::new(lock), port)
        }
    }

    fn generate_test_publisher_tasks(
        executor: Rc<LocalExecutor<'static>>,
        xs: Vec<Value>,
        ys: Vec<Value>,
        mqtt_port: u16,
    ) -> (
        (TickSender, smol::Task<anyhow::Result<()>>),
        (TickSender, smol::Task<anyhow::Result<()>>),
    ) {
        let (x_tick, x_pub_stream) = tick_stream(stream::iter(xs.clone()).boxed());
        let (y_tick, y_pub_stream) = tick_stream(stream::iter(ys.clone()).boxed());

        // Spawn dummy MQTT publisher nodes and keep handles to wait for completion
        let x_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_publisher(
                "x_publisher".to_string(),
                "mqtt_input_x".to_string(),
                x_pub_stream,
                xs.len(),
                mqtt_port,
            ),
            5,
            "x_publisher_task",
        ));

        let y_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_publisher(
                "y_publisher".to_string(),
                "mqtt_input_y".to_string(),
                y_pub_stream,
                ys.len(),
                mqtt_port,
            ),
            5,
            "y_publisher_task",
        ));
        ((x_tick, x_publisher_task), (y_tick, y_publisher_task))
    }

    #[apply(async_test)]
    async fn test_reconf_mqtt_no_reconf(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let xs = vec![Value::Int(1), Value::Int(2)];
        let ys = vec![Value::Int(3), Value::Int(4)];
        let stream_len = xs.len();

        let (_mqtt_server, mqtt_port) = start_mqtt_get_port().await;

        let var_topics = [
            ("x".into(), "mqtt_input_x".to_string()),
            ("y".into(), "mqtt_input_y".to_string()),
        ]
        .into_iter()
        .collect::<BTreeMap<VarName, _>>();

        // Empty reconfiguration stream that never ends:
        let reconf_stream: OutputStream<common::VarTopicMap> =
            futures::stream::pending().boxed_local();

        // Create the MQTT input provider
        let mut input_provider = ReconfMQTTInputProvider::new(
            executor.clone(),
            MQTT_FACTORY,
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

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), ys.clone(), mqtt_port);

        let (x_vals, y_vals) =
            receive_values_serially(&mut x_tick, &mut y_tick, x_stream, y_stream, stream_len)
                .await?;

        let exp_iter = xs.clone().into_iter().zip(ys.clone().into_iter());
        let (exp_x_vals, exp_y_vals) = interleave_with_constant(exp_iter, Value::NoVal);
        info!(?x_vals, ?y_vals, "Received values");
        assert_eq!(x_vals, exp_x_vals);
        assert_eq!(y_vals, exp_y_vals);

        // Final ticks to let them complete
        x_tick.send(()).await?;
        y_tick.send(()).await?;
        // Wait for publishers to complete and then shutdown MQTT server to terminate connections
        info!("Waiting for publishers to complete...");
        x_publisher_task.await?;
        y_publisher_task.await?;
        info!("All publishers completed, shutting down MQTT server");

        Ok(())
    }

    #[apply(async_test)]
    async fn test_reconf_mqtt_reconf_before_run(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let xs = vec![Value::Int(1), Value::Int(2)];
        let ys = vec![Value::Int(3), Value::Int(4)];

        let (_mqtt_server, mqtt_port) = start_mqtt_get_port().await;

        let wrong_var_topics: common::VarTopicMap = [
            ("xx".into(), "mqtt_input_xx".to_string()),
            ("yy".into(), "mqtt_input_yy".to_string()),
        ]
        .into_iter()
        .collect();
        let var_topics: common::VarTopicMap = [
            ("x".into(), "mqtt_input_x".to_string()),
            ("y".into(), "mqtt_input_y".to_string()),
        ]
        .into_iter()
        .collect();

        // Reconfiguration stream that first yields topics, then waits forever
        let reconf_stream: OutputStream<common::VarTopicMap> =
            stream! {yield var_topics; futures::future::pending::<()>().await; }.boxed_local();

        // Create the MQTT input provider
        let mut input_provider = ReconfMQTTInputProvider::new(
            executor.clone(),
            MQTT_FACTORY,
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

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), ys.clone(), mqtt_port);

        let (x_vals, y_vals) =
            receive_values_serially(&mut x_tick, &mut y_tick, x_stream, y_stream, xs.len()).await?;

        let exp_iter = xs.clone().into_iter().zip(ys.clone().into_iter());
        let (exp_x_vals, exp_y_vals) = interleave_with_constant(exp_iter, Value::NoVal);
        info!(?x_vals, ?y_vals, "Received values");
        assert_eq!(x_vals, exp_x_vals);
        assert_eq!(y_vals, exp_y_vals);

        // Final ticks to let them complete
        x_tick.send(()).await?;
        y_tick.send(()).await?;
        // Wait for publishers to complete and then shutdown MQTT server to terminate connections
        info!("Waiting for publishers to complete...");
        x_publisher_task.await?;
        y_publisher_task.await?;
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

        // We need control over when x and y is being published

        let (_mqtt_server, mqtt_port) = start_mqtt_get_port().await;

        let initial_var_topics: common::VarTopicMap = [("x".into(), "mqtt_input_x".to_string())]
            .into_iter()
            .collect();
        let final_var_topics: common::VarTopicMap = [("y".into(), "mqtt_input_y".to_string())]
            .into_iter()
            .collect();

        // Reconfiguration stream that first waits for a tick, then yields topics, then waits forever
        let reconf_stream: OutputStream<common::VarTopicMap> =
            stream! {yield final_var_topics; futures::future::pending::<()>().await; }
                .boxed_local();
        let (mut r_tick, reconf_stream) = tick_stream(reconf_stream);

        // Create the MQTT input provider
        let mut input_provider = ReconfMQTTInputProvider::new(
            executor.clone(),
            MQTT_FACTORY,
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

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(
                executor.clone(),
                xs_expected.clone(),
                ys_expected.clone(),
                mqtt_port,
            );

        for _ in 0..xs_expected.len() {
            x_tick.send(()).await?;
        }
        let x_vals = with_timeout(
            x_stream.take(xs_expected.len()).collect::<Vec<_>>(),
            5,
            "x_stream.take",
        )
        .await?;
        assert_eq!(x_vals, xs_expected);

        r_tick.send(()).await?; // Trigger reconf to y

        // Wait for reconf request to be processed.
        // (Without this, the outcome depends on how the Executor happens to run things)
        smol::Timer::after(std::time::Duration::from_millis(200)).await;

        let y_stream = input_provider
            .input_stream(&"y".into())
            .ok_or_else(|| anyhow::anyhow!("y stream unavailable"))?;

        for _ in 0..ys_expected.len() {
            y_tick.send(()).await?;
        }
        let y_vals = with_timeout(
            y_stream.take(ys_expected.len()).collect::<Vec<_>>(),
            5,
            "y_stream.take",
        )
        .await?;
        assert_eq!(y_vals, ys_expected);

        // Final ticks to let them complete
        x_tick.send(()).await?;
        y_tick.send(()).await?;
        // Wait for publishers to complete and then shutdown MQTT server to terminate connections
        info!("Waiting for publishers to complete...");
        x_publisher_task.await?;
        y_publisher_task.await?;
        info!("All publishers completed, shutting down MQTT server");

        Ok(())
    }
}
