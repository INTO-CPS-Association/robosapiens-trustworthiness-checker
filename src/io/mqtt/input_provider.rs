use std::{collections::BTreeMap, rc::Rc};

use async_cell::unsync::AsyncCell;
use futures::{FutureExt, StreamExt, future::LocalBoxFuture};
use smol::LocalExecutor;
use tracing::{Level, debug, info_span, instrument, warn};

use unsync::oneshot::Receiver as OSReceiver;
use unsync::spsc::Sender as SpscSender;

use crate::{
    InputProvider, OutputStream, Value,
    core::VarName,
    io::mqtt::{MqttClient, MqttFactory, MqttMessage},
    utils::cancellation_token::CancellationToken,
};

use super::common_input_provider::common;

// TODO: Add support for changing the channels being monitored over time
pub struct MQTTInputProvider {
    base: common::Base,

    // Streams that can be taken ownership of by calling `input_stream`
    available_streams: BTreeMap<VarName, OutputStream<Value>>,
}

impl MQTTInputProvider {
    #[instrument(level = Level::INFO, skip(var_topics))]
    pub fn new(
        _executor: Rc<LocalExecutor<'static>>,
        factory: MqttFactory,
        host: &str,
        port: Option<u16>,
        var_topics: common::VarTopicMap,
        max_reconnect_attempts: u32,
    ) -> Self {
        let (available_streams, base) =
            common::Base::new(factory, host, port, var_topics, max_reconnect_attempts);

        Self {
            base,
            available_streams,
        }
    }

    pub async fn connect(&mut self) -> anyhow::Result<()> {
        self.base.connect().await
    }

    async fn run_logic(
        var_topics: BTreeMap<VarName, String>,
        mut senders: BTreeMap<VarName, SpscSender<Value>>,
        started: Rc<AsyncCell<bool>>,
        cancellation_token: CancellationToken,
        client_streams_rx: OSReceiver<(Box<dyn MqttClient>, OutputStream<MqttMessage>)>,
    ) -> anyhow::Result<()> {
        let mqtt_input_span = info_span!("MQTTInputProvider run_logic");
        let _enter = mqtt_input_span.enter();

        let (client, mut mqtt_stream, var_topics_inverse) =
            common::Base::initial_run_logic(var_topics, started.clone(), client_streams_rx).await?;

        let result = async {
            loop {
                futures::select! {
                    msg = mqtt_stream.next().fuse() => {
                        match msg {
                            Some(msg) => {
                                common::Base::handle_mqtt_message(msg, &var_topics_inverse, &mut senders, None).await?;
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

impl InputProvider for MQTTInputProvider {
    type Val = Value;

    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        // Take ownership of the stream for the variable, if it exists
        self.available_streams.remove(var)
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(Self::run_logic(
            self.base.var_topics.clone(),
            self.base.take_senders(),
            self.base.started.clone(),
            self.base.drop_guard.clone_tok(),
            self.base.take_client_streams_rx(),
        ))
    }

    fn ready(&self) -> LocalBoxFuture<'static, Result<(), anyhow::Error>> {
        self.base.ready()
    }

    fn vars(&self) -> Vec<VarName> {
        self.base.vars()
    }
}
