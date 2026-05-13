use std::{collections::BTreeMap, mem, rc::Rc};

use anyhow::Context;
use futures::{
    FutureExt, StreamExt,
    future::{LocalBoxFuture, join_all},
};
use redis::{AsyncTypedCommands, aio::MultiplexedConnection};
use smol::LocalExecutor;
use tracing::{debug, info};
use unsync::oneshot::{Receiver as OSReceiver, Sender as OSSender};

use crate::{
    OutputStream, Value, VarName,
    core::OutputHandler,
    utils::cancellation_token::{CancellationToken, DropGuard},
};

async fn publish_stream(
    topic_name: String,
    mut stream: OutputStream<Value>,
    mut con: MultiplexedConnection,
    cancellation_token: CancellationToken,
) -> anyhow::Result<()> {
    let mut cancelled = cancellation_token.cancelled().fuse();
    loop {
        futures::select_biased! {
            _ = cancelled => {
                return Ok(());
            },
            data = stream.next().fuse() => {
                let Some(data) = data else {
                    return Ok(());
                };
                if data == Value::NoVal {
                    continue;
                }

                let data = serde_json::to_string(&data).unwrap();
                con.publish(topic_name.clone(), data.clone())
                    .await
                    .context("Failed to publish output message")?;
            }
        }
    }
}

async fn drain_stream(
    mut stream: OutputStream<Value>,
    cancellation_token: CancellationToken,
) -> anyhow::Result<()> {
    let mut cancelled = cancellation_token.cancelled().fuse();
    loop {
        futures::select_biased! {
            _ = cancelled => {
                return Ok(());
            },
            data = stream.next().fuse() => {
                if data.is_none() {
                    return Ok(());
                }
            }
        }
    }
}

pub struct VarData {
    pub variable: VarName,
    pub topic_name: String,
    stream: Option<OutputStream<Value>>,
}

// A map between channel names and the Redis topics they
// correspond to
pub type OutputChannelMap = BTreeMap<VarName, String>;

pub struct RedisOutputHandler {
    pub var_map: BTreeMap<VarName, VarData>,
    pub hostname: String,
    pub port: Option<u16>,
    pub aux_info: Vec<VarName>,
    pub uri: String,
    executor: Rc<LocalExecutor<'static>>,
    cancellation_drop_guard: DropGuard,
    client_tx: Option<OSSender<redis::Client>>,

    client_rx: Option<OSReceiver<redis::Client>>,
    connected: bool,
}

impl OutputHandler for RedisOutputHandler {
    type Val = Value;

    fn provide_streams(&mut self, streams: BTreeMap<VarName, OutputStream<Value>>) {
        for (var, stream) in streams {
            let var_data = self.var_map.get_mut(&var).expect("Variable not found");
            var_data.stream = Some(stream);
        }
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let streams = self
            .var_map
            .iter_mut()
            .map(|(_, var_data)| {
                let var_name = var_data.variable.clone();
                let channel_name = var_data.topic_name.clone();
                let stream = mem::take(&mut var_data.stream).expect("Stream not found");
                (var_name, channel_name, stream)
            })
            .collect::<Vec<_>>();
        let client_rx = mem::take(&mut self.client_rx)
            .expect("Redis output handler client receiver already taken");
        let aux_info = self.aux_info.clone();
        let executor = self.executor.clone();
        let cancellation_token = self.cancellation_drop_guard.clone_tok();
        let connected = self.connected;

        info!(?self.hostname, num_streams = ?streams.len(), "OutputProvider Redis startup task launched");

        Box::pin(async move {
            if !connected {
                return Err(anyhow::anyhow!(
                    "RedisOutputHandler not connected before run"
                ));
            }

            let client = client_rx.await.ok_or_else(|| {
                anyhow::anyhow!("Failed to receive Redis client for output handler")
            })?;
            RedisOutputHandler::inner_handler(
                client,
                streams,
                aux_info,
                executor,
                cancellation_token,
            )
            .await
        })
    }
}

impl RedisOutputHandler {
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        hostname: &str,
        port: Option<u16>,
        var_topics: OutputChannelMap,
        aux_info: Vec<VarName>,
    ) -> Result<Self, anyhow::Error> {
        let hostname = hostname.to_string();
        let uri = match port {
            Some(p) => format!("redis://{}:{}", hostname, p),
            None => format!("redis://{}", hostname),
        };
        let (client_tx, client_rx) = unsync::oneshot::channel();
        let cancellation_drop_guard = CancellationToken::new().drop_guard();

        let var_map = var_topics
            .into_iter()
            .map(|(var, topic_name)| {
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

        Ok(RedisOutputHandler {
            var_map,
            hostname,
            port,
            aux_info,
            uri,
            executor,
            cancellation_drop_guard,
            client_tx: Some(client_tx),

            client_rx: Some(client_rx),
            connected: false,
        })
    }

    pub async fn connect(&mut self) -> anyhow::Result<()> {
        info!(?self.uri, "Starting Redis output handler connection");
        let client_tx = mem::take(&mut self.client_tx)
            .expect("Redis output handler client sender already taken");
        let client = redis::Client::open(self.uri.clone())?;
        client_tx
            .send(client)
            .map_err(|_| anyhow::anyhow!("Failed to send Redis client to output handler run"))?;
        self.connected = true;
        Ok(())
    }

    async fn inner_handler(
        client: redis::Client,
        streams: Vec<(VarName, String, OutputStream<Value>)>,
        aux_info: Vec<VarName>,
        executor: Rc<LocalExecutor<'static>>,
        cancellation_token: CancellationToken,
    ) -> anyhow::Result<()> {
        join_all(streams.into_iter().map(|(var_name, channel_name, stream)| {
            let aux_info = aux_info.clone();
            let client = client.clone();
            let executor = executor.clone();
            let cancellation_token = cancellation_token.clone();
            async move {
                if aux_info.contains(&var_name) {
                    executor
                        .spawn(async move {
                            if let Err(err) = drain_stream(stream, cancellation_token.clone()).await
                            {
                                debug!(?err, "Failed to drain Redis auxiliary stream");
                            }
                        })
                        .detach();
                    return Ok(());
                }

                let con = client.get_multiplexed_async_connection().await.unwrap();
                publish_stream(channel_name, stream, con, cancellation_token).await
            }
        }))
        .await
        .into_iter()
        .fold(Ok(()), |acc, res| match res {
            Ok(_) => acc,
            Err(e) => Err(e),
        })?;

        cancellation_token.cancel();
        Ok(())
    }
}
