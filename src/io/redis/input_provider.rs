use std::{collections::BTreeMap, error::Error};

use anyhow::anyhow;
use async_stream::stream;
use async_trait::async_trait;
use async_unsync::bounded::{self, Receiver, Sender};
use futures::{
    StreamExt,
    future::{self, LocalBoxFuture},
    stream,
};
use tracing::{info, warn};

use crate::{InputProvider, OutputStream, Value, VarName};

pub struct RedisInputProvider {
    client: redis::Client,
    redis_stream: Option<redis::aio::PubSubStream>,
    var_topics: BTreeMap<VarName, String>,
    var_streams: BTreeMap<VarName, OutputStream<Value>>,
    senders: BTreeMap<VarName, Sender<Value>>,
    connected: bool,
}

impl RedisInputProvider {
    pub fn new(
        hostname: &str,
        port: Option<u16>,
        var_topics: BTreeMap<VarName, String>,
    ) -> Result<RedisInputProvider, Box<dyn Error>> {
        let url = match port {
            Some(p) => format!("redis://{}:{}", hostname, p),
            None => format!("redis://{}", hostname),
        };

        let (senders, receivers): (BTreeMap<_, Sender<Value>>, BTreeMap<_, Receiver<Value>>) =
            var_topics
                .iter()
                .map(|(v, _)| {
                    let (tx, rx) = bounded::channel(10).into_split();
                    ((v.clone(), tx), (v.clone(), rx))
                })
                .unzip();

        let var_streams: BTreeMap<VarName, OutputStream<Value>> = receivers
            .into_iter()
            .map(|(v, mut rx)| {
                let stream: OutputStream<Value> = Box::pin(stream! {
                    while let Some(x) = rx.recv().await {
                        yield x
                    }
                });
                (v.clone(), stream)
            })
            .collect();

        let client = redis::Client::open(url.clone())?;

        Ok(RedisInputProvider {
            client: client,
            redis_stream: None,
            var_topics: var_topics,
            senders: senders,
            var_streams: var_streams,
            connected: false,
        })
    }

    pub async fn connect(&mut self) -> anyhow::Result<()> {
        let mut pubsub = self.client.get_async_pubsub().await?;
        let channel_names = self.var_topics.values().collect::<Vec<_>>();
        info!("Subscribing to Redis channel_names: {:?}", channel_names);
        pubsub.subscribe(channel_names).await?;
        let stream = pubsub.into_on_message();
        self.redis_stream = Some(stream);
        self.connected = true;
        Ok(())
    }

    async fn run_logic(
        var_topics: BTreeMap<VarName, String>,
        senders: BTreeMap<VarName, Sender<Value>>,
        redis_stream: redis::aio::PubSubStream,
        connected: bool,
    ) -> anyhow::Result<()> {
        let mut stream =
            Self::create_run_stream(var_topics, senders, redis_stream, connected).await;
        loop {
            let res = stream.next().await;
            match res {
                Some(Ok(())) => continue,
                Some(Err(e)) => {
                    warn!("Error in Redis input provider run stream: {}", e);
                    return Err(e);
                }
                None => {
                    info!("Redis input provider run stream ended");
                    return Ok(());
                }
            }
        }
    }

    async fn create_run_stream(
        var_topics: BTreeMap<VarName, String>,
        mut senders: BTreeMap<VarName, Sender<Value>>,
        mut redis_stream: redis::aio::PubSubStream,
        connected: bool,
    ) -> OutputStream<anyhow::Result<()>> {
        let topic_vars = var_topics
            .iter()
            .map(|(k, v)| (v.clone(), k.clone()))
            .collect::<BTreeMap<_, _>>();
        Box::pin(stream! {
                if !connected {
                    yield Err(anyhow!("RedisInputProvider not connected before waiting for data"));
                    return;
                }

                while let Some(msg) = redis_stream.next().await {
                    let var_name = match topic_vars.get(msg.get_channel_name()) {
                        Some(name) => name,
                        None => {
                            yield Err(anyhow!("Unknown channel name"));
                            return;
                        }
                    };
                    let value: Value = match msg.get_payload() {
                        Ok(val) => val,
                        Err(e) => {
                            yield Err(anyhow!("Failed to get payload: {}", e));
                            return;
                        }
                    };
                    if let Some(sender) = senders.get(var_name) {
                        if let Err(e) = sender.send(value).await {
                            yield Err(anyhow!("Failed to send value: {}", e));
                            return;
                        }
                    } else {
                        yield Err(anyhow!("Unknown sender for var: {}", var_name));
                        continue;
                    }
                    // Send `NoVal` to all other senders concurrently
                    let futs = senders
                        .iter_mut()
                        .filter(|(name, _)| *name != var_name)
                        .map(|(_, s)| s.send(Value::NoVal));
                    // Run them all concurrently
                    let results = future::join_all(futs).await;
                    // Check for errors
                    if results.iter().any(|r| r.is_err()) {
                        yield Err(anyhow!("Failed to send NoVal"));
                        return;
                    }
            }
        })
    }
}

#[async_trait(?Send)]
impl InputProvider for RedisInputProvider {
    type Val = Value;

    fn var_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        self.var_streams.remove(var)
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let stream = self.redis_stream.take();

        match stream {
            Some(stream) => Box::pin(Self::run_logic(
                self.var_topics.clone(),
                std::mem::take(&mut self.senders),
                stream,
                self.connected.clone(),
            )),
            None => Box::pin(future::ready(Err(anyhow!("Not connected to Redis yet")))),
        }
    }

    async fn control_stream(&mut self) -> OutputStream<anyhow::Result<()>> {
        let stream = self.redis_stream.take();

        match stream {
            Some(stream) => {
                Self::create_run_stream(
                    self.var_topics.clone(),
                    std::mem::take(&mut self.senders),
                    stream,
                    self.connected.clone(),
                )
                .await
            }
            None => Box::pin(stream::once(async {
                Err(anyhow!("Not connected to Redis yet"))
            })),
        }
    }

    fn vars(&self) -> Vec<VarName> {
        self.var_topics.keys().cloned().collect()
    }
}
