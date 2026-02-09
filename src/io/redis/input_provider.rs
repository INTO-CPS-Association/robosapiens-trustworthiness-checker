use std::{collections::BTreeMap, error::Error, rc::Rc};

use anyhow::anyhow;
use async_cell::unsync::AsyncCell;
use async_stream::stream;
use async_unsync::bounded::{self, Receiver, Sender};
use futures::{
    StreamExt,
    future::{self, LocalBoxFuture},
};
use tracing::{info, warn};

use crate::{InputProvider, OutputStream, Value, VarName};

pub struct RedisInputProvider {
    pub started: Rc<AsyncCell<bool>>,
    client: redis::Client,
    redis_stream: Option<redis::aio::PubSubStream>,
    var_topics: BTreeMap<VarName, String>,
    var_streams: BTreeMap<VarName, OutputStream<Value>>,
    senders: BTreeMap<VarName, Sender<Value>>,
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

        let started = AsyncCell::shared();

        let client = redis::Client::open(url.clone())?;

        Ok(RedisInputProvider {
            started,
            client: client,
            redis_stream: None,
            var_topics: var_topics,
            senders: senders,
            var_streams: var_streams,
        })
    }

    pub async fn connect(&mut self) -> anyhow::Result<()> {
        let mut pubsub = self.client.get_async_pubsub().await?;
        let channel_names = self.var_topics.values().collect::<Vec<_>>();
        info!("Subscribing to Redis channel_names: {:?}", channel_names);
        pubsub.subscribe(channel_names).await?;
        let stream = pubsub.into_on_message();
        self.redis_stream = Some(stream);
        self.started.set(true);
        Ok(())
    }

    async fn run_logic(
        var_topics: BTreeMap<VarName, String>,
        mut senders: BTreeMap<VarName, Sender<Value>>,
        mut redis_stream: redis::aio::PubSubStream,
    ) -> anyhow::Result<()> {
        let topic_vars = var_topics
            .iter()
            .map(|(k, v)| (v.clone(), k.clone()))
            .collect::<BTreeMap<_, _>>();
        while let Some(msg) = redis_stream.next().await {
            let var_name = topic_vars
                .get(msg.get_channel_name())
                .ok_or_else(|| anyhow!("Unknown channel name"))?;
            let value: Value = msg.get_payload()?;

            let sender = senders
                .get(var_name)
                .ok_or_else(|| anyhow!("Unknown sender"))?;
            sender.send(value).await?;

            // Send `NoVal` to all other senders concurrently
            let futs = senders
                .iter_mut()
                .filter(|(name, _)| *name != var_name)
                .map(|(_, s)| s.send(Value::NoVal));

            // Run them all concurrently
            let results = future::join_all(futs).await;

            // Check for errors
            if results.iter().any(|r| r.is_err()) {
                anyhow::bail!("Failed to send NoVal");
            }
        }

        Ok(())
    }
}

impl InputProvider for RedisInputProvider {
    type Val = Value;

    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        self.var_streams.remove(var)
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let stream = self.redis_stream.take();

        match stream {
            Some(stream) => Box::pin(Self::run_logic(
                self.var_topics.clone(),
                std::mem::take(&mut self.senders),
                stream,
            )),
            None => Box::pin(future::ready(Err(anyhow!("Not connected to Redis yet")))),
        }
    }

    fn ready(&self) -> LocalBoxFuture<'static, Result<(), anyhow::Error>> {
        let started = self.started.clone();
        Box::pin(async move {
            info!("Checking if Redis input provider is ready");
            let mut attempts = 0;
            while !started.get().await {
                attempts += 1;
                info!(
                    "Redis input provider not ready yet, checking again (attempt #{})",
                    attempts
                );
                smol::Timer::after(std::time::Duration::from_millis(100)).await;

                if attempts > 50 {
                    warn!(
                        "Redis input provider still not ready after 5 seconds, continuing to wait"
                    );
                    attempts = 0;
                }
            }
            info!("Redis input provider is ready");
            Ok(())
        })
    }

    fn vars(&self) -> Vec<VarName> {
        self.var_topics.keys().cloned().collect()
    }
}
