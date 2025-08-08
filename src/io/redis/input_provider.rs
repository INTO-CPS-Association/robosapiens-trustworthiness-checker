use std::{collections::BTreeMap, error::Error, rc::Rc};

use anyhow::anyhow;
use async_cell::unsync::AsyncCell;
use async_stream::stream;
use async_unsync::bounded::{self, Receiver, Sender};
use futures::{StreamExt, future::LocalBoxFuture};
use smol::{LocalExecutor, future::yield_now};

use crate::{InputProvider, OutputStream, Value, VarName};

pub struct VarData {
    pub variable: VarName,
    pub channel_name: String,
    stream: Option<OutputStream<Value>>,
}

pub struct RedisInputProvider {
    pub host: String,
    pub var_data: BTreeMap<VarName, VarData>,
    pub result: Rc<AsyncCell<anyhow::Result<()>>>,
    pub started: Rc<AsyncCell<bool>>,
}

impl RedisInputProvider {
    pub fn new(
        ex: Rc<LocalExecutor<'static>>,
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
        let topic_vars = var_topics
            .iter()
            .map(|(k, v)| (v.clone(), k.clone()))
            .collect::<BTreeMap<_, _>>();

        let started = AsyncCell::shared();

        let client = redis::Client::open(url.clone())?;

        let result = AsyncCell::shared();

        ex.spawn(RedisInputProvider::input_monitor(
            result.clone(),
            client,
            var_topics.clone(),
            topic_vars,
            senders,
            started.clone(),
        ))
        .detach();

        let var_data = var_topics
            .into_iter()
            .zip(receivers.into_values())
            .map(|((v, topic), mut rx)| {
                let stream = stream! {
                    while let Some(x) = rx.recv().await {
                        yield x
                    }
                };
                (
                    v.clone(),
                    VarData {
                        variable: v,
                        channel_name: topic,
                        stream: Some(Box::pin(stream)),
                    },
                )
            })
            .collect();

        Ok(RedisInputProvider {
            host: url,
            result,
            var_data,
            started,
        })
    }

    async fn input_monitor(
        result: Rc<AsyncCell<anyhow::Result<()>>>,
        client: redis::Client,
        var_topics: BTreeMap<VarName, String>,
        topic_vars: BTreeMap<String, VarName>,
        senders: BTreeMap<VarName, Sender<Value>>,
        started: Rc<AsyncCell<bool>>,
    ) {
        let result = result.guard_shared(Err(anyhow!("RedisInputProvider crashed")));
        result.set(
            RedisInputProvider::input_monitor_with_result(
                client, var_topics, topic_vars, senders, started,
            )
            .await,
        );
    }

    async fn input_monitor_with_result(
        client: redis::Client,
        var_topics: BTreeMap<VarName, String>,
        topic_vars: BTreeMap<String, VarName>,
        senders: BTreeMap<VarName, Sender<Value>>,
        started: Rc<AsyncCell<bool>>,
    ) -> anyhow::Result<()> {
        let mut pubsub = client.get_async_pubsub().await?;
        let channel_names = var_topics.values().collect::<Vec<_>>();
        pubsub.subscribe(channel_names).await?;
        started.set(true);
        let mut stream = pubsub.on_message();

        while let Some(msg) = stream.next().await {
            let var_name = topic_vars
                .get(msg.get_channel_name())
                .ok_or_else(|| anyhow!("Unknown channel name"))?;
            let value: Value = msg.get_payload()?;

            let sender = senders
                .get(var_name)
                .ok_or_else(|| anyhow!("Unknown sender"))?;
            sender.send(value).await?;
        }

        Ok(())
    }
}

impl InputProvider for RedisInputProvider {
    type Val = Value;

    fn input_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        let var_data = self.var_data.get_mut(var)?;
        let stream = var_data.stream.take()?;
        Some(stream)
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.result.take_shared())
    }

    fn ready(&self) -> LocalBoxFuture<'static, Result<(), anyhow::Error>> {
        let started = self.started.clone();
        Box::pin(async move {
            while !started.get().await {
                yield_now().await;
            }
            Ok(())
        })
    }
}
