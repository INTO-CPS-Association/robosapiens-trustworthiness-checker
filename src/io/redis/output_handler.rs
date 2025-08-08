use std::{collections::BTreeMap, mem, rc::Rc};

use anyhow::Context;
use futures::{
    StreamExt,
    future::{LocalBoxFuture, join_all},
};
use redis::{AsyncTypedCommands, aio::MultiplexedConnection};
use smol::LocalExecutor;
use tracing::{debug, info};

use crate::{OutputStream, Value, VarName, core::OutputHandler};

async fn publish_stream(
    topic_name: String,
    mut stream: OutputStream<Value>,
    mut con: MultiplexedConnection,
) -> anyhow::Result<()> {
    while let Some(data) = stream.next().await {
        let data = serde_json::to_string(&data).unwrap();
        con.publish(topic_name.clone(), data.clone())
            .await
            .context("Failed to publish output message")?;
    }

    Ok(())
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
    pub var_names: Vec<VarName>,
    pub var_map: BTreeMap<VarName, VarData>,
    pub hostname: String,
    pub port: Option<u16>,
}

impl OutputHandler for RedisOutputHandler {
    type Val = Value;

    fn var_names(&self) -> Vec<VarName> {
        self.var_names.clone()
    }

    fn provide_streams(&mut self, streams: Vec<OutputStream<Value>>) {
        for (var, stream) in self.var_names().iter().zip(streams.into_iter()) {
            let var_data = self.var_map.get_mut(var).expect("Variable not found");
            var_data.stream = Some(stream);
        }
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let streams = self
            .var_map
            .iter_mut()
            .map(|(_, var_data)| {
                let channel_name = var_data.topic_name.clone();
                let stream = mem::take(&mut var_data.stream).expect("Stream not found");
                (channel_name, stream)
            })
            .collect::<Vec<_>>();
        let hostname = self.hostname.clone();
        let port = self.port;
        info!(name: "OutputProvider MQTT startup task launched",
            ?hostname, num_streams = ?streams.len());

        Box::pin(RedisOutputHandler::inner_handler(hostname, port, streams))
    }
}

impl RedisOutputHandler {
    pub fn new(
        _executor: Rc<LocalExecutor<'static>>,
        var_names: Vec<VarName>,
        hostname: &str,
        port: Option<u16>,
        var_topics: OutputChannelMap,
    ) -> Result<Self, anyhow::Error> {
        let hostname = hostname.to_string();

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
            var_names,
            var_map,
            hostname,
            port,
        })
    }

    async fn inner_handler(
        hostname: String,
        port: Option<u16>,
        streams: Vec<(String, OutputStream<Value>)>,
    ) -> anyhow::Result<()> {
        debug!("Awaiting client creation");
        debug!("Client created");
        let url = match port {
            Some(p) => format!("redis://{}:{}", hostname, p),
            None => format!("redis://{}", hostname),
        };
        let client = redis::Client::open(url)?;

        join_all(streams.into_iter().map(|(channel_name, stream)| async {
            let con = client.get_multiplexed_async_connection().await.unwrap();
            publish_stream(channel_name, stream, con).await
        }))
        .await
        .into_iter()
        .fold(Ok(()), |acc, res| match res {
            Ok(_) => acc,
            Err(e) => Err(e),
        })?;

        Ok(())
    }
}
