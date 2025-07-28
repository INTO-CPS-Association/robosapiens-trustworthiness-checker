use std::{collections::BTreeMap, rc::Rc};

use async_compat::Compat as TokioCompat;
use async_stream::stream;
use async_unsync::bounded::{self, Receiver, Sender};
use async_unsync::oneshot::{self};
use futures::future::LocalBoxFuture;
use redis::AsyncTypedCommands;
use smol::LocalExecutor;
use testcontainers_modules::{redis::Redis, testcontainers::runners::AsyncRunner};
use tracing::{debug, instrument};
use trustworthiness_checker::{OutputStream, Value};

use crate::testcontainers::ContainerAsync;

#[instrument(level = tracing::Level::INFO)]
pub async fn start_redis() -> ContainerAsync<Redis> {
    ContainerAsync::new(
        TokioCompat::new(Redis::default().start())
            .await
            .expect("Failed to start Redis test container"),
    )
}

pub async fn dummy_redis_sender(
    host: &str,
    port: Option<u16>,
    channel: String,
    messages: Vec<Value>,
    ready_rx: LocalBoxFuture<'static, ()>,
) -> anyhow::Result<()> {
    let uri = match port {
        Some(port) => format!("redis://{}:{}", host, port),
        None => format!("redis://{}", host),
    };
    let client = redis::Client::open(uri)?;
    let mut con = client.get_multiplexed_async_connection().await?;

    // Wait for receiver to be ready
    let _ = ready_rx.await;

    for message in messages.into_iter() {
        debug!(name: "Publishing message", ?message, ?channel);
        con.publish(&channel, message).await?;
    }

    Ok(())
}

pub async fn dummy_redis_receiver(
    executor: Rc<LocalExecutor<'static>>,
    host: &str,
    port: Option<u16>,
    channels: Vec<String>,
    ready_tx: oneshot::Sender<()>,
) -> anyhow::Result<Vec<OutputStream<Value>>> {
    let uri = match port {
        Some(port) => format!("redis://{}:{}", host, port),
        None => format!("redis://{}", host),
    };
    let client = redis::Client::open(uri)?;
    let mut con = client.get_async_pubsub().await?;
    con.subscribe(&channels).await?;

    // Signal that we're ready to receive messages
    let _ = ready_tx.send(());

    let (senders, receivers): (Vec<Sender<Value>>, Vec<Receiver<Value>>) = channels
        .iter()
        .map(|_| bounded::channel(10).into_split())
        .unzip();
    let mut senders: BTreeMap<String, Sender<Value>> =
        channels.iter().cloned().zip(senders.into_iter()).collect();
    let outputs = receivers
        .into_iter()
        .map(|mut rx| {
            Box::pin(stream! {
                while let Some(x) = rx.recv().await {
                    yield x
                }
            }) as OutputStream<Value>
        })
        .collect();

    executor
        .spawn(async move {
            let mut stream = con.on_message();
            while let Some(message) = futures::StreamExt::next(&mut stream).await {
                let channel = message.get_channel_name();
                let message = message.get_payload().unwrap();
                if let Some(sender) = senders.get_mut(channel) {
                    if sender.send(message).await.is_err() {
                        break;
                    }
                }
            }
        })
        .detach();

    Ok(outputs)
}
