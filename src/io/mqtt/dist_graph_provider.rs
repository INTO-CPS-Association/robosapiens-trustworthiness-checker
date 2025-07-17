use std::{
    collections::BTreeMap,
    mem,
    rc::Rc,
    sync::{
        LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::{
    OutputStream,
    distributed::distribution_graphs::{
        DistributionGraph, NodeName, Pos, dist_graph_from_positions,
    },
};

use super::client::provide_mqtt_client_with_subscription;
use async_stream::stream;
use async_unsync::bounded;
use futures::future::join_all;
use paho_mqtt as mqtt;
use serde_json::Value as JValue;
use smol::{LocalExecutor, stream::StreamExt};
use tracing::{debug, info, info_span, warn};

const QOS: i32 = 1;

pub trait DistGraphProvider {
    fn dist_graph_stream(&mut self) -> OutputStream<Rc<DistributionGraph>>;
    // let central_node = self.central_node.clone();
    // let locations = self.locations.keys().cloned().collect::<Vec<_>>();
    // Box::pin(self.locations_stream().map(move |positions| {
    //     Rc::new(dist_graph_from_positions(
    //         central_node.clone(),
    //         locations.clone(),
    //         positions,
    //     ))
    // }))
    // fn central_node(&self) -> NodeName;

    // fn locations(&self) ->
}

static_assertions::assert_obj_safe!(DistGraphProvider);

pub struct StaticDistGraphProvider {
    graph: Rc<DistributionGraph>,
}

impl StaticDistGraphProvider {
    pub fn new(graph: Rc<DistributionGraph>) -> Self {
        Self { graph }
    }
}

impl DistGraphProvider for StaticDistGraphProvider {
    fn dist_graph_stream(&mut self) -> OutputStream<Rc<DistributionGraph>> {
        let graph = self.graph.clone();
        Box::pin(stream! {
            loop {
                yield graph.clone()
            }
        })
    }
}

pub struct MQTTDistGraphProvider {
    pub executor: Rc<LocalExecutor<'static>>,
    pub central_node: NodeName,
    pub locations: BTreeMap<NodeName, String>,
    position_stream: Option<OutputStream<Vec<Pos>>>,
}

impl DistGraphProvider for MQTTDistGraphProvider {
    fn dist_graph_stream(&mut self) -> OutputStream<Rc<DistributionGraph>> {
        let central_node = self.central_node.clone();
        let locations = self.locations.keys().cloned().collect::<Vec<_>>();
        Box::pin(self.locations_stream().map(move |positions| {
            info!("Providing dist graph");
            Rc::new(dist_graph_from_positions(
                central_node.clone(),
                locations.clone(),
                positions,
            ))
        }))
    }
}

static PROVIDER_ID: LazyLock<AtomicUsize> = LazyLock::new(|| 0.into());

impl MQTTDistGraphProvider {
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        central_node: NodeName,
        locations: BTreeMap<NodeName, String>,
    ) -> Result<Self, mqtt::Error> {
        let topics = locations.values().cloned().collect::<Vec<_>>();
        let (location_txs, mut location_rxs): (Vec<_>, Vec<_>) = locations
            .values()
            .map(|_| bounded::channel(100).into_split())
            .unzip();
        let position_stream = Some(Box::pin(stream! {
            while let Some(poss) = join_all(location_rxs.iter_mut().map(|rx| rx.recv())).await.into_iter().fold(Some(vec![]), |acc, res| {
                match (acc, res) {
                    (Some(mut acc), Some(pos)) => {
                        acc.push(pos);
                        Some(acc)
                    }
                    _ => None
                }
            }) {
                info!("Received positions: {:?}", poss);
                yield poss;
            }
        }) as OutputStream<Vec<Pos>>);

        executor
            .spawn(async move {
                let provider_id = PROVIDER_ID.fetch_add(1, Ordering::Relaxed);
                let span = info_span!("MQTTDistGraphProvider with ID {}", provider_id);
                let _ = span.enter();
                debug!("MQTTDistGraphProvider with ID {}", provider_id);

                let (client, mut output) =
                    provide_mqtt_client_with_subscription("localhost".to_string(), u32::MAX)
                        .await
                        .unwrap();

                loop {
                    match client.subscribe_many_same_qos(&topics, QOS).await {
                        Ok(_) => break,
                        Err(e) => {
                            warn!(name: "Failed to subscribe to topics", ?topics, err=?e);
                            info!("Retrying in 100ms");
                            let _e = client.reconnect().await;
                        }
                    }
                }

                while let Some(msg) = output.next().await {
                    let topic = msg.topic();
                    if let Some(index) = topics.iter().position(|t| t == topic) {
                        if let Ok(Some(Some(pos))) =
                            serde_json::from_str::<JValue>(&msg.payload_str()).map(|x| {
                                x.get("source_robot_pose")
                                    .cloned()
                                    .map(|y| y.get("position").cloned())
                            })
                        {
                            let pos = match (pos.get("x"), pos.get("y"), pos.get("z")) {
                                (Some(x), Some(y), Some(z)) => Some((
                                    x.as_f64().unwrap_or(0.0),
                                    y.as_f64().unwrap_or(0.0),
                                    z.as_f64().unwrap_or(0.0),
                                )),
                                _ => None,
                            };

                            match pos {
                                Some(pos) => {
                                    debug!("Parsed position from topic {}: {:?}", topic, pos);
                                    if let Err(_) = location_txs[index].send(pos).await {
                                        warn!(
                                            "Provider {} failed to send position for topic = {} at index {}",
                                            provider_id,
                                            topic,
                                            index,
                                        )
                                    };
                                }
                                None => warn!(
                                    "Failed to parse inner position from topic {}: {}",
                                    topic,
                                    msg.payload_str()
                                ),
                            }
                        } else {
                            warn!(
                                "Failed to parse position from topic {}: {}",
                                topic,
                                msg.payload_str()
                            );
                        }
                    }
                }
            })
            .detach();

        Ok(Self {
            executor,
            central_node,
            locations,
            position_stream,
        })
    }

    pub fn locations_stream(&mut self) -> OutputStream<Vec<Pos>> {
        info!("Taking locations stream");
        Box::pin(mem::take(&mut self.position_stream).unwrap())
    }
}
