use std::rc::Rc;
use std::{
    collections::BTreeMap,
    mem,
    sync::{
        LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::{LocalStream, distributed::distribution_graphs::DistributionGraph};
use crate::{
    distributed::distribution_graphs::{NodeName, Pos, dist_graph_from_positions},
    io::mqtt,
};

use async_stream::stream;
use async_unsync::bounded;
use futures::future::join_all;
use serde_json::Value as JValue;
use smol::{LocalExecutor, stream::StreamExt};
use tracing::{debug, info, info_span, warn};

const QOS: i32 = 1;

pub trait DistGraphProvider {
    fn dist_graph_stream(&mut self) -> LocalStream<Rc<DistributionGraph>>;
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
    fn dist_graph_stream(&mut self) -> LocalStream<Rc<DistributionGraph>> {
        let graph = self.graph.clone();
        Box::pin(stream! {
            yield graph.clone();
            futures::future::pending::<()>().await;
        })
    }
}

pub struct MqttDistGraphProvider {
    pub executor: Rc<LocalExecutor<'static>>,
    pub central_node: NodeName,
    pub locations: BTreeMap<NodeName, String>,
    position_stream: Option<LocalStream<Vec<Pos>>>,
}

impl DistGraphProvider for MqttDistGraphProvider {
    fn dist_graph_stream(&mut self) -> LocalStream<Rc<DistributionGraph>> {
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

impl MqttDistGraphProvider {
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        central_node: NodeName,
        locations: BTreeMap<NodeName, String>,
        protocol: mqtt::MqttProtocol,
    ) -> anyhow::Result<Self> {
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
        }) as LocalStream<Vec<Pos>>);

        executor
            .spawn(async move {
                let provider_id = PROVIDER_ID.fetch_add(1, Ordering::Relaxed);
                let span = info_span!("MQTTDistGraphProvider with ID {}", provider_id);
                let _ = span.enter();
                debug!("MQTTDistGraphProvider with ID {}", provider_id);

                let (client, mut output) = mqtt::connect_and_receive_with_protocol_and_retry(
                    "tcp://localhost",
                    protocol,
                    crate::io::RetryPolicy::input_default(),
                )
                .await
                .unwrap();

                if let Err(error) = client.subscribe_many_same_qos(&topics, QOS).await {
                    warn!(?topics, ?error, "Failed to subscribe to MQTT graph topics");
                    return;
                }

                while let Some(msg) = output.next().await {
                    let msg = match msg {
                        Ok(msg) => msg,
                        Err(error) => {
                            warn!(?error, "MQTT graph input stopped");
                            break;
                        }
                    };
                    let topic = msg.topic;
                    if let Some(index) = topics.iter().position(|t| t == &topic) {
                        if let Ok(Some(Some(pos))) =
                            json5::from_str::<JValue>(&msg.payload).map(|x| {
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
                                    msg.payload
                                ),
                            }
                        } else {
                            warn!(
                                "Failed to parse position from topic {}: {}",
                                topic,
                                msg.payload
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

    pub fn locations_stream(&mut self) -> LocalStream<Vec<Pos>> {
        info!("Taking locations stream");
        Box::pin(mem::take(&mut self.position_stream).unwrap())
    }
}
