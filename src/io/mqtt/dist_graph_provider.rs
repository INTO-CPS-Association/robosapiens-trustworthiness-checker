use std::{collections::BTreeMap, mem, rc::Rc};

use crate::{
    OutputStream,
    distributed::distribution_graphs::{
        DistributionGraph, LabelledDistributionGraph, NodeName, Pos, dist_graph_from_positions,
    },
    semantics::distributed::localisation,
};

use super::client::provide_mqtt_client_with_subscription;
use async_stream::stream;
use async_unsync::bounded;
use futures::future::join_all;
use paho_mqtt as mqtt;
use serde_json::Value as JValue;
use smol::{LocalExecutor, stream::StreamExt};
use tracing::{info, warn};

const QOS: i32 = 1;

pub struct MQTTDistGraphProvider {
    pub executor: Rc<LocalExecutor<'static>>,
    pub central_node: NodeName,
    pub locations: BTreeMap<NodeName, String>,
    position_stream: Option<OutputStream<Vec<Pos>>>,
}

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
                    (_, _) => None,
                }
            }) {
                yield poss;
            }
        }) as OutputStream<Vec<Pos>>);

        executor
            .spawn(async move {
                let (client, mut output) =
                    provide_mqtt_client_with_subscription("localhost".to_string())
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
                        if let Ok(Some(pos)) = serde_json::from_str::<JValue>(&msg.payload_str())
                            .map(|x| x.get("source_robot_pose").cloned())
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
                                    info!("Parsed position from topic {}: {:?}", topic, pos);
                                    location_txs[index].send(pos).await.unwrap();
                                }
                                None => warn!(
                                    "Failed to parse position from topic {}: {}",
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
        Box::pin(mem::take(&mut self.position_stream).unwrap())
    }

    pub fn dist_graph_stream(&mut self) -> OutputStream<DistributionGraph> {
        let central_node = self.central_node.clone();
        let locations = self.locations.keys().cloned().collect::<Vec<_>>();
        Box::pin(self.locations_stream().map(move |positions| {
            dist_graph_from_positions(central_node.clone(), locations.clone(), positions)
        }))
    }
}
