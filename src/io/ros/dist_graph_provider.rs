use std::{collections::BTreeMap, mem, rc::Rc};

use async_stream::stream;
use async_unsync::bounded;
use futures::{future::join_all, select_biased};
use smol::{LocalExecutor, stream::StreamExt};
use tracing::{debug, info, warn};

use crate::{
    OutputStream,
    distributed::distribution_graphs::{
        DistributionGraph, NodeName, Pos, dist_graph_from_positions,
    },
    io::mqtt::dist_graph_provider::DistGraphProvider,
    utils::cancellation_token::CancellationToken,
};

/// ROS-backed provider for live distribution graphs.
///
/// This provider subscribes to a shared RVData topic (typically `/dist_graph`)
/// and routes updates to logical nodes by `source_robot_id`.
pub struct ROSDistGraphProvider {
    pub executor: Rc<LocalExecutor<'static>>,
    pub central_node: NodeName,
    /// Mapping from logical node name to RVData `source_robot_id`
    pub locations: BTreeMap<NodeName, String>,
    position_stream: Option<OutputStream<Vec<Pos>>>,
}

impl DistGraphProvider for ROSDistGraphProvider {
    fn dist_graph_stream(&mut self) -> OutputStream<Rc<DistributionGraph>> {
        let central_node = self.central_node.clone();
        let locations = self.locations.keys().cloned().collect::<Vec<_>>();
        Box::pin(self.locations_stream().map(move |positions| {
            info!("Providing distribution graph");
            Rc::new(dist_graph_from_positions(
                central_node.clone(),
                locations.clone(),
                positions,
            ))
        }))
    }
}

impl ROSDistGraphProvider {
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        central_node: NodeName,
        locations: BTreeMap<NodeName, String>,
        rv_data_topic: String,
    ) -> anyhow::Result<Self> {
        let source_ids = locations.values().cloned().collect::<Vec<_>>();
        let (location_txs, mut location_rxs): (Vec<_>, Vec<_>) = locations
            .values()
            .map(|_| bounded::channel(100).into_split())
            .unzip();

        let position_stream = Some(Box::pin(stream! {
            while let Some(poss) = join_all(location_rxs.iter_mut().map(|rx| rx.recv()))
                .await
                .into_iter()
                .fold(Some(vec![]), |acc, res| {
                    match (acc, res) {
                        (Some(mut acc), Some(pos)) => {
                            acc.push(pos);
                            Some(acc)
                        }
                        _ => None,
                    }
                })
            {
                info!("Received ROS positions: {:?}", poss);
                yield poss;
            }
        }) as OutputStream<Vec<Pos>>);

        let ctx = r2r::Context::create()
            .map_err(|e| anyhow::anyhow!("Failed to create ROS context: {:?}", e))?;
        let mut node = r2r::Node::create(ctx, "tc_dist_graph_provider", "")
            .map_err(|e| anyhow::anyhow!("Failed to create ROS node: {:?}", e))?;

        // Single shared RVData topic; split up by source_robot_id
        let mut rv_data_stream = node
            .subscribe::<r2r::id_pose_msgs::msg::RVData>(
                rv_data_topic.as_str(),
                r2r::QosProfile::default(),
            )
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to subscribe to RVData topic {}: {:?}",
                    rv_data_topic,
                    e
                )
            })?;

        let cancellation_token = CancellationToken::new();
        let mut cancelled = futures::FutureExt::fuse(cancellation_token.cancelled());

        // ROS node spinner
        executor
            .spawn(async move {
                loop {
                    select_biased! {
                        _ = cancelled => break,
                        _ = futures::FutureExt::fuse(smol::future::yield_now()) =>
                        node.spin_once(std::time::Duration::from_millis(0))
                    }
                }
            })
            .detach();

        executor
            .spawn(async move {
                let _cancellation_token_guard = cancellation_token.drop_guard();
                loop {
                    if let Some(msg) = rv_data_stream.next().await {
                        let src = msg.source_robot_id.clone();
                        if let Some(index) = source_ids.iter().position(|id| id == &src) {
                            let pos = (
                                msg.source_robot_pose.position.x,
                                msg.source_robot_pose.position.y,
                                msg.source_robot_pose.position.z,
                            );
                            debug!(
                                "Parsed ROS RVData position for source_robot_id={} at index {}: {:?}",
                                src, index, pos
                            );
                            if location_txs[index].send(pos).await.is_err() {
                                warn!(
                                    "Failed to send parsed ROS RVData position for source_robot_id={} at index {}",
                                    src, index
                                );
                            }
                        } else {
                            debug!(
                                "Ignoring RVData for unmapped source_robot_id={}",
                                src
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
        info!("Taking ROS locations stream");
        Box::pin(mem::take(&mut self.position_stream).expect("Position stream already taken"))
    }
}
