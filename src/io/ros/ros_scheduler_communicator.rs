use std::{collections::BTreeMap, rc::Rc};

use async_trait::async_trait;
use async_unsync::bounded::{Receiver, Sender};
use futures::{future::LocalBoxFuture, select_biased};
use smol::LocalExecutor;
use tracing::{debug, info};

use crate::{
    Specification,
    distributed::{
        distribution_graphs::NodeName,
        scheduling::communication::{MonitorWork, SchedulerCommunicator},
    },
    utils::cancellation_token::{CancellationToken, DropGuard},
};

pub struct RosSchedulerCommunicator {
    pub ros_node_name: String,
    work_txs: BTreeMap<NodeName, Sender<String>>,
    last_payloads: BTreeMap<NodeName, String>,
    _drop_guard: DropGuard,
}

fn create_ros_node(node_name: String) -> anyhow::Result<r2r::Node> {
    let ctx = r2r::Context::create()
        .map_err(|e| anyhow::anyhow!("Failed to create ROS context: {:?}", e))?;
    let node = r2r::Node::create(ctx, node_name.as_str(), "")
        .map_err(|e| anyhow::anyhow!("Failed to create ROS node: {:?}", e))?;
    Ok(node)
}

async fn spin_ros_node(mut node: r2r::Node, cancellation_token: CancellationToken) {
    let mut cancelled = futures::FutureExt::fuse(cancellation_token.cancelled());
    loop {
        select_biased! {
            _ = cancelled => {
                debug!("ROS Value Sender cancelled");
                return;
            },
            _ = futures::FutureExt::fuse(smol::future::yield_now()) => {
                node.spin_once(std::time::Duration::from_millis(0));
            }
        }
    }
}

/// Sends values to a ROS String topic. Will keep the node alive until cancellation token
/// is used to signal cancellation.
fn ros_value_sender(
    node: &mut r2r::Node,
    topic_name: String,
    mut work_rx: Receiver<String>,
    cancellation_token: CancellationToken,
) -> anyhow::Result<LocalBoxFuture<'static, anyhow::Result<()>>> {
    // TODO: check that this is the correct profile
    let qos = r2r::QosProfile::default();
    debug!("Created ROS publisher node 'tc_ros_output'");
    let normalized_topic_name = if topic_name.starts_with('/') {
        topic_name.clone()
    } else {
        format!("/{}", topic_name)
    };
    let publisher = node
        .create_publisher::<r2r::std_msgs::msg::String>(normalized_topic_name.as_str(), qos)
        .map_err(|e| anyhow::anyhow!("Failed to create ROS publisher: {:?}", e))?;

    Ok(Box::pin(async move {
        let mut cancelled = futures::FutureExt::fuse(cancellation_token.cancelled());
        loop {
            select_biased! {
                _ = cancelled => {
                    debug!("ROS Value Sender cancelled");
                    return Ok(());
                },
                value = futures::FutureExt::fuse(work_rx.recv()) => {
                    match value {
                        Some(value) => {
                            info!("Received request to send reconf message to topic {} with spec {}", normalized_topic_name, value);
                            let msg = r2r::std_msgs::msg::String { data: value.to_string() };
                            publisher.publish(&msg)
                                .map_err(|e| anyhow::anyhow!("Failed to publish reconf message: {:?}", e))?;
                        }
                        None => {
                            debug!("ROS Stream ended");
                            return Ok(());
                        }
                    }
                }
            }
        }
    }))
}

impl RosSchedulerCommunicator {
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        node_names: Vec<NodeName>,
        ros_node_name: String,
        reconf_base_topic: String,
    ) -> anyhow::Result<Self> {
        let cancellation_token = CancellationToken::new();

        let drop_guard = cancellation_token.drop_guard();

        let mut node = create_ros_node(ros_node_name.clone())?;

        let (work_txs, work_rxs): (Vec<_>, Vec<_>) = node_names
            .iter()
            .map(|_node_name| async_unsync::bounded::channel::<String>(100).into_split())
            .unzip();

        for (node_name, work_rx) in node_names.iter().cloned().zip(work_rxs.into_iter()) {
            let topic_name = format!(
                "{}_{}",
                reconf_base_topic.trim_start_matches('/'),
                node_name
            );
            executor
                .spawn(ros_value_sender(
                    &mut node,
                    topic_name,
                    work_rx,
                    cancellation_token.clone(),
                )?)
                .detach();
        }

        executor
            .spawn(spin_ros_node(node, cancellation_token))
            .detach();

        let work_txs: BTreeMap<NodeName, Sender<String>> =
            node_names.iter().cloned().zip(work_txs).collect();

        Ok(Self {
            ros_node_name,
            work_txs,
            last_payloads: BTreeMap::new(),
            _drop_guard: drop_guard,
        })
    }
}

#[async_trait(?Send)]
impl<M: Specification> SchedulerCommunicator<M> for RosSchedulerCommunicator {
    async fn schedule_work(
        &mut self,
        node: NodeName,
        work: MonitorWork<M>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let work = serde_json::to_string(&work)?;

        let tx = self
            .work_txs
            .get(&node)
            .ok_or_else(|| anyhow::anyhow!("Node not found: {node}"))?;
        info!(
            "Sending reconfig message for node {} with payload {}",
            node, work
        );

        tx.send(work.clone()).await?;
        self.last_payloads.insert(node, work);

        Ok(())
    }
}
