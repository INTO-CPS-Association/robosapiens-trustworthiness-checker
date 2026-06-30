use std::{collections::BTreeMap, rc::Rc};

use async_trait::async_trait;
use futures::{FutureExt, StreamExt, select_biased};
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

use super::{ROS_SPIN_INTERVAL, ROS_SPIN_TIMEOUT};

pub struct RosSchedulerCommunicator {
    pub ros_node_name: String,
    publishers: BTreeMap<NodeName, (String, r2r::Publisher<r2r::std_msgs::msg::String>)>,
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
    let mut spin_ticks = smol::Timer::interval(ROS_SPIN_INTERVAL);
    loop {
        select_biased! {
            _ = cancelled => {
                debug!("ROS Value Sender cancelled");
                return;
            },
            _ = spin_ticks.next().fuse() => {
                node.spin_once(ROS_SPIN_TIMEOUT);
            }
        }
    }
}

fn create_reconfig_publisher(
    node: &mut r2r::Node,
    topic_name: String,
) -> anyhow::Result<(String, r2r::Publisher<r2r::std_msgs::msg::String>)> {
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

    Ok((normalized_topic_name, publisher))
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

        let publishers: BTreeMap<_, _> = node_names
            .iter()
            .cloned()
            .map(|node_name| {
                let topic_name = format!(
                    "{}_{}",
                    reconf_base_topic.trim_start_matches('/'),
                    node_name
                );
                let publisher = create_reconfig_publisher(&mut node, topic_name)?;
                Ok((node_name, publisher))
            })
            .collect::<anyhow::Result<_>>()?;

        executor
            .spawn(spin_ros_node(node, cancellation_token))
            .detach();

        Ok(Self {
            ros_node_name,
            publishers,
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
        if self.last_payloads.get(&node) == Some(&work) {
            debug!(
                "Skipping unchanged reconfig message for node {} with payload {}",
                node, work
            );
            return Ok(());
        }

        let (_, publisher) = self
            .publishers
            .get(&node)
            .ok_or_else(|| anyhow::anyhow!("Node not found: {node}"))?;
        info!(
            "Sending reconfig message for node {} with payload {}",
            node, work
        );

        let msg = r2r::std_msgs::msg::String { data: work.clone() };
        publisher.publish(&msg)?;
        self.last_payloads.insert(node, work);

        Ok(())
    }
}
