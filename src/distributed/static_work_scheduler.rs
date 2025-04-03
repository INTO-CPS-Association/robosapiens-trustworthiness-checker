use std::sync::Arc;
use std::sync::Mutex;

use crate::{VarName, io::mqtt::provide_mqtt_client};
use async_trait::async_trait;
use paho_mqtt::Message;
use tracing::info;

use super::distribution_graphs::{LabelledDistributionGraph, NodeName};

#[async_trait]
pub trait SchedulerCommunicator {
    async fn schedule_work(
        &mut self,
        node: NodeName,
        work: Vec<VarName>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct MQTTSchedulerCommunicator {
    mqtt_uri: String,
}

impl MQTTSchedulerCommunicator {
    pub fn new(mqtt_uri: String) -> Self {
        Self { mqtt_uri }
    }
}

#[async_trait]
impl SchedulerCommunicator for MQTTSchedulerCommunicator {
    async fn schedule_work(
        &mut self,
        node: NodeName,
        work: Vec<VarName>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mqtt_client = provide_mqtt_client(self.mqtt_uri.clone()).await?;
        let work_msg = serde_json::to_string(&work)?;
        let work_topic = format!("start_monitors_at_{}", node);
        let work_msg = Message::new(work_topic, work_msg, 2);

        mqtt_client.publish(work_msg).await?;

        Ok(())
    }
}

struct MockSchedulerCommunicator {
    pub log: Vec<(NodeName, Vec<VarName>)>,
}

#[async_trait]
impl SchedulerCommunicator for MockSchedulerCommunicator {
    async fn schedule_work(
        &mut self,
        node: NodeName,
        work: Vec<VarName>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.log.push((node, work));
        Ok(())
    }
}

#[async_trait]
impl SchedulerCommunicator for Arc<Mutex<MockSchedulerCommunicator>> {
    async fn schedule_work(
        &mut self,
        node: NodeName,
        work: Vec<VarName>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Clone the data and drop the MutexGuard before awaiting
        let mock = {
            let mut lock = self.lock().unwrap();
            lock.log.push((node, work));
            Ok(())
        };
        mock
    }
}

pub async fn static_work_scheduler(
    dist_graph: LabelledDistributionGraph,
    mut communicator: impl SchedulerCommunicator,
) -> Result<(), Box<dyn std::error::Error>> {
    let nodes = dist_graph.dist_graph.graph.node_indices();
    // is this really the best way?
    for node in nodes {
        info!("processing node {:?}", node);
        let node_name = dist_graph.dist_graph.graph[node].clone();
        let work = dist_graph.node_labels[&node].clone();
        info!("Scheduling work {:?} for node {}", work, node_name);
        communicator.schedule_work(node_name, work).await?;
        info!("Scheduled work");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use proptest::proptest;
    use test_log::test;
    use testcontainers_modules::testcontainers::{
        ContainerAsync, GenericImage, ImageExt,
        core::{IntoContainerPort, WaitFor},
        runners::AsyncRunner,
    };
    use tokio_stream::StreamExt;
    use tracing::{info, instrument};

    use super::*;

    use crate::{
        distributed::distribution_graphs::generation::arb_labelled_conc_distribution_graph,
        io::mqtt::provide_mqtt_client_with_subscription,
    };

    #[instrument(level = tracing::Level::INFO)]
    async fn start_emqx() -> ContainerAsync<GenericImage> {
        let image = GenericImage::new("emqx/emqx", "5.8.3")
            .with_wait_for(WaitFor::message_on_stdout("EMQX 5.8.3 is running now!"))
            .with_exposed_port(1883_u16.tcp())
            .with_startup_timeout(Duration::from_secs(30));
        // .with_mount(Mount::bind_mount("/tmp/emqx_logs", "/opt/emqx/log"));

        image
            .start()
            .await
            .expect("Failed to start EMQX test container")
    }

    #[cfg_attr(not(feature = "testcontainers"), ignore)]
    #[test(tokio::test)]
    async fn test_mqtt_scheduler_distribution() {
        let graph_str: String =
            tokio::fs::read_to_string("examples/simple_add_distribution_graph.json")
                .await
                .unwrap();
        let dist_graph: LabelledDistributionGraph = serde_json::from_str(&graph_str).unwrap();

        let mqtt_server = start_emqx().await;
        let mqtt_port = mqtt_server.get_host_port_ipv4(1883).await.unwrap();
        let mqtt_uri = format!("tcp://localhost:{}", mqtt_port).to_string();
        let communicator =
            MQTTSchedulerCommunicator::new(format!("tcp://localhost:{}", mqtt_port).to_string());

        let monitor_topics = vec![
            "start_monitors_at_A".to_string(),
            "start_monitors_at_B".to_string(),
        ];

        let (mqtt_client, stream) = provide_mqtt_client_with_subscription(mqtt_uri)
            .await
            .unwrap();

        mqtt_client
            .subscribe_many_same_qos(&monitor_topics, 2)
            .await
            .unwrap();

        let scheduler_handle = tokio::spawn(async move {
            // This should happen after the clients are waiting for their schedules
            tokio::time::sleep(Duration::from_millis(50)).await;
            info!("Starting scheduler");
            static_work_scheduler(dist_graph, communicator)
                .await
                .unwrap();
        });

        info!("Started dependencies and waiting for schedule");
        let schedule: Vec<(String, Vec<String>)> = stream
            .take(2)
            .map(|msg| {
                let topic = msg.topic().to_string();
                let payload: Vec<String> = serde_json::from_str(&msg.payload_str()).unwrap();
                (topic, payload)
            })
            .collect()
            .await;

        let expected = vec![
            ("start_monitors_at_A".to_string(), vec!["w".to_string()]),
            ("start_monitors_at_B".to_string(), vec!["v".to_string()]),
        ];

        assert_eq!(schedule, expected);
        info!("Finished test");
        scheduler_handle.await.unwrap();
        info!("Finished shutdown");
    }

    proptest! {
        #[test]
        fn test_prop_static_work_scheduler(dist_graph in arb_labelled_conc_distribution_graph()) {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let mock_communicator = Arc::new(Mutex::new(MockSchedulerCommunicator {
                    log: Vec::new()
                }));

                static_work_scheduler(dist_graph.clone(), mock_communicator.clone()).await.unwrap();

                let graph = dist_graph.dist_graph.graph;
                for node in graph.node_indices() {
                    let work = dist_graph.node_labels.get(&node);
                    let node_name = graph[node].clone();
                    if let Some(work) = work {
                        assert!(mock_communicator.lock().unwrap().log.contains(&(node_name, work.clone())));
                    }
                }
            });
        }
    }
}
