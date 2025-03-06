use clap::Parser;
use std::path::PathBuf;
use tokio;
use tracing::{info, instrument};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::{fmt, prelude::*};
use trustworthiness_checker::distributed::{
    distribution_graphs::LabelledConcDistributionGraph,
    static_work_scheduler::{MQTTSchedulerCommunicator, static_work_scheduler},
};

/// Worker scheduler application for distributed monitoring
///
/// Schedules work for monitors across distributed nodes based on a distribution graph
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to distribution graph JSON file
    #[arg(short, long)]
    distribution_graph: PathBuf,
}

#[instrument]
async fn load_distribution_graph(
    path: PathBuf,
) -> Result<LabelledConcDistributionGraph, Box<dyn std::error::Error>> {
    info!("Loading distribution graph from {:?}", path);
    let file_content = tokio::fs::read_to_string(path).await?;
    let dist_graph: LabelledConcDistributionGraph = serde_json::from_str(&file_content)?;
    info!("Successfully loaded distribution graph");
    Ok(dist_graph)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line arguments
    let args = Args::parse();

    // Initialize logging
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let mqtt_uri = "tcp://localhost:1883".to_string();

    info!("Work scheduler starting");

    // Load distribution graph
    let dist_graph = load_distribution_graph(args.distribution_graph).await?;

    // Create MQTT communicator
    let communicator = MQTTSchedulerCommunicator::new(mqtt_uri);

    info!("Distribution graph loaded, scheduling work...");

    // Run the static work scheduler
    static_work_scheduler(dist_graph, communicator).await?;

    info!("Work scheduling completed successfully");

    Ok(())
}
