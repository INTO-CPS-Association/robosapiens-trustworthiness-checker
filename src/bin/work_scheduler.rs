use clap::Parser;
use std::path::PathBuf;
use std::rc::Rc;
use tracing::{info, instrument};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::{fmt, prelude::*};
use trustworthiness_checker::distributed::distribution_graphs::LabelledDistributionGraph;
use trustworthiness_checker::distributed::scheduling::planners::core::StaticFixedSchedulerPlanner;
use trustworthiness_checker::distributed::scheduling::{ReplanningCondition, Scheduler};
use trustworthiness_checker::io::mqtt::MQTTSchedulerCommunicator;
use trustworthiness_checker::io::mqtt::dist_graph_provider::StaticDistGraphProvider;

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
async fn load_distribution_graph(path: PathBuf) -> anyhow::Result<LabelledDistributionGraph> {
    info!("Loading distribution graph from {:?}", path);
    let file_content = smol::fs::read_to_string(path).await?;
    let dist_graph: LabelledDistributionGraph = serde_json::from_str(&file_content)?;
    info!("Successfully loaded distribution graph");
    Ok(dist_graph)
}

fn main() -> anyhow::Result<()> {
    smol::block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
    // Parse command line arguments
    let args = Args::parse();

    // Initialize logging
    tracing_subscriber::registry()
        .with(fmt::layer().with_writer(std::io::stderr))
        .with(EnvFilter::from_default_env())
        .init();

    let mqtt_uri = "tcp://localhost:1883".to_string();

    info!("Work scheduler starting");

    // Load distribution graph
    let dist_graph = Rc::new(load_distribution_graph(args.distribution_graph).await?);

    // Create MQTT communicator
    let communicator = Box::new(MQTTSchedulerCommunicator::new(mqtt_uri));

    info!("Distribution graph loaded, scheduling work...");

    let planner = Box::new(StaticFixedSchedulerPlanner {
        fixed_graph: dist_graph.clone(),
    });

    let dist_graph_provider = Box::new(StaticDistGraphProvider::new(dist_graph.dist_graph.clone()));

    // Run the static work scheduler
    let scheduler = Scheduler::new(
        planner,
        communicator,
        dist_graph_provider,
        ReplanningCondition::Never,
        false,
    );

    scheduler.run().await?;

    info!("Work scheduling completed successfully");

    Ok(())
}
