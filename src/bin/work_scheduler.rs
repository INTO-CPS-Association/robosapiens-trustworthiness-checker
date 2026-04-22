use clap::Parser;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::rc::Rc;
use tracing::{info, instrument};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::{fmt, prelude::*};
use trustworthiness_checker::distributed::distribution_graphs::LabelledDistributionGraph;
use trustworthiness_checker::distributed::scheduling::communication::NullSchedulerCommunicator;
use trustworthiness_checker::distributed::scheduling::planners::core::StaticFixedSchedulerPlanner;
use trustworthiness_checker::distributed::scheduling::{ReplanningCondition, Scheduler};
use trustworthiness_checker::io::mqtt::dist_graph_provider::StaticDistGraphProvider;
use trustworthiness_checker::lang::dsrv::lalr_parser::parse_file as lalr_parse_file;
use trustworthiness_checker::{DsrvSpecification, Specification};

/// Worker scheduler application for distributed monitoring
///
/// Schedules work for monitors across distributed nodes based on a distribution graph
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the main spec
    #[arg(short, long)]
    spec: PathBuf,
    /// Path to distribution graph JSON file
    #[arg(short, long)]
    distribution_graph: PathBuf,
}

#[instrument]
async fn load_spec(path: PathBuf) -> anyhow::Result<DsrvSpecification> {
    // info!("Loading spec from {:?}", path);
    // TODO: we should be using PathBufs for parse_file
    let path_str = path
        .into_os_string()
        .into_string()
        .map_err(|_| anyhow::anyhow!("Failed to convert path to string"))?;
    info!("Loading spec from {:?}", path_str);
    let spec: DsrvSpecification = lalr_parse_file(path_str.as_str()).await?;
    info!("Successfully loaded spec");
    Ok(spec)
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

    info!("Work scheduler starting");

    // Load specification
    let spec = load_spec(args.spec).await?;

    // Load distribution graph
    let dist_graph = Rc::new(load_distribution_graph(args.distribution_graph).await?);

    // Create Mock communicator
    // TODO: Switch to ROS communicator when implemented
    let communicator = Box::new(NullSchedulerCommunicator {});

    // Mock types for scheduler communicator
    // TODO: for ROS should be loaded from the commandline, otherwise should be ignored
    let types = spec.var_names().into_iter().map(|_| "Int32".into());
    let var_msg_types = spec
        .var_names()
        .into_iter()
        .zip(types)
        .collect::<BTreeMap<_, _>>();

    info!("Distribution graph loaded, scheduling work...");

    let planner = Box::new(StaticFixedSchedulerPlanner {
        fixed_graph: dist_graph.clone(),
    });

    let dist_graph_provider = Box::new(StaticDistGraphProvider::new(dist_graph.dist_graph.clone()));

    // Run the static work scheduler
    let scheduler: Scheduler<DsrvSpecification> = Scheduler::new(
        spec,
        var_msg_types,
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
