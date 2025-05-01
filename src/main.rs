use std::rc::Rc;

// #![deny(warnings)]
use clap::Parser;
use futures::future::LocalBoxFuture;
use smol::LocalExecutor;
use tracing::{debug, info};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::{fmt, prelude::*};
use trustworthiness_checker::core::{AbstractMonitorBuilder, MQTT_HOSTNAME, Runnable};
use trustworthiness_checker::dep_manage::interface::{DependencyKind, create_dependency_manager};
use trustworthiness_checker::distributed::distribution_graphs::LabelledDistributionGraph;
use trustworthiness_checker::distributed::locality_receiver::LocalityReceiver;
use trustworthiness_checker::io::InputProviderBuilder;
use trustworthiness_checker::io::builders::OutputHandlerBuilder;
use trustworthiness_checker::runtime::RuntimeBuilder;
use trustworthiness_checker::runtime::builder::DistributionMode;
use trustworthiness_checker::semantics::distributed::localisation::{Localisable, LocalitySpec};
use trustworthiness_checker::{self as tc, io::file::parse_file};

use macro_rules_attribute::apply;
use smol_macros::main as smol_main;
use trustworthiness_checker::cli::args::{
    Cli, DistributionMode as CliDistMode, Language, ParserMode,
};
#[cfg(feature = "ros")]
use trustworthiness_checker::io::ros::{
    input_provider::ROSInputProvider, ros_topic_stream_mapping,
};

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn create_dist_mode(cli: Cli) -> LocalBoxFuture<'static, DistributionMode> {
    Box::pin(async move {
        match cli.distribution_mode {
            CliDistMode {
                distribution_graph: Some(s),
                ..
            } => {
                debug!("centralised mode");
                let f =
                    std::fs::read_to_string(&s).expect("Distribution graph file could not be read");
                let distribution_graph: LabelledDistributionGraph =
                    serde_json::from_str(&f).expect("Distribution graph could not be parsed");
                let local_node = cli.local_node.expect("Local node not specified").into();

                DistributionMode::LocalMonitor(Box::new((local_node, distribution_graph)))
            }
            CliDistMode {
                local_topics: Some(topics),
                ..
            } => DistributionMode::LocalMonitor(Box::new(
                topics
                    .into_iter()
                    .map(|v| v.into())
                    .collect::<Vec<tc::VarName>>(),
            )),
            CliDistMode {
                distributed_work: true,
                ..
            } => {
                let local_node = cli.local_node.expect("Local node not specified");
                info!("Waiting for work assignment on node {}", local_node);
                let receiver =
                    tc::io::mqtt::MQTTLocalityReceiver::new(MQTT_HOSTNAME.to_string(), local_node);
                let locality = receiver
                    .receive()
                    .await
                    .expect("Work could not be received");
                info!("Received work: {:?}", locality.local_vars());
                DistributionMode::LocalMonitor(Box::new(locality))
            }
            CliDistMode {
                mqtt_centralised_distributed: Some(locations),
                ..
            } => {
                debug!("setting up distributed centralised mode");
                DistributionMode::DistributedCentralised(locations)
            }
            CliDistMode {
                mqtt_randomized_distributed: Some(locations),
                ..
            } => {
                debug!("setting up distributed random mode");
                DistributionMode::DistributedRandom(locations)
            }
            CliDistMode {
                mqtt_static_optimized: Some(locations),
                ..
            } => {
                info!("setting up static optimization mode");
                let dist_constraints = cli
                    .distribution_constraints
                    .expect("Distribution constraints must be provided")
                    .into_iter()
                    .map(|x| x.into())
                    .collect();
                DistributionMode::DistributedOptimizedStatic(locations, dist_constraints)
            }
            CliDistMode {
                centralised: true, ..
            } => DistributionMode::CentralMonitor,
            _ => unreachable!(),
        }
    })
}

#[apply(smol_main)]
async fn main(executor: Rc<LocalExecutor<'static>>) {
    tracing_subscriber::registry()
        .with(fmt::layer())
        // Uncomment the following line to enable full span events which logs
        // every time the code enters/exits an instrumented function/block
        // .with(fmt::layer().with_span_events(FmtSpan::FULL))
        .with(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    let builder = RuntimeBuilder::new();

    let parser = cli.parser_mode.unwrap_or(ParserMode::Combinator);
    let language = cli.language.unwrap_or(Language::Lola);

    let builder = builder.executor(executor.clone());

    let builder = builder.maybe_semantics(cli.semantics);

    let builder = builder.maybe_runtime(cli.runtime);

    let model_parser = match language {
        Language::Lola => tc::lang::dynamic_lola::parser::lola_specification,
    };

    let builder = builder.scheduler_mode(cli.scheduling_mode.clone());

    debug!("Choosing distribution mode");
    let builder = builder.distribution_mode_fn(cli.clone(), create_dist_mode);

    let model = match parser {
        ParserMode::Combinator => parse_file(model_parser, cli.model.as_str())
            .await
            .expect("Model file could not be parsed"),
        ParserMode::LALR => unimplemented!(),
    };
    info!(name: "Parsed model", ?model, output_vars=?model.output_vars, input_vars=?model.input_vars);

    // Localise the model to contain only the local variables (if needed)
    let model = if let DistributionMode::LocalMonitor(locality_mode) = &builder.distribution_mode {
        let model = model.localise(locality_mode);
        info!(name: "Localised model", ?model, output_vars=?model.output_vars, input_vars=?model.input_vars);
        model
    } else {
        model
    };

    // Create the dependency manager
    let builder = builder.dependencies(create_dependency_manager(
        DependencyKind::DepGraph,
        model.clone(),
    ));

    let output_var_names = model.output_vars.clone();
    let builder = builder.model(model.clone());

    // Create the input provider builder
    let input_provider_builder = InputProviderBuilder::new(cli.input_mode)
        .executor(executor.clone())
        .model(model)
        .lang(language);
    let builder = builder.input_provider_builder(input_provider_builder);

    // Create the output handler
    let output_handler_builder = OutputHandlerBuilder::new(cli.output_mode)
        .executor(executor.clone())
        .output_var_names(output_var_names);

    let builder = builder.output_handler_builder(output_handler_builder);

    // Create the runtime
    let monitor = builder.async_build().await;

    monitor.run().await;
}
