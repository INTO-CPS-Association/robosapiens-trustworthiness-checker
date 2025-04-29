use core::panic;
use std::rc::Rc;

// #![deny(warnings)]
use clap::Parser;
use smol::LocalExecutor;
use tracing::{debug, info, info_span};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::{fmt, prelude::*};
use trustworthiness_checker::VarName;
use trustworthiness_checker::core::{AbstractMonitorBuilder, Runnable};
use trustworthiness_checker::dep_manage::interface::{DependencyKind, create_dependency_manager};
use trustworthiness_checker::distributed::distribution_graphs::LabelledDistributionGraph;
use trustworthiness_checker::distributed::locality_receiver::LocalityReceiver;
use trustworthiness_checker::io::mqtt::MQTTOutputHandler;
use trustworthiness_checker::runtime::RuntimeBuilder;
use trustworthiness_checker::runtime::builder::DistributionMode;
use trustworthiness_checker::semantics::distributed::localisation::{Localisable, LocalitySpec};
use trustworthiness_checker::{self as tc, io::file::parse_file};

use macro_rules_attribute::apply;
use smol_macros::main as smol_main;
use trustworthiness_checker::cli::args::{
    Cli, DistributionMode as CliDistMode, Language, ParserMode,
};
use trustworthiness_checker::io::cli::StdoutOutputHandler;
#[cfg(feature = "ros")]
use trustworthiness_checker::io::ros::{
    input_provider::ROSInputProvider, ros_topic_stream_mapping,
};

const MQTT_HOSTNAME: &str = "localhost";

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

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

    let input_mode = cli.input_mode;

    let builder = RuntimeBuilder::new();

    let parser = cli.parser_mode.unwrap_or(ParserMode::Combinator);
    let language = cli.language.unwrap_or(Language::Lola);

    let builder = builder.executor(executor.clone());

    let builder = builder.maybe_semantics(cli.semantics);

    let builder = builder.maybe_runtime(cli.runtime);

    let model_parser = match language {
        Language::Lola => tc::lang::dynamic_lola::parser::lola_specification,
    };

    debug!("Choosing distribution mode");
    let builder = builder.distribution_mode(match cli.distribution_mode {
        CliDistMode {
            centralised: true,
            distribution_graph: None,
            local_topics: None,
            distributed_work: false,
            mqtt_centralised_distributed: None,
            mqtt_randomized_distributed: None,
            mqtt_static_optimized: None,
        } => DistributionMode::CentralMonitor,
        CliDistMode {
            centralised: _,
            distribution_graph: Some(s),
            local_topics: _,
            distributed_work: _,
            mqtt_centralised_distributed: None,
            mqtt_randomized_distributed: None,
            mqtt_static_optimized: None,
        } => {
            debug!("centralised mode");
            let f = std::fs::read_to_string(&s).expect("Distribution graph file could not be read");
            let distribution_graph: LabelledDistributionGraph =
                serde_json::from_str(&f).expect("Distribution graph could not be parsed");
            let local_node = cli.local_node.expect("Local node not specified").into();

            DistributionMode::LocalMonitor(Box::new((local_node, distribution_graph)))
        }
        CliDistMode {
            centralised: _,
            distribution_graph: _,
            local_topics: Some(topics),
            distributed_work: _,
            mqtt_centralised_distributed: None,
            mqtt_randomized_distributed: None,
            mqtt_static_optimized: None,
        } => DistributionMode::LocalMonitor(Box::new(
            topics
                .into_iter()
                .map(|v| v.into())
                .collect::<Vec<tc::VarName>>(),
        )),
        CliDistMode {
            centralised: _,
            distribution_graph: _,
            local_topics: _,
            distributed_work: true,
            mqtt_centralised_distributed: None,
            mqtt_randomized_distributed: None,
            mqtt_static_optimized: None,
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
            centralised: _,
            distribution_graph: _,
            local_topics: _,
            distributed_work: _,
            mqtt_centralised_distributed: Some(locations),
            mqtt_randomized_distributed: None,
            mqtt_static_optimized: None,
        } => {
            debug!("setting up distributed centralised mode");
            DistributionMode::DistributedCentralised(locations)
        }
        CliDistMode {
            centralised: _,
            distribution_graph: _,
            local_topics: _,
            distributed_work: _,
            mqtt_centralised_distributed: None,
            mqtt_randomized_distributed: Some(locations),
            mqtt_static_optimized: None,
        } => {
            debug!("setting up distributed random mode");
            DistributionMode::DistributedRandom(locations)
        }
        CliDistMode {
            centralised: _,
            distribution_graph: _,
            local_topics: _,
            distributed_work: _,
            mqtt_centralised_distributed: None,
            mqtt_randomized_distributed: None,
            mqtt_static_optimized: Some(locations),
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
        _ => unreachable!(),
    });

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
    let input_var_names = model.input_vars.clone();
    let builder = builder.model(model);

    // Create the input provider
    let builder = builder.input({
        if let Some(input_file) = input_mode.input_file {
            let input_file_parser = match language {
                Language::Lola => tc::lang::untimed_input::untimed_input_file,
            };

            Box::new(
                tc::parse_file(input_file_parser, &input_file)
                    .await
                    .expect("Input file could not be parsed"),
            )
        } else if let Some(_input_ros_topics) = input_mode.input_ros_topics {
            #[cfg(feature = "ros")]
            {
                let input_mapping_str = std::fs::read_to_string(&_input_ros_topics)
                    .expect("Input mapping file could not be read");
                let input_mapping = ros_topic_stream_mapping::json_to_mapping(&input_mapping_str)
                    .expect("Input mapping file could not be parsed");
                Box::new(
                    ROSInputProvider::new(executor.clone(), input_mapping)
                        .expect("ROS input provider could not be created"),
                )
            }
            #[cfg(not(feature = "ros"))]
            {
                unimplemented!("ROS support not enabled")
            }
        } else if let Some(input_mqtt_topics) = input_mode.input_mqtt_topics {
            let var_topics = input_mqtt_topics
                .iter()
                .map(|topic| (VarName::new(topic), topic.clone()))
                .collect();
            let mut mqtt_input_provider =
                tc::io::mqtt::MQTTInputProvider::new(executor.clone(), MQTT_HOSTNAME, var_topics)
                    .expect("MQTT input provider could not be created");
            mqtt_input_provider
                .started
                .wait_for(|x| info_span!("Waited for input provider started").in_scope(|| *x))
                .await
                .expect("MQTT input provider failed to start");
            Box::new(mqtt_input_provider)
        } else if let Some(input_map_mqtt_topics) = input_mode.input_map_mqtt_topics {
            let var_topics = input_map_mqtt_topics
                .iter()
                .map(|topic| (VarName::new(topic), topic.clone()))
                .collect();
            let mut map_mqtt_input_provider = tc::io::mqtt::MapMQTTInputProvider::new(
                executor.clone(),
                MQTT_HOSTNAME,
                var_topics,
            )
            .expect("Map MQTT input provider could not be created");
            map_mqtt_input_provider
                .started
                .wait_for(|x| info_span!("Waited for input provider started").in_scope(|| *x))
                .await
                .expect("Map MQTT input provider failed to start");
            Box::new(map_mqtt_input_provider)
        } else if input_mode.mqtt_input {
            let var_topics = input_var_names
                .iter()
                .map(|var| (var.clone(), var.into()))
                .collect();
            let mut mqtt_input_provider =
                tc::io::mqtt::MQTTInputProvider::new(executor.clone(), MQTT_HOSTNAME, var_topics)
                    .expect("MQTT input provider could not be created");
            mqtt_input_provider
                .started
                .wait_for(|x| info_span!("Waited for input provider started").in_scope(|| *x))
                .await
                .expect("MQTT input provider failed to start");
            Box::new(mqtt_input_provider)
        } else {
            panic!("Input provider not specified")
        }
    });

    // Create the output handler
    let builder = builder.output(match cli.output_mode {
        trustworthiness_checker::cli::args::OutputMode {
            output_stdout: true,
            output_mqtt_topics: None,
            mqtt_output: false,
            output_ros_topics: None,
        } => Box::new(StdoutOutputHandler::<tc::Value>::new(
            executor.clone(),
            output_var_names,
        )),
        trustworthiness_checker::cli::args::OutputMode {
            output_stdout: false,
            output_mqtt_topics: Some(topics),
            mqtt_output: false,
            output_ros_topics: None,
        } => {
            let topics = topics
                .into_iter()
                // Only include topics that are in the output_vars
                // this is necessary for localisation support
                .filter(|topic| output_var_names.contains(&VarName::new(topic.as_str())))
                .map(|topic| (topic.clone().into(), topic))
                .collect();
            Box::new(
                MQTTOutputHandler::new(executor.clone(), output_var_names, MQTT_HOSTNAME, topics)
                    .expect("MQTT output handler could not be created"),
            )
        }
        trustworthiness_checker::cli::args::OutputMode {
            output_stdout: false,
            output_mqtt_topics: None,
            mqtt_output: true,
            output_ros_topics: None,
        } => {
            let topics = output_var_names
                .iter()
                .map(|var| (var.clone(), var.into()))
                .collect();
            Box::new(
                MQTTOutputHandler::new(executor.clone(), output_var_names, MQTT_HOSTNAME, topics)
                    .expect("MQTT output handler could not be created"),
            )
        }
        trustworthiness_checker::cli::args::OutputMode {
            output_stdout: false,
            mqtt_output: false,
            output_mqtt_topics: None,
            output_ros_topics: Some(_),
        } => unimplemented!("ROS output not implemented"),
        // Default to stdout
        _ => Box::new(StdoutOutputHandler::<tc::Value>::new(
            executor.clone(),
            output_var_names.clone(),
        )),
    });

    // Create the runtime
    let monitor = builder.build();
    monitor.run().await;
}
