use core::panic;

// #![deny(warnings)]
use clap::Parser;
use tracing::{info, info_span};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::{fmt, prelude::*};
use trustworthiness_checker::core::OutputHandler;
use trustworthiness_checker::dependencies::traits::{DependencyKind, create_dependency_manager};
use trustworthiness_checker::distributed::distribution_graphs::LabelledConcDistributionGraph;
use trustworthiness_checker::distributed::locality_receiver::LocalityReceiver;
use trustworthiness_checker::io::mqtt::MQTTOutputHandler;
use trustworthiness_checker::lang::dynamic_lola::type_checker::type_check;
use trustworthiness_checker::semantics::distributed::localisation::{Localisable, LocalitySpec};
use trustworthiness_checker::{self as tc, Monitor, io::file::parse_file};
use trustworthiness_checker::{InputProvider, Value};

use trustworthiness_checker::cli::args::{Cli, Language, Runtime, Semantics};
use trustworthiness_checker::io::cli::StdoutOutputHandler;
#[cfg(feature = "ros")]
use trustworthiness_checker::io::ros::{
    input_provider::ROSInputProvider, ros_topic_stream_mapping,
};

const MQTT_HOSTNAME: &str = "localhost";

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::registry()
        .with(fmt::layer())
        // Uncomment the following line to enable full span events which logs
        // every time the code enters/exits an instrumented function/block
        // .with(fmt::layer().with_span_events(FmtSpan::FULL))
        .with(EnvFilter::from_default_env())
        .init();

    // Could use tokio-console for debugging
    // console_subscriber::init();
    let cli = Cli::parse();

    // let model = std::fs::read_to_string(cli.model).expect("Model file could not be read");
    let input_mode = cli.input_mode;

    let language = cli.language.unwrap_or(Language::Lola);
    let semantics = cli.semantics.unwrap_or(Semantics::Untimed);
    let runtime = cli.runtime.unwrap_or(Runtime::Async);

    let model_parser = match language {
        Language::Lola => tc::lang::dynamic_lola::parser::lola_specification,
    };

    let locality_mode: Option<Box<dyn LocalitySpec>> = match cli.distribution_mode {
        trustworthiness_checker::cli::args::DistributionMode {
            centralised: true,
            distribution_graph: _,
            local_topics: _,
            distributed_work: _,
        } => None,
        trustworthiness_checker::cli::args::DistributionMode {
            centralised: false,
            distribution_graph: Some(s),
            local_topics: _,
            distributed_work: _,
        } => {
            let f = std::fs::read_to_string(&s).expect("Distribution graph file could not be read");
            let distribution_graph: LabelledConcDistributionGraph =
                serde_json::from_str(&f).expect("Distribution graph could not be parsed");
            let local_node = cli.local_node.expect("Local node not specified").into();

            Some(Box::new((local_node, distribution_graph)))
        }
        trustworthiness_checker::cli::args::DistributionMode {
            centralised: false,
            distribution_graph: None,
            local_topics: Some(topics),
            distributed_work: _,
        } => Some(Box::new(
            topics
                .into_iter()
                .map(tc::VarName)
                .collect::<Vec<tc::VarName>>(),
        )),
        trustworthiness_checker::cli::args::DistributionMode {
            centralised: false,
            distribution_graph: None,
            local_topics: None,
            distributed_work: true,
        } => {
            let local_node = cli.local_node.expect("Local node not specified").into();
            info!("Waiting for work assignment on node {}", local_node);
            let receiver =
                tc::io::mqtt::MQTTLocalityReceiver::new(MQTT_HOSTNAME.to_string(), local_node);
            let locality = receiver
                .receive()
                .await
                .expect("Work could not be received");
            info!("Received work: {:?}", locality.local_vars());
            Some(Box::new(locality))
        }
        _ => unreachable!(),
    };

    let model = parse_file(model_parser, cli.model.as_str())
        .await
        .expect("Model file could not be parsed");
    info!(name: "Parsed model", ?model, output_vars=?model.output_vars, input_vars=?model.input_vars);

    // Localise the model to contain only the local variables (if needed)
    let model = match locality_mode {
        Some(locality_mode) => {
            let model = model.localise(&locality_mode);
            info!(name: "Localised model", ?model, output_vars=?model.output_vars, input_vars=?model.input_vars);
            model
        }
        None => model,
    };

    let mut input_streams: Box<dyn InputProvider<tc::Value>> = {
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
                    ROSInputProvider::new(input_mapping)
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
                .map(|topic| (tc::VarName(topic.clone()), topic.clone()))
                .collect();
            let mut mqtt_input_provider =
                tc::io::mqtt::MQTTInputProvider::new(MQTT_HOSTNAME, var_topics)
                    .expect("MQTT input provider could not be created");
            mqtt_input_provider
                .started
                .wait_for(|x| info_span!("Waited for input provider started").in_scope(|| *x))
                .await
                .expect("MQTT input provider failed to start");
            Box::new(mqtt_input_provider)
        } else if input_mode.mqtt_input {
            let var_topics = model
                .input_vars
                .iter()
                .map(|var| (var.clone(), var.0.clone()))
                .collect();
            let mut mqtt_input_provider =
                tc::io::mqtt::MQTTInputProvider::new(MQTT_HOSTNAME, var_topics)
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
    };

    let output_handler: Box<dyn OutputHandler<Value>> = match cli.output_mode {
        trustworthiness_checker::cli::args::OutputMode {
            output_stdout: true,
            output_mqtt_topics: None,
            mqtt_output: false,
            output_ros_topics: None,
        } => Box::new(StdoutOutputHandler::<tc::Value>::new(
            model.output_vars.clone(),
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
                .filter(|topic| model.output_vars.contains(&tc::VarName(topic.clone())))
                .map(|topic| (tc::VarName(topic.clone()), topic))
                .collect();
            Box::new(
                MQTTOutputHandler::new(MQTT_HOSTNAME, topics)
                    .expect("MQTT output handler could not be created"),
            )
        }
        trustworthiness_checker::cli::args::OutputMode {
            output_stdout: false,
            output_mqtt_topics: None,
            mqtt_output: true,
            output_ros_topics: None,
        } => {
            let topics = model
                .output_vars
                .iter()
                .map(|var| (var.clone(), var.0.clone()))
                .collect();
            Box::new(
                MQTTOutputHandler::new(MQTT_HOSTNAME, topics)
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
            model.output_vars.clone(),
        )),
    };

    // Get the outputs from the Monitor
    let task = match (runtime, semantics) {
        (Runtime::Async, Semantics::Untimed) => {
            let runner = Box::new(tc::runtime::asynchronous::AsyncMonitorRunner::<
                _,
                _,
                tc::semantics::UntimedLolaSemantics,
                _,
            >::new(
                model.clone(),
                &mut *input_streams,
                output_handler,
                create_dependency_manager(DependencyKind::Empty, Box::new(model)),
            ));
            tokio::spawn(runner.run())
        }
        (Runtime::Queuing, Semantics::Untimed) => {
            let runner = tc::runtime::queuing::QueuingMonitorRunner::<
                _,
                _,
                tc::semantics::UntimedLolaSemantics,
                _,
            >::new(
                model.clone(),
                &mut *input_streams,
                output_handler,
                create_dependency_manager(DependencyKind::Empty, Box::new(model)),
            );
            tokio::spawn(runner.run())
        }
        (Runtime::Async, Semantics::TypedUntimed) => {
            let typed_model = type_check(model.clone()).expect("Model failed to type check");

            let runner = tc::runtime::asynchronous::AsyncMonitorRunner::<
                _,
                _,
                tc::semantics::TypedUntimedLolaSemantics,
                _,
            >::new(
                typed_model,
                &mut *input_streams,
                output_handler,
                create_dependency_manager(DependencyKind::Empty, Box::new(model)),
            );
            tokio::spawn(runner.run())
        }
        (Runtime::Queuing, Semantics::TypedUntimed) => {
            let typed_model = type_check(model.clone()).expect("Model failed to type check");

            let runner = tc::runtime::queuing::QueuingMonitorRunner::<
                _,
                _,
                tc::semantics::TypedUntimedLolaSemantics,
                _,
            >::new(
                typed_model,
                &mut *input_streams,
                output_handler,
                create_dependency_manager(DependencyKind::Empty, Box::new(model)),
            );
            tokio::spawn(runner.run())
        }
        (Runtime::Constraints, Semantics::Untimed) => {
            let runner = tc::runtime::constraints::ConstraintBasedMonitor::new(
                model.clone(),
                &mut *input_streams,
                output_handler,
                create_dependency_manager(DependencyKind::DepGraph, Box::new(model)),
            );
            tokio::spawn(runner.run())
        }
        _ => unimplemented!(),
    };

    task.await.expect("Monitor failed to run");
}
