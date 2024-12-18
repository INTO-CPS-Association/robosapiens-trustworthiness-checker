use core::panic;

// #![deny(warnings)]
use clap::Parser;
use trustworthiness_checker::core::OutputHandler;
use trustworthiness_checker::mqtt_output_handler::MQTTOutputHandler;
use trustworthiness_checker::{self as tc, parse_file, type_checking::type_check, Monitor};
use trustworthiness_checker::{InputProvider, Value};

use trustworthiness_checker::commandline_args::{Cli, Language, Runtime, Semantics};
#[cfg(feature = "ros")]
use trustworthiness_checker::ros_input_provider;
use trustworthiness_checker::stdout_output_handler::StdoutOutputHandler;

const MQTT_HOSTNAME: &str = "localhost";

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Could use tokio-console for debugging
    // console_subscriber::init();
    let cli = Cli::parse();

    // let model = std::fs::read_to_string(cli.model).expect("Model file could not be read");
    let input_mode = cli.input_mode;

    let language = cli.language.unwrap_or(Language::Lola);
    let semantics = cli.semantics.unwrap_or(Semantics::Untimed);
    let runtime = cli.runtime.unwrap_or(Runtime::Async);

    let model_parser = match language {
        Language::Lola => tc::parser::lola_specification,
    };

    let mut input_streams: Box<dyn InputProvider<tc::Value>> = {
        if let Some(input_file) = input_mode.input_file {
            let input_file_parser = match language {
                Language::Lola => tc::parser::lola_input_file,
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
                let input_mapping =
                    tc::ros_topic_stream_mapping::json_to_mapping(&input_mapping_str)
                        .expect("Input mapping file could not be parsed");
                Box::new(
                    ros_input_provider::ROSInputProvider::new(input_mapping)
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

            let mqtt_input_provider =
                tc::mqtt_input_provider::MQTTInputProvider::new(MQTT_HOSTNAME, var_topics)
                    .expect("MQTT input provider could not be created");
            Box::new(mqtt_input_provider)
        } else {
            panic!("Input provider not specified")
        }
    };

    let model = parse_file(model_parser, cli.model.as_str())
        .await
        .expect("Model file could not be parsed");
    println!("Model: {:?}", model);

    let output_handler: Box<dyn OutputHandler<Value>> = match cli.output_mode {
        trustworthiness_checker::commandline_args::OutputMode {
            output_stdout: true,
            output_mqtt_topics: None,
            output_ros_topics: None,
        } => Box::new(StdoutOutputHandler::<tc::Value>::new(
            model.output_vars.clone(),
        )),
        trustworthiness_checker::commandline_args::OutputMode {
            output_stdout: false,
            output_mqtt_topics: Some(topics),
            output_ros_topics: None,
        } => {
            let topics = topics
                .into_iter()
                .map(|topic| (tc::VarName(topic.clone()), topic))
                .collect();
            Box::new(
                MQTTOutputHandler::new(MQTT_HOSTNAME, topics)
                    .expect("MQTT output handler could not be created"),
            )
        }
        trustworthiness_checker::commandline_args::OutputMode {
            output_stdout: false,
            output_mqtt_topics: None,
            output_ros_topics: Some(_),
        } => unimplemented!("ROS output not implemented"),
        // Default to stdout
        _ => Box::new(StdoutOutputHandler::<tc::Value>::new(
            model.output_vars.clone(),
        )),
    };

    // println!("Outputs: {:?}", model.output_vars);
    // println!("Inputs: {:?}", model.input_vars);
    // println!("Model: {:?}", model);

    // Get the outputs from the Monitor
    let task = match (runtime, semantics) {
        (Runtime::Async, Semantics::Untimed) => {
            let runner = Box::new(
                tc::AsyncMonitorRunner::<_, _, tc::UntimedLolaSemantics, _>::new(
                    model,
                    &mut *input_streams,
                    output_handler,
                ),
            );
            tokio::spawn(runner.run())
        }
        (Runtime::Queuing, Semantics::Untimed) => {
            let runner = tc::queuing_runtime::QueuingMonitorRunner::<
                _,
                _,
                tc::UntimedLolaSemantics,
                _,
            >::new(model, &mut *input_streams, output_handler);
            tokio::spawn(runner.run())
        }
        (Runtime::Async, Semantics::TypedUntimed) => {
            let typed_model = type_check(model).expect("Model failed to type check");
            // let typed_input_streams = d

            let runner = tc::AsyncMonitorRunner::<_, _, tc::TypedUntimedLolaSemantics, _>::new(
                typed_model,
                &mut *input_streams,
                output_handler,
            );
            tokio::spawn(runner.run())
        }
        (Runtime::Queuing, Semantics::TypedUntimed) => {
            let typed_model = type_check(model).expect("Model failed to type check");

            let runner = tc::queuing_runtime::QueuingMonitorRunner::<
                _,
                _,
                tc::TypedUntimedLolaSemantics,
                _,
            >::new(typed_model, &mut *input_streams, output_handler);
            tokio::spawn(runner.run())
        }
        (Runtime::Constraints, Semantics::Untimed) => {
            let runner = tc::constraint_based_runtime::ConstraintBasedMonitor::new(
                model,
                &mut *input_streams,
                output_handler,
            );
            tokio::spawn(runner.run())
        }
        _ => unimplemented!(),
    };

    task.await.expect("Monitor failed to run");
}
