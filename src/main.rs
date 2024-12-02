use core::panic;

// #![deny(warnings)]
use clap::Parser;
use futures::StreamExt;
use trustworthiness_checker::InputProvider;
use trustworthiness_checker::{self as tc, parse_file, type_checking::type_check, Monitor};

use trustworthiness_checker::commandline_args::{Cli, Language, Runtime, Semantics};
#[cfg(feature = "ros")]
use trustworthiness_checker::ros_input_provider;

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
        } else if let Some(input_ros_topics) = input_mode.input_ros_topics {
            #[cfg(feature = "ros")]
            {
                let input_mapping_str = std::fs::read_to_string(&input_ros_topics)
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

    // println!("Outputs: {:?}", model.output_vars);
    // println!("Inputs: {:?}", model.input_vars);
    // println!("Model: {:?}", model);

    // Get the outputs from the Monitor
    let outputs = match (runtime, semantics) {
        (Runtime::Async, Semantics::Untimed) => {
            let mut runner = tc::AsyncMonitorRunner::<_, _, tc::UntimedLolaSemantics, _>::new(
                model,
                &mut *input_streams,
            );
            runner.monitor_outputs()
        }
        (Runtime::Queuing, Semantics::Untimed) => {
            let mut runner = tc::queuing_runtime::QueuingMonitorRunner::<
                _,
                _,
                tc::UntimedLolaSemantics,
                _,
            >::new(model, &mut *input_streams);
            runner.monitor_outputs()
        }
        (Runtime::Async, Semantics::TypedUntimed) => {
            let typed_model = type_check(model).expect("Model failed to type check");
            // let typed_input_streams = d

            let mut runner = tc::AsyncMonitorRunner::<_, _, tc::TypedUntimedLolaSemantics, _>::new(
                typed_model,
                &mut *input_streams,
            );
            runner.monitor_outputs()
        }
        (Runtime::Queuing, Semantics::TypedUntimed) => {
            let typed_model = type_check(model).expect("Model failed to type check");

            let mut runner = tc::queuing_runtime::QueuingMonitorRunner::<
                _,
                _,
                tc::TypedUntimedLolaSemantics,
                _,
            >::new(typed_model, &mut *input_streams);
            runner.monitor_outputs()
        }
        (Runtime::Constraints, Semantics::Untimed) => {
            let mut runner = tc::constraint_based_runtime::ConstraintBasedMonitor::new(
                model,
                &mut *input_streams,
            );
            runner.monitor_outputs()
        }
        _ => unimplemented!(),
    };

    // Print the outputs
    let mut enumerated_outputs = outputs.enumerate();
    while let Some((i, output)) = enumerated_outputs.next().await {
        for (var, data) in output {
            println!("{}[{}] = {:?}", var, i, data);
        }
    }
}
