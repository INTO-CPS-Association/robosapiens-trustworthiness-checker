#![deny(warnings)]
use futures::StreamExt;
use trustworthiness_checker::{
    self as tc, parse_file, ros_input_provider, type_checking::type_check, Monitor,
};

use clap::Parser;

use trustworthiness_checker::commandline_args::{CliROS, Language, Runtime, Semantics};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Could use tokio-console for debugging
    // console_subscriber::init();
    let cli = CliROS::parse();

    let language = cli.language.unwrap_or(Language::Lola);
    let semantics = cli.semantics.unwrap_or(Semantics::Untimed);
    let runtime = cli.runtime.unwrap_or(Runtime::Async);

    let model_parser = match language {
        Language::Lola => tc::parser::lola_specification,
    };

    let input_mapping_str = std::fs::read_to_string(&cli.ros_input_mapping_file)
        .expect("Input mapping file could not be read");
    let input_mapping = tc::ros_topic_stream_mapping::json_to_mapping(&input_mapping_str)
        .expect("Input mapping file could not be parsed");

    let input_streams = ros_input_provider::ROSInputProvider::new(input_mapping)
        .expect("ROS input provider could not be created");

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
                input_streams,
            );
            runner.monitor_outputs()
        }
        (Runtime::Queuing, Semantics::Untimed) => {
            let mut runner = tc::queuing_runtime::QueuingMonitorRunner::<
                _,
                _,
                tc::UntimedLolaSemantics,
                _,
            >::new(model, input_streams);
            runner.monitor_outputs()
        }
        (Runtime::Async, Semantics::TypedUntimed) => {
            let typed_model = type_check(model).expect("Model failed to type check");
            // let typed_input_streams = d

            let mut runner = tc::AsyncMonitorRunner::<_, _, tc::TypedUntimedLolaSemantics, _>::new(
                typed_model,
                input_streams,
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
            >::new(typed_model, input_streams);
            runner.monitor_outputs()
        }
        (Runtime::Constraints, Semantics::Untimed) => {
            let mut runner =
                tc::constraint_based_runtime::ConstraintBasedMonitor::new(model, input_streams);
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
