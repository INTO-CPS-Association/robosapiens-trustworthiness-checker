#![cfg_attr(not(feature = "ros"), allow(unused_imports, dead_code))]

use std::future::Future;

use futures::FutureExt;
use futures::StreamExt;
use macro_rules_attribute::apply;
#[cfg(feature = "ros")]
use r2r::{WrappedTypesupport, std_msgs::msg::Int32};
use smol::LocalExecutor;
use std::rc::Rc;
use trustworthiness_checker::async_test;
use trustworthiness_checker::core::AbstractMonitorBuilder;
use trustworthiness_checker::core::Runnable;
#[cfg(feature = "ros")]
use trustworthiness_checker::io::ros::ROSInputProvider;
#[cfg(feature = "ros")]
use trustworthiness_checker::io::ros::json_to_mapping;
use trustworthiness_checker::io::testing::ManualOutputHandler;
use trustworthiness_checker::lola_fixtures::spec_simple_add_monitor;
use trustworthiness_checker::runtime::asynchronous::AsyncMonitorBuilder;
use trustworthiness_checker::runtime::asynchronous::AsyncMonitorRunner;
use trustworthiness_checker::runtime::asynchronous::Context;
use trustworthiness_checker::semantics::UntimedLolaSemantics;
use trustworthiness_checker::utils::cancellation_token::CancellationToken;
use trustworthiness_checker::{Value, lola_specification};
use winnow::Parser;

/* A simple ROS publisher node which publishes a sequence of values on a topic
 * This creates a ROS node node_name which runs in a background thread
 * until all the values have been published. */
#[cfg(feature = "ros")]
fn dummy_publisher<T: WrappedTypesupport + 'static>(
    ex: Rc<LocalExecutor<'static>>,
    node_name: String,
    topic: String,
    values: Vec<T>,
) -> impl Future<Output = ()> {
    // Create a ROS node and publisher

    use futures::select;
    let ctx = r2r::Context::create().unwrap();
    let mut node = r2r::Node::create(ctx, &*node_name, "").unwrap();
    let publisher = node
        .create_publisher::<T>(&topic, r2r::QosProfile::default())
        .unwrap();

    async move {
        // Cancellation token for managing the lifetime of the background task
        let cancellation_token = CancellationToken::new();

        // Create a drop guard to ensure the background task is not dropped
        // until we are done
        let _drop_guard = cancellation_token.clone().drop_guard();

        // Spawn a background async task to run the ROS node
        // and spin until cancelled
        ex.spawn(async move {
            loop {
                select! {
                    _ = cancellation_token.cancelled().fuse() => {
                        return;
                    },
                    _ = smol::future::yield_now().fuse() => {
                        node.spin_once(std::time::Duration::from_millis(0));
                    },
                }
            }
        })
        .detach();

        // Publish the values on the topic
        for val in values {
            println!("Publishing value: {:?} on topic: {}", val, topic);
            publisher.publish(&val).unwrap();
        }
    }
}

#[cfg(feature = "ros")]
#[apply(async_test)]
async fn test_add_monitor_ros(ex: Rc<LocalExecutor<'static>>) {
    let var_topics = json_to_mapping(
        r#"
        {
            "x": {
                "topic": "/x",
                "msg_type": "Int32"
            },
            "y": {
                "topic": "/y",
                "msg_type": "Int32"
            }
        }
        "#,
    )
    .unwrap();

    let model = lola_specification
        .parse(spec_simple_add_monitor())
        .expect("Model could not be parsed");

    // let pool = smol::LocalExecutor::new();

    let xs = vec![Int32 { data: 1 }, Int32 { data: 2 }];
    let ys = vec![Int32 { data: 3 }, Int32 { data: 4 }];
    let zs = vec![Value::Int(4), Value::Int(6)];

    // Spawn dummy ROS publisher nodes
    ex.spawn(dummy_publisher(
        ex.clone(),
        "x_publisher".to_string(),
        "/x".to_string(),
        xs,
    ))
    .detach();

    ex.spawn(dummy_publisher(
        ex.clone(),
        "y_publisher".to_string(),
        "/y".to_string(),
        ys,
    ))
    .detach();

    // Create the ROS input provider
    let input_provider = ROSInputProvider::new(ex.clone(), var_topics).unwrap();

    let mut output_handler = ManualOutputHandler::new(ex.clone(), vec!["z".into()]);

    let outputs = output_handler.get_output();

    // Create the monitor
    let runner: AsyncMonitorRunner<_, Value, UntimedLolaSemantics, _, Context<Value>> =
        AsyncMonitorBuilder::new()
            .executor(ex.clone())
            .model(model)
            .input(Box::new(input_provider))
            .output(Box::new(output_handler))
            .build();

    // Lauch the monitor runner
    ex.spawn(runner.run()).detach();

    // We have to specify how many outputs we want to take as the ROS
    // topic is not assumed to tell us when it is done
    let outputs = outputs.take(zs.len()).collect::<Vec<_>>().await;
    println!("Outputs: {:?}", outputs);
    let expected_outputs = zs.into_iter().map(|z| vec![z]).collect::<Vec<_>>();

    assert_eq!(outputs, expected_outputs);
}
