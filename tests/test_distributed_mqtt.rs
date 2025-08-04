use std::rc::Rc;
use std::vec;

use futures::StreamExt;
use smol::LocalExecutor;
use tc_testutils::mqtt::{dummy_mqtt_publisher, get_mqtt_outputs, start_mqtt};
use tracing::info;
use trustworthiness_checker::core::Runnable;
use trustworthiness_checker::distributed::distribution_graphs::LabelledDistributionGraph;
use trustworthiness_checker::lola_fixtures::*;
use trustworthiness_checker::{Specification, Value};
use winnow::Parser;

use macro_rules_attribute::apply;
use std::collections::BTreeMap;
use trustworthiness_checker::async_test;

use trustworthiness_checker::{
    InputProvider, VarName,
    dep_manage::interface::{DependencyKind, create_dependency_manager},
    io::mqtt::{MQTTInputProvider, MQTTOutputHandler},
    lola_specification,
    runtime::asynchronous::AsyncMonitorRunner,
    semantics::{UntimedLolaSemantics, distributed::localisation::Localisable},
};

#[cfg_attr(not(feature = "testcontainers"), ignore)]
#[apply(async_test)]
async fn manually_decomposed_monitor_test(executor: Rc<LocalExecutor<'static>>) {
    let model1 = lola_specification
        .parse(spec_simple_add_decomposed_1())
        .expect("Model could not be parsed");
    let model2 = lola_specification
        .parse(spec_simple_add_decomposed_2())
        .expect("Model could not be parsed");

    let xs = vec![Value::Int(1), Value::Int(2)];
    let ys = vec![Value::Int(3), Value::Int(4)];
    let zs = vec![Value::Int(4), Value::Int(6)];

    let var_in_topics_1 = [
        ("x".into(), "mqtt_input_dec_x".to_string()),
        ("y".into(), "mqtt_input_dec_y".to_string()),
    ];
    let var_out_topics_1 = [("w".into(), "mqtt_input_dec_w".to_string())];
    let var_in_topics_2 = [
        ("w".into(), "mqtt_input_dec_w".to_string()),
        ("z".into(), "mqtt_input_dec_z".to_string()),
    ];
    let var_out_topics_2 = [("v".into(), "mqtt_output_dec_v".to_string())];

    let mqtt_server = start_mqtt().await;
    let mqtt_port = mqtt_server
        .get_host_port_ipv4(1883)
        .await
        .expect("Failed to get host port for MQTT server");
    let mqtt_host = "localhost";
    let input_provider_1 = MQTTInputProvider::new(
        executor.clone(),
        mqtt_host,
        Some(mqtt_port),
        var_in_topics_1.iter().cloned().collect(),
        0,
    )
    .expect("Failed to create input provider 1");

    let output_handler_1 = MQTTOutputHandler::new(
        executor.clone(),
        vec!["w".into()],
        mqtt_host,
        Some(mqtt_port),
        var_out_topics_1.into_iter().collect(),
    )
    .expect("Failed to create output handler 1");

    let input_provider_2 = MQTTInputProvider::new(
        executor.clone(),
        mqtt_host,
        Some(mqtt_port),
        var_in_topics_2.iter().cloned().collect(),
        0,
    )
    .expect("Failed to create input provider 2");

    let output_handler_2 = MQTTOutputHandler::new(
        executor.clone(),
        vec!["v".into()],
        mqtt_host,
        Some(mqtt_port),
        var_out_topics_2.into_iter().collect(),
    )
    .expect("Failed to create output handler 2");

    let input_provider_1_ready = input_provider_1.ready();
    let input_provider_2_ready = input_provider_2.ready();

    let runner_1 = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        model1.clone(),
        Box::new(input_provider_1),
        Box::new(output_handler_1),
        create_dependency_manager(DependencyKind::Empty, model1),
    );
    executor.spawn(runner_1.run()).detach();
    input_provider_1_ready
        .await
        .expect("Input provider 1 should be ready");

    let runner_2 = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        model2.clone(),
        Box::new(input_provider_2),
        Box::new(output_handler_2),
        create_dependency_manager(DependencyKind::Empty, model2),
    );
    executor.spawn(runner_2.run()).detach();
    input_provider_2_ready
        .await
        .expect("Input provider 2 should be ready");

    // Get the output stream before starting publishers to ensure subscription is ready
    let outputs_z = get_mqtt_outputs(
        "mqtt_output_dec_v".to_string(),
        "v_subscriber".to_string(),
        mqtt_port,
    )
    .await;

    // Start publishers for x and y inputs and keep task handles
    let x_publisher_task = executor.spawn(dummy_mqtt_publisher(
        "x_dec_publisher".to_string(),
        "mqtt_input_dec_x".to_string(),
        xs,
        mqtt_port,
    ));

    let y_publisher_task = executor.spawn(dummy_mqtt_publisher(
        "y_dec_publisher".to_string(),
        "mqtt_input_dec_y".to_string(),
        ys,
        mqtt_port,
    ));

    let z_publisher_task = executor.spawn(dummy_mqtt_publisher(
        "z_dec_publisher".to_string(),
        "mqtt_input_dec_z".to_string(),
        zs,
        mqtt_port,
    ));

    // Collect outputs
    let outputs = outputs_z.take(2).collect::<Vec<_>>().await;
    assert_eq!(outputs, vec![Value::Int(8), Value::Int(12)]);

    // Wait for publishers to complete
    x_publisher_task.await;
    y_publisher_task.await;
    z_publisher_task.await;
}

#[cfg_attr(not(feature = "testcontainers"), ignore)]
#[apply(async_test)]
async fn localisation_distribution_test(executor: Rc<LocalExecutor<'static>>) {
    let model1 = lola_specification
        .parse(spec_simple_add_decomposed_1())
        .expect("Model could not be parsed");
    let model2 = lola_specification
        .parse(spec_simple_add_decomposed_2())
        .expect("Model could not be parsed");

    let xs = vec![Value::Int(1), Value::Int(2)];
    let ys = vec![Value::Int(3), Value::Int(4)];
    let zs = vec![Value::Int(4), Value::Int(6)];

    let local_spec1 = model1.localise(&vec!["w".into()]);
    let local_spec2 = model2.localise(&vec!["v".into()]);

    let mqtt_server = start_mqtt().await;
    let mqtt_port = mqtt_server
        .get_host_port_ipv4(1883)
        .await
        .expect("Failed to get host port for MQTT server");
    let mqtt_host = "localhost";

    let input_provider_1 = MQTTInputProvider::new(
        executor.clone(),
        mqtt_host,
        Some(mqtt_port),
        local_spec1
            .input_vars()
            .iter()
            .map(|v| (v.clone(), format!("{}", v)))
            .collect(),
        0,
    )
    .expect("Failed to create input provider 1");
    input_provider_1
        .ready()
        .await
        .expect("Input provider 1 should be ready");
    let input_provider_2 = MQTTInputProvider::new(
        executor.clone(),
        mqtt_host,
        Some(mqtt_port),
        local_spec2
            .input_vars()
            .iter()
            .map(|v| (v.clone(), format!("{}", v)))
            .collect(),
        0,
    )
    .expect("Failed to create input provider 2");
    input_provider_2
        .ready()
        .await
        .expect("Input provider 2 should be ready");

    let var_out_topics_1: BTreeMap<VarName, String> = local_spec1
        .output_vars()
        .iter()
        .map(|v| (v.clone(), format!("{}", v)))
        .collect();
    let output_handler_1 = MQTTOutputHandler::new(
        executor.clone(),
        vec!["w".into()],
        mqtt_host,
        Some(mqtt_port),
        var_out_topics_1,
    )
    .expect("Failed to create output handler 1");
    let var_out_topics_2: BTreeMap<VarName, String> = local_spec2
        .output_vars()
        .iter()
        .map(|v| (v.clone(), format!("{}", v)))
        .collect();
    let output_handler_2 = MQTTOutputHandler::new(
        executor.clone(),
        vec!["v".into()],
        mqtt_host,
        Some(mqtt_port),
        var_out_topics_2.into_iter().collect(),
    )
    .expect("Failed to create output handler 2");

    let runner_1 = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        model1.clone(),
        Box::new(input_provider_1),
        Box::new(output_handler_1),
        create_dependency_manager(DependencyKind::Empty, model1),
    );

    let runner_2 = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        model2.clone(),
        Box::new(input_provider_2),
        Box::new(output_handler_2),
        create_dependency_manager(DependencyKind::Empty, model2),
    );

    executor.spawn(runner_1.run()).detach();
    executor.spawn(runner_2.run()).detach();

    // Get the output stream before starting publishers to ensure subscription is ready
    let outputs_z = get_mqtt_outputs("v".to_string(), "v_subscriber".to_string(), mqtt_port).await;

    // Start publishers and keep task handles
    let x_publisher_task = executor.spawn(dummy_mqtt_publisher(
        "x_dec_publisher".to_string(),
        "x".to_string(),
        xs,
        mqtt_port,
    ));
    let y_publisher_task = executor.spawn(dummy_mqtt_publisher(
        "y_dec_publisher".to_string(),
        "y".to_string(),
        ys,
        mqtt_port,
    ));
    let z_publisher_task = executor.spawn(dummy_mqtt_publisher(
        "z_dec_publisher".to_string(),
        "z".to_string(),
        zs,
        mqtt_port,
    ));

    // Collect outputs
    let outputs = outputs_z.take(2).collect::<Vec<_>>().await;
    assert_eq!(outputs, vec![Value::Int(8), Value::Int(12)]);

    // Wait for publishers to complete
    x_publisher_task.await;
    y_publisher_task.await;
    z_publisher_task.await;
}

#[cfg_attr(not(feature = "testcontainers"), ignore)]
#[apply(async_test)]
async fn localisation_distribution_graphs_test(
    executor: Rc<LocalExecutor<'static>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let model1 = lola_specification
        .parse(spec_simple_add_decomposed_1())
        .expect("Model could not be parsed");
    let model2 = lola_specification
        .parse(spec_simple_add_decomposed_2())
        .expect("Model could not be parsed");

    let file_content =
        smol::fs::read_to_string("fixtures/simple_add_distribution_graph.json").await?;
    let dist_graph: LabelledDistributionGraph = serde_json::from_str(&file_content)?;

    let xs = vec![Value::Int(1), Value::Int(2)];
    let ys = vec![Value::Int(3), Value::Int(4)];
    let zs = vec![Value::Int(4), Value::Int(6)];

    info!("Dist graph: {:?}", dist_graph);

    let local_spec1 = model1.localise(&("A".into(), &dist_graph));
    let local_spec2 = model2.localise(&("B".into(), &dist_graph));

    let mqtt_server = start_mqtt().await;
    let mqtt_port = mqtt_server
        .get_host_port_ipv4(1883)
        .await
        .expect("Failed to get host port for MQTT server");
    let mqtt_host = "localhost";

    let input_provider_1 = MQTTInputProvider::new(
        executor.clone(),
        mqtt_host,
        Some(mqtt_port),
        local_spec1
            .input_vars()
            .iter()
            .map(|v| (v.clone(), v.into()))
            .collect(),
        0,
    )
    .expect("Failed to create input provider 1");
    input_provider_1
        .ready()
        .await
        .expect("Input provider 1 should be ready");
    let input_provider_2 = MQTTInputProvider::new(
        executor.clone(),
        mqtt_host,
        Some(mqtt_port),
        local_spec2
            .input_vars()
            .iter()
            .map(|v| (v.clone(), v.into()))
            .collect(),
        0,
    )
    .expect("Failed to create input provider 2");
    input_provider_2
        .ready()
        .await
        .expect("Input provider 2 should be ready");

    let var_out_topics_1: BTreeMap<VarName, String> = local_spec1
        .output_vars()
        .iter()
        .map(|v| (v.clone(), format!("{}", v)))
        .collect();
    let output_handler_1 = MQTTOutputHandler::new(
        executor.clone(),
        vec!["w".into()],
        mqtt_host,
        Some(mqtt_port),
        var_out_topics_1,
    )
    .expect("Failed to create output handler 1");
    let var_out_topics_2: BTreeMap<VarName, String> = local_spec2
        .output_vars()
        .iter()
        .map(|v| (v.clone(), format!("{}", v)))
        .collect();
    let output_handler_2 = MQTTOutputHandler::new(
        executor.clone(),
        vec!["v".into()],
        mqtt_host,
        Some(mqtt_port),
        var_out_topics_2.into_iter().collect(),
    )
    .expect("Failed to create output handler 2");

    let runner_1 = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        model1.clone(),
        Box::new(input_provider_1),
        Box::new(output_handler_1),
        create_dependency_manager(DependencyKind::Empty, model1),
    );

    let runner_2 = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        model2.clone(),
        Box::new(input_provider_2),
        Box::new(output_handler_2),
        create_dependency_manager(DependencyKind::Empty, model2),
    );

    executor.spawn(runner_1.run()).detach();
    executor.spawn(runner_2.run()).detach();

    // Get the output stream before starting publishers to ensure subscription is ready
    let outputs_z = get_mqtt_outputs("v".to_string(), "v_subscriber".to_string(), mqtt_port).await;

    // Start publishers and keep task handles
    let x_publisher_task = executor.spawn(dummy_mqtt_publisher(
        "x_dec_publisher".to_string(),
        "x".to_string(),
        xs,
        mqtt_port,
    ));
    let y_publisher_task = executor.spawn(dummy_mqtt_publisher(
        "y_dec_publisher".to_string(),
        "y".to_string(),
        ys,
        mqtt_port,
    ));
    let z_publisher_task = executor.spawn(dummy_mqtt_publisher(
        "z_dec_publisher".to_string(),
        "z".to_string(),
        zs,
        mqtt_port,
    ));

    // Collect outputs
    let outputs = outputs_z.take(2).collect::<Vec<_>>().await;
    assert_eq!(outputs, vec![Value::Int(8), Value::Int(12)]);

    // Wait for publishers to complete
    x_publisher_task.await;
    y_publisher_task.await;
    z_publisher_task.await;

    Ok(())
}
