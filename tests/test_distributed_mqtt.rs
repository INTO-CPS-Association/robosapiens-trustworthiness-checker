#![allow(warnings)]
use std::fmt::Result as FmtResult;
use std::rc::Rc;
use std::time::Duration;
use std::{future::Future, vec};

use futures::{StreamExt, executor, stream};
use paho_mqtt as mqtt;
use smol::LocalExecutor;
use std::pin::Pin;
use std::{mem, task};
use tokio::fs::File;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument};
use trustworthiness_checker::distributed::distribution_graphs::LabelledDistributionGraph;
use trustworthiness_checker::io::mqtt::client::{
    provide_mqtt_client, provide_mqtt_client_with_subscription,
};
use trustworthiness_checker::lola_fixtures::*;
use trustworthiness_checker::{OutputStream, Specification, Value};
use winnow::Parser;

use async_compat::Compat as TokioCompat;
use async_once_cell::Lazy;
use macro_rules_attribute::apply;
use smol_macros::test as smol_test;
use std::collections::BTreeMap;
use test_log::test;
use testcontainers_modules::testcontainers::core::WaitFor;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, Mount};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{
    ContainerAsync as ContainerAsyncTokio, GenericImage, Image, ImageExt,
};
use tokio::time::sleep;
use tracing::info_span;
use trustworthiness_checker::{
    Monitor, VarName,
    dep_manage::interface::{DependencyKind, create_dependency_manager},
    io::{
        mqtt::{MQTTInputProvider, MQTTOutputHandler},
        testing::manual_output_handler::ManualOutputHandler,
    },
    lola_specification,
    runtime::asynchronous::AsyncMonitorRunner,
    semantics::{UntimedLolaSemantics, distributed::localisation::Localisable},
};

struct ContainerAsync<T: Image> {
    inner: Option<ContainerAsyncTokio<T>>,
}

impl<T: Image> ContainerAsync<T> {
    fn new(inner: ContainerAsyncTokio<T>) -> Self {
        Self { inner: Some(inner) }
    }

    async fn get_host_port_ipv4(
        &self,
        port: u16,
    ) -> Result<u16, testcontainers_modules::testcontainers::TestcontainersError> {
        TokioCompat::new(self.inner.as_ref().unwrap().get_host_port_ipv4(port)).await
    }
}

impl<T: Image> Drop for ContainerAsync<T> {
    fn drop(&mut self) {
        let inner = mem::take(&mut self.inner);
        TokioCompat::new(async move { mem::drop(inner) });
    }
}

#[instrument(level = tracing::Level::INFO)]
async fn start_emqx() -> ContainerAsync<GenericImage> {
    let image = GenericImage::new("emqx/emqx", "5.8.3")
        .with_wait_for(WaitFor::message_on_stdout("EMQX 5.8.3 is running now!"))
        .with_exposed_port(1883_u16.tcp())
        .with_startup_timeout(Duration::from_secs(30));
    // .with_mount(Mount::bind_mount("/tmp/emqx_logs", "/opt/emqx/log"));

    ContainerAsync::new(
        TokioCompat::new(image.start())
            .await
            .expect("Failed to start EMQX test container"),
    )
}

#[instrument(level = tracing::Level::INFO)]
async fn get_outputs(topic: String, client_name: String, port: u16) -> OutputStream<Value> {
    // Create a new client
    let (mqtt_client, mut stream) =
        provide_mqtt_client_with_subscription(format!("tcp://localhost:{}", port))
            .await
            .expect("Failed to create MQTT client");
    info!(
        "Received client for Z with client_id: {:?}",
        mqtt_client.client_id()
    );

    // Try to get the messages
    //let mut stream = mqtt_client.clone().get_stream(10);
    mqtt_client.subscribe(topic, 1).await.unwrap();
    info!("Subscribed to Z outputs");
    return Box::pin(stream.map(|msg| {
        let binding = msg;
        let payload = binding.payload_str();
        let res = serde_json::from_str(&payload).unwrap();
        debug!(name:"Received message", ?res, topic=?binding.topic());
        res
    }));
}

#[instrument(level = tracing::Level::INFO)]
async fn dummy_publisher(client_name: String, topic: String, values: Vec<Value>, port: u16) {
    // Create a new client
    let mqtt_client = provide_mqtt_client(format!("tcp://localhost:{}", port))
        .await
        .expect("Failed to create MQTT client");

    // Try to send the messages
    let mut output_strs = values.iter().map(|val| serde_json::to_string(val).unwrap());
    while let Some(output_str) = output_strs.next() {
        let message = mqtt::Message::new(topic.clone(), output_str.clone(), 1);
        match mqtt_client.publish(message.clone()).await {
            Ok(_) => {
                debug!(name: "Published message", ?message, topic=?message.topic());
            }
            Err(e) => {
                panic!("Lost MQTT connection with error {:?}.", e);
            }
        }
    }
}

#[cfg_attr(not(feature = "testcontainers"), ignore)]
#[test(apply(smol_test))]
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

    let emqx_server = start_emqx().await;
    let mqtt_port = emqx_server
        .get_host_port_ipv4(1883)
        .await
        .expect("Failed to get host port for EMQX server");
    let mqtt_host = format!("tcp://localhost:{}", mqtt_port);

    let mut input_provider_1 = MQTTInputProvider::new(
        executor.clone(),
        mqtt_host.as_str(),
        var_in_topics_1.iter().cloned().collect(),
    )
    .expect("Failed to create input provider 1");
    input_provider_1
        .started
        .wait_for(|x| info_span!("Waited for input provider 1 started").in_scope(|| *x))
        .await;
    let mut input_provider_2 = MQTTInputProvider::new(
        executor.clone(),
        mqtt_host.as_str(),
        var_in_topics_2.iter().cloned().collect(),
    )
    .expect("Failed to create input provider 2");
    input_provider_2
        .started
        .wait_for(|x| info_span!("Waited for input provider 2 started").in_scope(|| *x))
        .await;

    let mut output_handler_1 = MQTTOutputHandler::new(
        executor.clone(),
        vec!["w".into()],
        mqtt_host.as_str(),
        var_out_topics_1.into_iter().collect(),
    )
    .expect("Failed to create output handler 1");
    let mut output_handler_2 = MQTTOutputHandler::new(
        executor.clone(),
        vec!["v".into()],
        mqtt_host.as_str(),
        var_out_topics_2.into_iter().collect(),
    )
    .expect("Failed to create output handler 2");

    let mut runner_1 = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        model1.clone(),
        Box::new(input_provider_1),
        Box::new(output_handler_1),
        create_dependency_manager(DependencyKind::Empty, model1),
    );

    let mut runner_2 = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        model2.clone(),
        Box::new(input_provider_2),
        Box::new(output_handler_2),
        create_dependency_manager(DependencyKind::Empty, model2),
    );

    executor.spawn(runner_1.run()).detach();
    executor.spawn(runner_2.run()).detach();

    executor
        .spawn(dummy_publisher(
            "x_dec_publisher".to_string(),
            "mqtt_input_dec_x".to_string(),
            xs,
            mqtt_port,
        ))
        .detach();
    executor
        .spawn(dummy_publisher(
            "y_dec_publisher".to_string(),
            "mqtt_input_dec_y".to_string(),
            ys,
            mqtt_port,
        ))
        .detach();
    executor
        .spawn(dummy_publisher(
            "z_dec_publisher".to_string(),
            "mqtt_input_dec_z".to_string(),
            zs,
            mqtt_port,
        ))
        .detach();

    let outputs_z = get_outputs(
        "mqtt_output_dec_v".to_string(),
        "v_subscriber".to_string(),
        mqtt_port,
    )
    .await;

    assert_eq!(
        outputs_z.take(2).collect::<Vec<_>>().await,
        vec![Value::Int(8), Value::Int(12)]
    );
}

#[cfg_attr(not(feature = "testcontainers"), ignore)]
#[test(apply(smol_test))]
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

    let emqx_server = start_emqx().await;
    let mqtt_port = emqx_server
        .get_host_port_ipv4(1883)
        .await
        .expect("Failed to get host port for EMQX server");
    let mqtt_host = format!("tcp://localhost:{}", mqtt_port);

    let mut input_provider_1 = MQTTInputProvider::new(
        executor.clone(),
        mqtt_host.as_str(),
        local_spec1
            .input_vars()
            .iter()
            .map(|v| (v.clone(), format!("{}", v)))
            .collect(),
    )
    .expect("Failed to create input provider 1");
    input_provider_1
        .started
        .wait_for(|x| info_span!("Waited for input provider 1 started").in_scope(|| *x))
        .await;
    let mut input_provider_2 = MQTTInputProvider::new(
        executor.clone(),
        mqtt_host.as_str(),
        local_spec2
            .input_vars()
            .iter()
            .map(|v| (v.clone(), format!("{}", v)))
            .collect(),
    )
    .expect("Failed to create input provider 2");
    input_provider_2
        .started
        .wait_for(|x| info_span!("Waited for input provider 2 started").in_scope(|| *x))
        .await;

    let var_out_topics_1: BTreeMap<VarName, String> = local_spec1
        .output_vars()
        .iter()
        .map(|v| (v.clone(), format!("{}", v)))
        .collect();
    let mut output_handler_1 = MQTTOutputHandler::new(
        executor.clone(),
        vec!["w".into()],
        mqtt_host.as_str(),
        var_out_topics_1,
    )
    .expect("Failed to create output handler 1");
    let var_out_topics_2: BTreeMap<VarName, String> = local_spec2
        .output_vars()
        .iter()
        .map(|v| (v.clone(), format!("{}", v)))
        .collect();
    let mut output_handler_2 = MQTTOutputHandler::new(
        executor.clone(),
        vec!["v".into()],
        mqtt_host.as_str(),
        var_out_topics_2.into_iter().collect(),
    )
    .expect("Failed to create output handler 2");

    let mut runner_1 = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        model1.clone(),
        Box::new(input_provider_1),
        Box::new(output_handler_1),
        create_dependency_manager(DependencyKind::Empty, model1),
    );

    let mut runner_2 = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        model2.clone(),
        Box::new(input_provider_2),
        Box::new(output_handler_2),
        create_dependency_manager(DependencyKind::Empty, model2),
    );

    executor.spawn(runner_1.run()).detach();
    executor.spawn(runner_2.run()).detach();

    executor
        .spawn(dummy_publisher(
            "x_dec_publisher".to_string(),
            "x".to_string(),
            xs,
            mqtt_port,
        ))
        .detach();
    executor
        .spawn(dummy_publisher(
            "y_dec_publisher".to_string(),
            "y".to_string(),
            ys,
            mqtt_port,
        ))
        .detach();
    executor
        .spawn(dummy_publisher(
            "z_dec_publisher".to_string(),
            "z".to_string(),
            zs,
            mqtt_port,
        ))
        .detach();

    let outputs_z = get_outputs("v".to_string(), "v_subscriber".to_string(), mqtt_port).await;

    assert_eq!(
        outputs_z.take(2).collect::<Vec<_>>().await,
        vec![Value::Int(8), Value::Int(12)]
    );
}

#[cfg_attr(not(feature = "testcontainers"), ignore)]
#[test(apply(smol_test))]
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
        smol::fs::read_to_string("examples/simple_add_distribution_graph.json").await?;
    let dist_graph: LabelledDistributionGraph = serde_json::from_str(&file_content)?;

    let xs = vec![Value::Int(1), Value::Int(2)];
    let ys = vec![Value::Int(3), Value::Int(4)];
    let zs = vec![Value::Int(4), Value::Int(6)];

    info!("Dist graph: {:?}", dist_graph);

    let local_spec1 = model1.localise(&("A".into(), &dist_graph));
    let local_spec2 = model2.localise(&("B".into(), &dist_graph));

    let emqx_server = start_emqx().await;
    let mqtt_port = emqx_server
        .get_host_port_ipv4(1883)
        .await
        .expect("Failed to get host port for EMQX server");
    let mqtt_host = format!("tcp://localhost:{}", mqtt_port);

    let mut input_provider_1 = MQTTInputProvider::new(
        executor.clone(),
        mqtt_host.as_str(),
        local_spec1
            .input_vars()
            .iter()
            .map(|v| (v.clone(), v.into()))
            .collect(),
    )
    .expect("Failed to create input provider 1");
    input_provider_1
        .started
        .wait_for(|x| info_span!("Waited for input provider 1 started").in_scope(|| *x))
        .await;
    let mut input_provider_2 = MQTTInputProvider::new(
        executor.clone(),
        mqtt_host.as_str(),
        local_spec2
            .input_vars()
            .iter()
            .map(|v| (v.clone(), v.into()))
            .collect(),
    )
    .expect("Failed to create input provider 2");
    input_provider_2
        .started
        .wait_for(|x| info_span!("Waited for input provider 2 started").in_scope(|| *x))
        .await;

    let var_out_topics_1: BTreeMap<VarName, String> = local_spec1
        .output_vars()
        .iter()
        .map(|v| (v.clone(), format!("{}", v)))
        .collect();
    let mut output_handler_1 = MQTTOutputHandler::new(
        executor.clone(),
        vec!["w".into()],
        mqtt_host.as_str(),
        var_out_topics_1,
    )
    .expect("Failed to create output handler 1");
    let var_out_topics_2: BTreeMap<VarName, String> = local_spec2
        .output_vars()
        .iter()
        .map(|v| (v.clone(), format!("{}", v)))
        .collect();
    let mut output_handler_2 = MQTTOutputHandler::new(
        executor.clone(),
        vec!["v".into()],
        mqtt_host.as_str(),
        var_out_topics_2.into_iter().collect(),
    )
    .expect("Failed to create output handler 2");

    let mut runner_1 = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        model1.clone(),
        Box::new(input_provider_1),
        Box::new(output_handler_1),
        create_dependency_manager(DependencyKind::Empty, model1),
    );

    let mut runner_2 = AsyncMonitorRunner::<_, _, UntimedLolaSemantics, _, _>::new(
        executor.clone(),
        model2.clone(),
        Box::new(input_provider_2),
        Box::new(output_handler_2),
        create_dependency_manager(DependencyKind::Empty, model2),
    );

    executor.spawn(runner_1.run()).detach();
    executor.spawn(runner_2.run()).detach();

    executor
        .spawn(dummy_publisher(
            "x_dec_publisher".to_string(),
            "x".to_string(),
            xs,
            mqtt_port,
        ))
        .detach();
    executor
        .spawn(dummy_publisher(
            "y_dec_publisher".to_string(),
            "y".to_string(),
            ys,
            mqtt_port,
        ))
        .detach();
    executor
        .spawn(dummy_publisher(
            "z_dec_publisher".to_string(),
            "z".to_string(),
            zs,
            mqtt_port,
        ))
        .detach();

    let outputs_z = get_outputs("v".to_string(), "v_subscriber".to_string(), mqtt_port).await;

    assert_eq!(
        outputs_z.take(2).collect::<Vec<_>>().await,
        vec![Value::Int(8), Value::Int(12)]
    );

    Ok(())
}
