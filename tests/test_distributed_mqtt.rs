#[cfg(test)]
#[cfg(feature = "testcontainers")]
mod integration_tests {

    use std::rc::Rc;
    use std::vec;

    use futures::{StreamExt, stream};
    use smol::LocalExecutor;
    use tc_testutils::mqtt::{dummy_stream_mqtt_publisher, get_mqtt_outputs, start_mqtt};
    use tc_testutils::streams::{TickSender, tick_stream, with_timeout, with_timeout_res};
    use tracing::{info, warn};
    use trustworthiness_checker::core::Runnable;
    use trustworthiness_checker::distributed::distribution_graphs::LabelledDistributionGraph;
    use trustworthiness_checker::io::mqtt::MqttFactory;
    use trustworthiness_checker::{OutputStream, lola_fixtures::*};
    use trustworthiness_checker::{Specification, Value};
    use winnow::Parser;

    use macro_rules_attribute::apply;
    use std::collections::BTreeMap;
    use trustworthiness_checker::async_test;

    use trustworthiness_checker::{
        VarName,
        io::mqtt::{MQTTInputProvider, MQTTOutputHandler},
        lola_specification,
        semantics::distributed::localisation::Localisable,
    };

    const MQTT_FACTORY: MqttFactory = MqttFactory::Paho;

    fn generate_test_publisher_tasks(
        executor: Rc<LocalExecutor<'static>>,
        xs: Vec<Value>,
        ys: Vec<Value>,
        zs: Vec<Value>,
        mqtt_port: u16,
    ) -> (
        (TickSender, smol::Task<anyhow::Result<()>>),
        (TickSender, smol::Task<anyhow::Result<()>>),
        (TickSender, smol::Task<anyhow::Result<()>>),
    ) {
        let (x_tick, x_pub_stream) = tick_stream(stream::iter(xs.clone()).boxed());
        let (y_tick, y_pub_stream) = tick_stream(stream::iter(ys.clone()).boxed());
        let (z_tick, z_pub_stream) = tick_stream(stream::iter(zs.clone()).boxed());

        // Spawn dummy MQTT publisher nodes and keep handles to wait for completion
        let x_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_publisher(
                "x_publisher".to_string(),
                "x".to_string(),
                x_pub_stream,
                xs.len(),
                mqtt_port,
            ),
            5,
            "x_publisher_task",
        ));

        let y_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_publisher(
                "y_publisher".to_string(),
                "y".to_string(),
                y_pub_stream,
                ys.len(),
                mqtt_port,
            ),
            5,
            "y_publisher_task",
        ));
        let z_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_publisher(
                "z_publisher".to_string(),
                "z".to_string(),
                z_pub_stream,
                zs.len(),
                mqtt_port,
            ),
            5,
            "z_publisher_task",
        ));

        (
            (x_tick, x_publisher_task),
            (y_tick, y_publisher_task),
            (z_tick, z_publisher_task),
        )
    }

    async fn verify_results(
        mut x_tick: TickSender,
        mut y_tick: TickSender,
        mut z_tick: TickSender,
        mut outputs_v: OutputStream<Value>,
        mut outputs_w: OutputStream<Value>,
    ) -> anyhow::Result<()> {
        x_tick.send(()).await?; // 1
        y_tick.send(()).await?; // 3
        let w_val = with_timeout(outputs_w.next(), 5, "outputs_w.next()")
            .await?
            .expect("outputs_w ended");
        assert_eq!(w_val, Value::Int(4)); // 1 + 3

        z_tick.send(()).await?; // 5
        let v_val = with_timeout(outputs_v.next(), 5, "outputs_v.next()")
            .await?
            .expect("outputs_v ended");
        assert_eq!(v_val, Value::Int(9)); // 4 + 5

        x_tick.send(()).await?; // 2
        let w_val = with_timeout(outputs_w.next(), 5, "outputs_w.next()")
            .await?
            .expect("outputs_w ended");
        assert_eq!(w_val, Value::Int(5)); // 2 + 3
        let v_val = with_timeout(outputs_v.next(), 5, "outputs_v.next()")
            .await?
            .expect("outputs_v ended");
        assert_eq!(v_val, Value::Int(10)); // 5 + 5

        y_tick.send(()).await?; // 4
        let w_val = with_timeout(outputs_w.next(), 5, "outputs_w.next()")
            .await?
            .expect("outputs_w ended");
        assert_eq!(w_val, Value::Int(6)); // 2 + 4
        let v_val = with_timeout(outputs_v.next(), 5, "outputs_v.next()")
            .await?
            .expect("outputs_v ended");
        assert_eq!(v_val, Value::Int(11)); // 6 + 5

        z_tick.send(()).await?; // 6
        let v_val = with_timeout(outputs_v.next(), 5, "outputs_v.next()")
            .await?
            .expect("outputs_v ended");
        assert_eq!(v_val, Value::Int(12)); // 6 + 6

        // Finishing ticks:
        x_tick.send(()).await?;
        y_tick.send(()).await?;
        z_tick.send(()).await?;

        Ok(())
    }

    #[apply(async_test)]
    async fn manually_decomposed_monitor_test(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let model1 = lola_specification
            .parse(spec_simple_add_decomposed_1())
            .expect("Model could not be parsed");
        let model2 = lola_specification
            .parse(spec_simple_add_decomposed_2())
            .expect("Model could not be parsed");

        let xs = vec![Value::Int(1), Value::Int(2)];
        let ys = vec![Value::Int(3), Value::Int(4)];
        let zs = vec![Value::Int(5), Value::Int(6)];

        let var_in_topics_1 = [("x".into(), "x".to_string()), ("y".into(), "y".to_string())];
        let var_out_topics_1 = [("w".into(), "w".to_string())];
        let var_in_topics_2 = [("w".into(), "w".to_string()), ("z".into(), "z".to_string())];
        let var_out_topics_2 = [("v".into(), "v".to_string())];

        let mqtt_server = start_mqtt().await;
        let mqtt_port = mqtt_server
            .get_host_port_ipv4(1883)
            .await
            .expect("Failed to get host port for MQTT server");
        let mqtt_host = "localhost";
        let mut input_provider_1 = MQTTInputProvider::new(
            executor.clone(),
            MQTT_FACTORY,
            mqtt_host,
            Some(mqtt_port),
            var_in_topics_1.iter().cloned().collect(),
            0,
        );
        with_timeout_res(input_provider_1.connect(), 10, "input_provider_1.connect()")
            .await
            .expect("Failed to connect to MQTT with input provider 1");

        let output_handler_1 = MQTTOutputHandler::new(
            executor.clone(),
            MQTT_FACTORY,
            vec!["w".into()],
            mqtt_host,
            Some(mqtt_port),
            var_out_topics_1.into_iter().collect(),
            vec![],
        )
        .expect("Failed to create output handler 1");

        let mut input_provider_2 = MQTTInputProvider::new(
            executor.clone(),
            MQTT_FACTORY,
            mqtt_host,
            Some(mqtt_port),
            var_in_topics_2.iter().cloned().collect(),
            0,
        );
        with_timeout_res(input_provider_2.connect(), 10, "input_provider_2.connect()")
            .await
            .expect("Failed to connect to MQTT with input provider 2");

        let output_handler_2 = MQTTOutputHandler::new(
            executor.clone(),
            MQTT_FACTORY,
            vec!["v".into()],
            mqtt_host,
            Some(mqtt_port),
            var_out_topics_2.into_iter().collect(),
            vec![],
        )
        .expect("Failed to create output handler 2");

        let runner_1 = TestMonitorRunner::new(
            executor.clone(),
            model1.clone(),
            Box::new(input_provider_1),
            Box::new(output_handler_1),
        );
        executor.spawn(runner_1.run()).detach();

        let runner_2 = TestMonitorRunner::new(
            executor.clone(),
            model2.clone(),
            Box::new(input_provider_2),
            Box::new(output_handler_2),
        );
        executor.spawn(runner_2.run()).detach();

        // Get the output stream before starting publishers to ensure subscription is ready
        let outputs_v =
            get_mqtt_outputs("v".to_string(), "v_subscriber".to_string(), mqtt_port).await;
        let outputs_w =
            get_mqtt_outputs("w".to_string(), "w_subscriber".to_string(), mqtt_port).await;

        let ((x_tick, x_publisher_task), (y_tick, y_publisher_task), (z_tick, z_publisher_task)) =
            generate_test_publisher_tasks(
                executor.clone(),
                xs.clone(),
                ys.clone(),
                zs.clone(),
                mqtt_port,
            );

        verify_results(x_tick, y_tick, z_tick, outputs_v, outputs_w).await?;

        // Wait for publishers to complete
        x_publisher_task
            .await
            .expect("X publisher task failed to complete");
        y_publisher_task
            .await
            .expect("Y publisher task failed to complete");
        z_publisher_task
            .await
            .expect("Z publisher task failed to complete");

        Ok(())
    }

    #[apply(async_test)]
    async fn test_localisation_distribution(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let model1 = lola_specification
            .parse(spec_simple_add_decomposed_1())
            .expect("Model could not be parsed");
        let model2 = lola_specification
            .parse(spec_simple_add_decomposed_2())
            .expect("Model could not be parsed");

        let xs = vec![Value::Int(1), Value::Int(2)];
        let ys = vec![Value::Int(3), Value::Int(4)];
        let zs = vec![Value::Int(5), Value::Int(6)];

        let local_spec1 = model1.localise(&vec!["w".into()]);
        let local_spec2 = model2.localise(&vec!["v".into()]);

        let mqtt_server = start_mqtt().await;
        let mqtt_port = mqtt_server
            .get_host_port_ipv4(1883)
            .await
            .expect("Failed to get host port for MQTT server");
        let mqtt_host = "localhost";

        let var_topics1 = local_spec1
            .input_vars()
            .iter()
            .map(|v| (v.clone(), format!("{}", v)))
            .collect();
        warn!(?var_topics1, "Var topics 1");

        let mut input_provider_1 = MQTTInputProvider::new(
            executor.clone(),
            MQTT_FACTORY,
            mqtt_host,
            Some(mqtt_port),
            var_topics1,
            0,
        );
        input_provider_1
            .connect()
            .await
            .expect("Failed to connect to MQTT with input provider 1");

        let var_topics_2 = local_spec2
            .input_vars()
            .iter()
            .map(|v| (v.clone(), format!("{}", v)))
            .collect();
        warn!(?var_topics_2, "Var topics 2");

        let mut input_provider_2 = MQTTInputProvider::new(
            executor.clone(),
            MQTT_FACTORY,
            mqtt_host,
            Some(mqtt_port),
            var_topics_2,
            0,
        );
        input_provider_2
            .connect()
            .await
            .expect("Failed to connect to MQTT with input provider 2");

        let var_out_topics_1: BTreeMap<VarName, String> = local_spec1
            .output_vars()
            .iter()
            .map(|v| (v.clone(), format!("{}", v)))
            .collect();
        warn!(?var_out_topics_1, "Var out topics 1");

        let output_handler_1 = MQTTOutputHandler::new(
            executor.clone(),
            MQTT_FACTORY,
            vec!["w".into()],
            mqtt_host,
            Some(mqtt_port),
            var_out_topics_1,
            vec![],
        )
        .expect("Failed to create output handler 1");
        let var_out_topics_2: BTreeMap<VarName, String> = local_spec2
            .output_vars()
            .iter()
            .map(|v| (v.clone(), format!("{}", v)))
            .collect();
        warn!(?var_out_topics_2, "Var out topics 2");

        let output_handler_2 = MQTTOutputHandler::new(
            executor.clone(),
            MQTT_FACTORY,
            vec!["v".into()],
            mqtt_host,
            Some(mqtt_port),
            var_out_topics_2.into_iter().collect(),
            vec![],
        )
        .expect("Failed to create output handler 2");

        let runner_1 = TestMonitorRunner::new(
            executor.clone(),
            model1.clone(),
            Box::new(input_provider_1),
            Box::new(output_handler_1),
        );

        let runner_2 = TestMonitorRunner::new(
            executor.clone(),
            model2.clone(),
            Box::new(input_provider_2),
            Box::new(output_handler_2),
        );

        executor.spawn(runner_1.run()).detach();
        executor.spawn(runner_2.run()).detach();

        // Get the output stream before starting publishers to ensure subscription is ready
        let outputs_v =
            get_mqtt_outputs("v".to_string(), "v_subscriber".to_string(), mqtt_port).await;
        let outputs_w =
            get_mqtt_outputs("w".to_string(), "w_subscriber".to_string(), mqtt_port).await;

        let ((x_tick, x_publisher_task), (y_tick, y_publisher_task), (z_tick, z_publisher_task)) =
            generate_test_publisher_tasks(
                executor.clone(),
                xs.clone(),
                ys.clone(),
                zs.clone(),
                mqtt_port,
            );

        verify_results(x_tick, y_tick, z_tick, outputs_v, outputs_w).await?;

        // Wait for publishers to complete
        x_publisher_task
            .await
            .expect("X publisher task failed to complete");
        y_publisher_task
            .await
            .expect("Y publisher task failed to complete");
        z_publisher_task
            .await
            .expect("Z publisher task failed to complete");

        Ok(())
    }

    #[apply(async_test)]
    async fn test_localisation_distribution_graphs(
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
        let zs = vec![Value::Int(5), Value::Int(6)];

        info!("Dist graph: {:?}", dist_graph);

        let local_spec1 = model1.localise(&("A".into(), &dist_graph));
        let local_spec2 = model2.localise(&("B".into(), &dist_graph));

        let mqtt_server = start_mqtt().await;
        let mqtt_port = mqtt_server
            .get_host_port_ipv4(1883)
            .await
            .expect("Failed to get host port for MQTT server");
        let mqtt_host = "localhost";

        let mut input_provider_1 = MQTTInputProvider::new(
            executor.clone(),
            MQTT_FACTORY,
            mqtt_host,
            Some(mqtt_port),
            local_spec1
                .input_vars()
                .iter()
                .map(|v| (v.clone(), v.into()))
                .collect(),
            0,
        );
        input_provider_1
            .connect()
            .await
            .expect("Failed to connect to MQTT with input provider 1");

        let mut input_provider_2 = MQTTInputProvider::new(
            executor.clone(),
            MQTT_FACTORY,
            mqtt_host,
            Some(mqtt_port),
            local_spec2
                .input_vars()
                .iter()
                .map(|v| (v.clone(), v.into()))
                .collect(),
            0,
        );
        input_provider_2
            .connect()
            .await
            .expect("Failed to connect to MQTT with input provider 2");

        let var_out_topics_1: BTreeMap<VarName, String> = local_spec1
            .output_vars()
            .iter()
            .map(|v| (v.clone(), format!("{}", v)))
            .collect();
        let output_handler_1 = MQTTOutputHandler::new(
            executor.clone(),
            MQTT_FACTORY,
            vec!["w".into()],
            mqtt_host,
            Some(mqtt_port),
            var_out_topics_1,
            vec![],
        )
        .expect("Failed to create output handler 1");
        let var_out_topics_2: BTreeMap<VarName, String> = local_spec2
            .output_vars()
            .iter()
            .map(|v| (v.clone(), format!("{}", v)))
            .collect();
        let output_handler_2 = MQTTOutputHandler::new(
            executor.clone(),
            MQTT_FACTORY,
            vec!["v".into()],
            mqtt_host,
            Some(mqtt_port),
            var_out_topics_2.into_iter().collect(),
            vec![],
        )
        .expect("Failed to create output handler 2");

        let runner_1 = TestMonitorRunner::new(
            executor.clone(),
            model1.clone(),
            Box::new(input_provider_1),
            Box::new(output_handler_1),
        );

        let runner_2 = TestMonitorRunner::new(
            executor.clone(),
            model2.clone(),
            Box::new(input_provider_2),
            Box::new(output_handler_2),
        );

        executor.spawn(runner_1.run()).detach();
        executor.spawn(runner_2.run()).detach();

        let outputs_v =
            get_mqtt_outputs("v".to_string(), "v_subscriber".to_string(), mqtt_port).await;
        let outputs_w =
            get_mqtt_outputs("w".to_string(), "w_subscriber".to_string(), mqtt_port).await;

        let ((x_tick, x_publisher_task), (y_tick, y_publisher_task), (z_tick, z_publisher_task)) =
            generate_test_publisher_tasks(
                executor.clone(),
                xs.clone(),
                ys.clone(),
                zs.clone(),
                mqtt_port,
            );

        verify_results(x_tick, y_tick, z_tick, outputs_v, outputs_w).await?;

        // Wait for publishers to complete
        x_publisher_task
            .await
            .expect("X publisher task failed to complete");
        y_publisher_task
            .await
            .expect("Y publisher task failed to complete");
        z_publisher_task
            .await
            .expect("Z publisher task failed to complete");

        Ok(())
    }
}
