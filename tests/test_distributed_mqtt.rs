#[cfg(test)]
#[cfg(feature = "testcontainers")]
mod integration_tests {

    use std::rc::Rc;
    use std::time::Duration;
    use std::vec;

    use futures::{StreamExt, stream};
    use smol::LocalExecutor;
    use tc_testutils::mqtt::{
        dummy_mqtt_publisher, dummy_stream_mqtt_publisher, get_mqtt_outputs, start_mqtt,
    };
    use tc_testutils::streams::{TickSender, tick_stream, with_timeout, with_timeout_res};
    use tracing::{info, warn};
    use trustworthiness_checker::cli::args::OutputMode;
    use trustworthiness_checker::core::{AbstractMonitorBuilder, Runnable};
    use trustworthiness_checker::distributed::distribution_graphs::LabelledDistributionGraph;
    use trustworthiness_checker::io::InputProviderBuilder;
    use trustworthiness_checker::io::builders::OutputHandlerBuilder;
    use trustworthiness_checker::io::mqtt::{MQTTLocalityReceiver, MqttFactory, MqttMessage};
    use trustworthiness_checker::lang::dynamic_lola::parser::CombExprParser;
    use trustworthiness_checker::runtime::asynchronous::Context;
    use trustworthiness_checker::runtime::reconfigurable_async::ReconfAsyncMonitorBuilder;
    use trustworthiness_checker::semantics::UntimedLolaSemantics;
    use trustworthiness_checker::{LOLASpecification, OutputStream, lola_fixtures::*};
    use trustworthiness_checker::{Specification, Value};
    use winnow::Parser;

    use macro_rules_attribute::apply;
    use std::collections::BTreeMap;
    use trustworthiness_checker::async_test;

    use trustworthiness_checker::{
        InputProvider, VarName,
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

        let input_provider_1_ready =
            with_timeout_res(input_provider_1.ready(), 5, "input_provider_1.ready");
        let input_provider_2_ready =
            with_timeout_res(input_provider_2.ready(), 5, "input_provider_2.ready");

        let runner_1 = TestMonitorRunner::new(
            executor.clone(),
            model1.clone(),
            Box::new(input_provider_1),
            Box::new(output_handler_1),
        );
        executor.spawn(runner_1.run()).detach();
        input_provider_1_ready
            .await
            .expect("Input provider 1 should be ready");

        let runner_2 = TestMonitorRunner::new(
            executor.clone(),
            model2.clone(),
            Box::new(input_provider_2),
            Box::new(output_handler_2),
        );
        executor.spawn(runner_2.run()).detach();
        input_provider_2_ready
            .await
            .expect("Input provider 2 should be ready");

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

        let input_provider_1_ready =
            with_timeout_res(input_provider_1.ready(), 5, "input_provider_1.ready");
        let input_provider_2_ready =
            with_timeout_res(input_provider_2.ready(), 5, "input_provider_2.ready");

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

        input_provider_1_ready
            .await
            .expect("Input provider 1 should be ready");
        input_provider_2_ready
            .await
            .expect("Input provider 2 should be ready");

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

        let input_provider_1_ready =
            with_timeout_res(input_provider_1.ready(), 10, "input_provider_1.ready");
        let input_provider_2_ready =
            with_timeout_res(input_provider_2.ready(), 10, "input_provider_2.ready");

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

        input_provider_1_ready
            .await
            .expect("Input provider 1 should be ready");
        input_provider_2_ready
            .await
            .expect("Input provider 2 should be ready");

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

    /// Test for reconfigurable async runtime with 2 reconfigurations.
    /// This test monitors fixtures/simple_add_distributable.lola and reconfigures
    /// the runtime twice: (w,v) -> w -> (w,v)
    #[apply(async_test)]
    #[ignore = "Too much work to fix in this commit..."]
    async fn test_reconfigurable_async_runtime_two_reconfigurations(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        info!("Starting reconfigurable async runtime test with 2 reconfigurations");

        // Setup: Parse spec and prepare test infrastructure
        let spec_content = std::fs::read_to_string("fixtures/simple_add_distributable.lola")
            .expect("Failed to read simple_add_distributable.lola");
        let mut spec_str = spec_content.as_str();
        let spec: LOLASpecification = lola_specification
            .parse(&mut spec_str)
            .expect("Failed to parse specification");

        let local_node = "test_node";
        let mqtt_host = "localhost";

        // Start MQTT server
        let mqtt_server = start_mqtt().await;
        let mqtt_port = mqtt_server
            .get_host_port_ipv4(1883)
            .await
            .expect("Failed to get host port for MQTT server");
        let mqtt_uri = format!("tcp://{}:{}", mqtt_host, mqtt_port);

        info!("MQTT server started on port {}", mqtt_port);

        // Create locality receiver for work assignments
        let locality_receiver = MQTTLocalityReceiver::new_with_port(
            mqtt_host.to_string(),
            local_node.to_string(),
            mqtt_port,
        );

        with_timeout_res(locality_receiver.ready(), 10, "locality_receiver.ready()")
            .await
            .expect("Locality receiver should be ready");
        info!("Locality receiver is ready");

        // Create input provider and output handler builders
        let input_provider_builder = InputProviderBuilder::mqtt(Some(vec![
            "x".to_string(),
            "y".to_string(),
            "z".to_string(),
        ]))
        .executor(executor.clone())
        .mqtt_port(Some(mqtt_port))
        .model(spec.clone());

        let output_mode = OutputMode {
            output_stdout: false,
            output_mqtt_topics: Some(vec!["w".to_string(), "v".to_string()]),
            mqtt_output: false,
            output_redis_topics: None,
            redis_output: false,
            output_ros_topics: None,
        };

        let output_handler_builder = OutputHandlerBuilder::new(output_mode)
            .executor(executor.clone())
            .output_var_names(vec!["w".into(), "v".into()])
            .aux_info(vec![])
            .mqtt_port(Some(mqtt_port));

        // Create and spawn the reconfigurable async monitor
        info!("Creating reconfigurable async monitor");
        let monitor_builder = ReconfAsyncMonitorBuilder::<
            LOLASpecification,
            Context<Value>,
            Value,
            _,
            UntimedLolaSemantics<CombExprParser>,
        >::new()
        .executor(executor.clone())
        .model(spec.clone())
        .reconf_provider(locality_receiver.clone())
        .input_builder(input_provider_builder)
        .output_builder(output_handler_builder);

        let monitor = Box::new(monitor_builder.async_build().await);

        // Get output subscribers for collecting results BEFORE spawning monitor
        // to ensure MQTT subscriptions are ready to receive outputs
        info!("Getting output subscribers");
        let outputs_w =
            get_mqtt_outputs("w".to_string(), "w_subscriber".to_string(), mqtt_port).await;
        let outputs_v =
            get_mqtt_outputs("v".to_string(), "v_subscriber".to_string(), mqtt_port).await;

        let (w_tx, mut w_rx) = async_unsync::bounded::channel::<Value>(10).into_split();
        let (v_tx, mut v_rx) = async_unsync::bounded::channel::<Value>(10).into_split();

        executor
            .spawn(async move {
                let mut outputs_w = outputs_w;
                while let Some(value) = outputs_w.next().await {
                    w_tx.send(value)
                        .await
                        .expect("Failed to send w output to channel");
                }
            })
            .detach();

        executor
            .spawn(async move {
                let mut outputs_v = outputs_v;
                while let Some(value) = outputs_v.next().await {
                    v_tx.send(value)
                        .await
                        .expect("Failed to send v output to channel");
                }
            })
            .detach();

        // Set up channel to signal monitor readiness
        let (monitor_ready_tx, mut monitor_ready_rx) =
            async_unsync::bounded::channel::<()>(1).into_split();
        executor
            .spawn(async move {
                monitor_ready_tx
                    .send(())
                    .await
                    .expect("Failed to send monitor ready signal");
                monitor.run_boxed().await.expect("Monitor run failed");
            })
            .detach();

        // Wait for monitor to be ready
        with_timeout_res(
            async {
                match monitor_ready_rx.recv().await {
                    Some(_) => Ok(()),
                    None => Err(anyhow::anyhow!("monitor ready channel closed")),
                }
            },
            5,
            "monitor_ready",
        )
        .await
        .expect("Monitor should signal ready");
        info!("Monitor is ready");

        // Create MQTT client for sending work assignments
        let mqtt_client = MQTT_FACTORY
            .connect(&mqtt_uri)
            .await
            .expect("Failed to create MQTT client");

        let work_topic = format!("start_monitors_at_{}", local_node);

        // Define test phases: (work_assignment, inputs, expected_outputs)
        let phases = vec![
            (
                vec!["w".to_string(), "v".to_string()],
                (Value::Int(1), Value::Int(2), Value::Int(3)),
                (Value::Int(3), Some(Value::Int(6))),
            ),
            (
                vec!["w".to_string()],
                (Value::Int(4), Value::Int(5), Value::Int(6)),
                (Value::Int(9), None),
            ),
            (
                vec!["w".to_string(), "v".to_string()],
                (Value::Int(7), Value::Int(8), Value::Int(9)),
                (Value::Int(15), Some(Value::Int(24))),
            ),
        ];

        // Execute each test phase
        for (phase_idx, (work_assignment, inputs, expected_outputs)) in phases.iter().enumerate() {
            let phase_num = phase_idx + 1;
            info!(
                "Phase {}: Sending work assignment {:?}",
                phase_num, work_assignment
            );

            // Send work assignment
            let work_msg =
                serde_json::to_string(work_assignment).expect("Failed to serialize work");
            let message = MqttMessage::new(work_topic.clone(), work_msg, 2);
            mqtt_client
                .publish(message)
                .await
                .expect("Failed to publish work assignment");

            smol::Timer::after(Duration::from_millis(25)).await;

            // Publish inputs
            info!(
                "Phase {}: Publishing inputs: x={:?}, y={:?}, z={:?}",
                phase_num, inputs.0, inputs.1, inputs.2
            );

            let x_pub = executor.spawn(dummy_mqtt_publisher(
                format!("x_pub_phase{}", phase_num),
                "x".to_string(),
                vec![inputs.0.clone()],
                mqtt_port,
            ));
            let y_pub = executor.spawn(dummy_mqtt_publisher(
                format!("y_pub_phase{}", phase_num),
                "y".to_string(),
                vec![inputs.1.clone()],
                mqtt_port,
            ));
            let z_pub = executor.spawn(dummy_mqtt_publisher(
                format!("z_pub_phase{}", phase_num),
                "z".to_string(),
                vec![inputs.2.clone()],
                mqtt_port,
            ));

            smol::Timer::after(Duration::from_millis(100)).await;

            // Collect outputs
            info!("Phase {}: Collecting outputs", phase_num);
            let w_output = with_timeout_res(
                async {
                    match w_rx.recv().await {
                        Some(val) => Ok(val),
                        None => Err(anyhow::anyhow!("w output channel closed")),
                    }
                },
                12,
                &format!("Phase {} w output", phase_num),
            )
            .await
            .expect("Failed to get w output");

            assert_eq!(
                w_output, expected_outputs.0,
                "Phase {}: w output mismatch",
                phase_num
            );
            info!("Phase {}: w = {:?} ✓", phase_num, w_output);

            if let Some(expected_v) = &expected_outputs.1 {
                let v_output = with_timeout_res(
                    async {
                        match v_rx.recv().await {
                            Some(val) => Ok(val),
                            None => Err(anyhow::anyhow!("v output channel closed")),
                        }
                    },
                    12,
                    &format!("Phase {} v output", phase_num),
                )
                .await
                .expect("Failed to get v output");

                assert_eq!(
                    v_output, *expected_v,
                    "Phase {}: v output mismatch",
                    phase_num
                );
                info!("Phase {}: v = {:?} ✓", phase_num, v_output);
            }

            // Wait for publishers to complete
            x_pub.await.expect("X publisher task failed");
            y_pub.await.expect("Y publisher task failed");
            z_pub.await.expect("Z publisher task failed");
        }
    }
}
