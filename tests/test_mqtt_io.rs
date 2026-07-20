#[cfg(test)]
#[cfg(feature = "testcontainers")]
mod integration_tests {
    use async_compat::Compat as TokioCompat;

    use futures::StreamExt;
    use futures::stream;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::vec;
    use tc_testutils::mqtt::dummy_stream_mqtt_publisher;
    use tc_testutils::streams::{
        TickSender, expect_events_serially, tick_stream, with_timeout, with_timeout_res,
    };
    use tracing::info;
    use trustworthiness_checker::async_test;
    use trustworthiness_checker::core::{RuntimeSpec, Semantics};
    use trustworthiness_checker::dsrv_fixtures::spec_simple_add_monitor;
    use trustworthiness_checker::io::mqtt::{MqttFactory, MqttInputBackend};
    use trustworthiness_checker::io::testing::ManualOutputHandler;
    use trustworthiness_checker::lang::mstlo::MstloSpecification;
    use trustworthiness_checker::runtime::builder::GeneralRuntimeBuilder;
    use trustworthiness_checker::runtime::mstlo::MstloRuntimeBuilder;

    use winnow::Parser;

    use approx::assert_abs_diff_eq;
    use std::{collections::BTreeMap, rc::Rc};
    use tc_testutils::mqtt::{get_mqtt_outputs, start_mqtt};

    use trustworthiness_checker::dsrv_fixtures::{TestRuntime, integer_pair_input_stream};
    use trustworthiness_checker::{
        DsrvSpecification, Value, VarName,
        core::Runtime,
        dsrv_fixtures::{float_pair_input_stream, spec_simple_add_monitor_typed_float},
        dsrv_specification,
        io::mqtt::{self, MqttMessage, MqttOutputHandler},
        runtime::RuntimeBuilder,
    };

    const MQTT_FACTORY: MqttFactory = MqttFactory::Paho;
    const MQTT_INPUT_BACKEND: MqttInputBackend = MqttInputBackend::Paho;

    async fn start_mqtt_get_port() -> (Box<dyn std::any::Any>, u16) {
        let mqtt_server = start_mqtt().await;
        let port = with_timeout_res(
            TokioCompat::new(mqtt_server.get_host_port_ipv4(1883)),
            5,
            "get_host_port",
        )
        .await
        .expect("Failed to get host port for MQTT server");

        (Box::new(mqtt_server), port)
    }

    const X_TOPIC: &str = "x";
    const Y_TOPIC: &str = "y";
    const Z_TOPIC: &str = "z";

    fn mstlo_mqtt_input(time_ms: i64, value: f64) -> Value {
        Value::Map(BTreeMap::from([(
            "value".into(),
            Value::Map(BTreeMap::from([
                ("time".into(), Value::Int(time_ms)),
                ("value".into(), Value::Float(value)),
            ])),
        )]))
    }

    fn mstlo_output_tuple(value: &Value) -> (i64, f64) {
        let Value::Map(map) = value else {
            panic!("MSTLO MQTT output should be a map");
        };
        let Value::Int(time) = map.get("time").expect("output has time") else {
            panic!("MSTLO MQTT output time should be int");
        };
        let value = match map.get("value").expect("output has value") {
            Value::Float(value) => *value,
            Value::Int(value) => *value as f64,
            other => panic!("MSTLO MQTT output value should be numeric, got {other:?}"),
        };
        (*time, value)
    }

    fn generate_test_publisher_tasks(
        executor: Rc<LocalExecutor<'static>>,
        xs: Vec<Value>,
        ys: Vec<Value>,
        mqtt_port: u16,
    ) -> (
        (TickSender, smol::Task<anyhow::Result<()>>),
        (TickSender, smol::Task<anyhow::Result<()>>),
    ) {
        let (x_tick, x_pub_stream) = tick_stream(stream::iter(xs.clone()).boxed_local());
        let (y_tick, y_pub_stream) = tick_stream(stream::iter(ys.clone()).boxed_local());

        // Spawn dummy MQTT publisher nodes and keep handles to wait for completion
        let x_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_publisher(
                "x_publisher".to_string(),
                X_TOPIC.to_string(),
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
                Y_TOPIC.to_string(),
                y_pub_stream,
                ys.len(),
                mqtt_port,
            ),
            5,
            "y_publisher_task",
        ));

        ((x_tick, x_publisher_task), (y_tick, y_publisher_task))
    }

    #[apply(async_test)]
    async fn test_add_monitor_mqtt_output(executor: Rc<LocalExecutor<'static>>) {
        let spec = dsrv_specification
            .parse(spec_simple_add_monitor())
            .expect("Model could not be parsed");

        let expected_outputs = vec![Value::Int(3), Value::Int(7)];

        let mqtt_server = start_mqtt().await;
        let mqtt_port = TokioCompat::new(mqtt_server.get_host_port_ipv4(1883))
            .await
            .expect("Failed to get host port for MQTT server");

        let input_stream = integer_pair_input_stream();
        let mqtt_host = "localhost";
        let mqtt_topic = BTreeMap::from_iter(vec![("z".into(), Z_TOPIC.into())]);

        let outputs = with_timeout(
            get_mqtt_outputs(Z_TOPIC.to_string(), "z_subscriber".to_string(), mqtt_port),
            10,
            "get_mqtt_outputs",
        )
        .await
        .unwrap();

        let mut output_handler = MqttOutputHandler::new(
            executor.clone(),
            MQTT_FACTORY,
            vec!["z".into()],
            mqtt_host,
            Some(mqtt_port),
            mqtt_topic,
            vec![],
        )
        .unwrap();
        output_handler.connect().await.unwrap();
        let output_handler = Box::new(output_handler);
        let async_monitor: TestRuntime =
            TestRuntime::new(executor.clone(), spec.clone(), input_stream, output_handler).await;
        executor.spawn(async_monitor.run()).detach();
        // Test the outputs
        let outputs = with_timeout(outputs.take(2).collect::<Vec<_>>(), 10, "outputs.take")
            .await
            .unwrap();
        assert_eq!(outputs, expected_outputs);
    }

    #[apply(async_test)]
    async fn test_add_monitor_mqtt_output_float(executor: Rc<LocalExecutor<'static>>) {
        let spec = dsrv_specification
            .parse(spec_simple_add_monitor_typed_float())
            .expect("Model could not be parsed");

        let mqtt_server = start_mqtt().await;
        let mqtt_port = TokioCompat::new(mqtt_server.get_host_port_ipv4(1883))
            .await
            .expect("Failed to get host port for MQTT server");

        let input_stream = float_pair_input_stream();
        let mqtt_host = "localhost";
        let mqtt_topics = BTreeMap::from_iter(vec![("z".into(), Z_TOPIC.into())]);

        let outputs = with_timeout(
            get_mqtt_outputs(
                Z_TOPIC.to_string(),
                "z_float_subscriber".to_string(),
                mqtt_port,
            ),
            10,
            "get_mqtt_outputs",
        )
        .await
        .unwrap();

        let mut output_handler = MqttOutputHandler::new(
            executor.clone(),
            MQTT_FACTORY,
            vec!["z".into()],
            mqtt_host,
            Some(mqtt_port),
            mqtt_topics,
            vec![],
        )
        .unwrap();
        output_handler.connect().await.unwrap();
        let output_handler = Box::new(output_handler);
        let async_monitor: TestRuntime =
            TestRuntime::new(executor.clone(), spec.clone(), input_stream, output_handler).await;
        executor.spawn(async_monitor.run()).detach();
        // Test the outputs
        let outputs = with_timeout(outputs.take(2).collect::<Vec<_>>(), 10, "outputs.take")
            .await
            .unwrap();
        match outputs[0] {
            Value::Float(f) => assert_abs_diff_eq!(f, 3.7, epsilon = 1e-4),
            _ => panic!("Expected float"),
        }
        match outputs[1] {
            Value::Float(f) => assert_abs_diff_eq!(f, 7.7, epsilon = 1e-4),
            _ => panic!("Expected float"),
        }
    }

    #[apply(async_test)]
    async fn test_add_monitor_mqtt_input(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let xs = vec![Value::Int(1), Value::Int(2)];
        let ys = vec![Value::Int(3), Value::Int(4)];
        let (_mqtt_server, mqtt_port) = start_mqtt_get_port().await;

        let var_topics = BTreeMap::from_iter([
            ("x".into(), X_TOPIC.to_string()),
            ("y".into(), Y_TOPIC.to_string()),
        ]);

        let mut input_stream = with_timeout_res(
            mqtt::input_stream(
                MQTT_INPUT_BACKEND,
                "localhost",
                Some(mqtt_port),
                var_topics,
                0,
            ),
            5,
            "input_stream_connect",
        )
        .await?;
        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), ys.clone(), mqtt_port);
        expect_events_serially(&mut x_tick, &mut y_tick, &mut input_stream, xs, ys).await?;

        // Final ticks to let them complete
        x_tick.send(()).await?;
        y_tick.send(()).await?;
        // Wait for publishers to complete and then shutdown MQTT server to terminate connections
        info!("Waiting for publishers to complete...");
        x_publisher_task.await?;
        y_publisher_task.await?;
        info!("All publishers completed, shutting down MQTT server");

        Ok(())
    }

    #[apply(async_test)]
    async fn test_add_monitor_rumqttc_input(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let xs = vec![Value::Int(1), Value::Int(2)];
        let ys = vec![Value::Int(3), Value::Int(4)];
        let (_mqtt_server, mqtt_port) = start_mqtt_get_port().await;
        let var_topics = BTreeMap::from([
            (VarName::new("x"), X_TOPIC.to_owned()),
            (VarName::new("y"), Y_TOPIC.to_owned()),
        ]);

        let mut input_batches = with_timeout_res(
            mqtt::input_stream(
                MqttInputBackend::Rumqttc,
                "localhost",
                Some(mqtt_port),
                var_topics,
                3,
            ),
            5,
            "rumqttc_input_stream_connect",
        )
        .await?;

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(executor, xs.clone(), ys.clone(), mqtt_port);
        expect_events_serially(&mut x_tick, &mut y_tick, &mut input_batches, xs, ys).await?;

        x_tick.send(()).await?;
        y_tick.send(()).await?;
        x_publisher_task.await?;
        y_publisher_task.await?;
        Ok(())
    }

    async fn publish_malformed_input(mqtt_port: u16, topic: &str) -> anyhow::Result<()> {
        let publisher = MqttFactory::Paho
            .connect(&format!("tcp://localhost:{mqtt_port}"))
            .await?;
        publisher
            .publish(MqttMessage::new(
                topic.to_owned(),
                "not valid JSON".to_owned(),
                1,
            ))
            .await?;
        publisher.disconnect().await
    }

    #[apply(async_test)]
    async fn paho_input_reports_malformed_payload() -> anyhow::Result<()> {
        let (_mqtt_server, mqtt_port) = start_mqtt_get_port().await;
        let mut batches = with_timeout_res(
            mqtt::input_stream(
                MqttInputBackend::Paho,
                "localhost",
                Some(mqtt_port),
                BTreeMap::from([(VarName::new("x"), X_TOPIC.to_owned())]),
                0,
            ),
            5,
            "paho malformed input connect",
        )
        .await?;

        publish_malformed_input(mqtt_port, X_TOPIC).await?;
        let error = with_timeout(batches.next(), 5, "paho malformed input")
            .await?
            .ok_or_else(|| anyhow::anyhow!("Paho input ended without reporting malformed data"))?
            .unwrap_err();
        assert!(error.to_string().contains("Failed to parse value"));
        Ok(())
    }

    #[apply(async_test)]
    async fn rumqttc_input_reports_malformed_payload() -> anyhow::Result<()> {
        let (_mqtt_server, mqtt_port) = start_mqtt_get_port().await;
        let mut batches = with_timeout_res(
            mqtt::input_stream(
                MqttInputBackend::Rumqttc,
                "localhost",
                Some(mqtt_port),
                BTreeMap::from([(VarName::new("x"), X_TOPIC.to_owned())]),
                3,
            ),
            5,
            "rumqttc malformed input connect",
        )
        .await?;

        publish_malformed_input(mqtt_port, X_TOPIC).await?;
        let error = with_timeout(batches.next(), 5, "rumqttc malformed input")
            .await?
            .ok_or_else(|| anyhow::anyhow!("rumqttc input ended without reporting malformed data"))?
            .unwrap_err();
        assert!(error.to_string().contains("Failed to parse value"));
        Ok(())
    }

    #[apply(async_test)]
    async fn test_mstlo_runtime_mqtt_input_output(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        const MSTLO_IN_TOPIC: &str = "mstlo/x";
        const MSTLO_OUT_TOPIC: &str = "mstlo/out";

        let (_mqtt_server, mqtt_port) = start_mqtt_get_port().await;

        let input_stream = with_timeout_res(
            mqtt::input_stream(
                MQTT_INPUT_BACKEND,
                "localhost",
                Some(mqtt_port),
                BTreeMap::from([(VarName::new("x"), MSTLO_IN_TOPIC.to_string())]),
                0,
            ),
            5,
            "mstlo_input_connect",
        )
        .await?;

        let mut output_handler = MqttOutputHandler::new(
            executor.clone(),
            MQTT_FACTORY,
            vec![VarName::new("robustness")],
            "localhost",
            Some(mqtt_port),
            BTreeMap::from([(VarName::new("robustness"), MSTLO_OUT_TOPIC.to_string())]),
            vec![],
        )?;
        output_handler.connect().await?;

        let outputs = with_timeout(
            get_mqtt_outputs(
                MSTLO_OUT_TOPIC.to_string(),
                "mstlo_output_subscriber".to_string(),
                mqtt_port,
            ),
            5,
            "mstlo output subscription",
        )
        .await?;

        let formula = MstloSpecification::single(
            VarName::new("robustness"),
            mstlo::FormulaDefinition::GreaterThan("x", 5.0),
        );
        let (input_stream, input_controller) =
            trustworthiness_checker::io::controlled(input_stream);
        let builder = MstloRuntimeBuilder::new()
            .executor(executor.clone())
            .model(formula)
            .input(input_stream);
        let runtime = builder.output(Box::new(output_handler)).build().await;
        let runtime_task = executor.spawn(runtime.run());

        let values = vec![mstlo_mqtt_input(0, 7.0), mstlo_mqtt_input(10, 4.0)];
        let (mut tick, publish_stream) = tick_stream(stream::iter(values.clone()).boxed_local());
        let publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_publisher(
                "mstlo_x_publisher".to_string(),
                MSTLO_IN_TOPIC.to_string(),
                publish_stream,
                values.len(),
                mqtt_port,
            ),
            5,
            "mstlo publisher task",
        ));

        let mut outputs = outputs;

        tick.send(()).await?;
        input_controller.advance().await?;
        let first = with_timeout(outputs.next(), 5, "first mstlo mqtt output")
            .await?
            .expect("first MSTLO MQTT output");
        assert_eq!(mstlo_output_tuple(&first), (0, 2.0));

        tick.send(()).await?;
        input_controller.advance().await?;
        let second = with_timeout(outputs.next(), 5, "second mstlo mqtt output")
            .await?
            .expect("second MSTLO MQTT output");
        assert_eq!(mstlo_output_tuple(&second), (10, -1.0));

        tick.send(()).await?;
        publisher_task.await?;
        runtime_task.detach();

        Ok(())
    }

    #[apply(async_test)]
    async fn test_mstlo_runtime_mqtt_multiple_input_streams(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        const MSTLO_X_TOPIC: &str = "mstlo/multi/x";
        const MSTLO_Y_TOPIC: &str = "mstlo/multi/y";
        const MSTLO_GT_TOPIC: &str = "mstlo/multi/gt";
        const MSTLO_LT_TOPIC: &str = "mstlo/multi/lt";

        let (_mqtt_server, mqtt_port) = start_mqtt_get_port().await;

        let input_stream = with_timeout_res(
            mqtt::input_stream(
                MQTT_INPUT_BACKEND,
                "localhost",
                Some(mqtt_port),
                BTreeMap::from([
                    (VarName::new("x"), MSTLO_X_TOPIC.to_string()),
                    (VarName::new("y"), MSTLO_Y_TOPIC.to_string()),
                ]),
                0,
            ),
            5,
            "mstlo_multi_input_connect",
        )
        .await?;

        let mut output_handler = MqttOutputHandler::new(
            executor.clone(),
            MQTT_FACTORY,
            vec![VarName::new("gt"), VarName::new("lt")],
            "localhost",
            Some(mqtt_port),
            BTreeMap::from([
                (VarName::new("gt"), MSTLO_GT_TOPIC.to_string()),
                (VarName::new("lt"), MSTLO_LT_TOPIC.to_string()),
            ]),
            vec![],
        )?;
        output_handler.connect().await?;

        let mut gt_outputs = with_timeout(
            get_mqtt_outputs(
                MSTLO_GT_TOPIC.to_string(),
                "mstlo_multi_gt_subscriber".to_string(),
                mqtt_port,
            ),
            5,
            "mstlo multi gt output subscription",
        )
        .await?;
        let _lt_outputs = with_timeout(
            get_mqtt_outputs(
                MSTLO_LT_TOPIC.to_string(),
                "mstlo_multi_lt_subscriber".to_string(),
                mqtt_port,
            ),
            5,
            "mstlo multi lt output subscription",
        )
        .await?;

        let formula = MstloSpecification::new(BTreeMap::from([
            (
                VarName::new("gt"),
                mstlo::FormulaDefinition::GreaterThan("x", 5.0),
            ),
            (
                VarName::new("lt"),
                mstlo::FormulaDefinition::LessThan("y", 3.0),
            ),
        ]));
        let runtime = MstloRuntimeBuilder::new()
            .executor(executor.clone())
            .model(formula)
            .input(input_stream)
            .output(Box::new(output_handler))
            .build()
            .await;
        let runtime_task = executor.spawn(runtime.run());

        let x_values = vec![mstlo_mqtt_input(0, 7.0)];
        let y_values = vec![mstlo_mqtt_input(0, 2.0)];
        let (mut x_tick, x_stream) = tick_stream(stream::iter(x_values.clone()).boxed_local());
        let (mut y_tick, y_stream) = tick_stream(stream::iter(y_values.clone()).boxed_local());
        let x_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_publisher(
                "mstlo_multi_x_publisher".to_string(),
                MSTLO_X_TOPIC.to_string(),
                x_stream,
                x_values.len(),
                mqtt_port,
            ),
            5,
            "mstlo multi x publisher task",
        ));
        let y_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_publisher(
                "mstlo_multi_y_publisher".to_string(),
                MSTLO_Y_TOPIC.to_string(),
                y_stream,
                y_values.len(),
                mqtt_port,
            ),
            5,
            "mstlo multi y publisher task",
        ));

        x_tick.send(()).await?;
        let gt = with_timeout(gt_outputs.next(), 5, "mstlo multi gt output")
            .await?
            .expect("MSTLO gt MQTT output");
        assert_eq!(mstlo_output_tuple(&gt), (0, 2.0));

        y_tick.send(()).await?;

        x_tick.send(()).await?;
        y_tick.send(()).await?;
        x_publisher_task.await?;
        y_publisher_task.await?;
        runtime_task.detach();

        Ok(())
    }

    async fn run_mqtt_json_object_input_monitor(
        executor: Rc<LocalExecutor<'static>>,
        spec_src: &str,
        semantics: Semantics,
        client_suffix: &str,
    ) -> anyhow::Result<Vec<(usize, BTreeMap<VarName, Value>)>> {
        let mut spec_src = spec_src;
        let spec: DsrvSpecification = dsrv_specification(&mut spec_src).unwrap();
        let (_mqtt_server, mqtt_port) = start_mqtt_get_port().await;

        let var_topics = BTreeMap::from_iter([("payload".into(), "payload".to_string())]);
        let input_stream = with_timeout_res(
            mqtt::input_stream(
                MQTT_INPUT_BACKEND,
                "localhost",
                Some(mqtt_port),
                var_topics,
                0,
            ),
            5,
            "input_stream_connect",
        )
        .await?;

        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars().clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor: Box<dyn Runtime> = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec)
            .input(input_stream)
            .output(output_handler)
            .runtime(RuntimeSpec::Async)
            .semantics(semantics)
            .build()
            .await;
        executor.spawn(monitor.run()).detach();

        let payloads = vec![
            Value::Map(BTreeMap::from([
                ("extra".into(), Value::Int(99)),
                ("x".into(), Value::Int(10)),
                ("y".into(), Value::Int(20)),
            ])),
            Value::Map(BTreeMap::from([
                ("extra".into(), Value::Int(100)),
                ("x".into(), Value::Int(30)),
                ("y".into(), Value::Int(40)),
            ])),
        ];
        let (mut payload_tick, payload_stream) =
            tick_stream(stream::iter(payloads.clone()).boxed_local());
        let publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_publisher(
                format!("payload_publisher_{client_suffix}"),
                "payload".to_string(),
                payload_stream,
                payloads.len(),
                mqtt_port,
            ),
            5,
            "payload_publisher_task",
        ));

        payload_tick.send(()).await?;
        payload_tick.send(()).await?;
        let outputs = with_timeout(
            outputs.enumerate().take(2).collect::<Vec<_>>(),
            5,
            "mqtt json object input outputs.collect()",
        )
        .await?;

        payload_tick.send(()).await?;
        publisher_task.await?;

        Ok(outputs)
    }

    #[apply(async_test)]
    async fn test_mqtt_json_map_input_can_be_used_as_typed_struct(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let spec = r#"
in payload: Struct<x: Int, y: Int, ...>
out selected: Int
out echoed: Struct<x: Int, y: Int, ...>
selected = Map.get(payload, "x")
echoed = payload
"#;

        let outputs = run_mqtt_json_object_input_monitor(
            executor,
            spec,
            Semantics::TypedUntimed,
            "typed_struct_input",
        )
        .await?;

        assert_eq!(
            outputs,
            vec![
                (
                    0,
                    BTreeMap::from([
                        (
                            "echoed".into(),
                            Value::Map(BTreeMap::from([
                                ("extra".into(), Value::Int(99)),
                                ("x".into(), Value::Int(10)),
                                ("y".into(), Value::Int(20)),
                            ])),
                        ),
                        ("selected".into(), Value::Int(10)),
                    ]),
                ),
                (
                    1,
                    BTreeMap::from([
                        (
                            "echoed".into(),
                            Value::Map(BTreeMap::from([
                                ("extra".into(), Value::Int(100)),
                                ("x".into(), Value::Int(30)),
                                ("y".into(), Value::Int(40)),
                            ])),
                        ),
                        ("selected".into(), Value::Int(30)),
                    ]),
                ),
            ]
        );
        Ok(())
    }

    #[apply(async_test)]
    async fn test_mqtt_json_map_input_can_be_used_as_untyped_struct_like_map(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let spec = r#"
in payload
out selected
out echoed
selected = Map.get(payload, "x")
echoed = payload
"#;

        let outputs = run_mqtt_json_object_input_monitor(
            executor,
            spec,
            Semantics::Untimed,
            "untyped_struct_like_input",
        )
        .await?;

        assert_eq!(
            outputs,
            vec![
                (
                    0,
                    BTreeMap::from([
                        (
                            "echoed".into(),
                            Value::Map(BTreeMap::from([
                                ("extra".into(), Value::Int(99)),
                                ("x".into(), Value::Int(10)),
                                ("y".into(), Value::Int(20)),
                            ])),
                        ),
                        ("selected".into(), Value::Int(10)),
                    ]),
                ),
                (
                    1,
                    BTreeMap::from([
                        (
                            "echoed".into(),
                            Value::Map(BTreeMap::from([
                                ("extra".into(), Value::Int(100)),
                                ("x".into(), Value::Int(30)),
                                ("y".into(), Value::Int(40)),
                            ])),
                        ),
                        ("selected".into(), Value::Int(30)),
                    ]),
                ),
            ]
        );
        Ok(())
    }

    #[apply(async_test)]
    async fn test_add_monitor_mqtt_input_float(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let xs = vec![Value::Float(1.3), Value::Float(3.4)];
        let ys = vec![Value::Float(2.4), Value::Float(4.3)];
        let (_mqtt_server, mqtt_port) = start_mqtt_get_port().await;

        let var_topics = BTreeMap::from_iter([
            ("x".into(), X_TOPIC.to_string()),
            ("y".into(), Y_TOPIC.to_string()),
        ]);

        let mut input_stream = with_timeout_res(
            mqtt::input_stream(
                MQTT_INPUT_BACKEND,
                "localhost",
                Some(mqtt_port),
                var_topics,
                0,
            ),
            5,
            "input_stream_connect",
        )
        .await?;
        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), ys.clone(), mqtt_port);
        expect_events_serially(&mut x_tick, &mut y_tick, &mut input_stream, xs, ys).await?;

        // Final ticks to let them complete
        x_tick.send(()).await?;
        y_tick.send(()).await?;
        // Wait for publishers to complete and then shutdown MQTT server to terminate connections
        info!("Waiting for publishers to complete...");
        x_publisher_task.await?;
        y_publisher_task.await?;
        info!("All publishers completed, shutting down MQTT server");

        Ok(())
    }
}

#[cfg(feature = "testcontainers")]
#[cfg(test)]
mod reconf_tests {

    use async_compat::Compat as TokioCompat;
    use futures::{FutureExt, StreamExt, stream};
    use macro_rules_attribute::apply;
    use serde_json::json;
    use smol::LocalExecutor;
    use std::collections::{BTreeMap, BTreeSet};
    use std::rc::Rc;
    use tc_testutils::mqtt::{dummy_stream_mqtt_publisher, get_mqtt_outputs, start_mqtt};
    use tc_testutils::streams::{TickSender, tick_stream, with_timeout, with_timeout_res};
    use tracing::info;
    use trustworthiness_checker::async_test;
    use trustworthiness_checker::cli::args::OutputMode;
    use trustworthiness_checker::core::Runtime;
    use trustworthiness_checker::core::values::Value;
    use trustworthiness_checker::dsrv_fixtures::*;
    use trustworthiness_checker::dsrv_specification;
    use trustworthiness_checker::io::{InputStreamFactory, OutputHandlerBuilder};
    use trustworthiness_checker::lang::dsrv::lalr_parser::LALRParser;
    use trustworthiness_checker::runtime::RuntimeBuilder;
    use trustworthiness_checker::runtime::builder::SemiSyncValueConfig;
    use trustworthiness_checker::runtime::reconfigurable_semi_sync::ReconfSemiSyncRuntimeBuilder;
    use trustworthiness_checker::semantics::UntimedDsrvSemantics;

    type TestRuntimeBuilder = ReconfSemiSyncRuntimeBuilder<
        SemiSyncValueConfig,
        UntimedDsrvSemantics<LALRParser>,
        LALRParser,
    >;

    // TODO: Thomas suggested implement a type of OutputHandler for these tests that uses mpsc channels because
    // this is possible while still being clonable

    const X_TOPIC: &str = "x";
    const Y_TOPIC: &str = "y";
    const Z_TOPIC: &str = "z";
    const V_TOPIC: &str = "v";
    const W_TOPIC: &str = "w";
    const RECONF_TOPIC: &str = "RECONF_ME";

    fn generate_test_publisher_tasks(
        executor: Rc<LocalExecutor<'static>>,
        xs: Vec<Value>,
        ys: Vec<Value>,
        mqtt_port: u16,
    ) -> (
        (TickSender, smol::Task<anyhow::Result<()>>),
        (TickSender, smol::Task<anyhow::Result<()>>),
    ) {
        let (x_tick, x_pub_stream) = tick_stream(stream::iter(xs.clone()).boxed_local());
        let (y_tick, y_pub_stream) = tick_stream(stream::iter(ys.clone()).boxed_local());

        // Spawn dummy MQTT publisher nodes and keep handles to wait for completion
        let x_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_publisher(
                "x_publisher".to_string(),
                X_TOPIC.to_string(),
                x_pub_stream,
                xs.len(),
                mqtt_port,
            ),
            60,
            "x_publisher_task",
        ));

        let y_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_publisher(
                "y_publisher".to_string(),
                Y_TOPIC.to_string(),
                y_pub_stream,
                ys.len(),
                mqtt_port,
            ),
            60,
            "y_publisher_task",
        ));

        ((x_tick, x_publisher_task), (y_tick, y_publisher_task))
    }

    #[apply(async_test)]
    async fn test_reconf_simple_add_no_reconf(executor: Rc<LocalExecutor<'static>>) {
        // Tests the ReconfSemiSyncRuntime with the simple add monitor, without actually sending a
        // reconfiguration, to check that the basic MQTT input/output works as expected.

        let spec = dsrv_specification(&mut spec_simple_add_monitor()).unwrap();
        let xs = vec![Value::Int(1), Value::Int(3)];
        let ys = vec![Value::Int(2), Value::Int(4)];
        let expected = vec![Value::Int(3), Value::Int(5), Value::Int(7)];

        let mqtt_server = start_mqtt().await;
        let mqtt_port = with_timeout_res(
            TokioCompat::new(mqtt_server.get_host_port_ipv4(1883)),
            5,
            "get_host_port",
        )
        .await
        .expect("Failed to get host port for MQTT server");

        // Input stream is MQTT server:
        let input_factory = InputStreamFactory::mqtt(
            Some(BTreeMap::from([
                (X_TOPIC.into(), X_TOPIC.into()),
                (Y_TOPIC.into(), Y_TOPIC.into()),
            ])),
            Some(mqtt_port),
        );

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), ys.clone(), mqtt_port);

        let output_mode = OutputMode {
            output_stdout: false,
            output_mqtt_file: None,
            mqtt_output: true,
            output_redis_file: None,
            redis_output: false,
            output_ros_file: None,
        };

        let output_builder = OutputHandlerBuilder::new(output_mode)
            .executor(executor.clone())
            .output_var_names(BTreeSet::from(["z".into()]))
            .mqtt_port(Some(mqtt_port))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestRuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec.clone())
                .input_factory(input_factory)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.build().await;
        executor.spawn(monitor.run()).detach();

        let mut x_sub = with_timeout(
            get_mqtt_outputs(X_TOPIC.to_string(), "x_subscriber".to_string(), mqtt_port),
            5,
            "x_subscriber",
        )
        .await
        .unwrap();
        let mut y_sub = with_timeout(
            get_mqtt_outputs(Y_TOPIC.to_string(), "y_subscriber".to_string(), mqtt_port),
            5,
            "y_subscriber",
        )
        .await
        .unwrap();
        let mut z_sub = with_timeout(
            get_mqtt_outputs(Z_TOPIC.to_string(), "z_subscriber".to_string(), mqtt_port),
            5,
            "z_subscriber",
        )
        .await
        .unwrap();

        let mut x_iter = xs.into_iter();
        let mut y_iter = ys.into_iter();
        let mut z_iter = expected.into_iter();

        // Initial send/receive only yields one z-value:
        x_tick.send(()).await.expect("Failed to send tick");
        let x_res = with_timeout(x_sub.next(), 5, "x_sub.next()")
            .await
            .expect("Failed to get x result");
        assert_eq!(x_res, x_iter.next());
        y_tick.send(()).await.expect("Failed to send tick");
        let y_res = with_timeout(y_sub.next(), 5, "y_sub.next()")
            .await
            .expect("Failed to get y result");
        assert_eq!(y_res, y_iter.next());
        let z_res = with_timeout(z_sub.next(), 5, "z_sub.next()")
            .await
            .expect("Failed to get z result");
        let z_exp = z_iter.next();
        assert_eq!(z_res, z_exp);

        // Afterwards we receive one on each tick:
        for (x_exp, y_exp) in x_iter.zip(y_iter) {
            x_tick.send(()).await.expect("Failed to send tick");
            let x_res = with_timeout(x_sub.next(), 5, "x_sub.next()")
                .await
                .expect("Failed to get x result");
            assert_eq!(x_res, Some(x_exp));

            let z_res = with_timeout(z_sub.next(), 5, "z_sub.next()")
                .await
                .expect("Failed to get z result");
            let z_exp = z_iter.next();
            assert_eq!(z_res, z_exp);

            y_tick.send(()).await.expect("Failed to send tick");
            let y_res = with_timeout(y_sub.next(), 5, "y_sub.next()")
                .await
                .expect("Failed to get y result");
            assert_eq!(y_res, Some(y_exp));

            let z_res = with_timeout(z_sub.next(), 5, "z_sub.next()")
                .await
                .expect("Failed to get z result");
            let z_exp = z_iter.next();
            assert_eq!(z_res, z_exp);
        }

        x_tick.send(()).await.expect("Failed to send tick");
        y_tick.send(()).await.expect("Failed to send tick");
        with_timeout_res(x_publisher_task, 5, "x_publisher_task")
            .await
            .expect("x publisher task should finish");
        with_timeout_res(y_publisher_task, 5, "y_publisher_task")
            .await
            .expect("y publisher task should finish");
    }

    #[apply(async_test)]
    async fn test_reconf_no_change_of_streams(executor: Rc<LocalExecutor<'static>>) {
        // Tests the ReconfSemiSyncRuntime with the simple add monitor, where we reconfigure but do
        // not introduce/remove any streams

        let spec = dsrv_specification(&mut spec_simple_add_monitor()).unwrap();
        let xs = vec![Value::Int(1), Value::Int(3), Value::Int(5), Value::Int(7)];
        let ys = vec![Value::Int(2), Value::Int(4), Value::Int(6), Value::Int(8)];
        let in_len = xs.len();
        let expected = vec![
            Value::Int(3),
            Value::Int(5),
            Value::Int(7),
            // Here we reconf:
            Value::Int(12),
            Value::Int(14),
            Value::Int(16),
        ];

        let mqtt_server = start_mqtt().await;
        let mqtt_port = with_timeout_res(
            TokioCompat::new(mqtt_server.get_host_port_ipv4(1883)),
            5,
            "get_host_port",
        )
        .await
        .expect("Failed to get host port for MQTT server");

        // Input stream is MQTT server:
        let input_factory = InputStreamFactory::mqtt(
            Some(BTreeMap::from([
                (X_TOPIC.into(), X_TOPIC.into()),
                (Y_TOPIC.into(), Y_TOPIC.into()),
            ])),
            Some(mqtt_port),
        );

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), ys.clone(), mqtt_port);

        let output_mode = OutputMode {
            output_stdout: false,
            output_mqtt_file: None,
            mqtt_output: true,
            output_redis_file: None,
            redis_output: false,
            output_ros_file: None,
        };

        let output_builder = OutputHandlerBuilder::new(output_mode)
            .executor(executor.clone())
            .output_var_names(BTreeSet::from(["z".into()]))
            .mqtt_port(Some(mqtt_port))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestRuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec.clone())
                .input_factory(input_factory)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.build().await;
        executor.spawn(monitor.run()).detach();

        let mut x_sub = with_timeout(
            get_mqtt_outputs(X_TOPIC.to_string(), "x_subscriber".to_string(), mqtt_port),
            5,
            "x_subscriber",
        )
        .await
        .unwrap();
        let mut y_sub = with_timeout(
            get_mqtt_outputs(Y_TOPIC.to_string(), "y_subscriber".to_string(), mqtt_port),
            5,
            "y_subscriber",
        )
        .await
        .unwrap();
        let mut z_sub = with_timeout(
            get_mqtt_outputs(Z_TOPIC.to_string(), "z_subscriber".to_string(), mqtt_port),
            5,
            "z_subscriber",
        )
        .await
        .unwrap();

        let mut x_iter1 = xs.clone().into_iter().take(in_len / 2);
        let mut x_iter2 = xs.into_iter().skip(in_len / 2);
        let mut y_iter1 = ys.clone().into_iter().take(in_len / 2);
        let mut y_iter2 = ys.into_iter().skip(in_len / 2);
        let mut z_iter = expected.into_iter();

        // Initial send/receive only yields one z-value:
        x_tick.send(()).await.expect("Failed to send tick");
        let x_res = with_timeout(x_sub.next(), 5, "x_sub.next()")
            .await
            .expect("Failed to get x result");
        assert_eq!(x_res, x_iter1.next());
        y_tick.send(()).await.expect("Failed to send tick");
        let y_res = with_timeout(y_sub.next(), 5, "y_sub.next()")
            .await
            .expect("Failed to get y result");
        assert_eq!(y_res, y_iter1.next());
        let z_res = with_timeout(z_sub.next(), 5, "z_sub.next()")
            .await
            .expect("Failed to get z result");
        let z_exp = z_iter.next();
        assert_eq!(z_res, z_exp);

        // Afterwards we receive one on each tick:
        // (Take the first half of the batch)
        for (x_exp, y_exp) in x_iter1.zip(y_iter1) {
            x_tick.send(()).await.expect("Failed to send tick");
            let x_res = with_timeout(x_sub.next(), 5, "x_sub.next()")
                .await
                .expect("Failed to get x result");
            assert_eq!(x_res, Some(x_exp));

            let z_res = with_timeout(z_sub.next(), 5, "z_sub.next()")
                .await
                .expect("Failed to get z result");
            let z_exp = z_iter.next();
            assert_eq!(z_res, z_exp);

            y_tick.send(()).await.expect("Failed to send tick");
            let y_res = with_timeout(y_sub.next(), 5, "y_sub.next()")
                .await
                .expect("Failed to get y result");
            assert_eq!(y_res, Some(y_exp));

            let z_res = with_timeout(z_sub.next(), 5, "z_sub.next()")
                .await
                .expect("Failed to get z result");
            let z_exp = z_iter.next();
            info!(?z_res, ?z_exp, "Received z value");
            assert_eq!(z_res, z_exp);
        }

        // Reconfigure:
        let mut reconf_sub = with_timeout(
            get_mqtt_outputs(
                RECONF_TOPIC.to_string(),
                "reconf_subscriber".to_string(),
                mqtt_port,
            ),
            5,
            "reconf_subscriber",
        )
        .await
        .unwrap();

        let reconf_stream = futures::stream::once(async {
            json!({
                "spec": spec_simple_add_monitor_plus_one(),
                "type_info": {},
                "topic_mapping": {}
            })
            .to_string()
        })
        .boxed_local();
        let _reconf_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_publisher(
                "reconf_publisher".to_string(),
                RECONF_TOPIC.to_string(),
                reconf_stream,
                1,
                mqtt_port,
            ),
            5,
            "reconf_publisher_task",
        ));
        reconf_sub
            .next()
            .await
            .expect("Failed to get reconf message");

        // TODO: Should not be needed in the future when reconf is more stable
        //
        // Wait a while. Needed because the OutputHandler needs to reconnect to the MQTT server,
        // but we have no way of knowing when this is done since runtime is being spawned...
        // Effects visible mainly when running single-threaded either with `-j 1 -- --test-threads 1` or on test runner.
        smol::Timer::after(std::time::Duration::from_millis(2000)).await;

        // Take the rest (again initially just one z value)
        x_tick.send(()).await.expect("Failed to send tick");
        let x_res = with_timeout(x_sub.next(), 5, "x_sub.next()")
            .await
            .expect("Failed to get x result");
        assert_eq!(x_res, x_iter2.next());
        y_tick.send(()).await.expect("Failed to send tick");
        let y_res = with_timeout(y_sub.next(), 5, "y_sub.next()")
            .await
            .expect("Failed to get y result");
        assert_eq!(y_res, y_iter2.next());
        let z_res = with_timeout(z_sub.next(), 5, "z_sub.next()")
            .await
            .expect("Failed to get z result");
        let z_exp = z_iter.next();
        info!(?z_res, ?z_exp, "Received z value");
        assert_eq!(z_res, z_exp);

        for (x_exp, y_exp) in x_iter2.zip(y_iter2) {
            x_tick.send(()).await.expect("Failed to send tick");
            let x_res = with_timeout(x_sub.next(), 5, "x_sub.next()")
                .await
                .expect("Failed to get x result");
            assert_eq!(x_res, Some(x_exp));

            let z_res = with_timeout(z_sub.next(), 5, "z_sub.next()")
                .await
                .expect("Failed to get z result");
            let z_exp = z_iter.next();
            assert_eq!(z_res, z_exp);

            y_tick.send(()).await.expect("Failed to send tick");
            let y_res = with_timeout(y_sub.next(), 5, "y_sub.next()")
                .await
                .expect("Failed to get y result");
            assert_eq!(y_res, Some(y_exp));

            let z_res = with_timeout(z_sub.next(), 5, "z_sub.next()")
                .await
                .expect("Failed to get z result");
            let z_exp = z_iter.next();
            assert_eq!(z_res, z_exp);
        }

        x_tick.send(()).await.expect("Failed to send tick");
        y_tick.send(()).await.expect("Failed to send tick");
        with_timeout_res(x_publisher_task, 5, "x_publisher_task")
            .await
            .expect("x publisher task should finish");
        with_timeout_res(y_publisher_task, 5, "y_publisher_task")
            .await
            .expect("y publisher task should finish");
    }

    #[apply(async_test)]
    async fn test_reconf_delete_input_stream(executor: Rc<LocalExecutor<'static>>) {
        // Tests the ReconfSemiSyncRuntime with the simple add monitor, where we reconfigure to a
        // spec that does not require a y stream

        let spec = dsrv_specification(&mut spec_simple_add_monitor()).unwrap();
        let xs = vec![Value::Int(1), Value::Int(3), Value::Int(5), Value::Int(7)];
        let ys = vec![Value::Int(2), Value::Int(4)];
        let y_len = ys.len();
        let expected = vec![
            Value::Int(3),
            Value::Int(5),
            Value::Int(7),
            // Here we reconf:
            Value::Int(5),
            Value::Int(12),
        ];

        let mqtt_server = start_mqtt().await;
        let mqtt_port = with_timeout_res(
            TokioCompat::new(mqtt_server.get_host_port_ipv4(1883)),
            5,
            "get_host_port",
        )
        .await
        .expect("Failed to get host port for MQTT server");

        // Input stream is MQTT server:
        let input_factory = InputStreamFactory::mqtt(
            Some(BTreeMap::from([
                (X_TOPIC.into(), X_TOPIC.into()),
                (Y_TOPIC.into(), Y_TOPIC.into()),
            ])),
            Some(mqtt_port),
        );

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), ys.clone(), mqtt_port);

        let output_mode = OutputMode {
            output_stdout: false,
            output_mqtt_file: None,
            mqtt_output: true,
            output_redis_file: None,
            redis_output: false,
            output_ros_file: None,
        };

        let output_builder = OutputHandlerBuilder::new(output_mode)
            .executor(executor.clone())
            .output_var_names(BTreeSet::from(["z".into()]))
            .mqtt_port(Some(mqtt_port))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestRuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec.clone())
                .input_factory(input_factory)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.build().await;
        executor.spawn(monitor.run()).detach();

        let mut x_sub = with_timeout(
            get_mqtt_outputs(X_TOPIC.to_string(), "x_subscriber".to_string(), mqtt_port),
            5,
            "x_subscriber",
        )
        .await
        .unwrap();
        let mut y_sub = with_timeout(
            get_mqtt_outputs(Y_TOPIC.to_string(), "y_subscriber".to_string(), mqtt_port),
            5,
            "y_subscriber",
        )
        .await
        .unwrap();
        let mut z_sub = with_timeout(
            get_mqtt_outputs(Z_TOPIC.to_string(), "z_subscriber".to_string(), mqtt_port),
            5,
            "z_subscriber",
        )
        .await
        .unwrap();

        let mut x_iter1 = xs.clone().into_iter().take(y_len);
        let x_iter2 = xs.into_iter().skip(y_len);
        let mut y_iter = ys.into_iter();
        let mut z_iter = expected.into_iter();

        // Initial send/receive only yields one z-value:
        x_tick.send(()).await.expect("Failed to send tick");
        let x_res = with_timeout(x_sub.next(), 5, "x_sub.next()")
            .await
            .expect("Failed to get x result");
        assert_eq!(x_res, x_iter1.next());
        y_tick.send(()).await.expect("Failed to send tick");
        let y_res = with_timeout(y_sub.next(), 5, "y_sub.next()")
            .await
            .expect("Failed to get y result");
        assert_eq!(y_res, y_iter.next());
        let z_res = with_timeout(z_sub.next(), 5, "z_sub.next()")
            .await
            .expect("Failed to get z result");
        let z_exp = z_iter.next();
        assert_eq!(z_res, z_exp);

        // Afterwards we receive one on each tick:
        // (Take the first half of the batch)
        for (x_exp, y_exp) in x_iter1.zip(y_iter) {
            x_tick.send(()).await.expect("Failed to send tick");
            let x_res = with_timeout(x_sub.next(), 5, "x_sub.next()")
                .await
                .expect("Failed to get x result");
            assert_eq!(x_res, Some(x_exp));

            let z_res = with_timeout(z_sub.next(), 5, "z_sub.next()")
                .await
                .expect("Failed to get z result");
            let z_exp = z_iter.next();
            assert_eq!(z_res, z_exp);

            y_tick.send(()).await.expect("Failed to send tick");
            let y_res = with_timeout(y_sub.next(), 5, "y_sub.next()")
                .await
                .expect("Failed to get y result");
            assert_eq!(y_res, Some(y_exp));

            let z_res = with_timeout(z_sub.next(), 5, "z_sub.next()")
                .await
                .expect("Failed to get z result");
            let z_exp = z_iter.next();
            info!(?z_res, ?z_exp, "Received z value");
            assert_eq!(z_res, z_exp);
        }

        // Reconfigure:
        let mut reconf_sub = with_timeout(
            get_mqtt_outputs(
                RECONF_TOPIC.to_string(),
                "reconf_subscriber".to_string(),
                mqtt_port,
            ),
            5,
            "reconf_subscriber",
        )
        .await
        .unwrap();

        let reconf_stream = futures::stream::once(async {
            json!({
                "spec": spec_acc_monitor(),
                "type_info": {},
                "topic_mapping": {}
            })
            .to_string()
        })
        .boxed_local();
        let _reconf_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_publisher(
                "reconf_publisher".to_string(),
                RECONF_TOPIC.to_string(),
                reconf_stream,
                1,
                mqtt_port,
            ),
            5,
            "reconf_publisher_task",
        ));
        reconf_sub
            .next()
            .await
            .expect("Failed to get reconf message");

        // TODO: Should not be needed in the future when reconf is more stable
        //
        // Wait a while. Needed because the OutputHandler needs to reconnect to the MQTT server,
        // but we have no way of knowing when this is done since runtime is being spawned...
        // Effects visible mainly when running single-threaded either with `-j 1 -- --test-threads 1` or on test runner.
        smol::Timer::after(std::time::Duration::from_millis(2000)).await;

        // Let y_tick end:
        y_tick.send(()).await.expect("Failed to send tick");
        with_timeout_res(y_publisher_task, 5, "y_publisher_task")
            .await
            .expect("y publisher task should finish");

        // Run the rest of the acc spec:
        for x_exp in x_iter2 {
            x_tick.send(()).await.expect("Failed to send tick");
            let x_res = with_timeout(x_sub.next(), 5, "x_sub.next()")
                .await
                .expect("Failed to get x result");
            assert_eq!(x_res, Some(x_exp));

            let z_res = with_timeout(z_sub.next(), 5, "z_sub.next()")
                .await
                .expect("Failed to get z result");
            let z_exp = z_iter.next();
            assert_eq!(z_res, z_exp);
        }

        x_tick.send(()).await.expect("Failed to send tick");
        with_timeout_res(x_publisher_task, 5, "x_publisher_task")
            .await
            .expect("x publisher task should finish");
    }

    #[apply(async_test)]
    async fn test_reconf_add_input_stream(executor: Rc<LocalExecutor<'static>>) {
        // Tests the ReconfSemiSyncRuntime with the acc spec, where we reconfigure to
        // run the simple_add spec, which includes an extra input stream

        let spec = dsrv_specification(&mut spec_acc_monitor()).unwrap();
        let xs = vec![Value::Int(1), Value::Int(3), Value::Int(5), Value::Int(7)];
        let ys = vec![Value::Int(2), Value::Int(4)];
        let y_len = ys.len();
        let expected = vec![
            Value::Int(1),
            Value::Int(4),
            // Here we reconf:
            Value::Int(7),
            Value::Int(9),
            Value::Int(11),
        ];

        let mqtt_server = start_mqtt().await;
        let mqtt_port = with_timeout_res(
            TokioCompat::new(mqtt_server.get_host_port_ipv4(1883)),
            5,
            "get_host_port",
        )
        .await
        .expect("Failed to get host port for MQTT server");

        // Input stream is MQTT server:
        // NOTE: No way of giving new Y_TOPIC after reconf - defaults to /y
        let input_factory = InputStreamFactory::mqtt(
            Some(BTreeMap::from([(X_TOPIC.into(), X_TOPIC.into())])),
            Some(mqtt_port),
        );

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), ys.clone(), mqtt_port);

        let output_mode = OutputMode {
            output_stdout: false,
            output_mqtt_file: None,
            mqtt_output: true,
            output_redis_file: None,
            redis_output: false,
            output_ros_file: None,
        };

        let output_builder = OutputHandlerBuilder::new(output_mode)
            .executor(executor.clone())
            .output_var_names(BTreeSet::from(["z".into()]))
            .mqtt_port(Some(mqtt_port))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestRuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec.clone())
                .input_factory(input_factory)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.build().await;
        executor.spawn(monitor.run()).detach();

        let mut x_sub = with_timeout(
            get_mqtt_outputs(X_TOPIC.to_string(), "x_subscriber".to_string(), mqtt_port),
            5,
            "x_subscriber",
        )
        .await
        .unwrap();
        let mut y_sub = with_timeout(
            get_mqtt_outputs(Y_TOPIC.to_string(), "y_subscriber".to_string(), mqtt_port),
            5,
            "y_subscriber",
        )
        .await
        .unwrap();
        let mut z_sub = with_timeout(
            get_mqtt_outputs(Z_TOPIC.to_string(), "z_subscriber".to_string(), mqtt_port),
            5,
            "z_subscriber",
        )
        .await
        .unwrap();

        let x_iter1 = xs.clone().into_iter().take(y_len);
        let mut x_iter2 = xs.into_iter().skip(y_len);
        let mut y_iter = ys.into_iter();
        let mut z_iter = expected.into_iter();

        // Afterwards we receive one on each tick:
        // (Take the first half of the batch)
        for x_exp in x_iter1 {
            x_tick.send(()).await.expect("Failed to send tick");
            let x_res = with_timeout(x_sub.next(), 5, "x_sub.next()")
                .await
                .expect("Failed to get x result");
            assert_eq!(x_res, Some(x_exp));

            let z_res = with_timeout(z_sub.next(), 5, "z_sub.next()")
                .await
                .expect("Failed to get z result");
            let z_exp = z_iter.next();
            assert_eq!(z_res, z_exp);
        }

        // Reconfigure:
        let mut reconf_sub = with_timeout(
            get_mqtt_outputs(
                RECONF_TOPIC.to_string(),
                "reconf_subscriber".to_string(),
                mqtt_port,
            ),
            5,
            "reconf_subscriber",
        )
        .await
        .unwrap();

        let reconf_stream = futures::stream::once(async {
            json!({
                "spec": spec_simple_add_monitor(),
                "type_info": {},
                "topic_mapping": {}
            })
            .to_string()
        })
        .boxed_local();
        let _reconf_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_publisher(
                "reconf_publisher".to_string(),
                RECONF_TOPIC.to_string(),
                reconf_stream,
                1,
                mqtt_port,
            ),
            5,
            "reconf_publisher_task",
        ));
        reconf_sub
            .next()
            .await
            .expect("Failed to get reconf message");

        // TODO: Should not be needed in the future when reconf is more stable
        //
        // Wait a while. Needed because the OutputHandler needs to reconnect to the MQTT server,
        // but we have no way of knowing when this is done since runtime is being spawned...
        // Effects visible mainly when running single-threaded either with `-j 1 -- --test-threads 1` or on test runner.
        smol::Timer::after(std::time::Duration::from_millis(2000)).await;

        // Take the rest (now with 2 input streams)
        x_tick.send(()).await.expect("Failed to send tick");
        let x_res = with_timeout(x_sub.next(), 5, "x_sub.next()")
            .await
            .expect("Failed to get x result");
        assert_eq!(x_res, x_iter2.next());
        y_tick.send(()).await.expect("Failed to send tick");
        let y_res = with_timeout(y_sub.next(), 5, "y_sub.next()")
            .await
            .expect("Failed to get y result");
        assert_eq!(y_res, y_iter.next());
        let z_res = with_timeout(z_sub.next(), 5, "z_sub.next()")
            .await
            .expect("Failed to get z result");
        let z_exp = z_iter.next();
        info!(?z_res, ?z_exp, "Received z value");
        assert_eq!(z_res, z_exp);

        for (x_exp, y_exp) in x_iter2.zip(y_iter) {
            x_tick.send(()).await.expect("Failed to send tick");
            let x_res = with_timeout(x_sub.next(), 5, "x_sub.next()")
                .await
                .expect("Failed to get x result");
            assert_eq!(x_res, Some(x_exp));

            let z_res = with_timeout(z_sub.next(), 5, "z_sub.next()")
                .await
                .expect("Failed to get z result");
            let z_exp = z_iter.next();
            assert_eq!(z_res, z_exp);

            y_tick.send(()).await.expect("Failed to send tick");
            let y_res = with_timeout(y_sub.next(), 5, "y_sub.next()")
                .await
                .expect("Failed to get y result");
            assert_eq!(y_res, Some(y_exp));

            let z_res = with_timeout(z_sub.next(), 5, "z_sub.next()")
                .await
                .expect("Failed to get z result");
            let z_exp = z_iter.next();
            assert_eq!(z_res, z_exp);
        }

        x_tick.send(()).await.expect("Failed to send tick");
        with_timeout_res(x_publisher_task, 5, "x_publisher_task")
            .await
            .expect("x publisher task should finish");
        y_tick.send(()).await.expect("Failed to send tick");
        with_timeout_res(y_publisher_task, 5, "y_publisher_task")
            .await
            .expect("y publisher task should finish");
    }

    #[apply(async_test)]
    async fn test_reconf_delete_output_stream(executor: Rc<LocalExecutor<'static>>) {
        // Tests the ReconfSemiSyncRuntime with the where we initally have two output streams,
        // and reconfigure into having one

        let spec = dsrv_specification(&mut spec_assignment2_monitor()).unwrap();
        let xs = vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4)];
        let vs = xs.clone();
        let ws = vec![Value::Int(2), Value::Int(3)];
        let ws_len = ws.len();

        let mqtt_server = start_mqtt().await;
        let mqtt_port = with_timeout_res(
            TokioCompat::new(mqtt_server.get_host_port_ipv4(1883)),
            5,
            "get_host_port",
        )
        .await
        .expect("Failed to get host port for MQTT server");

        // Input stream is MQTT server:
        let input_factory = InputStreamFactory::mqtt(
            Some(BTreeMap::from([(X_TOPIC.into(), X_TOPIC.into())])),
            Some(mqtt_port),
        );

        let ((mut x_tick, x_publisher_task), (_, _)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), vec![], mqtt_port);

        let output_mode = OutputMode {
            output_stdout: false,
            output_mqtt_file: None,
            mqtt_output: true,
            output_redis_file: None,
            redis_output: false,
            output_ros_file: None,
        };

        let output_builder = OutputHandlerBuilder::new(output_mode)
            .executor(executor.clone())
            .output_var_names(BTreeSet::from(["v".into(), "w".into()]))
            .mqtt_port(Some(mqtt_port))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestRuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec.clone())
                .input_factory(input_factory)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.build().await;
        executor.spawn(monitor.run()).detach();

        let mut x_sub = with_timeout(
            get_mqtt_outputs(X_TOPIC.to_string(), "x_subscriber".to_string(), mqtt_port),
            5,
            "x_subscriber",
        )
        .await
        .unwrap();
        let mut v_sub = with_timeout(
            get_mqtt_outputs(V_TOPIC.to_string(), "v_subscriber".to_string(), mqtt_port),
            5,
            "v_subscriber",
        )
        .await
        .unwrap();
        let mut w_sub = with_timeout(
            get_mqtt_outputs(W_TOPIC.to_string(), "w_subscriber".to_string(), mqtt_port),
            5,
            "w_subscriber",
        )
        .await
        .unwrap();

        let x_iter1 = xs.clone().into_iter().take(ws_len);
        let x_iter2 = xs.into_iter().skip(ws_len);
        let mut v_iter = vs.into_iter();
        let mut w_iter = ws.into_iter();

        // Take the first half of the batch
        for x_exp in x_iter1 {
            x_tick.send(()).await.expect("Failed to send tick");
            let x_res = with_timeout(x_sub.next(), 5, "x_sub.next()")
                .await
                .expect("Failed to get x result");
            assert_eq!(x_res, Some(x_exp));

            let v_res = with_timeout(v_sub.next(), 5, "v_sub.next()")
                .await
                .expect("Failed to get v result");
            let v_exp = v_iter.next();
            assert_eq!(v_res, v_exp);

            let w_res = with_timeout(w_sub.next(), 5, "w_sub.next()")
                .await
                .expect("Failed to get w result");
            let w_exp = w_iter.next();
            assert_eq!(w_res, w_exp);
        }

        // Reconfigure:
        let mut reconf_sub = with_timeout(
            get_mqtt_outputs(
                RECONF_TOPIC.to_string(),
                "reconf_subscriber".to_string(),
                mqtt_port,
            ),
            5,
            "reconf_subscriber",
        )
        .await
        .unwrap();

        let reconf_stream = futures::stream::once(async {
            json!({
                "spec": spec_assignment_monitor(),
                "type_info": {},
                "topic_mapping": {}
            })
            .to_string()
        })
        .boxed_local();
        let _reconf_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_publisher(
                "reconf_publisher".to_string(),
                RECONF_TOPIC.to_string(),
                reconf_stream,
                1,
                mqtt_port,
            ),
            5,
            "reconf_publisher_task",
        ));
        reconf_sub
            .next()
            .await
            .expect("Failed to get reconf message");

        // TODO: Should not be needed in the future when reconf is more stable
        //
        // Wait a while. Needed because the OutputHandler needs to reconnect to the MQTT server,
        // but we have no way of knowing when this is done since runtime is being spawned...
        // Effects visible mainly when running single-threaded either with `-j 1 -- --test-threads 1` or on test runner.
        smol::Timer::after(std::time::Duration::from_millis(2000)).await;

        // Run the rest of the assignment1 spec:
        for x_exp in x_iter2 {
            x_tick.send(()).await.expect("Failed to send tick");
            let x_res = with_timeout(x_sub.next(), 5, "x_sub.next()")
                .await
                .expect("Failed to get x result");
            assert_eq!(x_res, Some(x_exp));

            let v_res = with_timeout(v_sub.next(), 5, "v_sub.next()")
                .await
                .expect("Failed to get v result");
            let v_exp = v_iter.next();
            assert_eq!(v_res, v_exp);

            // Give w some time to potentially produce a value (it should not after reconf)
            smol::Timer::after(std::time::Duration::from_millis(10)).await;
            let w_res = w_sub.next().now_or_never();
            assert!(
                w_res.is_none() || w_res.unwrap().is_none(),
                "w stream should not produce values after reconf"
            );
        }

        x_tick.send(()).await.expect("Failed to send tick");
        with_timeout_res(x_publisher_task, 5, "x_publisher_task")
            .await
            .expect("x publisher task should finish");
    }

    #[apply(async_test)]
    async fn test_reconf_add_output_stream(executor: Rc<LocalExecutor<'static>>) {
        // Tests the ReconfSemiSyncRuntime with the where we initally have one output streams,
        // and reconfigure into having two

        let spec = dsrv_specification(&mut spec_assignment_monitor()).unwrap();
        let xs = vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4)];
        let vs = xs.clone();
        let ws = vec![Value::Int(4), Value::Int(5)];
        let ws_len = ws.len();

        let mqtt_server = start_mqtt().await;
        let mqtt_port = with_timeout_res(
            TokioCompat::new(mqtt_server.get_host_port_ipv4(1883)),
            5,
            "get_host_port",
        )
        .await
        .expect("Failed to get host port for MQTT server");

        // Input stream is MQTT server:
        let input_factory = InputStreamFactory::mqtt(
            Some(BTreeMap::from([(X_TOPIC.into(), X_TOPIC.into())])),
            Some(mqtt_port),
        );

        let ((mut x_tick, x_publisher_task), (_, _)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), vec![], mqtt_port);

        // NOTE: No way of giving new W_TOPIC after reconf - defaults to /w
        let output_mode = OutputMode {
            output_stdout: false,
            output_mqtt_file: None,
            mqtt_output: true,
            output_redis_file: None,
            redis_output: false,
            output_ros_file: None,
        };

        let output_builder = OutputHandlerBuilder::new(output_mode)
            .executor(executor.clone())
            .output_var_names(BTreeSet::from(["v".into()]))
            .mqtt_port(Some(mqtt_port))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestRuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec.clone())
                .input_factory(input_factory)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.build().await;
        executor.spawn(monitor.run()).detach();

        let mut x_sub = with_timeout(
            get_mqtt_outputs(X_TOPIC.to_string(), "x_subscriber".to_string(), mqtt_port),
            5,
            "x_subscriber",
        )
        .await
        .unwrap();
        let mut v_sub = with_timeout(
            get_mqtt_outputs(V_TOPIC.to_string(), "v_subscriber".to_string(), mqtt_port),
            5,
            "v_subscriber",
        )
        .await
        .unwrap();
        let mut w_sub = with_timeout(
            get_mqtt_outputs(W_TOPIC.to_string(), "w_subscriber".to_string(), mqtt_port),
            5,
            "w_subscriber",
        )
        .await
        .unwrap();

        let x_iter1 = xs.clone().into_iter().take(ws_len);
        let x_iter2 = xs.into_iter().skip(ws_len);
        let mut v_iter = vs.into_iter();
        let mut w_iter = ws.into_iter();

        // Take the first half of the batch
        for x_exp in x_iter1 {
            x_tick.send(()).await.expect("Failed to send tick");
            let x_res = with_timeout(x_sub.next(), 5, "x_sub.next()")
                .await
                .expect("Failed to get x result");
            assert_eq!(x_res, Some(x_exp));

            let v_res = with_timeout(v_sub.next(), 5, "v_sub.next()")
                .await
                .expect("Failed to get v result");
            let v_exp = v_iter.next();
            assert_eq!(v_res, v_exp);
        }

        // Reconfigure:
        let mut reconf_sub = with_timeout(
            get_mqtt_outputs(
                RECONF_TOPIC.to_string(),
                "reconf_subscriber".to_string(),
                mqtt_port,
            ),
            5,
            "reconf_subscriber",
        )
        .await
        .unwrap();

        let reconf_stream = futures::stream::once(async {
            json!({
                "spec": spec_assignment2_monitor(),
                "type_info": {},
                "topic_mapping": {}
            })
            .to_string()
        })
        .boxed_local();
        let _reconf_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_publisher(
                "reconf_publisher".to_string(),
                RECONF_TOPIC.to_string(),
                reconf_stream,
                1,
                mqtt_port,
            ),
            5,
            "reconf_publisher_task",
        ));
        reconf_sub
            .next()
            .await
            .expect("Failed to get reconf message");

        // TODO: Should not be needed in the future when reconf is more stable
        //
        // Wait a while. Needed because the OutputHandler needs to reconnect to the MQTT server,
        // but we have no way of knowing when this is done since runtime is being spawned...
        // Effects visible mainly when running single-threaded either with `-j 1 -- --test-threads 1` or on test runner.
        smol::Timer::after(std::time::Duration::from_millis(2000)).await;

        // Run the rest of the assignment1 spec:
        for x_exp in x_iter2 {
            x_tick.send(()).await.expect("Failed to send tick");
            let x_res = with_timeout(x_sub.next(), 5, "x_sub.next()")
                .await
                .expect("Failed to get x result");
            assert_eq!(x_res, Some(x_exp));

            let v_res = with_timeout(v_sub.next(), 5, "v_sub.next()")
                .await
                .expect("Failed to get v result");
            let v_exp = v_iter.next();
            assert_eq!(v_res, v_exp);

            let w_res = with_timeout(w_sub.next(), 5, "w_sub.next()")
                .await
                .expect("Failed to get w result");
            let w_exp = w_iter.next();
            assert_eq!(w_res, w_exp);
        }

        x_tick.send(()).await.expect("Failed to send tick");
        with_timeout_res(x_publisher_task, 5, "x_publisher_task")
            .await
            .expect("x publisher task should finish");
    }

    #[apply(async_test)]
    async fn test_reconf_sindex_context_transfer_simple(executor: Rc<LocalExecutor<'static>>) {
        // Tests the ReconfSemiSyncRuntime correctly transfers the context from the old spec to the
        // new one

        let spec = dsrv_specification(&mut spec_sindex()).unwrap();
        let xs = vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4)];
        let in_len = xs.len();
        let expected = vec![
            Value::Deferred,
            Value::Int(1),
            // Reconf to same spec here - notice no new deferred
            Value::Int(2),
            Value::Int(3),
        ];

        let mqtt_server = start_mqtt().await;
        let mqtt_port = with_timeout_res(
            TokioCompat::new(mqtt_server.get_host_port_ipv4(1883)),
            5,
            "get_host_port",
        )
        .await
        .expect("Failed to get host port for MQTT server");

        // Input stream is MQTT server:
        let input_factory = InputStreamFactory::mqtt(
            Some(BTreeMap::from([(X_TOPIC.into(), X_TOPIC.into())])),
            Some(mqtt_port),
        );

        let ((mut x_tick, x_publisher_task), (_, _)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), vec![], mqtt_port);

        let output_mode = OutputMode {
            output_stdout: false,
            output_mqtt_file: None,
            mqtt_output: true,
            output_redis_file: None,
            redis_output: false,
            output_ros_file: None,
        };

        let output_builder = OutputHandlerBuilder::new(output_mode)
            .executor(executor.clone())
            .output_var_names(BTreeSet::from(["z".into()]))
            .mqtt_port(Some(mqtt_port))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestRuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec.clone())
                .input_factory(input_factory)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.build().await;
        executor.spawn(monitor.run()).detach();

        let mut x_sub = with_timeout(
            get_mqtt_outputs(X_TOPIC.to_string(), "x_subscriber".to_string(), mqtt_port),
            5,
            "x_subscriber",
        )
        .await
        .unwrap();
        let mut z_sub = with_timeout(
            get_mqtt_outputs(Z_TOPIC.to_string(), "z_subscriber".to_string(), mqtt_port),
            5,
            "z_subscriber",
        )
        .await
        .unwrap();

        let x_iter1 = xs.clone().into_iter().take(in_len / 2);
        let x_iter2 = xs.into_iter().skip(in_len / 2);
        let mut z_iter = expected.into_iter();

        // Take the first half of the batch
        for x_exp in x_iter1 {
            x_tick.send(()).await.expect("Failed to send tick");
            let x_res = with_timeout(x_sub.next(), 5, "x_sub.next()")
                .await
                .expect("Failed to get x result");
            assert_eq!(x_res, Some(x_exp));

            let z_res = with_timeout(z_sub.next(), 5, "z_sub.next()")
                .await
                .expect("Failed to get z result");
            let z_exp = z_iter.next();
            assert_eq!(z_res, z_exp);
        }

        // Reconfigure:
        let mut reconf_sub = with_timeout(
            get_mqtt_outputs(
                RECONF_TOPIC.to_string(),
                "reconf_subscriber".to_string(),
                mqtt_port,
            ),
            5,
            "reconf_subscriber",
        )
        .await
        .unwrap();

        let reconf_stream = futures::stream::once(async {
            json!({
                "spec": spec_sindex(),
                "type_info": {},
                "topic_mapping": {}
            })
            .to_string()
        })
        .boxed_local();
        let _reconf_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_publisher(
                "reconf_publisher".to_string(),
                RECONF_TOPIC.to_string(),
                reconf_stream,
                1,
                mqtt_port,
            ),
            5,
            "reconf_publisher_task",
        ));
        reconf_sub
            .next()
            .await
            .expect("Failed to get reconf message");

        // TODO: Should not be needed in the future when reconf is more stable
        //
        // Wait a while. Needed because the OutputHandler needs to reconnect to the MQTT server,
        // but we have no way of knowing when this is done since runtime is being spawned...
        // Effects visible mainly when running single-threaded either with `-j 1 -- --test-threads 1` or on test runner.
        smol::Timer::after(std::time::Duration::from_millis(2000)).await;

        // Take the rest
        for x_exp in x_iter2 {
            x_tick.send(()).await.expect("Failed to send tick");
            let x_res = with_timeout(x_sub.next(), 5, "x_sub.next()")
                .await
                .expect("Failed to get x result");
            assert_eq!(x_res, Some(x_exp));

            let z_res = with_timeout(z_sub.next(), 5, "z_sub.next()")
                .await
                .expect("Failed to get z result");
            let z_exp = z_iter.next();
            assert_eq!(z_res, z_exp);
        }

        x_tick.send(()).await.expect("Failed to send tick");
        with_timeout_res(x_publisher_task, 5, "x_publisher_task")
            .await
            .expect("x publisher task should finish");
    }
    // TODO: MHK - Implement test with topic mapping (currently unsupported for MQTT)
}
