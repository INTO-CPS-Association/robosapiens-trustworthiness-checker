#[cfg(test)]
#[cfg(feature = "testcontainers")]
mod integration_tests {
    fn input_retry() -> trustworthiness_checker::io::RetryPolicy {
        use std::{num::NonZeroU32, time::Duration};
        use trustworthiness_checker::io::{RetryLimit, RetryPolicy};
        RetryPolicy::new(
            RetryLimit::Attempts(NonZeroU32::MIN),
            Duration::from_millis(250),
            Duration::from_secs(5),
        )
        .unwrap()
    }
    use async_compat::Compat as TokioCompat;

    use futures::StreamExt;
    use futures::stream;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::vec;
    use tc_testutils::mqtt::{
        dummy_stream_mqtt_json_publisher, dummy_stream_mqtt_publisher, get_mqtt_json_outputs,
    };
    use tc_testutils::streams::{
        TickSender, expect_events_serially, tick_stream, with_timeout, with_timeout_res,
    };
    use tracing::info;
    use trustworthiness_checker::async_test;
    use trustworthiness_checker::core::{RuntimeSpec, Semantics, Specification};
    use trustworthiness_checker::dsrv_fixtures::spec_simple_add_monitor;
    use trustworthiness_checker::io::mqtt::{self, MqttInputBackend, MqttMessage};
    use trustworthiness_checker::lang::mstlo::MstloSpecification;

    use trustworthiness_checker::runtime::mstlo::{
        MstloRuntimeBuilder, MstloTimedValue, MstloValue,
    };

    use approx::assert_abs_diff_eq;
    use std::{collections::BTreeMap, rc::Rc};
    use tc_testutils::mqtt::{get_mqtt_outputs, start_mqtt};

    use trustworthiness_checker::dsrv_fixtures::integer_pair_input_stream;
    use trustworthiness_checker::{
        DsrvSpecification, Value, VarName,
        core::Runtime,
        dsrv_fixtures::{float_pair_input_stream, spec_simple_add_monitor_typed_float},
        io::{OutputBackendConfig, OutputDestination, OutputPipeline, Route},
        runtime::{RuntimeBuilder, builder::GeneralRuntimeBuilder},
    };

    const MQTT_INPUT_BACKEND: MqttInputBackend = MqttInputBackend::Rumqttc;

    fn mqtt_output_pipeline<V>(port: u16, routes: BTreeMap<VarName, String>) -> OutputPipeline<V> {
        let routes = routes
            .into_iter()
            .map(|(variable, route)| {
                (
                    variable,
                    Route::new(route.into_boxed_str(), None)
                        .expect("test MQTT output route should be valid"),
                )
            })
            .collect();
        OutputPipeline::from_destination(
            OutputDestination::new("mqtt", OutputBackendConfig::mqtt("localhost", Some(port)))
                .with_route_catalog(routes),
        )
        .expect("test MQTT output destination should construct")
    }

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

    fn mstlo_mqtt_input(time_ms: u64, value: f64) -> MstloTimedValue {
        MstloTimedValue::new(
            std::time::Duration::from_millis(time_ms),
            MstloValue::Float(value),
        )
    }

    fn mstlo_output_tuple(value: &MstloTimedValue) -> (u128, f64) {
        let MstloValue::Float(number) = value.value else {
            panic!("MSTLO MQTT output should contain a float");
        };
        (value.timestamp.as_millis(), number)
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
        let spec = (spec_simple_add_monitor())
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");

        let expected_outputs = vec![Value::Int(3), Value::Int(7)];

        let mqtt_server = start_mqtt().await;
        let mqtt_port = TokioCompat::new(mqtt_server.get_host_port_ipv4(1883))
            .await
            .expect("Failed to get host port for MQTT server");

        let input_stream = integer_pair_input_stream();
        let mqtt_topic = BTreeMap::from_iter(vec![(VarName::new("z"), Z_TOPIC.to_owned())]);

        let outputs = with_timeout(
            get_mqtt_outputs(Z_TOPIC.to_string(), "z_subscriber".to_string(), mqtt_port),
            10,
            "get_mqtt_outputs",
        )
        .await
        .unwrap();

        let async_monitor = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(input_stream)
            .output_pipeline(mqtt_output_pipeline(mqtt_port, mqtt_topic))
            .runtime(RuntimeSpec::Async)
            .semantics(Semantics::Untimed)
            .build()
            .await
            .expect("MQTT async runtime builder should succeed");
        executor.spawn(async_monitor.run()).detach();
        // Test the outputs
        let outputs = with_timeout(outputs.take(2).collect::<Vec<_>>(), 10, "outputs.take")
            .await
            .unwrap();
        assert_eq!(outputs, expected_outputs);
    }

    #[apply(async_test)]
    async fn test_add_monitor_mqtt_output_float(executor: Rc<LocalExecutor<'static>>) {
        let spec = (spec_simple_add_monitor_typed_float())
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");

        let mqtt_server = start_mqtt().await;
        let mqtt_port = TokioCompat::new(mqtt_server.get_host_port_ipv4(1883))
            .await
            .expect("Failed to get host port for MQTT server");

        let input_stream = float_pair_input_stream();
        let mqtt_topics = BTreeMap::from_iter(vec![(VarName::new("z"), Z_TOPIC.to_owned())]);

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

        let async_monitor = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(input_stream)
            .output_pipeline(mqtt_output_pipeline(mqtt_port, mqtt_topics))
            .runtime(RuntimeSpec::Async)
            .semantics(Semantics::Untimed)
            .build()
            .await
            .expect("MQTT async runtime builder should succeed");
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
                input_retry(),
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
                trustworthiness_checker::io::RetryPolicy::new(
                    trustworthiness_checker::io::RetryLimit::Attempts(
                        std::num::NonZeroU32::new(3).unwrap(),
                    ),
                    std::time::Duration::from_millis(250),
                    std::time::Duration::from_secs(5),
                )
                .unwrap(),
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
        let publisher = mqtt::connect(&format!("tcp://localhost:{mqtt_port}")).await?;
        publisher
            .publish(MqttMessage::new(
                topic.to_owned(),
                "not valid JSON".to_owned(),
                1,
            ))
            .await?;
        publisher.disconnect().await?;
        Ok(())
    }

    #[apply(async_test)]
    async fn mqtt_input_reports_malformed_payload() -> anyhow::Result<()> {
        let (_mqtt_server, mqtt_port) = start_mqtt_get_port().await;
        let mut batches = with_timeout_res(
            mqtt::input_stream::<Value>(
                MqttInputBackend::Rumqttc,
                "localhost",
                Some(mqtt_port),
                BTreeMap::from([(VarName::new("x"), X_TOPIC.to_owned())]),
                trustworthiness_checker::io::RetryPolicy::new(
                    trustworthiness_checker::io::RetryLimit::Attempts(
                        std::num::NonZeroU32::new(3).unwrap(),
                    ),
                    std::time::Duration::from_millis(250),
                    std::time::Duration::from_secs(5),
                )
                .unwrap(),
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
        assert!(error.to_string().contains("failed to parse value"));
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
            mqtt::input_stream::<MstloTimedValue>(
                MQTT_INPUT_BACKEND,
                "localhost",
                Some(mqtt_port),
                BTreeMap::from([(VarName::new("x"), MSTLO_IN_TOPIC.to_string())]),
                input_retry(),
            ),
            5,
            "mstlo_input_connect",
        )
        .await?;

        let outputs = with_timeout(
            get_mqtt_json_outputs::<MstloTimedValue>(
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
        let output_writer = mqtt_output_pipeline(
            mqtt_port,
            BTreeMap::from([(VarName::new("robustness"), MSTLO_OUT_TOPIC.to_owned())]),
        )
        .build(formula.output_vars(), std::iter::empty::<VarName>(), None)
        .await?;
        let (input_stream, input_controller) =
            trustworthiness_checker::io::controlled(input_stream);
        let runtime = MstloRuntimeBuilder::<MstloTimedValue>::new()
            .executor(executor.clone())
            .model(formula)
            .input(input_stream.into())
            .output_writer(output_writer)
            .build()
            .await;
        let runtime_task = executor.spawn(runtime.run());

        let values = vec![mstlo_mqtt_input(0, 7.0), mstlo_mqtt_input(10, 4.0)];
        let (mut tick, publish_stream) = tick_stream(stream::iter(values.clone()).boxed_local());
        let publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_json_publisher(
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
            mqtt::input_stream::<MstloTimedValue>(
                MQTT_INPUT_BACKEND,
                "localhost",
                Some(mqtt_port),
                BTreeMap::from([
                    (VarName::new("x"), MSTLO_X_TOPIC.to_string()),
                    (VarName::new("y"), MSTLO_Y_TOPIC.to_string()),
                ]),
                input_retry(),
            ),
            5,
            "mstlo_multi_input_connect",
        )
        .await?;

        let mut gt_outputs = with_timeout(
            get_mqtt_json_outputs::<MstloTimedValue>(
                MSTLO_GT_TOPIC.to_string(),
                "mstlo_multi_gt_subscriber".to_string(),
                mqtt_port,
            ),
            5,
            "mstlo multi gt output subscription",
        )
        .await?;
        let _lt_outputs = with_timeout(
            get_mqtt_json_outputs::<MstloTimedValue>(
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
        let output_writer = mqtt_output_pipeline(
            mqtt_port,
            BTreeMap::from([
                (VarName::new("gt"), MSTLO_GT_TOPIC.to_owned()),
                (VarName::new("lt"), MSTLO_LT_TOPIC.to_owned()),
            ]),
        )
        .build(formula.output_vars(), std::iter::empty::<VarName>(), None)
        .await?;
        let runtime = MstloRuntimeBuilder::<MstloTimedValue>::new()
            .executor(executor.clone())
            .model(formula)
            .input(input_stream.into())
            .output_writer(output_writer)
            .build()
            .await;
        let runtime_task = executor.spawn(runtime.run());

        let x_values = vec![mstlo_mqtt_input(0, 7.0)];
        let y_values = vec![mstlo_mqtt_input(0, 2.0)];
        let (mut x_tick, x_stream) = tick_stream(stream::iter(x_values.clone()).boxed_local());
        let (mut y_tick, y_stream) = tick_stream(stream::iter(y_values.clone()).boxed_local());
        let x_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_json_publisher(
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
            dummy_stream_mqtt_json_publisher(
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
        let spec: DsrvSpecification = (spec_src)
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");
        let (_mqtt_server, mqtt_port) = start_mqtt_get_port().await;

        let var_topics = BTreeMap::from_iter([("payload".into(), "payload".to_string())]);
        let input_stream = with_timeout_res(
            mqtt::input_stream(
                MQTT_INPUT_BACKEND,
                "localhost",
                Some(mqtt_port),
                var_topics,
                input_retry(),
            ),
            5,
            "input_stream_connect",
        )
        .await?;

        let (output_sender, output_receiver) =
            async_unsync::bounded::channel::<BTreeMap<VarName, Value>>(1024).into_split();
        let outputs = Box::pin(futures::stream::unfold(
            output_receiver,
            |mut receiver| async move { receiver.recv().await.map(|row| (row, receiver)) },
        ));

        let monitor: Box<dyn Runtime> = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(input_stream)
            .output_pipeline(OutputPipeline::from_backend(OutputBackendConfig::channel(
                output_sender,
            )))
            .runtime(RuntimeSpec::Async)
            .semantics(semantics)
            .build()
            .await
            .expect("MQTT JSON object runtime builder should succeed");
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
            outputs.enumerate().take(4).collect::<Vec<_>>(),
            5,
            "mqtt json object input outputs.collect()",
        )
        .await?;

        payload_tick.send(()).await?;
        publisher_task.await?;

        Ok(outputs)
    }

    fn assert_async_json_object_outputs(outputs: &[(usize, BTreeMap<VarName, Value>)]) {
        assert_eq!(outputs.len(), 4);
        assert!(
            outputs.iter().all(|(_, tick)| tick.len() == 1),
            "async outputs must remain independent singleton ticks"
        );

        let selected = outputs
            .iter()
            .filter_map(|(_, tick)| tick.get(&VarName::new("selected")))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(selected, vec![Value::Int(10), Value::Int(30)]);

        let echoed = outputs
            .iter()
            .filter_map(|(_, tick)| tick.get(&VarName::new("echoed")))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            echoed,
            vec![
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
            ]
        );
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

        assert_async_json_object_outputs(&outputs);
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

        assert_async_json_object_outputs(&outputs);
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
                input_retry(),
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
    use futures::{StreamExt, stream};
    use macro_rules_attribute::apply;
    use serde_json::json;
    use smol::LocalExecutor;
    use std::collections::BTreeMap;
    use std::rc::Rc;
    use tc_testutils::mqtt::{
        dummy_stream_mqtt_payload_publisher, dummy_stream_mqtt_publisher, get_mqtt_outputs,
        start_mqtt,
    };
    use tc_testutils::streams::{TickSender, tick_stream, with_timeout, with_timeout_res};
    use tracing::info;
    use trustworthiness_checker::DsrvSpecification;
    use trustworthiness_checker::async_test;

    use trustworthiness_checker::core::Runtime;
    use trustworthiness_checker::core::values::Value;
    use trustworthiness_checker::dsrv_fixtures::*;
    use trustworthiness_checker::io::{
        InputPipeline, InputSource, OutputBackendConfig, OutputPipeline, Route,
    };

    use trustworthiness_checker::runtime::RuntimeBuilder;
    use trustworthiness_checker::runtime::builder::SemiSyncValueConfig;
    use trustworthiness_checker::runtime::reconfigurable_semi_sync::ReconfSemiSyncRuntimeBuilder;
    use trustworthiness_checker::semantics::UntimedDsrvSemantics;

    type TestRuntimeBuilder =
        ReconfSemiSyncRuntimeBuilder<SemiSyncValueConfig, UntimedDsrvSemantics>;

    fn parse_str(input: &str) -> anyhow::Result<DsrvSpecification> {
        Ok(input.parse()?)
    }

    fn route(topic: &str) -> Route {
        Route::new(topic.to_owned().into_boxed_str(), None).expect("test route is non-empty")
    }

    // TODO: Add a clonable in-memory output backend for tests that need mpsc channels.

    const X_TOPIC: &str = "x";
    const Y_TOPIC: &str = "y";
    const Z_TOPIC: &str = "z";
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
    async fn test_reconf_no_change_of_streams(executor: Rc<LocalExecutor<'static>>) {
        // Tests the ReconfSemiSyncRuntime with the simple add monitor, where we reconfigure but do
        // not introduce/remove any streams

        let spec = (spec_simple_add_monitor())
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");
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
        let input_source = InputSource::mqtt(
            Some(BTreeMap::from([
                (X_TOPIC.into(), route(X_TOPIC)),
                (Y_TOPIC.into(), route(Y_TOPIC)),
            ])),
            Some(mqtt_port),
        );

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), ys.clone(), mqtt_port);

        let output_pipeline =
            OutputPipeline::from_backend(OutputBackendConfig::mqtt("localhost", Some(mqtt_port)));
        let monitor_builder = Box::new(
            TestRuntimeBuilder::new()
                .parse_spec(parse_str)
                .executor(executor.clone())
                .model(spec.clone())
                .input_pipeline(InputPipeline::new(input_source))
                .output_pipeline(output_pipeline)
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
                "specification": spec_simple_add_monitor_plus_one()
            })
            .to_string()
        })
        .boxed_local();
        let _reconf_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_payload_publisher(
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
        // Wait a while. Needed because the MQTT output path reconnects to the server,
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
    async fn test_reconf_add_input_stream(executor: Rc<LocalExecutor<'static>>) {
        // Tests the ReconfSemiSyncRuntime with the acc spec, where we reconfigure to
        // run the simple_add spec, which includes an extra input stream

        let spec = (spec_acc_monitor())
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");
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

        // An unmapped MQTT source uses the variable name as its default route,
        // allowing the replacement model to add `y` without a fake binding.
        let input_source = InputSource::mqtt(None, Some(mqtt_port));

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), ys.clone(), mqtt_port);

        let output_pipeline =
            OutputPipeline::from_backend(OutputBackendConfig::mqtt("localhost", Some(mqtt_port)));
        let monitor_builder = Box::new(
            TestRuntimeBuilder::new()
                .parse_spec(parse_str)
                .executor(executor.clone())
                .model(spec.clone())
                .input_pipeline(InputPipeline::new(input_source))
                .output_pipeline(output_pipeline)
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
                "specification": spec_simple_add_monitor()
            })
            .to_string()
        })
        .boxed_local();
        let _reconf_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_payload_publisher(
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
        // Wait a while. Needed because the MQTT output path reconnects to the server,
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

    // TODO: MHK - Implement test with topic mapping (currently unsupported for MQTT)
}

#[cfg(feature = "testcontainers")]
#[cfg(test)]
mod reconf_dataflow_mqtt_tests {
    use async_compat::Compat as TokioCompat;
    use async_unsync::bounded;
    use futures::{StreamExt, stream};
    use macro_rules_attribute::apply;
    use serde_json::json;
    use smol::LocalExecutor;
    use std::{collections::BTreeMap, rc::Rc};
    use tc_testutils::mqtt::{
        dummy_mqtt_publisher, dummy_stream_mqtt_payload_publisher, get_mqtt_outputs, start_mqtt,
    };
    use tc_testutils::streams::{with_timeout, with_timeout_res};
    use trustworthiness_checker::async_test;
    use trustworthiness_checker::core::{ExecutionPolicy, Runtime, RuntimeSpec, Semantics};

    use trustworthiness_checker::io::mqtt::MqttInputBackend;
    use trustworthiness_checker::io::{
        InputPipeline, InputSource, OutputBackendConfig, OutputDestination, OutputPipeline, Route,
    };
    use trustworthiness_checker::runtime::builder::GeneralRuntimeBuilder;
    use trustworthiness_checker::runtime::dataflow::ReconfigurationAck;
    use trustworthiness_checker::{DsrvSpecification, Value, VarName};

    const INPUT_A: &str = "reconf-dataflow/input-a";
    const INPUT_B: &str = "reconf-dataflow/input-b";
    const OUT_A: &str = "reconf-dataflow/output-a";
    const OUT_B: &str = "reconf-dataflow/output-b";
    const CONTROL_TOPIC: &str = "reconf-dataflow/control";
    const SPECIFICATION: &str = "in x\nout z\nz = x";

    fn route(topic: &str) -> Route {
        Route::new(topic.to_owned().into_boxed_str(), None)
            .expect("test MQTT route should be non-empty")
    }

    fn mqtt_output_pipeline(port: u16) -> OutputPipeline<Value> {
        let destination =
            OutputDestination::new("mqtt", OutputBackendConfig::mqtt("localhost", Some(port)))
                .with_route_catalog(BTreeMap::from([(VarName::new("z"), route(OUT_A))]));
        OutputPipeline::from_destination(destination)
            .expect("test MQTT output destination should construct")
    }

    async fn exercise_reconf_dataflow_mqtt(
        executor: Rc<LocalExecutor<'static>>,
        backend: MqttInputBackend,
    ) -> anyhow::Result<()> {
        let mqtt_server = start_mqtt().await;
        let mqtt_port = with_timeout_res(
            TokioCompat::new(mqtt_server.get_host_port_ipv4(1883)),
            5,
            "get_host_port",
        )
        .await?;

        // Subscribe before opening the runtime so the initial output cannot race the subscriber.
        let mut out_a = with_timeout(
            get_mqtt_outputs(
                OUT_A.to_owned(),
                "reconf_dataflow_out_a_subscriber".to_owned(),
                mqtt_port,
            ),
            5,
            "OUT_A subscriber",
        )
        .await?;
        let mut out_b = with_timeout(
            get_mqtt_outputs(
                OUT_B.to_owned(),
                "reconf_dataflow_out_b_subscriber".to_owned(),
                mqtt_port,
            ),
            5,
            "OUT_B subscriber",
        )
        .await?;

        let input_source = InputSource::mqtt_with_routes(
            Some(BTreeMap::from([(VarName::new("x"), route(INPUT_A))])),
            Some(mqtt_port),
            backend,
        )
        .with_reconfiguration_route(CONTROL_TOPIC)?;
        let (ack_tx, mut ack_rx) = bounded::channel::<ReconfigurationAck>(1).into_split();
        let spec = SPECIFICATION
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");

        let runtime = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec)
            .input_pipeline(InputPipeline::new(input_source))?
            .output_pipeline(mqtt_output_pipeline(mqtt_port))
            .runtime(RuntimeSpec::ReconfDataflow(ExecutionPolicy::Synchronous))
            .semantics(Semantics::Untimed)
            .reconf_topic(CONTROL_TOPIC.to_owned())
            .acknowledgements(ack_tx)
            .build()
            .await?;
        executor.spawn(runtime.run()).detach();

        with_timeout_res(
            dummy_mqtt_publisher(
                "reconf_dataflow_initial_input".to_owned(),
                INPUT_A.to_owned(),
                vec![Value::Int(1)],
                mqtt_port,
            ),
            5,
            "initial input publisher",
        )
        .await?;
        let initial_output = with_timeout(out_a.next(), 5, "initial OUT_A output").await?;
        assert_eq!(initial_output, Some(Value::Int(1)));

        let request = json!({
            "specification": SPECIFICATION,
            "input": {
                "source": "default",
                "inputs": { "x": INPUT_B },
            },
            "output": {
                "outputs": { "z": OUT_B },
            },
        })
        .to_string();
        let control_publisher = executor.spawn(with_timeout_res(
            dummy_stream_mqtt_payload_publisher(
                "reconf_dataflow_control".to_owned(),
                CONTROL_TOPIC.to_owned(),
                stream::once(async move { request }).boxed_local(),
                1,
                mqtt_port,
            ),
            5,
            "reconfiguration publisher",
        ));

        let acknowledgement = with_timeout(ack_rx.recv(), 5, "reconfiguration acknowledgement")
            .await
            .expect("reconfiguration acknowledgement should arrive")
            .expect("reconfiguration acknowledgement channel should remain open");
        assert!(!acknowledgement.monitor_changed);
        assert!(acknowledgement.interface_changed);
        with_timeout_res(control_publisher, 5, "reconfiguration publisher task").await?;

        // Both output subscribers must stay quiet for an old-topic publish after the barrier. The
        // OUT_B assertion also catches an input cutover that accidentally leaves the old topic live.
        with_timeout_res(
            dummy_mqtt_publisher(
                "reconf_dataflow_old_input".to_owned(),
                INPUT_A.to_owned(),
                vec![Value::Int(2)],
                mqtt_port,
            ),
            5,
            "old input publisher",
        )
        .await?;
        let (old_route_output, new_route_output) = futures::join!(
            with_timeout(out_a.next(), 1, "output on old route after cutover"),
            with_timeout(out_b.next(), 1, "output on new route from old input"),
        );
        assert!(
            old_route_output.is_err(),
            "old input topic must not produce output on OUT_A"
        );
        assert!(
            new_route_output.is_err(),
            "old input topic must not produce output on OUT_B"
        );

        with_timeout_res(
            dummy_mqtt_publisher(
                "reconf_dataflow_new_input".to_owned(),
                INPUT_B.to_owned(),
                vec![Value::Int(3)],
                mqtt_port,
            ),
            5,
            "new input publisher",
        )
        .await?;
        let new_output = with_timeout(out_b.next(), 5, "new OUT_B output").await?;
        assert_eq!(new_output, Some(Value::Int(3)));

        let old_route_after_new_input =
            with_timeout(out_a.next(), 1, "output on old route after new input").await;
        assert!(
            old_route_after_new_input.is_err(),
            "post-acknowledgement output must use only OUT_B"
        );

        Ok(())
    }

    #[apply(async_test)]
    async fn test_reconf_dataflow_mqtt_input(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        exercise_reconf_dataflow_mqtt(executor, MqttInputBackend::Rumqttc).await
    }
}
