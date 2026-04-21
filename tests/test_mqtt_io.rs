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
        TickSender, interleave_with_constant, receive_values_serially, tick_stream, with_timeout,
        with_timeout_res,
    };
    use tracing::{error, info};
    use trustworthiness_checker::InputProvider;
    use trustworthiness_checker::async_test;
    use trustworthiness_checker::dsrv_fixtures::spec_simple_add_monitor;
    use trustworthiness_checker::io::mqtt::MqttFactory;
    use winnow::Parser;

    use approx::assert_abs_diff_eq;
    use std::{collections::BTreeMap, rc::Rc};
    use tc_testutils::mqtt::{get_mqtt_outputs, start_mqtt};

    use trustworthiness_checker::dsrv_fixtures::{TestMonitorRunner, input_streams1};
    use trustworthiness_checker::{
        Value,
        core::Runnable,
        dsrv_fixtures::{input_streams_float, spec_simple_add_monitor_typed_float},
        dsrv_specification,
        io::mqtt::{MQTTInputProvider, MQTTOutputHandler},
    };

    const MQTT_FACTORY: MqttFactory = MqttFactory::Paho;

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

    fn generate_test_publisher_tasks(
        executor: Rc<LocalExecutor<'static>>,
        xs: Vec<Value>,
        ys: Vec<Value>,
        mqtt_port: u16,
    ) -> (
        (TickSender, smol::Task<anyhow::Result<()>>),
        (TickSender, smol::Task<anyhow::Result<()>>),
    ) {
        let (x_tick, x_pub_stream) = tick_stream(stream::iter(xs.clone()).boxed());
        let (y_tick, y_pub_stream) = tick_stream(stream::iter(ys.clone()).boxed());

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

        let input_streams = input_streams1();
        let mqtt_host = "localhost";
        let mqtt_topic = BTreeMap::from_iter(vec![("z".into(), Z_TOPIC.into())]);

        let outputs = with_timeout(
            get_mqtt_outputs(Z_TOPIC.to_string(), "z_subscriber".to_string(), mqtt_port),
            10,
            "get_mqtt_outputs",
        )
        .await
        .unwrap();

        let mut output_handler = MQTTOutputHandler::new(
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
        let async_monitor = TestMonitorRunner::new(
            executor.clone(),
            spec.clone(),
            Box::new(input_streams),
            output_handler,
        );
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

        let input_streams = input_streams_float();
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

        let mut output_handler = MQTTOutputHandler::new(
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
        let async_monitor = TestMonitorRunner::new(
            executor.clone(),
            spec.clone(),
            Box::new(input_streams),
            output_handler,
        );
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
        let stream_len = xs.len();

        let (_mqtt_server, mqtt_port) = start_mqtt_get_port().await;

        let var_topics = BTreeMap::from_iter([
            ("x".into(), X_TOPIC.to_string()),
            ("y".into(), Y_TOPIC.to_string()),
        ]);

        // Create the MQTT input provider
        let mut input_provider = MQTTInputProvider::new(
            executor.clone(),
            MQTT_FACTORY,
            "localhost",
            Some(mqtt_port),
            var_topics,
            0,
        );
        with_timeout_res(input_provider.connect(), 5, "input_provider_connect").await?;

        let x_stream = input_provider
            .var_stream(&"x".into())
            .ok_or_else(|| anyhow::anyhow!("x stream unavailable"))?;
        let y_stream = input_provider
            .var_stream(&"y".into())
            .ok_or_else(|| anyhow::anyhow!("y stream unavailable"))?;

        // Note: Test should be refactored to use control_stream instead of spawning with old `run`
        // behavior.
        let mut input_provider_stream = input_provider.control_stream().await;
        let input_provider_future = Box::pin(async move {
            while let Some(res) = input_provider_stream.next().await {
                if res.is_err() {
                    error!("Input provider stream returned error: {:?}", res);
                    return res;
                }
            }
            Ok(())
        });
        executor.spawn(input_provider_future).detach();

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), ys.clone(), mqtt_port);

        let (x_vals, y_vals) =
            receive_values_serially(&mut x_tick, &mut y_tick, x_stream, y_stream, stream_len)
                .await?;

        let exp_iter = xs.clone().into_iter().zip(ys.clone().into_iter());
        let (exp_x_vals, exp_y_vals) = interleave_with_constant(exp_iter, Value::NoVal);
        info!(?x_vals, ?y_vals, "Received values");
        assert_eq!(x_vals, exp_x_vals);
        assert_eq!(y_vals, exp_y_vals);

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
    async fn test_add_monitor_mqtt_input_float(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let xs = vec![Value::Float(1.3), Value::Float(3.4)];
        let ys = vec![Value::Float(2.4), Value::Float(4.3)];
        let stream_len = xs.len();

        let (_mqtt_server, mqtt_port) = start_mqtt_get_port().await;

        let var_topics = BTreeMap::from_iter([
            ("x".into(), X_TOPIC.to_string()),
            ("y".into(), Y_TOPIC.to_string()),
        ]);

        // Create the MQTT input provider
        let mut input_provider = MQTTInputProvider::new(
            executor.clone(),
            MQTT_FACTORY,
            "localhost",
            Some(mqtt_port),
            var_topics,
            0,
        );
        with_timeout_res(input_provider.connect(), 5, "input_provider_connect").await?;

        let x_stream = input_provider
            .var_stream(&"x".into())
            .ok_or_else(|| anyhow::anyhow!("x stream unavailable"))?;
        let y_stream = input_provider
            .var_stream(&"y".into())
            .ok_or_else(|| anyhow::anyhow!("y stream unavailable"))?;

        // Note: Test should be refactored to use control_stream instead of spawning with old `run`
        // behavior.
        let mut input_provider_stream = input_provider.control_stream().await;
        let input_provider_future = Box::pin(async move {
            while let Some(res) = input_provider_stream.next().await {
                if res.is_err() {
                    error!("Input provider stream returned error: {:?}", res);
                    return res;
                }
            }
            Ok(())
        });
        executor.spawn(input_provider_future).detach();

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), ys.clone(), mqtt_port);

        let (x_vals, y_vals) =
            receive_values_serially(&mut x_tick, &mut y_tick, x_stream, y_stream, stream_len)
                .await?;

        let exp_iter = xs.clone().into_iter().zip(ys.clone().into_iter());
        let (exp_x_vals, exp_y_vals) = interleave_with_constant(exp_iter, Value::NoVal);
        info!(?x_vals, ?y_vals, "Received values");
        assert_eq!(x_vals, exp_x_vals);
        assert_eq!(y_vals, exp_y_vals);

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
    use std::rc::Rc;
    use tc_testutils::mqtt::{dummy_stream_mqtt_publisher, get_mqtt_outputs, start_mqtt};
    use tc_testutils::streams::{TickSender, tick_stream, with_timeout, with_timeout_res};
    use tracing::info;
    use trustworthiness_checker::async_test;
    use trustworthiness_checker::cli::args::OutputMode;
    use trustworthiness_checker::core::values::Value;
    use trustworthiness_checker::core::{AbstractMonitorBuilder, Runnable};
    use trustworthiness_checker::dsrv_fixtures::*;
    use trustworthiness_checker::dsrv_specification;
    use trustworthiness_checker::io::builders::{
        InputProviderBuilder, InputProviderSpec, OutputHandlerBuilder,
    };
    use trustworthiness_checker::lang::dsrv::lalr_parser::LALRParser;
    use trustworthiness_checker::runtime::builder::SemiSyncValueConfig;
    use trustworthiness_checker::runtime::reconfigurable_semi_sync::ReconfSemiSyncMonitorBuilder;
    use trustworthiness_checker::semantics::UntimedDsrvSemantics;

    type TestMonitorBuilder = ReconfSemiSyncMonitorBuilder<
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
        let (x_tick, x_pub_stream) = tick_stream(stream::iter(xs.clone()).boxed());
        let (y_tick, y_pub_stream) = tick_stream(stream::iter(ys.clone()).boxed());

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
        // Tests the ReconfSemiSyncMonitor with the simple add monitor, without actually sending a
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

        // InputProvider is MQTT server:
        let input_spec = InputProviderSpec::MQTT(Some(vec![X_TOPIC.into(), Y_TOPIC.into()]));
        let input_builder = InputProviderBuilder::new(input_spec)
            .model(spec.clone())
            .executor(executor.clone())
            .mqtt_port(Some(mqtt_port));

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), ys.clone(), mqtt_port);

        let output_mode = OutputMode {
            output_stdout: false,
            output_mqtt_topics: Some(vec![Z_TOPIC.into()]),
            mqtt_output: true,
            output_redis_topics: None,
            redis_output: false,
            output_ros_file: None,
        };

        let output_builder = OutputHandlerBuilder::new(output_mode)
            .executor(executor.clone())
            .output_var_names(vec!["z".into()])
            .mqtt_port(Some(mqtt_port))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestMonitorBuilder::new()
                .executor(executor.clone())
                .model(spec.clone())
                .input_builder(input_builder)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.async_build().await;
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
        // Tests the ReconfSemiSyncMonitor with the simple add monitor, where we reconfigure but do
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

        // InputProvider is MQTT server:
        let input_spec = InputProviderSpec::MQTT(Some(vec![X_TOPIC.into(), Y_TOPIC.into()]));
        let input_builder = InputProviderBuilder::new(input_spec)
            .model(spec.clone())
            .executor(executor.clone())
            .mqtt_port(Some(mqtt_port));

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), ys.clone(), mqtt_port);

        let output_mode = OutputMode {
            output_stdout: false,
            output_mqtt_topics: Some(vec![Z_TOPIC.into()]),
            mqtt_output: true,
            output_redis_topics: None,
            redis_output: false,
            output_ros_file: None,
        };

        let output_builder = OutputHandlerBuilder::new(output_mode)
            .executor(executor.clone())
            .output_var_names(vec!["z".into()])
            .mqtt_port(Some(mqtt_port))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestMonitorBuilder::new()
                .executor(executor.clone())
                .model(spec.clone())
                .input_builder(input_builder)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.async_build().await;
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
                "type_info": {}
            })
            .to_string()
        })
        .boxed();
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
        // Tests the ReconfSemiSyncMonitor with the simple add monitor, where we reconfigure to a
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

        // InputProvider is MQTT server:
        let input_spec = InputProviderSpec::MQTT(Some(vec![X_TOPIC.into(), Y_TOPIC.into()]));
        let input_builder = InputProviderBuilder::new(input_spec)
            .model(spec.clone())
            .executor(executor.clone())
            .mqtt_port(Some(mqtt_port));

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), ys.clone(), mqtt_port);

        let output_mode = OutputMode {
            output_stdout: false,
            output_mqtt_topics: Some(vec![Z_TOPIC.into()]),
            mqtt_output: true,
            output_redis_topics: None,
            redis_output: false,
            output_ros_file: None,
        };

        let output_builder = OutputHandlerBuilder::new(output_mode)
            .executor(executor.clone())
            .output_var_names(vec!["z".into()])
            .mqtt_port(Some(mqtt_port))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestMonitorBuilder::new()
                .executor(executor.clone())
                .model(spec.clone())
                .input_builder(input_builder)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.async_build().await;
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
                "type_info": {}
            })
            .to_string()
        })
        .boxed();
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
        // Tests the ReconfSemiSyncMonitor with the acc spec, where we reconfigure to
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

        // InputProvider is MQTT server:
        // NOTE: No way of giving new Y_TOPIC after reconf - defaults to /y
        let input_spec = InputProviderSpec::MQTT(Some(vec![X_TOPIC.into()]));
        let input_builder = InputProviderBuilder::new(input_spec)
            .model(spec.clone())
            .executor(executor.clone())
            .mqtt_port(Some(mqtt_port));

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), ys.clone(), mqtt_port);

        let output_mode = OutputMode {
            output_stdout: false,
            output_mqtt_topics: Some(vec![Z_TOPIC.into()]),
            mqtt_output: true,
            output_redis_topics: None,
            redis_output: false,
            output_ros_file: None,
        };

        let output_builder = OutputHandlerBuilder::new(output_mode)
            .executor(executor.clone())
            .output_var_names(vec!["z".into()])
            .mqtt_port(Some(mqtt_port))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestMonitorBuilder::new()
                .executor(executor.clone())
                .model(spec.clone())
                .input_builder(input_builder)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.async_build().await;
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
                "type_info": {}
            })
            .to_string()
        })
        .boxed();
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
        // Tests the ReconfSemiSyncMonitor with the where we initally have two output streams,
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

        // InputProvider is MQTT server:
        let input_spec = InputProviderSpec::MQTT(Some(vec![X_TOPIC.into()]));
        let input_builder = InputProviderBuilder::new(input_spec)
            .model(spec.clone())
            .executor(executor.clone())
            .mqtt_port(Some(mqtt_port));

        let ((mut x_tick, x_publisher_task), (_, _)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), vec![], mqtt_port);

        let output_mode = OutputMode {
            output_stdout: false,
            output_mqtt_topics: Some(vec![V_TOPIC.into(), W_TOPIC.into()]),
            mqtt_output: true,
            output_redis_topics: None,
            redis_output: false,
            output_ros_file: None,
        };

        let output_builder = OutputHandlerBuilder::new(output_mode)
            .executor(executor.clone())
            .output_var_names(vec!["v".into(), "w".into()])
            .mqtt_port(Some(mqtt_port))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestMonitorBuilder::new()
                .executor(executor.clone())
                .model(spec.clone())
                .input_builder(input_builder)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.async_build().await;
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
                "type_info": {}
            })
            .to_string()
        })
        .boxed();
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
        // Tests the ReconfSemiSyncMonitor with the where we initally have one output streams,
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

        // InputProvider is MQTT server:
        let input_spec = InputProviderSpec::MQTT(Some(vec![X_TOPIC.into()]));
        let input_builder = InputProviderBuilder::new(input_spec)
            .model(spec.clone())
            .executor(executor.clone())
            .mqtt_port(Some(mqtt_port));

        let ((mut x_tick, x_publisher_task), (_, _)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), vec![], mqtt_port);

        // NOTE: No way of giving new W_TOPIC after reconf - defaults to /w
        let output_mode = OutputMode {
            output_stdout: false,
            output_mqtt_topics: Some(vec![V_TOPIC.into()]),
            mqtt_output: true,
            output_redis_topics: None,
            redis_output: false,
            output_ros_file: None,
        };

        let output_builder = OutputHandlerBuilder::new(output_mode)
            .executor(executor.clone())
            .output_var_names(vec!["v".into()])
            .mqtt_port(Some(mqtt_port))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestMonitorBuilder::new()
                .executor(executor.clone())
                .model(spec.clone())
                .input_builder(input_builder)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.async_build().await;
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
                "type_info": {}
            })
            .to_string()
        })
        .boxed();
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

    #[ignore] // TODO: MHK fix test when context transfer is implemented
    #[apply(async_test)]
    async fn test_reconf_sindex_context_transfer(executor: Rc<LocalExecutor<'static>>) {
        // Tests the ReconfSemiSyncMonitor correctly transfers the context from the old spec to the
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

        // InputProvider is MQTT server:
        let input_spec = InputProviderSpec::MQTT(Some(vec![X_TOPIC.into()]));
        let input_builder = InputProviderBuilder::new(input_spec)
            .model(spec.clone())
            .executor(executor.clone())
            .mqtt_port(Some(mqtt_port));

        let ((mut x_tick, x_publisher_task), (_, _)) =
            generate_test_publisher_tasks(executor.clone(), xs.clone(), vec![], mqtt_port);

        let output_mode = OutputMode {
            output_stdout: false,
            output_mqtt_topics: Some(vec![Z_TOPIC.into()]),
            mqtt_output: true,
            output_redis_topics: None,
            redis_output: false,
            output_ros_file: None,
        };

        let output_builder = OutputHandlerBuilder::new(output_mode)
            .executor(executor.clone())
            .output_var_names(vec!["z".into()])
            .mqtt_port(Some(mqtt_port))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestMonitorBuilder::new()
                .executor(executor.clone())
                .model(spec.clone())
                .input_builder(input_builder)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.async_build().await;
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
                "type_info": {}
            })
            .to_string()
        })
        .boxed();
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
}
