#[cfg(test)]
#[cfg(feature = "ros")]
mod integration_tests {
    use std::collections::BTreeMap;
    use std::rc::Rc;
    use std::time::Duration;

    use async_unsync::bounded;
    use futures::{FutureExt, StreamExt, future};
    use macro_rules_attribute::apply;
    use r2r::{
        WrappedTypesupport,
        std_msgs::msg::{Int32, String as RosString},
    };
    use smol::LocalExecutor;
    use tc_testutils::ros::generate_xy_test_publisher_tasks_with_topics;
    use tc_testutils::ros::qualified_ros_name;
    use tc_testutils::ros::recv_ros_int_stream;
    use tc_testutils::streams::{expect_events_serially, with_timeout};
    use tracing::info;
    use trustworthiness_checker::async_test;
    use trustworthiness_checker::core::{ExecutionPolicy, Runtime, RuntimeSpec, Semantics};
    use trustworthiness_checker::io::ros;
    use trustworthiness_checker::io::ros::ros_topic_stream_mapping::{
        RosMsgType, VariableMappingData,
    };
    use trustworthiness_checker::io::{
        FormatId, InputPipeline, InputSource, OutputBackendConfig, OutputDestination,
        OutputPipeline, Route,
    };
    use trustworthiness_checker::runtime::dataflow::ReconfigurationAck;
    use trustworthiness_checker::utils::cancellation_token::CancellationToken;
    use trustworthiness_checker::{DsrvSpecification, LocalStream, OutputBatch, Value, VarName};

    #[apply(async_test)]
    async fn test_add_monitor_ros_input(ex: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
        let xs_ros = vec![Int32 { data: 1 }, Int32 { data: 2 }];
        let ys_ros = vec![Int32 { data: 3 }, Int32 { data: 4 }];
        let xs = vec![Value::Int(1), Value::Int(2)];
        let ys = vec![Value::Int(3), Value::Int(4)];

        let x_topic = qualified_ros_name(test_add_monitor_ros_input, "x");
        let y_topic = qualified_ros_name(test_add_monitor_ros_input, "y");

        let var_topics = BTreeMap::from([
            (
                "x".to_string(),
                VariableMappingData {
                    topic: x_topic.clone(),
                    msg_type: RosMsgType::Int32,
                },
            ),
            (
                "y".to_string(),
                VariableMappingData {
                    topic: y_topic.clone(),
                    msg_type: RosMsgType::Int32,
                },
            ),
        ]);

        let (mut input_stream, mut input_owner) = ros::open_ros_input(ex.clone(), var_topics)?;

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_xy_test_publisher_tasks_with_topics(
                ex.clone(),
                test_add_monitor_ros_input,
                &x_topic,
                &y_topic,
                xs_ros,
                ys_ros,
            );

        expect_events_serially(&mut x_tick, &mut y_tick, &mut input_stream, xs, ys).await?;

        // Final ticks to let them complete
        x_tick.send(()).await?;
        y_tick.send(()).await?;
        // Wait for publishers to complete and then shutdown MQTT server to terminate connections
        info!("Waiting for publishers to complete...");
        x_publisher_task.await?;
        y_publisher_task.await?;
        info!("All publishers completed, shutting down MQTT server");

        input_owner.shutdown().await?;
        Ok(())
    }

    #[apply(async_test)]
    async fn test_add_monitor_ros_output_with_aux(
        ex: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let z = VarName::new("z");
        let w = VarName::new("w");
        let z_topic = qualified_ros_name(test_add_monitor_ros_output_with_aux, "z");
        let routes = BTreeMap::from([(
            z.clone(),
            Route::new(
                z_topic.clone().into_boxed_str(),
                Some(FormatId::new("Int32")),
            )?,
        )]);
        let mut writer = OutputPipeline::from_destination(
            OutputDestination::new(
                "ros",
                OutputBackendConfig::ros(
                    ex.clone(),
                    qualified_ros_name(test_add_monitor_ros_output_with_aux, "pub"),
                ),
            )
            .with_route_catalog(routes),
        )?
        .build([z.clone()], [w.clone()], None)
        .await
        .map_err(anyhow::Error::from)?;

        let z_output_stream = recv_ros_int_stream(
            ex.clone(),
            qualified_ros_name(test_add_monitor_ros_output_with_aux, "z_int_receiver"),
            z_topic,
            1,
        )
        .unwrap()
        .take(2);
        let w_topic = qualified_ros_name(test_add_monitor_ros_output_with_aux, "w");
        let w_output_stream = recv_ros_int_stream(
            ex.clone(),
            qualified_ros_name(test_add_monitor_ros_output_with_aux, "w_int_receiver"),
            w_topic,
            1,
        )
        .unwrap()
        .take(2);

        writer
            .send(OutputBatch::update(z.clone(), Value::Int(1)))
            .await?;
        writer.send(OutputBatch::update(z, Value::Int(2))).await?;
        writer
            .send(OutputBatch::update(w.clone(), Value::Int(3)))
            .await?;
        writer.send(OutputBatch::update(w, Value::Int(4))).await?;
        writer.flush().await?;
        let z_expected_output = vec![1, 2];
        let z_actual_output = z_output_stream.collect::<Vec<_>>().await;
        assert_eq!(z_actual_output, z_expected_output);

        let w_expected_output: Vec<i32> = vec![];
        let w_actual_output = w_output_stream.collect::<Vec<_>>().await;
        assert_eq!(w_actual_output, w_expected_output);
        writer.close().await?;

        Ok(())
    }

    #[apply(async_test)]
    async fn test_add_monitor_ros_output_no_aux(
        ex: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let z = VarName::new("z");
        let z_topic = qualified_ros_name(test_add_monitor_ros_output_no_aux, "z");
        let routes = BTreeMap::from([(
            z.clone(),
            Route::new(
                z_topic.clone().into_boxed_str(),
                Some(FormatId::new("Int32")),
            )?,
        )]);
        let mut writer = OutputPipeline::from_destination(
            OutputDestination::new(
                "ros",
                OutputBackendConfig::ros(
                    ex.clone(),
                    qualified_ros_name(test_add_monitor_ros_output_no_aux, "pub"),
                ),
            )
            .with_route_catalog(routes),
        )?
        .build([z.clone()], std::iter::empty::<VarName>(), None)
        .await
        .map_err(anyhow::Error::from)?;

        let z_output_stream = recv_ros_int_stream(
            ex.clone(),
            qualified_ros_name(test_add_monitor_ros_output_no_aux, "z_int_receiver"),
            z_topic,
            1,
        )
        .unwrap()
        .take(2);

        writer
            .send(OutputBatch::update(z.clone(), Value::Int(1)))
            .await?;
        writer.send(OutputBatch::update(z, Value::Int(2))).await?;
        writer.flush().await?;

        let z_expected_output = vec![1, 2];
        let z_actual_output = z_output_stream.collect::<Vec<_>>().await;
        assert_eq!(z_actual_output, z_expected_output);
        writer.close().await?;

        Ok(())
    }

    struct RosTestPublisher<T: WrappedTypesupport + 'static> {
        publisher: r2r::Publisher<T>,
        topic: String,
        cancellation: CancellationToken,
        _spinner: smol::Task<()>,
    }

    impl<T: WrappedTypesupport + 'static> RosTestPublisher<T> {
        fn new(
            executor: Rc<LocalExecutor<'static>>,
            node_name: String,
            topic: String,
        ) -> anyhow::Result<Self> {
            let context = r2r::Context::create()
                .map_err(|error| anyhow::anyhow!("failed to create ROS context: {error:?}"))?;
            let mut node = r2r::Node::create(context, node_name.as_str(), "")
                .map_err(|error| anyhow::anyhow!("failed to create ROS node: {error:?}"))?;
            let publisher = node
                .create_publisher::<T>(&topic, r2r::QosProfile::default())
                .map_err(|error| {
                    anyhow::anyhow!("failed to create ROS publisher on `{topic}`: {error:?}")
                })?;

            let cancellation = CancellationToken::new();
            let cancellation_for_spinner = cancellation.clone();
            let spinner = executor.spawn(async move {
                let mut cancelled = cancellation_for_spinner.cancelled().fuse();
                loop {
                    futures::select_biased! {
                        _ = cancelled => break,
                        _ = smol::future::yield_now().fuse() => {
                            node.spin_once(Duration::from_millis(0));
                        }
                    }
                }
            });

            Ok(Self {
                publisher,
                topic,
                cancellation,
                _spinner: spinner,
            })
        }

        async fn wait_for_subscribers(&self, label: &str) -> anyhow::Result<()> {
            let wait_for_subscribers = self
                .publisher
                .wait_for_inter_process_subscribers()
                .map_err(|error| {
                    anyhow::anyhow!(
                        "failed to wait for ROS subscribers on `{}`: {error:?}",
                        self.topic
                    )
                })?;
            let result = with_timeout(wait_for_subscribers, 5, label).await?;
            result.map_err(|error| {
                anyhow::anyhow!(
                    "waiting for ROS subscribers on `{}` failed: {error:?}",
                    self.topic
                )
            })?;
            Ok(())
        }

        fn publish(&self, value: T) -> anyhow::Result<()> {
            self.publisher.publish(&value).map_err(|error| {
                anyhow::anyhow!(
                    "failed to publish ROS message on `{}`: {error:?}",
                    self.topic
                )
            })
        }
    }

    impl<T: WrappedTypesupport + 'static> Drop for RosTestPublisher<T> {
        fn drop(&mut self) {
            self.cancellation.cancel();
        }
    }

    struct RosIntSubscriber {
        stream: LocalStream<Int32>,
        cancellation: CancellationToken,
        _spinner: smol::Task<()>,
    }

    impl RosIntSubscriber {
        fn new(
            executor: Rc<LocalExecutor<'static>>,
            node_name: String,
            topic: String,
        ) -> anyhow::Result<Self> {
            let context = r2r::Context::create()
                .map_err(|error| anyhow::anyhow!("failed to create ROS context: {error:?}"))?;
            let mut node = r2r::Node::create(context, node_name.as_str(), "")
                .map_err(|error| anyhow::anyhow!("failed to create ROS node: {error:?}"))?;
            let stream = node
                .subscribe::<Int32>(&topic, r2r::QosProfile::default())
                .map_err(|error| {
                    anyhow::anyhow!("failed to subscribe to ROS topic `{topic}`: {error:?}")
                })?
                .boxed_local();

            let cancellation = CancellationToken::new();
            let cancellation_for_spinner = cancellation.clone();
            let spinner = executor.spawn(async move {
                let mut cancelled = cancellation_for_spinner.cancelled().fuse();
                loop {
                    futures::select_biased! {
                        _ = cancelled => break,
                        _ = smol::future::yield_now().fuse() => {
                            node.spin_once(Duration::from_millis(0));
                        }
                    }
                }
            });

            Ok(Self {
                stream,
                cancellation,
                _spinner: spinner,
            })
        }

        async fn next_with_timeout(&mut self, timeout: Duration) -> Option<i32> {
            let next = self.stream.next().fuse();
            let timer = futures::FutureExt::fuse(smol::Timer::after(timeout));
            futures::pin_mut!(next, timer);
            futures::select! {
                message = next => message.map(|message| message.data),
                _ = timer => None,
            }
        }
    }

    impl Drop for RosIntSubscriber {
        fn drop(&mut self) {
            self.cancellation.cancel();
        }
    }

    #[apply(async_test)]
    async fn test_reconfigurable_dataflow_ros_live_session_switches_topics(
        ex: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let input_a_topic = format!("/reconf_dataflow_input_a_{suffix}");
        let input_b_topic = format!("/reconf_dataflow_input_b_{suffix}");
        let control_topic = format!("/reconf_dataflow_control_{suffix}");
        let output_a_topic = format!("/reconf_dataflow_output_a_{suffix}");
        let output_b_topic = format!("/reconf_dataflow_output_b_{suffix}");
        let specification = "in x: Int\nout z: Int\nz = x";

        let mut output_a = RosIntSubscriber::new(
            ex.clone(),
            qualified_ros_name(
                test_reconfigurable_dataflow_ros_live_session_switches_topics,
                "output_a_receiver",
            ),
            output_a_topic.clone(),
        )?;
        let mut output_b = RosIntSubscriber::new(
            ex.clone(),
            qualified_ros_name(
                test_reconfigurable_dataflow_ros_live_session_switches_topics,
                "output_b_receiver",
            ),
            output_b_topic.clone(),
        )?;

        let input_source = InputSource::<Value>::ros(
            BTreeMap::from([(
                VarName::new("x"),
                Route::new(
                    input_a_topic.clone().into_boxed_str(),
                    Some(FormatId::new("Int32")),
                )?,
            )]),
            ex.clone(),
        )
        .with_reconfiguration_route(control_topic.clone().into_boxed_str())?;
        let output_pipeline = OutputPipeline::<Value>::from_destination(
            OutputDestination::new(
                "ros",
                OutputBackendConfig::ros(
                    ex.clone(),
                    qualified_ros_name(
                        test_reconfigurable_dataflow_ros_live_session_switches_topics,
                        "output",
                    ),
                ),
            )
            .with_route_catalog(BTreeMap::from([(
                VarName::new("z"),
                Route::new(
                    output_a_topic.clone().into_boxed_str(),
                    Some(FormatId::new("Int32")),
                )?,
            )])),
        )?;

        let input_a_publisher = RosTestPublisher::<Int32>::new(
            ex.clone(),
            qualified_ros_name(
                test_reconfigurable_dataflow_ros_live_session_switches_topics,
                "input_a_publisher",
            ),
            input_a_topic.clone(),
        )?;
        let control_publisher = RosTestPublisher::<RosString>::new(
            ex.clone(),
            qualified_ros_name(
                test_reconfigurable_dataflow_ros_live_session_switches_topics,
                "control_publisher",
            ),
            control_topic.clone(),
        )?;

        let spec = specification.parse::<DsrvSpecification>()?;
        let (ack_tx, mut ack_rx) = bounded::channel::<ReconfigurationAck>(1).into_split();
        let runtime = trustworthiness_checker::runtime::GeneralRuntimeBuilder::new()
            .executor(ex.clone())
            .model(spec)
            .input_pipeline(InputPipeline::new(input_source))?
            .output_pipeline(output_pipeline)
            .runtime(RuntimeSpec::ReconfDataflow(ExecutionPolicy::Synchronous))
            .semantics(Semantics::TypedUntimed)
            .reconf_topic(control_topic.clone())
            .acknowledgements(ack_tx)
            .build()
            .await?;
        let runtime_task = ex.spawn(runtime.run());

        input_a_publisher
            .wait_for_subscribers("ROS data input topic A subscription")
            .await?;
        input_a_publisher.publish(Int32 { data: 1 })?;
        let initial_output = output_a
            .next_with_timeout(Duration::from_secs(5))
            .await
            .ok_or_else(|| anyhow::anyhow!("ROS output on OUT_A did not arrive"))?;
        assert_eq!(initial_output, 1);

        control_publisher
            .wait_for_subscribers("ROS String control topic subscription")
            .await?;
        let request = serde_json::json!({
            "specification": specification,
            "input": {
                "source": "default",
                "inputs": {"x": [input_b_topic.clone(), "Int32"]},
            },
            "output": {
                "outputs": {"z": [output_b_topic.clone(), "Int32"]},
            },
        })
        .to_string();
        control_publisher.publish(RosString { data: request })?;
        let acknowledgement = with_timeout(
            ack_rx.recv(),
            5,
            "ROS dataflow reconfiguration acknowledgement",
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("ROS dataflow acknowledgement channel closed"))?;
        assert!(!acknowledgement.monitor_changed);
        assert!(acknowledgement.interface_changed);

        let input_b_publisher = RosTestPublisher::<Int32>::new(
            ex.clone(),
            qualified_ros_name(
                test_reconfigurable_dataflow_ros_live_session_switches_topics,
                "input_b_publisher",
            ),
            input_b_topic,
        )?;
        input_b_publisher
            .wait_for_subscribers("ROS data input topic B subscription")
            .await?;

        input_a_publisher.publish(Int32 { data: 100 })?;
        let (old_output_a, old_output_b) = future::join(
            output_a.next_with_timeout(Duration::from_secs(2)),
            output_b.next_with_timeout(Duration::from_secs(2)),
        )
        .await;
        assert!(
            old_output_a.is_none(),
            "the old input topic must not produce output on OUT_A after the acknowledgement barrier"
        );
        assert!(
            old_output_b.is_none(),
            "the old input topic must not drive the new OUT_B route after the acknowledgement barrier"
        );

        input_b_publisher.publish(Int32 { data: 3 })?;
        let (post_barrier_output_a, post_barrier_output_b) = future::join(
            output_a.next_with_timeout(Duration::from_secs(2)),
            output_b.next_with_timeout(Duration::from_secs(5)),
        )
        .await;
        assert!(
            post_barrier_output_a.is_none(),
            "post-barrier output must not appear on OUT_A"
        );
        assert_eq!(post_barrier_output_b, Some(3));

        let runtime_result =
            with_timeout(runtime_task.cancel(), 5, "ROS dataflow runtime shutdown").await?;
        if let Some(runtime_result) = runtime_result {
            runtime_result?;
        }
        Ok(())
    }
}
