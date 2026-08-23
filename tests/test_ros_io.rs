#[cfg(test)]
#[cfg(feature = "ros")]
mod integration_tests {
    use std::collections::BTreeMap;

    use futures::StreamExt;

    use macro_rules_attribute::apply;
    use r2r::std_msgs::msg::Int32;
    use smol::LocalExecutor;
    use std::rc::Rc;
    use tc_testutils::ros::generate_xy_test_publisher_tasks_with_topics;
    use tc_testutils::ros::qualified_ros_name;
    use tc_testutils::ros::recv_ros_int_stream;
    use tc_testutils::streams::expect_events_serially;
    use tracing::info;
    use trustworthiness_checker::OutputBatch;
    use trustworthiness_checker::Value;
    use trustworthiness_checker::VarName;
    use trustworthiness_checker::async_test;
    use trustworthiness_checker::io::ros;
    use trustworthiness_checker::io::ros::ros_topic_stream_mapping::{
        RosMsgType, VariableMappingData,
    };
    use trustworthiness_checker::io::{
        CodecId, OutputBackendBuilder, OutputBackendConfig, OutputDestination, Route,
    };

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

        let mut input_stream = ros::input_stream(ex.clone(), var_topics)?;

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
                Some(CodecId::new("Int32")),
            )?,
        )]);
        let mut writer = OutputBackendBuilder::from_destination(
            OutputDestination::new(
                "ros",
                OutputBackendConfig::ros(
                    ex.clone(),
                    qualified_ros_name(test_add_monitor_ros_output_with_aux, "pub"),
                ),
            )
            .with_route_catalog(routes),
        )
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
                Some(CodecId::new("Int32")),
            )?,
        )]);
        let mut writer = OutputBackendBuilder::from_destination(
            OutputDestination::new(
                "ros",
                OutputBackendConfig::ros(
                    ex.clone(),
                    qualified_ros_name(test_add_monitor_ros_output_no_aux, "pub"),
                ),
            )
            .with_route_catalog(routes),
        )
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
}
