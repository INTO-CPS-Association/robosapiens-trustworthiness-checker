#[cfg(test)]
#[cfg(feature = "ros")]
mod integration_tests {
    use futures::StreamExt;
    use futures::stream;
    use macro_rules_attribute::apply;
    use r2r::std_msgs::msg::Int32;
    use smol::LocalExecutor;
    use std::rc::Rc;
    use tc_testutils::ros::generate_xy_test_publisher_tasks_with_topics;
    use tc_testutils::ros::qualified_ros_name;
    use tc_testutils::ros::recv_ros_int_stream;
    use tc_testutils::streams::interleave_with_constant;
    use tc_testutils::streams::receive_values_serially;
    use tracing::{error, info};
    use trustworthiness_checker::InputProvider;
    use trustworthiness_checker::OutputStream;
    use trustworthiness_checker::Value;
    use trustworthiness_checker::async_test;
    use trustworthiness_checker::core::OutputHandler;
    use trustworthiness_checker::io::ros::ROSInputProvider;
    use trustworthiness_checker::io::ros::ROSOutputHandler;
    use trustworthiness_checker::io::ros::json_to_mapping;

    #[apply(async_test)]
    async fn test_add_monitor_ros_input(ex: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
        let xs_ros = vec![Int32 { data: 1 }, Int32 { data: 2 }];
        let ys_ros = vec![Int32 { data: 3 }, Int32 { data: 4 }];
        let xs = vec![Value::Int(1), Value::Int(2)];
        let ys = vec![Value::Int(3), Value::Int(4)];

        let x_topic = qualified_ros_name(test_add_monitor_ros_input, "x");
        let y_topic = qualified_ros_name(test_add_monitor_ros_input, "y");

        let var_topics = json_to_mapping(&format!(
            r#"
        {{
            "x": {{
                "topic": "{}",
                "msg_type": "Int32"
            }},
            "y": {{
                "topic": "{}",
                "msg_type": "Int32"
            }}
        }}
        "#,
            x_topic, y_topic
        ))
        .unwrap();

        // Create the ROS input provider
        let mut input_provider = ROSInputProvider::new(ex.clone(), var_topics).unwrap();

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
        ex.spawn(input_provider_future).detach();

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_xy_test_publisher_tasks_with_topics(
                ex.clone(),
                test_add_monitor_ros_input,
                &x_topic,
                &y_topic,
                xs_ros,
                ys_ros,
            );

        let (x_vals, y_vals) =
            receive_values_serially(&mut x_tick, &mut y_tick, x_stream, y_stream, xs.len()).await?;

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
    async fn test_add_monitor_ros_output_with_aux(
        ex: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        // let xs_ros = vec![Int32 { data: 1 }, Int32 { data: 2 }];
        // let ys_ros = vec![Int32 { data: 3 }, Int32 { data: 4 }];
        let zs: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let ws: OutputStream<Value> = Box::pin(stream::iter(vec![3.into(), 4.into()]));

        let z_topic = qualified_ros_name(test_add_monitor_ros_output_with_aux, "z");

        let var_topics = json_to_mapping(&format!(
            r#"
        {{
            "z": {{
                "topic": "{}",
                "msg_type": "Int32"
            }}
        }}
        "#,
            z_topic
        ))
        .unwrap();

        let aux_info = vec!["w".into()];

        let var_names = vec!["w".into(), "z".into()];

        // Create the ROS output handler
        let mut output_handler = ROSOutputHandler::new(
            ex.clone(),
            qualified_ros_name(test_add_monitor_ros_output_with_aux, "pub"),
            var_names,
            var_topics,
            aux_info,
        )
        .unwrap();

        output_handler.provide_streams(vec![ws, zs]);

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

        ex.spawn(output_handler.run()).detach();

        let z_expected_output = vec![1, 2];
        let z_actual_output = z_output_stream.collect::<Vec<_>>().await;
        assert_eq!(z_actual_output, z_expected_output);

        let w_expected_output: Vec<i32> = vec![];
        let w_actual_output = w_output_stream.collect::<Vec<_>>().await;
        assert_eq!(w_actual_output, w_expected_output);

        Ok(())
    }

    #[apply(async_test)]
    async fn test_add_monitor_ros_output_no_aux(
        ex: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        let zs: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into()]));

        let z_topic = qualified_ros_name(test_add_monitor_ros_output_no_aux, "z");

        let var_topics = json_to_mapping(&format!(
            r#"
            {{
                "z": {{
                    "topic": "{}",
                    "msg_type": "Int32"
                }}
            }}
            "#,
            z_topic
        ))
        .unwrap();

        let aux_info = vec![];

        let var_names = vec!["z".into()];

        // Create the ROS output handler
        let mut output_handler = ROSOutputHandler::new(
            ex.clone(),
            qualified_ros_name(test_add_monitor_ros_output_no_aux, "pub"),
            var_names,
            var_topics,
            aux_info,
        )
        .unwrap();

        output_handler.provide_streams(vec![zs]);

        let z_output_stream = recv_ros_int_stream(
            ex.clone(),
            qualified_ros_name(test_add_monitor_ros_output_no_aux, "z_int_receiver"),
            z_topic,
            1,
        )
        .unwrap()
        .take(2);

        ex.spawn(output_handler.run()).detach();

        let z_expected_output = vec![1, 2];
        let z_actual_output = z_output_stream.collect::<Vec<_>>().await;
        assert_eq!(z_actual_output, z_expected_output);

        Ok(())
    }
}
