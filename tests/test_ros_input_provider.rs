#[cfg(test)]
#[cfg(feature = "ros")]
mod integration_tests {
    use futures::StreamExt;
    use futures::stream;
    use macro_rules_attribute::apply;
    use r2r::{WrappedTypesupport, std_msgs::msg::Int32};
    use smol::LocalExecutor;
    use std::rc::Rc;
    use tc_testutils::streams::TickSender;
    use tc_testutils::streams::interleave_with_constant;
    use tc_testutils::streams::receive_values_serially;
    use tc_testutils::streams::tick_stream;
    use tc_testutils::streams::with_timeout_res;
    use tracing::info;
    use trustworthiness_checker::InputProvider;
    use trustworthiness_checker::OutputStream;
    use trustworthiness_checker::Value;
    use trustworthiness_checker::async_test;
    use trustworthiness_checker::io::ros::ROSInputProvider;
    use trustworthiness_checker::io::ros::json_to_mapping;

    /* A simple ROS publisher node which publishes a sequence of values on a topic
     * This creates a ROS node node_name which runs in a background thread
     * until all the values have been published. */
    async fn dummy_stream_publisher<T: WrappedTypesupport + 'static>(
        ex: Rc<LocalExecutor<'static>>,
        node_name: String,
        topic: &str,
        mut values: OutputStream<T>,
        values_len: usize,
    ) -> Result<(), anyhow::Error> {
        info!(
            "Starting publisher {} for topic {} with {} values",
            node_name, topic, values_len
        );
        let ctx = r2r::Context::create().unwrap();
        let mut node = r2r::Node::create(ctx, node_name.as_str(), "").unwrap();
        let publisher = node
            .create_publisher::<T>(&topic, r2r::QosProfile::default())
            .unwrap();

        // Spawn a background async task to run the ROS node
        ex.spawn(async move {
            loop {
                smol::future::yield_now().await;
                node.spin_once(std::time::Duration::from_millis(0));
            }
        })
        .detach();

        let mut index = 0;
        while let Some(value) = values.next().await {
            info!(
                "Publishing message number {} with value: {:?} on topic: {}",
                index, value, topic
            );
            publisher.publish(&value).unwrap();
            index += 1;
        }

        info!(
            "Finished publishing all {} messages for topic {}",
            values_len, topic
        );

        // Ensure we wait a moment before disconnecting to allow for message delivery
        smol::Timer::after(std::time::Duration::from_millis(100)).await;

        Ok(())
    }

    fn generate_test_publisher_tasks(
        executor: Rc<LocalExecutor<'static>>,
        xs: Vec<Int32>,
        ys: Vec<Int32>,
    ) -> (
        (TickSender, smol::Task<anyhow::Result<()>>),
        (TickSender, smol::Task<anyhow::Result<()>>),
    ) {
        let (x_tick, x_pub_stream) = tick_stream(stream::iter(xs.clone()).boxed());
        let (y_tick, y_pub_stream) = tick_stream(stream::iter(ys.clone()).boxed());

        // Spawn dummy MQTT publisher nodes and keep handles to wait for completion
        let x_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_publisher(
                executor.clone(),
                "x_publisher".to_string(),
                "/x",
                x_pub_stream,
                xs.len(),
            ),
            5,
            "x_publisher_task",
        ));

        let y_publisher_task = executor.spawn(with_timeout_res(
            dummy_stream_publisher(
                executor.clone(),
                "y_publisher".to_string(),
                "/y",
                y_pub_stream,
                ys.len(),
            ),
            5,
            "y_publisher_task",
        ));

        ((x_tick, x_publisher_task), (y_tick, y_publisher_task))
    }

    #[apply(async_test)]
    async fn test_add_monitor_ros(ex: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
        let xs_ros = vec![Int32 { data: 1 }, Int32 { data: 2 }];
        let ys_ros = vec![Int32 { data: 3 }, Int32 { data: 4 }];
        let xs = vec![Value::Int(1), Value::Int(2)];
        let ys = vec![Value::Int(3), Value::Int(4)];

        let var_topics = json_to_mapping(
            r#"
        {
            "x": {
                "topic": "/x",
                "msg_type": "Int32"
            },
            "y": {
                "topic": "/y",
                "msg_type": "Int32"
            }
        }
        "#,
        )
        .unwrap();

        // Create the ROS input provider
        let mut input_provider = ROSInputProvider::new(ex.clone(), var_topics).unwrap();

        let x_stream = input_provider
            .var_stream(&"x".into())
            .ok_or_else(|| anyhow::anyhow!("x stream unavailable"))?;
        let y_stream = input_provider
            .var_stream(&"y".into())
            .ok_or_else(|| anyhow::anyhow!("y stream unavailable"))?;

        ex.spawn(input_provider.run()).detach();

        let ((mut x_tick, x_publisher_task), (mut y_tick, y_publisher_task)) =
            generate_test_publisher_tasks(ex.clone(), xs_ros, ys_ros);

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
}
