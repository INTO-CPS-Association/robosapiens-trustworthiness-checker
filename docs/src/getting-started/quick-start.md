# Quick Start

This guide will walk you through your first runtime monitoring session using the RoboSAPIENS Trustworthiness Checker.

## Your First Monitor

Let's start with a simple example that adds two numbers and monitors the result using MQTT.

### Step 1: Start an MQTT Broker

First, start a local MQTT broker in a terminal:

```bash
mosquitto
```

Leave this running in the background.

### Step 2: Run the Monitor

In a new terminal, navigate to your project directory and run:

```bash
cargo run --release -- examples/simple_add.lola --mqtt-input --mqtt-output
```

You should see output indicating the monitor is running and listening for MQTT messages.

### Step 3: Send Input Values

Open MQTT Explorer or use `mosquitto_pub` to send messages to the input topics.

Using `mosquitto_pub`:

```bash
# Send value to topic 'x'
mosquitto_pub -t x -m '42'

# Send value to topic 'y'
mosquitto_pub -t y -m '42'
```

### Step 4: Observe the Output

Subscribe to the output topic to see the result:

```bash
mosquitto_sub -t z
```

You should receive:

```json
{
    "value": 84
}
```

**Congratulations!** You've just run your first runtime monitor! 🎉

## Understanding What Happened

The `simple_add.lola` specification defines a monitor that:

1. **Listens** to two input streams: `x` and `y` via MQTT topics
2. **Computes** the sum: `z = x + y`
3. **Outputs** the result to the `z` topic

The monitor continuously evaluates this specification as new values arrive.

## Working with Special Values

### Deferred Values

Sometimes a value might not be immediately available. You can send a deferred value:

```bash
mosquitto_pub -t x -m '{"Deferred": null}'
```

The monitor will wait for the actual value before computing dependent streams.

### Complex Values

For more complex data types, use JSON format:

```bash
mosquitto_pub -t x -m '{"value": 100, "timestamp": 1234567890}'
```

## Next Steps

Now that you've run a basic example, you can:

- **Learn MQTT Integration**: Check out the [MQTT Integration Tutorial](../tutorials/mqtt-integration.md) for more advanced patterns
- **Try ROS2**: Follow the [ROS2 Integration Tutorial](../tutorials/ros2-integration.md) to connect with robotic systems
- **Explore Distributed Monitoring**: Learn how to run monitors across multiple nodes in the [Distributed Monitoring Tutorial](../tutorials/distributed-monitoring.md)
- **Write Your Own Specifications**: Dive into [Lola Specifications](../advanced/lola-specifications.md) to create custom monitors

## Quick Reference

### Common Commands

```bash
# Run with MQTT
cargo run -- <spec.lola> --mqtt-input --mqtt-output

# Run with ROS2 (requires 'ros' feature)
cargo run --features ros -- --input-ros-topics <config.json> <spec.lola>

# Run distributed monitor
cargo run -- <spec.lola> --mqtt-input --mqtt-output \
  --distribution-graph <graph.json> --local-node <NODE_NAME>

# Show help
cargo run -- --help
```

### MQTT Testing Tools

```bash
# Publish a message
mosquitto_pub -t <topic> -m '<message>'

# Subscribe to a topic
mosquitto_sub -t <topic>

# Subscribe to all topics (useful for debugging)
mosquitto_sub -t '#'
```

## Troubleshooting

**Monitor doesn't start:**
- Check that the `.lola` file exists and is valid
- Ensure the MQTT broker is running

**No output received:**
- Verify you've sent messages to all required input topics
- Check the topic names match your specification
- Use `mosquitto_sub -t '#'` to see all MQTT traffic

**Connection refused:**
- Ensure the MQTT broker is running on localhost:1883
- Check firewall settings

For more detailed troubleshooting, see the [Installation Guide](./installation.md#troubleshooting).
