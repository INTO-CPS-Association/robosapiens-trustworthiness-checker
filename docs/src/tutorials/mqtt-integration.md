# MQTT Integration

This tutorial covers how to integrate the RoboSAPIENS Trustworthiness Checker with MQTT for real-time monitoring of distributed systems.

## Overview

MQTT (Message Queuing Telemetry Transport) is a lightweight messaging protocol ideal for IoT and distributed systems. The trustworthiness checker can use MQTT topics as input and output streams, enabling seamless integration with existing MQTT-based systems.

## Prerequisites

- Completed [Installation](../getting-started/installation.md) and [Quick Start](../getting-started/quick-start.md)
- Running MQTT broker (e.g., Mosquitto)
- Basic understanding of MQTT concepts (topics, publish/subscribe)

## Basic MQTT Integration

### Automatic Topic Mapping

The simplest way to use MQTT is with automatic topic mapping. The monitor automatically maps Lola input/output streams to MQTT topics with matching names.

**Example Specification** (`simple_add.lola`):

```lola
input x: Int
input y: Int
output z := x + y
```

**Run the monitor:**

```bash
cargo run --release -- examples/simple_add.lola --mqtt-input --mqtt-output
```

With this configuration:
- Input stream `x` → MQTT topic `x`
- Input stream `y` → MQTT topic `y`
- Output stream `z` → MQTT topic `z`

**Send values:**

```bash
mosquitto_pub -t x -m '42'
mosquitto_pub -t y -m '42'
```

**Observe output:**

```bash
mosquitto_sub -t z
# Output: {"value": 84}
```

## Legacy Syntax: Explicit Topic Mapping

For more control over topic names, use the legacy syntax with explicit topic mapping.

**Run with explicit topics:**

```bash
cargo run --release -- examples/simple_add.lola \
  --input-mqtt-topics x y \
  --output-mqtt-topics z
```

This syntax is useful when:
- You need to verify topic names explicitly
- Working with existing systems where topic names must match specific conventions
- Debugging topic mapping issues

### Auxiliary Streams Note

⚠️ **Important**: If your specification has auxiliary streams (internal streams not marked as input/output), they must currently also be specified in `--output-mqtt-topics`, even though they won't actually be published. This is a current limitation.

**Example with auxiliary stream:**

```lola
input x: Int
input y: Int
output sum := temp
temp := x + y
```

```bash
cargo run -- examples/spec.lola \
  --input-mqtt-topics x y \
  --output-mqtt-topics sum temp  # temp must be listed but won't be published
```

## Message Formats

### Simple Values

For basic data types, send values directly:

```bash
# Integer
mosquitto_pub -t x -m '42'

# Float
mosquitto_pub -t temperature -m '23.5'

# Boolean
mosquitto_pub -t active -m 'true'

# String
mosquitto_pub -t status -m '"running"'
```

### Deferred Values

When a value is not yet available, send a deferred marker:

```bash
mosquitto_pub -t x -m '{"Deferred": null}'
```

The monitor will:
1. Recognize the value as deferred
2. Wait for the actual value before computing dependent streams
3. Continue processing other independent streams

**Use case example:**

```lola
input sensor1: Int
input sensor2: Int
output avg := (sensor1 + sensor2) / 2
output sensor1_only := sensor1 * 2
```

If `sensor2` is deferred:
- `avg` computation will wait
- `sensor1_only` will still be computed and published

### Structured Data

For complex types, use JSON:

```bash
mosquitto_pub -t robot_pose -m '{
  "x": 1.5,
  "y": 2.3,
  "theta": 0.785
}'
```

## Advanced Patterns

### 1. Monitoring Rate Limits

Monitor that a system doesn't exceed a rate limit:

```lola
input events: Int
output events_per_second := events
output violation := events_per_second > 100

trigger violation "Rate limit exceeded!"
```

### 2. Temporal Properties

Check properties over time windows:

```lola
input temperature: Float
output avg_temp := (temperature + temperature[-1] + temperature[-2]) / 3
output overheating := avg_temp > 80.0

trigger overheating "System overheating!"
```

The `[-1]` notation accesses the previous value in the stream.

### 3. Multi-Sensor Fusion

Combine data from multiple sensors:

```lola
input sensor_a: Float
input sensor_b: Float
input sensor_c: Float

output fused := (sensor_a + sensor_b + sensor_c) / 3
output variance := (
  (sensor_a - fused)^2 + 
  (sensor_b - fused)^2 + 
  (sensor_c - fused)^2
) / 3

output sensor_failure := variance > 10.0
trigger sensor_failure "Sensor readings diverging!"
```

### 4. Stateful Monitoring

Track state across multiple events:

```lola
input command: String
output state := if command == "start" then "running"
                else if command == "stop" then "stopped"
                else state[-1]

output illegal_transition := 
  state[-1] == "stopped" && state == "running" && command != "start"

trigger illegal_transition "Illegal state transition detected!"
```

## Best Practices

### 1. Topic Naming Conventions

Use clear, hierarchical topic names:

```
✓ robot/sensors/temperature
✓ robot/actuators/motor1/speed
✓ system/health/cpu_usage

✗ temp
✗ m1
✗ data123
```

### 2. Quality of Service (QoS)

Consider MQTT QoS levels based on your requirements:

- **QoS 0** (At most once): Fast, but messages may be lost
- **QoS 1** (At least once): Guaranteed delivery, possible duplicates
- **QoS 2** (Exactly once): Slowest, but guaranteed exactly-once delivery

Currently, the trustworthiness checker uses the default QoS level. For critical monitoring, ensure your MQTT broker is configured appropriately.

### 3. Handle Missing Values Gracefully

Design specifications that handle deferred or missing values:

```lola
input sensor: Int
output safe_sensor := if sensor != null then sensor else 0
output status := if sensor == null then "disconnected" else "active"
```

### 4. Use Retained Messages for State

For state topics, use MQTT retained messages so new monitors receive the current state:

```bash
mosquitto_pub -t system/state -m '"running"' -r
```

### 5. Monitor the Monitors

Set up health checks for your monitors:

```lola
input heartbeat: Int
output heartbeat_received := heartbeat != null
output heartbeat_age := time_since_last(heartbeat)

trigger (heartbeat_age > 5) "Monitor heartbeat timeout!"
```

## Debugging Tips

### View All MQTT Traffic

Subscribe to all topics to see what's happening:

```bash
mosquitto_sub -v -t '#'
```

The `-v` flag shows topic names with each message.

### Test Topic Connectivity

Before running complex monitors, test that topics work:

```bash
# Terminal 1: Subscribe
mosquitto_sub -t test

# Terminal 2: Publish
mosquitto_pub -t test -m 'hello'
```

### Enable Verbose Logging

Run the monitor with verbose output:

```bash
RUST_LOG=debug cargo run -- examples/simple_add.lola --mqtt-input --mqtt-output
```

### Check Topic Names

Ensure topic names in your specification exactly match MQTT topics (case-sensitive):

```lola
input Temperature: Int  # Topic: "Temperature"
input temperature: Int  # Topic: "temperature" (different!)
```

## Common Issues

### Messages Not Received

**Problem**: Monitor running but not receiving input

**Solutions**:
- Verify MQTT broker is running: `ps aux | grep mosquitto`
- Check topic names match exactly (case-sensitive)
- Ensure messages are valid JSON format
- Try `mosquitto_sub -t '<your-topic>'` to verify messages are published

### Output Not Published

**Problem**: Monitor computes but doesn't publish to MQTT

**Solutions**:
- Verify `--mqtt-output` flag is set
- Check output streams are marked as `output` in specification
- Ensure dependent input streams have values (not deferred)
- For legacy syntax, verify output topics are listed in `--output-mqtt-topics`

### Broker Connection Failed

**Problem**: "Connection refused" or similar errors

**Solutions**:
- Check broker is listening on correct port (default: 1883)
- Verify no firewall blocking port
- Try connecting with mosquitto client: `mosquitto_pub -t test -m test`
- Check broker logs for errors

## Example: Real-World Scenario

Let's build a complete example monitoring a robotic arm:

**Specification** (`robot_arm.lola`):

```lola
input joint1_angle: Float
input joint2_angle: Float
input joint3_angle: Float
input gripper_force: Float
input target_position: Float

output joint1_in_range := joint1_angle >= -180 && joint1_angle <= 180
output joint2_in_range := joint2_angle >= -90 && joint2_angle <= 90
output joint3_in_range := joint3_angle >= -90 && joint3_angle <= 90

output all_joints_valid := joint1_in_range && joint2_in_range && joint3_in_range

output gripper_overload := gripper_force > 100.0
output position_error := abs(joint1_angle - target_position)
output position_reached := position_error < 1.0

trigger !all_joints_valid "Joint angle out of range!"
trigger gripper_overload "Gripper force too high!"
```

**Run the monitor:**

```bash
cargo run --release -- examples/robot_arm.lola --mqtt-input --mqtt-output
```

**Simulate robot operation:**

```bash
# Normal operation
mosquitto_pub -t joint1_angle -m '45.0'
mosquitto_pub -t joint2_angle -m '30.0'
mosquitto_pub -t joint3_angle -m '-20.0'
mosquitto_pub -t gripper_force -m '50.0'
mosquitto_pub -t target_position -m '45.0'

# Monitor outputs
mosquitto_sub -t all_joints_valid
mosquitto_sub -t position_reached
```

This creates a safety monitor that continuously verifies the robot operates within safe parameters!

## Next Steps

- Explore [ROS2 Integration](./ros2-integration.md) for robotic systems
- Learn about [Distributed Monitoring](./distributed-monitoring.md) across multiple nodes
- Study [Lola Specifications](../advanced/lola-specifications.md) in depth
