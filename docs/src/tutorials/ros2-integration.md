# ROS2 Integration

This tutorial demonstrates how to integrate the RoboSAPIENS Trustworthiness Checker with ROS2 for monitoring robotic systems.

## Overview

ROS2 (Robot Operating System 2) is the de facto standard for building robotic applications. The trustworthiness checker provides native ROS2 integration, allowing you to monitor ROS2 topics in real-time using Lola specifications.

## Prerequisites

- Completed [Installation](../getting-started/installation.md) with ROS2 support
- ROS2 Humble Hawksbill or later installed
- Basic understanding of ROS2 concepts (nodes, topics, messages)
- The project built with the `ros` feature flag

## Setup

### 1. Install ROS2

If you haven't already installed ROS2, follow the [official installation guide](https://docs.ros.org/en/humble/Installation.html).

For Ubuntu 22.04:

```bash
sudo apt update && sudo apt install -y \
  ros-humble-desktop \
  ros-dev-tools
```

### 2. Source ROS2 Environment

Before working with ROS2, source the setup file:

```bash
source /opt/ros/humble/setup.bash
```

Add this to your `~/.bashrc` to make it permanent:

```bash
echo "source /opt/ros/humble/setup.bash" >> ~/.bashrc
```

### 3. Build Custom Message Types

The trustworthiness checker includes custom ROS2 message types that need to be compiled:

```bash
# From the project root
colcon build

# Source the local workspace
source install/setup.bash
```

This creates the necessary message type definitions for communication between the monitor and ROS2 nodes.

### 4. Build with ROS2 Feature

Ensure the project is built with ROS2 support:

```bash
cargo build --release --features ros
```

## Basic ROS2 Monitoring

### Example: Counter Monitor

Let's start with a simple counter example that monitors a ROS2 topic.

**Step 1: Create the topic mapping configuration** (`counter_ros_map.json`):

```json
{
  "x": {
    "topic": "/x",
    "message_type": "std_msgs/msg/Int32",
    "field_path": "data"
  }
}
```

This maps:
- Lola stream `x` → ROS2 topic `/x`
- Uses standard `Int32` message type
- Extracts the `data` field from the message

**Step 2: Create the Lola specification** (`counter.lola`):

```lola
input x: Int
output count := if x[-1] == null then x else count[-1] + x
```

This specification:
- Takes integer input from stream `x`
- Maintains a running count by adding each new value
- Initializes count with the first value received

**Step 3: Start the monitor:**

```bash
cargo run --features ros -- \
  --input-ros-topics examples/counter_ros_map.json \
  examples/counter.lola
```

**Step 4: Publish values from ROS2:**

In another terminal (with ROS2 sourced):

```bash
source /opt/ros/humble/setup.bash

# Publish value 1
ros2 topic pub /x std_msgs/msg/Int32 "{data: 1}"
```

The monitor will now count each value published to `/x`!

## Topic Mapping Configuration

The ROS2 topic mapping configuration is a JSON file that connects Lola streams to ROS2 topics.

### Basic Structure

```json
{
  "stream_name": {
    "topic": "/ros2/topic/name",
    "message_type": "package_name/msg/MessageType",
    "field_path": "field.subfield"
  }
}
```

### Configuration Fields

- **`topic`**: The ROS2 topic name (must start with `/`)
- **`message_type`**: The ROS2 message type in format `package/msg/Type`
- **`field_path`**: Dot-separated path to extract value from message

### Common Message Types

#### Standard Messages (`std_msgs`)

```json
{
  "int_value": {
    "topic": "/my_int",
    "message_type": "std_msgs/msg/Int32",
    "field_path": "data"
  },
  "float_value": {
    "topic": "/my_float",
    "message_type": "std_msgs/msg/Float64",
    "field_path": "data"
  },
  "string_value": {
    "topic": "/my_string",
    "message_type": "std_msgs/msg/String",
    "field_path": "data"
  },
  "bool_value": {
    "topic": "/my_bool",
    "message_type": "std_msgs/msg/Bool",
    "field_path": "data"
  }
}
```

#### Geometry Messages (`geometry_msgs`)

```json
{
  "position_x": {
    "topic": "/robot/pose",
    "message_type": "geometry_msgs/msg/Pose",
    "field_path": "position.x"
  },
  "position_y": {
    "topic": "/robot/pose",
    "message_type": "geometry_msgs/msg/Pose",
    "field_path": "position.y"
  },
  "orientation_z": {
    "topic": "/robot/pose",
    "message_type": "geometry_msgs/msg/Pose",
    "field_path": "orientation.z"
  }
}
```

#### Sensor Messages (`sensor_msgs`)

```json
{
  "temperature": {
    "topic": "/sensors/temp",
    "message_type": "sensor_msgs/msg/Temperature",
    "field_path": "temperature"
  },
  "camera_height": {
    "topic": "/camera/info",
    "message_type": "sensor_msgs/msg/CameraInfo",
    "field_path": "height"
  }
}
```

### Multiple Fields from Same Topic

You can extract multiple fields from the same topic:

```json
{
  "pos_x": {
    "topic": "/robot/odom",
    "message_type": "nav_msgs/msg/Odometry",
    "field_path": "pose.pose.position.x"
  },
  "pos_y": {
    "topic": "/robot/odom",
    "message_type": "nav_msgs/msg/Odometry",
    "field_path": "pose.pose.position.y"
  },
  "velocity": {
    "topic": "/robot/odom",
    "message_type": "nav_msgs/msg/Odometry",
    "field_path": "twist.twist.linear.x"
  }
}
```

## Complete Example: Mobile Robot Monitor

Let's build a comprehensive monitor for a mobile robot.

### Specification (`mobile_robot.lola`)

```lola
input pos_x: Float
input pos_y: Float
input velocity: Float
input battery_level: Float
input obstacle_distance: Float

# Position tracking
output distance_traveled := sqrt(
  (pos_x - pos_x[-1])^2 + 
  (pos_y - pos_y[-1])^2
)

# Safety checks
output safe_velocity := velocity <= 2.0
output safe_battery := battery_level >= 20.0
output safe_obstacle := obstacle_distance > 0.5

output all_safe := safe_velocity && safe_battery && safe_obstacle

# Battery prediction
output battery_drain_rate := battery_level[-1] - battery_level
output estimated_time_remaining := battery_level / (battery_drain_rate + 0.001)

# Triggers
trigger !safe_velocity "Robot moving too fast!"
trigger !safe_battery "Battery critically low!"
trigger !safe_obstacle "Obstacle too close!"
trigger estimated_time_remaining < 5.0 "Battery will die soon!"
```

### Topic Mapping (`mobile_robot_map.json`)

```json
{
  "pos_x": {
    "topic": "/robot/odom",
    "message_type": "nav_msgs/msg/Odometry",
    "field_path": "pose.pose.position.x"
  },
  "pos_y": {
    "topic": "/robot/odom",
    "message_type": "nav_msgs/msg/Odometry",
    "field_path": "pose.pose.position.y"
  },
  "velocity": {
    "topic": "/robot/odom",
    "message_type": "nav_msgs/msg/Odometry",
    "field_path": "twist.twist.linear.x"
  },
  "battery_level": {
    "topic": "/robot/battery",
    "message_type": "sensor_msgs/msg/BatteryState",
    "field_path": "percentage"
  },
  "obstacle_distance": {
    "topic": "/robot/proximity",
    "message_type": "sensor_msgs/msg/Range",
    "field_path": "range"
  }
}
```

### Running the Monitor

```bash
cargo run --features ros -- \
  --input-ros-topics mobile_robot_map.json \
  mobile_robot.lola
```

## Advanced Topics

### Custom Message Types

If you need custom message types:

1. **Define the message** in `ros_interfaces/msg/`:

```msg
# CustomSensor.msg
float64 value
string sensor_id
uint32 timestamp
```

2. **Update `CMakeLists.txt`** to include your message

3. **Rebuild with colcon:**

```bash
colcon build
source install/setup.bash
```

4. **Use in topic mapping:**

```json
{
  "sensor_value": {
    "topic": "/custom/sensor",
    "message_type": "ros_interfaces/msg/CustomSensor",
    "field_path": "value"
  }
}
```

### Working with Arrays

For array fields, you can access specific indices:

```json
{
  "first_joint": {
    "topic": "/robot/joint_states",
    "message_type": "sensor_msgs/msg/JointState",
    "field_path": "position.0"
  },
  "second_joint": {
    "topic": "/robot/joint_states",
    "message_type": "sensor_msgs/msg/JointState",
    "field_path": "position.1"
  }
}
```

### Header Timestamps

Extract timestamps from message headers:

```json
{
  "message_time": {
    "topic": "/sensor/data",
    "message_type": "sensor_msgs/msg/Image",
    "field_path": "header.stamp.sec"
  }
}
```

## Testing with ROS2 Tools

### Inspect Available Topics

```bash
# List all topics
ros2 topic list

# Show topic info
ros2 topic info /robot/odom

# Show message type
ros2 topic type /robot/odom
```

### Publish Test Messages

```bash
# Simple integer
ros2 topic pub /x std_msgs/msg/Int32 "{data: 42}"

# Pose message
ros2 topic pub /robot/pose geometry_msgs/msg/Pose "{
  position: {x: 1.0, y: 2.0, z: 0.0},
  orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}
}"

# With rate (10 Hz)
ros2 topic pub -r 10 /x std_msgs/msg/Int32 "{data: 1}"
```

### Monitor Topic Output

```bash
# Echo messages
ros2 topic echo /robot/odom

# Show message rate
ros2 topic hz /robot/odom
```

## Best Practices

### 1. Use Namespaces

Organize topics with namespaces:

```
/robot1/odom
/robot1/battery
/robot2/odom
/robot2/battery
```

### 2. Choose Appropriate QoS

ROS2 uses Quality of Service (QoS) profiles. Consider:

- **Reliable**: Guaranteed delivery (default for most)
- **Best Effort**: Faster, may lose messages (sensors, video)

Currently uses default QoS settings.

### 3. Monitor Critical Topics

Prioritize monitoring safety-critical topics:

- Emergency stops
- Battery levels
- Collision detection
- Hardware faults

### 4. Use Standard Message Types

Prefer `std_msgs`, `geometry_msgs`, `sensor_msgs` when possible for better interoperability.

### 5. Handle Message Frequency

Some topics publish very fast (e.g., camera at 30 Hz). Ensure your monitor can handle the rate or use downsampling.

## Troubleshooting

### ROS2 Environment Not Sourced

**Error**: `ros2: command not found` or similar

**Solution**:
```bash
source /opt/ros/humble/setup.bash
source install/setup.bash
```

### Message Type Not Found

**Error**: Unknown message type

**Solution**:
```bash
# Rebuild custom messages
colcon build
source install/setup.bash

# Check if message exists
ros2 interface show std_msgs/msg/Int32
```

### No Messages Received

**Problem**: Monitor starts but receives no data

**Solutions**:
- Check topic exists: `ros2 topic list`
- Verify topic name in mapping (must match exactly)
- Ensure publisher is running: `ros2 topic info /your/topic`
- Check message type matches: `ros2 topic type /your/topic`

### Field Path Errors

**Problem**: Cannot extract field from message

**Solutions**:
- Verify field path is correct: `ros2 interface show <message_type>`
- Check for typos (case-sensitive)
- Ensure nested fields use dot notation: `pose.position.x`

## Integration with ROS2 Ecosystem

### Launch Files

Create ROS2 launch files to start the monitor with your robot:

```python
from launch import LaunchDescription
from launch.actions import ExecuteProcess

def generate_launch_description():
    return LaunchDescription([
        ExecuteProcess(
            cmd=['cargo', 'run', '--features', 'ros', '--',
                 '--input-ros-topics', 'config.json',
                 'spec.lola'],
            cwd='/path/to/trustworthiness-checker'
        )
    ])
```

### Docker Containers

Deploy monitors in containers alongside ROS2 nodes for easy distribution.

### Recording and Replay

Use ROS2 bags to record and replay monitoring sessions:

```bash
# Record
ros2 bag record -a

# Replay
ros2 bag play <bag_file>
```

## Next Steps

- Learn about [Distributed Monitoring](./distributed-monitoring.md) for multi-robot systems
- Explore [Lola Specifications](../advanced/lola-specifications.md) for complex properties
- Check out [Custom Message Types](../advanced/custom-messages.md) for advanced use cases
