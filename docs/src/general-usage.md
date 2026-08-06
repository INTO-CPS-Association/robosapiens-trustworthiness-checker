# General Usage

This page gives a short overview of the standard command-line workflow for running the Trustworthiness Checker (TC) with the most common input and output configurations.

## Basic usage
The most basic usage of the TC is to run it locally on a model and an input trace.

Run the TC locally from the repository with `cargo run -- <other options>`. The `--` separates Cargo's own arguments from the arguments passed to the TC itself.

The general shape of the command is:

```bash
cargo run -- <model> <input-options> [output-options] [extra options]
```

In the simplest case, provide a model, and an input source:

```bash
cargo run -- examples/simple_add.dsrv --input-file examples/simple_add.input 
```

This starts the TC with the model in `examples/simple_add.dsrv`, reads the trace from `examples/simple_add.input`, and prints the monitoring result to standard output. Printing to standard output can also be explicitly specified with `--output-stdout`.

Additional flags can be added for language selection, parser choice, runtime settings, or distributed monitoring.

### Getting help 

To see the available command-line options, run:

```bash
cargo run -- --help
```

The help output shows the supported input and output flags together with the available option values.

## MQTT Usage

For MQTT-based monitoring, use `--mqtt-input` and `--mqtt-output`.

```bash
cargo run -- examples/simple_add.dsrv --mqtt-input --mqtt-output
```

With these flags, the TC maps stream names in the specification directly to MQTT topics. For example, if the model has input streams `x` and `y`, the TC subscribes to the topics `x` and `y`. If the model produces an output stream `z`, the TC publishes the result on the topic `z`.

### Message format

Publish MQTT payloads as JSON values that match the expected stream type. On Linux systems, we suggest using MQTT Explorer to publish and inspect messages. For the `simple_add.lola` example, publish the following value to topic `x` and then to topic `y`:

```json
42
```

The result is then published on topic `z` as:

```json
{
    "value": 84
}
```

## ROS2 Usage

For ROS2-based monitoring, run the TC with the `ros` feature enabled. I.e., `cargo run --features ros -- <other options>`

Before starting the TC, source your ROS2 installation in the terminal, for example:

```bash
source /opt/ros/<distro>/setup.bash
```

Then build the custom messages from the ROS interface workspace and source the generated overlay:

```bash
cd ros_interfaces
colcon build
source install/setup.bash
cd ..
```

You can then start the TC with a ROS mapping file. The current CLI names are `--input-ros-file` and `--output-ros-file`:

```bash
cargo run --features ros -- examples/simple_add.dsrv \
  --input-ros-file examples/ros/simple_add_mapping.json \
  --output-ros-file examples/ros/simple_add_output_mapping.json
```

In a second terminal, source ROS again and subscribe to the z topic:

```bash
source /opt/ros/<distro>/setup.bash
ros2 topic echo /z
```

In a third terminal, publish to the x and y topics:
```bash
source /opt/ros/<distro>/setup.bash
ros2 topic pub /x std_msgs/msg/Int32 "{data: 1}"
ros2 topic pub /y std_msgs/msg/Int32 "{data: 1}"
```

The second terminal should now show the result.

### MSTLO ROS values

MSTLO uses the generated `robo_sapiens_interfaces/msg/MstloTimedValue` message rather than JSON over ROS. Build and source the interface overlay before compiling with `--features ros`, then use `MstloTimedValue` in both input and output mapping files. The example mapping is [examples/ros/mstlo_timed_value_mapping.json](../../examples/ros/mstlo_timed_value_mapping.json):

```json
{
  "x": {
    "topic": "/signals/x",
    "msg_type": "MstloTimedValue"
  }
}
```

An MSTLO file sample keeps the outer delivery tick separate from the inner signal timestamp:

```text
0: x = {"time": 0, "value": 7.0}
1: x = {"time": 1000, "value": 4.0}
```

The ROS message carries quantitative, qualitative, and robustness-interval outputs in its tagged fields. `NoVal` is an internal sparse-stream marker and is never published.

## Redis Usage

TODO: TW - please write a brief demonstration on how to use Redis. Cant seem to get it working.
