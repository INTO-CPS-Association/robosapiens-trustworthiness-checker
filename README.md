The work presented here is supported by the RoboSAPIENS project funded by the European Commission's Horizon Europe programme under grant agreement number 101133807.

## MQTT:
For a minimum example of running with MQTT, run the following specification:
```bash
cargo run -- examples/simple_add.lola --input-mqtt-topics x y --output-mqtt-topics z
```
In MQTT Explorer or similar, send the following message on the topic "x" followed by sending the same message on the topic "y":
```json
42
```
The following result should be visible on the "z" topic:
```json
{
    "value": 84
}
```

### Note:
If you want to provide e.g., a Deferred value then it must be done with:
```json
{
    "Deferred": null
}
```

## MQTT - Legacy syntax:
For a minimum example of running with MQTT, run the following specification:
```bash
cargo run -- examples/simple_add.lola --input-mqtt-topics x y --output-mqtt-topics z
```
Follow the same syntax for sending to topics as decribed above.

### Note:
If you have auxiliary streams they must currently also be specified in `output-mqtt-topics` but they are not outputted through MQTT. This is a current limitation.

## ROS2:
For a minimum example of running with ROS2 open a terminal and source ROS2.
Then run colcon to compile the custom message types:
```bash
colcon build
```
And source the install file:
```bash
source install/setup.bash
```
Start monitoring the specification:
```bash
cargo run --features ros -- --input-ros-topics examples/counter_ros_map.json examples/counter.lola
```
In another terminal, source ROS2 and run the following command:
```bash
ros2 topic pub /x std_msgs/msg/Int32 "{data: 1}"
```
The output in the first terminal should now be counting forever.

# Distribution - local node:
In terminal 1:
`cargo run -- examples/simple_add_distributable.lola --mqtt-input --mqtt-output --distribution-graph examples/simple_add_distribution_graph.json --local-node A`

In terminal 2:
`cargo run -- examples/simple_add_distributable.lola --mqtt-input --mqtt-output --distribution-graph examples/simple_add_distribution_graph.json --local-node B`

Use an MQTT client: Publish `1` to topic `x`, publish `2` to topic `y`. Observe that `w` is calculated.
Publish `3` to topic `z`. Observe that `v` is calculated.
