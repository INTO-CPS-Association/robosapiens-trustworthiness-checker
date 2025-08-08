The work presented here is supported by the RoboSAPIENS project funded by the European Commission's Horizon Europe programme under grant agreement number 101133807.

## MQTT:
For a minimum example of running with MQTT, run the following specification:
```bash
cargo run -- examples/simple_add.lola --input-mqtt-topics x y --output-mqtt-topics z
```
In MQTT Explorer or similar, send the following message on the topic "x" followed by sending the same message on the topic "y":
```json
{
    "Int": 42
}
```
The following result should be visible on the "z" topic:
```json
{
    "Int": 84
}
```

Note that if you want to provide e.g., an Unknown value then it must be done with:
```json
{
    "Unknown": null
}
```

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

