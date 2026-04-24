# Simple add with full dynamic redistribution

Launch scheduler node:
```bash
RUST_LOG=INFO cargo run --features ros -- examples/distributed/simple_add_distributable_alt_constraints.dsrv --runtime distributed --distribution-graph examples/distributed/simple_add_alt_constraints_distribution_graph.json --dist-constraint-solver sat --distribution-constraints distW distV --scheduling-mode ros --scheduler-ros-node-name tc_scheduler_main --scheduler-reconf-topic reconfig --input-ros-file examples/distributed/simple_add_distributable_ros_in_full.json --output-ros-file examples/distributed/simple_add_distributable_ros_out.json
```

Subscribe to `w` and `v` topics:
```bash
ros2 topic echo /w "std_msgs/msg/Int32"
ros2 topic echo /v "std_msgs/msg/Int32"
```

Subscribe to reconfig topics:
```bash
ros2 topic echo /reconfig_A
ros2 topic echo /reconfig_B
```

Launch reconfigurable local nodes:
```bash
RUST_LOG=INFO cargo run --features ros -- examples/simple_add_distributable.dsrv --runtime reconf-semi-sync --local-node A --input-ros-file examples/distributed/simple_add_distributable_ros_in_full.json --output-ros-file examples/distributed/simple_add_distributable_ros_out.json --reconf-topic reconfig_A
RUST_LOG=INFO cargo run --features ros -- examples/simple_add_distributable.dsrv --runtime reconf-semi-sync --local-node B --input-ros-file examples/distributed/simple_add_distributable_ros_in_full.json --output-ros-file examples/distributed/simple_add_distributable_ros_out.json --reconf-topic reconfig_B
```

Send some simulated inputs:
```bash
ros2 topic pub -1 /c "std_msgs/msg/Bool" "{data: True}"
ros2 topic pub -1 /x "std_msgs/msg/Int32" "{data: 42}"
ros2 topic pub -1 /y "std_msgs/msg/Int32" "{data: 69}"
ros2 topic pub -1 /z "std_msgs/msg/Int32" "{data: 33}"
ros2 topic pub -1 /c "std_msgs/msg/Bool" "{data: False}"
ros2 topic pub -1 /x "std_msgs/msg/Int32" "{data: 3}"
ros2 topic pub -1 /y "std_msgs/msg/Int32" "{data: 4}"
ros2 topic pub -1 /z "std_msgs/msg/Int32" "{data: 5}"
```
