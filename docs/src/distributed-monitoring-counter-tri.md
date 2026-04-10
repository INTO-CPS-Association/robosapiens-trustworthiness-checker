# Distributed Monitoring -- Counter Tri demo

The counter tri example is a basic demo of dynamic distribution based on distribution constraints.

## Distribution Graph Format

The distribution graph is present at `examples/distributed/counter_tri.dsrv`

## Main command

Using Ros for everything
```bash
RUST_LOG=INFO cargo run --features ros --   examples/distributed/counter_tri.dsrv   --runtime distributed   --input-ros-file examples/ros/counter_tri_reconf_input_map.json   --output-stdout   --distribution-graph examples/counter_tri_distribution_graph.json   --distribution-constraints graphConstraintX graphConstraintY   --scheduling-mode ros   --scheduler-ros-node-name tc_scheduler_main   --scheduler-reconf-topic reconfig
```

From input file:
```bash
RUST_LOG=INFO cargo run --features ros --   examples/distributed/counter_tri.dsrv   --runtime distributed   --input-file examples/counter_tri.input   --output-stdout   --distribution-graph examples/counter_tri_distribution_graph.json   --distribution-constraints graphConstraintX graphConstraintY   --scheduling-mode ros   --scheduler-ros-node-name tc_scheduler_main   --scheduler-reconf-topic reconfig
```

## Adjacent node command

```bash
cargo run --features ros -- examples/distributed/counter_tri_nodc.dsrv --runtime reconf-semi-sync --reconf-topic reconfig_node1 --input-ros-file examples/ros/counter_tri_reconf_input_map.json --output-ros-file examples/ros/counter_tri_output_map.json
```

## Node commands

```bash
cargo run --features ros -- examples/distributed/empty.dsrv --runtime reconf-semi-sync --reconf-topic reconfig_node2 --input-ros-file examples/ros/counter_tri_reconf_input_map.json --output-ros-file examples/ros/counter_tri_output_map.json
```
```bash
cargo run --features ros -- examples/distributed/empty.dsrv --runtime reconf-semi-sync --reconf-topic reconfig_node3 --input-ros-file examples/ros/counter_tri_reconf_input_map.json --output-ros-file examples/ros/counter_tri_output_map.json
```

## Monitoring commands

```bash
ros2 topic echo /reconfig_node1
```

```bash
ros2 topic echo /reconfig_node2
```

```bash
ros2 topic echo /reconfig_node3
```
