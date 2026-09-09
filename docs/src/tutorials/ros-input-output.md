# Tutorial: use ROS input and output

The ROS adapter connects model variables to ROS 2 topics using checked-in topic
and message-type mappings. This is a live workflow: the Trustworthiness Checker
waits for ROS messages and publishes results until its foreground process is
stopped.

## Prerequisites and overlay

Run the setup from the repository root. You need a sourced ROS 2 installation,
`colcon`, the `ros2` CLI, and the custom message package in `ros_interfaces`.
Replace `<ros-distro>` with the ROS 2 distribution installed on the machine:

```sh
source /opt/ros/<ros-distro>/setup.bash
cd ros_interfaces
colcon build
source install/setup.bash
cd ..
```

Build the overlay once unless `ros_interfaces/` changes. Source both setup files
again in every terminal that runs `cargo` or `ros2`.

## Add two values received through ROS

The DSRV program adds inputs `x` and `y` and exposes the result as `z`:

```dsrv
in x: Int
in y: Int
out z: Int
z = x + y
```

The repository stores this program as `examples/simple_add.dsrv`. Its input mapping binds `x` to `/x` and `y` to `/y` as `std_msgs/msg/Int32`; its output mapping binds `z` to `/z` with the same message type. From the repository root, start the checker in Terminal 1:

```sh
cargo run --features ros -- examples/simple_add.dsrv \
  --input-ros-file examples/ros/simple_add_mapping.json \
  --output-ros-file examples/ros/simple_add_output_mapping.json
```

In Terminal 2, from the repository root, source ROS and the overlay, then watch
`z`:

```sh
source /opt/ros/<ros-distro>/setup.bash
source ros_interfaces/install/setup.bash
ros2 topic echo /z
```

In Terminal 3, source the same environments and publish one input pair:

```sh
source /opt/ros/<ros-distro>/setup.bash
source ros_interfaces/install/setup.bash
ros2 topic pub --once /x std_msgs/msg/Int32 '{data: 1}'
ros2 topic pub --once /y std_msgs/msg/Int32 '{data: 2}'
```

Terminal 2 should show an `Int32` message containing:

```text
data: 3
---
```

The first publication alone may not yield a visible `z` value because the other
input is missing for that evaluation. The first `z` message proves that the
checker has received enough input, evaluated the model, and published through
its ROS output path. Topic discovery or a running checker process alone does not
provide a separate Trustworthiness Checker readiness signal.

## MSTLO message compatibility

The ordinary DSRV mapping above carries ordinary typed values such as `Int32`.
MSTLO is different: select it with `--language mstlo` and use the generated
`MstloTimedValue` message, as shown by
`examples/ros/mstlo_timed_value_mapping.json`:

```json
{
  "x": [
    "/signals/x",
    "MstloTimedValue"
  ]
}
```

`MstloTimedValue` is not a drop-in replacement for the ordinary DSRV/`Value`
ROS backend. Conversely, an ordinary `Int32` mapping is not an MSTLO timed-value
mapping. Build and source the overlay before compiling the MSTLO path; do not
pass an explicit `--runtime` with `--language mstlo`, because MSTLO selects its
own runtime.

## Stop and cleanup

The checker and `ros2 topic echo` are long-lived foreground processes. Press
`Ctrl-C` in Terminal 1 and Terminal 2 when finished; the `--once` publishers in
Terminal 3 exit after one message. This walkthrough creates no container or
persistent topic data, and the built ROS overlay can be kept for the next run.
