
# 1) Publish data without c first
ros2 topic pub --once /x std_msgs/msg/Int32 "{data: 1}"
ros2 topic pub --once /y std_msgs/msg/Int32 "{data: 2}"
ros2 topic pub --once /z std_msgs/msg/Int32 "{data: 3}"

# 2) Now publish c (this should unblock constraint-based scheduling)
ros2 topic pub --once /c std_msgs/msg/Bool "{data: true}"

# 3) Publish one more full data point after c
ros2 topic pub --once /x std_msgs/msg/Int32 "{data: 4}"
ros2 topic pub --once /y std_msgs/msg/Int32 "{data: 5}"
ros2 topic pub --once /z std_msgs/msg/Int32 "{data: 6}"