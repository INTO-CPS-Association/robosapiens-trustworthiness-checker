# Combines data on self (robot's own pose), and robots nearby

float64 timestamp
string source_robot_id
geometry_msgs/Pose source_robot_pose
string[] nearby_robot_ids
geometry_msgs/Pose[] nearby_robot_poses
