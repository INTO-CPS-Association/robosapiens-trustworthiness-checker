#!/bin/bash

if [ $# -ne 2 ]; then
  echo "Usage: $0 <file> <topic>"
  echo "Publishes the file content as std_msgs/msg/String 'data' field once via ros2 topic pub."
  exit 1
fi

FILE="$1"
TOPIC="$2"

if [ ! -f "$FILE" ]; then
  echo "Error: File '$FILE' not found."
  exit 1
fi

CONTENT=$(cat "$FILE")

ros2 topic pub --once "$TOPIC" std_msgs/msg/String "data: |-
$(echo "$CONTENT" | sed 's/^/  /; $!G')"
