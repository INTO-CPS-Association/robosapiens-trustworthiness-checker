# Configuration

This reference guide covers all configuration options available in the RoboSAPIENS Trustworthiness Checker.

## Overview

The trustworthiness checker can be configured through:

1. **Command-line arguments**: Primary method for runtime configuration
2. **Environment variables**: System-level settings
3. **Configuration files**: Distribution graphs and topic mappings

## Command-Line Arguments

### Basic Options

#### `<SPECIFICATION>`

The Lola specification file to monitor.

```bash
cargo run -- examples/simple_add.lola
```

**Required**: Yes  
**Type**: File path  
**Example**: `examples/simple_add.lola`

#### `--help`, `-h`

Display help information and exit.

```bash
cargo run -- --help
```

### Input/Output Configuration

#### `--mqtt-input`

Enable MQTT input streams. Automatically maps Lola input streams to MQTT topics.

```bash
cargo run -- spec.lola --mqtt-input
```

**Required**: No  
**Default**: Disabled  
**Conflicts with**: `--input-ros-topics`

#### `--mqtt-output`

Enable MQTT output streams. Automatically maps Lola output streams to MQTT topics.

```bash
cargo run -- spec.lola --mqtt-output
```

**Required**: No  
**Default**: Disabled  
**Conflicts with**: `--output-ros-topics`

#### `--input-mqtt-topics <TOPICS...>`

(Legacy syntax) Explicitly specify MQTT input topics.

```bash
cargo run -- spec.lola --input-mqtt-topics x y z
```

**Required**: No  
**Type**: Space-separated list  
**Example**: `x y z temperature pressure`

#### `--output-mqtt-topics <TOPICS...>`

(Legacy syntax) Explicitly specify MQTT output topics.

```bash
cargo run -- spec.lola --output-mqtt-topics result status
```

**Required**: No  
**Type**: Space-separated list  
**Note**: Must include auxiliary streams even though they won't be published

#### `--input-ros-topics <CONFIG_FILE>`

Enable ROS2 input with topic mapping configuration.

```bash
cargo run --features ros -- --input-ros-topics config.json spec.lola
```

**Required**: No (unless using ROS2)  
**Type**: JSON file path  
**Example**: `examples/ros_topic_map.json`  
**Requires**: Built with `--features ros`

#### `--output-ros-topics <CONFIG_FILE>`

Enable ROS2 output with topic mapping configuration.

```bash
cargo run --features ros -- --output-ros-topics config.json spec.lola
```

**Required**: No  
**Type**: JSON file path  
**Requires**: Built with `--features ros`

### Distribution Options

#### `--distribution-graph <GRAPH_FILE>`

Enable distributed monitoring with the specified distribution graph.

```bash
cargo run -- spec.lola --mqtt-input --mqtt-output \
  --distribution-graph graph.json
```

**Required**: No (unless using distributed monitoring)  
**Type**: JSON file path  
**Example**: `examples/distribution_graph.json`

#### `--local-node <NODE_NAME>`

Specify which node this instance represents in the distribution graph.

```bash
cargo run -- spec.lola --mqtt-input --mqtt-output \
  --distribution-graph graph.json \
  --local-node A
```

**Required**: Yes (when using `--distribution-graph`)  
**Type**: String  
**Must match**: Node name in distribution graph

### Advanced Options

#### `--verbosity <LEVEL>`, `-v`

Set logging verbosity level.

```bash
cargo run -- spec.lola --verbosity debug
```

**Options**:
- `error`: Only errors
- `warn`: Warnings and errors
- `info`: Informational messages (default)
- `debug`: Detailed debugging information
- `trace`: Very detailed trace information

**Default**: `info`

#### `--offline`

Run in offline mode (process recorded data instead of live streams).

```bash
cargo run -- spec.lola --offline --input-file data.json
```

**Required**: No  
**Use case**: Replay and analysis

## Environment Variables

### MQTT Configuration

#### `MQTT_BROKER`

MQTT broker address.

```bash
export MQTT_BROKER=192.168.1.100:1883
cargo run -- spec.lola --mqtt-input --mqtt-output
```

**Default**: `localhost:1883`  
**Format**: `host:port`

#### `MQTT_CLIENT_ID`

Custom MQTT client identifier.

```bash
export MQTT_CLIENT_ID=monitor_node_1
cargo run -- spec.lola --mqtt-input --mqtt-output
```

**Default**: Auto-generated  
**Format**: String (must be unique per client)

#### `MQTT_USERNAME`

MQTT broker authentication username.

```bash
export MQTT_USERNAME=monitor_user
export MQTT_PASSWORD=secret
cargo run -- spec.lola --mqtt-input --mqtt-output
```

**Default**: None (anonymous connection)

#### `MQTT_PASSWORD`

MQTT broker authentication password.

**Default**: None  
**Security**: Use with caution; consider using secrets management

#### `MQTT_QOS`

MQTT Quality of Service level.

```bash
export MQTT_QOS=1
cargo run -- spec.lola --mqtt-input --mqtt-output
```

**Options**:
- `0`: At most once (fast, may lose messages)
- `1`: At least once (guaranteed, may duplicate)
- `2`: Exactly once (slowest, guaranteed once)

**Default**: `0`

#### `MQTT_KEEP_ALIVE`

MQTT keep-alive interval in seconds.

```bash
export MQTT_KEEP_ALIVE=60
cargo run -- spec.lola --mqtt-input --mqtt-output
```

**Default**: `30`  
**Type**: Integer (seconds)

### ROS2 Configuration

#### `ROS_DOMAIN_ID`

ROS2 domain ID for network isolation.

```bash
export ROS_DOMAIN_ID=42
cargo run --features ros -- --input-ros-topics config.json spec.lola
```

**Default**: `0`  
**Range**: 0-232

#### `RMW_IMPLEMENTATION`

ROS2 middleware implementation.

```bash
export RMW_IMPLEMENTATION=rmw_cyclonedds_cpp
cargo run --features ros -- --input-ros-topics config.json spec.lola
```

**Options**:
- `rmw_cyclonedds_cpp`
- `rmw_fastrtps_cpp`
- Other RMW implementations

**Default**: System default

### Logging Configuration

#### `RUST_LOG`

Rust logging configuration (more granular than `--verbosity`).

```bash
export RUST_LOG=trustworthiness_checker=debug,warn
cargo run -- spec.lola --mqtt-input --mqtt-output
```

**Format**: `[target::]level[,target::level]*`  
**Examples**:
- `debug`: All debug and above
- `trustworthiness_checker=trace`: Trace only for this crate
- `info,mio=error`: Info by default, error for mio crate

**Default**: `info`

#### `RUST_BACKTRACE`

Enable stack traces on panic.

```bash
export RUST_BACKTRACE=1
cargo run -- spec.lola --mqtt-input --mqtt-output
```

**Options**:
- `0`: Disabled
- `1`: Enabled
- `full`: Full backtrace

**Default**: `0`

### Performance Configuration

#### `MONITOR_BUFFER_SIZE`

Size of internal stream buffers.

```bash
export MONITOR_BUFFER_SIZE=1000
cargo run -- spec.lola --mqtt-input --mqtt-output
```

**Default**: `100`  
**Type**: Integer  
**Use case**: High-frequency streams

#### `WORKER_THREADS`

Number of worker threads for parallel processing.

```bash
export WORKER_THREADS=4
cargo run -- spec.lola --mqtt-input --mqtt-output
```

**Default**: Number of CPU cores  
**Type**: Integer

## Configuration Files

### Distribution Graph Format

**File**: `distribution_graph.json`

```json
{
  "nodes": {
    "node_name": {
      "streams": ["stream1", "stream2"],
      "mqtt_broker": "optional_broker_override",
      "priority": 1
    }
  },
  "edges": [
    {
      "from": "source_node",
      "to": "destination_node",
      "streams": ["stream1"],
      "transport": "mqtt",
      "compression": false
    }
  ],
  "metadata": {
    "version": "1.0",
    "description": "Distribution graph description"
  }
}
```

**Fields**:

- `nodes`: Map of node configurations
  - `streams`: Streams handled by this node (required)
  - `mqtt_broker`: Override MQTT broker for this node (optional)
  - `priority`: Node priority for scheduling (optional)

- `edges`: Array of connections between nodes
  - `from`: Source node name (required)
  - `to`: Destination node name (required)
  - `streams`: Streams transmitted (required)
  - `transport`: Transport protocol (optional, default: mqtt)
  - `compression`: Enable compression (optional, default: false)

- `metadata`: Optional metadata
  - `version`: Graph format version
  - `description`: Human-readable description

### ROS2 Topic Mapping Format

**File**: `ros_topic_map.json`

```json
{
  "stream_name": {
    "topic": "/ros2/topic/name",
    "message_type": "package_name/msg/MessageType",
    "field_path": "field.subfield.value",
    "qos": {
      "reliability": "reliable",
      "durability": "volatile",
      "history": "keep_last",
      "depth": 10
    },
    "transform": "optional_transform_function"
  }
}
```

**Fields**:

- `topic`: ROS2 topic name (required)
- `message_type`: ROS2 message type (required)
- `field_path`: Dot-separated path to value (required)
- `qos`: Quality of Service settings (optional)
  - `reliability`: `reliable` or `best_effort`
  - `durability`: `transient_local` or `volatile`
  - `history`: `keep_last` or `keep_all`
  - `depth`: History depth (integer)
- `transform`: Transform function name (optional)

## Configuration Precedence

When multiple configuration sources are present, they are applied in this order (later overrides earlier):

1. Default values
2. Environment variables
3. Configuration files
4. Command-line arguments

**Example**:

```bash
# Default MQTT broker: localhost:1883
# Environment overrides: 192.168.1.100:1883
export MQTT_BROKER=192.168.1.100:1883

# Command-line argument would override environment (if supported)
cargo run -- spec.lola --mqtt-input --mqtt-output
```

## Configuration Best Practices

### 1. Use Environment Variables for Deployment

Store deployment-specific settings in environment variables:

```bash
# .env file
MQTT_BROKER=production.broker.com:1883
MQTT_USERNAME=monitor_prod
RUST_LOG=info
```

### 2. Keep Distribution Graphs Version Controlled

Store distribution graphs in version control alongside specifications:

```
project/
├── specs/
│   ├── monitor.lola
│   └── distributed_monitor.lola
├── graphs/
│   ├── local_test.json
│   └── production.json
└── configs/
    ├── ros_dev.json
    └── ros_prod.json
```

### 3. Use Separate Configurations per Environment

```bash
# Development
cargo run -- spec.lola --distribution-graph graphs/dev.json

# Staging
cargo run -- spec.lola --distribution-graph graphs/staging.json

# Production
cargo run -- spec.lola --distribution-graph graphs/production.json
```

### 4. Document Custom Configurations

Include README files with your configuration:

```
configs/
├── README.md           # Explains configuration structure
├── production.json     # Production settings
└── production.env      # Environment variables
```

### 5. Validate Configurations Before Deployment

```bash
# Test configuration locally first
cargo run -- spec.lola --distribution-graph graph.json --local-node A --verbosity debug
```

## Configuration Examples

### Example 1: Simple MQTT Monitor

```bash
cargo run --release -- examples/simple.lola \
  --mqtt-input \
  --mqtt-output \
  --verbosity info
```

### Example 2: ROS2 Monitor with Custom Domain

```bash
export ROS_DOMAIN_ID=42
cargo run --release --features ros -- \
  --input-ros-topics configs/robot_sensors.json \
  examples/robot_monitor.lola
```

### Example 3: Distributed Monitor with Authentication

```bash
export MQTT_BROKER=secure.broker.com:8883
export MQTT_USERNAME=monitor_node_a
export MQTT_PASSWORD=secretpassword
export MQTT_QOS=1

cargo run --release -- examples/distributed.lola \
  --mqtt-input \
  --mqtt-output \
  --distribution-graph graphs/production.json \
  --local-node NodeA
```

### Example 4: High-Performance Configuration

```bash
export MONITOR_BUFFER_SIZE=10000
export WORKER_THREADS=8
export RUST_LOG=warn

cargo run --release -- high_frequency.lola \
  --mqtt-input \
  --mqtt-output
```

## Troubleshooting Configuration Issues

### Configuration Not Applied

**Problem**: Setting doesn't seem to take effect

**Check**:
1. Verify correct environment variable name (case-sensitive)
2. Check command-line argument syntax
3. Ensure configuration file is valid JSON
4. Check logs for parsing errors

### Connection Issues

**Problem**: Cannot connect to MQTT broker or ROS2

**Check**:
1. Verify `MQTT_BROKER` or `ROS_DOMAIN_ID` is correct
2. Test connectivity: `ping <broker_host>`
3. Check firewall settings
4. Verify credentials if using authentication

### Performance Issues

**Problem**: Monitor is slow or uses too much memory

**Tune**:
1. Reduce `MONITOR_BUFFER_SIZE` for memory
2. Adjust `WORKER_THREADS` for CPU usage
3. Lower `RUST_LOG` verbosity
4. Use `MQTT_QOS=0` for faster processing

## Next Steps

- Review [CLI Options](./cli-options.md) for complete command reference
- Check [MQTT Integration](../tutorials/mqtt-integration.md) for MQTT-specific configuration
- See [Distributed Monitoring](../tutorials/distributed-monitoring.md) for distribution graph details
