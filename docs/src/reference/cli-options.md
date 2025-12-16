# CLI Options Reference

Complete reference for all command-line options available in the RoboSAPIENS Trustworthiness Checker.

## Synopsis

```bash
cargo run [--features <FEATURES>] -- [OPTIONS] <SPECIFICATION>
```

## Arguments

### `<SPECIFICATION>`

**Required**. Path to the Lola specification file.

```bash
cargo run -- examples/simple_add.lola
```

## Options

### MQTT Options

#### `--mqtt-input`

Enable MQTT input mode. The monitor will subscribe to MQTT topics matching the input stream names in the specification.

**Example**:
```bash
cargo run -- spec.lola --mqtt-input
```

When enabled:
- Input streams are mapped to MQTT topics with the same name
- Monitor subscribes to these topics automatically
- Received messages are parsed as JSON values

#### `--mqtt-output`

Enable MQTT output mode. The monitor will publish computed output streams to MQTT topics.

**Example**:
```bash
cargo run -- spec.lola --mqtt-output
```

When enabled:
- Output streams are published to MQTT topics with the same name
- Values are serialized as JSON
- Published on each computation cycle

#### `--input-mqtt-topics <TOPICS>...`

**Legacy syntax**. Explicitly specify which MQTT topics to use as inputs.

**Arguments**: Space-separated list of topic names

**Example**:
```bash
cargo run -- spec.lola --input-mqtt-topics x y z
```

This maps the first input stream to topic `x`, second to `y`, etc., in the order they appear in the specification.

#### `--output-mqtt-topics <TOPICS>...`

**Legacy syntax**. Explicitly specify which MQTT topics to use as outputs.

**Arguments**: Space-separated list of topic names

**Example**:
```bash
cargo run -- spec.lola --output-mqtt-topics result status
```

**Note**: With legacy syntax, auxiliary streams must also be listed even though they won't be published.

#### `--mqtt-broker <HOST:PORT>`

Specify the MQTT broker address.

**Default**: `localhost:1883`

**Example**:
```bash
cargo run -- spec.lola --mqtt-input --mqtt-broker 192.168.1.100:1883
```

#### `--mqtt-client-id <ID>`

Set a custom MQTT client ID.

**Default**: Auto-generated unique ID

**Example**:
```bash
cargo run -- spec.lola --mqtt-input --mqtt-client-id monitor_node_1
```

### ROS2 Options

#### `--input-ros-topics <CONFIG_FILE>`

Enable ROS2 input mode using a topic mapping configuration file.

**Requires**: Build with `--features ros`

**Arguments**: Path to JSON configuration file

**Example**:
```bash
cargo run --features ros -- --input-ros-topics config.json spec.lola
```

The configuration file maps Lola streams to ROS2 topics. See [ROS2 Integration](../tutorials/ros2-integration.md) for format details.

#### `--output-ros-topics <CONFIG_FILE>`

Enable ROS2 output mode using a topic mapping configuration file.

**Requires**: Build with `--features ros`

**Arguments**: Path to JSON configuration file

**Example**:
```bash
cargo run --features ros -- --output-ros-topics config.json spec.lola
```

### Distribution Options

#### `--distribution-graph <GRAPH_FILE>`

Enable distributed monitoring using the specified distribution graph.

**Arguments**: Path to JSON distribution graph file

**Example**:
```bash
cargo run -- spec.lola --distribution-graph graph.json --local-node A
```

The distribution graph defines:
- Which streams are computed on which nodes
- Communication edges between nodes

See [Distributed Monitoring](../tutorials/distributed-monitoring.md) for details.

#### `--local-node <NODE_NAME>`

Specify which node this instance represents in the distribution graph.

**Required when**: Using `--distribution-graph`

**Arguments**: Node name (must match a node in the distribution graph)

**Example**:
```bash
cargo run -- spec.lola \
  --distribution-graph graph.json \
  --local-node NodeA
```

### Logging and Debugging

#### `--verbose` / `-v`

Enable verbose output.

**Example**:
```bash
cargo run -- spec.lola -v
```

Can be specified multiple times for increased verbosity:
- `-v`: Info level
- `-vv`: Debug level
- `-vvv`: Trace level

#### `--quiet` / `-q`

Suppress all output except errors.

**Example**:
```bash
cargo run -- spec.lola -q
```

#### `--log-file <PATH>`

Write logs to a file instead of stdout.

**Example**:
```bash
cargo run -- spec.lola --log-file monitor.log
```

### Output Options

#### `--output-format <FORMAT>`

Specify the output format for monitoring results.

**Values**: `json`, `text`, `csv`

**Default**: `json`

**Example**:
```bash
cargo run -- spec.lola --mqtt-input --output-format text
```

#### `--no-color`

Disable colored output (useful for log files or CI/CD).

**Example**:
```bash
cargo run -- spec.lola --no-color
```

### Timing and Performance

#### `--clock-step <MILLISECONDS>`

Set the monitoring clock step in milliseconds.

**Default**: `100` (10 Hz)

**Example**:
```bash
cargo run -- spec.lola --mqtt-input --clock-step 50
```

This controls how frequently the monitor evaluates the specification.

#### `--timeout <SECONDS>`

Stop monitoring after specified duration.

**Example**:
```bash
cargo run -- spec.lola --mqtt-input --timeout 60
```

Useful for automated testing or bounded monitoring sessions.

### Validation Options

#### `--check`

Validate the specification without running the monitor.

**Example**:
```bash
cargo run -- spec.lola --check
```

Checks for:
- Syntax errors
- Type errors
- Undefined streams
- Circular dependencies

#### `--print-ast`

Print the Abstract Syntax Tree of the specification.

**Example**:
```bash
cargo run -- spec.lola --print-ast
```

Useful for debugging specification parsing.

#### `--print-dependencies`

Print the stream dependency graph.

**Example**:
```bash
cargo run -- spec.lola --print-dependencies
```

Shows which streams depend on which other streams.

### Help and Version

#### `--help` / `-h`

Display help information.

**Example**:
```bash
cargo run -- --help
```

#### `--version` / `-V`

Display version information.

**Example**:
```bash
cargo run -- --version
```

## Environment Variables

### `RUST_LOG`

Control logging level using the `env_logger` format.

**Values**: `error`, `warn`, `info`, `debug`, `trace`

**Example**:
```bash
RUST_LOG=debug cargo run -- spec.lola
```

**Module-specific logging**:
```bash
RUST_LOG=trustworthiness_checker=debug,mqtt=trace cargo run -- spec.lola
```

### `MQTT_BROKER`

Default MQTT broker address (can be overridden by `--mqtt-broker`).

**Example**:
```bash
export MQTT_BROKER=192.168.1.100:1883
cargo run -- spec.lola --mqtt-input
```

### `ROS_DOMAIN_ID`

ROS2 domain ID (when using ROS2 features).

**Example**:
```bash
export ROS_DOMAIN_ID=42
cargo run --features ros -- --input-ros-topics config.json spec.lola
```

## Cargo Features

Features must be enabled during build or run:

### `ros`

Enable ROS2 support.

**Usage**:
```bash
cargo run --features ros -- --input-ros-topics config.json spec.lola
```

### `distributed`

Enable distributed monitoring features (usually enabled by default).

**Usage**:
```bash
cargo build --features distributed
```

## Common Command Combinations

### Basic MQTT Monitoring

```bash
cargo run -- spec.lola --mqtt-input --mqtt-output
```

### ROS2 Monitoring with Logging

```bash
RUST_LOG=info cargo run --features ros -- \
  --input-ros-topics config.json \
  spec.lola \
  --log-file monitor.log
```

### Distributed Monitoring with Verbose Output

```bash
cargo run -- spec.lola \
  --mqtt-input --mqtt-output \
  --distribution-graph graph.json \
  --local-node NodeA \
  -vv
```

### Validation Only

```bash
cargo run -- spec.lola --check --print-dependencies
```

### Testing with Timeout

```bash
cargo run -- spec.lola \
  --mqtt-input --mqtt-output \
  --timeout 30 \
  --output-format text
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0    | Success |
| 1    | General error |
| 2    | Specification parsing error |
| 3    | Invalid command-line arguments |
| 4    | Connection error (MQTT/ROS2) |
| 5    | Runtime monitoring error |
| 6    | Timeout reached |

## Tips

### Performance Optimization

For high-frequency monitoring, use binary releases:
```bash
cargo build --release
./target/release/trustworthiness-checker spec.lola --mqtt-input
```

### Debug Specification Issues

Use validation options to understand your specification:
```bash
cargo run -- spec.lola --check --print-ast --print-dependencies -vv
```

### Testing MQTT Connectivity

Verify MQTT connection before running complex specifications:
```bash
# Test with minimal spec
echo "input x: Int\noutput y := x" > test.lola
cargo run -- test.lola --mqtt-input --mqtt-output -v
```

### Capture Complete Logs

For debugging, capture all output:
```bash
RUST_LOG=trace cargo run -- spec.lola --mqtt-input 2>&1 | tee full.log
```

## See Also

- [Configuration Reference](./configuration.md) - Configuration file options
- [MQTT Integration](../tutorials/mqtt-integration.md) - MQTT setup and usage
- [ROS2 Integration](../tutorials/ros2-integration.md) - ROS2 setup and usage
- [Distributed Monitoring](../tutorials/distributed-monitoring.md) - Distribution graph format
