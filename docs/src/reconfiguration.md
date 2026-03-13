# Reconfiguration Runtime

One of the novel features of the TC is the ability to reconfigure a running monitor at runtime. In practice, this means that the TC can receive an updated specification while it is already running and then rebuild the monitoring pipeline around the new specification.

This is useful when the property to be monitored changes during execution, for example because new streams become relevant, old streams are removed, or a different monitoring task should take over without restarting the full deployment.

## How it works

When the TC runs in reconfiguration mode, it listens for reconfiguration commands on a dedicated input stream. This means that an extra topic is injected automatically into the input configuration. By default, this topic is named `reconfig`, but you can specify it with `--reconf-topic`.

Each reconfiguration command is sent as a JSON string payload with the following structure:

```json
{
  "spec": "in x: Int\nout z: Int\nz = x",
  "type_info": {}
}
```

The `spec` field contains the new specification as a string. The `type_info` field is mainly relevant for ROS-based setups, where each input and output stream in the new specification must be associated with a ROS message type.

For ROS-based reconfiguration, the payload must include the ROS message types for all inputs and outputs used by the new specification. For example:

```json
{
  "spec": "in x: Int\nout z: Int\nz = x",
  "type_info": {
    "x": "Int32",
    "z": "Int32"
  }
}
```

## How to do it

Start the TC with the reconfiguration runtime:

```bash
cargo run -- \
  --runtime reconf-semi-sync \
  <model> <input-options> [output-options]
```

For example, with MQTT input and output and custom reconfiguration topic `my-reconfig`:

```bash
cargo run -- --runtime reconf-semi-sync \
  --reconf-topic my-reconfig examples/simple_add.dsrv \
  --mqtt-input --mqtt-output
```

After the TC is running, publish a reconfiguration command to the configured reconfiguration topic. The TC will parse the new specification, rebuild the relevant input and output handlers, and continue execution with the updated monitor.

## Development status

- Basic reconfiguration: Monitor can switch to a new specification without restarting the process

- ✅: Runtime reconfiguration through `--runtime reconf-semi-sync`
- ✅: Configurable reconfiguration topic through `--reconf-topic`
- ✅: ROS2 reconfiguration -- requires `type_info` since ROS2 messages are typed
- ✅: MQTT reconfiguration
  - ⚠️: No custom topic mapping yet, topic naming based on stream names
- ✅: Redis reconfiguration
  - ⚠️: No custom topic mapping yet, topic naming based on stream names
- ⚠️: A short delay occurs during reconfiguration due to reconnecting to external services. (WIP)
- ❌: Context transfer to preserve as much of the trace context as possible across reconfiguration
- ❌: File-based reconfiguration
