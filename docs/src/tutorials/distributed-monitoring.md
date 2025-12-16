# Distributed Monitoring

This tutorial explains how to deploy the RoboSAPIENS Trustworthiness Checker across multiple nodes in a distributed system.

## Overview

Distributed monitoring allows you to split a DynSRV specification across multiple computing nodes, enabling:

- **Scalability**: Process large specifications by distributing computation
- **Resilience**: Continue partial monitoring even if some nodes fail
- **Network Efficiency**: Process data closer to its source

## Architecture

In distributed monitoring:

1. A **distribution graph** defines which streams are computed on which nodes
2. Each **local node** runs an instance of the monitor
3. Nodes communicate via **MQTT** to exchange intermediate results
4. The specification is split automatically based on the distribution graph

```
┌─────────────┐         ┌─────────────┐
│   Node A    │◄───────►│   Node B    │
│             │  MQTT   │             │
│ x, y → w    │         │ z, w → v    │
└─────────────┘         └─────────────┘
      ▲                       ▲
      │                       │
      └───────────┬───────────┘
                  │
            MQTT Broker
```

The distributed monitoring system can be used in multiple configurations:
1. Static: nodes run independently and are assigned work following a predefined *distribution graph* which labels each node with a work assignment consisting of a list of streams from the specification.
2. Centralised, random: the nodes run at each location and an additional centralised node assigns random work to each node.
3. Centralised, constraints: the nodes run at each location and an additional centralised node generates a work assignment which optimised for a number of *distribution constraints* which limit where each stream of the specification 

## Static distribution

In this section we will set up a static distribution of a specification across two nodes. 

### Step 1: Create the Specification

**File**: `simple_add_distributable.lola`

```ocaml
input x: Int
input y: Int
input z: Int

output w := x + y
output v := w + z
```

This specification:
- Takes inputs `x`, `y`, `z`
- Computes intermediate `w = x + y`
- Computes final `v = w + z`

We'll split this so Node A computes `w` and Node B computes `v`.

### Step 2: Create the Distribution Graph

**File**: `simple_add_distribution_graph.json`

```json
{
  "nodes": {
    "A": {
      "streams": ["x", "y", "w"]
    },
    "B": {
      "streams": ["z", "w", "v"]
    }
  },
  "edges": [
    {
      "from": "A",
      "to": "B",
      "streams": ["w"]
    }
  ]
}
```

**Explanation**:
- **Node A** handles streams `x`, `y`, and computes `w`
- **Node B** handles stream `z` and computes `v` (needs `w` from Node A)
- **Edge** A→B indicates `w` is sent from Node A to Node B via MQTT

### Step 3: Start the Nodes

**Terminal 1 (Node A)**:

```bash
cargo run --release -- examples/simple_add_distributable.lola \
  --mqtt-input --mqtt-output \
  --distribution-graph examples/simple_add_distribution_graph.json \
  --local-node A
```

**Terminal 2 (Node B)**:

```bash
cargo run --release -- examples/simple_add_distributable.lola \
  --mqtt-input --mqtt-output \
  --distribution-graph examples/simple_add_distribution_graph.json \
  --local-node B
```

### Step 4: Send Inputs

Using MQTT Explorer or `mosquitto_pub`:

```bash
# Send inputs for Node A
mosquitto_pub -t x -m '1'
mosquitto_pub -t y -m '2'

# Send input for Node B
mosquitto_pub -t z -m '3'
```

### Step 5: Observe Results

```bash
# Node A computes w
mosquitto_sub -t w
# Output: {"value": 3}

# Node B computes v
mosquitto_sub -t v
# Output: {"value": 6}
```

**What happened**:
1. Node A received `x=1` and `y=2`, computed `w=3`
2. Node A published `w=3` to MQTT
3. Node B received `z=3` and `w=3` (from MQTT), computed `v=6`
4. Node B published `v=6` to MQTT

## Dynamic distribution

In this section we will set up a dynamic distribution of a specification based on a central coordination node. 

This will use the same specification as before (`simple_add_distributable.lola`) but with a dynamic distribution graph.

### Step 1: Create the Specification

**File**: `simple_add_distributable.lola`

```ocaml
input x: Int
input y: Int
input z: Int

output w := x + y
output v := w + z
```

This specification:
- Takes inputs `x`, `y`, `z`
- Computes intermediate `w = x + y`
- Computes final `v = w + z`

We'll split this so Node A computes `w` and Node B computes `v`.

### Step 2: Start the local Nodes

**Terminal 1 (Node A)**:

```bash
cargo run --release -- examples/simple_add_distributable.lola \
  --mqtt-input --mqtt-output \
  --distributed-work \
  --local-node A
```

**Terminal 2 (Node B)**:

```bash
cargo run --release -- examples/simple_add_distributable.lola \
  --mqtt-input --mqtt-output \
  --distributed-work \
  --local-node B
```

This uses different arguments compared to the static deployment:
 - The `--distributed-work` argument enables distributed work assignment, so the node will wait to be sent a `start_monitors_at_<node name>` message before it starts monitoring.
 
### Step 5: Central Node

#### Option A: Mock Central Node

To test the local nodes without starting the central node, you can send the following MQTT messages.

Using MQTT Explorer or `mosquitto_pub`:
```bash
# Send inputs for Node A
mosquitto_pub -t start_monitors_at_A -m "[w]"

# Send input for Node B
mosquitto_pub -t start_monitors_at_B -m "[v]"
```

#### Option B: Randomized Central Node

**Note:** This option is under development and may not yet work reliably.

To automatically distribute work randomly across nodes, start the central coordinator:

**Terminal 3 (Central Coordinator)**:

```bash
cargo run --release -- examples/simple_add_distributable.lola \
  --mqtt-input --mqtt-output \
  --mqtt-randomized-distributed A B
```

The coordinator will randomly assign streams to the specified nodes (A, B) and send `start_monitors_at_<node>` messages.

#### Option C: Constraint-based Central Node
**Note:** This option is under development and may not yet work reliably.

For optimized distribution based on constraints, create a specification with distribution constraints and start the coordinator:

**File**: `simple_add_dist_constraints.lola`

```ocaml
in x
in y
in z
out w
out v
w = x + y
v = z + w

can_run w: source(x) || source(y)
locality w: dist(source(x)) + dist(source(y))

can_run v: source(z) && monitor(w)
locality v: dist(source(z)) + dist(monitor(w))
```


**Terminal 3 (Central Coordinator)**:

```bash
cargo run --release -- examples/simple_add_dist_constraints.lola \
  --mqtt-input --mqtt-output \
  --mqtt-static-optimized A B \
  --distribution-constraints w v
```

The coordinator analyzes the constraints for streams `w` and `v`, computes optimal assignments to minimize locality scores while satisfying `can_run` conditions, and distributes work to nodes A and B. Use `--mqtt-dynamic-optimized` for dynamic reconfiguration.

### Step 4: Send Inputs

Using MQTT Explorer or `mosquitto_pub`:

```bash
# Send inputs for Node A
mosquitto_pub -t x -m '1'
mosquitto_pub -t y -m '2'

# Send input for Node B
mosquitto_pub -t z -m '3'
```

### Step 5: Observe Results

```bash
# Node A computes w
mosquitto_sub -t w
# Output: {"value": 3}

# Node B computes v
mosquitto_sub -t v
# Output: {"value": 6}
```

**What happened**:
1. Node A received `x=1` and `y=2`, computed `w=3`
2. Node A published `w=3` to MQTT
3. Node B received `z=3` and `w=3` (from MQTT), computed `v=6`
4. Node B published `v=6` to MQTT

## Distribution Graph Format

The distribution graph is a JSON file defining the topology.

### Structure

```json
{
  "nodes": {
    "node_name": {
      "streams": ["stream1", "stream2", ...]
    }
  },
  "edges": [
    {
      "from": "source_node",
      "to": "destination_node",
      "streams": ["stream1", ...]
    }
  ]
}
```

### Fields

**`nodes`**: Object mapping node names to their configuration
- **`streams`**: Array of stream names this node is responsible for
  - Includes: inputs, outputs, and intermediate streams computed by this node

**`edges`**: Array of connections between nodes
- **`from`**: Source node name
- **`to`**: Destination node name
- **`streams`**: Array of streams sent from source to destination

## Distribution constraint syntax

Distribution constraints follow the following syntax:

- `can_run <var>: <bool_expr>` - Boolean condition: when can this stream be computed at a node?
- `locality <var>: <numeric_expr>` - Numeric score: lower is better (minimizes network traffic)
- `source(x)` - True if input `x` originates at this node
- `monitor(w)` - True if output `w` is computed at this node
- `dist(target)` - Network distance to target

## Next Steps

- Explore [Lola Specifications](../advanced/lola-specifications.md) for complex distributed properties
- Learn about [Configuration](../reference/configuration.md) options for distributed systems
- See [CLI Options](../reference/cli-options.md) for advanced deployment
