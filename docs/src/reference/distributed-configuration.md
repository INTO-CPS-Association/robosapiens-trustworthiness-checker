# Distributed configuration reference

Distributed monitoring assigns parts of one DSRV specification to named locations and exchanges the required intermediate values through a transport. Use a static distribution graph for a predefined assignment; scheduler-backed modes are separate live deployments and need their own transport, feature, and work-assignment setup.

## Static graph

Pass a JSON5 graph with `--distribution-graph PATH` and select one local node with `--local-node NAME`:

```json5
{
  dist_graph: {
    central_monitor: 1,
    graph: {
      nodes: ["A", "B"],
      edge_property: "directed",
      edges: [[0, 1, 0]],
    },
  },
  var_names: ["x", "y", "z", "w", "v"],
  node_labels: {
    "0": ["w"],
    "1": ["v"],
  },
}
```

The node indexes in `node_labels` refer to the `graph.nodes` order. The labels identify computed streams assigned to each node; source route ownership remains a local input configuration concern.

The distributed addition example computes an intermediate sum `w`, then uses it in final sum `v`:

```dsrv
in x
in y
in z
in c
out w
out v

w = x + y
v = z + w
```

The repository stores it as `examples/simple_add_distributable.dsrv`. Run node A's local assignment with the included graph and input:

```sh
cargo run -- examples/simple_add_distributable.dsrv \
  --input-file examples/simple_add.input \
  --output-stdout \
  --distribution-graph examples/simple_add_distribution_graph.json \
  --local-node A
```

The checked-in graph assigns `w` to `A`; the local stdout is:

```text
w[0] = Int(3)
w[1] = Int(7)
```

This verifies the local graph projection only. It does not exercise cross-process MQTT exchange.

## Scheduler-backed modes

The distribution flags select different builders:

| Mode | CLI option | Extra input |
|---|---|---|
| MQTT centralised distributed | `--mqtt-centralised-distributed NODE...` | Node locations; MQTT transport. |
| MQTT randomized distributed | `--mqtt-randomized-distributed NODE...` | Node locations; scheduler assigns a random labelling. |
| MQTT static optimized | `--mqtt-static-optimized NODE...` | `--distribution-constraints VAR...`; optional `--dist-constraint-solver brute-force|sat`. |
| MQTT dynamic optimized | `--mqtt-dynamic-optimized NODE...` | Distribution constraints and scheduler state. |
| ROS variants | `--ros-centralised-distributed`, `--ros-randomized-distributed`, `--ros-static-optimized`, `--ros-dynamic-optimized` | `--features ros`; ROS distribution graph topic defaults to `/dist_graph`. |
| Scheduler work flag | `--distributed-work` | Parsed and requires `--local-node`, but the current runtime builder does not consume it to establish a wait-for-assignment contract. Do not rely on it operationally. |

`--centralised` is the default distribution selection. `--local-topics` supplies local topics for the relevant distributed modes. `--scheduling-mode` defaults to `mock`; `ros` uses the ROS scheduler communicator and requires ROS support. `--scheduler-ros-node-name` defaults to `tc_scheduler`, and `--scheduler-reconf-topic` defaults to `reconfig`.

The `sat` solver value requires the `sat` Cargo feature. The optimized modes require distribution constraints; parser acceptance alone does not start a scheduler or prove a complete distributed run. In particular, the generated CLI row for `--distributed-work` reproduces its current Clap help, while the adapter path provides no corresponding waiting behavior.

## Ordering and completion

Each node builds its own input and output routes. Distributed transports do not provide a global observation order, cross-node transaction, or rollback. A local output admission or flush is not proof that every remote node or destination consumed the value.

Start with [Run static distributed monitoring](../tutorials/distributed-monitoring.md) for the node-A addition walkthrough. The deeper assignment and runtime ownership material is in [dataflow architecture](../architecture/dataflow/index.md).
