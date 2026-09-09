# Run static distributed monitoring

## Outcome

Run a two-stage addition program assigned across two named locations. The distribution graph assigns intermediate sum `w` to node `A` and final sum `v` to node `B`, with a directed `A` to `B` edge.

This walkthrough evaluates node A's local assignment from a timestamped input file and writes `w` to stdout. It is not a cross-process broker deployment; dynamic scheduling, ROS, SAT, and container-backed paths are not required.

## Prerequisites

Run from the repository root with Rust/Cargo available. The DSRV program first adds `x` and `y`, then adds that intermediate result to `z`:

```dsrv
in x: Int
in y: Int
in z: Int
in c: Any
out w: Int
out v: Int

w = x + y
v = z + w
```

The repository stores the program as `examples/simple_add_distributable.dsrv`, the node assignment as `examples/simple_add_distribution_graph.json`, and the input values as `examples/simple_add.input`. No ROS feature or broker setup is needed for the node-A walkthrough.

## Start node A

```bash
cargo run -- \
  examples/simple_add_distributable.dsrv \
  --input-file examples/simple_add.input \
  --output-stdout \
  --distribution-graph examples/simple_add_distribution_graph.json \
  --local-node A
```

`--distribution-graph` selects the predefined graph and `--local-node A` selects node A's localized projection. In the program shown above, node A owns `w`. The included input supplies two pairs of `x` and `y` values, so stdout is:

```text
w[0] = Int(3)
w[1] = Int(7)
```

The command exits after producing both local results. To exercise node B
separately, use an input trace that supplies its `z` and `w` dependencies; the
checked-in `simple_add.input` supplies neither a `z` value nor a cross-process
transport.

## Current distributed-runtime boundary

The scheduler-owned CLI mode using `--runtime distributed` currently panics before processing the same program, input, and graph with:

```text
Variable message types not set
```

It is therefore not presented as a successful runbook. This is an
implementation limitation, not a readiness condition. Dynamic work assignment and ROS/SAT scheduling require their feature-specific environments; this page does not give them basic copy/paste commands.

## Completion and semantics

The task is complete when the two `w` lines appear and the process exits. The
localized run's stdout is a local observation. Distributed transports do not create a cross-node transaction or a promised total observation order.

For output routing and destination completion limits, see
[output architecture](../output.md).
