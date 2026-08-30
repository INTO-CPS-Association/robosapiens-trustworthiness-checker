# Reconfiguration Runtime

One of the novel features of the TC is the ability to reconfigure a running monitor at runtime. In practice, this means that the TC can receive an updated specification while it is already running and then rebuild the monitoring pipeline around the new specification.

This is useful when the property to be monitored changes during execution, for example because new streams become relevant, old streams are removed, or a different monitoring task should take over without restarting the full deployment.

## Supported implementations

Reconfiguration is provided by two separate supported implementations. They have distinct execution machinery and failure policies:

```text
--runtime reconf-dataflow       serial in-place dataflow owner loop
--runtime reconf-semi-sync      independent semisynchronous runtime
```

`RuntimeSpec::ReconfDataflow(execution_policy)` and `RuntimeSpec::ReconfSemiSync` continue to select those separate implementations. `ReconfDataflow` carries the selected `ExecutionPolicy`; semisync is not moved onto the dataflow evaluator, scheduler, or failure policy.

## Semisync runtime

### How it works

The reconfigurable semi-sync runtime is built from a reusable `InputPipeline`,
not from a pre-opened ordinary `InputStream`. The pipeline keeps an owned local
source set, route catalogs, source ownership, and the optional input window. The
runtime opens one input session at a time and uses a private control adapter
to listen for reconfiguration messages.

Ordinary `InputStream` values contain only `InputBatch` data. The control route
is not added as a fake model variable and does not pass through data mapping.
Internally, the adapter carries either a data batch or a terminal-for-semi-sync
`Reconfigure(ReconfigurationRequest)` item. When a window is configured, ordinary and
reconfigurable inputs both use the same private window driver: ordinary input
has data events only, while the reconfigurable adapter adds the control event.
This keeps control-plane messages out of ordinary runtimes while giving both
paths the same batch, atomic-step, timer, and flush behavior.

A control message is a cutover barrier:

1. Data accepted before the message is emitted normally.
2. The shared private window driver flushes pending batch or atomic-step state.
3. The private reconfiguration item is delivered and the window driver
   terminates.
4. No later data from the old input session is polled or emitted.
5. The old input and runtime tasks are dropped before the replacement pipeline
   is opened.

The replacement specification is parsed and validated, its request-local
input bindings are resolved against the reusable, owned local `InputSources`
set, output routes are updated, and the replacement monitor starts. A replacement is rebuilt
even when its input and output sets have the same shape.

### Reconfiguration message format

Reconfiguration messages are JSON5. Standard JSON is accepted because it is a
subset of JSON5.

Each message contains exactly three fields: the new specification in
`specification`, plus optional nested `input` and `output` objects. Route values
inside those objects use the same compact route form as route catalog files: a
string route, or a two-element array containing `[route, codec]`.

```json
{
  "specification": "in x: Int\nout z: Int\nz = x",
  "input": {
    "inputs": {
      "x": "/robot/input/x"
    }
  },
  "output": {
    "outputs": {
      "z": "/robot/output/z"
    }
  }
}
```

For ROS routes, the codec is the ROS message type required by the route:

```json
{
  "specification": "in x: Int\nout z: Int\nz = x",
  "input": {
    "inputs": {
      "x": ["/x", "Int32"]
    }
  },
  "output": {
    "outputs": {
      "z": ["/z", "Int32"]
    }
  }
}
```

In a single-source deployment, `input` can be omitted entirely. The runtime
then resolves the next specification's variables from the local source
catalog and default source:

```json
{
  "specification": "in x: Int\nout z: Int\nz = x"
}
```

With a named multi-source local source set, use `input.source` and
`input.inputs` to make one active source explicit. All active bindings must use
the selected control source; other configured source catalogs remain inactive:

```json
{
  "specification": "in x: Int\nin y: Int\nout z: Int\nz = x + y",
  "input": {
    "source": "robot-mqtt",
    "inputs": {
      "x": "/robot/x",
      "y": "/robot/y"
    }
  }
}
```

Within `input`, `inputs` and `sources` are alternatives. `source` may accompany
`inputs` to select one named source for all of those bindings. A `sources`
object that assigns active bindings to two source IDs is rejected before opening
by a reconfigurable runtime: it cannot establish a sound order between
independent source streams. If no explicit input routes are present, the owned
local source set supplies them from its catalogs and default, but the resolved
bindings must still all belong to the selected control source. `output` is
optional and uses the same compact route representation to override output
routes for the replacement monitor.

For a manually authored message, put route and codec information inside the
compact `input` and `output` objects; the owned local source set remains
responsible for transport configuration.

### Control route and source configuration

For a single configured live source, that source is selected automatically; it
does not need a `reconfiguration_route` marker. Its control route is chosen in
this order for either reconfigurable runtime: CLI `--reconf-topic`, the source's
configured `reconfiguration_route`, or the built-in default `reconf` route:

```bash
cargo run -- --runtime reconf-semi-sync \
  examples/simple_add.dsrv \
  --mqtt-input \
  --reconf-topic my-reconfig \
  --output-stdout
```

Publish a compact `ReconfigurationRequest` JSON5 payload to `my-reconfig`. The same
pattern works with `--redis-input` and its Redis route.

A named multi-source input configuration keeps source transport settings,
model-data route catalogs, and the fixed control route local to the monitor.
With multiple configured sources, exactly one source must declare
`reconfiguration_route` when the reconfigurable runtime is selected. Other
sources may simultaneously own active model-data bindings:

```json
{
  "default": "robot-mqtt",
  "sources": {
    "robot-mqtt": {
      "kind": "mqtt",
      "host": "localhost",
      "reconfiguration_route": "monitor/reconfigure",
      "routes": {
        "x": "/robot/x",
        "y": "/robot/y"
      }
    },
    "robot-ros": {
      "kind": "ros",
      "routes": {
        "pose": ["/robot/pose", "Pose2D"]
      }
    }
  }
}
```

Here `robot-mqtt` carries the control route while `robot-ros` may concurrently
supply `pose`. The runtime composes both source streams. This observed order is
not a distributed total order, so a source move still requires producer
quiescence and acknowledgement.

Start the runtime with:

```bash
cargo run --features ros -- --runtime reconf-semi-sync \
  examples/simple_add.dsrv \
  --input-config examples/input-config.json \
  --output-stdout
```

`--reconf-topic` overrides the declared route but never changes the selected
source. A multi-source config with no declaration or more than one declaration
fails clearly. The selected source may act as a control-only provider while
other active sources own model data. `--input-config` cannot be combined
with another input-selection mode.

### Input windows

Input windows are configured with `--input-window-ms`,
`--input-window-update-limit`, and `--input-window-mode batch|atomic-step`.
The default mode when a bound is supplied is `batch`.

The update limit is a flush threshold and soft bound. The shared private driver
accepts each complete logical tick and flushes once the accumulated update count
reaches or exceeds the threshold; it never splits an atomic logical tick. A
wide tick can therefore make either mode's output exceed the nominal limit.

- A **batch** window accumulates data while preserving every logical tick and
  simultaneous boundary. Its crate-private storage may concatenate several
  physical segments.
- An **atomic-step** window reduces all ticks in the window to one simultaneous
  tick using last-update-wins for each variable.

Both modes flush pending data at end-of-stream and at a terminal
reconfiguration barrier. The atomic-step mode is deliberately stronger than
batching: it changes a window of independent updates into one evaluation step. File input
cannot be used with this runtime, and file atomic-step input has an additional
CLI update-limit requirement in ordinary runtimes.

### Context transfer

Context transfer is enabled by default. During replacement, retained history is
kept by variable identity for variables that still exist in the new model. The
histories are aligned to the longest retained history with `NoVal` on the left,
allowing compatible temporal context to survive changes to the specification.
Use `--no-context-transfer` when the replacement must start without prior
history.

Context transfer is independent of input ownership. Unchanged source streams
remain live. Removed streams drain their locally ready backlog through the old
monitor before matching state is transferred; replacement streams are opened
after that drain.

### Source-local ordering and producer acknowledgements

A reconfigurable session may compose multiple source-ownership domains. One
selected source carries control while any configured source may carry model
data. Composition establishes only the order observed by the local runtime; it
does not establish an intrinsic order between independent transports.

The backend then determines what order can be observed:

- **MQTT and Redis** expose one backend item stream containing the subscribed
  data and control routes. The adapter preserves the order observed by that
  transport client. This is transport-local observation, not an intrinsic
  data-before-control order between independently published topics/channels;
  producers must provide any stronger semantic ordering they need.
- **ROS** uses independent subscriptions for model-data topics and the control
  topic, and combines those streams. The ROS adapter therefore supplies no
  data/control ordering guarantee; a control topic colliding with an active data
  topic is rejected before ROS resources are opened.
- **Manual** input uses independent data and control fanouts. Those receivers
  also have no shared sequence or ordering edge.

For ROS and manual, an external controller must quiesce data production and
obtain an application/runtime acknowledgement that all preceding data has
crossed the required boundary before publishing control. It must wait for the
runtime's reconfiguration acknowledgement, where provided, before publishing
rows for the replacement session. A broker publish acknowledgement, a
second source, a quiet stream, `Poll::Pending`, a sleep or yield, or control-poll
priority is not a substitute for this contract.

Library callers can install the in-process `ReconfigurationAck` sink through
`ReconfigurableDataflowRuntimeBuilder::acknowledgements` or
`GeneralRuntimeBuilder::acknowledgements`. CLI deployments do not install that
sink and must instead use source/backend-specific external controller
coordination; this change does not define a new network acknowledgement
protocol. The in-process sink provides delivery to a library controller, not
proof that a remote controller has separately observed the message.

### Unsupported file reconfiguration

File input is a finite replay source and has no live control route. It is
supported by ordinary runtimes through `--input-file`, but it cannot be used
with either reconfigurable runtime:

```text
--input-file cannot be used with --runtime reconf-semi-sync
--input-file cannot be used with --runtime reconf-dataflow
```

In-memory row and tick sources likewise provide ordinary data only. The public
builder reports an actionable error if one is supplied to a reconfigurable
runtime. Use MQTT, Redis, ROS, or a manual library source with a configured
control channel for runtime reconfiguration. File-based reconfiguration is not
implemented.

## Serial in-place dataflow reconfiguration

The reconfigurable dataflow runtime has one persistent owner loop. At any instant it owns exactly one
`DataflowMonitor`, one `InputPipelineSession`, and one `OutputPipelineSession`. The reusable
`InputPipeline` and `OutputBackendBuilder` remain configuration. Root reconfiguration is serial and
plans both pipeline transitions before application. An unchanged input/output plan leaves the live
sessions untouched; input changes retain unchanged source streams and use break-drain-make for
changed owners, while fixed output owners flush and update their bindings/interfaces in place. Unsupported output interface updates are
terminal rather than falling back to a replacement.

### Typed input barrier and acknowledgement

For `RuntimeSpec::ReconfDataflow`, the reusable `InputPipeline` is owned by
`ReconfigurableInput`. Opening a session returns a typed `ReconfigurableInputStream` whose items
are either:

```text
ReconfigurableInputItem::Data(InputBatch)
ReconfigurableInputItem::Reconfigure(ReconfigurationRequest)
```

The source adapter parses JSON5 into a `ReconfigurationRequest` before yielding the typed control
item. The dataflow owner receives that request directly; no control variable is added to the model
interface. A data item keeps its normal logical-tick and packed-row representation.

The reserved control route is a **global input barrier**. If an input window is configured, its
barrier stage flushes all pending data items before forwarding the typed
`ReconfigurableInputItem::Reconfigure(ReconfigurationRequest)` item. The live input stream then
continues, so post-control data can be delivered after cutover. Data and control remain separate typed
items at this boundary; the control item is not an end-of-stream marker.

Active model-data bindings may span multiple source IDs. Exactly one source owns the control route,
and the input session composes all resolved sources while retaining their ownership identities.
Independent subscriptions still require an external controller to establish any ordering stronger
than the runtime's observed item order.

`plan_runtime_reconfiguration` owns concrete `InputPipelineReconfigurationPlan` and
`OutputPipelineReconfigurationPlan` values for the request, each built from a complete resolved
candidate. Planning opens no resource, so a rejected request leaves the active runtime untouched.
An unchanged plan does no I/O work; output destination owners remain the durable registry declared by
the pipeline.

An acknowledgement contains `monitor_changed`, `interface_changed`, `monitor_revision`, and
`interface_revision`. It is sent only after planning, pending engine rows have been submitted, the
input/output plans have been applied, and the monitor plan has been applied. Removed input sources
have drained and additions have opened, while changed output owners have been flushed and updated in place.

The acknowledgement confirms that the cutover and the replacement open completed. It does not guarantee that a
future `OutputWriter::send`, backend flush/close, or remote transport publish cannot fail: later send
failures are terminal, with committed rows drained where the writer/backend permits. Producers must
wait for the acknowledgement before sending the next model row; added-input producers start after it,
and removed-input producers stop at or before the barrier. A bounded in-process acknowledgement
channel provides the runtime-to-library-controller delivery contract. CLI deployments do not install
this sink: they must use source/backend-specific external controller coordination, and this integration
does not define a new network acknowledgement protocol. The sink does not prove that a remote
controller has separately observed the message. No sleep, yield, `Poll::Pending` observation, or
control-poll priority substitutes for the required quiescence and acknowledgement contract.

### Root reconfiguration sequence

The owner performs this sequence synchronously for a typed control item:

```text
ReconfigurableInputItem::Reconfigure(ReconfigurationRequest)
→ plan_runtime_reconfiguration: compile DataflowProgram, plan monitor, resolve and plan I/O
→ submit pending engine rows
→ apply the input plan at the ordered input barrier
→ flush changed output owners (or one shared stage) and update their interfaces/routing
→ apply MonitorReconfigurationPlan and rebuild the monitor layout
→ publish ReconfigurationAck
```

The request is already parsed before the item reaches the owner. `plan_runtime_reconfiguration` is
resource-free: it validates the request, compiles the immutable `DataflowProgram`, resolves the
complete target input/output interfaces, and selects `RetainExact`, `InstallCold`, or `Transfer`.
`OutputBackendBuilder::resolve` and the input resolver open no resources.

Applying the monitor plan materializes `DataflowMonitor::from_program(target)` only for
`InstallCold` and `Transfer`. `RetainExact` keeps the healthy live monitor without target
materialization or monitor context mapping. For `Transfer`, `ReconfigurationMapping` was made from
the immutable source and target programs before target construction, and the target destructively calls
`DataflowMonitor::context_transfer_from`.

Pending engine rows are submitted before plan application, and changed output owners are flushed
before their interfaces/routing are updated. A success acknowledgement is not sent until both
pipeline plans and the monitor plan are in place. Planning errors do not mutate the monitor. Once
application begins there is no rollback: any later error, including a failed acknowledgement,
terminates the runtime through its terminal cleanup policy.

### Monitor and interface revision accounting

`MonitorRevision` records successful monitor and nested semantic installation history. An accepted
root request advances it once even when the target `DefinitionKey` is exact and
`monitor_changed` is false. A nested body change advances it as well. `InterfaceRevision` records
effective external input/output binding history and advances only when those bindings change. The
control route is a source-local barrier route rather than a model input. Overflow is a terminal
internal error rather than a saturating reuse of an identity.

Root output rows are not filtered by revision. A nested expression reconfiguration publishes its own successful row;
`MonitorRevision` is history, not an output fence.

### Nested dynamic and defer expression reconfiguration

Nested expression reconfiguration happens at the existing source barrier, after prerequisites and before the owner
executes:

1. unchanged dynamic definitions take an allocation-free fast path;
2. a changed body is compiled into a local evaluator;
3. context is transferred before the body is installed;
4. the local body is installed once;
5. dependencies and the schedule are repaired; and
6. `MonitorRevision` advances immediately after each successful transfer/install.

A later failure leaves the poisoned monitor carrying the revisions for bodies already installed, but
publishes no row for the failed tick. The next evaluation returns `MonitorFailed`. `None` starts cold;
`Compatible` preserves safe semantic state and resets incompatible or ambiguous cells; `Strict` fails
when every required state owner cannot be transferred. Wrong-provenance history never transfers.

`defer` activates only on its first accepted definition, seals after a successful activation tick,
and releases its source only after temporal commit. Invalid dynamic/defer definitions, unsupported
nested reconfiguration, dependency cycles, schedule failures, and transfer failures are terminal
monitor failures rather than rejected candidates that can be retried.

### Context transfer

Root transfer first builds a fallible `PreparedContextTransfer`, then performs a cold-path, destructive
`DataflowMonitor::context_transfer_from` handoff of semantic evaluator owners, retained values, sealed
expressions, active dynamic dependencies, and live bounded histories. `ReconfigurationMapping` maps
environment state by variable identity and stores target-indexed stream correspondence plus executable
node-owner moves; it is created before the target monitor is materialized. `Compatible` preserves exact
owners, rebuilds safe compatible owners, and initializes new or ambiguous owners. `Strict` rejects
incompatible owners. JIT coordinator activation, fused artifacts, schedule-wide replay state, and schedule-specific routes remain target-owned. Required native state is materialized before transfer; exact matches move canonical and quickening state while retaining target-bound native artifacts, and compatible transfer rewrites canonical owners and rebuilds or synchronizes derived tiers.

An exact transferred active body keeps the nested layout against which it was compiled. Preparation
builds an `EnvironmentProjection` from nested slots to current outer slots and projects its scheduler
and history requirements by variable identity. Each monitor owns the resulting `HistoryStore`; matching
live histories move through the environment correspondence and are resized to the target effective
depth, so a deeper target does not invent older samples and a shallower target keeps only its visible
suffix.

### Transport limitations

Manual, ROS, MQTT, and Redis factories can carry the persistent control route subject to feature and
connection setup. File input is not supported for reconfiguration because reopening it would restart
the file session. MQTT and Redis present one backend item stream and preserve its observed
transport-local order, but do not invent an intrinsic order between independently published routes.
ROS and manual adapters combine independent subscriptions/fanouts, so their external controller
must quiesce and acknowledge preceding data before publishing control. The runtime does not infer
quiescence from `Poll::Pending`, sleeps, yields, or control-poll priority.
