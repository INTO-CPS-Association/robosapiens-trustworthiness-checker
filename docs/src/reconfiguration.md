# Reconfiguration Runtime

One of the novel features of the TC is the ability to reconfigure a running monitor at runtime. In practice, this means that the TC can receive an updated specification while it is already running and then rebuild the monitoring pipeline around the new specification.

This is useful when the property to be monitored changes during execution, for example because new streams become relevant, old streams are removed, or a different monitoring task should take over without restarting the full deployment.

## Supported implementations

Reconfiguration is provided by two separate supported implementations. They have distinct execution machinery and failure policies:

```text
--runtime reconf-dataflow       serial region-based dataflow runtime
--runtime reconf-semi-sync      independent semisynchronous runtime
```

`RuntimeSpec::ReconfDataflow(execution_policy)` and `RuntimeSpec::ReconfSemiSync` continue to select those separate implementations. `ReconfDataflow` carries the selected `ExecutionPolicy`; semisync is not moved onto the dataflow evaluator, scheduler, or failure policy.

## Semisync runtime

### How it works

The reconfigurable semi-sync runtime is built from a reusable `InputPipeline`,
not from a pre-opened ordinary `InputStream`. The pipeline keeps an owned local
source set, route catalogs, source ownership, and the optional input window. The
runtime opens one input generation at a time and uses a private control adapter
to listen for reconfiguration messages.

Ordinary `InputStream` values contain only `InputBatch` data. The control route
is not added as a fake model variable and does not pass through data mapping.
Internally, the adapter carries either a data batch or a terminal
`Reconfigure(MonitorConfig)` item. When a window is configured, ordinary and
reconfigurable inputs both use the same private window driver: ordinary input
has data events only, while the reconfigurable adapter adds the control event.
This keeps control-plane messages out of ordinary runtimes while giving both
paths the same batch, atomic-step, timer, and flush behavior.

A control message is a generation barrier:

1. Data accepted before the message is emitted normally.
2. The shared private window driver flushes pending batch or atomic-step state.
3. The private reconfiguration item is delivered and the window driver
   terminates.
4. No later data from the old source generation is polled or emitted.
5. The old input and runtime tasks are dropped before the replacement pipeline
   is opened.

The replacement specification is parsed and validated, its generation-local
input bindings are resolved against the reusable, owned local `InputSources`
set, output routes are updated, and the next generation starts. A replacement is rebuilt
even when its input and output sets have the same shape.

### Reconfiguration message format

Reconfiguration messages are JSON5. Standard JSON is accepted because it is a
subset of JSON5.

Each message must contain the new specification in `spec`. Input and output
routes use the same compact route form as route catalog files: a string route,
or a two-element array containing `[route, codec]`.

```json
{
  "spec": "in x: Int\nout z: Int\nz = x",
  "inputs": {
    "x": "/robot/input/x"
  },
  "outputs": {
    "z": "/robot/output/z"
  }
}
```

For ROS routes, the codec is the ROS message type required by the route:

```json
{
  "spec": "in x: Int\nout z: Int\nz = x",
  "inputs": {
    "x": ["/x", "Int32"]
  },
  "outputs": {
    "z": ["/z", "Int32"]
  }
}
```

In a single-source deployment, `inputs` can be omitted entirely. The runtime
then resolves the next specification's variables from the local source
catalog and default source:

```json
{
  "spec": "in x: Int\nout z: Int\nz = x"
}
```

With a named multi-source local source set, use `source` and `inputs` to make
one active source explicit. All active bindings in a reconfigurable generation
must use the selected control source; other configured source catalogs remain
inactive for that generation:

```json
{
  "spec": "in x: Int\nin y: Int\nout z: Int\nz = x + y",
  "source": "robot-mqtt",
  "inputs": {
    "x": "/robot/x",
    "y": "/robot/y"
  }
}
```

`inputs` and `sources` are alternatives. `source` may accompany `inputs` to
select one named source for all of those bindings. A `sources` object that
assigns active bindings to two source IDs is rejected before opening by a
reconfigurable runtime: it cannot establish a sound order between independent
source streams. If no explicit input routes are present, the owned local source
set supplies them from its catalogs and default, but the resolved bindings must
still all belong to the selected control source. `outputs` is optional and uses
the same compact route representation to override output routes for the new
generation.

For a manually authored message, put route and codec information inside the
compact `inputs`, `sources`, or `outputs` objects; the owned local source set
remains responsible for transport configuration.

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

Publish a compact `MonitorConfig` JSON5 payload to `my-reconfig`. The same
pattern works with `--redis-input` and its Redis route.

A named multi-source input configuration keeps source transport settings,
model-data route catalogs, and the fixed control route local to the monitor.
With multiple configured sources, exactly one source must declare
`reconfiguration_route` when the reconfigurable runtime is selected. The
selected source must also own every active model-data binding:

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

Here `robot-ros` is an additional configured but inactive source for this
reconfigurable generation. Binding `pose` from it together with `x` or `y`
would fail before any source is opened. Adding or removing streams remains
supported when the replacement resolves them from the selected source's
catalog (or explicitly binds them to that source).

Start the runtime with:

```bash
cargo run --features ros -- --runtime reconf-semi-sync \
  examples/simple_add.dsrv \
  --input-config examples/input-config.json \
  --output-stdout
```

`--reconf-topic` overrides the declared route but never changes the selected
source. A multi-source config with no declaration or more than one declaration
fails clearly. An empty catalog on the selected source is allowed only when the
generation has no model-data bindings; it cannot act as a control-only provider
for data owned by another active source. `--input-config` cannot be combined
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

Context transfer does not keep the old input source alive. The old generation is
dropped before the replacement source is opened, so old-generation data cannot
feed the new model.

### Source-local ordering and producer acknowledgements

A reconfigurable generation has one source-ownership domain: its active model
bindings and its control route are owned by the same `InputSource`/source ID.
The runtime rejects bindings that span source IDs, or data bindings that differ
from the selected control source, before transport setup. Extra configured
sources are allowed only while inactive for that generation. This validation
prevents an accidental cross-source merge; it does not itself establish an
order between data and control.

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
rows for the replacement generation. A broker publish acknowledgement, a
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
`DataflowMonitor`, one `ReconfigurableInputStream` generation, and one generation-specific
`OutputWriter`. The reusable `InputPipeline` and `OutputBackendBuilder` remain configuration; their
opened resources do not overlap. Root replacement is a stop-the-world configuration phase between
logical ticks: the old definition and its opened I/O are drained and closed, then the replacement is
installed in its place.

### Typed input barrier and acknowledgement

For `RuntimeSpec::ReconfDataflow`, the reusable `InputPipeline` is owned by
`ReconfigurableInput`. Opening a generation returns a typed `ReconfigurableInputStream` whose items
are either:

```text
ReconfigurableInputItem::Data(InputBatch)
ReconfigurableInputItem::Reconfigure(MonitorConfig)
```

The source adapter parses JSON5 and validates the `MonitorConfig` before yielding the typed control
item. The dataflow owner receives that typed config directly; no control variable is added to the model
interface. A data item keeps its normal logical-tick and packed-row representation.

The reserved control route is a **global input barrier**. If an input window is configured, its
barrier stage flushes all pending data items before forwarding the typed
`ReconfigurableInputItem::Reconfigure(MonitorConfig)` item and then terminates that input generation.
Post-control data from the old generation is not emitted. Data and control remain separate typed items
at this boundary.

The active model-data bindings and the control route must belong to one source. Input resolution
rejects a generation whose active bindings span multiple source IDs or whose data source differs from
the selected control source before any transport is opened. Other configured sources may remain
inactive. This source-ID check prevents a cross-source merge but does not turn independent
subscriptions into an ordered data/control stream. MQTT and Redis expose one backend item stream
that preserves the order observed by that transport client; ROS and manual input combine independent
subscriptions/fanouts and require an external controller to quiesce and acknowledge preceding data
before publishing control.

An acknowledgement contains the active semantic `RevisionId`, the active `InterfaceEpoch`, and
whether the command changed anything. It is sent only after the old output has been drained by
submitting pending packed rows and completing the old writer's `flush`/`close`, the replacement has
been validated and compiled, context transfer has completed if requested, the old input has been
dropped, the new input generation and output writer have opened, and the active monitor and runtime
state have been installed.

The acknowledgement confirms cutover and generation-open completion. It does not guarantee that a
future `OutputWriter::send`, backend flush/close, or remote transport publish cannot fail: later send
failures are terminal, with committed rows drained where the writer/backend permits. Producers must
wait for the acknowledgement before sending the next model row; added-input producers start after it,
and removed-input producers stop at or before the barrier. A bounded in-process acknowledgement
channel provides the runtime-to-library-controller delivery contract. CLI deployments do not install
this sink: they must use source/backend-specific external controller coordination, and this integration
does not define a new network acknowledgement protocol. The sink does not prove that a remote
controller has separately observed the message. No sleep, yield, `Poll::Pending` observation, or
control-poll priority substitutes for the required quiescence and acknowledgement contract.

### Root replacement sequence

The owner performs this sequence synchronously for a typed control item:

```text
typed Reconfigure(MonitorConfig)
→ submit pending rows as one packed OutputBatch
→ flush and close the old OutputWriter
→ validate the replacement frontier and config-derived interfaces
→ compile the replacement specification
→ transfer context, if requested
→ install candidate monitor identities
→ close and drop the old input generation
→ open and install the sole replacement input generation
→ resolve/open the generation-specific output interface and writer
→ install active input/output state and the replacement engine
→ publish ReconfigurationAck
```

`MonitorConfig` is already parsed before the item reaches the owner. The old output is therefore
submitted and drained through `OutputWriter::flush`/`close` before the owner validates, compiles,
transfers, opens, or installs the replacement configuration. `OutputBackendBuilder::resolve` computes
the candidate `ResolvedOutput` and destination `OutputInterface` without opening resources;
`OutputBackendBuilder::open` then opens the new generation's destinations and returns one writer,
cleaning up partial opens on failure. The owner performs no additional control decoding.

No success acknowledgement is sent before the old writer drain, replacement validation/compile/context
transfer, new input/output open, and state installation complete. No old row can enter the new mapping,
and no new row can overtake an old row. The acknowledgement is a cutover/open barrier, not a guarantee
that later backend or transport publishes will succeed.

Input and output replacement is always serial, including when the effective interfaces are unchanged.
The old input generation is explicitly dropped before the replacement input is opened, and the old
`OutputWriter` is closed before the replacement writer is opened. A failed input/output replacement,
source or validation error, acknowledgement delivery failure, later output send failure, or any other
root reconfiguration error terminates the runtime. The old monitor is not restored; cleanup drains rows
already accepted by the old writer where possible.

A semantic no-op still crosses the command barrier, consumes no logical tick, advances neither
identity, and does not transfer monitor state. Source formatting is normalized through the compiled
`DefinitionKey`, while unused mapping metadata is excluded from effective interface comparisons.

### Revision and interface accounting

`RevisionId` is semantic definition history. It advances only when the normalized monitor definition
changes. `InterfaceEpoch` is external binding/layout history for active model-data and output routes.
It advances when effective input/output membership or transport binding changes. The control route is a
source-local barrier route rather than a model input; its source compatibility is validated
before the generation opens. Thus semantic-only, interface-only, and combined changes advance
independently; an exact semantic and effective-interface no-op advances neither. Overflow is a
terminal internal error rather than a saturating reuse of an identity.

Root output rows are not filtered by revision. A nested replacement publishes its own successful row;
`RevisionId` is history, not an output-generation fence.

### Nested dynamic and defer replacement

Nested replacement happens at the existing source barrier, after prerequisites and before the owner
executes:

1. unchanged dynamic definitions take an allocation-free fast path;
2. a changed body is compiled into a local evaluator;
3. context is transferred before the body is installed;
4. the local body is installed once;
5. dependencies and the schedule are repaired; and
6. `RevisionId` advances immediately after each successful transfer/install.

A later failure leaves the poisoned monitor carrying the revisions for bodies already installed, but
publishes no row for the failed tick. The next evaluation returns `MonitorFailed`. `None` starts cold;
`Compatible` preserves safe semantic state and resets incompatible or ambiguous cells; `Strict` fails
when every required state owner cannot be transferred. Wrong-provenance history never transfers.

`defer` activates only on its first accepted definition, seals after a successful activation tick,
and releases its source only after temporal commit. Invalid dynamic/defer definitions, unsupported
nested reconfiguration, dependency cycles, schedule failures, and transfer failures are terminal
monitor failures rather than rejected candidates that can be retried.

### Context transfer

`DataflowContext` is a cold-path, portable snapshot of semantic evaluator state, retained values,
sealed regions, and active dynamic dependency bindings. External references remap by variable name,
not dense environment slot. JIT/native artifacts are derived physical state and do not define
identity; export is non-destructive to the active monitor. `Compatible` preserves unchanged delay
history across stateless edits where the semantic provenance is unambiguous, while `Strict` reports an
incompatible owner as an error.

### Transport limitations

Manual, ROS, MQTT, and Redis factories can carry the persistent control route subject to feature and
connection setup. File input is not supported for reconfiguration because reopening it would restart
the file session. MQTT and Redis present one backend item stream and preserve its observed
transport-local order, but do not invent an intrinsic order between independently published routes.
ROS and manual adapters combine independent subscriptions/fanouts, so their external controller
must quiesce and acknowledge preceding data before publishing control. The runtime does not infer
quiescence from `Poll::Pending`, sleeps, yields, or control-poll priority.
