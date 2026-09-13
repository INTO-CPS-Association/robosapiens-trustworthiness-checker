# Synchronous monitor lifecycle

`DataflowProgram` is an immutable compiled definition that can be reused to
create independent `DataflowMonitor` sessions. This supports embedded callers
that evaluate many traces without repeating DSRV parsing, type checking, or
Dataflow compilation.

## Ownership

`DataflowProgram` is a shallow-clone handle. Its definition data is shared
through `Rc`, while every monitor owns its mutable runtime state:

- input, computed, and retained environment values;
- temporal histories and delayed writes;
- lazy and reconfigurable evaluator state;
- expression activation and scheduler state;
- monitor and interface revisions;
- failure and staged-tick state;
- quickened plans, caches, counters, and integrated JIT state.

The handle is cheap to clone within one thread. It does not add a `Send` or
`Sync` guarantee. Creating a monitor still allocates its private execution
arena, scheduler, and histories.

## Creating independent monitors

Retain the compiled program and clone its handle for each monitor:

```rust
{{#include ../../../../tests/docs_examples.rs:dataflow_program_lifecycle}}
```

Each monitor starts from the same definition but has independent temporal and
execution state. Evaluating or resetting one monitor cannot affect another.

For integrated JIT execution, use `DataflowMonitor::from_program_with_jit`.
The selected JIT activation policy belongs to the monitor configuration, not
the shared compiled program.

## Evaluating traces

`DataflowMonitor::evaluate_trace` accepts an iterator of input rows and appends
one complete output row for each successful logical tick. Rows contain the
existing runtime-tagged `Value` type and follow `input_vars()` and
`output_vars()` order.

The method preserves every output cell, including `NoVal` and `Deferred`.
Application timestamps are ordinary input values: equal timestamps on
successive rows remain distinct ticks because call order establishes logical
time.

The trace helper is an ordered loop over `DataflowMonitor::evaluate`:

- existing rows in the caller's output vector are retained;
- zero-output ticks append empty rows;
- the first evaluation error is returned unchanged;
- successful rows before an error remain available;
- no later input row is consumed after an error;
- exhausting the iterator does not add a final tick or resolve pending state;
- evaluation does not reset the monitor implicitly.

Call `evaluate` directly with a reusable output buffer when accumulating an
entire output timeline would use too much memory.

## Reset

`DataflowMonitor::reset` ends the current execution session and reconstructs
the monitor from its original compiled program. It uses the same initialization
path as a fresh monitor rather than trying to clear individual caches or
histories.

Reset restores:

- initial environment and retained values;
- initial temporal histories and history depths;
- initial lazy, dynamic, and deferred-expression state;
- initial activation and dependency scheduling;
- the original root definition and interface;
- initial monitor and interface revisions;
- a healthy monitor with no staged tick;
- cold quickening and integrated JIT state.

The latest selected quickening, integrated-JIT, and internal context-transfer
configuration is retained, but session-derived plans, artifacts, counters,
reports, replay records, and cached runtime expressions are discarded. An eager
JIT configuration may therefore compile native code again during reset.

Evaluation failures remain terminal until reset. Input or output arity errors
that fail before a tick begins retain their existing non-poisoning behavior.

## Reconfiguration

Dynamic and deferred expressions are supported because reset reconstructs their
initial activation, scheduler, retained environment, and history topology.

Root replacement changes the active definition only for the current session.
Reset returns to the program originally used to construct the monitor. A caller
that wants a replacement definition to become a new reset baseline must create
a new monitor from that compiled program.

Context transfer remains a separate state-preserving operation and is not used
to implement reset.

## Runtime boundaries

The lifecycle API applies to in-memory `DataflowMonitor` evaluation, including
canonical, quickened, and integrated-JIT `Value` execution under their existing
feature gates.

It does not add lifecycle control to:

- `TypedDataflowMonitor`, `TypedJitMonitor`, or extracted direct-JIT monitors;
- asynchronous, semi-synchronous, reconfigurable semi-synchronous, distributed,
  or MSTLO runtimes;
- MQTT, Redis, ROS, file, or other transport sessions.

The CLI retains its existing behavior. Its synchronous Dataflow mode remains an
external compatibility path rather than the embedded lifecycle API.
