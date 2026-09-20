# Runtime capabilities

Not every DSRV runtime evaluates every construct. A runtime refuses a
specification that needs something it does not support before it builds any
stream, naming the first such construct:

```text
Error: `monitored_at` at Span { start: 105, end: 127 } cannot run on the semi-sync runtime, which does not support distribution; see "Runtime capabilities" in the documentation
```

| Capability | Constructs |
|---|---|
| distribution | `dist` and `monitored_at`, which also need `language distributed` |

The table below is produced by running each runtime against a specification
for each capability, under every `--semantics` value, so it records what this
build does:

<!-- runtime-capabilities:start -->
| Runtime | distribution | tagged unions | pattern matching |
|---|---|---|---|
| `async` | no | yes | yes |
| `dataflow` | no | yes | yes |
| `distributed` | not checked automatically (needs distribution settings) | not checked automatically (needs distribution settings) | not checked automatically (needs distribution settings) |
| `semi-sync` | no | yes | yes |
| `reconf-semi-sync` | not checked automatically (needs an input pipeline) | not checked automatically (needs an input pipeline) | not checked automatically (needs an input pipeline) |
| `reconf-dataflow` | not checked automatically (needs an input pipeline) | not checked automatically (needs an input pipeline) | not checked automatically (needs an input pipeline) |
<!-- runtime-capabilities:end -->

Text supplied while a specification runs, through `dynamic` or `defer`, is
not checked in advance: a construct the runtime does not support stops it
while it runs.

## For contributors

- `core::Capability` lists the capabilities, and
  `src/lang/dsrv/ast/requirements.rs` says which expression needs which. That
  match has no wildcard, so a new expression kind must be placed.
- Each evaluator declares what it supports where it is implemented: every
  `MonitoringSemantics` has a `CAPABILITIES` constant, and each runtime calls
  `core::admit` with its evaluator's declaration at its own entry point.
- `tests/runtime_capabilities.rs` runs every runtime it can start from files
  and fails on a panic, a timeout or a failure other than the refusal, and on
  any difference from this table. Regenerate the table with
  `CAPABILITY_TABLE=overwrite cargo test --test runtime_capabilities`.
