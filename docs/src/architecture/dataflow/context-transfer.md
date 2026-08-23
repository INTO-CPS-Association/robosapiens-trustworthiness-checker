# Context transfer

[← Previous: The replacement contract](replacement-contract.md) · [Next: Failure and termination](failure-model.md) →

A replacement monitor is a different machine. Its evaluator arena is freshly allocated, its dense slot assignments are specific to its own definition, and its delay rings start empty. Without transfer, replacing a definition would restart every delay, forget every branch timeline, and reset every accumulated fold — even for streams the replacement did not touch.

Context transfer carries canonical semantic state from the outgoing monitor into the replacement. It is a cold-path operation performed once per replacement, between logical ticks, and it is deliberately conservative: state moves only where the replacement can prove it means the same thing.

## What a context contains

`DataflowContext` is a semantic snapshot, not a runtime handle graph:

| Field | Keyed by | Carries |
|---|---|---|
| `stream_evaluators` | `VarName` | One canonical `StreamEvaluator` per top-level stream. |
| `retained_values` | `VarName` | The retained outer environment row, if the monitor allocated one. |
| `sealed_regions` | `RegionAddress` | Which `defer` points had already sealed. |
| `dynamic_dependencies` | `RegionAddress` | The active same-tick dependencies of each live point, as variable names. |
| `revision` / `interface_epoch` | — | The identities the snapshot was exported from. |

Every collection is keyed by **name or region address, never by dense slot**. This is the property the whole mechanism rests on. A `StreamId` or `EnvironmentSlot` means something only relative to the layout that assigned it; carrying one across a definition boundary would silently reinterpret it as whatever the replacement happens to have allocated at that index.

The evaluator values are otherwise opaque. A context can only be applied through `import_context`, which re-validates program identity in the replacement's own terms before copying anything.

## Export happens between ticks

`export_context` fails with `TickInProgress` if a tick is in flight. Mid-tick state is not a coherent semantic snapshot: some streams have published for the current row and others have not.

Export also **materializes native state into the snapshot only**. The active monitor keeps its compiled artifacts, activation counter, and packed native state untouched, because at export time the replacement has not yet been proven viable. If compilation or validation subsequently fails, the active monitor is still fully able to continue — the snapshot is a copy, not a deoptimization of the running machine.

## Two ways state can move

Import considers each stream in the replacement independently and tries two mechanisms, in order.

### Exact transfer

`transfer_from` first requires `graphs_semantically_equal` — the bound graph *and* the environment layout must match. Only then is state copied.

An exactly transferred stream is fully equivalent to its predecessor, which is what qualifies it for the three follow-on restorations described below.

### Compatible transfer

`transfer_compatible_from` handles a stream whose graph has changed. The replacement owns a fresh graph, and only matching state cells are copied across. Stateless edits keep their history; new owners and reshaped owners stay cold.

Compatible transfer is available only when **the stream contains no reconfiguration points**. A stream holding a `dynamic` or `defer` point takes the exact path or nothing. The reason is provenance: a nested body's state belongs to whichever source text was active, and matching cells across a changed outer graph cannot establish that the nested owner is still the same owner. Reviving nested history under a different provenance is precisely the error the design refuses.

## Policies

| Policy | Behaviour | Use |
|---|---|---|
| `None` | Nothing transfers. Every stream is reported `Reset("policy=None")`. | Restart semantics; a clean machine per definition. |
| `Compatible` (default) | Exact transfer where possible, compatible transfer otherwise, cold start where neither applies. | Preserve as much history as can be justified. |
| `Strict` | Exact or compatible transfer must succeed for every existing stream; the first failure rejects the replacement. | Reject any replacement that would silently lose history. |

`Strict` fails *before* installation. For nested replacement the ordering is explicit in `replace_reconfiguration_point`: the body is compiled and transferred into a local value, and only a successful transfer results in installation. A strict failure therefore never leaves a partially transferred body visible.

Root transfer is attempted only when the `DefinitionKey` actually changed. A semantic no-op keeps the active monitor and performs no transfer at all — there is nothing to transfer *into*.

## What exact transfer additionally restores

Three things are restored only for exactly transferred streams, because only exact equivalence justifies them:

1. **Retained stream values.** Retained *input* values are restored by name for any input the replacement declares. Retained *stream* values are restored only for exactly transferred streams, since a changed stream's retained value would be a value of a different computation.
2. **Sealed `defer` regions.** A sealed point is restored only when its containing stream transferred exactly and the replacement still has a point at that address. A `defer` that sealed under a different definition of its containing stream re-arms rather than staying sealed.
3. **Active dynamic dependencies.** The recorded dependency variable names are re-resolved to slots in the replacement's own layout. A dependency naming a variable the replacement does not have is `IncompatibleDependencies` — a hard error, not a reset, because the scheduler would otherwise have no edge for an active nested body.

## What never transfers

| Not transferred | Why |
|---|---|
| Wrong-provenance delay history | A delay ring only means something relative to the operation that filled it. |
| JIT and native artifacts | Derived physical state, tied to a `PlanId` or program identity that no longer exists. |
| Quickening plans and deoptimization decisions | Rebuilt from the replacement's own graphs; a previous tier decision is not semantic. |
| Schedule order and plan caches | Routing over the old definition's stream identities. |
| `tick_in_progress` and other execution flags | Export is only valid between ticks, so there is nothing in flight to carry. |

The unifying rule: **semantic state transfers, derived state is rebuilt.** Anything a tier could reconstruct from the replacement's own programs is reconstructed rather than carried, which is also what keeps the replacement free to make different tier decisions.

## Reading a transfer report

`ContextTransferReport` carries three counters plus a portable entry per owner:

```text
transferred_streams  reset_streams  rejected_streams  entries[]
```

The counters preserve a lightweight API for logging — `replace_root` emits `transferred_streams` and `reset_streams` at info level. The entries carry the `RegionAddress` and `StateKey` needed to explain *which* owners reset and why, which is what makes a reset diagnosable rather than merely countable.

Reset reasons distinguish the two ordinary causes:

- `Reset("new owner")` — the outgoing definition had no stream by that name. Expected when a replacement adds a stream.
- `Reset("incompatible or ambiguous state")` — the stream exists in both, but neither transfer mechanism applied. Worth investigating: under `Compatible` it is silent history loss.

A failed transfer leaves some evaluators already holding copied state, so `import_context` sets `failed` on the replacement monitor and the caller discards the value.

[← Previous: The replacement contract](replacement-contract.md) · [Next: Failure and termination](failure-model.md) →
