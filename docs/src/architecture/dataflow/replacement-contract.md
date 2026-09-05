# Replacement identity

Replacing a monitor definition raises one question that nothing else in the layer has to answer: when two definitions both contain a stream called `total`, is it *the same* `total`? State survives replacement only when the target can prove it is, and this page is about what that proof consists of.

Schedule positions, physical route positions, and coincidental storage indices are not answers. They describe where something currently sits, not what it means.

{{#include ../../assets/dataflow/environment-layout.svg}}

**Reading rule.** Stable stream and environment identities select state and row locations. Scheduler and output projections can order those identities differently without redefining them, so a position cannot serve as an identity.

## Two kinds of key

The identities divide by how equality is decided.

**Structural keys** are fingerprints of the compiled body. `StreamStateKey` and `DefinitionKey` are 128-bit values derived from the canonical graph descriptor, so two of them are equal precisely when the compiled bodies are semantically identical — same operations, same operands, same resolved variable names. Renaming an intermediate, reordering equations, or changing a schedule leaves them equal. Changing what the stream computes does not.

**Positional keys** identify a location. `ExpressionStateKey` is literally `stream/owner/occurrence`: the third `dynamic` in stream `alert` is the same occurrence across two definitions because it sits in the same place, regardless of what source text it happens to be running. A reconfiguration point is identified this way because its source text is expected to change while the point itself persists.

| Identity | Scope | Two are equal when |
|---|---|---|
| `DefinitionKey` | complete compiled definition | the whole definition fingerprints identically |
| `StreamStateKey` | state-bearing semantics of one computed stream | the stream's compiled body fingerprints identically |
| `ExpressionStateKey` | one nested reconfiguration occurrence | the same stream, owner, and occurrence index |
| `StreamId` | logical computed stream within one monitor | same monitor, same index — never compared across definitions |
| `EnvironmentSlot` | current-row location within one definition | same definition, same slot |
| `NodeId` | operation and its state slot within one graph | same graph, same position |
| `PlanId` | schedule-specific physical route | never a state identity at all |
| `MonitorRevision` | accepted root or nested activations | monotonic counter, not an identity |
| `InterfaceRevision` | effective input/output interface activations | monotonic counter, not an identity |

`PlanId`, `MonitorRevision` and `InterfaceRevision` are listed to rule them out as state identities. `PlanId` is deliberately outside the semantic domains: a route can be replaced while every evaluator identity persists, which is the subject of [runtime ownership](runtime-ownership.md#replaceable-routing). The two revisions record that something was accepted; they never decide whether two things are the same.

`StreamId`, `EnvironmentSlot` and `NodeId` are stable *within* one definition, where execution uses them to locate state and row positions, and meaningless across two. A replacement never compares them.

## Cross-definition mapping

`ReconfigurationMapping::between` is indexed by the target definition, and asks for each target stream whether some source stream can supply its state. A computed stream maps when variable identity and `StreamStateKey` both agree; environment values map by variable identity and compatible type.

Because the key is structural rather than positional, this permits stream reordering without moving any state: a definition that declares the same equations in a different order produces the same fingerprints, so every stream maps and nothing is rebuilt. Conversely a stream whose body changed at all is a different stream by this rule, and starts cold — there is no partial match.

Added or semantically changed target streams remain unmapped and start cold. Removed source state has no target owner and is dropped. Mapping validation checks target completeness, source bounds, source injectivity, names, and state keys **before** any destructive transfer begins, which is what makes the later movement infallible.

## Implementation mapping

- identity keys, revisions, and their construction: `src/dataflow/reconfiguration.rs`;
- the canonical descriptor those fingerprints are taken over: `src/dataflow/ir.rs`;
- target-indexed mapping and its validation: `src/dataflow/reconfiguration_mapping.rs`;
- complete definition keys: `src/dataflow/program.rs`.

Focused tests cover reordered streams, exact matches, changed streams, mapping validation, domain separation, and revision overflow.

Continue with [context transfer](context-transfer.md) for what actually moves once identity has been established, or [root cutover](reconfigurable-runtime.md) for the order the surrounding replacement applies in.
