# The replacement contract

[← Previous: The reconfigurable runtime](reconfigurable-runtime.md) · [Next: Context transfer](context-transfer.md) →

Root replacement and nested `dynamic`/`defer` replacement happen in different places, at different times, under different owners, with different failure policies. They nevertheless answer the same three questions: *what is being replaced*, *is this a safe moment*, and *is the replacement based on the definition that is actually running*.

`src/dataflow/reconfiguration.rs` is the one place where both paths agree on those answers. It is deliberately small, portable, cold-path metadata: installed execution continues to use dense `StreamId`, `EnvironmentSlot`, and `NodeId` values, and these types appear only while compiling a replacement, validating a frontier, and constructing transfer reports.

## Replacement is serial

A replacement is compiled into a local value, checked against the contract below, installed at one point, and discarded if any step fails. One definition is live at a time, and the vocabulary in this module describes exactly that: a target, a definition, a frontier, and the identities the result carries.

## Addressing semantic regions

A `RegionAddress` is a deterministic, source-independent owner address:

| Constructor | Form | Names |
|---|---|---|
| `RegionAddress::root()` | `root` | The whole monitor definition. |
| `RegionAddress::stream(var)` | `root/stream:<var>` | One top-level computed stream. |
| `RegionAddress::dynamic_body_owner(stream, owner, occurrence)` | `root/stream:<var>/dynamic:<owner>:<n>` | One `dynamic` or `defer` body position. |

Two properties matter more than the syntax.

**Active source text is intentionally absent from the address.** A body position keeps the same address across every replacement of its content. That is what allows a transfer report to say "this owner was reset" and a frontier to say "this owner has not executed yet" without either statement depending on which expression currently occupies it.

**Occurrence disambiguates equal owners.** Two `dynamic` points in one stream bound to the same source variable would otherwise be indistinguishable. The occurrence index is positional within that stream, so it is stable for a fixed outer definition — which is exactly the lifetime over which nested replacement operates.

The doc comment states the corresponding prohibition directly: addresses are strings suitable for diagnostics, transfer reports, and revision construction, and **must not be used for lookups on a stable evaluation tick**. The tick-hot path uses dense indices; mixing the two would put string hashing on the fast path and, worse, would make a cold-path naming convention load-bearing for evaluation.

## Semantic identity: `DefinitionKey`

`DefinitionKey` is a canonical semantic descriptor, retained as text alongside any digest a caller wants to compute.

Two properties are deliberate:

- **Equality never relies on a collision-prone hash.** A digest collision between two different definitions would cause a real semantic change to be classified as a no-op, silently skipping the revision advance and the transfer.
- **It never includes schedule or machine layout data.** Two monitors that mean the same thing but were scheduled differently, quickened differently, or compiled with different JIT settings have the same key. Derived physical state is not semantic identity.

`replace_root` compares `compiled.monitor.definition_key()` against the active monitor's key to decide whether anything semantic changed at all. An unchanged key means the active monitor is kept and no transfer occurs.

`StateKey` pairs a `RegionAddress` with an owner name to identify one logical state owner portably — the granularity at which transfer decisions are reported.

## Activation frontiers

An `ActivationFrontier` describes the moment at which a replacement is being attempted. There are exactly two safe moments:

| Frontier | Meaning | Used by |
|---|---|---|
| `EmptyEvaluation { revision, interface_epoch }` | No stream has executed for the next tick, and the previous tick has committed. | Root replacement |
| `SourceBarrier { revision, region, owner_executed }` | A nested source barrier, after its prerequisite closure and before its owner executes. | Nested replacement |

The pairing is strict: root replacement requires `EmptyEvaluation`, nested replacement requires `SourceBarrier`. Neither may borrow the other's frontier.

The reason is what each frontier guarantees about in-flight state. `EmptyEvaluation` guarantees the whole machine is between rows, which is what makes it safe to discard the entire monitor including its environment and evaluator arena. `SourceBarrier` guarantees only that *this owner* has not yet run this tick — other streams already have. That is sufficient to swap one nested body, and insufficient to swap the monitor around it.

`owner_executed` is carried explicitly rather than inferred. Once the owning stream has evaluated for the current tick, replacing its nested body would change the meaning of a row that is already partly computed.

## `validate_replacement`

One function gates both paths, in a fixed order:

```text
1. base_revision == frontier.revision()?      else StaleRevision
2. definition text non-empty after trim?       else EmptyDefinition
3. target and frontier agree?                  else InvalidRootFrontier
                                                  / InvalidNestedFrontier
                                                  / TargetMismatch
                                                  / OwnerAlreadyExecuted
```

The order is meaningful. Staleness is checked first because a stale request should be reported as stale regardless of what else is wrong with it: the requester was working from a definition that is no longer running, and the most useful diagnosis is that fact rather than a downstream symptom of it.

The two nested rejections are also distinguished on purpose:

- **`TargetMismatch`** — the barrier belongs to a different region than the one the request names. The request is misaddressed.
- **`OwnerAlreadyExecuted`** — the barrier names the right region, but its owner has already run. The request is correctly addressed and merely too late.

`validate_replacement` performs no mutation, so every failure is reported before the caller has changed anything. Each caller then applies its own scope: the owner loop ends, or the monitor ends.

## Identity accounting

Two independent counters record history. Conflating them would make every semantic change look like an interface change to producers, and every route remap look like a new definition to a controller.

| Identity | Records | Advances when |
|---|---|---|
| `RevisionId` | Normalized semantic monitor-definition history. | The replacement's `DefinitionKey` differs from the active one. |
| `InterfaceEpoch` | Effective external membership, layout, and transport history. | The effective input or output bindings differ. |

The four cases follow directly:

| Change | Revision | Epoch |
|---|---|---|
| Semantics only | advances | unchanged |
| Interface only | unchanged | advances |
| Both | advances | advances |
| Exact no-op | unchanged | unchanged |

**Effective** comparison is the subtle part. It ignores unused mapping entries and honours the active transport's binding kind, so a command that supplies routes for variables the replacement does not use is not treated as an interface change. Comparing raw configuration instead would advance the epoch on cosmetic differences and force producers through an unnecessary barrier.

Both counters use `checked_next`: overflow terminates rather than reusing `u64::MAX`, which would let a stale replacement compare as current.

The pair is what a `ReconfigurationAck` carries back to producers, together with an `applied` flag that is `false` exactly when neither counter moved.

## Transfer reporting

`TransferReportEntry` pairs a `RegionAddress` and a `StateKey` with one `TransferDecision`:

| Decision | Meaning |
|---|---|
| `Transferred` | The owner's state was carried into the replacement. |
| `Reset(reason)` | The owner starts cold; the reason distinguishes a new owner from incompatible state. |
| `Rejected(reason)` | Under `Strict`, this owner's incompatibility refused the whole replacement. |

These entries are the portable explanation of a transfer. [Context transfer](context-transfer.md) covers how the decisions are reached.

[← Previous: The reconfigurable runtime](reconfigurable-runtime.md) · [Next: Context transfer](context-transfer.md) →
